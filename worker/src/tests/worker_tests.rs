// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, listener, transaction};
use futures::{SinkExt as _, StreamExt as _};
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use std::fs;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;
use tokio::time::{sleep, timeout, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

fn unused_local_address() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

#[tokio::test]
async fn ack_recovery_timer_updates_all_handler_clones_once() {
    let (tx_helper, _rx_helper) = channel(1);
    let (tx_processor, _rx_processor) = channel(1);
    let handler = WorkerReceiverHandler {
        tx_helper,
        tx_processor,
        ack_policy: WorkerAckPolicy::new(WorkerAckMode::Withhold),
    };
    let first_clone = handler.clone();
    let second_clone = handler.clone();
    let (authority, _) = keys().pop().unwrap();

    spawn_ack_recovery(handler.ack_policy.clone(), authority, 0, 20);
    sleep(Duration::from_millis(100)).await;

    assert!(handler.ack_policy.should_ack());
    assert!(first_clone.ack_policy.should_ack());
    assert!(second_clone.ack_policy.should_ack());
    assert!(!handler.ack_policy.recover());
}

#[tokio::test]
async fn ack_recovery_acks_only_a_new_frame_on_the_same_live_socket() {
    let address = unused_local_address();
    let (tx_helper, mut rx_helper) = channel(2);
    let (tx_processor, _rx_processor) = channel(1);
    let ack_policy = WorkerAckPolicy::new(WorkerAckMode::Withhold);
    let handler = WorkerReceiverHandler {
        tx_helper,
        tx_processor,
        ack_policy: ack_policy.clone(),
    };
    Receiver::spawn(address, handler);
    sleep(Duration::from_millis(50)).await;

    let (requestor, _) = keys().pop().unwrap();
    let request = Bytes::from(
        bincode::serialize(&WorkerMessage::BatchRequest(Vec::new(), requestor)).unwrap(),
    );
    let stream = TcpStream::connect(address).await.unwrap();
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());

    transport.send(request.clone()).await.unwrap();
    assert!(timeout(Duration::from_secs(1), rx_helper.recv())
        .await
        .unwrap()
        .is_some());
    assert!(timeout(Duration::from_millis(50), transport.next())
        .await
        .is_err());

    assert!(ack_policy.recover());
    assert!(timeout(Duration::from_millis(50), transport.next())
        .await
        .is_err());

    transport.send(request).await.unwrap();
    let ack = timeout(Duration::from_secs(1), transport.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(&ack[..], b"Ack");
    assert!(timeout(Duration::from_secs(1), rx_helper.recv())
        .await
        .unwrap()
        .is_some());
    assert!(timeout(Duration::from_millis(50), transport.next())
        .await
        .is_err());
}

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Worker` instance.
    Worker::spawn(name, id, committee.clone(), parameters, store);

    // Spawn a network listener to receive our batch's digest.
    let primary_address = committee.primary(&name).unwrap().worker_to_primary;
    let expected = bincode::serialize(&WorkerPrimaryMessage::OurBatch(batch_digest(), id)).unwrap();
    let handle = listener(primary_address, Some(Bytes::from(expected)));

    // Spawn enough workers' listeners to acknowledge our batches.
    for (_, addresses) in committee.others_workers(&name, &id) {
        let address = addresses.worker_to_worker;
        let _ = listener(address, /* expected */ None);
    }

    // Send enough transactions to create a batch.
    let mut network = SimpleSender::new();
    let address = committee.worker(&name, &id).unwrap().transactions;
    network.send(address, Bytes::from(transaction())).await;
    network.send(address, Bytes::from(transaction())).await;

    // Ensure the primary received the batch's digest (ie. it did not panic).
    assert!(handle.await.is_ok());
}
