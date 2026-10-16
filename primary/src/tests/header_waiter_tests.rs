// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{committee_with_base_port, header, keys};
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use std::fs;
use std::sync::atomic::AtomicU64;
use tokio::net::TcpListener;
use tokio::sync::mpsc::channel;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test]
async fn missing_payload_routes_through_requester_worker_and_resumes() {
    let committee = committee_with_base_port(14_000);
    let test_header = header();
    let author = test_header.author;
    let requester = keys()
        .into_iter()
        .map(|(name, _)| name)
        .find(|name| name != &author)
        .unwrap();
    let worker_id = 0;
    let digest = Digest::default();
    let mut payload_header = test_header;
    payload_header
        .payload
        .insert(digest.clone(), worker_id);

    let control_address = committee
        .worker(&requester, &worker_id)
        .unwrap()
        .primary_to_worker;
    let listener = TcpListener::bind(control_address).await.unwrap();
    let control = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        let bytes = reader.next().await.unwrap().unwrap().freeze();
        writer.send(Bytes::from("Ack")).await.unwrap();
        bincode::deserialize::<PrimaryWorkerMessage>(&bytes).unwrap()
    });

    let path = ".db_test_header_waiter_missing_payload";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let (tx_synchronizer, rx_synchronizer) = channel(1);
    let (tx_core, mut rx_core) = channel(1);
    HeaderWaiter::spawn(
        requester,
        committee,
        store.clone(),
        Arc::new(AtomicU64::new(0)),
        50,
        1_000_000,
        3,
        rx_synchronizer,
        tx_core,
    );

    tx_synchronizer
        .send(WaiterMessage::SyncBatches(
            [(digest.clone(), worker_id)].iter().cloned().collect(),
            payload_header.clone(),
        ))
        .await
        .unwrap();

    match timeout(Duration::from_secs(5), control)
        .await
        .unwrap()
        .unwrap()
    {
        PrimaryWorkerMessage::Synchronize(digests, target) => {
            assert_eq!(digests, vec![digest.clone()]);
            assert_eq!(target, author);
        }
        _ => panic!("Expected a missing-payload synchronization command"),
    }

    let marker = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
    store.write(marker, Vec::new()).await;
    let resumed = timeout(Duration::from_secs(5), rx_core.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.id, payload_header.id);
}
