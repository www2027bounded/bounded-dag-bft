// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, listener};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use std::fs;
use tokio::sync::mpsc::channel;
use tokio::task::JoinSet;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn independently_polled_waiters_do_not_hold_store_permits() {
    let (tx, mut rx) = channel(2);
    tx.send(0).await.unwrap();
    tx.send(1).await.unwrap();

    let mut coupled = FuturesUnordered::new();
    coupled.push(tx.send(10));
    coupled.push(tx.send(11));

    // Poll both child sends once so they enter the semaphore wait queue.
    assert!(timeout(Duration::from_millis(20), coupled.next())
        .await
        .is_err());

    assert_eq!(rx.recv().await, Some(0));
    assert_eq!(rx.recv().await, Some(1));
    tokio::task::yield_now().await;

    // The released permits were assigned to the two child futures. The
    // messages are not queued until FuturesUnordered is polled again.
    assert_eq!(rx.len(), 0);
    assert_eq!(tx.capacity(), 0);
    assert!(timeout(Duration::from_millis(20), tx.send(12))
        .await
        .is_err());

    assert!(matches!(coupled.next().await, Some(Ok(()))));
    assert!(matches!(coupled.next().await, Some(Ok(()))));
    let mut coupled_values = [rx.recv().await.unwrap(), rx.recv().await.unwrap()];
    coupled_values.sort_unstable();
    assert_eq!(coupled_values, [10, 11]);

    let (tx, mut rx) = channel(2);
    tx.send(0).await.unwrap();
    tx.send(1).await.unwrap();
    let mut independent = JoinSet::new();
    for value in [10, 11] {
        let tx = tx.clone();
        independent.spawn(async move { tx.send(value).await });
    }
    tokio::task::yield_now().await;

    assert_eq!(rx.recv().await, Some(0));
    assert_eq!(rx.recv().await, Some(1));
    let first = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("independent waiter did not consume its assigned permit")
        .unwrap();
    let second = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("independent waiter did not consume its assigned permit")
        .unwrap();
    assert!([10, 11].contains(&first));
    assert!([10, 11].contains(&second));
    assert_ne!(first, second);
    while let Some(result) = independent.join_next().await {
        assert!(result.unwrap().is_ok());
    }
}

#[tokio::test]
async fn synchronize() {
    let (tx_message, rx_message) = channel(1);

    let mut keys = keys();
    let (name, _) = keys.pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(9_000);

    // Create a new test store.
    let path = ".db_test_synchronize";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Synchronizer` instance.
    Synchronizer::spawn(
        name,
        id,
        committee.clone(),
        store.clone(),
        /* gc_depth */ 50, // Not used in this test.
        /* sync_retry_delay */ 1_000_000, // Ensure it is not triggered.
        /* sync_retry_nodes */ 3, // Not used in this test.
        rx_message,
    );

    // Spawn a listener to receive our batch requests.
    let (target, _) = keys.pop().unwrap();
    let address = committee.worker(&target, &id).unwrap().worker_to_worker;
    let missing = vec![batch_digest()];
    let message = WorkerMessage::BatchRequest(missing.clone(), name);
    let serialized = bincode::serialize(&message).unwrap();
    let handle = listener(address, Some(Bytes::from(serialized)));

    // Send a sync request.
    let message = PrimaryWorkerMessage::Synchronize(missing, target);
    tx_message.send(message).await.unwrap();

    // Ensure the target receives the sync request.
    assert!(handle.await.is_ok());
}
