// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::transaction;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000, // Ensure the timer is not triggered.
        /* max_batch_bytes */ 1_024,
        rx_transaction,
        tx_message,
    );

    // Send enough transactions to seal a batch.
    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = vec![transaction(), transaction()];
    let batch = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        /* max_batch_size */ 200,
        /* max_batch_delay */ 50, // Ensure the timer is triggered.
        /* max_batch_bytes */ 1_024,
        rx_transaction,
        tx_message,
    );

    // Do not send enough transactions to seal a batch..
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = vec![transaction()];
    let batch = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn serialized_cap_splits_before_overflow() {
    let transaction = transaction();
    let one = bincode::serialize(&WorkerMessage::Batch(vec![transaction.clone()])).unwrap();
    let two = bincode::serialize(&WorkerMessage::Batch(vec![
        transaction.clone(),
        transaction.clone(),
    ]))
    .unwrap();
    assert!(two.len() > one.len());

    let (tx_transaction, rx_transaction) = channel(2);
    let (tx_message, mut rx_message) = channel(2);
    BatchMaker::spawn(
        /* max_batch_size */ 10_000,
        /* max_batch_delay */ 1_000_000,
        /* max_batch_bytes */ one.len(),
        rx_transaction,
        tx_message,
    );

    tx_transaction.send(transaction.clone()).await.unwrap();
    tx_transaction.send(transaction.clone()).await.unwrap();

    let first = rx_message.recv().await.unwrap();
    assert_eq!(first.len(), one.len());
    match bincode::deserialize::<WorkerMessage>(&first).unwrap() {
        WorkerMessage::Batch(batch) => assert_eq!(batch, vec![transaction]),
        _ => panic!("Unexpected message"),
    }
}
