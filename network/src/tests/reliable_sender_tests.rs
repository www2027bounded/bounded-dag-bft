// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use futures::future::try_join_all;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;

fn ack_listener(listener: TcpListener, expected: Bytes) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) =
            Framed::new(socket, LengthDelimitedCodec::new()).split();
        assert_eq!(reader.next().await.unwrap().unwrap().freeze(), expected);
        writer.send(Bytes::from_static(b"Ack")).await.unwrap();
    })
}

async fn ephemeral_ack_listener(expected: Bytes) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    (address, ack_listener(listener, expected))
}

#[tokio::test]
async fn send() {
    let message = "Hello, world!";
    let (address, handle) = ephemeral_ack_listener(Bytes::from(message)).await;

    // Make the network sender and send the message.
    let mut sender = ReliableSender::new();
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    // Ensure we get back an acknowledgement.
    assert!(cancel_handler.await.is_ok());

    // Ensure the server received the expected message (ie. it did not panic).
    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn broadcast() {
    // Run 3 TCP servers.
    let message = "Hello, world!";
    let mut handles = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..3 {
        let (address, handle) = ephemeral_ack_listener(Bytes::from(message)).await;
        addresses.push(address);
        handles.push(handle);
    }

    // Make the network sender and send the message.
    let mut sender = ReliableSender::new();
    let cancel_handlers = sender.broadcast(addresses, Bytes::from(message)).await;

    // Ensure we get back an acknowledgement for each message.
    assert!(try_join_all(cancel_handlers).await.is_ok());

    // Ensure all servers received the broadcast.
    assert!(try_join_all(handles).await.is_ok());
}

#[tokio::test]
async fn retry() {
    // Reserve an ephemeral address, then release it so the first connection
    // attempt observes that no listener is running.
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let message = "Hello, world!";
    let mut sender = ReliableSender::new();
    let cancel_handler = sender.send(address, Bytes::from(message)).await;

    // Run a TCP server.
    sleep(Duration::from_millis(50)).await;
    let listener = TcpListener::bind(address).await.unwrap();
    let handle = ack_listener(listener, Bytes::from(message));

    // Ensure we get back an acknowledgement.
    assert!(cancel_handler.await.is_ok());

    // Ensure the server received the message (ie. it did not panic).
    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn dropping_sender_preserves_in_flight_delivery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (seen_sender, seen_receiver) = oneshot::channel();
    let (ack_sender, ack_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) =
            Framed::new(socket, LengthDelimitedCodec::new()).split();
        assert_eq!(
            reader.next().await.unwrap().unwrap().freeze(),
            Bytes::from_static(b"in flight")
        );
        seen_sender.send(()).unwrap();
        ack_receiver.await.unwrap();
        writer.send(Bytes::from_static(b"Ack")).await.unwrap();
    });

    let mut sender = ReliableSender::new();
    let handler = sender
        .send(address, Bytes::from_static(b"in flight"))
        .await;
    timeout(Duration::from_secs(3), seen_receiver)
        .await
        .unwrap()
        .unwrap();
    drop(sender);
    ack_sender.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), handler)
            .await
            .unwrap()
            .unwrap(),
        Bytes::from_static(b"Ack")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn retire_joins_old_generation_and_isolates_delayed_ack() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (first_seen_sender, first_seen_receiver) = oneshot::channel();
    let (try_old_ack_sender, try_old_ack_receiver) = oneshot::channel();
    let (old_ack_attempted_sender, old_ack_attempted_receiver) = oneshot::channel();
    let (second_seen_sender, second_seen_receiver) = oneshot::channel();
    let (ack_second_sender, ack_second_receiver) = oneshot::channel();

    let server = tokio::spawn(async move {
        let (old_socket, _) = listener.accept().await.unwrap();
        let (mut old_writer, mut old_reader) =
            Framed::new(old_socket, LengthDelimitedCodec::new()).split();
        assert_eq!(
            old_reader.next().await.unwrap().unwrap().freeze(),
            Bytes::from_static(b"old generation")
        );
        first_seen_sender.send(()).unwrap();

        try_old_ack_receiver.await.unwrap();
        let _ = old_writer.send(Bytes::from_static(b"Ack")).await;
        old_ack_attempted_sender.send(()).unwrap();

        let (new_socket, _) = listener.accept().await.unwrap();
        let (mut new_writer, mut new_reader) =
            Framed::new(new_socket, LengthDelimitedCodec::new()).split();
        assert_eq!(
            new_reader.next().await.unwrap().unwrap().freeze(),
            Bytes::from_static(b"new generation")
        );
        second_seen_sender.send(()).unwrap();
        ack_second_receiver.await.unwrap();
        new_writer.send(Bytes::from_static(b"Ack")).await.unwrap();
    });

    let mut sender = ReliableSender::new();
    let old_handler = sender
        .send(address, Bytes::from_static(b"old generation"))
        .await;
    timeout(Duration::from_secs(3), first_seen_receiver)
        .await
        .unwrap()
        .unwrap();

    assert!(sender.retire(address).await);
    assert!(old_handler.await.is_err());

    let mut new_handler = sender
        .send(address, Bytes::from_static(b"new generation"))
        .await;
    try_old_ack_sender.send(()).unwrap();
    old_ack_attempted_receiver.await.unwrap();
    timeout(Duration::from_secs(3), second_seen_receiver)
        .await
        .unwrap()
        .unwrap();
    assert!(timeout(Duration::from_millis(100), &mut new_handler)
        .await
        .is_err());
    ack_second_sender.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), new_handler)
            .await
            .unwrap()
            .unwrap(),
        Bytes::from_static(b"Ack")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn retire_repeatedly_joins_connected_no_ack_generations() {
    const GENERATIONS: u8 = 3;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (seen_sender, mut seen_receiver) = mpsc::channel(GENERATIONS as usize);
    let server = tokio::spawn(async move {
        for generation in 0..GENERATIONS {
            let (socket, _) = listener.accept().await.unwrap();
            let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
            assert_eq!(
                transport.next().await.unwrap().unwrap().freeze(),
                Bytes::from(vec![generation])
            );
            seen_sender.send(generation).await.unwrap();
        }
    });

    let mut sender = ReliableSender::new();
    for generation in 0..GENERATIONS {
        let handler = sender.send(address, Bytes::from(vec![generation])).await;
        assert_eq!(
            timeout(Duration::from_secs(3), seen_receiver.recv())
                .await
                .unwrap(),
            Some(generation)
        );
        assert!(sender.retire(address).await);
        assert!(handler.await.is_err());
    }
    assert!(!sender.retire(address).await);
    server.await.unwrap();
}

#[tokio::test]
async fn retire_joins_connection_blocked_by_nonreading_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_sender, accepted_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        accepted_sender.send(()).unwrap();
        release_receiver.await.unwrap();
    });

    let mut sender = ReliableSender::new();
    let payload = Bytes::from(vec![7u8; 1024 * 1024]);
    let mut handlers = Vec::new();
    for _ in 0..32 {
        handlers.push(sender.send(address, payload.clone()).await);
    }
    timeout(Duration::from_secs(3), accepted_receiver)
        .await
        .unwrap()
        .unwrap();

    assert!(sender.retire(address).await);
    for handler in handlers {
        assert!(handler.await.is_err());
    }
    release_sender.send(()).unwrap();
    server.await.unwrap();
}
