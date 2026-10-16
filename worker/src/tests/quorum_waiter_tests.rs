// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch, committee_with_base_port, keys, listener};
use crate::worker::WorkerMessage;
use bytes::Bytes;
use futures::future::try_join_all;
use futures::sink::SinkExt as _;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

fn ack_all_listener(address: SocketAddr, expected: Vec<Vec<u8>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        for message in expected {
            let received = reader.next().await.unwrap().unwrap().freeze();
            assert_eq!(received, Bytes::from(message));
            writer.send(Bytes::from_static(ACK)).await.unwrap();
        }
    })
}

#[tokio::test]
async fn baseline_waits_for_valid_quorum() {
    let (tx_message, rx_message) = mpsc::channel(1);
    let (tx_batch, mut rx_batch) = mpsc::channel(1);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(7_000);

    let message = WorkerMessage::Batch(batch());
    let serialized = bincode::serialize(&message).unwrap();
    let expected = Bytes::from(serialized.clone());

    let mut workers = Vec::new();
    let mut listener_handles = Vec::new();
    for (name, address) in committee.others_workers(&myself, /* id */ &0) {
        let address = address.worker_to_worker;
        listener_handles.push(listener(address, Some(expected.clone())));
        workers.push((name, address));
    }

    QuorumWaiter::spawn(
        committee.clone(),
        myself,
        /* local_stake */ 1,
        DisseminationMode::Baseline,
        QidReclaimPolicy::SafeCut,
        /* max_active_batches */ 4,
        /* peer_max_records */ 2,
        /* peer_max_bytes */ 1_024,
        /* reclaim_interval_ms */ 0,
        rx_message,
        tx_batch,
        workers,
    );

    tx_message.send(serialized.clone()).await.unwrap();
    let output = rx_batch.recv().await.unwrap();
    assert_eq!(output, serialized);
    assert!(try_join_all(listener_handles).await.is_ok());
}

#[tokio::test]
async fn failed_ack_channel_contributes_no_stake() {
    let (sender, receiver) = oneshot::channel();
    drop(sender);
    assert_eq!(QuorumWaiter::valid_ack(receiver, 1).await, None);
}

#[tokio::test]
async fn invalid_ack_contributes_no_stake() {
    let (sender, receiver) = oneshot::channel();
    sender.send(Bytes::from_static(b"not-an-ack")).unwrap();
    assert_eq!(QuorumWaiter::valid_ack(receiver, 1).await, None);
}

#[tokio::test]
async fn handoff_acceptance_waits_for_processor_channel_capacity() {
    let (sender, mut receiver) = mpsc::channel(1);
    sender.send(vec![1]).await.unwrap();
    let mut handoff = tokio::spawn(async move {
        QuorumWaiter::accept_handoff(&sender, vec![2]).await;
    });

    assert!(timeout(Duration::from_millis(50), &mut handoff)
        .await
        .is_err());
    assert_eq!(receiver.recv().await.unwrap(), vec![1]);
    timeout(Duration::from_secs(1), &mut handoff)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receiver.recv().await.unwrap(), vec![2]);
}

#[tokio::test]
async fn baseline_fanout_remains_pipelined() {
    let (tx_message, rx_message) = mpsc::channel(2);
    let (tx_batch, mut rx_batch) = mpsc::channel(2);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(20_000);
    let first = vec![1u8; 32];
    let second = vec![2u8; 32];
    let (tx_seen_second, mut rx_seen_second) = mpsc::channel(3);

    let mut workers = Vec::new();
    let mut listeners = Vec::new();
    for (name, addresses) in committee.others_workers(&myself, &0) {
        let address = addresses.worker_to_worker;
        let expected_first = first.clone();
        let expected_second = second.clone();
        let tx_seen_second = tx_seen_second.clone();
        listeners.push(tokio::spawn(async move {
            let listener = TcpListener::bind(&address).await.unwrap();
            let (socket, _) = listener.accept().await.unwrap();
            let (mut writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
            assert_eq!(
                reader.next().await.unwrap().unwrap().freeze(),
                Bytes::from(expected_first)
            );
            assert_eq!(
                reader.next().await.unwrap().unwrap().freeze(),
                Bytes::from(expected_second)
            );
            tx_seen_second.send(()).await.unwrap();
            writer.send(Bytes::from_static(ACK)).await.unwrap();
            writer.send(Bytes::from_static(ACK)).await.unwrap();
        }));
        workers.push((name, address));
    }
    drop(tx_seen_second);

    QuorumWaiter::spawn(
        committee,
        myself,
        1,
        DisseminationMode::Baseline,
        QidReclaimPolicy::SafeCut,
        4,
        2,
        1_024,
        0,
        rx_message,
        tx_batch,
        workers,
    );
    tx_message.send(first.clone()).await.unwrap();
    tx_message.send(second.clone()).await.unwrap();

    for _ in 0..3 {
        timeout(Duration::from_secs(3), rx_seen_second.recv())
            .await
            .expect("Second batch did not reach every peer before the first ACK")
            .expect("Pipeline listener ended early");
    }
    assert_eq!(
        timeout(Duration::from_secs(3), rx_batch.recv())
            .await
            .unwrap()
            .unwrap(),
        first
    );
    assert_eq!(
        timeout(Duration::from_secs(3), rx_batch.recv())
            .await
            .unwrap()
            .unwrap(),
        second
    );
    assert!(try_join_all(listeners).await.is_ok());
}

#[tokio::test]
async fn invalid_ack_is_quarantined_and_bounded() {
    let (tx_message, rx_message) = mpsc::channel(3);
    let (tx_batch, mut rx_batch) = mpsc::channel(3);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(21_000);
    let batches = vec![vec![10u8; 32], vec![11u8; 32], vec![12u8; 32]];
    let mut workers: Vec<_> = committee
        .others_workers(&myself, &0)
        .into_iter()
        .map(|(name, addresses)| (name, addresses.worker_to_worker))
        .collect();
    let (bad_name, bad_address) = workers.remove(0);

    let bad_first = batches[0].clone();
    let bad_listener = tokio::spawn(async move {
        let listener = TcpListener::bind(&bad_address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        assert_eq!(
            reader.next().await.unwrap().unwrap().freeze(),
            Bytes::from(bad_first)
        );
        writer
            .send(Bytes::from_static(b"Byzantine ACK"))
            .await
            .unwrap();
        assert!(timeout(Duration::from_millis(500), reader.next())
            .await
            .is_err());
    });

    let mut listeners = Vec::new();
    for (_, address) in &workers {
        listeners.push(ack_all_listener(*address, batches.clone()));
    }
    workers.push((bad_name, bad_address));

    QuorumWaiter::spawn(
        committee,
        myself,
        1,
        DisseminationMode::Qid,
        QidReclaimPolicy::SafeCut,
        4,
        1,
        1_024,
        0,
        rx_message,
        tx_batch,
        workers,
    );
    for batch in &batches {
        tx_message.send(batch.clone()).await.unwrap();
    }

    let mut delivered = Vec::new();
    for _ in 0..batches.len() {
        delivered.push(
            timeout(Duration::from_secs(3), rx_batch.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    delivered.sort();
    let mut expected = batches;
    expected.sort();
    assert_eq!(delivered, expected);
    bad_listener.await.unwrap();
    assert!(try_join_all(listeners).await.is_ok());
}

#[tokio::test]
async fn reclaim_disabled_retains_debt_after_transport_quorum() {
    let (tx_message, rx_message) = mpsc::channel(2);
    let (tx_batch, mut rx_batch) = mpsc::channel(2);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(22_000);
    let batches = vec![vec![20u8; 32], vec![21u8; 32]];
    let mut workers: Vec<_> = committee
        .others_workers(&myself, &0)
        .into_iter()
        .map(|(name, addresses)| (name, addresses.worker_to_worker))
        .collect();
    let (slow_name, slow_address) = workers.remove(0);
    let (tx_release, rx_release) = oneshot::channel();
    let slow_first = batches[0].clone();
    let slow_listener = tokio::spawn(async move {
        let listener = TcpListener::bind(&slow_address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        assert_eq!(
            reader.next().await.unwrap().unwrap().freeze(),
            Bytes::from(slow_first)
        );
        rx_release.await.unwrap();
        writer.send(Bytes::from_static(ACK)).await.unwrap();
        assert!(timeout(Duration::from_millis(500), reader.next())
            .await
            .is_err());
    });

    let mut listeners = Vec::new();
    for (_, address) in &workers {
        listeners.push(ack_all_listener(*address, batches.clone()));
    }
    workers.push((slow_name, slow_address));
    QuorumWaiter::spawn(
        committee,
        myself,
        1,
        DisseminationMode::Qid,
        QidReclaimPolicy::SafeCut,
        4,
        1,
        1_024,
        0,
        rx_message,
        tx_batch,
        workers,
    );
    for batch in &batches {
        tx_message.send(batch.clone()).await.unwrap();
    }
    for _ in 0..2 {
        timeout(Duration::from_secs(3), rx_batch.recv())
            .await
            .unwrap()
            .unwrap();
    }
    tx_release.send(()).unwrap();
    slow_listener.await.unwrap();
    assert!(try_join_all(listeners).await.is_ok());
}

#[tokio::test]
async fn recovered_peer_releases_credit_and_drains_oldest_work() {
    let (tx_message, rx_message) = mpsc::channel(2);
    let (tx_batch, mut rx_batch) = mpsc::channel(2);
    let (myself, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(23_000);
    let batches = vec![vec![30u8; 32], vec![31u8; 32]];
    let mut workers: Vec<_> = committee
        .others_workers(&myself, &0)
        .into_iter()
        .map(|(name, addresses)| (name, addresses.worker_to_worker))
        .collect();
    let (recovering_name, recovering_address) = workers.remove(0);
    let (silent_name, silent_address) = workers.remove(0);
    let (tx_first_seen, rx_first_seen) = oneshot::channel();
    let (tx_release, rx_release) = oneshot::channel();
    let recovering_batches = batches.clone();
    let recovering_listener = tokio::spawn(async move {
        let listener = TcpListener::bind(&recovering_address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (mut writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        assert_eq!(
            reader.next().await.unwrap().unwrap().freeze(),
            Bytes::from(recovering_batches[0].clone())
        );
        tx_first_seen.send(()).unwrap();
        rx_release.await.unwrap();
        writer.send(Bytes::from_static(ACK)).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(3), reader.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .freeze(),
            Bytes::from(recovering_batches[1].clone())
        );
        writer.send(Bytes::from_static(ACK)).await.unwrap();
    });
    let silent_first = batches[0].clone();
    let silent_listener = tokio::spawn(async move {
        let listener = TcpListener::bind(&silent_address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let (_writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        assert_eq!(
            reader.next().await.unwrap().unwrap().freeze(),
            Bytes::from(silent_first)
        );
        assert!(timeout(Duration::from_millis(700), reader.next())
            .await
            .is_err());
    });
    let honest_listener = ack_all_listener(workers[0].1, batches.clone());
    workers.push((recovering_name, recovering_address));
    workers.push((silent_name, silent_address));

    QuorumWaiter::spawn(
        committee,
        myself,
        1,
        DisseminationMode::Qid,
        QidReclaimPolicy::SafeCut,
        4,
        1,
        1_024,
        0,
        rx_message,
        tx_batch,
        workers,
    );
    for batch in &batches {
        tx_message.send(batch.clone()).await.unwrap();
    }
    timeout(Duration::from_secs(3), rx_first_seen)
        .await
        .unwrap()
        .unwrap();
    assert!(timeout(Duration::from_millis(200), rx_batch.recv())
        .await
        .is_err());
    tx_release.send(()).unwrap();
    for _ in 0..2 {
        timeout(Duration::from_secs(3), rx_batch.recv())
            .await
            .unwrap()
            .unwrap();
    }
    recovering_listener.await.unwrap();
    silent_listener.await.unwrap();
    honest_listener.await.unwrap();
}

#[test]
fn qid_reserves_credit_for_oldest_large_batch() {
    let peer = PublicKey::default();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (control, _control_receiver) = mpsc::unbounded_channel();
    let mut peers = HashMap::new();
    peers.insert(
        peer,
        PeerState {
            sender,
            control,
            debt_records: 1,
            debt_bytes: 4,
            outstanding: HashMap::from([(99, 4)]),
            generation: 0,
            closing: false,
            blocked: false,
            stall_since: Some(Instant::now()),
            last_retired_at: Instant::now(),
            retire_cycles: 0,
        },
    );

    let mut active = HashMap::new();
    active.insert(
        0,
        ActiveBatch {
            bytes: Bytes::from(vec![0u8; 8]),
            digest: QuorumWaiter::digest(&[0u8; 8]),
            acknowledged_stake: 1,
            acknowledged: HashSet::new(),
            admitted: HashSet::new(),
        },
    );
    active.insert(
        1,
        ActiveBatch {
            bytes: Bytes::from(vec![0u8; 2]),
            digest: QuorumWaiter::digest(&[0u8; 2]),
            acknowledged_stake: 1,
            acknowledged: HashSet::new(),
            admitted: HashSet::new(),
        },
    );
    let order = VecDeque::from(vec![0, 1]);
    let started_at = Instant::now();
    let mut event_sequence = 0;

    QuorumWaiter::schedule_qid(
        &mut active,
        &order,
        &mut peers,
        3,
        10,
        QidReclaimPolicy::SafeCut,
        peer,
        started_at,
        &mut event_sequence,
        "qid",
    );
    assert!(receiver.try_recv().is_err());
    assert!(!active.get(&0).unwrap().admitted.contains(&peer));
    assert!(!active.get(&1).unwrap().admitted.contains(&peer));

    let state = peers.get_mut(&peer).unwrap();
    state.debt_records = 0;
    state.debt_bytes = 0;
    state.outstanding.clear();
    QuorumWaiter::schedule_qid(
        &mut active,
        &order,
        &mut peers,
        3,
        10,
        QidReclaimPolicy::SafeCut,
        peer,
        started_at,
        &mut event_sequence,
        "qid",
    );
    assert_eq!(receiver.try_recv().unwrap().batch_id, 0);
    assert_eq!(receiver.try_recv().unwrap().batch_id, 1);
}

#[test]
fn late_duplicate_and_out_of_order_receipts_are_safe() {
    let (sender, _receiver) = mpsc::unbounded_channel();
    let (control, _control_receiver) = mpsc::unbounded_channel();
    let mut peer = PeerState {
        sender,
        control,
        debt_records: 2,
        debt_bytes: 12,
        outstanding: HashMap::from([(10, 5), (11, 7)]),
        generation: 0,
        closing: false,
        blocked: false,
        stall_since: Some(Instant::now()),
        last_retired_at: Instant::now(),
        retire_cycles: 0,
    };

    assert!(QuorumWaiter::release_peer_debt(&mut peer, 11, 7));
    assert_eq!(peer.debt_records, 1);
    assert_eq!(peer.debt_bytes, 5);
    assert!(QuorumWaiter::release_peer_debt(&mut peer, 10, 999));
    assert_eq!(peer.debt_records, 0);
    assert_eq!(peer.debt_bytes, 0);
    assert!(!QuorumWaiter::release_peer_debt(&mut peer, 10, 5));
    assert_eq!(peer.debt_records, 0);
    assert_eq!(peer.debt_bytes, 0);
}

#[test]
fn reclaim_active_debt_blocks_safe_retirement() {
    let peer = PublicKey::default();
    let (sender, _receiver) = mpsc::unbounded_channel();
    let (control, mut control_receiver) = mpsc::unbounded_channel();
    let mut peers = HashMap::from([(
        peer,
        PeerState {
            sender,
            control,
            debt_records: 1,
            debt_bytes: 8,
            outstanding: HashMap::from([(7, 8)]),
            generation: 0,
            closing: false,
            blocked: true,
            stall_since: Some(Instant::now() - Duration::from_secs(2)),
            last_retired_at: Instant::now() - Duration::from_secs(2),
            retire_cycles: 0,
        },
    )]);
    let active = HashMap::from([(
        7,
        ActiveBatch {
            bytes: Bytes::from(vec![1u8; 8]),
            digest: QuorumWaiter::digest(&[1u8; 8]),
            acknowledged_stake: 1,
            acknowledged: HashSet::new(),
            admitted: HashSet::from([peer]),
        },
    )]);

    let mut event_sequence = 0;
    QuorumWaiter::start_retirements(
        &active,
        &mut peers,
        1,
        64,
        Duration::from_secs(1),
        QidReclaimPolicy::SafeCut,
        peer,
        Instant::now(),
        &mut event_sequence,
        "qid",
    );

    let state = peers.get(&peer).unwrap();
    assert!(!state.closing);
    assert_eq!(state.debt_records, 1);
    assert_eq!(state.debt_bytes, 8);
    assert!(control_receiver.try_recv().is_err());
}

#[test]
fn reclaim_completed_capped_debt_stays_charged_until_matching_retired() {
    let peer = PublicKey::default();
    let (sender, _receiver) = mpsc::unbounded_channel();
    let (control, mut control_receiver) = mpsc::unbounded_channel();
    let mut peers = HashMap::from([(
        peer,
        PeerState {
            sender,
            control,
            debt_records: 2,
            debt_bytes: 12,
            outstanding: HashMap::from([(10, 5), (11, 7)]),
            generation: 3,
            closing: false,
            blocked: false,
            stall_since: Some(Instant::now() - Duration::from_secs(2)),
            last_retired_at: Instant::now() - Duration::from_secs(2),
            retire_cycles: 0,
        },
    )]);

    let mut event_sequence = 0;
    QuorumWaiter::start_retirements(
        &HashMap::new(),
        &mut peers,
        2,
        64,
        Duration::from_secs(1),
        QidReclaimPolicy::SafeCut,
        peer,
        Instant::now(),
        &mut event_sequence,
        "qid",
    );

    match control_receiver.try_recv().unwrap() {
        PeerControl::Retire { generation } => assert_eq!(generation, 3),
    }
    let state = peers.get_mut(&peer).unwrap();
    assert!(state.closing);
    assert_eq!(state.retire_cycles, 1);
    assert_eq!(state.debt_records, 2);
    assert_eq!(state.debt_bytes, 12);
    assert!(!QuorumWaiter::release_receipt_debt(state, 10, 5, 3));
    assert_eq!(state.outstanding.len(), 2);
    assert_eq!(QuorumWaiter::complete_retirement(state, 2), None);
    assert_eq!(state.debt_records, 2);

    assert_eq!(
        QuorumWaiter::complete_retirement(state, 3),
        Some((2, 12, 4))
    );
    assert_eq!(state.generation, 4);
    assert!(!state.closing);
    assert_eq!(state.debt_records, 0);
    assert_eq!(state.debt_bytes, 0);
    assert!(state.outstanding.is_empty());
    assert_eq!(QuorumWaiter::complete_retirement(state, 3), None);
}

#[test]
fn timeout_replay_fences_mixed_active_and_completed_debt() {
    let peer = PublicKey::default();
    let (sender, _receiver) = mpsc::unbounded_channel();
    let (control, mut control_receiver) = mpsc::unbounded_channel();
    let mut peers = HashMap::from([(
        peer,
        PeerState {
            sender,
            control,
            debt_records: 2,
            debt_bytes: 12,
            outstanding: HashMap::from([(10, 5), (11, 7)]),
            generation: 4,
            closing: false,
            blocked: true,
            stall_since: Some(Instant::now() - Duration::from_secs(2)),
            last_retired_at: Instant::now() - Duration::from_secs(2),
            retire_cycles: 0,
        },
    )]);
    let active = HashMap::from([(
        10,
        ActiveBatch {
            bytes: Bytes::from(vec![1u8; 5]),
            digest: QuorumWaiter::digest(&[1u8; 5]),
            acknowledged_stake: 1,
            acknowledged: HashSet::new(),
            admitted: HashSet::from([peer]),
        },
    )]);
    let mut event_sequence = 0;

    QuorumWaiter::start_retirements(
        &active,
        &mut peers,
        2,
        64,
        Duration::from_secs(1),
        QidReclaimPolicy::TimeoutReplay,
        peer,
        Instant::now(),
        &mut event_sequence,
        "qid",
    );

    assert!(peers.get(&peer).unwrap().closing);
    assert_eq!(peers.get(&peer).unwrap().debt_records, 2);
    assert_eq!(peers.get(&peer).unwrap().debt_bytes, 12);
    match control_receiver.try_recv().unwrap() {
        PeerControl::Retire { generation } => assert_eq!(generation, 4),
    }
}

#[test]
fn timeout_replay_selection_excludes_batch_completed_during_closing() {
    let peer = PublicKey::default();
    let (sender, _receiver) = mpsc::unbounded_channel();
    let (control, _control_receiver) = mpsc::unbounded_channel();
    let state = PeerState {
        sender,
        control,
        debt_records: 2,
        debt_bytes: 12,
        outstanding: HashMap::from([(20, 5), (21, 7)]),
        generation: 2,
        closing: true,
        blocked: true,
        stall_since: Some(Instant::now()),
        last_retired_at: Instant::now(),
        retire_cycles: 1,
    };
    let mut active = HashMap::from([
        (
            20,
            ActiveBatch {
                bytes: Bytes::from(vec![2u8; 5]),
                digest: QuorumWaiter::digest(&[2u8; 5]),
                acknowledged_stake: 1,
                acknowledged: HashSet::new(),
                admitted: HashSet::from([peer]),
            },
        ),
        (
            21,
            ActiveBatch {
                bytes: Bytes::from(vec![3u8; 7]),
                digest: QuorumWaiter::digest(&[3u8; 7]),
                acknowledged_stake: 1,
                acknowledged: HashSet::new(),
                admitted: HashSet::from([peer]),
            },
        ),
    ]);
    let order = VecDeque::from([20, 21]);

    active.remove(&20);
    assert_eq!(
        QuorumWaiter::select_replay_batches(&active, &order, &state),
        vec![21]
    );
}

#[test]
fn timeout_replay_reserves_oldest_subset_before_younger_admission() {
    let peer = PublicKey::default();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let (control, _control_receiver) = mpsc::unbounded_channel();
    let mut peers = HashMap::from([(
        peer,
        PeerState {
            sender,
            control,
            debt_records: 2,
            debt_bytes: 12,
            outstanding: HashMap::from([(30, 5), (31, 7)]),
            generation: 7,
            closing: true,
            blocked: true,
            stall_since: Some(Instant::now()),
            last_retired_at: Instant::now(),
            retire_cycles: 1,
        },
    )]);
    let mut active = HashMap::from([
        (
            30,
            ActiveBatch {
                bytes: Bytes::from(vec![4u8; 5]),
                digest: QuorumWaiter::digest(&[4u8; 5]),
                acknowledged_stake: 1,
                acknowledged: HashSet::new(),
                admitted: HashSet::from([peer]),
            },
        ),
        (
            31,
            ActiveBatch {
                bytes: Bytes::from(vec![5u8; 7]),
                digest: QuorumWaiter::digest(&[5u8; 7]),
                acknowledged_stake: 1,
                acknowledged: HashSet::new(),
                admitted: HashSet::from([peer]),
            },
        ),
        (
            32,
            ActiveBatch {
                bytes: Bytes::from(vec![6u8; 3]),
                digest: QuorumWaiter::digest(&[6u8; 3]),
                acknowledged_stake: 1,
                acknowledged: HashSet::new(),
                admitted: HashSet::new(),
            },
        ),
    ]);
    let order = VecDeque::from([31, 30, 32]);
    let replay_ids =
        QuorumWaiter::select_replay_batches(&active, &order, peers.get(&peer).unwrap());
    assert_eq!(replay_ids, vec![31, 30]);

    assert_eq!(
        QuorumWaiter::complete_retirement(peers.get_mut(&peer).unwrap(), 7),
        Some((2, 12, 8))
    );
    let mut event_sequence = 0;
    QuorumWaiter::reserve_replays(
        &mut active,
        &mut peers,
        peer,
        7,
        &replay_ids,
        12,
        QidReclaimPolicy::TimeoutReplay,
        peer,
        Instant::now(),
        &mut event_sequence,
        "qid",
    );

    assert_eq!(receiver.try_recv().unwrap().batch_id, 31);
    assert_eq!(receiver.try_recv().unwrap().batch_id, 30);
    assert!(receiver.try_recv().is_err());
    assert_eq!(peers.get(&peer).unwrap().debt_records, 2);
    assert_eq!(peers.get(&peer).unwrap().debt_bytes, 12);
    assert_eq!(active.get(&30).unwrap().acknowledged_stake, 1);
    assert_eq!(active.get(&31).unwrap().acknowledged_stake, 1);

    QuorumWaiter::schedule_qid(
        &mut active,
        &order,
        &mut peers,
        2,
        12,
        QidReclaimPolicy::TimeoutReplay,
        peer,
        Instant::now(),
        &mut event_sequence,
        "qid",
    );
    assert!(receiver.try_recv().is_err());
    assert!(!active.get(&32).unwrap().admitted.contains(&peer));
}

#[test]
fn reclaim_stale_receipts_cannot_release_reopened_generation_credit() {
    let (sender, _receiver) = mpsc::unbounded_channel();
    let (control, _control_receiver) = mpsc::unbounded_channel();
    let mut peer = PeerState {
        sender,
        control,
        debt_records: 1,
        debt_bytes: 9,
        outstanding: HashMap::from([(20, 9)]),
        generation: 5,
        closing: false,
        blocked: false,
        stall_since: Some(Instant::now()),
        last_retired_at: Instant::now(),
        retire_cycles: 2,
    };

    assert!(!QuorumWaiter::release_receipt_debt(&mut peer, 20, 9, 4));
    assert_eq!(peer.debt_records, 1);
    assert_eq!(peer.debt_bytes, 9);
    assert!(QuorumWaiter::release_receipt_debt(&mut peer, 20, 9, 5));
    assert!(!QuorumWaiter::release_receipt_debt(&mut peer, 20, 9, 5));
    assert_eq!(peer.debt_records, 0);
    assert_eq!(peer.debt_bytes, 0);
}

#[test]
fn reclaim_generation_overflow_keeps_closing_debt_charged() {
    let (sender, _receiver) = mpsc::unbounded_channel();
    let (control, _control_receiver) = mpsc::unbounded_channel();
    let mut peer = PeerState {
        sender,
        control,
        debt_records: 1,
        debt_bytes: 6,
        outstanding: HashMap::from([(30, 6)]),
        generation: u64::MAX,
        closing: true,
        blocked: true,
        stall_since: Some(Instant::now()),
        last_retired_at: Instant::now(),
        retire_cycles: 1,
    };

    let overflow = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        QuorumWaiter::complete_retirement(&mut peer, u64::MAX)
    }));
    assert!(overflow.is_err());
    assert!(peer.closing);
    assert_eq!(peer.generation, u64::MAX);
    assert_eq!(peer.debt_records, 1);
    assert_eq!(peer.debt_bytes, 6);
    assert_eq!(peer.outstanding, HashMap::from([(30, 6)]));
}

#[test]
fn reclaim_closing_peer_does_not_block_open_peer_admission() {
    let closing_peer = PublicKey::default();
    let open_peer = keys()
        .into_iter()
        .map(|(name, _)| name)
        .find(|name| name != &closing_peer)
        .unwrap();
    let (closing_sender, mut closing_receiver) = mpsc::unbounded_channel();
    let (closing_control, _closing_control_receiver) = mpsc::unbounded_channel();
    let (open_sender, mut open_receiver) = mpsc::unbounded_channel();
    let (open_control, _open_control_receiver) = mpsc::unbounded_channel();
    let mut peers = HashMap::from([
        (
            closing_peer,
            PeerState {
                sender: closing_sender,
                control: closing_control,
                debt_records: 1,
                debt_bytes: 4,
                outstanding: HashMap::from([(50, 4)]),
                generation: 0,
                closing: true,
                blocked: true,
                stall_since: Some(Instant::now()),
                last_retired_at: Instant::now(),
                retire_cycles: 1,
            },
        ),
        (
            open_peer,
            PeerState {
                sender: open_sender,
                control: open_control,
                debt_records: 0,
                debt_bytes: 0,
                outstanding: HashMap::new(),
                generation: 0,
                closing: false,
                blocked: false,
                stall_since: None,
                last_retired_at: Instant::now(),
                retire_cycles: 0,
            },
        ),
    ]);
    let mut active = HashMap::from([(
        51,
        ActiveBatch {
            bytes: Bytes::from(vec![2u8; 8]),
            digest: QuorumWaiter::digest(&[2u8; 8]),
            acknowledged_stake: 1,
            acknowledged: HashSet::new(),
            admitted: HashSet::new(),
        },
    )]);
    let order = VecDeque::from([51]);
    let mut event_sequence = 0;

    QuorumWaiter::schedule_qid(
        &mut active,
        &order,
        &mut peers,
        2,
        64,
        QidReclaimPolicy::SafeCut,
        closing_peer,
        Instant::now(),
        &mut event_sequence,
        "qid",
    );

    assert!(closing_receiver.try_recv().is_err());
    let delivery = open_receiver.try_recv().unwrap();
    assert_eq!(delivery.batch_id, 51);
    assert_eq!(delivery.generation, 0);
    assert!(active.get(&51).unwrap().admitted.contains(&open_peer));
    assert!(!active.get(&51).unwrap().admitted.contains(&closing_peer));
}

#[tokio::test]
async fn reclaim_peer_destroys_quarantined_and_queued_generation_state() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let first = Bytes::from_static(b"first old delivery");
    let queued = Bytes::from_static(b"queued old delivery");
    let fresh = Bytes::from_static(b"fresh generation delivery");
    let server_first = first.clone();
    let server_fresh = fresh.clone();
    let server = tokio::spawn(async move {
        let (old_socket, _) = listener.accept().await.unwrap();
        let (mut old_writer, mut old_reader) =
            Framed::new(old_socket, LengthDelimitedCodec::new()).split();
        assert_eq!(
            old_reader.next().await.unwrap().unwrap().freeze(),
            server_first
        );
        old_writer
            .send(Bytes::from_static(b"invalid acknowledgement"))
            .await
            .unwrap();
        while old_reader.next().await.is_some() {}

        let (new_socket, _) = listener.accept().await.unwrap();
        let (mut new_writer, mut new_reader) =
            Framed::new(new_socket, LengthDelimitedCodec::new()).split();
        assert_eq!(
            new_reader.next().await.unwrap().unwrap().freeze(),
            server_fresh
        );
        new_writer.send(Bytes::from_static(ACK)).await.unwrap();
    });

    let peer = PublicKey::default();
    let (delivery_sender, delivery_receiver) = mpsc::unbounded_channel();
    let (control_sender, control_receiver) = mpsc::unbounded_channel();
    let (event_sender, mut event_receiver) = mpsc::unbounded_channel();
    QuorumWaiter::spawn_peer(
        peer,
        address,
        delivery_receiver,
        control_receiver,
        event_sender,
    );
    delivery_sender
        .send(PeerDelivery {
            batch_id: 40,
            digest: QuorumWaiter::digest(first.as_ref()),
            bytes: first,
            generation: 0,
        })
        .unwrap();
    delivery_sender
        .send(PeerDelivery {
            batch_id: 41,
            digest: QuorumWaiter::digest(queued.as_ref()),
            bytes: queued,
            generation: 0,
        })
        .unwrap();

    match timeout(Duration::from_secs(3), event_receiver.recv())
        .await
        .unwrap()
        .unwrap()
    {
        PeerEvent::Quarantined {
            batch_id,
            generation,
            ..
        } => {
            assert_eq!(batch_id, 40);
            assert_eq!(generation, 0);
        }
        _ => panic!("Expected invalid ACK quarantine"),
    }
    control_sender
        .send(PeerControl::Retire { generation: 0 })
        .unwrap();
    match timeout(Duration::from_secs(3), event_receiver.recv())
        .await
        .unwrap()
        .unwrap()
    {
        PeerEvent::Retired { generation, .. } => assert_eq!(generation, 0),
        _ => panic!("Expected old generation retirement"),
    }

    delivery_sender
        .send(PeerDelivery {
            batch_id: 42,
            digest: QuorumWaiter::digest(fresh.as_ref()),
            bytes: fresh,
            generation: 1,
        })
        .unwrap();
    match timeout(Duration::from_secs(3), event_receiver.recv())
        .await
        .unwrap()
        .unwrap()
    {
        PeerEvent::Receipt {
            batch_id,
            generation,
            ..
        } => {
            assert_eq!(batch_id, 42);
            assert_eq!(generation, 1);
        }
        _ => panic!("Expected fresh generation receipt"),
    }
    server.await.unwrap();
}
