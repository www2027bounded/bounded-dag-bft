// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use bytes::Bytes;
use config::{Committee, DisseminationMode, QidReclaimPolicy, Stake};
use crypto::{Digest, PublicKey};
use ed25519_dalek::{Digest as _, Sha512};
use futures::future::BoxFuture;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{error, info, warn};
use network::{CancelHandler, ReliableSender};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryInto as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::time::{Duration, Instant, MissedTickBehavior};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

type BatchId = u64;

const ACK: &[u8] = b"Ack";

#[derive(Clone)]
struct PeerDelivery {
    batch_id: BatchId,
    digest: Digest,
    bytes: Bytes,
    generation: u64,
}

enum PeerControl {
    Retire { generation: u64 },
}

enum PeerEvent {
    Receipt {
        batch_id: BatchId,
        digest: Digest,
        peer: PublicKey,
        bytes: usize,
        generation: u64,
    },
    Quarantined {
        batch_id: BatchId,
        digest: Digest,
        peer: PublicKey,
        bytes: usize,
        reason: String,
        generation: u64,
    },
    Fatal {
        peer: PublicKey,
        reason: String,
        generation: u64,
    },
    Retired {
        peer: PublicKey,
        generation: u64,
    },
}

enum PeerActorStep {
    Control(Option<PeerControl>),
    Delivery(Option<PeerDelivery>),
    Sent(CancelHandler),
    Receipt(
        Option<(
            PeerDelivery,
            Result<Bytes, tokio::sync::oneshot::error::RecvError>,
        )>,
    ),
}

struct ActiveBatch {
    bytes: Bytes,
    digest: Digest,
    acknowledged_stake: Stake,
    acknowledged: HashSet<PublicKey>,
    admitted: HashSet<PublicKey>,
}

struct PeerState {
    sender: UnboundedSender<PeerDelivery>,
    control: UnboundedSender<PeerControl>,
    debt_records: usize,
    debt_bytes: usize,
    outstanding: HashMap<BatchId, usize>,
    generation: u64,
    closing: bool,
    blocked: bool,
    stall_since: Option<Instant>,
    last_retired_at: Instant,
    retire_cycles: u64,
}

/// Selectable worker payload dissemination implementations.
pub struct QuorumWaiter;

impl QuorumWaiter {
    fn policy_name(policy: QidReclaimPolicy) -> &'static str {
        match policy {
            QidReclaimPolicy::SafeCut => "safe_cut",
            QidReclaimPolicy::TimeoutReplay => "timeout_replay",
        }
    }

    fn next_sequence(sequence: &mut u64) -> u64 {
        let current = *sequence;
        *sequence = sequence
            .checked_add(1)
            .expect("Manager event sequence overflow");
        current
    }

    fn digest(bytes: &[u8]) -> Digest {
        Digest(
            Sha512::digest(bytes).as_slice()[..32]
                .try_into()
                .expect("SHA-512 prefix has fixed size"),
        )
    }

    fn invalid_ack_reason(bytes: &Bytes) -> String {
        let prefix = &bytes[..bytes.len().min(32)];
        format!("invalid ACK length={} prefix={:?}", bytes.len(), prefix)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        committee: Committee,
        authority: PublicKey,
        local_stake: Stake,
        mode: DisseminationMode,
        reclaim_policy: QidReclaimPolicy,
        max_active_batches: usize,
        peer_max_records: usize,
        peer_max_bytes: usize,
        reclaim_interval_ms: u64,
        rx_batch: Receiver<SerializedBatchMessage>,
        tx_batch: Sender<SerializedBatchMessage>,
        workers: Vec<(PublicKey, SocketAddr)>,
    ) {
        assert!(
            max_active_batches > 0,
            "Active batch window must be positive"
        );
        assert!(peer_max_records > 0, "Peer record budget must be positive");
        assert!(peer_max_bytes > 0, "Peer byte budget must be positive");

        tokio::spawn(async move {
            match mode {
                DisseminationMode::Baseline => {
                    Self::run_baseline(
                        committee,
                        authority,
                        local_stake,
                        rx_batch,
                        tx_batch,
                        workers,
                    )
                    .await
                }
                DisseminationMode::Concurrent => {
                    Self::run_isolated(
                        committee,
                        authority,
                        local_stake,
                        max_active_batches,
                        None,
                        None,
                        0,
                        reclaim_policy,
                        rx_batch,
                        tx_batch,
                        workers,
                        "concurrent",
                    )
                    .await
                }
                DisseminationMode::Qid => {
                    Self::run_isolated(
                        committee,
                        authority,
                        local_stake,
                        max_active_batches,
                        Some(peer_max_records),
                        Some(peer_max_bytes),
                        reclaim_interval_ms,
                        reclaim_policy,
                        rx_batch,
                        tx_batch,
                        workers,
                        "qid",
                    )
                    .await
                }
            }
        });
    }

    async fn valid_ack(wait_for: CancelHandler, deliver: Stake) -> Option<Stake> {
        match wait_for.await {
            Ok(bytes) if bytes.as_ref() == ACK => Some(deliver),
            Ok(bytes) => {
                warn!("Ignoring worker {}", Self::invalid_ack_reason(&bytes));
                None
            }
            Err(failure) => {
                warn!("Ignoring failed worker ACK channel: {}", failure);
                None
            }
        }
    }

    async fn accept_handoff(
        tx_batch: &Sender<SerializedBatchMessage>,
        batch: SerializedBatchMessage,
    ) {
        tx_batch.send(batch).await.expect("Failed to deliver batch");
    }

    async fn run_baseline(
        committee: Committee,
        authority: PublicKey,
        local_stake: Stake,
        mut rx_batch: Receiver<SerializedBatchMessage>,
        tx_batch: Sender<SerializedBatchMessage>,
        workers: Vec<(PublicKey, SocketAddr)>,
    ) {
        struct BaselineBatch {
            batch_id: BatchId,
            digest: Digest,
            bytes: SerializedBatchMessage,
            handlers: Vec<(PublicKey, CancelHandler)>,
        }

        // Preserve the native two-stage pipeline: the broadcaster keeps accepting
        // and faning out later batches while this task waits for transport quorums.
        let (tx_handlers, mut rx_handlers) = mpsc::channel(1_000);
        let started_at = Instant::now();
        let sequence = Arc::new(AtomicU64::new(0));
        let broadcaster_sequence = sequence.clone();
        tokio::spawn(async move {
            let mut network = ReliableSender::new();
            let mut next_batch_id = 0u64;
            while let Some(batch) = rx_batch.recv().await {
                let batch_id = next_batch_id;
                next_batch_id += 1;
                let digest = Self::digest(&batch);
                info!(
                    "QID_EVENT admitted mode=baseline batch={} digest={:?} size={} policy=disabled active=none authority={:?} peer=none generation=none records_delta=0 bytes_delta=0 at_ms={} seq={}",
                    batch_id,
                    digest,
                    batch.len(),
                    authority,
                    started_at.elapsed().as_millis(),
                    broadcaster_sequence.fetch_add(1, Ordering::Relaxed)
                );
                let (names, addresses): (Vec<_>, Vec<_>) = workers.iter().cloned().unzip();
                let handlers = network
                    .broadcast(addresses, Bytes::from(batch.clone()))
                    .await;
                info!(
                    "QID_EVENT fanout_complete mode=baseline batch={} digest={:?} size={} policy=disabled authority={:?} peer=none generation=none records_delta=0 bytes_delta=0 at_ms={} seq={}",
                    batch_id,
                    digest,
                    batch.len(),
                    authority,
                    started_at.elapsed().as_millis(),
                    broadcaster_sequence.fetch_add(1, Ordering::Relaxed)
                );
                tx_handlers
                    .send(BaselineBatch {
                        batch_id,
                        digest,
                        bytes: batch,
                        handlers: names.into_iter().zip(handlers.into_iter()).collect(),
                    })
                    .await
                    .expect("Baseline quorum waiter terminated");
            }
        });

        while let Some(BaselineBatch {
            batch_id,
            digest,
            bytes,
            handlers,
        }) = rx_handlers.recv().await
        {
            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| Self::valid_ack(handler, committee.stake(&name)))
                .collect();
            let mut total_stake = local_stake;
            while let Some(stake) = wait_for_quorum.next().await {
                if let Some(stake) = stake {
                    total_stake += stake;
                }
                if total_stake >= committee.quorum_threshold() {
                    let size = bytes.len();
                    info!(
                        "QID_EVENT transport_quorum mode=baseline batch={} digest={:?} size={} policy=disabled authority={:?} peer=none generation=none records_delta=0 bytes_delta=0 at_ms={} seq={}",
                        batch_id,
                        digest,
                        size,
                        authority,
                        started_at.elapsed().as_millis(),
                        sequence.fetch_add(1, Ordering::Relaxed)
                    );
                    Self::accept_handoff(&tx_batch, bytes).await;
                    info!(
                        "QID_EVENT handoff_accepted mode=baseline policy=disabled batch={} digest={:?} size={} active=none authority={:?} peer=none generation=none records_delta=0 bytes_delta=0 at_ms={} seq={}",
                        batch_id,
                        digest,
                        size,
                        authority,
                        started_at.elapsed().as_millis(),
                        sequence.fetch_add(1, Ordering::Relaxed)
                    );
                    break;
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_isolated(
        committee: Committee,
        authority: PublicKey,
        local_stake: Stake,
        max_active_batches: usize,
        peer_max_records: Option<usize>,
        peer_max_bytes: Option<usize>,
        reclaim_interval_ms: u64,
        reclaim_policy: QidReclaimPolicy,
        mut rx_batch: Receiver<SerializedBatchMessage>,
        tx_batch: Sender<SerializedBatchMessage>,
        workers: Vec<(PublicKey, SocketAddr)>,
        mode: &'static str,
    ) {
        let (event_sender, mut event_receiver) = mpsc::unbounded_channel();
        let mut peers = HashMap::new();
        let started_at = Instant::now();
        for (peer, address) in workers {
            let (sender, receiver) = mpsc::unbounded_channel();
            let (control, control_receiver) = mpsc::unbounded_channel();
            Self::spawn_peer(
                peer,
                address,
                receiver,
                control_receiver,
                event_sender.clone(),
            );
            peers.insert(
                peer,
                PeerState {
                    sender,
                    control,
                    debt_records: 0,
                    debt_bytes: 0,
                    outstanding: HashMap::new(),
                    generation: 0,
                    closing: false,
                    blocked: false,
                    stall_since: None,
                    last_retired_at: started_at,
                    retire_cycles: 0,
                },
            );
        }
        drop(event_sender);

        let mut active = HashMap::<BatchId, ActiveBatch>::new();
        let mut order = VecDeque::<BatchId>::new();
        let mut next_batch_id = 0u64;
        let mut event_sequence = 0u64;
        let mut input_open = true;
        let mut reclaim_timer =
            tokio::time::interval(Duration::from_millis(reclaim_interval_ms.max(1)));
        reclaim_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            if let (Some(max_records), Some(max_bytes)) = (peer_max_records, peer_max_bytes) {
                if reclaim_interval_ms > 0 {
                    Self::start_retirements(
                        &active,
                        &mut peers,
                        max_records,
                        max_bytes,
                        Duration::from_millis(reclaim_interval_ms),
                        reclaim_policy,
                        authority,
                        started_at,
                        &mut event_sequence,
                        mode,
                    );
                }
                Self::schedule_qid(
                    &mut active,
                    &order,
                    &mut peers,
                    max_records,
                    max_bytes,
                    reclaim_policy,
                    authority,
                    started_at,
                    &mut event_sequence,
                    mode,
                );
            }

            if !input_open && active.is_empty() {
                return;
            }

            tokio::select! {
                _ = reclaim_timer.tick(), if reclaim_interval_ms > 0 => {}
                message = rx_batch.recv(), if input_open && active.len() < max_active_batches => {
                    match message {
                        Some(batch) => {
                            let batch_id = next_batch_id;
                            next_batch_id += 1;
                            let bytes = Bytes::from(batch);
                            let digest = Self::digest(bytes.as_ref());
                            let mut state = ActiveBatch {
                                bytes: bytes.clone(),
                                digest: digest.clone(),
                                acknowledged_stake: local_stake,
                                acknowledged: HashSet::new(),
                                admitted: HashSet::new(),
                            };

                            if peer_max_records.is_none() {
                                for (peer, peer_state) in &mut peers {
                                    state.admitted.insert(*peer);
                                    peer_state.debt_records += 1;
                                    peer_state.debt_bytes += bytes.len();
                                    if peer_state.stall_since.is_none() {
                                        peer_state.stall_since = Some(Instant::now());
                                    }
                                    assert!(
                                        peer_state
                                            .outstanding
                                            .insert(batch_id, bytes.len())
                                            .is_none(),
                                        "Duplicate peer delivery reservation"
                                    );
                                    peer_state.sender.send(PeerDelivery {
                                        batch_id,
                                        digest: digest.clone(),
                                        bytes: bytes.clone(),
                                        generation: peer_state.generation,
                                    }).expect("Peer actor terminated");
                                }
                            }

                            let seq = Self::next_sequence(&mut event_sequence);
                            info!(
                                "QID_EVENT admitted mode={} batch={} digest={:?} size={} active={} policy={} authority={:?} peer=none generation=none records_delta=0 bytes_delta=0 at_ms={} seq={}",
                                mode,
                                batch_id,
                                digest,
                                bytes.len(),
                                active.len() + 1,
                                Self::policy_name(reclaim_policy),
                                authority,
                                started_at.elapsed().as_millis(),
                                seq
                            );
                            active.insert(batch_id, state);
                            order.push_back(batch_id);
                        }
                        None => input_open = false,
                    }
                }
                event = event_receiver.recv() => {
                    match event.expect("All peer actors terminated") {
                        PeerEvent::Receipt { batch_id, digest, peer, bytes, generation } => {
                            let peer_state = peers
                                .get_mut(&peer)
                                .expect("Receipt from unknown peer");
                            if peer_state.closing || generation != peer_state.generation {
                                let seq = Self::next_sequence(&mut event_sequence);
                                warn!(
                                    "QID_EVENT peer_receipt_ignored mode={} batch={} peer={:?} generation={} current_generation={} disposition=fenced_or_stale digest={:?} policy={} authority={:?} size={} records_delta=0 bytes_delta=0 cycle={} at_ms={} seq={}",
                                    mode,
                                    batch_id,
                                    peer,
                                    generation,
                                    peer_state.generation,
                                    digest,
                                    Self::policy_name(reclaim_policy),
                                    authority,
                                    bytes,
                                    peer_state.retire_cycles,
                                    started_at.elapsed().as_millis(),
                                    seq
                                );
                                continue;
                            }
                            let reserved_bytes = peer_state
                                .outstanding
                                .get(&batch_id)
                                .copied()
                                .unwrap_or(bytes);
                            if !Self::release_receipt_debt(
                                peer_state,
                                batch_id,
                                bytes,
                                generation,
                            ) {
                                let seq = Self::next_sequence(&mut event_sequence);
                                warn!(
                                    "QID_EVENT peer_receipt_ignored mode={} batch={} peer={:?} generation={} current_generation={} disposition=duplicate_or_late digest={:?} policy={} authority={:?} size={} records_delta=0 bytes_delta=0 cycle={} at_ms={} seq={}",
                                    mode,
                                    batch_id,
                                    peer,
                                    generation,
                                    peer_state.generation,
                                    digest,
                                    Self::policy_name(reclaim_policy),
                                    authority,
                                    bytes,
                                    peer_state.retire_cycles,
                                    started_at.elapsed().as_millis(),
                                    seq
                                );
                                continue;
                            }
                            peer_state.stall_since = if peer_state.outstanding.is_empty() {
                                None
                            } else {
                                Some(Instant::now())
                            };
                            let mut completed = false;
                            let mut stake_applied = false;
                            if let Some(batch) = active.get_mut(&batch_id) {
                                if batch.acknowledged.insert(peer) {
                                    batch.acknowledged_stake += committee.stake(&peer);
                                    stake_applied = true;
                                }
                                completed = batch.acknowledged_stake >= committee.quorum_threshold();
                            }
                            let seq = Self::next_sequence(&mut event_sequence);
                            info!(
                                "QID_EVENT peer_receipt mode={} batch={} peer={:?} debt_records={} debt_bytes={} generation={} digest={:?} policy={} authority={:?} size={} records_delta=-1 bytes_delta=-{} stake_applied={} cycle={} at_ms={} seq={}",
                                mode,
                                batch_id,
                                peer,
                                peer_state.debt_records,
                                peer_state.debt_bytes,
                                generation,
                                digest,
                                Self::policy_name(reclaim_policy),
                                authority,
                                reserved_bytes,
                                reserved_bytes,
                                stake_applied,
                                peer_state.retire_cycles,
                                started_at.elapsed().as_millis(),
                                seq
                            );

                            if completed {
                                let batch = active.remove(&batch_id).expect("Completed batch missing");
                                order.retain(|id| id != &batch_id);
                                let batch_size = batch.bytes.len();
                                let batch_digest = batch.digest.clone();
                                let seq = Self::next_sequence(&mut event_sequence);
                                info!(
                                    "QID_EVENT transport_quorum mode={} batch={} digest={:?} size={} active={} policy={} authority={:?} peer=none generation=none records_delta=0 bytes_delta=0 at_ms={} seq={}",
                                    mode,
                                    batch_id,
                                    batch_digest,
                                    batch_size,
                                    active.len(),
                                    Self::policy_name(reclaim_policy),
                                    authority,
                                    started_at.elapsed().as_millis(),
                                    seq
                                );
                                Self::accept_handoff(&tx_batch, batch.bytes.to_vec()).await;
                                let seq = Self::next_sequence(&mut event_sequence);
                                info!(
                                    "QID_EVENT handoff_accepted mode={} batch={} digest={:?} size={} active={} policy={} authority={:?} peer=none generation=none records_delta=0 bytes_delta=0 at_ms={} seq={}",
                                    mode,
                                    batch_id,
                                    batch_digest,
                                    batch_size,
                                    active.len(),
                                    Self::policy_name(reclaim_policy),
                                    authority,
                                    started_at.elapsed().as_millis(),
                                    seq
                                );
                            }
                        }
                        PeerEvent::Quarantined { batch_id, digest, peer, bytes, reason, generation } => {
                            let (retained, debt_records, debt_bytes) = peers.get(&peer).map_or(
                                (false, 0, 0),
                                |state| {
                                    (
                                        state.outstanding.contains_key(&batch_id),
                                        state.debt_records,
                                        state.debt_bytes,
                                    )
                                },
                            );
                            let cycle = peers.get(&peer).map_or(0, |state| state.retire_cycles);
                            let seq = Self::next_sequence(&mut event_sequence);
                            warn!(
                                "QID_EVENT peer_quarantined mode={} batch={} peer={:?} delivery_bytes={} retained={} debt_records={} debt_bytes={} reason={:?} generation={} digest={:?} policy={} authority={:?} records_delta=0 bytes_delta=0 cycle={} at_ms={} seq={}",
                                mode,
                                batch_id,
                                peer,
                                bytes,
                                retained,
                                debt_records,
                                debt_bytes,
                                reason,
                                    generation,
                                    digest,
                                Self::policy_name(reclaim_policy),
                                authority,
                                cycle,
                                started_at.elapsed().as_millis(),
                                seq
                            );
                        }
                        PeerEvent::Fatal { peer, reason, generation } => {
                            let current = peers.get(&peer).map(|state| state.generation);
                            if current != Some(generation) {
                                warn!("Ignoring stale peer fatal event peer={:?} generation={}", peer, generation);
                                continue;
                            }
                            error!("Peer dissemination actor {:?} generation {} failed: {}", peer, generation, reason);
                            std::process::abort();
                        }
                        PeerEvent::Retired { peer, generation } => {
                            let state = peers.get(&peer).expect("Retirement from unknown peer");
                            if !state.closing || generation != state.generation {
                                let seq = Self::next_sequence(&mut event_sequence);
                                warn!(
                                    "QID_EVENT peer_retired_ignored mode={} peer={:?} generation={} current_generation={} disposition=stale_or_duplicate policy={} authority={:?} records_delta=0 bytes_delta=0 cycle={} at_ms={} seq={}",
                                    mode,
                                    peer,
                                    generation,
                                    state.generation,
                                    Self::policy_name(reclaim_policy),
                                    authority,
                                    state.retire_cycles,
                                    started_at.elapsed().as_millis(),
                                    seq
                                );
                                continue;
                            }

                            let replay_ids = if reclaim_policy == QidReclaimPolicy::TimeoutReplay {
                                Self::select_replay_batches(&active, &order, state)
                            } else {
                                Vec::new()
                            };
                            let replay_bytes: usize = replay_ids
                                .iter()
                                .map(|batch_id| {
                                    let size = active
                                        .get(batch_id)
                                        .expect("Replay batch missing")
                                        .bytes
                                        .len();
                                    assert_eq!(
                                        Some(&size),
                                        state.outstanding.get(batch_id),
                                        "Replay batch size changed across generations"
                                    );
                                    size
                                })
                                .sum();
                            assert!(replay_ids.len() <= state.debt_records);
                            assert!(replay_bytes <= state.debt_bytes);

                            let (released_records, released_bytes, next_generation) = {
                                let peer_state = peers.get_mut(&peer).expect("Retirement peer missing");
                                Self::complete_retirement(peer_state, generation)
                                    .expect("Matching retirement completion rejected")
                            };
                            let replay_ids_text = Self::batch_ids_text(&replay_ids);
                            let retired_state = peers.get(&peer).expect("Retired peer missing");
                            assert_eq!(retired_state.debt_records, 0);
                            assert_eq!(retired_state.debt_bytes, 0);
                            let retire_cycle = retired_state.retire_cycles;
                            let seq = Self::next_sequence(&mut event_sequence);
                            info!(
                                "QID_EVENT peer_retire_end mode={} peer={:?} generation={} released_records={} released_bytes={} next_generation={} cycle={} expected_replay_count={} expected_replay_bytes={} expected_replay_ids={} debt_records=0 debt_bytes=0 policy={} authority={:?} records_delta=-{} bytes_delta=-{} at_ms={} seq={}",
                                mode,
                                peer,
                                generation,
                                released_records,
                                released_bytes,
                                next_generation,
                                retire_cycle,
                                replay_ids.len(),
                                replay_bytes,
                                replay_ids_text,
                                Self::policy_name(reclaim_policy),
                                authority,
                                released_records,
                                released_bytes,
                                started_at.elapsed().as_millis(),
                                seq
                            );
                            Self::reserve_replays(
                                &mut active,
                                &mut peers,
                                peer,
                                generation,
                                &replay_ids,
                                replay_bytes,
                                reclaim_policy,
                                authority,
                                started_at,
                                &mut event_sequence,
                                mode,
                            );
                            let peer_state = peers.get(&peer).expect("Retired peer missing");
                            let seq = Self::next_sequence(&mut event_sequence);
                            info!(
                                "QID_EVENT replay_phase_end mode={} peer={:?} old_generation={} generation={} cycle={} replay_count={} replay_bytes={} replay_ids={} debt_records={} debt_bytes={} policy={} authority={:?} records_delta=0 bytes_delta=0 at_ms={} seq={}",
                                mode,
                                peer,
                                generation,
                                next_generation,
                                peer_state.retire_cycles,
                                replay_ids.len(),
                                replay_bytes,
                                replay_ids_text,
                                peer_state.debt_records,
                                peer_state.debt_bytes,
                                Self::policy_name(reclaim_policy),
                                authority,
                                started_at.elapsed().as_millis(),
                                seq
                            );
                        }
                    }
                }
            }
        }
    }

    fn start_retirements(
        active: &HashMap<BatchId, ActiveBatch>,
        peers: &mut HashMap<PublicKey, PeerState>,
        max_records: usize,
        max_bytes: usize,
        interval: Duration,
        reclaim_policy: QidReclaimPolicy,
        authority: PublicKey,
        started_at: Instant,
        event_sequence: &mut u64,
        mode: &'static str,
    ) {
        let now = Instant::now();
        for (peer, peer_state) in peers.iter_mut() {
            if peer_state.closing || peer_state.outstanding.is_empty() {
                continue;
            }
            let under_pressure = peer_state.blocked
                || peer_state.debt_records >= max_records
                || peer_state.debt_bytes >= max_bytes;
            if !under_pressure {
                continue;
            }
            if reclaim_policy == QidReclaimPolicy::SafeCut
                && peer_state
                    .outstanding
                    .keys()
                    .any(|batch_id| active.contains_key(batch_id))
            {
                continue;
            }
            let stalled_long_enough = peer_state
                .stall_since
                .map_or(false, |started| now.duration_since(started) >= interval);
            if !stalled_long_enough || now.duration_since(peer_state.last_retired_at) < interval {
                continue;
            }

            // Preserve QSR's short-circuit eligibility check. Full membership is
            // materialized only for a reset that has passed every policy gate.
            let mut active_intersection: Vec<_> =
                if reclaim_policy == QidReclaimPolicy::TimeoutReplay {
                    peer_state
                        .outstanding
                        .keys()
                        .filter(|batch_id| active.contains_key(batch_id))
                        .copied()
                        .collect()
                } else {
                    Vec::new()
                };
            active_intersection.sort_unstable();

            peer_state.closing = true;
            peer_state.retire_cycles = peer_state
                .retire_cycles
                .checked_add(1)
                .expect("Peer retirement cycle overflow");
            let active_intersection_bytes: usize = active_intersection
                .iter()
                .map(|batch_id| {
                    *peer_state
                        .outstanding
                        .get(batch_id)
                        .expect("Active intersection missing peer debt")
                })
                .sum();
            let active_intersection_ids = Self::batch_ids_text(&active_intersection);
            let seq = Self::next_sequence(event_sequence);
            info!(
                "QID_EVENT peer_retire_begin mode={} peer={:?} generation={} debt_records={} debt_bytes={} cycle={} active_outstanding={} active_intersection_count={} active_intersection_bytes={} active_intersection_ids={} policy={} authority={:?} records_delta=0 bytes_delta=0 at_ms={} seq={}",
                mode,
                peer,
                peer_state.generation,
                peer_state.debt_records,
                peer_state.debt_bytes,
                peer_state.retire_cycles,
                active_intersection.len(),
                active_intersection.len(),
                active_intersection_bytes,
                active_intersection_ids,
                Self::policy_name(reclaim_policy),
                authority,
                started_at.elapsed().as_millis(),
                seq
            );
            peer_state
                .control
                .send(PeerControl::Retire {
                    generation: peer_state.generation,
                })
                .expect("Peer actor terminated before retirement");
        }
    }

    fn schedule_qid(
        active: &mut HashMap<BatchId, ActiveBatch>,
        order: &VecDeque<BatchId>,
        peers: &mut HashMap<PublicKey, PeerState>,
        max_records: usize,
        max_bytes: usize,
        reclaim_policy: QidReclaimPolicy,
        authority: PublicKey,
        started_at: Instant,
        event_sequence: &mut u64,
        mode: &'static str,
    ) {
        for (peer, peer_state) in peers.iter_mut() {
            if peer_state.closing {
                continue;
            }
            peer_state.blocked = false;
            loop {
                let Some(batch_id) = order
                    .iter()
                    .find(|batch_id| {
                        active
                            .get(batch_id)
                            .map_or(false, |batch| !batch.admitted.contains(peer))
                    })
                    .cloned()
                else {
                    break;
                };
                let batch = active.get_mut(&batch_id).expect("Active batch missing");
                let size = batch.bytes.len();

                // Strict oldest-unsent reservation: never bypass an older large batch
                // with a younger small batch while waiting for byte credit.
                if peer_state.debt_records >= max_records
                    || peer_state.debt_bytes.saturating_add(size) > max_bytes
                {
                    peer_state.blocked = true;
                    break;
                }

                batch.admitted.insert(*peer);
                if peer_state.outstanding.is_empty() {
                    peer_state.stall_since = Some(Instant::now());
                }
                peer_state.debt_records += 1;
                peer_state.debt_bytes += size;
                assert!(
                    peer_state.outstanding.insert(batch_id, size).is_none(),
                    "Duplicate peer delivery reservation"
                );
                peer_state
                    .sender
                    .send(PeerDelivery {
                        batch_id,
                        digest: batch.digest.clone(),
                        bytes: batch.bytes.clone(),
                        generation: peer_state.generation,
                    })
                    .expect("Peer actor terminated");
                let seq = Self::next_sequence(event_sequence);
                info!(
                    "QID_EVENT peer_admitted mode={} batch={} peer={:?} debt_records={} debt_bytes={} generation={} digest={:?} size={} reason=ordinary old_generation=none replay_count=0 replay_bytes=0 policy={} authority={:?} records_delta=1 bytes_delta={} cycle={} at_ms={} seq={}",
                    mode,
                    batch_id,
                    peer,
                    peer_state.debt_records,
                    peer_state.debt_bytes,
                    peer_state.generation,
                    batch.digest,
                    size,
                    Self::policy_name(reclaim_policy),
                    authority,
                    size,
                    peer_state.retire_cycles,
                    started_at.elapsed().as_millis(),
                    seq
                );
            }
        }
    }

    fn batch_ids_text(batch_ids: &[BatchId]) -> String {
        if batch_ids.is_empty() {
            return "-".to_string();
        }
        batch_ids
            .iter()
            .map(BatchId::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    fn select_replay_batches(
        active: &HashMap<BatchId, ActiveBatch>,
        order: &VecDeque<BatchId>,
        peer_state: &PeerState,
    ) -> Vec<BatchId> {
        let selected: Vec<_> = order
            .iter()
            .filter(|batch_id| {
                active.contains_key(batch_id) && peer_state.outstanding.contains_key(batch_id)
            })
            .copied()
            .collect();
        let expected = peer_state
            .outstanding
            .keys()
            .filter(|batch_id| active.contains_key(batch_id))
            .count();
        assert_eq!(
            selected.len(),
            expected,
            "Active replay candidate missing from global admission order"
        );
        selected
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_replays(
        active: &mut HashMap<BatchId, ActiveBatch>,
        peers: &mut HashMap<PublicKey, PeerState>,
        peer: PublicKey,
        old_generation: u64,
        replay_ids: &[BatchId],
        replay_bytes: usize,
        reclaim_policy: QidReclaimPolicy,
        authority: PublicKey,
        started_at: Instant,
        event_sequence: &mut u64,
        mode: &'static str,
    ) {
        struct PreparedReplay {
            batch_id: BatchId,
            digest: Digest,
            size: usize,
            bytes: Bytes,
            generation: u64,
            debt_records: usize,
            debt_bytes: usize,
            cycle: u64,
        }

        if replay_ids.is_empty() {
            return;
        }
        assert_eq!(reclaim_policy, QidReclaimPolicy::TimeoutReplay);

        let mut prepared = Vec::with_capacity(replay_ids.len());
        let sender = {
            let peer_state = peers.get_mut(&peer).expect("Replay peer missing");
            assert!(!peer_state.closing, "Replay peer must be reopened");
            assert!(peer_state.outstanding.is_empty());
            let sender = peer_state.sender.clone();
            for batch_id in replay_ids {
                let batch = active.get_mut(batch_id).expect("Replay batch missing");
                assert!(
                    batch.admitted.contains(&peer),
                    "Replay must preserve historical admission identity"
                );
                assert!(
                    !batch.acknowledged.contains(&peer),
                    "Acknowledged branch cannot remain replay debt"
                );
                let size = batch.bytes.len();
                if peer_state.outstanding.is_empty() {
                    peer_state.stall_since = Some(Instant::now());
                }
                peer_state.debt_records += 1;
                peer_state.debt_bytes += size;
                assert!(
                    peer_state.outstanding.insert(*batch_id, size).is_none(),
                    "Duplicate replay reservation"
                );
                prepared.push(PreparedReplay {
                    batch_id: *batch_id,
                    digest: batch.digest.clone(),
                    size,
                    bytes: batch.bytes.clone(),
                    generation: peer_state.generation,
                    debt_records: peer_state.debt_records,
                    debt_bytes: peer_state.debt_bytes,
                    cycle: peer_state.retire_cycles,
                });
            }
            assert_eq!(peer_state.debt_records, replay_ids.len());
            assert_eq!(peer_state.debt_bytes, replay_bytes);
            sender
        };

        for replay in prepared {
            if sender
                .send(PeerDelivery {
                    batch_id: replay.batch_id,
                    digest: replay.digest.clone(),
                    bytes: replay.bytes,
                    generation: replay.generation,
                })
                .is_err()
            {
                let seq = Self::next_sequence(event_sequence);
                error!(
                    "QID_EVENT peer_replay_enqueue_failed mode={} batch={} digest={:?} size={} peer={:?} generation={} old_generation={} policy={} authority={:?} records_delta=0 bytes_delta=0 cycle={} at_ms={} seq={}",
                    mode,
                    replay.batch_id,
                    replay.digest,
                    replay.size,
                    peer,
                    replay.generation,
                    old_generation,
                    Self::policy_name(reclaim_policy),
                    authority,
                    replay.cycle,
                    started_at.elapsed().as_millis(),
                    seq
                );
                std::process::abort();
            }
            let seq = Self::next_sequence(event_sequence);
            info!(
                "QID_EVENT peer_admitted mode={} batch={} peer={:?} debt_records={} debt_bytes={} generation={} digest={:?} size={} reason=replay old_generation={} replay_count={} replay_bytes={} policy={} authority={:?} records_delta=1 bytes_delta={} cycle={} at_ms={} seq={}",
                mode,
                replay.batch_id,
                peer,
                replay.debt_records,
                replay.debt_bytes,
                replay.generation,
                replay.digest,
                replay.size,
                old_generation,
                replay_ids.len(),
                replay_bytes,
                Self::policy_name(reclaim_policy),
                authority,
                replay.size,
                replay.cycle,
                started_at.elapsed().as_millis(),
                seq
            );
        }
    }

    fn release_peer_debt(
        peer_state: &mut PeerState,
        batch_id: BatchId,
        reported_bytes: usize,
    ) -> bool {
        let Some(reserved_bytes) = peer_state.outstanding.remove(&batch_id) else {
            return false;
        };
        if reserved_bytes != reported_bytes {
            warn!(
                "Receipt size mismatch for batch {}: reserved {} B, reported {} B",
                batch_id, reserved_bytes, reported_bytes
            );
        }
        peer_state.debt_records = peer_state
            .debt_records
            .checked_sub(1)
            .expect("Peer record debt underflow");
        peer_state.debt_bytes = peer_state
            .debt_bytes
            .checked_sub(reserved_bytes)
            .expect("Peer byte debt underflow");
        true
    }

    fn release_receipt_debt(
        peer_state: &mut PeerState,
        batch_id: BatchId,
        reported_bytes: usize,
        generation: u64,
    ) -> bool {
        if peer_state.closing || generation != peer_state.generation {
            return false;
        }
        Self::release_peer_debt(peer_state, batch_id, reported_bytes)
    }

    fn complete_retirement(
        peer_state: &mut PeerState,
        generation: u64,
    ) -> Option<(usize, usize, u64)> {
        if !peer_state.closing || generation != peer_state.generation {
            return None;
        }

        // Check the successor before mutating any charged state. Overflow leaves
        // the peer closed with its full debt intact.
        let next_generation = peer_state
            .generation
            .checked_add(1)
            .expect("Peer generation overflow");
        let released_records = peer_state.outstanding.len();
        let released_bytes: usize = peer_state.outstanding.values().sum();
        assert_eq!(
            released_records, peer_state.debt_records,
            "Retired record debt mismatch"
        );
        assert_eq!(
            released_bytes, peer_state.debt_bytes,
            "Retired byte debt mismatch"
        );
        peer_state.outstanding.clear();
        peer_state.debt_records = 0;
        peer_state.debt_bytes = 0;
        peer_state.closing = false;
        peer_state.blocked = false;
        peer_state.stall_since = None;
        peer_state.last_retired_at = Instant::now();
        peer_state.generation = next_generation;
        Some((released_records, released_bytes, next_generation))
    }

    fn spawn_peer(
        peer: PublicKey,
        address: SocketAddr,
        receiver: UnboundedReceiver<PeerDelivery>,
        control_receiver: UnboundedReceiver<PeerControl>,
        event_sender: UnboundedSender<PeerEvent>,
    ) {
        tokio::spawn(async move {
            Self::run_peer(peer, address, receiver, control_receiver, event_sender).await;
        });
    }

    async fn run_peer(
        peer: PublicKey,
        address: SocketAddr,
        mut receiver: UnboundedReceiver<PeerDelivery>,
        mut control_receiver: UnboundedReceiver<PeerControl>,
        event_sender: UnboundedSender<PeerEvent>,
    ) {
        let mut network = ReliableSender::new();
        let mut pending: FuturesUnordered<
            BoxFuture<
                'static,
                (
                    PeerDelivery,
                    Result<Bytes, tokio::sync::oneshot::error::RecvError>,
                ),
            >,
        > = FuturesUnordered::new();
        let mut outbound = VecDeque::<PeerDelivery>::new();
        let mut quarantined = Vec::<PeerDelivery>::new();
        let mut input_open = true;
        let mut control_open = true;
        let mut generation = 0u64;

        loop {
            if !input_open && !control_open && outbound.is_empty() && pending.is_empty() {
                return;
            }

            let step = if let Some(delivery) = outbound.front() {
                let bytes = delivery.bytes.clone();
                tokio::select! {
                    biased;
                    command = control_receiver.recv(), if control_open => PeerActorStep::Control(command),
                    result = pending.next(), if !pending.is_empty() => PeerActorStep::Receipt(result),
                    handler = network.send(address, bytes) => PeerActorStep::Sent(handler),
                    delivery = receiver.recv(), if input_open => PeerActorStep::Delivery(delivery),
                }
            } else {
                tokio::select! {
                    biased;
                    command = control_receiver.recv(), if control_open => PeerActorStep::Control(command),
                    result = pending.next(), if !pending.is_empty() => PeerActorStep::Receipt(result),
                    delivery = receiver.recv(), if input_open => PeerActorStep::Delivery(delivery),
                }
            };

            match step {
                PeerActorStep::Control(Some(PeerControl::Retire {
                    generation: retired,
                })) => {
                    if retired != generation {
                        event_sender
                            .send(PeerEvent::Fatal {
                                peer,
                                generation,
                                reason: format!(
                                    "retirement generation mismatch: command {}, actor {}",
                                    retired, generation
                                ),
                            })
                            .ok();
                        return;
                    }

                    let next_generation = match generation.checked_add(1) {
                        Some(next_generation) => next_generation,
                        None => {
                            event_sender
                                .send(PeerEvent::Fatal {
                                    peer,
                                    generation,
                                    reason: "peer actor generation overflow".to_string(),
                                })
                                .ok();
                            return;
                        }
                    };

                    // This awaited operation proves destruction of the nested
                    // connection task and all transport-owned old-generation state.
                    network.retire(address).await;
                    pending = FuturesUnordered::new();
                    outbound.clear();
                    quarantined.clear();
                    while let Ok(delivery) = receiver.try_recv() {
                        if delivery.generation != retired {
                            event_sender
                                .send(PeerEvent::Fatal {
                                    peer,
                                    generation,
                                    reason: format!(
                                        "future generation {} queued during retirement {}",
                                        delivery.generation, retired
                                    ),
                                })
                                .ok();
                            return;
                        }
                    }
                    generation = next_generation;
                    event_sender
                        .send(PeerEvent::Retired {
                            peer,
                            generation: retired,
                        })
                        .expect("Dissemination manager terminated");
                }
                PeerActorStep::Control(None) => control_open = false,
                PeerActorStep::Delivery(Some(delivery)) => {
                    if delivery.generation != generation {
                        event_sender
                            .send(PeerEvent::Fatal {
                                peer,
                                generation,
                                reason: format!(
                                    "delivery generation mismatch: delivery {}, actor {}",
                                    delivery.generation, generation
                                ),
                            })
                            .ok();
                        return;
                    }
                    outbound.push_back(delivery);
                }
                PeerActorStep::Delivery(None) => input_open = false,
                PeerActorStep::Sent(handler) => {
                    let delivery = outbound.pop_front().expect("Sent delivery missing");
                    pending.push(Box::pin(async move {
                        let result = handler.await;
                        (delivery, result)
                    }));
                }
                PeerActorStep::Receipt(Some((delivery, result))) => {
                    match result {
                        Ok(bytes) if bytes.as_ref() == ACK => {
                            let batch_id = delivery.batch_id;
                            let digest = delivery.digest.clone();
                            let generation = delivery.generation;
                            let delivery_bytes = delivery.bytes.len();
                            // Destroy the actor's payload owner before the manager can
                            // observe the receipt and release the corresponding debt.
                            drop(delivery);
                            event_sender
                                .send(PeerEvent::Receipt {
                                    batch_id,
                                    digest,
                                    peer,
                                    bytes: delivery_bytes,
                                    generation,
                                })
                                .expect("Dissemination manager terminated");
                        }
                        Ok(bytes) => {
                            event_sender
                                .send(PeerEvent::Quarantined {
                                    batch_id: delivery.batch_id,
                                    digest: delivery.digest.clone(),
                                    peer,
                                    bytes: delivery.bytes.len(),
                                    reason: Self::invalid_ack_reason(&bytes),
                                    generation: delivery.generation,
                                })
                                .ok();
                            // Preserve debt ownership and stop this peer from gaining
                            // the corresponding credit. Other peers remain independent.
                            quarantined.push(delivery);
                        }
                        Err(failure) => {
                            // ReliableSender already owns reconnection and replay. An error here
                            // means that ownership was lost, so abort instead of creating an
                            // unaccounted replacement sender.
                            event_sender
                                .send(PeerEvent::Fatal {
                                    peer,
                                    generation: delivery.generation,
                                    reason: format!("receipt channel failed: {}", failure),
                                })
                                .ok();
                            return;
                        }
                    }
                }
                PeerActorStep::Receipt(None) => {
                    event_sender
                        .send(PeerEvent::Fatal {
                            peer,
                            generation,
                            reason: "pending receipt stream ended unexpectedly".to_string(),
                        })
                        .ok();
                    return;
                }
            }
        }
    }
}
