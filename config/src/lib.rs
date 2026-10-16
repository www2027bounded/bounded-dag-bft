// Copyright(C) Facebook, Inc. and its affiliates.
use crypto::{generate_production_keypair, PublicKey, SecretKey};
use log::info;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(PublicKey),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    ExportError { file: String, message: String },
}

pub trait Import: DeserializeOwned {
    fn import(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ImportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub trait Export: Serialize {
    fn export(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::ExportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub type Stake = u32;
pub type WorkerId = u32;

#[derive(Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DisseminationMode {
    Baseline,
    Concurrent,
    Qid,
}

#[derive(Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum QidReclaimPolicy {
    SafeCut,
    TimeoutReplay,
}

impl Default for QidReclaimPolicy {
    fn default() -> Self {
        Self::SafeCut
    }
}

impl Default for DisseminationMode {
    fn default() -> Self {
        Self::Baseline
    }
}

#[derive(Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerAckMode {
    Normal,
    Withhold,
}

impl Default for WorkerAckMode {
    fn default() -> Self {
        Self::Normal
    }
}

fn default_max_batch_bytes() -> usize {
    4 * 1024 * 1024
}

fn default_qid_max_active_batches() -> usize {
    32
}

fn default_qid_peer_max_records() -> usize {
    8
}

fn default_qid_peer_max_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_qid_reclaim_interval_ms() -> u64 {
    0
}

#[derive(Deserialize, Clone)]
pub struct Parameters {
    /// The preferred header size. The primary creates a new header when it has enough parents and
    /// enough batches' digests to reach `header_size`. Denominated in bytes.
    pub header_size: usize,
    /// The maximum delay that the primary waits between generating two headers, even if the header
    /// did not reach `max_header_size`. Denominated in ms.
    pub max_header_delay: u64,
    /// The depth of the garbage collection (Denominated in number of rounds).
    pub gc_depth: u64,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    pub sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    pub sync_retry_nodes: usize,
    /// The preferred batch size. The workers seal a batch of transactions when it reaches this size.
    /// Denominated in bytes.
    pub batch_size: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached. Denominated in ms.
    pub max_batch_delay: u64,
    /// Selects the worker payload dissemination implementation.
    #[serde(default)]
    pub dissemination_mode: DisseminationMode,
    /// Hard upper bound for one serialized worker batch.
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
    /// Maximum number of batches awaiting a transport quorum.
    #[serde(default = "default_qid_max_active_batches")]
    pub qid_max_active_batches: usize,
    /// Maximum number of admitted, unacknowledged deliveries per peer.
    #[serde(default = "default_qid_peer_max_records")]
    pub qid_peer_max_records: usize,
    /// Maximum serialized payload debt per peer.
    #[serde(default = "default_qid_peer_max_bytes")]
    pub qid_peer_max_bytes: usize,
    /// Minimum receipt-stall and reset interval for safe QID connection-generation reclamation.
    /// Zero preserves the bounded QID control without reclamation.
    #[serde(default = "default_qid_reclaim_interval_ms")]
    pub qid_reclaim_interval_ms: u64,
    /// Selects the safe generation-reclamation policy. The default preserves QSR.
    #[serde(default)]
    pub qid_reclaim_policy: QidReclaimPolicy,
    /// Test-only transport behavior for worker-to-worker acknowledgements.
    #[serde(default)]
    pub worker_ack_mode: WorkerAckMode,
    /// Optional one-shot transition from Withhold to Normal without restarting the worker.
    /// Omission preserves the configured ACK mode for the full process lifetime.
    #[serde(default)]
    pub worker_ack_recover_after_ms: Option<u64>,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            header_size: 1_000,
            max_header_delay: 100,
            gc_depth: 50,
            sync_retry_delay: 5_000,
            sync_retry_nodes: 3,
            batch_size: 500_000,
            max_batch_delay: 100,
            dissemination_mode: DisseminationMode::Baseline,
            max_batch_bytes: default_max_batch_bytes(),
            qid_max_active_batches: default_qid_max_active_batches(),
            qid_peer_max_records: default_qid_peer_max_records(),
            qid_peer_max_bytes: default_qid_peer_max_bytes(),
            qid_reclaim_interval_ms: default_qid_reclaim_interval_ms(),
            qid_reclaim_policy: QidReclaimPolicy::SafeCut,
            worker_ack_mode: WorkerAckMode::Normal,
            worker_ack_recover_after_ms: None,
        }
    }
}

impl Import for Parameters {}

impl Parameters {
    pub fn log(&self) {
        info!("Header size set to {} B", self.header_size);
        info!("Max header delay set to {} ms", self.max_header_delay);
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay);
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!("Max batch delay set to {} ms", self.max_batch_delay);
        info!("Dissemination mode set to {:?}", self.dissemination_mode);
        info!(
            "Max serialized batch size set to {} B",
            self.max_batch_bytes
        );
        info!(
            "QID active batch window set to {}",
            self.qid_max_active_batches
        );
        info!(
            "QID peer delivery record limit set to {}",
            self.qid_peer_max_records
        );
        info!(
            "QID peer delivery byte limit set to {} B",
            self.qid_peer_max_bytes
        );
        info!(
            "QID safe generation reclaim interval set to {} ms",
            self.qid_reclaim_interval_ms
        );
        info!(
            "QID generation reclaim policy set to {:?}",
            self.qid_reclaim_policy
        );
        info!("Worker ACK mode set to {:?}", self.worker_ack_mode);
        info!(
            "Worker ACK recovery delay set to {:?} ms",
            self.worker_ack_recover_after_ms
        );
    }
}

#[derive(Clone, Deserialize)]
pub struct PrimaryAddresses {
    /// Address to receive messages from other primaries.
    pub primary_to_primary: SocketAddr,
    /// Address to receive messages from our workers.
    pub worker_to_primary: SocketAddr,
}

#[derive(Clone, Deserialize, Eq, Hash, PartialEq)]
pub struct WorkerAddresses {
    /// Address to receive client transactions.
    pub transactions: SocketAddr,
    /// Address to receive messages from other workers.
    pub worker_to_worker: SocketAddr,
    /// Address to receive messages from our primary.
    pub primary_to_worker: SocketAddr,
}

#[derive(Clone, Deserialize)]
pub struct Authority {
    /// The voting power of this authority.
    pub stake: Stake,
    /// The network addresses of the primary.
    pub primary: PrimaryAddresses,
    /// Map of workers' id and their network addresses.
    pub workers: HashMap<WorkerId, WorkerAddresses>,
}

#[derive(Clone, Deserialize)]
pub struct Committee {
    pub authorities: BTreeMap<PublicKey, Authority>,
}

impl Import for Committee {}

impl Committee {
    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(&name).map_or_else(|| 0, |x| x.stake)
    }

    /// Returns the stake of all authorities except `myself`.
    pub fn others_stake(&self, myself: &PublicKey) -> Vec<(PublicKey, Stake)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.stake))
            .collect()
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        (total_votes + 2) / 3
    }

    /// Returns the primary addresses of the target primary.
    pub fn primary(&self, to: &PublicKey) -> Result<PrimaryAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.primary.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all primaries except `myself`.
    pub fn others_primaries(&self, myself: &PublicKey) -> Vec<(PublicKey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.primary.clone()))
            .collect()
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(&self, to: &PublicKey, id: &WorkerId) -> Result<WorkerAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &to)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*to))?
            .workers
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &PublicKey) -> Result<Vec<WorkerAddresses>, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &myself)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*myself))?
            .workers
            .values()
            .cloned()
            .map(Ok)
            .collect()
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers(
        &self,
        myself: &PublicKey,
        id: &WorkerId,
    ) -> Vec<(PublicKey, WorkerAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .filter_map(|(name, authority)| {
                authority
                    .workers
                    .iter()
                    .find(|(worker_id, _)| worker_id == &id)
                    .map(|(_, addresses)| (*name, addresses.clone()))
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize)]
pub struct KeyPair {
    /// The node's public key (and identifier).
    pub name: PublicKey,
    /// The node's secret key.
    pub secret: SecretKey,
}

impl Import for KeyPair {}
impl Export for KeyPair {}

impl KeyPair {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}

impl Default for KeyPair {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_parameters_disable_qid_reclamation_by_default() {
        let parameters: Parameters = serde_json::from_str(
            r#"{
                "header_size": 1000,
                "max_header_delay": 100,
                "gc_depth": 50,
                "sync_retry_delay": 5000,
                "sync_retry_nodes": 3,
                "batch_size": 500000,
                "max_batch_delay": 100
            }"#,
        )
        .unwrap();

        assert_eq!(parameters.qid_reclaim_interval_ms, 0);
        assert_eq!(parameters.qid_reclaim_policy, QidReclaimPolicy::SafeCut);
        assert_eq!(Parameters::default().qid_reclaim_interval_ms, 0);
        assert_eq!(
            Parameters::default().qid_reclaim_policy,
            QidReclaimPolicy::SafeCut
        );
        assert_eq!(parameters.worker_ack_mode, WorkerAckMode::Normal);
        assert_eq!(parameters.worker_ack_recover_after_ms, None);
        assert_eq!(Parameters::default().worker_ack_recover_after_ms, None);
    }

}
