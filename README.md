This repo provides an implementation of quorum-isolated dissemination (QID) and quorum-safe reclamation
(QSR) and timeout replay (TR) on the `master` branch of the
[Narwhal and Tusk prototype](https://github.com/asonnino/narwhal), based on
commit [`f5145b7219f62b4607be65822d4e3c13147ce778`](https://github.com/asonnino/narwhal/commit/f5145b7219f62b4607be65822d4e3c13147ce778).
QID bounds each peer's outstanding deliveries. QSR reclaims a completed
generation of delivery state while preserving FIFO reply matching.

This repository accompanies our WWW 2027 submission:

> **Bounded Dissemination and Safe Reclamation for DAG-Based BFT Systems**

- **Submission number:** 2700

## Running QID

### Build requirements

Use Linux with Rust and Cargo **1.85.1**, Clang/libclang, a C/C++ build
toolchain, CMake, pkg-config, and OpenSSL development headers. RocksDB is
built as part of the Rust dependencies. Python **3.10 or later** is used
by the small deployment and measurement utilities in `benchmark/`. These
utilities use only the standard library. The paper used Ubuntu 22.04.3
and Linux 6.8.0-110.

On Ubuntu, install the native dependencies and build from this directory:

```sh
sudo apt-get update
sudo apt-get install build-essential cmake pkg-config libssl-dev clang libclang-dev python3 iproute2 iputils-ping
rustup toolchain install 1.85.1 --profile minimal
cargo +1.85.1 build --release --locked --features benchmark -p node --bin node --bin benchmark_client
```

The `benchmark` feature enables batch and commit logs used for measurement.
The first build also compiles RocksDB and can take several minutes.
`rust-toolchain.toml` pins the toolchain, and `Cargo.lock` pins dependencies.
The protocol regression tests can be run with:

```sh
cargo +1.85.1 test --workspace --locked -- --test-threads=1
```

### Quick Start: four authorities on localhost

Each authority runs a primary and a worker, with separate RocksDB stores.
Generate keys, a committee, and QSR parameters:

```sh
python3 benchmark/configure.py --authorities 4 --policy qsr --output .local
mkdir -p .local/logs .local/db
export TOKIO_WORKER_THREADS=2
for i in 0 1 2 3; do
  target/release/node -vv run --keys .local/keys/authority-$i.json \
    --committee .local/committee.json --parameters .local/parameters-$i.json \
    --store .local/db/primary-$i primary >.local/logs/primary-$i.log 2>&1 &
  echo $! >.local/primary-$i.pid
  target/release/node -vv run --keys .local/keys/authority-$i.json \
    --committee .local/committee.json --parameters .local/parameters-$i.json \
    --store .local/db/worker-$i worker --id 0 >.local/logs/worker-$i.log 2>&1 &
  echo $! >.local/worker-$i.pid
done
```

Send 512-byte transactions at an aggregate requested rate of 20,000 tx/s:

```sh
for i in 0 1 2 3; do
  target/release/benchmark_client 127.0.0.1:$((3004 + 10*i)) --size 512 --rate 5000 \
    --nodes 127.0.0.1:3004 127.0.0.1:3014 127.0.0.1:3024 127.0.0.1:3034 \
    >.local/logs/client-$i.log 2>&1 &
  echo $! >.local/client-$i.pid
done
```

Clients wait until all listed transaction endpoints are reachable. Primary
logs contain `Committed` entries. Worker logs contain `QID_EVENT`
admissions, receipts, quorum decisions, and handoffs. Stop the clients,
then the workers and primaries using the PIDs recorded above:

```sh
for i in 0 1 2 3; do kill "$(cat .local/client-$i.pid)"; done
for i in 0 1 2 3; do
  kill "$(cat .local/worker-$i.pid)" "$(cat .local/primary-$i.pid)"
done
wait
```

Use a new deployment and database directory for a fresh run. To keep the
committee while changing policy, pass `--key-dir .local/keys` and a new
`--output` directory to `configure.py`. The policy choices are `native`,
`concurrent`, `qid`, `qsr`, and `tr`.

### Distributed deployment: four hosts

The same binaries run across hosts. For example, assign authorities 0--3
to `10.0.0.11`, `10.0.0.12`, `10.0.0.13`, and `10.0.0.14`. These must be
addresses reachable between the hosts. Build the binaries on each host
or copy a build compatible with all four machines. Generate configuration
once on the coordinating machine:

```sh
python3 benchmark/configure.py --authorities 4 --policy qsr --output .cluster \
  --hosts 10.0.0.11 10.0.0.12 10.0.0.13 10.0.0.14
```

The five TCP endpoints for authority `i` use ports `3001 + 10*i` through
`3005 + 10*i`, in this order:

| Offset | Endpoint |
| --- | --- |
| 0 | Primary to primary |
| 1 | Worker to primary |
| 2 | Primary to worker |
| 3 | Client transactions |
| 4 | Worker to worker |

`deployment.tsv` lists the complete addresses. Allow these ports between
the participating hosts, and allow clients to reach the transaction ports.
Copy the common committee, that host's parameters, and only its own key
to each host. For example, to prepare authority 0:

```sh
ssh user@10.0.0.11 'mkdir -p ~/qid/.cluster/keys ~/qid/.cluster/logs ~/qid/.cluster/db'
scp .cluster/committee.json .cluster/parameters-0.json user@10.0.0.11:~/qid/.cluster/
scp .cluster/keys/authority-0.json user@10.0.0.11:~/qid/.cluster/keys/
```

Repeat for authorities 1--3 using their corresponding files and addresses.
On **each host**, from the repository root, set `i` to its authority index
and start the two processes:

```sh
i=0                         # use 1, 2, or 3 on the other hosts
export TOKIO_WORKER_THREADS=2
target/release/node -vv run --keys .cluster/keys/authority-$i.json \
  --committee .cluster/committee.json --parameters .cluster/parameters-$i.json \
  --store .cluster/db/primary-$i primary >.cluster/logs/primary-$i.log 2>&1 &
echo $! >.cluster/primary-$i.pid
target/release/node -vv run --keys .cluster/keys/authority-$i.json \
  --committee .cluster/committee.json --parameters .cluster/parameters-$i.json \
  --store .cluster/db/worker-$i worker --id 0 >.cluster/logs/worker-$i.log 2>&1 &
echo $! >.cluster/worker-$i.pid
```

Run one client per authority. For authority 0, the command is:

```sh
target/release/benchmark_client 10.0.0.11:3004 --size 512 --rate 5000 \
  --nodes 10.0.0.11:3004 10.0.0.12:3014 10.0.0.13:3024 10.0.0.14:3034 \
  >.cluster/logs/client-0.log 2>&1 &
echo $! >.cluster/client-0.pid
```

Use each authority's transaction address for its client. Keep clocks
synchronized when comparing timestamps across hosts. To stop a deployment,
terminate the recorded client, worker, and primary PIDs on each host.

### Fault deployment options

Choose the fault configuration before launching a fresh deployment. The
examples below use authority 3 in the four-host deployment above. Keep all
four authorities in the committee. For localhost, use `127.0.0.1` for all
hosts and replace `.cluster` with `.local` throughout these examples.

**NoRead.** Start the primaries and workers only on authorities 0--2.
On authority 3's host, leave its primary and worker stopped and replace
its worker-to-worker endpoint with:

```sh
python3 benchmark/no_read.py --host 10.0.0.14 --port 3035 \
  --receive-buffer-bytes 4096 >.cluster/logs/noread.log 2>&1 &
echo $! >.cluster/noread.pid
```

The helper accepts connections without consuming application frames or
sending ACKs.
Stop the helper with `kill "$(cat .cluster/noread.pid)"` when stopping
the deployment.

**WithholdACK.** Add `--withhold 3` to the configuration command above.
Start all four real primaries and workers, leaving the NoRead helper
stopped. The selected worker continues processing incoming frames but
suppresses ACKs. Pass multiple indices after `--withhold` to select
multiple workers in a larger committee.

For temporary withholding, also add `--recover-after-ms 40000` to the same
`configure.py` command. The selected workers resume ACKs after 40 seconds,
counted from each worker's startup. Omitting this option keeps ACKs
withheld. Recovery applies to newly processed frames. It does not send
ACKs retroactively for frames already consumed during withholding.

**Clients.** With one NoRead authority, run `n-1` clients. With `b`
withholding workers, run `n-b` clients, one per remaining worker. Set each
client's `--nodes` list to those remaining workers' transaction endpoints.
This list is a startup reachability barrier and does not change committee
membership. In either four-host example with authority 3 faulty, run
clients only for authorities 0--2. Authority 0's client becomes:

```sh
target/release/benchmark_client 10.0.0.11:3004 --size 512 --rate 5000 \
  --nodes 10.0.0.11:3004 10.0.0.12:3014 10.0.0.13:3024 \
  >.cluster/logs/client-0.log 2>&1 &
echo $! >.cluster/client-0.pid
```

Use the corresponding target, log, and PID paths for clients 1 and 2.
`--rate` is per client. Choose it for the desired aggregate load in
multiples of 20 tx/s. Stop only the processes actually started in that
deployment.

### Memory-backed databases with tmpfs

After generating configuration and creating directories, but before
starting any nodes, mount the empty database directory on each host:

```sh
DB_DIR=.cluster/db             # use .local/db for the localhost deployment
mkdir -p "$DB_DIR"
sudo mount -t tmpfs -o "size=4G,uid=$(id -u),gid=$(id -g)" tmpfs "$DB_DIR"
df -h "$DB_DIR"
```

The existing `--store` paths then put both primary and worker databases
on tmpfs, while logs remain outside the mount. Adjust the example 4 GiB
capacity to available RAM and monitor free space with `df -h "$DB_DIR"`.
After stopping all processes using these stores, unmount with:

```sh
sudo umount "$DB_DIR"
```

Unmounting discards the in-memory database contents.

## Implementation

The repository contains the Rust workspace directly at its root:

| File or directory | Role and principal changes |
| --- | --- |
| `worker/src/quorum_waiter.rs` | QID's active window, independent peer scheduling, record/byte reservations, receipt accounting, QSR completion guard, and TR comparison. It also contains the shared pipelined Native path. |
| `network/src/reliable_sender.rs` | Reliable delivery and FIFO replies, with explicit termination of an old connection generation before its resources are released. |
| `worker/src/batch_maker.rs` | Enforces the serialized batch-size cap before admitting a batch to dissemination. |
| `worker/src/worker.rs` | Wires policy parameters into the worker. `WorkerAckPolicy` supplies ACK withholding and timed recovery for fault experiments. |
| `config/src/lib.rs` | Committee, endpoint, and policy configuration. |
| `node/src/main.rs` | Key generation and primary/worker process entry points. |
| `node/src/benchmark_client.rs` | Transaction generator, tagged sample attempts, and completed socket-write counters. |
| `primary/`, `consensus/`, `store/`, `crypto/` | Narwhal/Tusk ordering, synchronization, storage, and cryptographic components. |
| `benchmark/` | General deployment configuration, a NoRead socket process, and Linux process-memory sampling. |

QSR retains the batch format, untagged ACK, processor, and primary protocol.
The retained unit and integration tests cover reservation limits,
oldest-first scheduling, invalid and stale replies, handoff acceptance,
generation destruction, and TR, alongside upstream component
tests.

The following settings select the five policies in the paper. All other
parameters are held fixed in each comparison.

| Policy | `dissemination_mode` | `qid_reclaim_interval_ms` | `qid_reclaim_policy` |
| --- | --- | ---: | --- |
| Native | `baseline` | 0 | `safe_cut` |
| Concurrent | `concurrent` | 0 | `safe_cut` |
| QID | `qid` | 0 | `safe_cut` |
| QSR | `qid` | 1000 | `safe_cut` |
| TR | `qid` | 1000 | `timeout_replay` |

`configure.py` uses the paper's workload settings: a 61,440-byte raw batch
target, 65,536-byte serialized cap, 100 ms batch timer, a 32-batch active
window, 96 records and 6,291,456 bytes per peer. Primary settings are a
1,000-byte header target, 1,000 ms header timer, garbage-collection depth
50, 10,000 ms synchronization retry delay, and three synchronization peers.
These values are explicit in the generated JSON and can be edited there.

## Measurements used in the paper

| Measurement | Steps from the new logs |
| --- | --- |
| Full-commit throughput | Read each batch digest and raw byte count from `Batch ... contains ... B`. Find the earliest `Committed ... -> digest` at each correct primary, then take their maximum. Sum the raw bytes divided by 512 for batches whose full-commit timestamp falls inside the measurement window, and divide by its duration. A batch missing a required primary's commit is unfinished. |
| Injected rate | Use `CLIENT_WRITE_EVENT accepted_write_completions`, which reports successful framed socket writes. For each client, sum `count` and `window_ms` over the complete reporting intervals contained in the measurement window, divide to obtain its rate, and sum client rates. Record covered duration and write count alongside the rate. |
| Sample completion | Collect `Sending sample transaction` attempts inside the measurement window. Match `(authority, sample ID)` to its worker's `Batch ... contains sample tx ...`, then to full-commit time. Count every attempt, including unmapped ones.|
| Conditional p95 | For completed samples, subtract attempt time from full-commit time. Sort the latencies and linearly interpolate at index `0.95*(m-1)` for `m` completed samples. Report completion and attempt count with p95. The paper averages per-run p95 values. |
| Worker RSS | Select process-time observations within the measurement window. Average RSS for the indicated worker population and divide KiB by 1,024 for MiB. Correct-worker RSS excludes withholding workers. Fault-worker RSS pools all observations from withholding workers and is per process, not their sum. Separate tmpfs database pages are outside worker RSS. |

For WithholdACK full commits, the required primary set excludes the
withholding authorities even though their primaries remain live. Sender
handoff, a successful client write, and full commit are distinct events.
Because pre-window injections can commit inside the window, commit rate
can slightly exceed the concurrent injected rate.

Source code is licensed under [Apache 2.0](LICENSE). Upstream attribution is in
[NOTICE.md](NOTICE.md).
