#!/usr/bin/env python3
"""Create a committee and per-authority parameters for a QID deployment."""

import argparse
import base64
import ipaddress
import json
from pathlib import Path
import shutil
import subprocess


POLICIES = {
    "native": ("baseline", 0, "safe_cut"),
    "concurrent": ("concurrent", 0, "safe_cut"),
    "qid": ("qid", 0, "safe_cut"),
    "qsr": ("qid", 1000, "safe_cut"),
    "tr": ("qid", 1000, "timeout_replay"),
}


def address(host, port):
    ip = ipaddress.ip_address(host)
    return f"[{ip}]:{port}" if ip.version == 6 else f"{ip}:{port}"


def write_json(path, value, private=False):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
    if private:
        path.chmod(0o600)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authorities", type=int, default=4)
    parser.add_argument("--hosts", nargs="+", help="one IP address per authority; defaults to localhost")
    parser.add_argument("--output", type=Path, required=True, help="a new deployment directory")
    parser.add_argument("--node", type=Path, default=Path("target/release/node"))
    parser.add_argument("--key-dir", type=Path, help="reuse authority-N.json keys from an existing deployment")
    parser.add_argument("--rank-zero", type=int, help="place this authority first in public-key order")
    parser.add_argument("--base-port", type=int, default=3001)
    parser.add_argument("--policy", choices=POLICIES, default="qsr")
    parser.add_argument("--withhold", nargs="*", type=int, default=[], metavar="AUTHORITY")
    parser.add_argument("--recover-after-ms", type=int)
    args = parser.parse_args()
    n = args.authorities
    if n < 2:
        parser.error("at least two authorities are required")
    hosts = args.hosts or ["127.0.0.1"] * n
    if len(hosts) != n:
        parser.error("--hosts must contain one IP address per authority")
    try:
        hosts = [str(ipaddress.ip_address(host)) for host in hosts]
    except ValueError as error:
        parser.error(str(error))
    if args.base_port < 1 or args.base_port + 10 * (n - 1) + 4 > 65535:
        parser.error("the five ports per authority must fit in 1..65535")
    for index in args.withhold + ([] if args.rank_zero is None else [args.rank_zero]):
        if not 0 <= index < n:
            parser.error("authority index outside the committee")
    if args.recover_after_ms is not None and (args.recover_after_ms <= 0 or not args.withhold):
        parser.error("--recover-after-ms requires --withhold and a positive delay")
    if args.output.exists():
        parser.error("output already exists; select a new deployment directory")
    if args.key_dir is None and not args.node.is_file():
        parser.error("build target/release/node first, or pass --node /path/to/node")
    if args.key_dir is not None:
        for i in range(n):
            if not (args.key_dir / f"authority-{i}.json").is_file():
                parser.error(f"missing authority-{i}.json in --key-dir")

    args.output.mkdir(parents=True)
    key_dir = args.output / "keys"
    key_dir.mkdir(mode=0o700)
    keys = []
    for i in range(n):
        target = key_dir / f"authority-{i}.json"
        if args.key_dir is None:
            subprocess.run([str(args.node.resolve()), "generate_keys", "--filename", str(target)], check=True)
        else:
            shutil.copyfile(args.key_dir / target.name, target)
        target.chmod(0o600)
        keys.append(json.loads(target.read_text(encoding="utf-8")))
    names = [base64.b64decode(key["name"], validate=True) for key in keys]
    if any(len(name) != 32 for name in names) or len(set(names)) != n:
        raise ValueError("committee keys must have distinct 32-byte public keys")
    if args.rank_zero is not None:
        first = min(range(n), key=lambda i: names[i])
        if args.key_dir is not None and first != args.rank_zero:
            raise ValueError("reused keys have a different rank-zero authority")
        if first != args.rank_zero:
            keys[first], keys[args.rank_zero] = keys[args.rank_zero], keys[first]
            for i in (first, args.rank_zero):
                # Only these freshly generated key files are reassigned.
                target = key_dir / f"authority-{i}.json"
                target.write_text(json.dumps(keys[i], indent=2) + "\n", encoding="utf-8")

    mode, interval, reclaim = POLICIES[args.policy]
    parameters = {
        "header_size": 1000,
        "max_header_delay": 1000,
        "gc_depth": 50,
        "sync_retry_delay": 10000,
        "sync_retry_nodes": 3,
        "batch_size": 61440,
        "max_batch_delay": 100,
        "dissemination_mode": mode,
        "max_batch_bytes": 65536,
        "qid_max_active_batches": 32,
        "qid_peer_max_records": 96,
        "qid_peer_max_bytes": 6291456,
        "qid_reclaim_interval_ms": interval,
        "qid_reclaim_policy": reclaim,
        "worker_ack_mode": "normal",
    }
    authorities = {}
    rows = ["authority\thost\tprimary\tworker_to_primary\tprimary_to_worker\ttransactions\tworker_to_worker"]
    for i, (host, key) in enumerate(zip(hosts, keys)):
        ports = [address(host, args.base_port + 10 * i + offset) for offset in range(5)]
        authorities[key["name"]] = {
            "stake": 1,
            "primary": {"primary_to_primary": ports[0], "worker_to_primary": ports[1]},
            "workers": {"0": {"primary_to_worker": ports[2], "transactions": ports[3], "worker_to_worker": ports[4]}},
        }
        local = dict(parameters)
        if i in args.withhold:
            local["worker_ack_mode"] = "withhold"
            if args.recover_after_ms is not None:
                local["worker_ack_recover_after_ms"] = args.recover_after_ms
        write_json(args.output / f"parameters-{i}.json", local)
        rows.append("\t".join([str(i), host] + ports))
    write_json(args.output / "committee.json", {"authorities": authorities})
    (args.output / "deployment.tsv").write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(f"Created {n} authorities in {args.output}. Addresses: {args.output / 'deployment.tsv'}")


if __name__ == "__main__":
    main()
