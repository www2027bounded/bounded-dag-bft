#!/usr/bin/env python3
"""Sample VmRSS for selected Linux processes; write timestamped observations to stdout."""

import argparse
import csv
from pathlib import Path
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pids", nargs="+", type=int)
    parser.add_argument("--interval", type=float, default=0.5)
    args = parser.parse_args()
    if args.interval <= 0 or any(pid <= 0 for pid in args.pids):
        parser.error("PIDs and interval must be positive")
    writer = csv.writer(sys.stdout)
    writer.writerow(["unix_seconds", "pid", "rss_kib"])
    try:
        while True:
            live = False
            for pid in args.pids:
                try:
                    status = Path(f"/proc/{pid}/status").read_text()
                except FileNotFoundError:
                    continue
                live = True
                for line in status.splitlines():
                    if line.startswith("VmRSS:"):
                        writer.writerow([f"{time.time():.6f}", pid, int(line.split()[1])])
                        break
            sys.stdout.flush()
            if not live:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
