#!/usr/bin/env python3
"""Accept worker connections without consuming application frames or sending ACKs."""

import argparse
import asyncio
import signal
import socket


async def serve(args):
    connections = set()

    async def hold(reader, writer):
        connections.add(writer)
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.receive_buffer_bytes)
        try:
            await asyncio.Future()
        finally:
            connections.discard(writer)
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(hold, args.host, args.port, backlog=256, reuse_address=True)
    for sock in server.sockets or []:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.receive_buffer_bytes)
    print(f"NoRead listening on {args.host}:{args.port}", flush=True)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        server.close()
        for writer in tuple(connections):
            writer.close()
        await server.wait_closed()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--receive-buffer-bytes", type=int, default=4096)
    args = parser.parse_args()
    asyncio.run(serve(args))


if __name__ == "__main__":
    main()
