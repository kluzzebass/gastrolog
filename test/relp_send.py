#!/usr/bin/env python3
"""Send test syslog messages over RELP to a gastrolog RELP ingester.

Usage:
    python3 test/relp_send.py [host:port] [count]

Defaults to localhost:2514 and 5 messages.
"""

import socket
import sys
import time


def read_response(sock):
    """Read a single RELP response frame."""
    buf = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
        if b"\n" in buf:
            break
    return buf.decode(errors="replace").strip()


def main():
    addr = sys.argv[1] if len(sys.argv) > 1 else "localhost:2514"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    host, port = addr.rsplit(":", 1)
    host = host or "localhost"

    print(f"Connecting to {host}:{port}...")
    s = socket.create_connection((host, int(port)))

    # Open RELP session.
    offer = b"relp_version=0\nrelp_software=gastrolog-test\ncommands=syslog"
    s.send(f"1 open {len(offer)} ".encode() + offer + b"\n")
    print(f"  open -> {read_response(s)}")

    # Send syslog messages.
    for i in range(count):
        txnr = i + 2
        msg = f"<14>{time.strftime('%b %d %H:%M:%S')} testhost myapp[{txnr}]: RELP test message {i}"
        s.send(f"{txnr} syslog {len(msg)} {msg}\n".encode())
        print(f"  msg {i} -> {read_response(s)}")

    # Close session.
    close_txnr = count + 2
    s.send(f"{close_txnr} close 0 \n".encode())
    print(f"  close -> {read_response(s)}")

    s.close()
    print(f"Done, sent {count} messages.")


if __name__ == "__main__":
    main()
