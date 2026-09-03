#!/usr/bin/env python3
"""Probe for the Nagle + delayed-ACK stall in a request/response protocol.

Reproduces the exact write shape the Aerospike Rust client uses: one
sendall() of the whole request, then a blocking read of the response.
Sweeps request sizes across the MSS boundary and reports the latency tail
with TCP_NODELAY off (Nagle on, the client's current behaviour) and on.

Usage:
    nagle_probe.py [--host H] [--port P] [--serve] [--iters N]

With no --host it runs its own echo server in a thread on 127.0.0.1.
With --serve it only runs the server (for cross-container runs).
"""

import argparse
import socket
import statistics
import struct
import sys
import threading
import time

RESP = b"\x00" * 8


def recvall(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf += chunk
    return buf


def handshake_server(sock, stop):
    """Replays the peer side of a rustls TLS handshake at the socket level.

    The syscall shape was measured from the real client stack with
    `examples/tls_write_shape.rs`, which traced: W1460 R1803 W6 W74.
    TLS crypto is irrelevant to Nagle -- only the write/read syscall
    sequence determines TCP behaviour -- so replaying that sequence over a
    plain socket reproduces the kernel-level conditions exactly.
    """
    sock.settimeout(0.5)
    while not stop.is_set():
        try:
            conn, _ = sock.accept()
        except socket.timeout:
            continue
        except OSError:
            return
        threading.Thread(target=serve_handshake, args=(conn,), daemon=True).start()


def serve_handshake(conn):
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        recvall(conn, 1460)          # ClientHello
        conn.sendall(b"\x00" * 1803)  # server flight
        recvall(conn, 6 + 74)         # change_cipher_spec + Finished
        conn.sendall(b"\x00")         # session ticket / first reply
    except (ConnectionError, OSError):
        pass
    finally:
        conn.close()


def measure_handshake(host, port, iters, nodelay):
    """Time a full connection setup, fresh socket each time."""
    lat = []
    for _ in range(iters):
        s = socket.create_connection((host, port))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1 if nodelay else 0)
        try:
            t0 = time.perf_counter()
            s.sendall(b"\x00" * 1460)
            recvall(s, 1803)
            # The two consecutive small writes. Nagle holds the second
            # because the first is small and still unacked; the peer holds
            # its ACK because it has no response to piggyback on yet.
            s.sendall(b"\x00" * 6)
            s.sendall(b"\x00" * 74)
            recvall(s, 1)
            lat.append((time.perf_counter() - t0) * 1000.0)
        finally:
            s.close()
    return lat


def server(sock, stop):
    """Read a length-prefixed request in full, then reply. The 'read the
    whole request before replying' part is what creates the deadlock: the
    server cannot piggyback an ACK on a response it cannot yet produce."""
    sock.settimeout(0.5)
    while not stop.is_set():
        try:
            conn, _ = sock.accept()
        except socket.timeout:
            continue
        except OSError:
            return
        threading.Thread(target=serve_conn, args=(conn, stop), daemon=True).start()


def serve_conn(conn, stop):
    # Server side gets NODELAY so its own 8-byte replies are never the
    # thing being delayed. We are measuring the request direction.
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        while not stop.is_set():
            hdr = recvall(conn, 4)
            (n,) = struct.unpack("!I", hdr)
            if n == 0:
                return
            recvall(conn, n - 4)
            conn.sendall(RESP)
    except (ConnectionError, OSError):
        return
    finally:
        conn.close()


def measure(host, port, size, iters, nodelay, split=False):
    """One request/response round trip, timed.

    split=False mirrors the Rust client: a single write_all() of the whole
    request. split=True is the positive control -- header and body as two
    separate writes, the classic Nagle/delayed-ACK deadlock shape. If the
    control shows no stall either, the probe itself is not sensitive and
    the negative result means nothing.
    """
    s = socket.create_connection((host, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1 if nodelay else 0)
    try:
        mss = s.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG)
    except OSError:
        mss = -1

    req = struct.pack("!I", size) + b"\xab" * (size - 4)

    def send_one():
        if split:
            # Small write first, then the rest. Nagle holds the second
            # write because a small segment is already unacked.
            s.sendall(req[:4])
            s.sendall(req[4:])
        else:
            s.sendall(req)

    # Warm up: get past slow-start and any quickack window at connect.
    for _ in range(20):
        send_one()
        recvall(s, len(RESP))

    lat = []
    for _ in range(iters):
        t0 = time.perf_counter()
        send_one()
        recvall(s, len(RESP))
        lat.append((time.perf_counter() - t0) * 1000.0)

    s.sendall(struct.pack("!I", 0))
    s.close()
    return lat, mss


def summarize(lat):
    lat_sorted = sorted(lat)
    n = len(lat_sorted)
    return {
        "p50": lat_sorted[n // 2],
        "p99": lat_sorted[min(n - 1, int(n * 0.99))],
        "max": lat_sorted[-1],
        "mean": statistics.fmean(lat_sorted),
        # Anything at/above 30ms is the delayed-ACK timer, not real work.
        "stalls": sum(1 for x in lat_sorted if x >= 30.0),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host")
    ap.add_argument("--port", type=int, default=48123)
    ap.add_argument("--serve", action="store_true")
    ap.add_argument("--iters", type=int, default=300)
    args = ap.parse_args()

    stop = threading.Event()
    host = args.host or "127.0.0.1"

    if args.serve or not args.host:
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", args.port))
        srv.listen(16)
        threading.Thread(target=server, args=(srv, stop), daemon=True).start()

        hsrv = socket.socket()
        hsrv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        hsrv.bind(("0.0.0.0", args.port + 1))
        hsrv.listen(16)
        threading.Thread(target=handshake_server, args=(hsrv, stop), daemon=True).start()

        if args.serve:
            print(f"serving on 0.0.0.0:{args.port}", flush=True)
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                stop.set()
            return

    # Sizes chosen to straddle both the 1500-byte-Ethernet MSS (~1448) and
    # the macOS loopback MSS (~16324). The stall is expected just above one
    # MSS, where the receiver holds a lone segment's ACK.
    sizes = [
        64, 512, 1400, 1448, 1460, 1500, 2000, 2896, 2900, 4096, 8192,
        14000, 16000, 16324, 16384, 16500, 17000, 20000, 32648, 33000,
    ]

    print(f"host={host}:{args.port}  iters={args.iters}  python={sys.platform}")

    for split in (False, True):
        shape = ("SPLIT WRITE (header+body, positive control)" if split
                 else "SINGLE write_all (what the Rust client does)")
        print(f"\n### {shape}")
        print(f"{'size':>7} {'MSS':>6} | {'Nagle ON (current)':>34} | {'NODELAY (fix)':>34}")
        print(f"{'':>7} {'':>6} | {'p50':>7} {'p99':>7} {'max':>8} {'stalls':>7} |"
              f" {'p50':>7} {'p99':>7} {'max':>8} {'stalls':>7}")
        print("-" * 96)

        for size in sizes:
            try:
                off, mss = measure(host, args.port, size, args.iters,
                                   nodelay=False, split=split)
                on, _ = measure(host, args.port, size, args.iters,
                                nodelay=True, split=split)
            except (ConnectionError, OSError) as e:
                print(f"{size:>7} {'-':>6} | error: {e}")
                continue
            a, b = summarize(off), summarize(on)
            flag = "  <== STALL" if a["stalls"] > 0 and b["stalls"] == 0 else ""
            print(
                f"{size:>7} {mss:>6} |"
                f" {a['p50']:>7.2f} {a['p99']:>7.2f} {a['max']:>8.2f} {a['stalls']:>7} |"
                f" {b['p50']:>7.2f} {b['p99']:>7.2f} {b['max']:>8.2f} {b['stalls']:>7}"
                f"{flag}"
            )

    # TLS connection setup. Write shape traced from the real rustls stack.
    print("\n### TLS HANDSHAKE replay (W1460 R1803 W6 W74) -- per new connection")
    print(f"{'':>7} {'':>6} | {'p50':>7} {'p99':>7} {'max':>8} {'stalls':>7} |"
          f" {'p50':>7} {'p99':>7} {'max':>8} {'stalls':>7}")
    print("-" * 96)
    hs_iters = max(30, args.iters // 5)
    try:
        off = measure_handshake(host, args.port + 1, hs_iters, nodelay=False)
        on = measure_handshake(host, args.port + 1, hs_iters, nodelay=True)
        a, b = summarize(off), summarize(on)
        flag = "  <== STALL" if a["stalls"] > 0 and b["stalls"] == 0 else ""
        print(
            f"{'setup':>7} {'':>6} |"
            f" {a['p50']:>7.2f} {a['p99']:>7.2f} {a['max']:>8.2f} {a['stalls']:>7} |"
            f" {b['p50']:>7.2f} {b['p99']:>7.2f} {b['max']:>8.2f} {b['stalls']:>7}"
            f"{flag}"
        )
        print(f"\n  n={hs_iters} connections per arm; "
              f"mean {a['mean']:.2f}ms -> {b['mean']:.2f}ms "
              f"({a['mean'] / max(b['mean'], 1e-9):.0f}x)")
    except (ConnectionError, OSError) as e:
        print(f"  handshake probe error: {e}")

    stop.set()


if __name__ == "__main__":
    main()
