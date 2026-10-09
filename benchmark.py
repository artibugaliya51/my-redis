"""Load test for My Redis, similar in spirit to redis-benchmark.

Measures SET and GET separately: throughput (ops/sec) and round-trip latency.
Uses raw sockets + pre-encoded RESP so the client is not the bottleneck.

Example: python benchmark.py --port 6380 --clients 50 --requests 100000 --pipeline 1
"""

import argparse
import asyncio
import multiprocessing
import time

import resp

MAX_WORKER_PROCESSES = 4  # client processes, so the client has more CPU than our 1-thread server
VALUE = "xxx"  # 3-byte value, same as redis-benchmark's default


async def run_connection(host, port, conn_id, command, rounds, pipeline, latencies):
    """One connection: send `pipeline` commands, read all replies, repeat."""
    reader, writer = await asyncio.open_connection(host, port)
    key = f"bench:{conn_id}"  # own key: GET always finds what SET wrote
    if command == "SET":
        one, reply = resp.array(["SET", key, VALUE]), b"+OK\r\n"
    else:
        one, reply = resp.array(["GET", key]), resp.bulk_string(VALUE)
    request = one * pipeline  # encoded ONCE, re-sent every round trip
    expected = reply * pipeline
    for _ in range(rounds):
        start = time.monotonic()
        writer.write(request)
        # Replies are known in advance: read exactly that many bytes, no parsing
        data = await reader.readexactly(len(expected))
        latencies.append(time.monotonic() - start)
        if data != expected:
            raise RuntimeError(f"unexpected reply: {data[:60]!r}")
    writer.close()
    await writer.wait_closed()


def worker(host, port, conn_ids, command, rounds, pipeline, barrier, results):
    """One client PROCESS running several connections with asyncio."""

    async def main():
        latencies = []
        barrier.wait()  # all processes start at the same moment
        start = time.monotonic()
        await asyncio.gather(*(
            run_connection(host, port, cid, command, rounds, pipeline, latencies)
            for cid in conn_ids
        ))
        return start, time.monotonic(), latencies

    results.put(asyncio.run(main()))


def percentile(sorted_values, p):
    """p-th percentile (0-100) of an already sorted list (nearest-rank)."""
    index = min(len(sorted_values) - 1, max(0, round(p / 100 * len(sorted_values)) - 1))
    return sorted_values[index]


def benchmark(command, args):
    per_conn = args.requests // args.clients
    rounds = max(1, per_conn // args.pipeline)
    total_ops = rounds * args.pipeline * args.clients  # actual number sent

    n_procs = min(MAX_WORKER_PROCESSES, args.clients)
    groups = [list(range(i, args.clients, n_procs)) for i in range(n_procs)]
    barrier = multiprocessing.Barrier(n_procs)
    results = multiprocessing.Queue()
    procs = [
        multiprocessing.Process(target=worker, args=(
            args.host, args.port, ids, command, rounds, args.pipeline, barrier, results))
        for ids in groups
    ]
    for p in procs:
        p.start()
    outputs = [results.get() for _ in procs]
    for p in procs:
        p.join()

    # monotonic() is system-wide, so times from different processes compare
    elapsed = max(end for _, end, _ in outputs) - min(start for start, _, _ in outputs)
    latencies = sorted(lat for _, _, lats in outputs for lat in lats)
    return {
        "command": command,
        "ops": total_ops,
        "ops_per_sec": total_ops / elapsed,
        "p50_ms": percentile(latencies, 50) * 1000,
        "p99_ms": percentile(latencies, 99) * 1000,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Benchmark SET and GET on My Redis.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--clients", type=int, default=50, help="concurrent connections (default: 50)")
    parser.add_argument("--requests", type=int, default=100_000, help="total requests per command (default: 100000)")
    parser.add_argument("--pipeline", type=int, default=1, help="commands per round trip (default: 1)")
    return parser.parse_args(argv)


def main():
    args = parse_args()
    print(f"clients={args.clients} requests={args.requests} pipeline={args.pipeline}")
    print("latency = one round trip"
          + (f" ({args.pipeline} commands)" if args.pipeline > 1 else ""))
    print(f"{'cmd':<4} {'ops':>8} {'ops/sec':>10} {'p50 ms':>8} {'p99 ms':>8}")
    for command in ("SET", "GET"):  # SET first, so GET finds the keys
        r = benchmark(command, args)
        print(f"{r['command']:<4} {r['ops']:>8} {r['ops_per_sec']:>10,.0f} "
              f"{r['p50_ms']:>8.2f} {r['p99_ms']:>8.2f}")


if __name__ == "__main__":
    main()
