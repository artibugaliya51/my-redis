# My Redis

A simplified Redis clone in Python: an asyncio TCP server that speaks the RESP2 protocol,
keeps data in memory, and persists it with an append-only file (AOF). It works with the
official `redis-py` client. The server uses **only the Python standard library**; `redis` and
`pytest` are used only for tests and the benchmark.

I built it to understand how Redis works inside (event loop, protocol, data structures,
expiry, persistence, pub/sub) well enough to explain every design decision.

**Features**

- **Protocol:** RESP2 over TCP (asyncio, single thread), plus inline commands like `PING`.
- **Data types:** 4 types (string, list, hash, set) with `WRONGTYPE` errors like real Redis.
- **Commands:** 48, with Redis's exact error messages (see the table below).
- **Key expiry:** `EX`/`PX`/`PXAT`, `EXPIRE`, `TTL`... with **lazy + active** expiration.
- **AOF persistence:**
  - `always` / `everysec` / `no` fsync policies.
  - Replay on startup, with a torn last write repaired automatically.
  - Compaction with `BGREWRITEAOF` (atomic file swap).
- **Pub/Sub:** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, with subscribed-mode rules.
- **Server tools:** `INFO`, `DBSIZE`, `FLUSHALL`, command-line config, `logging`.
- **Testing and tools:** 296 pytest tests (unit + integration), a benchmark tool, and an
  interactive client that shows raw RESP.

---

## 1. Quick start

Requires Python 3.10+ (developed with Python 3.13).

```bash
git clone <this-repo-url> my-redis && cd my-redis
python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt   # redis + pytest, only for tests / benchmark;
                                      # the server itself needs nothing
```

**Run the server** (terminal 1):

```bash
python server.py                 # 127.0.0.1:6380, AOF in ./appendonly.aof (everysec)
python server.py --help          # all options
python server.py --port 6400 --no-aof --log-level debug
```

**Talk to it** (terminal 2):

```bash
python client.py                 # interactive; prints raw RESP replies
my-redis> SET name "Arti Sharma"
b'+OK\r\n'
my-redis> GET name
b'$11\r\nArti Sharma\r\n'
```

Or use the official client:

```python
import redis
r = redis.Redis(port=6380, protocol=2, decode_responses=True)   # protocol=2: we speak RESP2
r.set("otp", "1234", ex=60)
print(r.get("otp"), r.ttl("otp"))
```

**Run the tests and the benchmark:**

```bash
pytest -q                                     # 296 tests, about 2 seconds
python benchmark.py --port 6380               # SET/GET ops/sec + p50/p99 latency
python benchmark.py --port 6380 --pipeline 16
```

`python server.py --help`:

```
usage: server.py [-h] [--host HOST] [--port PORT] [--aof-file AOF_FILE]
                 [--appendfsync {always,everysec,no}] [--no-aof]
                 [--log-level {DEBUG,INFO,WARNING,ERROR}]

My Redis: a simplified Redis clone (asyncio + RESP2).

options:
  -h, --help            show this help message and exit
  --host HOST           address to listen on (default: 127.0.0.1)
  --port PORT           TCP port to listen on (default: 6380)
  --aof-file AOF_FILE   path of the append-only file (default: appendonly.aof)
  --appendfsync {always,everysec,no}
                        when to fsync the AOF (default: everysec)
  --no-aof              disable persistence: no AOF is read or written
  --log-level {DEBUG,INFO,WARNING,ERROR}
                        logging level (default: INFO)
```

---

## 2. Supported commands

| Group | Commands | Notes |
|---|---|---|
| Connection | `PING [msg]`, `ECHO msg`, `QUIT` | `PING` replies `["pong", msg]` in subscribed mode |
| Keys | `DEL`, `EXISTS`, `KEYS pattern`, `TYPE` | `KEYS` supports glob patterns (`*`, `?`, `[abc]`), case-sensitive |
| Expiry | `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST` | A time ≤ now deletes the key immediately (like Redis) |
| Strings | `SET key value [EX s \| PX ms \| PXAT unix-ms]`, `GET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `MSET`, `MGET` | 64-bit overflow checks; `INCR`/`APPEND` keep the TTL, plain `SET` clears it |
| Lists (`deque`) | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE` | Negative indexes; the key is deleted when the list becomes empty |
| Hashes (`dict`) | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN` | `HSET` returns only the number of *new* fields |
| Sets (`set`) | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SINTER` | `SINTER` walks the smallest set first |
| Pub/Sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` | Subscribed clients may only use (UN)SUBSCRIBE / PING / QUIT |
| Server | `INFO`, `DBSIZE`, `FLUSHALL`, `BGREWRITEAOF` | `BGREWRITEAOF` runs synchronously (see limitations) |

Errors match real Redis: `WRONGTYPE ...`, `ERR wrong number of arguments for 'get' command`,
`ERR value is not an integer or out of range`, `ERR syntax error`, and so on.

---

## 3. Architecture

```
 client (redis-py / client.py / benchmark.py)
   │  TCP, RESP2 bytes
   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ server.py   asyncio event loop (ONE thread)                              │
│   handle_client() per connection:  read → execute → write → drain        │
│   background tasks: active_expiry_loop (100 ms), aof_fsync_loop (1 s)    │
│   config.py → options          client_state.py → Client (per connection) │
└───────┬───────────────────────────────┬──────────────────────────────────┘
        │ bytes ⇄ Python lists          │ ["SET", "name", "Arti"]
        ▼                               ▼
┌──────────────────┐     ┌────────────────────────────────────────────────┐
│ resp.py          │     │ commands.py   CommandHandler                   │
│ read_command()   │     │   dispatch table: name → handler method        │
│ encoders: +OK,   │     │   WRONGTYPE caught in ONE place                │
│ $bulk, :int, *arr│     │   subscribed-mode check                        │
└──────────────────┘     │   successful writes → AOF (absolute expiry)    │
                         └───────┬──────────────────┬──────────────┬──────┘
                                 ▼                  ▼              ▼
                     ┌──────────────────┐ ┌─────────────────┐ ┌─────────────┐
                     │ storage.py       │ │ persistence.py  │ │ pubsub.py   │
                     │ _data   {key:val}│ │ AOF: log, fsync,│ │ channel →   │
                     │ _expires{key:ms} │ │ replay, rewrite │ │ {Clients}   │
                     │ lazy+active exp. │ │ appendonly.aof  │ │             │
                     └──────────────────┘ └─────────────────┘ └─────────────┘
```

**One request, step by step** (`SET otp 1234 EX 60`):

1. `server.py` awaits `resp.read_command()`, which uses `readline()` / `readexactly()`, so
   commands split across TCP packets (or many pipelined in one packet) are handled correctly.
2. `CommandHandler.execute()` looks up `SET` in the dispatch table and runs `_set`.
3. `_set` validates *everything first*, converts `EX 60` to an absolute time `now + 60000 ms`,
   then calls `storage.set(...)`.
4. Because `SET` is a write and succeeded, `execute()` appends `SET otp 1234 PXAT <that same ms>`
   to the AOF.
5. The reply `+OK\r\n` goes back through `writer.write()` + `await writer.drain()`.

**On startup:** `load_aof()` replays the file through the same `CommandHandler` (logging is
switched off during replay), and only then is the AOF opened for appending.

---

## 4. Design decisions (and why)

| Decision | Why |
|---|---|
| **asyncio, one thread, no locks** | Same model as Redis. A coroutine can only switch at `await`, and `execute()` has no `await`, so every command is atomic. 5 threads × 1000 `INCR` always gives exactly 5000 (tested). |
| **RESP2** | Simple, binary-safe (length-prefixed bulk strings), and the format real clients speak. The same parser is reused to replay the AOF. |
| **`dict` for the keyspace and hashes** | O(1) average get/set/delete; it's a hash table, like Redis's own `dict`. |
| **`deque` for lists** | O(1) push/pop at **both** ends. A Python `list` has O(n) `insert(0)`/`pop(0)`, which would make `LPUSH`/`LPOP` queues slow. |
| **`set` for sets** | Uniqueness for free, O(1) membership. `SINTER` iterates the smallest set: O(N × M) with N = the smallest set. |
| **Separate `_expires` dict** | Keys without a TTL cost nothing extra. `TTL`/`PERSIST` are dict operations. Redis uses the same two-dict layout. |
| **Lazy + active expiry** | Lazy: every access checks the TTL (cheap, always correct). Active: every 100 ms, sample 20 keys with a TTL and delete the expired ones. Repeat while more than 25% were expired, with a 25 ms time budget so clients are never blocked for long. Without it, never-read expired keys would leak memory. |
| **Absolute expiry times (`time.time()` in ms)** | An absolute time stays correct after a restart. `EXPIRE k 100` is logged as `PEXPIREAT k <ms>`, so replaying later doesn't give the key a fresh 100 s. `time.monotonic()` is used only for *durations* (uptime, time budgets). |
| **Validate before mutate** | A failing command (bad option, overflow, WRONGTYPE) never leaves a half-applied change. |
| **WRONGTYPE caught centrally** | `storage.get_typed()` raises; `execute()` converts it once, instead of a try/except in ~30 handlers. |
| **Empty collections are deleted** | Like Redis: popping the last list item deletes the key *and its TTL*. |
| **AOF in RESP format** | Replay = feed the file to the same parser and run the commands. |
| **Flush on every command + fsync per policy** | `flush()` puts each write in the OS page cache, so a process crash (`kill -9`) loses nothing. Verified: the same crash test lost all keys before this change and none after it. fsync (power-loss safety) is per command (`always`), once per second by a background task (`everysec`), or left to the OS (`no`). |
| **Truncate a torn last command on replay** | A crash during a write can leave half a command. Because the file is opened in append mode, new commands would be written *after* that garbage and shift every later boundary, so the next replay would fail. Truncating to the last complete command keeps the file valid. Corruption in the *middle* stops the server instead of silently losing data. |
| **Atomic AOF rewrite** | `BGREWRITEAOF` writes the minimal commands for the current data to a temp file in the same folder, fsyncs it, then `os.replace()`s it in (an atomic rename). A crash at any moment leaves either the complete old file or the complete new one. It runs without `await`, so no write can happen during the rewrite. |
| **Bidirectional Pub/Sub index** | `channel → clients` makes `PUBLISH` fast; `client.channels` makes disconnect cleanup fast. Without both, one of them would need a full scan. |
| **`PUBLISH` does not `await drain()`** | A slow subscriber must not block the publisher. Trade-off: its buffer can grow without limit (measured: ~4.4 MB for a subscriber that never reads, after 500 × 10 KB messages). |
| **Subscribed mode** | In RESP2 a pushed message and a normal reply look the same, so like Redis only (UN)SUBSCRIBE / PING / QUIT are allowed while subscribed. |

---

## 5. Benchmark

`benchmark.py` uses raw sockets with pre-encoded RESP commands, reads replies with a single
`readexactly()` (no parsing), and spreads connections over 4 client processes, so the Python
client is less likely to be the bottleneck.

**Setup:**
- **Machine:** Apple A18 Pro (6 cores), macOS, Python 3.13.16.
- **Load:** 50 clients, 100,000 SETs then 100,000 GETs, 3-byte values.
- **Server:** a fresh server for every run, on loopback.
- **Reported values:** the **median of 3 runs**, with the min–max range in brackets.

| Config | SET ops/sec | SET p50 / p99 (ms) | GET ops/sec | GET p50 / p99 (ms) | Server CPU |
|---|---|---|---|---|---|
| `--no-aof` | 73,621 (71.7k–82.8k) | 0.64 / 1.43 | 74,297 (73.5k–83.3k) | 0.61 / 1.47 | ~87% |
| `--appendfsync everysec` | 52,045 (51.8k–53.4k) | 0.92 / 2.16 | 71,605 (70.5k–72.7k) | 0.67 / 1.50 | ~90% |
| `--appendfsync always` | 19,982 (19.9k–20.5k) | 2.41 / 3.87 | 67,816 (67.1k–68.0k) | 0.70 / 1.55 | ~67% |
| `--no-aof --pipeline 16` | 117,860 (116.0k–119.5k) | 6.64 / 8.93 *per 16-command batch* | 138,091 (135.9k–139.3k) | 5.61 / 7.80 *per batch* | ~83% |

**What the numbers show:**

- **AOF `everysec`: SET drops about 30%.** That comes from the extra `write()` per command
  (flush to the OS), not from fsync, which only happens once per second. GET is unaffected.
- **`always`: SET is about 3.7× slower than no AOF.** The server's CPU drops to ~67% because
  it is **waiting for the disk**: fsync runs on the event-loop thread, so every client waits.
- **Pipelining 16 gives about 1.6× (SET) and 1.9× (GET), not 16×.** It removes per-round-trip
  overhead, but our per-command cost stays. Every command is still parsed in Python and gets
  its own `write()` + `drain()`. Real Redis batches all replies into one write.

**Honest caveats:**

- **Run-to-run variation is about 15%** (config 1 ranged from 71.7k to 82.8k), so small
  differences are noise.
- **On macOS `os.fsync()` doesn't force the drive's cache** (that needs `F_FULLFSYNC`), so the
  `always` result is optimistic. Expect lower numbers on Linux with a real fsync or on a slower disk.
- **The client may limit the pipelined run:** server CPU was below 100%, so the client probably
  limited the pipelined run partly.
- **Real Redis was not measured on this machine.** It isn't installed, so there is no
  side-by-side comparison here.

---

## 6. Testing

**296 pytest tests, about 2 seconds:** `pytest -q`

| File | Tests | What |
|---|---|---|
| `test_resp.py` | 32 | Parser (split packets, pipelining, inline, Unicode, `\r\n` inside values, disconnects, invalid input) and all encoders |
| `test_storage.py` | 33 | Basic operations, glob `KEYS`, lazy expiry (including the exact boundary millisecond), active expiry, types, empty-collection deletion |
| `test_commands.py` | 134 | Every command through `CommandHandler.execute()`, grouped by type, plus parameterised `WRONGTYPE` and wrong-argument-count tests |
| `test_persistence.py` | 20 | Logging rules, absolute expiry, replay of all types, torn-write truncation, flush/fsync per policy, rewrite (size, identical data, crash during rename) |
| `test_pubsub.py` | 32 | Pub/Sub index, `SUBSCRIBE` / `UNSUBSCRIBE` / `PUBLISH`, subscribed mode, `QUIT` |
| `test_admin.py` | 11 | `DBSIZE`, `FLUSHALL` (including replay), `INFO` format and fields |
| `test_config.py` | 12 | Argument parsing, and the real `server.py` started as a subprocess with options |
| `test_server.py` | 22 | **Integration:** a real server on a free port with redis-py and raw sockets. Covers two clients sharing data, pipelining, protocol errors, active expiry, Pub/Sub between two connections, and **5 threads × 1000 `INCR` = exactly 5000** |

How the tests are built:

- **Unit vs integration.** Most tests call `Storage` / `CommandHandler` directly and run fast.
  The 26 integration tests (22 in `test_server.py`, 4 in `test_config.py`) go through real TCP
  connections and real processes.
- **Fake clock.** Expiry tests replace `now_ms` with a controllable clock (in *both* `storage`
  and `commands`, because `from storage import now_ms` copies the reference). TTL tests are
  instant and never flaky; only a few integration tests use real time, with polling instead of
  fixed sleeps.
- **Isolation.** Every test gets a fresh `Storage`. Persistence tests use `tmp_path`. Server
  tests use port 0 (any free port) and never touch port 6380 or the project folder.
- **Crash tests (run by hand, not part of pytest):** the server was killed with `kill -9`
  right after writes and then restarted, and all keys came back.

---

## 7. Comparison with real Redis and limitations

**Same ideas as Redis:**
- Single-threaded event loop.
- RESP2.
- The two-dict keyspace / expires layout.
- Lazy + active expiry (same 100 ms, 20 keys, 25 ms budget). Newer Redis versions repeat the
  cycle when more than ~10% of the sample was expired (tunable with `active-expire-effort`);
  ours uses the original 25% threshold.
- AOF with `appendfsync` policies and absolute expiry times.
- Replay with torn-tail truncation.
- Rewrite with an atomic rename.
- The two-way Pub/Sub index.
- Subscribed-mode rules and error messages.

**Differences and limitations:**

- **Performance:** Python + asyncio instead of C. Every command costs microseconds of
  interpreter work, every RESP line is a separate `await`, and replies are written one by one.
  This is why pipelining helps much less than in Redis.
- **No memory optimisations:** Redis uses compact encodings (listpack, intset, int-encoded
  strings, quicklist). We use plain Python objects for everything.
- **`BGREWRITEAOF` blocks the server.** Redis forks a child process (copy-on-write) and
  buffers writes that happen during the rewrite. Ours is synchronous, which is simple and safe
  but freezes clients with a large dataset. It is not triggered automatically either.
- **fsync runs on the event-loop thread** (Redis uses a background thread for `everysec`).
- **No output-buffer limit for slow Pub/Sub subscribers** (Redis disconnects them).
- **Strings are text, not bytes:** values are decoded as UTF-8, so invalid UTF-8 is rejected,
  and `STRLEN` / `APPEND` count characters (Redis counts bytes: `"नमस्ते"` is 6 here, 18 in Redis).
- **Missing features:** sorted sets, streams, RDB snapshots, `MULTI`/`EXEC`, Lua, replication,
  cluster, `AUTH`/ACLs, `SCAN`, `PSUBSCRIBE`, RESP3, `CONFIG GET/SET`, multiple databases,
  eviction (`maxmemory`), and many options (`SET NX/XX`, `LPOP count`...).
- **Expensive commands:** `KEYS`, `HGETALL`, `SMEMBERS` and `LRANGE 0 -1` are O(n) and block
  the server on large data (the same advice as in Redis: use `SCAN` in production, which we don't have).
- **No authentication:** do not run it with `--host 0.0.0.0` on an untrusted network.

---

## 8. How this was built

This project was built with **Claude Code**, an AI coding assistant.

- **The process:** I followed a phase-by-phase plan (storage → RESP → commands → server →
  expiry → types → persistence → pub/sub → config/INFO/benchmark) and gave Claude Code one
  step at a time.
- **My part:** for every step I read its explanation, reviewed the code and the test output,
  and ran manual tests and crash tests myself. I committed only after I understood the change.
- **Claude Code's part:** it wrote the code and the tests.
- **My goal:** to understand every design decision and be able to explain it, not just to
  have working code.

Some problems were found during the build, by the automated tests or by manual checks, and
fixed before the step was committed:

- **Crash safety:** a `kill -9` crash test showed the `everysec` AOF lost *every* write,
  because nothing left Python's buffer. The fix was a flush on every command, verified by
  running the same crash test on the old and the new code.
- **Path bug:** the rewrite tests failed because `pathlib.Path + str` raised a `TypeError`. The
  fix was to normalise paths with `os.fspath()`.
- **Logger name:** the logger name showed `__main__` when the server was run as a script. The
  fix was an explicit logger name.
- **Test setup problems:** some test setups didn't test what they were meant to, for example a
  background process that ignored SIGINT and a redis-py client that sent `HELLO` (RESP3). This
  was caught by checking the setup itself before trusting a failing result.

---

## 9. Project structure

```
my-redis/
├── server.py         asyncio TCP server: connections, background tasks, startup/shutdown
├── resp.py           RESP2 parser (read_command) and encoders
├── commands.py       CommandHandler: dispatch table, all 48 commands, AOF logging rules
├── storage.py        in-memory keyspace: data + expiry dicts, lazy/active expiry, types
├── persistence.py    AOF: log, flush/fsync policies, replay with truncation, atomic rewrite
├── pubsub.py         channel → subscribers index
├── client_state.py   per-connection state (writer, subscribed channels, QUIT flag)
├── config.py         command-line options (argparse)
├── client.py         interactive test client that prints raw RESP (listen mode after SUBSCRIBE)
├── benchmark.py      load generator: SET/GET ops/sec, p50/p99, pipelining
├── tests/            296 pytest tests (conftest.py has the store / handler / fake clock fixtures)
├── pytest.ini
├── requirements-dev.txt  redis + pytest (tests and benchmark only)
└── CLAUDE.md         instructions I gave Claude Code (rules + how to explain each step)
```
