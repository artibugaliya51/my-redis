"""Tests for every command through CommandHandler.execute()."""

from collections import deque

import pytest

WRONGTYPE = b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
NOT_INT = b"-ERR value is not an integer or out of range\r\n"
OK = b"+OK\r\n"
NIL = b"$-1\r\n"


def run(handler, *args):
    return handler.execute(list(args))


def arr(reply):
    """Decode a RESP array reply of bulk strings into a list (None for nil)."""
    lines = reply.split(b"\r\n")
    assert lines[0].startswith(b"*"), reply
    items, i = [], 1
    for _ in range(int(lines[0][1:])):
        if lines[i] == b"$-1":
            items.append(None)
            i += 1
        else:
            items.append(lines[i + 1].decode())
            i += 2
    return items


def wrong_args(name):
    return f"-ERR wrong number of arguments for '{name}' command\r\n".encode()


# ======================= basic =======================

def test_ping_and_echo(handler):
    assert run(handler, "PING") == b"+PONG\r\n"
    assert run(handler, "PING", "hi") == b"$2\r\nhi\r\n"
    assert run(handler, "ECHO", "नमस्ते") == b"$18\r\n" + "नमस्ते".encode() + b"\r\n"


def test_set_get_case_insensitive(handler):
    assert run(handler, "set", "name", "Arti") == OK
    assert run(handler, "GeT", "name") == b"$4\r\nArti\r\n"
    assert run(handler, "GET", "Name") == NIL  # keys ARE case-sensitive


def test_unknown_and_empty_command(handler):
    assert run(handler, "FOO", "x") == b"-ERR unknown command 'FOO'\r\n"
    assert run(handler) == b"-ERR empty command\r\n"


def test_del_exists_keys(handler):
    run(handler, "MSET", "user:1", "a", "user:2", "b", "x", "c")
    assert run(handler, "EXISTS", "user:1", "user:1", "nope") == b":2\r\n"
    assert sorted(arr(run(handler, "KEYS", "user:*"))) == ["user:1", "user:2"]
    assert run(handler, "DEL", "user:1", "nope") == b":1\r\n"
    assert run(handler, "EXISTS", "user:1") == b":0\r\n"


# ======================= expiry =======================

def test_set_ex_and_px(handler, store, clock):
    assert run(handler, "SET", "a", "1", "EX", "10") == OK
    assert run(handler, "set", "b", "2", "px", "500") == OK
    assert store.ttl_ms("a") == 10_000 and store.ttl_ms("b") == 500
    clock.advance(501)
    assert run(handler, "GET", "b") == NIL
    assert run(handler, "GET", "a") == b"$1\r\n1\r\n"


@pytest.mark.parametrize("opts, expected", [
    (["NX"], b"-ERR syntax error\r\n"),
    (["EX"], b"-ERR syntax error\r\n"),
    (["EX", "10", "PX", "5"], b"-ERR syntax error\r\n"),
    (["EX", "abc", "PX", "5"], b"-ERR syntax error\r\n"),  # syntax checked first
    (["EX", "abc"], NOT_INT),
    (["EX", "1.5"], NOT_INT),
    (["EX", "+5"], NOT_INT),
    (["EX", "0"], b"-ERR invalid expire time in 'set' command\r\n"),
    (["PX", "-5"], b"-ERR invalid expire time in 'set' command\r\n"),
])
def test_set_option_errors_change_nothing(handler, store, clock, opts, expected):
    run(handler, "SET", "k", "orig", "EX", "50")
    assert run(handler, "SET", "k", "NEW", *opts) == expected
    assert store.get("k") == "orig" and store.ttl_ms("k") == 50_000


def test_expire_ttl_pttl_persist(handler, clock):
    run(handler, "SET", "k", "v")
    assert run(handler, "TTL", "k") == b":-1\r\n"
    assert run(handler, "EXPIRE", "k", "10") == b":1\r\n"
    assert run(handler, "TTL", "k") == b":10\r\n"
    assert run(handler, "PTTL", "k") == b":10000\r\n"
    assert run(handler, "PERSIST", "k") == b":1\r\n"
    assert run(handler, "PERSIST", "k") == b":0\r\n"
    assert run(handler, "TTL", "k") == b":-1\r\n"
    assert run(handler, "TTL", "nope") == b":-2\r\n"
    assert run(handler, "PTTL", "nope") == b":-2\r\n"


def test_pexpire_then_key_disappears(handler, clock):
    run(handler, "SET", "k", "v")
    run(handler, "PEXPIRE", "k", "500")
    clock.advance(501)
    assert run(handler, "GET", "k") == NIL
    assert run(handler, "TTL", "k") == b":-2\r\n"


@pytest.mark.parametrize("ms, seconds", [(9600, b":10\r\n"), (9400, b":9\r\n"), (400, b":0\r\n")])
def test_ttl_rounds_to_nearest_second(handler, clock, ms, seconds):
    run(handler, "SET", "k", "v", "PX", str(ms))
    assert run(handler, "TTL", "k") == seconds


@pytest.mark.parametrize("cmd, n", [("EXPIRE", "0"), ("EXPIRE", "-1"), ("PEXPIRE", "-5")])
def test_expire_non_positive_deletes_immediately(handler, store, cmd, n):
    run(handler, "SET", "k", "v")
    assert run(handler, cmd, "k", n) == b":1\r\n"
    assert "k" not in store._data


def test_expire_missing_key_and_bad_numbers(handler):
    assert run(handler, "EXPIRE", "ghost", "10") == b":0\r\n"
    assert run(handler, "EXPIRE", "ghost", "0") == b":0\r\n"
    run(handler, "SET", "k", "v")
    assert run(handler, "EXPIRE", "k", "abc") == NOT_INT
    assert run(handler, "EXPIRE", "k", "9223372036854775807") == \
        b"-ERR invalid expire time in 'expire' command\r\n"


def test_plain_set_clears_ttl(handler):
    run(handler, "SET", "k", "v", "EX", "100")
    run(handler, "SET", "k", "v2")
    assert run(handler, "TTL", "k") == b":-1\r\n"


# ======================= string =======================

def test_incr_family(handler, store):
    assert run(handler, "INCR", "n") == b":1\r\n"  # missing key starts at 0
    assert run(handler, "INCRBY", "n", "10") == b":11\r\n"
    assert run(handler, "DECR", "n") == b":10\r\n"
    assert run(handler, "DECRBY", "n", "5") == b":5\r\n"
    assert run(handler, "INCRBY", "n", "-20") == b":-15\r\n"
    assert store.get("n") == "-15"  # stored as a string


@pytest.mark.parametrize("value", ["Arti", "1.5", " 5", "+5"])
def test_incr_on_non_integer(handler, value):
    run(handler, "SET", "k", value)
    assert run(handler, "INCR", "k") == NOT_INT


def test_incr_overflow_keeps_value(handler, store):
    run(handler, "SET", "n", "9223372036854775807")
    assert run(handler, "INCR", "n") == b"-ERR increment or decrement would overflow\r\n"
    assert store.get("n") == "9223372036854775807"
    run(handler, "SET", "m", "-9223372036854775808")
    assert run(handler, "DECR", "m") == b"-ERR increment or decrement would overflow\r\n"
    assert run(handler, "DECRBY", "m", "-9223372036854775808") == b"-ERR decrement would overflow\r\n"
    assert store.get("m") == "-9223372036854775808"


def test_incr_and_append_keep_ttl(handler, store, clock):
    run(handler, "SET", "c", "0", "EX", "100")
    run(handler, "INCR", "c")
    run(handler, "INCRBY", "c", "5")
    assert store.ttl_ms("c") == 100_000
    run(handler, "SET", "t", "hi", "EX", "100")
    run(handler, "APPEND", "t", "!")
    assert store.ttl_ms("t") == 100_000


def test_append_strlen(handler):
    assert run(handler, "STRLEN", "g") == b":0\r\n"
    assert run(handler, "APPEND", "g", "Hello") == b":5\r\n"
    assert run(handler, "APPEND", "g", " World") == b":11\r\n"
    assert run(handler, "GET", "g") == b"$11\r\nHello World\r\n"
    run(handler, "SET", "hi", "नमस्ते")
    assert run(handler, "STRLEN", "hi") == b":6\r\n"  # characters (Redis: 18 bytes)


def test_mset_mget(handler, store):
    run(handler, "SET", "old", "v", "EX", "100")
    run(handler, "RPUSH", "lst", "a")
    assert run(handler, "MSET", "k1", "v1", "k2", "v2", "old", "new") == OK
    assert store.ttl_ms("old") == -1  # MSET clears TTL like SET
    assert arr(run(handler, "MGET", "k1", "missing", "lst", "old")) == ["v1", None, None, "new"]


# ======================= list =======================

def test_push_order(handler):
    assert run(handler, "RPUSH", "q", "a", "b", "c") == b":3\r\n"
    assert run(handler, "LPUSH", "q", "z") == b":4\r\n"
    assert arr(run(handler, "LRANGE", "q", "0", "-1")) == ["z", "a", "b", "c"]
    run(handler, "LPUSH", "s", "a", "b", "c")
    assert arr(run(handler, "LRANGE", "s", "0", "-1")) == ["c", "b", "a"]


@pytest.mark.parametrize("start, stop, expected", [
    ("0", "1", ["z", "a"]),
    ("1", "-1", ["a", "b", "c"]),
    ("-2", "-1", ["b", "c"]),
    ("0", "100", ["z", "a", "b", "c"]),
    ("5", "10", []),
    ("-100", "1", ["z", "a"]),
    ("2", "1", []),
    ("-100", "-50", []),
    ("3", "3", ["c"]),
])
def test_lrange_indexes(handler, start, stop, expected):
    run(handler, "RPUSH", "q", "z", "a", "b", "c")
    assert arr(run(handler, "LRANGE", "q", start, stop)) == expected


def test_queue_and_stack(handler):
    for job in ["j1", "j2", "j3"]:
        run(handler, "LPUSH", "jobs", job)
    assert [run(handler, "RPOP", "jobs") for _ in range(3)] == \
        [b"$2\r\nj1\r\n", b"$2\r\nj2\r\n", b"$2\r\nj3\r\n"]  # FIFO
    run(handler, "LPUSH", "stk", "1", "2", "3")
    assert run(handler, "LPOP", "stk") == b"$1\r\n3\r\n"  # LIFO


def test_pop_last_item_deletes_key_and_ttl(handler, store):
    run(handler, "RPUSH", "tmp", "x", "y")
    run(handler, "EXPIRE", "tmp", "100")
    run(handler, "LPOP", "tmp")
    run(handler, "RPOP", "tmp")
    assert run(handler, "EXISTS", "tmp") == b":0\r\n"
    assert run(handler, "TYPE", "tmp") == b"+none\r\n"
    assert run(handler, "TTL", "tmp") == b":-2\r\n"
    assert "tmp" not in store._expires


def test_push_keeps_ttl(handler, store, clock):
    run(handler, "RPUSH", "t", "a")
    run(handler, "EXPIRE", "t", "100")
    run(handler, "LPUSH", "t", "b")
    assert store.ttl_ms("t") == 100_000


def test_list_commands_on_missing_key(handler):
    assert run(handler, "LLEN", "nope") == b":0\r\n"
    assert run(handler, "LPOP", "nope") == NIL
    assert run(handler, "RPOP", "nope") == NIL
    assert run(handler, "LRANGE", "nope", "0", "-1") == b"*0\r\n"


def test_lrange_bad_index(handler):
    assert run(handler, "LRANGE", "q", "a", "1") == NOT_INT
    assert run(handler, "LRANGE", "q", "0", "1.5") == NOT_INT


# ======================= hash =======================

def test_hset_counts_new_fields(handler):
    assert run(handler, "HSET", "user:1", "name", "Arti", "city", "Pune") == b":2\r\n"
    assert run(handler, "HSET", "user:1", "city", "Delhi", "age", "21") == b":1\r\n"
    assert run(handler, "HSET", "user:1", "x", "1", "x", "2") == b":1\r\n"


def test_hget_hgetall_hexists_hlen(handler):
    run(handler, "HSET", "u", "name", "Arti", "city", "Pune")
    assert run(handler, "HGET", "u", "city") == b"$4\r\nPune\r\n"
    assert run(handler, "HGET", "u", "email") == NIL
    assert run(handler, "HGET", "nope", "x") == NIL
    assert arr(run(handler, "HGETALL", "u")) == ["name", "Arti", "city", "Pune"]
    assert run(handler, "HGETALL", "nope") == b"*0\r\n"
    assert run(handler, "HEXISTS", "u", "name") == b":1\r\n"
    assert run(handler, "HEXISTS", "u", "email") == b":0\r\n"
    assert run(handler, "HEXISTS", "nope", "name") == b":0\r\n"
    assert run(handler, "HLEN", "u") == b":2\r\n"
    assert run(handler, "HLEN", "nope") == b":0\r\n"


def test_hdel_counts_and_deletes_empty_hash(handler, store):
    run(handler, "HSET", "s", "token", "abc", "ip", "1.2.3.4")
    run(handler, "EXPIRE", "s", "100")
    assert run(handler, "HDEL", "s", "token", "nope") == b":1\r\n"
    assert run(handler, "HDEL", "s", "ip") == b":1\r\n"
    assert run(handler, "EXISTS", "s") == b":0\r\n"
    assert run(handler, "TTL", "s") == b":-2\r\n"
    assert "s" not in store._expires


def test_hset_keeps_ttl(handler, store, clock):
    run(handler, "HSET", "u", "a", "1")
    run(handler, "EXPIRE", "u", "100")
    run(handler, "HSET", "u", "b", "2")
    assert store.ttl_ms("u") == 100_000


# ======================= set =======================

def test_sadd_smembers_sismember_scard(handler):
    assert run(handler, "SADD", "tags", "python", "redis", "python") == b":2\r\n"
    assert run(handler, "SADD", "tags", "redis", "go") == b":1\r\n"
    assert sorted(arr(run(handler, "SMEMBERS", "tags"))) == ["go", "python", "redis"]
    assert run(handler, "SMEMBERS", "nope") == b"*0\r\n"
    assert run(handler, "SISMEMBER", "tags", "redis") == b":1\r\n"
    assert run(handler, "SISMEMBER", "tags", "java") == b":0\r\n"
    assert run(handler, "SISMEMBER", "nope", "x") == b":0\r\n"
    assert run(handler, "SCARD", "tags") == b":3\r\n"
    assert run(handler, "SCARD", "nope") == b":0\r\n"


def test_srem_counts_and_deletes_empty_set(handler, store):
    run(handler, "SADD", "on", "u1", "u2")
    run(handler, "EXPIRE", "on", "100")
    assert run(handler, "SREM", "on", "u1", "nope", "u1") == b":1\r\n"
    assert run(handler, "SREM", "on", "u2") == b":1\r\n"
    assert run(handler, "EXISTS", "on") == b":0\r\n"
    assert "on" not in store._expires


def test_sadd_keeps_ttl(handler, store, clock):
    run(handler, "SADD", "s", "a")
    run(handler, "EXPIRE", "s", "100")
    run(handler, "SADD", "s", "b")
    assert store.ttl_ms("s") == 100_000


@pytest.mark.parametrize("keys, expected", [
    (["a", "b"], ["2", "3", "4"]),
    (["a", "b", "c"], ["3", "4"]),
    (["a", "nope", "b"], []),          # missing key = empty set
    (["a"], ["1", "2", "3", "4", "5"]),
    (["a", "x"], []),                  # no common members
])
def test_sinter(handler, keys, expected):
    run(handler, "SADD", "a", "1", "2", "3", "4", "5")
    run(handler, "SADD", "b", "2", "3", "4", "9")
    run(handler, "SADD", "c", "3", "4", "7")
    run(handler, "SADD", "x", "q")
    assert sorted(arr(run(handler, "SINTER", *keys))) == expected


def test_sinter_does_not_modify_sets(handler, store):
    run(handler, "SADD", "a", "1", "2", "3")
    run(handler, "SADD", "b", "2")
    run(handler, "SINTER", "a", "b")
    assert store._data["a"] == {"1", "2", "3"} and store._data["b"] == {"2"}


def test_sinter_wrongtype_wins_over_missing_key(handler, store):
    store.set("lst", deque(["1"]))
    assert run(handler, "SINTER", "nope", "lst") == WRONGTYPE


# ======================= TYPE + WRONGTYPE =======================

def test_type_command(handler, store):
    run(handler, "SET", "s", "v")
    run(handler, "RPUSH", "l", "a")
    run(handler, "HSET", "h", "f", "v")
    run(handler, "SADD", "z", "m")
    for key, name in [("s", b"string"), ("l", b"list"), ("h", b"hash"), ("z", b"set"), ("nope", b"none")]:
        assert run(handler, "TYPE", key) == b"+" + name + b"\r\n"


@pytest.fixture
def typed_keys(store):
    store.set("str", "text")
    store.set("lst", deque(["a"]))
    store.set("hsh", {"f": "v"})
    store.set("st", {"m"})


@pytest.mark.parametrize("cmd", [
    ["GET", "lst"], ["INCR", "hsh"], ["INCRBY", "st", "1"], ["DECR", "lst"], ["APPEND", "lst", "x"],
    ["STRLEN", "hsh"],
    ["LPUSH", "str", "x"], ["RPUSH", "hsh", "x"], ["LPOP", "st"], ["RPOP", "str"], ["LLEN", "hsh"],
    ["LRANGE", "str", "0", "-1"],
    ["HSET", "str", "f", "v"], ["HGET", "lst", "f"], ["HDEL", "st", "f"], ["HGETALL", "str"],
    ["HEXISTS", "lst", "f"], ["HLEN", "str"],
    ["SADD", "str", "m"], ["SREM", "hsh", "m"], ["SMEMBERS", "lst"], ["SISMEMBER", "str", "m"],
    ["SCARD", "hsh"], ["SINTER", "st", "lst"],
])
def test_wrongtype(handler, store, typed_keys, cmd):
    before = {k: (type(v), len(v)) for k, v in store._data.items()}
    assert run(handler, *cmd) == WRONGTYPE
    assert {k: (type(v), len(v)) for k, v in store._data.items()} == before  # nothing changed


def test_type_independent_commands_work_on_any_type(handler, typed_keys):
    assert run(handler, "EXISTS", "str", "lst", "hsh", "st") == b":4\r\n"
    assert run(handler, "EXPIRE", "hsh", "100") == b":1\r\n"
    assert run(handler, "TTL", "hsh") == b":100\r\n"
    assert run(handler, "MGET", "str", "lst") == b"*2\r\n$4\r\ntext\r\n$-1\r\n"
    assert run(handler, "SET", "lst", "now-a-string") == OK  # SET overwrites any type
    assert run(handler, "DEL", "lst", "hsh", "st") == b":3\r\n"


# ======================= wrong number of arguments =======================

@pytest.mark.parametrize("cmd", [
    ["PING", "a", "b"], ["ECHO"], ["SET", "k"], ["GET"], ["GET", "a", "b"], ["DEL"], ["EXISTS"],
    ["KEYS"], ["EXPIRE", "k"], ["EXPIRE", "k", "10", "NX"], ["PEXPIRE"], ["TTL"], ["TTL", "a", "b"],
    ["PTTL"], ["PERSIST"], ["TYPE"], ["INCR"], ["DECR", "a", "b"], ["INCRBY", "a"], ["DECRBY", "a"],
    ["APPEND", "a"], ["STRLEN"], ["MSET"], ["MSET", "a"], ["MSET", "a", "1", "b"], ["MGET"],
    ["LPUSH", "q"], ["RPUSH"], ["LPOP"], ["LPOP", "q", "2"], ["RPOP", "q", "1"], ["LLEN"],
    ["LRANGE", "q", "0"], ["HSET", "k", "f"], ["HSET", "k", "f", "v", "g"], ["HGET", "k"],
    ["HDEL", "k"], ["HGETALL"], ["HEXISTS", "k"], ["HLEN"], ["SADD", "k"], ["SREM", "k"],
    ["SMEMBERS"], ["SISMEMBER", "k"], ["SCARD"], ["SINTER"],
])
def test_wrong_number_of_arguments(handler, store, cmd):
    assert run(handler, *cmd) == wrong_args(cmd[0].lower())
    assert store._data == {}  # a bad command never creates a key
