"""Tests for DBSIZE, FLUSHALL and INFO."""

import asyncio

import commands
import resp
from client_state import Client
from commands import CommandHandler
from persistence import AOF, load_aof
from storage import Storage


def parse_info(reply):
    """Decode an INFO bulk-string reply into {field: value} (all strings)."""
    assert reply.startswith(b"$")
    body = reply.split(b"\r\n", 1)[1][:-2].decode()  # strip "$<len>\r\n" and final \r\n
    fields = {}
    for line in body.split("\r\n"):
        if line and not line.startswith("#"):
            key, value = line.split(":", 1)
            fields[key] = value
    return fields


def make_client(port=50000):
    return Client(writer=None, addr=("127.0.0.1", port))


# ---------- DBSIZE ----------

def test_dbsize(handler):
    assert handler.execute(["DBSIZE"]) == b":0\r\n"
    handler.execute(["MSET", "a", "1", "b", "2"])
    handler.execute(["RPUSH", "q", "x"])
    assert handler.execute(["dbsize"]) == b":3\r\n"
    handler.execute(["DEL", "a"])
    assert handler.execute(["DBSIZE"]) == b":2\r\n"


def test_dbsize_counts_expired_keys_until_removed(handler, store, clock):
    handler.execute(["SET", "tmp", "x", "PX", "100"])
    clock.advance(101)
    assert handler.execute(["DBSIZE"]) == b":1\r\n"  # not removed yet (like Redis)
    store.active_expire_cycle()
    assert handler.execute(["DBSIZE"]) == b":0\r\n"


# ---------- FLUSHALL ----------

def test_flushall_deletes_everything(handler, store):
    handler.execute(["SET", "a", "1", "EX", "100"])
    handler.execute(["HSET", "h", "f", "v"])
    handler.execute(["SADD", "s", "m"])
    assert handler.execute(["FLUSHALL"]) == b"+OK\r\n"
    assert store._data == {} and store._expires == {}
    assert handler.execute(["DBSIZE"]) == b":0\r\n"


def test_flushall_is_logged_and_replays_correctly(tmp_path):
    path = tmp_path / "appendonly.aof"
    aof = AOF(path, "always")
    handler = CommandHandler(Storage(), aof)
    handler.execute(["SET", "old", "1"])
    handler.execute(["FLUSHALL"])
    handler.execute(["SET", "new", "2"])
    aof.close()
    assert resp.array(["FLUSHALL"]) in path.read_bytes()

    fresh = Storage()
    asyncio.run(load_aof(path, CommandHandler(fresh)))
    assert fresh._data == {"new": "2"}  # "old" was flushed during replay too


def test_flushall_and_dbsize_bad_arguments(handler):
    assert handler.execute(["FLUSHALL", "ASYNC"]) == b"-ERR syntax error\r\n"
    assert handler.execute(["DBSIZE", "x"]) == b"-ERR wrong number of arguments for 'dbsize' command\r\n"
    assert handler.execute(["INFO", "server"]) == b"-ERR wrong number of arguments for 'info' command\r\n"


# ---------- INFO ----------

def test_info_format(handler):
    reply = handler.execute(["INFO"])
    body = reply.split(b"\r\n", 1)[1]
    assert int(reply[1:reply.index(b"\r\n")]) == len(body) - 2  # valid bulk length
    text = body.decode()
    for section in ("# Server", "# Clients", "# Stats", "# Persistence", "# Keyspace"):
        assert section in text
    assert "\r\n\r\n# Clients\r\n" in text  # blank line between sections


def test_info_fields(handler):
    handler.execute(["SET", "a", "1"])
    handler.execute(["SET", "b", "2", "EX", "100"])
    handler.execute(["RPUSH", "q", "x"])
    info = parse_info(handler.execute(["INFO"]))
    assert set(info) == {"uptime_in_seconds", "connected_clients", "total_commands_processed",
                         "keys", "keys_with_expiry", "aof_enabled", "appendfsync", "pubsub_channels"}
    assert info["keys"] == "3" and info["keys_with_expiry"] == "1"
    assert info["aof_enabled"] == "0" and info["appendfsync"] == "-"
    assert info["pubsub_channels"] == "0" and info["connected_clients"] == "0"
    assert int(info["uptime_in_seconds"]) >= 0


def test_info_with_aof(tmp_path):
    aof = AOF(tmp_path / "appendonly.aof", "always")
    info = parse_info(CommandHandler(Storage(), aof).execute(["INFO"]))
    aof.close()
    assert info["aof_enabled"] == "1" and info["appendfsync"] == "always"


def test_total_commands_counts_client_commands_only(handler):
    c = make_client()
    handler.execute(["SET", "a", "1"], c)
    handler.execute(["GET", "a"], c)
    handler.execute(["INCR", "a"], c)  # errors count too: the command was processed
    handler.execute(["SET", "replayed", "1"])  # no client = AOF replay: not counted
    handler.execute(["NOPE"], c)  # unknown command: not counted
    info = parse_info(handler.execute(["INFO"], c))
    assert info["total_commands_processed"] == "4"  # SET, GET, INCR, INFO itself


def test_info_pubsub_channels(handler):
    c1, c2 = make_client(1), make_client(2)
    handler.execute(["SUBSCRIBE", "news", "tech"], c1)
    handler.execute(["SUBSCRIBE", "news"], c2)
    assert parse_info(handler.execute(["INFO"]))["pubsub_channels"] == "2"


def test_uptime_grows(handler, monkeypatch):
    start = handler.start_time
    monkeypatch.setattr(commands.time, "monotonic", lambda: start + 42.7)
    assert parse_info(handler.execute(["INFO"]))["uptime_in_seconds"] == "42"
