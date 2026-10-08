"""Tests for persistence.py: AOF logging + replay (temporary files only)."""

import asyncio

import resp
from commands import CommandHandler
from persistence import AOF, load_aof
from storage import Storage


def replay(path):
    """Replay the AOF into a FRESH storage. Returns (storage, handler, count)."""
    store = Storage()
    handler = CommandHandler(store)
    count = asyncio.run(load_aof(path, handler))
    return store, handler, count


def write_with_aof(path, commands):
    """Run commands through a handler that logs to an AOF, then close it."""
    aof = AOF(path, "always")
    handler = CommandHandler(Storage(), aof)
    for cmd in commands:
        handler.execute(cmd)
    aof.close()


def test_all_four_types_come_back(tmp_path):
    path = tmp_path / "appendonly.aof"
    write_with_aof(path, [
        ["SET", "name", "Arti"], ["APPEND", "name", " Sharma"],
        ["INCR", "views"], ["INCRBY", "views", "9"],
        ["RPUSH", "q", "a", "b", "c"], ["LPOP", "q"],
        ["HSET", "user:1", "name", "Arti", "city", "Pune"], ["HDEL", "user:1", "city"],
        ["SADD", "tags", "py", "go"], ["SREM", "tags", "go"],
        ["SET", "tmp", "x"], ["DEL", "tmp"],
        ["GET", "name"], ["INCR", "name"],  # a read and a failing write: not logged
    ])
    store, handler, count = replay(path)
    assert count == 12
    assert store.get("name") == "Arti Sharma"
    assert store.get("views") == "10"
    assert list(store._data["q"]) == ["b", "c"]
    assert store._data["user:1"] == {"name": "Arti"}
    assert store._data["tags"] == {"py"}
    assert store.exists("tmp") == 0
    assert handler.execute(["TYPE", "q"]) == b"+list\r\n"


def test_expired_pxat_key_is_not_restored(tmp_path, clock):
    path = tmp_path / "appendonly.aof"
    write_with_aof(path, [["SET", "otp", "1234", "PX", "100"], ["SET", "keep", "v"]])
    assert b"PXAT" in path.read_bytes()  # relative PX was logged as absolute
    clock.advance(101)  # the server was down longer than the TTL
    store, handler, _ = replay(path)
    assert handler.execute(["GET", "otp"]) == b"$-1\r\n"
    assert store.exists("otp") == 0
    assert store.get("keep") == "v"


def test_future_expiry_keeps_the_same_time(tmp_path, clock):
    path = tmp_path / "appendonly.aof"
    original = Storage()
    aof = AOF(path, "always")
    handler = CommandHandler(original, aof)
    handler.execute(["SET", "otp", "1234", "EX", "100"])
    handler.execute(["SET", "k", "v"])
    handler.execute(["EXPIRE", "k", "50"])
    aof.close()

    clock.advance(30_000)  # restart 30 s later
    store, handler, _ = replay(path)
    assert store._expires["otp"] == original._expires["otp"]  # same absolute time
    assert store._expires["k"] == original._expires["k"]
    assert handler.execute(["TTL", "otp"]) == b":70\r\n"  # NOT a fresh 100 s
    assert handler.execute(["TTL", "k"]) == b":20\r\n"


def test_missing_file_gives_zero_commands(tmp_path):
    path = tmp_path / "does-not-exist.aof"
    store, _, count = replay(path)
    assert count == 0
    assert store._data == {}
    assert not path.exists()  # replay must not create the file


def test_truncated_last_command(tmp_path, capsys):
    path = tmp_path / "appendonly.aof"
    good = resp.array(["SET", "a", "1"]) + resp.array(["INCR", "a"])
    torn = resp.array(["SET", "name", "Arti"])[:-5]  # crash halfway through a write
    path.write_bytes(good + torn)

    store, _, count = replay(path)
    assert count == 2
    assert store.get("a") == "2"
    assert store.exists("name") == 0
    assert path.read_bytes() == good  # torn tail cut off
    assert "incomplete last command" in capsys.readouterr().out

    # After the restart new commands are appended; the next replay must be clean
    aof = AOF(path, "always")
    aof.log(["SET", "b", "2"])
    aof.close()
    store, _, count = replay(path)
    assert count == 3
    assert store.get("a") == "2" and store.get("b") == "2"


def test_replay_does_not_change_file_size(tmp_path):
    path = tmp_path / "appendonly.aof"
    write_with_aof(path, [["SET", "a", "1"], ["INCR", "a"], ["RPUSH", "q", "x"]])
    size = path.stat().st_size

    # Replay with an AOF ATTACHED, exactly like the real server's handler
    aof = AOF(path, "always")
    handler = CommandHandler(Storage(), aof)
    asyncio.run(load_aof(path, handler))
    aof.close()
    assert path.stat().st_size == size  # nothing was logged again
    assert handler.aof is aof  # logging is switched back on after replay


def test_failed_command_during_replay_is_skipped(tmp_path, capsys):
    path = tmp_path / "appendonly.aof"
    path.write_bytes(resp.array(["SET", "n", "abc"]) + resp.array(["INCR", "n"])
                     + resp.array(["SET", "ok", "yes"]))
    store, _, count = replay(path)
    assert count == 3
    assert store.get("ok") == "yes" and store.get("n") == "abc"
    assert "failed during replay" in capsys.readouterr().out
