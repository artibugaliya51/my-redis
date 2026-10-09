"""Tests for persistence.py: AOF logging + replay (temporary files only)."""

import asyncio

import pytest

import persistence
import resp
import server
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


# ---------- flush / fsync behaviour per policy ----------

@pytest.fixture
def fsync_calls(monkeypatch):
    """Count os.fsync calls instead of really syncing."""
    calls = []
    monkeypatch.setattr(persistence.os, "fsync", lambda fd: calls.append(fd))
    return calls


@pytest.mark.parametrize("policy, fsyncs_per_log", [("always", 1), ("everysec", 0), ("no", 0)])
def test_log_flushes_every_policy_and_fsyncs_only_always(tmp_path, fsync_calls, policy, fsyncs_per_log):
    path = tmp_path / "appendonly.aof"
    aof = AOF(path, policy)
    for i in range(3):
        aof.log(["SET", "k", str(i)])
        # Flushed to the OS after EVERY command: another reader already sees it
        expected = b"".join(resp.array(["SET", "k", str(j)]) for j in range(i + 1))
        assert path.read_bytes() == expected
    assert len(fsync_calls) == 3 * fsyncs_per_log
    aof.close()


def test_flush_and_fsync_and_close_always_fsync(tmp_path, fsync_calls):
    aof = AOF(tmp_path / "appendonly.aof", "no")
    aof.flush_and_fsync()
    assert len(fsync_calls) == 1
    aof.close()
    assert len(fsync_calls) == 2
    aof.close()  # second close does nothing
    assert len(fsync_calls) == 2


def test_aof_fsync_loop_calls_flush_and_fsync_periodically(monkeypatch):
    monkeypatch.setattr(server, "AOF_FSYNC_INTERVAL", 0.01)

    class FakeAOF:
        calls = 0

        def flush_and_fsync(self):
            FakeAOF.calls += 1

    async def run_briefly():
        task = asyncio.create_task(server.aof_fsync_loop(FakeAOF()))
        await asyncio.sleep(0.1)
        task.cancel()

    asyncio.run(run_briefly())
    assert FakeAOF.calls >= 3


def test_aof_fsync_loop_survives_an_error(monkeypatch, capsys):
    monkeypatch.setattr(server, "AOF_FSYNC_INTERVAL", 0.01)

    class BrokenAOF:
        calls = 0

        def flush_and_fsync(self):
            BrokenAOF.calls += 1
            raise OSError("disk full")

    async def run_briefly():
        task = asyncio.create_task(server.aof_fsync_loop(BrokenAOF()))
        await asyncio.sleep(0.1)
        task.cancel()

    asyncio.run(run_briefly())
    assert BrokenAOF.calls >= 2  # kept running after the first error
    assert "disk full" in capsys.readouterr().err


# ---------- AOF rewrite (BGREWRITEAOF) ----------

def rewrite_setup(path):
    """A storage + handler with an AOF attached, like the real server."""
    store = Storage()
    aof = AOF(path, "always")
    return store, CommandHandler(store, aof), aof


def test_rewrite_shrinks_1000_incr(tmp_path):
    path = tmp_path / "appendonly.aof"
    store, handler, aof = rewrite_setup(path)
    for _ in range(1000):
        handler.execute(["INCR", "counter"])
    before = path.stat().st_size

    assert handler.execute(["BGREWRITEAOF"]) == b"+Background append only file rewriting started\r\n"
    after = path.stat().st_size
    aof.close()

    assert after < before / 100
    assert path.read_bytes() == resp.array(["SET", "counter", "1000"])
    _, _, count = replay(path)
    assert count == 1


def test_rewrite_replays_to_identical_data_and_expiry(tmp_path, clock):
    path = tmp_path / "appendonly.aof"
    store, handler, aof = rewrite_setup(path)
    for cmd in [
        ["SET", "name", "Arti"], ["APPEND", "name", " Sharma"], ["SET", "otp", "1234", "EX", "100"],
        ["RPUSH", "q", "a", "b", "c"], ["LPUSH", "q", "z"], ["LPOP", "q"], ["EXPIRE", "q", "50"],
        ["HSET", "user:1", "name", "Arti", "city", "Pune", "age", "21"], ["HDEL", "user:1", "age"],
        ["PEXPIRE", "user:1", "7000"],
        ["SADD", "tags", "py", "go", "rust"], ["SREM", "tags", "go"], ["EXPIRE", "tags", "30"],
        ["SET", "plain", "v"], ["INCRBY", "n", "42"],
    ]:
        handler.execute(cmd)
    handler.execute(["BGREWRITEAOF"])
    aof.close()

    restored, _, _ = replay(path)
    assert restored._data == store._data  # all 4 types, same values and order
    assert restored._expires == store._expires  # same absolute expiry times
    assert list(restored._data["q"]) == ["a", "b", "c"]


def test_rewrite_skips_expired_keys(tmp_path, clock):
    path = tmp_path / "appendonly.aof"
    store, handler, aof = rewrite_setup(path)
    handler.execute(["SET", "otp", "1234", "PX", "100"])
    handler.execute(["RPUSH", "oldq", "x"])
    handler.execute(["PEXPIRE", "oldq", "100"])
    handler.execute(["SET", "keep", "v"])
    clock.advance(101)  # otp and oldq are now expired, never touched

    handler.execute(["BGREWRITEAOF"])
    aof.close()
    content = path.read_bytes()
    assert b"otp" not in content and b"oldq" not in content
    assert content == resp.array(["SET", "keep", "v"])


def test_writes_after_rewrite_are_appended_to_new_file(tmp_path):
    path = tmp_path / "appendonly.aof"
    store, handler, aof = rewrite_setup(path)
    handler.execute(["SET", "a", "1"])
    handler.execute(["INCR", "a"])
    handler.execute(["BGREWRITEAOF"])
    rewritten = path.read_bytes()

    handler.execute(["INCR", "a"])  # must go into the NEW file
    handler.execute(["SADD", "s", "x"])
    aof.close()

    assert path.read_bytes() == rewritten + resp.array(["INCR", "a"]) + resp.array(["SADD", "s", "x"])
    restored, _, count = replay(path)
    assert count == 3
    assert restored.get("a") == "3" and restored._data["s"] == {"x"}


def test_rewrite_leaves_no_temp_file(tmp_path):
    path = tmp_path / "appendonly.aof"
    _, handler, aof = rewrite_setup(path)
    handler.execute(["SET", "a", "1"])
    handler.execute(["BGREWRITEAOF"])
    aof.close()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["appendonly.aof"]


def test_failed_rewrite_keeps_old_aof(tmp_path, monkeypatch):
    path = tmp_path / "appendonly.aof"
    store, handler, aof = rewrite_setup(path)
    handler.execute(["SET", "a", "1"])
    handler.execute(["INCR", "a"])
    original = path.read_bytes()

    def broken_replace(src, dst):
        raise OSError("simulated crash during rename")

    monkeypatch.setattr(persistence.os, "replace", broken_replace)
    with pytest.raises(OSError):
        aof.rewrite(store)
    monkeypatch.undo()

    assert path.read_bytes() == original  # old AOF untouched
    assert not (tmp_path / "appendonly.aof.rewrite.tmp").exists()  # temp cleaned up
    handler.execute(["INCR", "a"])  # old handle still works
    aof.close()
    restored, _, _ = replay(path)
    assert restored.get("a") == "3"


def test_bgrewriteaof_errors(tmp_path):
    handler = CommandHandler(Storage())  # no AOF
    assert handler.execute(["BGREWRITEAOF"]) == b"-ERR AOF is not enabled\r\n"
    assert handler.execute(["BGREWRITEAOF", "x"]) == \
        b"-ERR wrong number of arguments for 'bgrewriteaof' command\r\n"
