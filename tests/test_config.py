"""Tests for config.py and for starting the real server with options."""

import os
import signal
import socket
import subprocess
import sys
import time

import pytest
import redis

from config import parse_args

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(PROJECT, "server.py")


# ---------- parse_args ----------

def test_defaults_match_the_old_hardcoded_values():
    c = parse_args([])
    assert (c.host, c.port, c.aof_file, c.appendfsync, c.no_aof, c.log_level) == \
        ("127.0.0.1", 6380, "appendonly.aof", "everysec", False, "INFO")


def test_all_options():
    c = parse_args(["--host", "0.0.0.0", "--port", "7000", "--aof-file", "/tmp/x.aof",
                    "--appendfsync", "always", "--no-aof", "--log-level", "debug"])
    assert (c.host, c.port, c.aof_file, c.appendfsync, c.no_aof, c.log_level) == \
        ("0.0.0.0", 7000, "/tmp/x.aof", "always", True, "DEBUG")


@pytest.mark.parametrize("argv", [
    ["--port", "abc"], ["--port", "0"], ["--port", "70000"],
    ["--appendfsync", "sometimes"], ["--log-level", "loud"], ["--unknown"],
])
def test_invalid_options_exit_with_error(argv, capsys):
    with pytest.raises(SystemExit) as exc:
        parse_args(argv)
    assert exc.value.code == 2  # argparse's "usage error" exit code
    assert "error:" in capsys.readouterr().err


# ---------- the real server process with options ----------

def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(cwd, *options):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, SERVER, "--port", str(port), *options], cwd=cwd,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL),
    )
    client = redis.Redis(port=port, protocol=2, decode_responses=True)
    for _ in range(100):
        try:
            client.ping()
            return proc, client
        except redis.ConnectionError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("server did not start:\n" + proc.stdout.read())


def stop_server(proc, client):
    client.close()
    proc.send_signal(signal.SIGINT)  # like Ctrl+C
    output, _ = proc.communicate(timeout=5)
    return output


def test_no_aof_creates_no_file(tmp_path):
    proc, r = start_server(tmp_path, "--no-aof")
    assert r.set("k", "v") is True and r.get("k") == "v"
    with pytest.raises(redis.ResponseError, match="AOF is not enabled"):
        r.bgrewriteaof()
    log = stop_server(proc, r)
    assert list(tmp_path.iterdir()) == []  # no appendonly.aof anywhere
    assert "AOF disabled (--no-aof)" in log


def test_no_aof_does_not_replay_an_existing_file(tmp_path):
    (tmp_path / "appendonly.aof").write_bytes(b"*3\r\n$3\r\nSET\r\n$3\r\nold\r\n$1\r\n1\r\n")
    proc, r = start_server(tmp_path, "--no-aof")
    assert r.get("old") is None
    stop_server(proc, r)


def test_custom_aof_file_and_policy(tmp_path):
    proc, r = start_server(tmp_path, "--aof-file", "data.aof", "--appendfsync", "always")
    r.set("name", "Arti")
    log = stop_server(proc, r)
    assert (tmp_path / "data.aof").exists() and not (tmp_path / "appendonly.aof").exists()
    # timestamp + level format from logging
    assert " INFO    server: My Redis listening on 127.0.0.1:" in log
    assert "Server stopped." in log

    proc, r = start_server(tmp_path, "--aof-file", "data.aof")
    assert r.get("name") == "Arti"  # replayed from the custom file
    log = stop_server(proc, r)
    assert "Loaded 1 commands from data.aof" in log


def test_log_level_warning_hides_info(tmp_path):
    proc, r = start_server(tmp_path, "--no-aof", "--log-level", "warning")
    r.ping()
    log = stop_server(proc, r)
    assert "Client connected" not in log and "listening" not in log
