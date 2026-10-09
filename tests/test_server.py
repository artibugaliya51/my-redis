"""Integration tests: the real server code over real TCP, using redis-py."""

import asyncio
import socket
import threading
import time

import pytest
import redis

import server
from commands import CommandHandler
from storage import Storage


@pytest.fixture
def server_port(monkeypatch):
    """Run the real server (handle_client + active_expiry_loop) on a free port.

    Port 0 lets the OS pick any free port, so we never touch 6380.
    The event loop runs in a background thread because redis-py is blocking.
    """
    # Fresh storage for every test (server.py reads these globals at runtime)
    fresh = Storage()
    monkeypatch.setattr(server, "storage", fresh)
    monkeypatch.setattr(server, "handler", CommandHandler(fresh))

    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()

    async def start():
        srv = await asyncio.start_server(server.handle_client, "127.0.0.1", 0)
        task = asyncio.create_task(server.active_expiry_loop())
        return srv, task

    srv, task = asyncio.run_coroutine_threadsafe(start(), loop).result(timeout=5)
    yield srv.sockets[0].getsockname()[1]

    async def stop():
        task.cancel()
        srv.close()
        await srv.wait_closed()

    asyncio.run_coroutine_threadsafe(stop(), loop).result(timeout=5)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
    loop.close()


@pytest.fixture
def r(server_port):
    client = redis.Redis(port=server_port, protocol=2, decode_responses=True)
    yield client
    client.close()


def wait_until(condition, timeout=2.0):
    """Poll instead of a fixed sleep: fast when it works, not flaky when slow."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.02)
    return False


# ---------- a few commands from every group ----------

def test_basic(r):
    assert r.ping() is True
    assert r.set("name", "Arti") is True
    assert r.get("name") == "Arti"
    assert r.get("nope") is None
    assert r.echo("नमस्ते") == "नमस्ते"
    assert r.exists("name", "nope") == 1
    assert r.delete("name") == 1


def test_expiry_with_real_clock(r):
    r.set("otp", "1234", px=100)
    r.set("k", "v")
    assert r.expire("k", 10) is True
    assert r.ttl("k") == 10
    assert r.persist("k") is True
    assert wait_until(lambda: r.get("otp") is None)


def test_strings(r):
    assert r.incr("views") == 1
    assert r.incrby("views", 10) == 11
    assert r.append("greet", "Hello") == 5
    assert r.mset({"a": "1", "b": "2"}) is True
    assert r.mget("a", "b", "nope") == ["1", "2", None]


def test_lists(r):
    assert r.rpush("q", "a", "b", "c") == 3
    assert r.lpush("q", "z") == 4
    assert r.lrange("q", 0, -1) == ["z", "a", "b", "c"]
    assert r.rpop("q") == "c"
    assert r.llen("q") == 3


def test_hashes(r):
    assert r.hset("user:1", mapping={"name": "Arti", "city": "Pune"}) == 2
    assert r.hget("user:1", "city") == "Pune"
    assert r.hgetall("user:1") == {"name": "Arti", "city": "Pune"}  # redis-py rebuilds the dict


def test_sets(r):
    assert r.sadd("a", "1", "2", "3") == 3
    r.sadd("b", "2", "3", "4")
    assert r.smembers("a") == {"1", "2", "3"}
    assert r.sinter("a", "b") == {"2", "3"}
    assert r.type("a") == "set"


def test_errors_reach_the_client(r):
    r.rpush("lst", "a")
    with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
        r.get("lst")
    with pytest.raises(redis.ResponseError, match="unknown command"):
        r.execute_command("FOO")
    assert r.ping() is True  # connection still alive after errors


# ---------- server behaviour ----------

def test_two_clients_share_data(server_port):
    c1 = redis.Redis(port=server_port, protocol=2, decode_responses=True)
    c2 = redis.Redis(port=server_port, protocol=2, decode_responses=True)
    try:
        c1.set("shared", "from-client-1")
        assert c2.get("shared") == "from-client-1"
    finally:
        c1.close()
        c2.close()


def test_pipelining(r):
    pipe = r.pipeline(transaction=False)
    pipe.set("p", "1").incr("p").get("p")
    assert pipe.execute() == [True, 2, "2"]


def test_protocol_error_closes_only_that_client(server_port, r):
    with socket.create_connection(("127.0.0.1", server_port), timeout=2) as bad:
        bad.sendall(b"*1\r\n:5\r\n")
        reply = bad.recv(1024)
        assert reply.startswith(b"-ERR Protocol error")
        assert bad.recv(1024) == b""  # server closed this connection
    assert r.ping() is True  # other clients unaffected


def test_active_expiry_removes_untouched_keys(r):
    pipe = r.pipeline(transaction=False)
    for i in range(200):
        pipe.set(f"otp:{i}", "x", px=50)
    pipe.set("user:1", "arti")
    pipe.execute()
    # Never read the keys: only the background task can delete them
    assert wait_until(lambda: len(server.storage._expires) == 0)
    assert list(server.storage._data) == ["user:1"]


def test_incr_is_atomic_with_5_threads(server_port):
    def worker():
        client = redis.Redis(port=server_port, protocol=2)
        for _ in range(1000):
            client.incr("counter")
        client.close()

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check = redis.Redis(port=server_port, protocol=2, decode_responses=True)
    assert check.get("counter") == "5000"
    check.close()


# ---------- Pub/Sub over a real connection ----------

def test_subscribe_over_socket_and_cleanup_on_disconnect(server_port):
    with socket.create_connection(("127.0.0.1", server_port), timeout=2) as s:
        s.sendall(b"*3\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n$6\r\nsports\r\n")
        expected = (b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n"
                    b"*3\r\n$9\r\nsubscribe\r\n$6\r\nsports\r\n:2\r\n")
        data = b""
        while len(data) < len(expected):
            data += s.recv(1024)
        assert data == expected
        assert set(server.handler.pubsub.channels) == {"news", "sports"}
    # Socket closed: the server must forget this client's subscriptions
    assert wait_until(lambda: server.handler.pubsub.channels == {})


def test_redis_py_pubsub_subscribe(r):
    p = r.pubsub()
    p.subscribe("news")
    msg = p.get_message(timeout=1)
    assert msg == {"type": "subscribe", "pattern": None, "channel": "news", "data": 1}
    p.unsubscribe("news")
    msg = p.get_message(timeout=1)
    assert msg["type"] == "unsubscribe" and msg["data"] == 0
    p.close()


def recv_exactly(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        assert chunk, "connection closed early"
        data += chunk
    return data


def test_publish_between_two_real_clients(server_port):
    addr = ("127.0.0.1", server_port)
    with socket.create_connection(addr, timeout=2) as sub, socket.create_connection(addr, timeout=2) as pub:
        sub.sendall(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
        ack = b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n"
        assert recv_exactly(sub, len(ack)) == ack

        pub.sendall(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nhello\r\n")
        assert recv_exactly(pub, 4) == b":1\r\n"  # 1 receiver
        msg = b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"
        assert recv_exactly(sub, len(msg)) == msg  # pushed to the subscriber


def test_redis_py_publish_and_listen(r, server_port):
    p = r.pubsub()
    p.subscribe("news")
    assert p.get_message(timeout=1)["type"] == "subscribe"
    assert r.publish("news", "hello") == 1
    assert r.publish("nobody-here", "x") == 0
    msg = p.get_message(timeout=1)
    assert msg == {"type": "message", "pattern": None, "channel": "news", "data": "hello"}
    p.close()


def test_slow_subscriber_does_not_block_publisher(server_port, r):
    # This subscriber NEVER reads: its socket buffers fill up quickly
    with socket.create_connection(("127.0.0.1", server_port), timeout=2) as lazy:
        lazy.sendall(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
        time.sleep(0.1)
        big = "x" * 10_000
        start = time.monotonic()
        for _ in range(500):  # ~5 MB, far more than the OS socket buffers hold
            assert r.publish("news", big) == 1
        elapsed = time.monotonic() - start
        assert elapsed < 5  # publisher was never blocked by the lazy subscriber
        # The downside: the undelivered bytes pile up in the server's memory
        (sub_client,) = server.handler.pubsub.channels["news"]
        assert sub_client.writer.transport.get_write_buffer_size() > 1_000_000
