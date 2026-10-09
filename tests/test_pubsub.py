"""Tests for Pub/Sub: the PubSub class and SUBSCRIBE / UNSUBSCRIBE."""

import pytest

import resp
from client_state import Client
from commands import CommandHandler
from persistence import AOF
from pubsub import PubSub
from storage import Storage


def make_client(port=50000):
    return Client(writer=None, addr=("127.0.0.1", port))


@pytest.fixture
def client():
    return make_client()


def sub_reply(kind, channel, count):
    return resp.array([kind, channel, count])


# ---------- resp.array with integers ----------

def test_array_encodes_ints_strings_and_nil():
    assert resp.array(["subscribe", "news", 1]) == b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n"
    assert resp.array(["unsubscribe", None, 0]) == b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
    assert resp.array(["a", "b"]) == b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"  # unchanged


# ---------- PubSub class ----------

def test_subscribe_updates_both_places():
    ps, c = PubSub(), make_client()
    assert ps.subscribe(c, "news") == 1
    assert ps.subscribe(c, "sports") == 2
    assert ps.subscribe(c, "news") == 2  # already subscribed: no change
    assert ps.channels == {"news": {c}, "sports": {c}}
    assert c.channels == {"news", "sports"}


def test_channel_removed_when_last_subscriber_leaves():
    ps, c1, c2 = PubSub(), make_client(1), make_client(2)
    ps.subscribe(c1, "news")
    ps.subscribe(c2, "news")
    assert ps.unsubscribe(c1, "news") == 0
    assert ps.channels == {"news": {c2}}
    ps.unsubscribe(c2, "news")
    assert ps.channels == {}  # no empty set left behind


def test_unsubscribe_unknown_channel_is_harmless():
    ps, c = PubSub(), make_client()
    assert ps.unsubscribe(c, "nope") == 0
    assert ps.channels == {}


def test_unsubscribe_all():
    ps, c, other = PubSub(), make_client(1), make_client(2)
    ps.subscribe(c, "b")
    ps.subscribe(c, "a")
    ps.subscribe(other, "a")
    assert ps.unsubscribe_all(c) == ["a", "b"]
    assert c.channels == set()
    assert ps.channels == {"a": {other}}


# ---------- SUBSCRIBE / UNSUBSCRIBE commands ----------

def test_subscribe_command(handler, client):
    reply = handler.execute(["SUBSCRIBE", "news", "sports"], client)
    assert reply == sub_reply("subscribe", "news", 1) + sub_reply("subscribe", "sports", 2)
    assert client.channels == {"news", "sports"}
    assert handler.pubsub.channels == {"news": {client}, "sports": {client}}


def test_subscribe_same_channel_twice(handler, client):
    handler.execute(["subscribe", "news"], client)
    assert handler.execute(["SUBSCRIBE", "news"], client) == sub_reply("subscribe", "news", 1)


def test_unsubscribe_named_channels(handler, client):
    handler.execute(["SUBSCRIBE", "a", "b", "c"], client)
    reply = handler.execute(["UNSUBSCRIBE", "b", "nope"], client)
    assert reply == sub_reply("unsubscribe", "b", 2) + sub_reply("unsubscribe", "nope", 2)
    assert client.channels == {"a", "c"}
    assert set(handler.pubsub.channels) == {"a", "c"}


def test_unsubscribe_without_args_removes_all(handler, client):
    handler.execute(["SUBSCRIBE", "b", "a"], client)
    reply = handler.execute(["UNSUBSCRIBE"], client)
    assert reply == sub_reply("unsubscribe", "a", 1) + sub_reply("unsubscribe", "b", 0)
    assert client.channels == set() and handler.pubsub.channels == {}


def test_unsubscribe_when_not_subscribed(handler, client):
    assert handler.execute(["UNSUBSCRIBE"], client) == b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"


def test_two_clients_share_a_channel(handler):
    c1, c2 = make_client(1), make_client(2)
    handler.execute(["SUBSCRIBE", "news"], c1)
    handler.execute(["SUBSCRIBE", "news", "tech"], c2)
    assert handler.pubsub.channels == {"news": {c1, c2}, "tech": {c2}}
    handler.execute(["UNSUBSCRIBE"], c2)
    assert handler.pubsub.channels == {"news": {c1}}


def test_commands_need_a_client(handler):
    assert handler.execute(["SUBSCRIBE", "news"]) == \
        b"-ERR SUBSCRIBE requires a client connection\r\n"
    assert handler.execute(["UNSUBSCRIBE"]) == \
        b"-ERR UNSUBSCRIBE requires a client connection\r\n"


def test_subscribe_wrong_number_of_arguments(handler, client):
    assert handler.execute(["SUBSCRIBE"], client) == \
        b"-ERR wrong number of arguments for 'subscribe' command\r\n"


def test_pubsub_commands_are_never_logged(tmp_path, client):
    path = tmp_path / "appendonly.aof"
    aof = AOF(path, "always")
    handler = CommandHandler(Storage(), aof)
    handler.execute(["SUBSCRIBE", "news"], client)
    handler.execute(["UNSUBSCRIBE"], client)
    handler.execute(["SET", "k", "v"], client)  # back in normal mode: allowed
    aof.close()
    assert path.read_bytes() == resp.array(["SET", "k", "v"])  # only the data write


# ---------- PUBLISH ----------

class FakeWriter:
    """Collects written bytes instead of sending them over a socket."""

    def __init__(self, closing=False):
        self.data = b""
        self.closing = closing

    def write(self, data):
        self.data += data

    def is_closing(self):
        return self.closing


def subscriber(handler, *channels, closing=False, port=0):
    c = Client(FakeWriter(closing), ("127.0.0.1", port))
    handler.execute(["SUBSCRIBE", *channels], c)
    c.writer.data = b""  # forget the SUBSCRIBE replies, keep only pushes
    return c


def test_publish_reaches_every_subscriber(handler):
    s1 = subscriber(handler, "news", port=1)
    s2 = subscriber(handler, "news", "tech", port=2)
    other = subscriber(handler, "sports", port=3)
    assert handler.execute(["PUBLISH", "news", "hello"]) == b":2\r\n"
    expected = resp.array(["message", "news", "hello"])
    assert s1.writer.data == expected and s2.writer.data == expected
    assert other.writer.data == b""  # different channel


def test_publish_without_subscribers_returns_zero(handler):
    assert handler.execute(["PUBLISH", "empty", "hi"]) == b":0\r\n"


def test_publish_skips_closing_writers(handler):
    alive = subscriber(handler, "news", port=1)
    dying = subscriber(handler, "news", closing=True, port=2)
    assert handler.execute(["PUBLISH", "news", "x"]) == b":1\r\n"
    assert alive.writer.data != b"" and dying.writer.data == b""


def test_publish_after_unsubscribe(handler):
    s = subscriber(handler, "news")
    handler.execute(["UNSUBSCRIBE", "news"], s)
    s.writer.data = b""
    assert handler.execute(["PUBLISH", "news", "x"]) == b":0\r\n"
    assert s.writer.data == b""


def test_publish_non_english_message(handler):
    s = subscriber(handler, "news")
    handler.execute(["PUBLISH", "news", "नमस्ते"])
    assert s.writer.data == b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$18\r\n" + "नमस्ते".encode() + b"\r\n"


def test_publish_wrong_number_of_arguments(handler):
    for cmd in (["PUBLISH"], ["PUBLISH", "news"], ["PUBLISH", "news", "a", "b"]):
        assert handler.execute(cmd) == b"-ERR wrong number of arguments for 'publish' command\r\n"


def test_publish_is_never_logged(tmp_path):
    path = tmp_path / "appendonly.aof"
    aof = AOF(path, "always")
    handler = CommandHandler(Storage(), aof)
    subscriber(handler, "news")
    handler.execute(["PUBLISH", "news", "hello"])
    aof.close()
    assert path.read_bytes() == b""


# ---------- subscribed mode ----------

def context_error(cmd):
    return (f"-ERR Can't execute '{cmd}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / "
            "PING / QUIT / RESET are allowed in this context\r\n").encode()


@pytest.mark.parametrize("cmd", [
    ["GET", "k"], ["set", "k", "v"], ["PUBLISH", "news", "x"], ["KEYS", "*"], ["Lpush", "q", "a"],
])
def test_subscribed_client_cannot_run_other_commands(handler, client, cmd):
    handler.execute(["SUBSCRIBE", "news"], client)
    assert handler.execute(cmd, client) == context_error(cmd[0].lower())


def test_blocked_write_changes_nothing_and_is_not_logged(tmp_path, client):
    path = tmp_path / "appendonly.aof"
    aof = AOF(path, "always")
    store = Storage()
    handler = CommandHandler(store, aof)
    handler.execute(["SUBSCRIBE", "news"], client)
    handler.execute(["SET", "k", "v"], client)
    aof.close()
    assert store._data == {} and path.read_bytes() == b""


def test_allowed_commands_in_subscribed_mode(handler, client):
    handler.execute(["SUBSCRIBE", "news"], client)
    assert handler.execute(["SUBSCRIBE", "tech"], client) == sub_reply("subscribe", "tech", 2)
    assert handler.execute(["PING"], client) == b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"
    assert handler.execute(["ping", "hi"], client) == b"*2\r\n$4\r\npong\r\n$2\r\nhi\r\n"
    assert handler.execute(["UNSUBSCRIBE", "tech"], client) == sub_reply("unsubscribe", "tech", 1)
    assert handler.execute(["QUIT"], client) == b"+OK\r\n"


def test_unknown_command_error_wins_over_subscribed_mode(handler, client):
    handler.execute(["SUBSCRIBE", "news"], client)
    assert handler.execute(["FOO"], client) == b"-ERR unknown command 'FOO'\r\n"


def test_back_to_normal_after_unsubscribing_all(handler, client):
    handler.execute(["SUBSCRIBE", "a", "b"], client)
    assert handler.execute(["GET", "k"], client) == context_error("get")
    handler.execute(["UNSUBSCRIBE"], client)
    assert handler.execute(["SET", "k", "v"], client) == b"+OK\r\n"
    assert handler.execute(["PING"], client) == b"+PONG\r\n"  # normal PONG again


def test_other_clients_are_not_restricted(handler, client):
    handler.execute(["SUBSCRIBE", "news"], client)
    other = make_client(2)
    assert handler.execute(["SET", "k", "v"], other) == b"+OK\r\n"
    assert handler.execute(["SET", "k", "v"]) == b"+OK\r\n"  # no client (e.g. AOF replay)


def test_quit_sets_flag_and_replies_ok(handler, client):
    assert handler.execute(["QUIT"], client) == b"+OK\r\n"
    assert client.quit_requested is True
    assert handler.execute(["QUIT"]) == b"+OK\r\n"  # no client: just +OK
