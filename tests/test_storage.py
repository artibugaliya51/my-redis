"""Unit tests for storage.py (uses the fake clock for expiry)."""

from collections import deque

import pytest

from storage import WrongTypeError


# ---------- basic operations ----------

def test_set_get_and_overwrite(store):
    store.set("name", "Arti")
    assert store.get("name") == "Arti"
    store.set("name", "Ravi")
    assert store.get("name") == "Ravi"


def test_get_missing_is_none(store):
    assert store.get("nope") is None


def test_delete_counts_only_existing_keys(store):
    store.set("a", "1")
    store.set("b", "2")
    assert store.delete("a", "missing", "a", "b") == 2
    assert store.get("a") is None


def test_exists_counts_duplicates(store):
    store.set("a", "1")
    assert store.exists("a", "a", "missing") == 2


@pytest.mark.parametrize("pattern, expected", [
    ("*", ["Hello", "hallo", "hello", "user:1", "user:2"]),
    ("user:*", ["user:1", "user:2"]),
    ("h?llo", ["hallo", "hello"]),          # case-sensitive: not "Hello"
    ("h[ae]llo", ["hallo", "hello"]),
    ("nomatch*", []),
])
def test_keys_glob(store, pattern, expected):
    for k in ["hello", "hallo", "Hello", "user:1", "user:2"]:
        store.set(k, "x")
    assert sorted(store.keys(pattern)) == expected


# ---------- lazy expiry ----------

def test_key_expires_lazily(store, clock):
    store.set("otp", "1234", expire_at_ms=clock.now + 1000)
    assert store.get("otp") == "1234"
    clock.advance(1001)
    assert store.get("otp") is None
    assert "otp" not in store._data and "otp" not in store._expires


def test_key_alive_exactly_at_expiry_ms(store, clock):
    # Expired only when now > expire_at (same rule as Redis)
    store.set("k", "v", expire_at_ms=clock.now + 500)
    clock.advance(500)
    assert store.get("k") == "v"
    clock.advance(1)
    assert store.get("k") is None


def test_expired_key_hidden_from_exists_keys_delete(store, clock):
    store.set("tmp", "x", expire_at_ms=clock.now + 10)
    store.set("perm", "y")
    clock.advance(11)
    assert store.exists("tmp") == 0
    assert store.keys("*") == ["perm"]
    assert store.delete("tmp") == 0


def test_ttl_ms(store, clock):
    store.set("a", "1", expire_at_ms=clock.now + 5000)
    store.set("b", "2")
    assert store.ttl_ms("a") == 5000
    assert store.ttl_ms("b") == -1
    assert store.ttl_ms("nope") == -2


def test_plain_set_clears_ttl_but_keep_ttl_keeps_it(store, clock):
    store.set("k", "v", expire_at_ms=clock.now + 5000)
    store.set("k", "v2", keep_ttl=True)
    assert store.ttl_ms("k") == 5000
    store.set("k", "v3")
    assert store.ttl_ms("k") == -1


def test_set_expiry_and_persist(store, clock):
    store.set("k", "v")
    assert store.set_expiry("k", clock.now + 100) is True
    assert store.set_expiry("nope", clock.now + 100) is False
    assert store.persist("k") is True
    assert store.persist("k") is False
    assert store.ttl_ms("k") == -1


def test_delete_removes_expiry_too(store, clock):
    store.set("k", "v", expire_at_ms=clock.now + 100)
    store.delete("k")
    assert store._expires == {}


# ---------- active expiry ----------

def test_active_expire_cycle_cleans_untouched_keys(store, clock):
    for i in range(100):
        store.set(f"otp:{i}", "x", expire_at_ms=clock.now + 10)
    for i in range(5):
        store.set(f"perm:{i}", "y")
    clock.advance(11)
    rounds = 0
    while store.active_expire_cycle() != (0, 0):
        rounds += 1
    assert rounds == 5  # 100 keys / 20 per sample
    assert len(store._data) == 5 and store._expires == {}


def test_active_expire_cycle_keeps_live_keys(store, clock):
    for i in range(30):
        store.set(f"k{i}", "x", expire_at_ms=clock.now + 10_000)
    assert store.active_expire_cycle() == (20, 0)
    assert len(store._data) == 30


def test_active_expire_cycle_with_no_ttl_keys(store):
    store.set("a", "1")
    assert store.active_expire_cycle() == (0, 0)


# ---------- types ----------

@pytest.mark.parametrize("value, name", [
    ("text", "string"), (deque(["a"]), "list"), ({"f": "v"}, "hash"), ({"m"}, "set"),
])
def test_type_of(store, value, name):
    store.set("k", value)
    assert store.type_of("k") == name


def test_type_of_missing_and_expired(store, clock):
    assert store.type_of("nope") == "none"
    store.set("l", deque(["a"]), expire_at_ms=clock.now + 10)
    clock.advance(11)
    assert store.type_of("l") == "none"


def test_get_typed(store):
    lst = deque(["a"])
    store.set("l", lst)
    assert store.get_typed("l", deque) is lst  # the real object, not a copy
    assert store.get_typed("nope", deque) is None
    with pytest.raises(WrongTypeError):
        store.get_typed("l", str)


@pytest.mark.parametrize("empty", [deque(), {}, set()])
def test_delete_if_empty_removes_empty_collection_and_ttl(store, clock, empty):
    store.set("k", empty, expire_at_ms=clock.now + 5000)
    assert store.delete_if_empty("k") is True
    assert "k" not in store._data and "k" not in store._expires


@pytest.mark.parametrize("value", [deque(["a"]), {"f": "v"}, {"m"}, ""])
def test_delete_if_empty_keeps_non_empty_and_empty_string(store, value):
    store.set("k", value)
    assert store.delete_if_empty("k") is False
    assert "k" in store._data


def test_delete_if_empty_missing_key(store):
    assert store.delete_if_empty("nope") is False
