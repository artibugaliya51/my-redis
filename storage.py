"""In-memory key-value storage for My Redis."""

import random
import time
from collections import deque
from fnmatch import fnmatchcase

# Python type of a stored value -> Redis type name
_TYPE_NAMES = {str: "string", deque: "list", dict: "hash", set: "set"}


class WrongTypeError(Exception):
    """A command was used on a key holding the wrong kind of value."""


def now_ms():
    """Current Unix time in milliseconds (wall clock, survives restarts)."""
    return int(time.time() * 1000)


class Storage:
    def __init__(self):
        # All data lives in one dict: key -> value. O(1) average lookup.
        self._data = {}
        # Only keys WITH a TTL: key -> absolute expiry time in ms.
        self._expires = {}

    def _expire_if_needed(self, key):
        """Lazy expiration: if key's time is up, delete it from BOTH dicts.

        Called at the start of every method that touches a key.
        Returns True if the key was expired (and is now gone).
        """
        expire_at = self._expires.get(key)
        if expire_at is not None and now_ms() > expire_at:
            del self._data[key]
            del self._expires[key]
            return True
        return False

    def set(self, key, value, expire_at_ms=None, keep_ttl=False):
        """Store value under key (overwrites any old value).

        Without expire_at_ms any old TTL is removed, like a plain Redis SET.
        keep_ttl=True updates only the value and leaves the TTL as it is
        (for commands that MODIFY a value, like INCR and APPEND).
        """
        self._data[key] = value
        if keep_ttl:
            return  # existing expiry (if any) stays untouched
        if expire_at_ms is None:
            self._expires.pop(key, None)
        else:
            self._expires[key] = expire_at_ms

    def get(self, key):
        """Return the value for key, or None if it does not exist."""
        self._expire_if_needed(key)
        return self._data.get(key)

    def type_of(self, key):
        """Redis type name of key: string / list / hash / set, or none."""
        self._expire_if_needed(key)
        if key not in self._data:
            return "none"
        return _TYPE_NAMES[type(self._data[key])]

    def get_typed(self, key, expected_type):
        """Return the value if it has expected_type, None if key is missing.

        Raises WrongTypeError if the key holds a different type.
        Returns the real object (not a copy), so commands can modify it in place.
        """
        self._expire_if_needed(key)
        if key not in self._data:
            return None
        value = self._data[key]
        if type(value) is not expected_type:
            raise WrongTypeError(key)
        return value

    def delete_if_empty(self, key):
        """Delete key (and its expiry) if it holds an EMPTY collection.

        Redis never keeps empty lists/hashes/sets. Call this after removing items.
        An empty string is a valid value, so it is never deleted here.
        Returns True if the key was deleted.
        """
        value = self._data.get(key)
        if isinstance(value, (deque, dict, set)) and len(value) == 0:
            del self._data[key]
            self._expires.pop(key, None)
            return True
        return False

    def delete(self, *keys):
        """Delete the given keys. Return how many actually existed."""
        deleted = 0
        for key in keys:
            self._expire_if_needed(key)  # an expired key doesn't count
            # Check membership first so missing keys don't raise KeyError
            if key in self._data:
                del self._data[key]
                self._expires.pop(key, None)
                deleted += 1
        return deleted

    def exists(self, *keys):
        """Return how many of the given keys exist.

        Duplicates are counted every time, like Redis: EXISTS a a -> 2.
        """
        count = 0
        for key in keys:
            self._expire_if_needed(key)
            if key in self._data:
                count += 1
        return count

    def keys(self, pattern):
        """Return all keys matching a glob pattern (case-sensitive).

        Supports *, ?, and [abc]. Scans every key, so it is O(N).
        """
        result = []
        # list(...) makes a copy: we may delete expired keys while looping,
        # and changing a dict during iteration raises RuntimeError.
        for key in list(self._data):
            if not self._expire_if_needed(key) and fnmatchcase(key, pattern):
                result.append(key)
        return result

    def dbsize(self):
        """Number of keys. Like Redis, expired keys not yet removed still count
        (counting only live keys would need an O(n) scan)."""
        return len(self._data)

    def keys_with_expiry(self):
        """Number of keys that have a TTL."""
        return len(self._expires)

    def flushall(self):
        """Delete ALL keys and ALL expiries."""
        self._data.clear()
        self._expires.clear()

    def snapshot(self):
        """List of (key, value, expire_at_ms or None) for every LIVE key.

        Expired keys are deleted on the way, not returned. Values are the real
        objects (not copies), so use the result before anything else changes.
        """
        result = []
        for key in list(self._data):  # copy: expired keys are deleted while looping
            if not self._expire_if_needed(key):
                result.append((key, self._data[key], self._expires.get(key)))
        return result

    def set_expiry(self, key, at_ms):
        """Set an absolute expiry (ms). True if the key exists, else False."""
        self._expire_if_needed(key)
        if key not in self._data:
            return False
        self._expires[key] = at_ms
        return True

    def ttl_ms(self, key):
        """Remaining ms; -1 if the key has no expiry; -2 if it doesn't exist."""
        self._expire_if_needed(key)
        if key not in self._data:
            return -2
        expire_at = self._expires.get(key)
        if expire_at is None:
            return -1
        return expire_at - now_ms()

    def persist(self, key):
        """Remove the key's expiry. True if an expiry was removed, else False."""
        self._expire_if_needed(key)
        if key in self._expires:
            del self._expires[key]
            return True
        return False

    def active_expire_cycle(self, sample_size=20):
        """Active expiration: check a RANDOM SAMPLE of keys that have a TTL.

        Cleans up expired keys nobody touches (lazy expiry alone would leak them).
        Returns (sampled, expired) so the caller can decide whether to repeat.
        """
        if not self._expires:
            return 0, 0
        # random.sample needs a sequence, so list() copies ALL TTL keys: O(n).
        # Real Redis picks from its hash table buckets directly, without a copy.
        sample = random.sample(list(self._expires), min(sample_size, len(self._expires)))
        expired = 0
        for key in sample:
            if self._expire_if_needed(key):  # deletes from both dicts
                expired += 1
        return len(sample), expired
