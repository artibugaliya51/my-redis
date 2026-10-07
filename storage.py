"""In-memory key-value storage for My Redis."""

import time
from fnmatch import fnmatchcase


def _now_ms():
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
        if expire_at is not None and _now_ms() > expire_at:
            del self._data[key]
            del self._expires[key]
            return True
        return False

    def set(self, key, value, expire_at_ms=None):
        """Store value under key (overwrites any old value).

        Without expire_at_ms any old TTL is removed, like a plain Redis SET.
        """
        self._data[key] = value
        if expire_at_ms is None:
            self._expires.pop(key, None)
        else:
            self._expires[key] = expire_at_ms

    def get(self, key):
        """Return the value for key, or None if it does not exist."""
        self._expire_if_needed(key)
        return self._data.get(key)

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
        return expire_at - _now_ms()

    def persist(self, key):
        """Remove the key's expiry. True if an expiry was removed, else False."""
        self._expire_if_needed(key)
        if key in self._expires:
            del self._expires[key]
            return True
        return False
