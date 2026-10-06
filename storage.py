"""In-memory key-value storage for My Redis."""

from fnmatch import fnmatchcase


class Storage:
    def __init__(self):
        # All data lives in one dict: key -> value. O(1) average lookup.
        self._data = {}

    def set(self, key, value):
        """Store value under key (overwrites any old value)."""
        self._data[key] = value

    def get(self, key):
        """Return the value for key, or None if it does not exist."""
        return self._data.get(key)

    def delete(self, *keys):
        """Delete the given keys. Return how many actually existed."""
        deleted = 0
        for key in keys:
            # Check membership first so missing keys don't raise KeyError
            if key in self._data:
                del self._data[key]
                deleted += 1
        return deleted

    def exists(self, *keys):
        """Return how many of the given keys exist.

        Duplicates are counted every time, like Redis: EXISTS a a -> 2.
        """
        return sum(1 for key in keys if key in self._data)

    def keys(self, pattern):
        """Return all keys matching a glob pattern (case-sensitive).

        Supports *, ?, and [abc]. Scans every key, so it is O(N).
        """
        return [key for key in self._data if fnmatchcase(key, pattern)]
