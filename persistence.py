"""AOF (Append-Only File) persistence: log every write command to disk."""

import os

import resp

FSYNC_POLICIES = ("always", "everysec", "no")


class AOF:
    def __init__(self, path, fsync_policy="everysec"):
        """fsync_policy:
        always   -> fsync after every command (safest, slowest)
        everysec -> the server calls flush_and_fsync() once per second
        no       -> never fsync ourselves; the OS writes to disk when it wants
        """
        if fsync_policy not in FSYNC_POLICIES:
            raise ValueError(
                f"invalid fsync_policy {fsync_policy!r}: "
                f"must be one of {', '.join(FSYNC_POLICIES)}"
            )
        self.path = path
        self.fsync_policy = fsync_policy
        # "ab" = binary APPEND: every write goes to the end of the file,
        # existing content is never overwritten. Creates the file if missing.
        self._file = open(path, "ab")

    def log(self, args):
        """Append one command, e.g. ["SET", "name", "Arti"], in RESP format.

        RESP is exactly what clients send, so replay can reuse read_command().
        """
        self._file.write(resp.array(args))  # only into Python's buffer so far
        if self.fsync_policy == "always":
            self.flush_and_fsync()

    def flush_and_fsync(self):
        """Push data all the way to the disk."""
        self._file.flush()  # Python buffer -> OS page cache
        os.fsync(self._file.fileno())  # OS page cache -> physical disk (waits)

    def close(self):
        """Flush, fsync and close. Safe to call more than once."""
        if self._file.closed:
            return
        self.flush_and_fsync()
        self._file.close()
