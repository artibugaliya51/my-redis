"""AOF (Append-Only File) persistence: log every write command to disk."""

import asyncio
import os
from collections import deque

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
        self.path = os.fspath(path)  # accept str or pathlib.Path, store as str
        self.fsync_policy = fsync_policy
        # "ab" = binary APPEND: every write goes to the end of the file,
        # existing content is never overwritten. Creates the file if missing.
        self._file = open(path, "ab")

    def log(self, args):
        """Append one command, e.g. ["SET", "name", "Arti"], in RESP format.

        RESP is exactly what clients send, so replay can reuse read_command().
        """
        self._file.write(resp.array(args))  # into Python's buffer
        # Flush on EVERY command (all policies): Python buffer -> OS page cache.
        # The page cache belongs to the kernel, so the data now survives a
        # crash of OUR process (kill -9). Only fsync survives a power cut.
        self._file.flush()
        if self.fsync_policy == "always":
            os.fsync(self._file.fileno())  # OS page cache -> disk, every command
        # everysec: the server's aof_fsync_loop() fsyncs once per second
        # no: never fsync; the OS writes to disk when it wants

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

    def rewrite(self, storage):
        """Compact the AOF: replace the history with the minimal commands that
        rebuild the CURRENT data. Returns the number of commands written.

        Runs synchronously (no await), so no client write can happen in the
        middle: the snapshot and the file swap are atomic for all clients.
        """
        tmp_path = self.path + ".rewrite.tmp"  # same folder -> os.replace is atomic
        count = 0
        try:
            with open(tmp_path, "wb") as tmp:
                for cmd in rewrite_commands(storage):
                    tmp.write(resp.array(cmd))
                    count += 1
                tmp.flush()
                os.fsync(tmp.fileno())  # new file fully on disk BEFORE the swap
            # Atomic swap: the name now points to the complete new file.
            # Until this line the old AOF was never touched.
            os.replace(tmp_path, self.path)
        except BaseException:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)  # old AOF is still intact and still open
            raise

        # Our open handle still points to the OLD (now unlinked) file:
        # reopen so new writes are appended to the NEW file.
        self._file.close()  # log() already flushed every command
        self._file = open(self.path, "ab")
        # fsync the folder so the rename itself survives a power cut
        dir_fd = os.open(os.path.dirname(os.path.abspath(self.path)), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        return count


def rewrite_commands(storage):
    """Yield the minimal commands that rebuild storage's current data."""
    for key, value, expire_at in storage.snapshot():  # expired keys skipped
        if isinstance(value, str):
            cmd = ["SET", key, value]
            if expire_at is not None:
                cmd += ["PXAT", str(expire_at)]  # TTL in the same command
            yield cmd
            continue
        if isinstance(value, deque):
            yield ["RPUSH", key, *value]  # RPUSH keeps the original order
        elif isinstance(value, dict):
            flat = [item for pair in value.items() for item in pair]  # f1, v1, f2, v2
            yield ["HSET", key, *flat]
        elif isinstance(value, set):
            yield ["SADD", key, *value]
        if expire_at is not None:
            yield ["PEXPIREAT", key, str(expire_at)]


async def load_aof(path, handler):
    """Replay the AOF at startup: run every logged command again.

    Returns the number of commands replayed. A missing file means a fresh start.
    An incomplete LAST command (crash during a write) is skipped and cut off.
    Corruption in the MIDDLE raises RESPError: better to refuse to start
    than to silently lose data.
    """
    if not os.path.exists(path):
        return 0
    with open(path, "rb") as f:
        data = f.read()

    # Reuse our network parser: the file is just a stream of RESP commands
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()

    # Never log replayed commands again (the file would grow on every restart
    # and INCRs would be applied twice on the next load)
    saved_aof, handler.aof = handler.aof, None
    count = 0
    offset = 0  # byte position where the last COMPLETE command ends
    try:
        while True:
            try:
                args = await resp.read_command(reader)
            except resp.RESPError as e:
                raise resp.RESPError(f"corrupt AOF {path!r} at byte {offset}: {e}") from None
            if args is None:
                break  # end of file (or an incomplete last command)

            # Our AOF is always written with resp.array(), so re-encoding gives
            # the exact bytes of this command in the file.
            encoded = resp.array(args)
            if data[offset:offset + len(encoded)] != encoded:
                raise resp.RESPError(f"AOF {path!r} has an unexpected format at byte {offset}")
            offset += len(encoded)

            reply = handler.execute(args)
            if reply.startswith(b"-"):
                print(f"[AOF] warning: command {args} failed during replay: {reply.decode().strip()}")
            count += 1
    finally:
        handler.aof = saved_aof

    if offset < len(data):
        # Torn write: cut it off, otherwise new appends would land after the
        # garbage and shift every command boundary on the next replay.
        print(f"[AOF] warning: incomplete last command ({len(data) - offset} bytes) "
              f"removed; truncating {path!r} to {offset} bytes")
        os.truncate(path, offset)
    return count
