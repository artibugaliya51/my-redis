"""AOF (Append-Only File) persistence: log every write command to disk."""

import asyncio
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
