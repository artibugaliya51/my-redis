"""My Redis server: asyncio TCP server speaking RESP2 (default 127.0.0.1:6380).

Options (port, AOF file, fsync policy, log level...): python server.py --help
"""

import asyncio
import logging
import time

import resp
from client_state import Client
from commands import CommandHandler
from config import parse_args
from persistence import AOF, load_aof
from storage import Storage

# Explicit name: when run as "python server.py", __name__ would be "__main__"
logger = logging.getLogger("server")

# ONE shared storage + handler for all clients. Safe without locks because
# asyncio runs everything on a single thread (same idea as real Redis).
storage = Storage()
handler = CommandHandler(storage)

# Active expiry settings (same numbers as real Redis defaults)
EXPIRE_INTERVAL = 0.1        # run every 100 ms (Redis "hz 10")
EXPIRE_TIME_LIMIT_MS = 25    # max time per run, so clients are never blocked long
EXPIRE_REPEAT_RATIO = 0.25   # repeat immediately if > 25% of the sample was expired

AOF_FSYNC_INTERVAL = 1.0     # everysec: fsync the AOF once per second


async def handle_client(reader, writer):
    """Runs once per connected client: read -> execute -> reply, in a loop."""
    addr = writer.get_extra_info("peername")
    client = Client(writer, addr)  # per-connection state (used by Pub/Sub)
    handler.clients.add(client)  # for INFO connected_clients
    logger.info("Client connected: %s", addr)
    try:
        while True:
            # 1. Read one command. Bad input = protocol error -> close this client.
            try:
                args = await resp.read_command(reader)
            except (resp.RESPError, ValueError, asyncio.LimitOverrunError) as e:
                # ValueError: StreamReader line longer than its limit (64 KB)
                writer.write(resp.error(f"ERR Protocol error: {e}"))
                await writer.drain()
                break

            if args is None:
                break  # client disconnected
            if not args:
                continue  # empty command (e.g. "*0\r\n"): ignore

            # 2. Execute. A bug in a command must not kill the connection.
            try:
                reply = handler.execute(args, client)
            except Exception:
                logger.exception("Error while executing %s", args[0])
                reply = resp.error("ERR internal error")

            # 3. Send the reply. drain() waits if the client reads slowly
            # (backpressure), so our output buffer can't grow forever.
            writer.write(reply)
            await writer.drain()
            if client.quit_requested:
                break  # QUIT: +OK is sent, now close (finally does the cleanup)

    except ConnectionError:
        pass  # client vanished (reset / broken pipe): just stop
    finally:
        logger.info("Client disconnected: %s", addr)
        # Forget its subscriptions, or PubSub would keep a dead client forever
        handler.pubsub.unsubscribe_all(client)
        handler.clients.discard(client)
        writer.close()
        try:
            await writer.wait_closed()
        except ConnectionError:
            pass


async def active_expiry_loop():
    """Background task: delete expired keys that no client ever touches."""
    while True:
        # sleep() gives control back to the event loop, so clients keep being served
        await asyncio.sleep(EXPIRE_INTERVAL)
        try:
            # monotonic() (not time()) because we measure a DURATION: it never
            # jumps if the wall clock is changed.
            start = time.monotonic()
            while True:
                sampled, expired = storage.active_expire_cycle()
                if sampled == 0 or expired <= sampled * EXPIRE_REPEAT_RATIO:
                    break  # few expired keys left: wait for the next tick
                if (time.monotonic() - start) * 1000 >= EXPIRE_TIME_LIMIT_MS:
                    break  # time budget used up: continue next tick
        except Exception:
            logger.exception("Active expiry cycle failed")  # must not kill the task


async def aof_fsync_loop(aof):
    """Background task for the everysec policy: fsync the AOF once per second.

    log() already flushed every command to the OS, so this only has to push
    the OS page cache to the disk. A power cut loses at most ~1 second.
    """
    while True:
        await asyncio.sleep(AOF_FSYNC_INTERVAL)
        try:
            aof.flush_and_fsync()
        except Exception:
            logger.exception("AOF fsync failed")  # a disk error must not kill the task


async def main(config):
    aof = None
    if config.no_aof:
        logger.info("AOF disabled (--no-aof): data is kept in memory only")
    else:
        # 1. Rebuild the data from the AOF BEFORE opening it for appending
        #    (replay may truncate a torn last command).
        count = await load_aof(config.aof_file, handler)
        logger.info("Loaded %d commands from %s", count, config.aof_file)
        # 2. Created here, not at import time, so importing server.py (e.g. in
        #    tests) never creates an AOF file.
        aof = AOF(config.aof_file, config.appendfsync)
        handler.aof = aof
    try:
        server = await asyncio.start_server(handle_client, config.host, config.port)
        # Keep a reference: asyncio holds tasks only weakly, so an unreferenced
        # task could be garbage-collected while still running.
        expiry_task = asyncio.create_task(active_expiry_loop())
        if aof is not None and config.appendfsync == "everysec":
            fsync_task = asyncio.create_task(aof_fsync_loop(aof))
        logger.info("My Redis listening on %s:%d (Ctrl+C to stop)", config.host, config.port)
        async with server:
            await server.serve_forever()
    finally:
        if aof is not None:
            aof.close()  # flush + fsync whatever is still buffered


if __name__ == "__main__":
    config = parse_args()
    logging.basicConfig(
        level=config.log_level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    try:
        asyncio.run(main(config))
    except KeyboardInterrupt:
        logger.info("Server stopped.")
