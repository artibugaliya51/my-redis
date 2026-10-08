"""My Redis server: asyncio TCP server speaking RESP2 on 127.0.0.1:6380."""

import asyncio
import time
import traceback

import resp
from commands import CommandHandler
from persistence import AOF, load_aof
from storage import Storage

HOST = "127.0.0.1"
PORT = 6380
AOF_PATH = "appendonly.aof"
AOF_FSYNC = "everysec"

# ONE shared storage + handler for all clients. Safe without locks because
# asyncio runs everything on a single thread (same idea as real Redis).
storage = Storage()
handler = CommandHandler(storage)

# Active expiry settings (same numbers as real Redis defaults)
EXPIRE_INTERVAL = 0.1        # run every 100 ms (Redis "hz 10")
EXPIRE_TIME_LIMIT_MS = 25    # max time per run, so clients are never blocked long
EXPIRE_REPEAT_RATIO = 0.25   # repeat immediately if > 25% of the sample was expired


async def handle_client(reader, writer):
    """Runs once per connected client: read -> execute -> reply, in a loop."""
    addr = writer.get_extra_info("peername")
    print(f"[+] Client connected: {addr}")
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
                reply = handler.execute(args)
            except Exception:
                traceback.print_exc()
                reply = resp.error("ERR internal error")

            # 3. Send the reply. drain() waits if the client reads slowly
            # (backpressure), so our output buffer can't grow forever.
            writer.write(reply)
            await writer.drain()

    except ConnectionError:
        pass  # client vanished (reset / broken pipe): just stop
    finally:
        print(f"[-] Client disconnected: {addr}")
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
            traceback.print_exc()  # a bug here must not silently kill the task


async def main():
    # 1. Rebuild the data from the AOF BEFORE opening it for appending
    #    (replay may truncate a torn last command).
    count = await load_aof(AOF_PATH, handler)
    print(f"Loaded {count} commands from {AOF_PATH}")
    # 2. Created here, not at import time, so importing server.py (e.g. in
    #    tests) never creates an AOF file.
    aof = AOF(AOF_PATH, AOF_FSYNC)
    handler.aof = aof
    try:
        server = await asyncio.start_server(handle_client, HOST, PORT)
        # Keep a reference: asyncio holds tasks only weakly, so an unreferenced
        # task could be garbage-collected while still running.
        expiry_task = asyncio.create_task(active_expiry_loop())
        print(f"My Redis listening on {HOST}:{PORT} (Ctrl+C to stop)")
        async with server:
            await server.serve_forever()
    finally:
        aof.close()  # flush + fsync whatever is still buffered


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
