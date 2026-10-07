"""My Redis server: asyncio TCP server speaking RESP2 on 127.0.0.1:6380."""

import asyncio
import traceback

import resp
from commands import CommandHandler
from storage import Storage

HOST = "127.0.0.1"
PORT = 6380

# ONE shared storage + handler for all clients. Safe without locks because
# asyncio runs everything on a single thread (same idea as real Redis).
storage = Storage()
handler = CommandHandler(storage)


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


async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"My Redis listening on {HOST}:{PORT} (Ctrl+C to stop)")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
