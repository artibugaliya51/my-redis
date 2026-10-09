"""Interactive test client for My Redis. Prints raw RESP replies with repr()."""

import shlex
import socket

import resp

HOST = "127.0.0.1"
PORT = 6380


def read_reply(f):
    """Read exactly ONE complete RESP reply from file f and return its raw bytes."""
    line = f.readline()
    if not line:
        raise ConnectionError("server closed the connection")
    prefix = line[:1]
    if prefix == b"$":
        length = int(line[1:])
        if length == -1:
            return line  # nil
        return line + f.read(length + 2)  # data + \r\n
    if prefix == b"*":
        count = int(line[1:])
        # Arrays contain other replies, so read each element recursively
        return line + b"".join(read_reply(f) for _ in range(max(count, 0)))
    return line  # +simple, -error, :integer are single lines


def main():
    try:
        sock = socket.create_connection((HOST, PORT))
    except ConnectionRefusedError:
        print(f"Could not connect to {HOST}:{PORT}. Is the server running? (python server.py)")
        return

    f = sock.makefile("rb")
    print(f"Connected to {HOST}:{PORT}. Type 'quit' or 'exit' to leave.")
    try:
        while True:
            line = input("my-redis> ").strip()
            if not line:
                continue
            if line.lower() in ("quit", "exit"):
                break
            try:
                parts = shlex.split(line)  # SET name "Arti Sharma" -> 3 parts
            except ValueError as e:
                print(f"(input error) {e}")  # e.g. unclosed quote
                continue

            sock.sendall(resp.array(parts))
            reply = read_reply(f)
            print(repr(reply))

            if parts[0].upper() == "SUBSCRIBE" and reply.startswith(b"*3\r\n$9\r\nsubscribe"):
                # Listen mode: the server now PUSHES messages to us at any time
                # (and SUBSCRIBE a b sends one reply per channel).
                print("Listening for messages... (Ctrl+C to quit)")
                while True:
                    print(repr(read_reply(f)))
    except (KeyboardInterrupt, EOFError):
        print()  # Ctrl+C / Ctrl+D: exit cleanly
    except ConnectionError as e:
        print(f"Disconnected: {e}")
    finally:
        f.close()
        sock.close()
        print("Bye!")


if __name__ == "__main__":
    main()
