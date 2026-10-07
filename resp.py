"""RESP2 protocol: parse client commands and encode server replies."""

import asyncio

CRLF = b"\r\n"


class RESPError(Exception):
    """Raised when the client sends data that is not valid RESP."""


# ---------- Parsing ----------

def _parse_int(line, prefix):
    """Parse the number after a prefix, e.g. b'*3\\r\\n' -> 3."""
    try:
        return int(line[1:].strip())
    except ValueError:
        raise RESPError(f"invalid {prefix} length: {line!r}") from None


def _decode(data):
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        raise RESPError("value is not valid UTF-8") from None


async def read_command(reader):
    """Read ONE command from the stream and return it as a list of str.

    Returns None if the client disconnected (EOF).
    Raises RESPError on malformed input.
    """
    try:
        while True:
            # readline() waits until a full line arrives, even if the bytes
            # come in many small TCP chunks.
            line = await reader.readline()
            if not line or not line.endswith(b"\n"):
                return None  # EOF (possibly in the middle of a line)
            if line.strip():
                break  # skip blank lines, like Redis does

        # Inline command: plain text like "PING\r\n" or "SET a 1\r\n"
        if not line.startswith(b"*"):
            return _decode(line).split()

        # Array of bulk strings: *<count>\r\n then <count> x ($<len>\r\n<data>\r\n)
        count = _parse_int(line, "array")
        if count < 0:
            raise RESPError("negative array length")

        parts = []
        for _ in range(count):
            header = await reader.readline()
            if not header or not header.endswith(b"\n"):
                return None
            if not header.startswith(b"$"):
                raise RESPError(f"expected '$', got {header[:1]!r}")
            length = _parse_int(header, "bulk")
            if length < 0:
                raise RESPError("negative bulk length")

            # Read exactly the data plus the trailing \r\n. Length-prefixed,
            # so the data itself may contain \r\n safely (binary-safe).
            data = await reader.readexactly(length + 2)
            if not data.endswith(CRLF):
                raise RESPError("bulk string not terminated by CRLF")
            parts.append(_decode(data[:-2]))
        return parts

    except asyncio.IncompleteReadError:
        return None  # client disconnected in the middle of a bulk string


# ---------- Encoding ----------

def simple_string(text):
    """+OK\\r\\n  (text must not contain \\r or \\n)"""
    return b"+" + text.encode("utf-8") + CRLF


def error(message):
    """-<message>\\r\\n

    The caller passes the full message including its prefix,
    e.g. error("ERR unknown command 'foo'") or error("WRONGTYPE ...").
    """
    return b"-" + message.encode("utf-8") + CRLF


def integer(n):
    """:5\\r\\n"""
    return b":" + str(n).encode() + CRLF


def bulk_string(text):
    """$4\\r\\nArti\\r\\n, or $-1\\r\\n (nil) for None.

    Length is the number of BYTES, not characters ("नमस्ते" = 6 chars, 18 bytes).
    """
    if text is None:
        return b"$-1" + CRLF
    data = text.encode("utf-8")
    return b"$" + str(len(data)).encode() + CRLF + data + CRLF


def array(items):
    """*<count>\\r\\n followed by each item as a bulk string."""
    out = [b"*" + str(len(items)).encode() + CRLF]
    out.extend(bulk_string(item) for item in items)
    return b"".join(out)
