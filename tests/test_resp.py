"""Unit tests for resp.py: parsing (read_command) and encoders."""

import asyncio

import pytest

import resp
from resp import RESPError, read_command


def parse(*chunks, eof=True):
    """Feed raw bytes into a StreamReader and parse ONE command."""

    async def run():
        reader = asyncio.StreamReader()
        for chunk in chunks:
            reader.feed_data(chunk)
        if eof:
            reader.feed_eof()
        return await read_command(reader)

    return asyncio.run(run())


SET_CMD = b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nArti\r\n"


# ---------- read_command ----------

def test_array_command():
    assert parse(SET_CMD) == ["SET", "name", "Arti"]


def test_command_split_into_small_pieces_arriving_later():
    async def run():
        reader = asyncio.StreamReader()

        async def feeder():
            # Splits inside "SET" and between \r and \n
            for piece in [b"*3\r\n$3\r\nSE", b"T\r\n$4\r\nname\r", b"\n$4\r\nArti\r\n"]:
                await asyncio.sleep(0.01)
                reader.feed_data(piece)

        asyncio.create_task(feeder())
        return await read_command(reader)

    assert asyncio.run(run()) == ["SET", "name", "Arti"]


def test_one_byte_at_a_time():
    assert parse(*[bytes([b]) for b in SET_CMD]) == ["SET", "name", "Arti"]


def test_pipelined_commands_are_read_one_by_one():
    async def run():
        reader = asyncio.StreamReader()
        reader.feed_data(SET_CMD + b"*1\r\n$4\r\nPING\r\n")
        reader.feed_eof()
        return [await read_command(reader) for _ in range(3)]

    assert asyncio.run(run()) == [["SET", "name", "Arti"], ["PING"], None]


def test_inline_command():
    assert parse(b"PING\r\n") == ["PING"]


def test_inline_command_extra_spaces():
    assert parse(b"SET  city   Pune\r\n") == ["SET", "city", "Pune"]


def test_blank_lines_are_skipped():
    assert parse(b"\r\n\r\nPING\r\n") == ["PING"]


def test_non_english_value():
    data = "नमस्ते".encode()
    raw = b"*2\r\n$3\r\nGET\r\n$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"
    assert parse(raw) == ["GET", "नमस्ते"]


def test_bulk_string_may_contain_crlf():
    # Length-prefixed, so \r\n inside the data is fine (binary-safe)
    assert parse(b"*1\r\n$4\r\na\r\nb\r\n") == ["a\r\nb"]


def test_empty_array_gives_empty_list():
    assert parse(b"*0\r\n") == []


@pytest.mark.parametrize("raw", [
    b"",                                   # clean disconnect
    b"PIN",                                # EOF in the middle of a line
    b"*2\r\n$3\r\nGET\r\n$4\r\nna",        # EOF in the middle of a bulk string
    b"*2\r\n$3\r\nGET\r\n",                # EOF before the second element
])
def test_disconnect_returns_none(raw):
    assert parse(raw) is None


@pytest.mark.parametrize("raw", [
    b"*abc\r\n",                           # non-number array length
    b"*-1\r\n",                            # negative array length
    b"*1\r\n:5\r\n",                       # bad prefix inside array
    b"*1\r\n$x\r\n",                       # non-number bulk length
    b"*1\r\n$-5\r\n",                      # negative bulk length
    b"*1\r\n$3\r\nabcde\r\n",              # data longer than declared
    b"*1\r\n$2\r\n\xff\xfe\r\n",           # invalid UTF-8
])
def test_invalid_input_raises_resperror(raw):
    with pytest.raises(RESPError):
        parse(raw)


# ---------- encoders ----------

def test_simple_string():
    assert resp.simple_string("OK") == b"+OK\r\n"


def test_error_uses_full_message():
    assert resp.error("ERR bad") == b"-ERR bad\r\n"
    assert resp.error("WRONGTYPE x") == b"-WRONGTYPE x\r\n"


@pytest.mark.parametrize("n, expected", [(5, b":5\r\n"), (0, b":0\r\n"), (-2, b":-2\r\n")])
def test_integer(n, expected):
    assert resp.integer(n) == expected


def test_bulk_string():
    assert resp.bulk_string("Arti") == b"$4\r\nArti\r\n"


def test_bulk_string_empty_vs_nil():
    assert resp.bulk_string("") == b"$0\r\n\r\n"
    assert resp.bulk_string(None) == b"$-1\r\n"


def test_bulk_string_length_is_in_bytes():
    encoded = resp.bulk_string("नमस्ते")  # 6 characters, 18 bytes
    assert encoded.startswith(b"$18\r\n")
    assert encoded == b"$18\r\n" + "नमस्ते".encode() + b"\r\n"


def test_array():
    assert resp.array(["a", "b"]) == b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"


def test_array_with_nil_and_empty():
    assert resp.array(["x", None]) == b"*2\r\n$1\r\nx\r\n$-1\r\n"
    assert resp.array([]) == b"*0\r\n"


def test_encode_then_parse_roundtrip():
    assert parse(resp.array(["SET", "k", "नमस्ते दुनिया"])) == ["SET", "k", "नमस्ते दुनिया"]
