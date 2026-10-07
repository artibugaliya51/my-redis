"""Command execution: turns a parsed command into a RESP reply."""

import re

import resp
from storage import _now_ms  # same clock as storage's expiry checks

INT64_MAX = 2**63 - 1

# Strict integer like Redis: optional '-', no spaces, '+', '_' or leading zeros.
_INT_RE = re.compile(r"-?(0|[1-9][0-9]*)")


def _wrong_args(name):
    return resp.error(f"ERR wrong number of arguments for '{name}' command")


def _parse_int(text):
    """Return text as int, or None if not a valid 64-bit integer."""
    if not _INT_RE.fullmatch(text):
        return None  # e.g. "abc", "1.5", " 5", "+5"
    n = int(text)
    if not -INT64_MAX - 1 <= n <= INT64_MAX:
        return None  # out of range for a 64-bit signed integer
    return n


class CommandHandler:
    def __init__(self, storage):
        self.storage = storage
        # Dispatch table: command name -> handler method.
        # O(1) lookup instead of a long if/elif chain.
        self.commands = {
            "PING": self._ping,
            "ECHO": self._echo,
            "SET": self._set,
            "GET": self._get,
            "DEL": self._del,
            "EXISTS": self._exists,
            "KEYS": self._keys,
        }

    def execute(self, args):
        """Run one command, e.g. ["SET", "name", "Arti"]. Returns RESP bytes."""
        if not args:
            return resp.error("ERR empty command")

        name = args[0].upper()  # command names are case-insensitive
        handler = self.commands.get(name)
        if handler is None:
            return resp.error(f"ERR unknown command '{args[0]}'")
        return handler(args[1:])  # pass only the arguments, not the name

    # ---------- Handlers (args = arguments without the command name) ----------

    def _ping(self, args):
        if len(args) > 1:
            return _wrong_args("ping")
        if args:
            return resp.bulk_string(args[0])
        return resp.simple_string("PONG")

    def _echo(self, args):
        if len(args) != 1:
            return _wrong_args("echo")
        return resp.bulk_string(args[0])

    def _set(self, args):
        """SET key value [EX seconds | PX milliseconds]"""
        if len(args) < 2:
            return _wrong_args("set")
        key, value, options = args[0], args[1], args[2:]

        # 1. Parse options (syntax only): at most one EX/PX, each followed
        #    by a number. The number itself is checked in step 2.
        unit = None  # "EX" or "PX"
        raw_number = None
        i = 0
        while i < len(options):
            opt = options[i].upper()  # options are case-insensitive
            if opt in ("EX", "PX") and unit is None and i + 1 < len(options):
                unit, raw_number = opt, options[i + 1]
                i += 2
            else:
                return resp.error("ERR syntax error")  # unknown/duplicate/missing number

        # 2. Validate the number and convert to an ABSOLUTE expiry time in ms.
        expire_at_ms = None
        if unit is not None:
            number = _parse_int(raw_number)
            if number is None:
                return resp.error("ERR value is not an integer or out of range")
            if number <= 0:
                return resp.error("ERR invalid expire time in 'set' command")
            duration_ms = number * 1000 if unit == "EX" else number
            expire_at_ms = _now_ms() + duration_ms
            if expire_at_ms > INT64_MAX:  # Redis also rejects overflow
                return resp.error("ERR invalid expire time in 'set' command")

        # 3. Everything is valid: now write. expire_at_ms=None clears any old TTL.
        self.storage.set(key, value, expire_at_ms=expire_at_ms)
        return resp.simple_string("OK")

    def _get(self, args):
        if len(args) != 1:
            return _wrong_args("get")
        return resp.bulk_string(self.storage.get(args[0]))

    def _del(self, args):
        if not args:
            return _wrong_args("del")
        return resp.integer(self.storage.delete(*args))

    def _exists(self, args):
        if not args:
            return _wrong_args("exists")
        return resp.integer(self.storage.exists(*args))

    def _keys(self, args):
        if len(args) != 1:
            return _wrong_args("keys")
        return resp.array(self.storage.keys(args[0]))
