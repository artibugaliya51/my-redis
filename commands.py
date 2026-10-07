"""Command execution: turns a parsed command into a RESP reply."""

import re

import resp
from storage import WrongTypeError, now_ms  # same clock as storage's expiry checks

INT64_MAX = 2**63 - 1

WRONGTYPE_MSG = "WRONGTYPE Operation against a key holding the wrong kind of value"

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
            "EXPIRE": self._expire,
            "PEXPIRE": self._pexpire,
            "TTL": self._ttl,
            "PTTL": self._pttl,
            "PERSIST": self._persist,
            "TYPE": self._type,
        }

    def execute(self, args):
        """Run one command, e.g. ["SET", "name", "Arti"]. Returns RESP bytes."""
        if not args:
            return resp.error("ERR empty command")

        name = args[0].upper()  # command names are case-insensitive
        handler = self.commands.get(name)
        if handler is None:
            return resp.error(f"ERR unknown command '{args[0]}'")
        try:
            return handler(args[1:])  # pass only the arguments, not the name
        except WrongTypeError:
            # One central place for every command's type errors
            return resp.error(WRONGTYPE_MSG)

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
            expire_at_ms = now_ms() + duration_ms
            if expire_at_ms > INT64_MAX:  # Redis also rejects overflow
                return resp.error("ERR invalid expire time in 'set' command")

        # 3. Everything is valid: now write. expire_at_ms=None clears any old TTL.
        self.storage.set(key, value, expire_at_ms=expire_at_ms)
        return resp.simple_string("OK")

    def _get(self, args):
        if len(args) != 1:
            return _wrong_args("get")
        # get_typed raises WrongTypeError for lists/hashes/sets
        return resp.bulk_string(self.storage.get_typed(args[0], str))

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

    # ---------- Expiry commands ----------

    def _generic_expire(self, args, name, unit_ms):
        """Shared logic for EXPIRE (unit_ms=1000) and PEXPIRE (unit_ms=1)."""
        if len(args) != 2:
            return _wrong_args(name)  # NX/XX/GT/LT options not supported
        key = args[0]
        number = _parse_int(args[1])
        if number is None:
            return resp.error("ERR value is not an integer or out of range")

        expire_at_ms = now_ms() + number * unit_ms  # relative -> ABSOLUTE
        if not -INT64_MAX - 1 <= expire_at_ms <= INT64_MAX:
            return resp.error(f"ERR invalid expire time in '{name}' command")

        if self.storage.exists(key) == 0:
            return resp.integer(0)
        if number <= 0:
            # Time already passed: delete now instead of keeping a dead key
            # in memory until someone touches it (same as real Redis).
            self.storage.delete(key)
            return resp.integer(1)
        # int(): storage returns True/False, but RESP needs :1 / :0
        return resp.integer(int(self.storage.set_expiry(key, expire_at_ms)))

    def _expire(self, args):
        return self._generic_expire(args, "expire", 1000)

    def _pexpire(self, args):
        return self._generic_expire(args, "pexpire", 1)

    def _ttl(self, args):
        if len(args) != 1:
            return _wrong_args("ttl")
        ms = self.storage.ttl_ms(args[0])
        if ms < 0:
            return resp.integer(ms)  # -1 no expiry, -2 missing key
        return resp.integer((ms + 500) // 1000)  # round to nearest second, like Redis

    def _pttl(self, args):
        if len(args) != 1:
            return _wrong_args("pttl")
        return resp.integer(self.storage.ttl_ms(args[0]))

    def _persist(self, args):
        if len(args) != 1:
            return _wrong_args("persist")
        return resp.integer(int(self.storage.persist(args[0])))

    def _type(self, args):
        if len(args) != 1:
            return _wrong_args("type")
        return resp.simple_string(self.storage.type_of(args[0]))
