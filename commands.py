"""Command execution: turns a parsed command into a RESP reply."""

import re
from collections import deque
from itertools import islice

import resp
from storage import WrongTypeError, now_ms  # same clock as storage's expiry checks

INT64_MAX = 2**63 - 1

WRONGTYPE_MSG = "WRONGTYPE Operation against a key holding the wrong kind of value"

# Commands that change data. Only these are written to the AOF.
WRITE_COMMANDS = {
    "SET", "DEL", "EXPIRE", "PEXPIRE", "PEXPIREAT", "PERSIST",
    "INCR", "DECR", "INCRBY", "DECRBY", "APPEND", "MSET",
    "LPUSH", "RPUSH", "LPOP", "RPOP", "HSET", "HDEL", "SADD", "SREM",
}

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
    def __init__(self, storage, aof=None):
        self.storage = storage
        self.aof = aof  # None = no persistence (e.g. in unit tests)
        # A handler sets this when the AOF must get a DIFFERENT command than
        # the one the client sent (e.g. EXPIRE -> PEXPIREAT with absolute time).
        self._aof_args = None
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
            "PEXPIREAT": self._pexpireat,
            "TTL": self._ttl,
            "PTTL": self._pttl,
            "PERSIST": self._persist,
            "TYPE": self._type,
            "INCR": self._incr,
            "DECR": self._decr,
            "INCRBY": self._incrby,
            "DECRBY": self._decrby,
            "APPEND": self._append,
            "STRLEN": self._strlen,
            "MSET": self._mset,
            "MGET": self._mget,
            "LPUSH": self._lpush,
            "RPUSH": self._rpush,
            "LPOP": self._lpop,
            "RPOP": self._rpop,
            "LLEN": self._llen,
            "LRANGE": self._lrange,
            "HSET": self._hset,
            "HGET": self._hget,
            "HDEL": self._hdel,
            "HGETALL": self._hgetall,
            "HEXISTS": self._hexists,
            "HLEN": self._hlen,
            "SADD": self._sadd,
            "SREM": self._srem,
            "SMEMBERS": self._smembers,
            "SISMEMBER": self._sismember,
            "SCARD": self._scard,
            "SINTER": self._sinter,
            "BGREWRITEAOF": self._bgrewriteaof,
        }

    def execute(self, args):
        """Run one command, e.g. ["SET", "name", "Arti"]. Returns RESP bytes."""
        if not args:
            return resp.error("ERR empty command")

        name = args[0].upper()  # command names are case-insensitive
        handler = self.commands.get(name)
        if handler is None:
            return resp.error(f"ERR unknown command '{args[0]}'")
        self._aof_args = None
        try:
            reply = handler(args[1:])  # pass only the arguments, not the name
        except WrongTypeError:
            # One central place for every command's type errors
            return resp.error(WRONGTYPE_MSG)

        # Log successful writes AFTER memory was changed (errors start with "-")
        if self.aof is not None and name in WRITE_COMMANDS and not reply.startswith(b"-"):
            self.aof.log(self._aof_args or [name] + args[1:])
        return reply

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
        """SET key value [EX seconds | PX milliseconds | PXAT unix-time-ms]"""
        if len(args) < 2:
            return _wrong_args("set")
        key, value, options = args[0], args[1], args[2:]

        # 1. Parse options (syntax only): at most one of EX/PX/PXAT, each
        #    followed by a number. The number itself is checked in step 2.
        unit = None  # "EX", "PX" or "PXAT"
        raw_number = None
        i = 0
        while i < len(options):
            opt = options[i].upper()  # options are case-insensitive
            if opt in ("EX", "PX", "PXAT") and unit is None and i + 1 < len(options):
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
            if unit == "PXAT":
                expire_at_ms = number  # already absolute
            else:
                duration_ms = number * 1000 if unit == "EX" else number
                expire_at_ms = now_ms() + duration_ms
            if expire_at_ms > INT64_MAX:  # Redis also rejects overflow
                return resp.error("ERR invalid expire time in 'set' command")

        # 3. Everything is valid: now write. expire_at_ms=None clears any old TTL.
        self.storage.set(key, value, expire_at_ms=expire_at_ms)
        if expire_at_ms is not None:
            # Log the SAME absolute time we stored, so replay gives the same TTL
            self._aof_args = ["SET", key, value, "PXAT", str(expire_at_ms)]
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

    def _generic_expire(self, args, name, unit_ms, absolute=False):
        """Shared logic for EXPIRE (unit_ms=1000), PEXPIRE (unit_ms=1)
        and PEXPIREAT (absolute=True: the number is already a unix time in ms)."""
        if len(args) != 2:
            return _wrong_args(name)  # NX/XX/GT/LT options not supported
        key = args[0]
        number = _parse_int(args[1])
        if number is None:
            return resp.error("ERR value is not an integer or out of range")

        now = now_ms()  # read the clock ONCE
        expire_at_ms = number if absolute else now + number * unit_ms  # -> ABSOLUTE
        if not -INT64_MAX - 1 <= expire_at_ms <= INT64_MAX:
            return resp.error(f"ERR invalid expire time in '{name}' command")

        # Always log the absolute form, so replay later gives the same expiry
        self._aof_args = ["PEXPIREAT", key, str(expire_at_ms)]
        if self.storage.exists(key) == 0:
            return resp.integer(0)
        if expire_at_ms <= now:
            # Time already passed: delete now instead of keeping a dead key
            # in memory until someone touches it (same as real Redis).
            self.storage.delete(key)
            self._aof_args = ["DEL", key]
            return resp.integer(1)
        # int(): storage returns True/False, but RESP needs :1 / :0
        return resp.integer(int(self.storage.set_expiry(key, expire_at_ms)))

    def _expire(self, args):
        return self._generic_expire(args, "expire", 1000)

    def _pexpire(self, args):
        return self._generic_expire(args, "pexpire", 1)

    def _pexpireat(self, args):
        return self._generic_expire(args, "pexpireat", 1, absolute=True)

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

    # ---------- String commands ----------

    def _incr_by(self, key, delta):
        """Shared logic for INCR / DECR / INCRBY / DECRBY."""
        value = self.storage.get_typed(key, str)  # WRONGTYPE for non-strings
        current = 0 if value is None else _parse_int(value)  # missing key = 0
        if current is None:
            return resp.error("ERR value is not an integer or out of range")
        result = current + delta
        if not -INT64_MAX - 1 <= result <= INT64_MAX:
            # Checked BEFORE writing, so the old value stays unchanged
            return resp.error("ERR increment or decrement would overflow")
        # Numbers are stored as strings. keep_ttl: INCR must not clear the TTL
        self.storage.set(key, str(result), keep_ttl=True)
        return resp.integer(result)

    def _incr(self, args):
        if len(args) != 1:
            return _wrong_args("incr")
        return self._incr_by(args[0], 1)

    def _decr(self, args):
        if len(args) != 1:
            return _wrong_args("decr")
        return self._incr_by(args[0], -1)

    def _incrby(self, args):
        if len(args) != 2:
            return _wrong_args("incrby")
        delta = _parse_int(args[1])
        if delta is None:
            return resp.error("ERR value is not an integer or out of range")
        return self._incr_by(args[0], delta)

    def _decrby(self, args):
        if len(args) != 2:
            return _wrong_args("decrby")
        delta = _parse_int(args[1])
        if delta is None:
            return resp.error("ERR value is not an integer or out of range")
        if delta == -INT64_MAX - 1:
            # -(INT64_MIN) does not fit in 64 bits; Redis rejects it too
            return resp.error("ERR decrement would overflow")
        return self._incr_by(args[0], -delta)

    def _append(self, args):
        if len(args) != 2:
            return _wrong_args("append")
        key, suffix = args
        value = self.storage.get_typed(key, str)
        new_value = (value or "") + suffix  # missing key: start from ""
        self.storage.set(key, new_value, keep_ttl=True)
        return resp.integer(len(new_value))

    def _strlen(self, args):
        if len(args) != 1:
            return _wrong_args("strlen")
        value = self.storage.get_typed(args[0], str)
        # Characters, not bytes (real Redis counts bytes: "नमस्ते" = 18)
        return resp.integer(0 if value is None else len(value))

    def _mset(self, args):
        if not args or len(args) % 2 != 0:
            return _wrong_args("mset")
        # Plain SET for every pair (clears TTL). No await inside, so atomic.
        for i in range(0, len(args), 2):
            self.storage.set(args[i], args[i + 1])
        return resp.simple_string("OK")

    def _mget(self, args):
        if not args:
            return _wrong_args("mget")
        values = []
        for key in args:
            try:
                values.append(self.storage.get_typed(key, str))
            except WrongTypeError:
                values.append(None)  # MGET never errors: non-strings are nil
        return resp.array(values)

    # ---------- List commands (value = collections.deque) ----------

    def _push(self, args, name, left):
        """Shared logic for LPUSH (left=True) and RPUSH (left=False)."""
        if len(args) < 2:
            return _wrong_args(name)
        key, values = args[0], args[1:]
        lst = self.storage.get_typed(key, deque)  # WRONGTYPE for non-lists
        if lst is None:
            lst = deque()
            self.storage.set(key, lst)  # new key: no TTL to keep
        # Modify the deque IN PLACE: storage holds this same object, and
        # _expires is never touched, so an existing TTL is kept.
        for v in values:
            if left:
                lst.appendleft(v)  # one by one: LPUSH q a b c -> c b a
            else:
                lst.append(v)
        return resp.integer(len(lst))

    def _lpush(self, args):
        return self._push(args, "lpush", left=True)

    def _rpush(self, args):
        return self._push(args, "rpush", left=False)

    def _pop(self, args, name, left):
        """Shared logic for LPOP (left=True) and RPOP (left=False)."""
        if len(args) != 1:
            return _wrong_args(name)  # COUNT argument not supported
        key = args[0]
        lst = self.storage.get_typed(key, deque)
        if lst is None:
            return resp.bulk_string(None)
        value = lst.popleft() if left else lst.pop()  # both O(1) on a deque
        self.storage.delete_if_empty(key)  # last item gone -> key + TTL gone
        return resp.bulk_string(value)

    def _lpop(self, args):
        return self._pop(args, "lpop", left=True)

    def _rpop(self, args):
        return self._pop(args, "rpop", left=False)

    def _llen(self, args):
        if len(args) != 1:
            return _wrong_args("llen")
        lst = self.storage.get_typed(args[0], deque)
        return resp.integer(0 if lst is None else len(lst))

    def _lrange(self, args):
        if len(args) != 3:
            return _wrong_args("lrange")
        key = args[0]
        start, stop = _parse_int(args[1]), _parse_int(args[2])
        if start is None or stop is None:
            return resp.error("ERR value is not an integer or out of range")
        lst = self.storage.get_typed(key, deque)
        if lst is None:
            return resp.array([])

        n = len(lst)
        # Negative index counts from the end: -1 -> n-1 (last), -2 -> n-2
        if start < 0:
            start += n
        if stop < 0:
            stop += n
        start = max(start, 0)  # e.g. -100 on a short list -> 0
        if start > stop or start >= n:
            return resp.array([])
        stop = min(stop, n - 1)  # stop beyond the end -> last element
        # deque has no slicing; islice walks from the head: O(start + count).
        # stop is INCLUSIVE in Redis, so +1 for Python's exclusive end.
        return resp.array(list(islice(lst, start, stop + 1)))

    # ---------- Hash commands (value = dict: field -> value) ----------

    def _hset(self, args):
        # key + at least one field/value pair, and pairs must be complete
        if len(args) < 3 or len(args) % 2 == 0:
            return _wrong_args("hset")
        key = args[0]
        h = self.storage.get_typed(key, dict)  # WRONGTYPE for non-hashes
        if h is None:
            h = {}
            self.storage.set(key, h)  # new key: no TTL to keep
        added = 0
        for i in range(1, len(args), 2):
            field, value = args[i], args[i + 1]
            if field not in h:
                added += 1  # only NEW fields count, updates don't
            h[field] = value  # in place: existing TTL is kept
        return resp.integer(added)

    def _hget(self, args):
        if len(args) != 2:
            return _wrong_args("hget")
        h = self.storage.get_typed(args[0], dict)
        return resp.bulk_string(None if h is None else h.get(args[1]))

    def _hdel(self, args):
        if len(args) < 2:
            return _wrong_args("hdel")
        key = args[0]
        h = self.storage.get_typed(key, dict)
        if h is None:
            return resp.integer(0)
        removed = 0
        for field in args[1:]:
            if field in h:
                del h[field]
                removed += 1
        self.storage.delete_if_empty(key)  # last field gone -> key + TTL gone
        return resp.integer(removed)

    def _hgetall(self, args):
        if len(args) != 1:
            return _wrong_args("hgetall")
        h = self.storage.get_typed(args[0], dict)
        if h is None:
            return resp.array([])
        # RESP2 has no map type, so flatten: [f1, v1, f2, v2, ...]
        flat = []
        for field, value in h.items():
            flat.extend((field, value))
        return resp.array(flat)

    def _hexists(self, args):
        if len(args) != 2:
            return _wrong_args("hexists")
        h = self.storage.get_typed(args[0], dict)
        return resp.integer(int(h is not None and args[1] in h))

    def _hlen(self, args):
        if len(args) != 1:
            return _wrong_args("hlen")
        h = self.storage.get_typed(args[0], dict)
        return resp.integer(0 if h is None else len(h))

    # ---------- Set commands (value = Python set) ----------

    def _sadd(self, args):
        if len(args) < 2:
            return _wrong_args("sadd")
        key = args[0]
        s = self.storage.get_typed(key, set)  # WRONGTYPE for non-sets
        if s is None:
            s = set()
            self.storage.set(key, s)  # new key: no TTL to keep
        added = 0
        for member in args[1:]:
            if member not in s:
                s.add(member)  # in place: existing TTL is kept
                added += 1  # duplicates don't count
        return resp.integer(added)

    def _srem(self, args):
        if len(args) < 2:
            return _wrong_args("srem")
        key = args[0]
        s = self.storage.get_typed(key, set)
        if s is None:
            return resp.integer(0)
        removed = 0
        for member in args[1:]:
            if member in s:
                s.remove(member)
                removed += 1
        self.storage.delete_if_empty(key)  # last member gone -> key + TTL gone
        return resp.integer(removed)

    def _smembers(self, args):
        if len(args) != 1:
            return _wrong_args("smembers")
        s = self.storage.get_typed(args[0], set)
        # Order is NOT guaranteed (hash order), same as real Redis
        return resp.array([] if s is None else list(s))

    def _sismember(self, args):
        if len(args) != 2:
            return _wrong_args("sismember")
        s = self.storage.get_typed(args[0], set)
        return resp.integer(int(s is not None and args[1] in s))  # O(1)

    def _scard(self, args):
        if len(args) != 1:
            return _wrong_args("scard")
        s = self.storage.get_typed(args[0], set)
        return resp.integer(0 if s is None else len(s))

    def _sinter(self, args):
        if not args:
            return _wrong_args("sinter")
        # 1. Look up ALL keys first: any wrong type -> WRONGTYPE (like Redis)
        sets = [self.storage.get_typed(key, set) for key in args]
        # 2. A missing key is an empty set, so the intersection is empty
        if any(s is None for s in sets):
            return resp.array([])
        # 3. Walk the SMALLEST set and keep members found in every other set.
        #    O(N * M): N = size of smallest set, M = number of sets.
        #    sorted() returns a new list; the stored sets are never modified.
        smallest, *others = sorted(sets, key=len)
        result = [m for m in smallest if all(m in other for other in others)]
        return resp.array(result)

    # ---------- Persistence commands ----------

    def _bgrewriteaof(self, args):
        if args:
            return _wrong_args("bgrewriteaof")
        if self.aof is None:
            return resp.error("ERR AOF is not enabled")
        # NOTE: unlike real Redis this runs synchronously (blocks the server),
        # but that also means no write can sneak in during the rewrite.
        self.aof.rewrite(self.storage)
        return resp.simple_string("Background append only file rewriting started")
