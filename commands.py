"""Command execution: turns a parsed command into a RESP reply."""

import resp


def _wrong_args(name):
    return resp.error(f"ERR wrong number of arguments for '{name}' command")


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
        if len(args) < 2:
            return _wrong_args("set")
        if len(args) > 2:
            return resp.error("ERR syntax error")  # options like EX not supported yet
        self.storage.set(args[0], args[1])
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
