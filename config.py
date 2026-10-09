"""Command-line configuration for the My Redis server."""

import argparse

LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR"]


def _port(text):
    """argparse type: an integer port between 1 and 65535."""
    try:
        port = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid port {text!r}: not a number") from None
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError(f"invalid port {port}: must be 1-65535")
    return port


def parse_args(argv=None):
    """Parse server options. argv=None reads sys.argv; argv=[] gives all defaults."""
    parser = argparse.ArgumentParser(
        prog="server.py",
        description="My Redis: a simplified Redis clone (asyncio + RESP2).",
    )
    parser.add_argument("--host", default="127.0.0.1",
                        help="address to listen on (default: %(default)s)")
    parser.add_argument("--port", type=_port, default=6380,
                        help="TCP port to listen on (default: %(default)s)")
    parser.add_argument("--aof-file", default="appendonly.aof",
                        help="path of the append-only file (default: %(default)s)")
    parser.add_argument("--appendfsync", choices=["always", "everysec", "no"], default="everysec",
                        help="when to fsync the AOF (default: %(default)s)")
    parser.add_argument("--no-aof", action="store_true",
                        help="disable persistence: no AOF is read or written")
    parser.add_argument("--log-level", type=str.upper, choices=LOG_LEVELS, default="INFO",
                        help="logging level (default: %(default)s)")
    return parser.parse_args(argv)
