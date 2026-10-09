"""Per-connection state: one Client object for every connected client."""


class Client:
    def __init__(self, writer, addr):
        self.writer = writer  # StreamWriter: lets the server push data to THIS client
        self.addr = addr  # (host, port), useful for logs
        # Pub/Sub channels this client is subscribed to (filled in Phase 5)
        self.channels = set()
        # Set by QUIT: the server closes the connection after sending +OK
        self.quit_requested = False

    def __repr__(self):
        return f"Client({self.addr}, channels={sorted(self.channels)})"
