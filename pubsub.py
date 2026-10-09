"""Pub/Sub: which clients are subscribed to which channels."""


class PubSub:
    def __init__(self):
        # channel -> set of Client objects. Answers "who gets a PUBLISH?" in O(1).
        # The reverse (client -> channels) lives in client.channels.
        # Both are only changed here, so they always stay in sync.
        self.channels = {}

    def subscribe(self, client, channel):
        """Subscribe client to channel. Returns the client's channel count."""
        self.channels.setdefault(channel, set()).add(client)
        client.channels.add(channel)
        return len(client.channels)

    def unsubscribe(self, client, channel):
        """Unsubscribe client from channel. Returns the client's channel count."""
        subscribers = self.channels.get(channel)
        if subscribers is not None:
            subscribers.discard(client)
            if not subscribers:
                del self.channels[channel]  # last subscriber left: no empty sets
        client.channels.discard(channel)
        return len(client.channels)

    def unsubscribe_all(self, client):
        """Remove client from every channel (e.g. on disconnect).

        Returns the channels it was subscribed to, in sorted order.
        """
        channels = sorted(client.channels)
        for channel in channels:
            self.unsubscribe(client, channel)
        return channels
