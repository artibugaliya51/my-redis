"""Shared pytest fixtures."""

import pytest

import commands
import storage
from commands import CommandHandler
from storage import Storage


class FakeClock:
    """Controllable time in ms, so expiry tests need no real sleeping."""

    def __init__(self, start_ms=1_000_000):
        self.now = start_ms

    def __call__(self):
        return self.now

    def advance(self, ms):
        self.now += ms


@pytest.fixture
def clock(monkeypatch):
    fake = FakeClock()
    # Patch BOTH modules: commands.py did "from storage import now_ms",
    # so it holds its own reference to the real function.
    monkeypatch.setattr(storage, "now_ms", fake)
    monkeypatch.setattr(commands, "now_ms", fake)
    return fake


@pytest.fixture
def store():
    """A fresh, empty Storage for every test."""
    return Storage()


@pytest.fixture
def handler(store):
    return CommandHandler(store)
