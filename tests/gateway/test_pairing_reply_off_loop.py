"""Unauthorized-DM pairing replies must not block the gateway event loop on the pairing lock.

``PairingStore`` edits hold a cross-process flock (the dashboard and the ``hermes pairing`` CLI
mutate the same files). Waiting on it inline in an async handler stalls every chat the gateway
serves for up to the lock timeout, then raises a ``TimeoutError`` the handler never handled.
"""

import asyncio
import threading
import time
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.pairing import PairingStore
from gateway.run import GatewayRunner
from gateway.run_inbound_unauthorized import pairing_code_reply
from gateway.session import SessionSource


def _runner(store, adapter):
    runner = object.__new__(GatewayRunner)
    runner._pairing_store_for = lambda _source: store
    runner._delivery_adapter_for = lambda _source: adapter
    runner.config = None
    return runner


def _source(user_id="stranger"):
    return SessionSource(platform=Platform.TELEGRAM, chat_id="1", chat_type="dm", user_id=user_id)


async def _max_loop_stall(handler, hold_s=0.6):
    """Run ``handler`` while another holder keeps the pairing lock; return the loop's longest stall."""
    # The reply text comes from the i18n catalog, whose first load per process (~0.15 s here,
    # over 0.3 s on a loaded CI runner) runs on the loop after the lock is released. Load it
    # first so only the lock wait is measured.
    pairing_code_reply("telegram", "WARMUP")
    locked = threading.Event()
    release = threading.Event()

    def hold_lock():
        with PairingStore()._locked():
            locked.set()
            release.wait(hold_s)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert locked.wait(5)
    threading.Timer(hold_s, release.set).start()
    stalls = []

    async def tick():
        last = time.monotonic()
        while not done.is_set():
            await asyncio.sleep(0.01)
            now = time.monotonic()
            stalls.append(now - last)
            last = now

    done = asyncio.Event()
    ticker = asyncio.create_task(tick())
    await handler()
    done.set()
    await ticker
    holder.join(5)
    return max(stalls, default=float("inf"))  # no tick at all: the handler held the loop throughout


@pytest.mark.asyncio
@pytest.mark.parametrize("handler", ["_hm_offer_pairing_code", "_hm_send_unauthorized_decline"])
async def test_waiting_on_the_pairing_lock_does_not_stall_the_event_loop(handler):
    adapter = AsyncMock()
    runner = _runner(PairingStore(), adapter)

    stall = await _max_loop_stall(lambda: getattr(runner, handler)(_source()))

    assert stall < 0.3
    adapter.send.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("handler, method", [
    ("_hm_offer_pairing_code", "generate_code"),
    ("_hm_send_unauthorized_decline", "record_decline"),
])
async def test_a_pairing_lock_timeout_stays_silent(monkeypatch, handler, method):
    store = PairingStore()

    def timed_out(*_args, **_kwargs):
        raise TimeoutError("Timed out waiting for the pairing store lock")

    monkeypatch.setattr(store, method, timed_out)
    adapter = AsyncMock()

    await getattr(_runner(store, adapter), handler)(_source())

    adapter.send.assert_not_awaited()
