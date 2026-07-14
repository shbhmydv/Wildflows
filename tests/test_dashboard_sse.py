"""Focused coverage for v2 dashboard SSE journal tailing."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from wildflows.dashboard.sse import tail_events
from wildflows.events import RunFinished


def _record(seq: int, text: str) -> bytes:
    event = RunFinished(
        seq=seq,
        run_id="run",
        outcome="ok",
        root_head="a" * 40,
        text=text,
    )
    return (event.model_dump_json(exclude_computed_fields=True) + "\n").encode()


def _payload(message: str) -> dict[str, object]:
    return json.loads(message.split("data: ", maxsplit=1)[1])


def test_tail_emits_only_complete_validated_records(tmp_path: Path) -> None:
    path = tmp_path / "events.ndjson"
    first = _record(0, "first")
    second = _record(1, "second")
    path.write_bytes(first + second[:-1])

    async def scenario() -> tuple[str, str]:
        stream = tail_events(path, poll_interval=0.001, heartbeat_interval=1.0)
        try:
            initial = await asyncio.wait_for(anext(stream), timeout=1)
            pending = asyncio.create_task(anext(stream))
            await asyncio.sleep(0.01)
            assert not pending.done()
            with path.open("ab") as journal:
                journal.write(b"\n")
            appended = await asyncio.wait_for(pending, timeout=1)
            return initial, appended
        finally:
            await stream.aclose()

    initial, appended = asyncio.run(scenario())
    assert initial.startswith("id: 0\nevent: journal\n")
    assert _payload(initial)["text"] == "first"
    assert appended.startswith("id: 1\nevent: journal\n")
    assert _payload(appended)["text"] == "second"


def test_tail_resumes_strictly_after_sequence_without_poll_duplicates(
    tmp_path: Path,
) -> None:
    path = tmp_path / "events.ndjson"
    original = _record(0, "old") + _record(1, "current")
    next_record = _record(2, "next")
    path.write_bytes(original)

    async def scenario() -> tuple[str, str]:
        stream = tail_events(path, after=0, poll_interval=0.001, heartbeat_interval=1.0)
        try:
            current = await asyncio.wait_for(anext(stream), timeout=1)
            pending = asyncio.create_task(anext(stream))
            await asyncio.sleep(0.01)
            assert not pending.done()
            with path.open("ab") as journal:
                journal.write(next_record)
            following = await asyncio.wait_for(pending, timeout=1)
            return current, following
        finally:
            await stream.aclose()

    current, following = asyncio.run(scenario())
    assert _payload(current)["seq"] == 1
    assert _payload(following)["seq"] == 2
    assert path.read_bytes() == original + next_record


def test_tail_rejects_invalid_complete_record(tmp_path: Path) -> None:
    path = tmp_path / "events.ndjson"
    path.write_bytes(_record(0, "valid") + b'{"seq":1}\n')

    async def scenario() -> str:
        stream = tail_events(path, poll_interval=0.001, heartbeat_interval=1.0)
        try:
            assert await asyncio.wait_for(anext(stream), timeout=1)
            with pytest.raises(ValidationError):
                await anext(stream)
            return "validated"
        finally:
            await stream.aclose()

    assert asyncio.run(scenario()) == "validated"


def test_tail_heartbeats_for_absent_journal_and_allows_cancellation(tmp_path: Path) -> None:
    path = tmp_path / "events.ndjson"

    async def scenario() -> str:
        heartbeat_stream = tail_events(
            path, poll_interval=0.001, heartbeat_interval=0.0
        )
        try:
            heartbeat = await asyncio.wait_for(anext(heartbeat_stream), timeout=1)
        finally:
            await heartbeat_stream.aclose()

        stream = tail_events(path, poll_interval=1.0, heartbeat_interval=60.0)
        pending = asyncio.create_task(anext(stream))
        try:
            await asyncio.sleep(0.01)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
        finally:
            await stream.aclose()
        return heartbeat

    assert asyncio.run(scenario()) == ": heartbeat\n\n"
    assert not path.exists()
