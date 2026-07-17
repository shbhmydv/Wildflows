"""Read-only Server-Sent Event tailing for v2 journals."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
import json
import os
from pathlib import Path
import time
from typing import cast

from wildflows.dashboard.journal import DashboardJournalRecord, decode_dashboard_record


DEFAULT_POLL_INTERVAL = 0.2
DEFAULT_HEARTBEAT_INTERVAL = 15.0


def _parse_record(record: bytes, position: int) -> DashboardJournalRecord:
    decoded = json.loads(record)
    if not isinstance(decoded, dict):
        raise ValueError("journal record is not an event object")
    return decode_dashboard_record(cast(dict[str, object], decoded), position)


def _message(record: DashboardJournalRecord) -> str:
    payload = json.dumps(record.raw, separators=(",", ":"), ensure_ascii=False)
    return f"id: {record.position}\nevent: journal\ndata: {payload}\n\n"


async def tail_events(
    path: Path,
    after: int = -1,
    *,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
) -> AsyncGenerator[str, None]:
    """Yield validated, complete journal records as SSE messages.

    Cancellation is deliberately allowed to propagate to ``StreamingResponse``.
    The journal is only ever opened for reading.
    """
    if type(after) is not int:
        raise TypeError("after must be an integer sequence number")
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive")
    if heartbeat_interval < 0:
        raise ValueError("heartbeat_interval must not be negative")

    offset = 0
    pending = b""
    expected = 0
    last = after
    file_identity: tuple[int, int] | None = None
    last_activity = time.monotonic()

    while True:
        try:
            with path.open("rb") as journal:
                status = os.fstat(journal.fileno())
                identity = (status.st_dev, status.st_ino)
                if file_identity != identity or status.st_size < offset:
                    offset = 0
                    pending = b""
                    expected = 0
                journal.seek(offset)
                chunk = journal.read()
                file_identity = identity
        except FileNotFoundError:
            offset = 0
            pending = b""
            expected = 0
            file_identity = None
            chunk = b""

        if chunk:
            offset += len(chunk)
            pending += chunk
            complete_end = pending.rfind(b"\n")
            if complete_end >= 0:
                records = pending[:complete_end].split(b"\n")
                pending = pending[complete_end + 1 :]
                for record_bytes in records:
                    record = _parse_record(record_bytes, expected)
                    expected += 1
                    if record.position > last:
                        last = record.position
                        yield _message(record)
                        last_activity = time.monotonic()

        now = time.monotonic()
        if now - last_activity >= heartbeat_interval:
            yield ": heartbeat\n\n"
            last_activity = now
        await asyncio.sleep(poll_interval)
