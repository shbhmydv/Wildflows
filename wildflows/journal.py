"""The journal: the run's durable spine.

An append-only in-memory list mirrored to <run_dir>/events.ndjson (one event per line,
fsynced on append). It is the ONLY durable run state resume and the dashboard consume.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import ValidationError

from wildflows.events import Event, parse_event


class Journal:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.run_dir / "events.ndjson"
        self._events: list[Event] = []

    def append(self, event: Event) -> int:
        """Append an event, assigning it the next seq; returns the seq."""
        seq = len(self._events)
        event.seq = seq
        self._events.append(event)
        line = event.model_dump_json() + "\n"
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
        return seq

    def events(self) -> list[Event]:
        return list(self._events)

    @classmethod
    def load(cls, run_dir: Path) -> "Journal":
        """Reconstruct a journal from its ndjson alone (the resume/dashboard entrypoint).

        Tolerates exactly ONE torn tail: a kill/power-loss during the final `write()`
        can leave the last line unterminated or malformed (`{"kind":"result"` with no
        newline). That last-line-only failure is dropped silently — the record never
        durably completed, so the next append safely reuses its seq. A malformed
        COMPLETE or MIDDLE record still raises: only the final line may be torn (B2).
        """
        j = cls(run_dir)
        if not j.path.exists():
            return j
        lines = [ln for ln in j.path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        last = len(lines) - 1
        for i, line in enumerate(lines):
            try:
                j._events.append(parse_event(json.loads(line)))
            except (json.JSONDecodeError, ValidationError):
                if i == last:
                    break  # torn final record — drop it, no durable log
                raise
        return j
