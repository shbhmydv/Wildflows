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
        can leave the last record unterminated (no trailing `\\n`) or a partial multibyte
        UTF-8 sequence. Only a record that lacks its terminating newline may be dropped,
        and only if it fails to parse. A newline-TERMINATED record durably completed its
        write, so if it is malformed the journal still raises — a complete invalid line
        is corruption, not a torn tail (pass-2 B2 / SHOULD-FIX 1). The file is read as
        RAW BYTES so a mid-UTF-8 unterminated tail is recoverable rather than a decode
        crash outside the handler.
        """
        j = cls(run_dir)
        if not j.path.exists():
            return j
        raw = j.path.read_bytes()
        if not raw:
            return j
        # A trailing newline means the physical final record fully completed its write;
        # its absence marks a possibly-torn tail we may drop on a parse/decode failure.
        final_terminated = raw.endswith(b"\n")
        records = [r for r in raw.split(b"\n") if r.strip()]
        last = len(records) - 1
        for i, rec in enumerate(records):
            try:
                j._events.append(parse_event(json.loads(rec.decode("utf-8"))))
            except (json.JSONDecodeError, ValidationError, UnicodeDecodeError):
                if i == last and not final_terminated:
                    break  # unterminated torn final record — drop it, no durable log
                raise
        return j
