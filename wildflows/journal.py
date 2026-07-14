from __future__ import annotations
import json
import os
from pathlib import Path
from wildflows.events import Event, parse_event
from wildflows.projection import RunProjection
class JournalPoisonedError(RuntimeError):
    pass
class JournalExistsError(RuntimeError):
    pass
class JournalCompatibilityError(ValueError):
    pass
def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
class Journal:
    def __init__(self, run_dir: Path) -> None:
        self._initialize(run_dir)
        if self.path.exists() and self.path.stat().st_size:
            raise JournalExistsError(
                "existing nonempty journal must be continued with Journal.load"
            )
    def _initialize(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)
        missing: list[Path] = []
        cursor = self.run_dir
        while not cursor.exists():
            missing.append(cursor)
            cursor = cursor.parent
        self.run_dir.mkdir(parents=True, exist_ok=True)
        for created in reversed(missing):
            _fsync_directory(created.parent)
        self.path = self.run_dir / "events.ndjson"
        self._events: list[Event] = []
        self.projection = RunProjection()
        self._poisoned = False
    def append(self, event: Event) -> int:
        if self._poisoned:
            raise JournalPoisonedError(
                "journal append owner is poisoned; load a fresh Journal before appending"
            )
        seq = self._events[-1].seq + 1 if self._events else 0
        assigned = event.model_copy(update={"seq": seq})
        line = assigned.model_dump_json(exclude_computed_fields=True) + "\n"
        new_journal = not self.path.exists()
        try:
            with open(self.path, "a", encoding="utf-8") as stream:
                stream.write(line)
                stream.flush()
                os.fsync(stream.fileno())
            if new_journal:
                _fsync_directory(self.run_dir)
        except BaseException:
            self._poisoned = True
            raise
        event.seq = seq
        self._events.append(event)
        self.projection.apply(event)
        return seq
    def events(self) -> list[Event]:
        return list(self._events)
    @property
    def n_events(self) -> int:
        return len(self._events)
    @classmethod
    def load(cls, run_dir: Path) -> "Journal":
        journal = cls.__new__(cls)
        journal._initialize(run_dir)
        if not journal.path.exists():
            return journal
        raw = journal.path.read_bytes()
        complete_end = len(raw) if raw.endswith(b"\n") else raw.rfind(b"\n") + 1
        records = raw[:complete_end].split(b"\n")[:-1] if complete_end else []
        previous = -1
        for position, record in enumerate(records):
            data = json.loads(record.decode("utf-8"))
            event = parse_event(data)
            expected = previous + 1
            if event.seq != expected:
                raise JournalCompatibilityError(
                    f"journal seq {event.seq} at physical position {position} is not "
                    f"contiguous (expected {expected})"
                )
            previous = event.seq
            journal._events.append(event)
            journal.projection.apply(event)
        fd = os.open(journal.path, os.O_RDWR)
        try:
            if complete_end != len(raw):
                os.ftruncate(fd, complete_end)
            os.fsync(fd)
        finally:
            os.close(fd)
        _fsync_directory(journal.run_dir)
        return journal
