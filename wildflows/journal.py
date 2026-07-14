"""The journal: the run's durable spine AND the single append owner.

An append-only in-memory list mirrored to <run_dir>/events.ndjson (one event per line,
fsynced on append). It is the sole lifecycle/projection state resume and the dashboard
consume; effect recovery also validates lease/intent/recovery certificates under run_dir.
`append` is the one place that assigns a seq, fsyncs, and updates the live
`RunProjection`; `load` replays the ndjson through the same `projection.apply`, so a
running projection and a reloaded one are bit-identical. Parallel dispatch (step 3)
serializes through this owner (DESIGN §6).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from wildflows.events import Event, parse_event
from wildflows.projection import RunProjection


class JournalPoisonedError(RuntimeError):
    """This append owner saw uncertain durability and must be freshly loaded."""


class JournalExistsError(RuntimeError):
    """Creation cannot continue an existing journal; use ``Journal.load``."""


class JournalCompatibilityError(ValueError):
    """A journal the current engine refuses to resume/replay.

    Journals are PRE-V1 and unstable (DESIGN §6). A COMPLETE legacy history still folds
    via the compatibility readers, but an INTERRUPTED legacy tail (pre-provenance shapes
    after the last boundary with no terminal result) cannot be provenance-recovered — the
    operator must complete or archive the run with the engine version that wrote it.
    Non-contiguous / physically-misordered sequence streams are refused for the same
    reason: the projection floors trust `seq`.
    """


# Raw-line markers of a PRE-V1 (legacy) event shape — each is a field the current engine
# would emit differently. Their presence in an INTERRUPTED tail means the run cannot be
# resumed by this engine (no durable attempt provenance to key recovery off).
def _is_legacy_shape(raw: dict[str, object]) -> bool:
    kind = raw.get("kind")
    if kind == "dispatched":
        return "pre_head" not in raw  # provenance anchor added in v1 (hand-8)
    if kind == "integrated":
        return "commits" not in raw   # single-commit `commit`/`paths` shape
    if kind == "loop_iter":
        return any(f in raw for f in ("body_text", "body_files", "body_exit_code"))
    if kind == "result":
        if "outcome" not in raw:
            return True  # pre-collapse `ok`-only result
        if "receipt_required" not in raw:
            return True  # cannot distinguish a torn allow-empty commit from no effect
        # An EFFECTFUL leaf result (non-empty declared `files`) with no `post_head`
        # completion certificate is an interrupted pre-v1 tail, NOT a durable success
        # (hand-10, PRINCIPLE A). post_head is sampled on every modern effectful leaf result,
        # so its absence/None over a non-empty `files` cannot be reconstructed (no range end)
        # and must never be accepted as a durable receipt-less effect certificate. A LOOP's
        # final result (identified by a non-None `loop_status`) legitimately carries the body
        # artifact `files` with no post_head — its durability rides on the body iterations'
        # own integrated events, so it is exempt.
        if raw.get("files") and raw.get("post_head") is None and raw.get("loop_status") is None:
            return True
    return False


def _refuse_legacy_interrupted_tail(raws: list[dict[str, object]]) -> None:
    """Refuse to resume a legacy INTERRUPTED tail. A complete history ends at its closing
    boundary (empty tail); any records AFTER the last boundary are an in-flight tail. If
    that tail carries a pre-v1 shape it cannot be provenance-recovered — raise so the
    operator finishes/archives the run with the old engine (hand-8, LEGACY-COMPLETION-TAIL).
    """
    last_boundary = max(
        (i for i, r in enumerate(raws) if r.get("kind") == "boundary"), default=-1
    )
    for raw in raws[last_boundary + 1:]:
        if _is_legacy_shape(raw):
            raise JournalCompatibilityError(
                "interrupted legacy journal tail (pre-v1 event shape after the last "
                "boundary): complete or archive this run with the engine version that "
                "wrote it before resuming"
            )


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
        # Persist every newly-created directory entry from the first existing ancestor
        # down to run_dir before any workspace lease can open.
        for created in reversed(missing):
            _fsync_directory(created.parent)
        self.path = self.run_dir / "events.ndjson"
        self._events: list[Event] = []
        self.projection = RunProjection()
        self._poisoned = False

    def append(self, event: Event) -> int:
        """Append an event: assign the next seq, fsync, fold into the projection.

        The next seq is one past the last event's (load enforces strictly-increasing
        seqs, so the tail holds the max). Deriving from the max — not the list length —
        keeps seqs collision-free even after a gap-truncated resume, so the strict
        ordering `load` checks always holds.
        """
        if self._poisoned:
            raise JournalPoisonedError(
                "journal append owner is poisoned; load a fresh Journal before appending"
            )
        seq = self._events[-1].seq + 1 if self._events else 0
        assigned = event.model_copy(update={"seq": seq})
        line = assigned.model_dump_json() + "\n"
        new_journal = not self.path.exists()
        try:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(line)
                fh.flush()
                os.fsync(fh.fileno())
            if new_journal:
                _fsync_directory(self.run_dir)
        except BaseException:
            # A write/fsync exception cannot prove whether the line reached disk.  Keep
            # memory at its durable prefix and forbid this owner from guessing; load()
            # is the only authority that may classify the physical tail.
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
        """Reconstruct a journal from its ndjson alone (the resume/dashboard entrypoint).

        An unterminated final record is uncertain even when its bytes form valid JSON, so
        it is dropped and the file is durably truncated to the last complete newline before
        this fresh owner returns. A malformed newline-terminated record remains corruption.
        Raw-byte parsing also makes a torn multibyte UTF-8 tail recoverable.
        """
        j = cls.__new__(cls)
        j._initialize(run_dir)
        if not j.path.exists():
            return j
        raw = j.path.read_bytes()
        complete_end = len(raw) if raw.endswith(b"\n") else raw.rfind(b"\n") + 1
        records = raw[:complete_end].split(b"\n")[:-1] if complete_end else []
        raws: list[dict[str, object]] = []
        prev_seq = -1
        for i, rec in enumerate(records):
            data = json.loads(rec.decode("utf-8"))
            event = parse_event(data)
            # Recorded seq must be EXACTLY contiguous with physical record order: each seq
            # is previous+1, starting at 0; negatives rejected (hand-9, SEQ+RECEIPT). Terra
            # proved a torn TAIL cannot create a middle gap — after dropping the final
            # partial record, the next append reuses `last_seq + 1` — so any gap can only
            # hide a deleted/missing MIDDLE durability event. A reordered or duplicated
            # stream (the parallel-writer corruption N2 warns of) is refused for the same
            # reason: the projection floors trust `seq`.
            expected = prev_seq + 1
            if event.seq != expected:
                raise JournalCompatibilityError(
                    f"journal seq {event.seq} at physical position {i} is not contiguous "
                    f"(expected {expected}): a gapped, reordered or duplicated stream"
                )
            prev_seq = event.seq
            j._events.append(event)
            raws.append(data)
            j.projection.apply(event)
        _refuse_legacy_interrupted_tail(raws)
        # A fresh owner cannot know whether a complete physical tail came from an append
        # whose file or first-file directory fsync failed.  Establish durability for every
        # accepted existing file (including empty files) before returning ownership.
        fd = os.open(j.path, os.O_RDWR)
        try:
            if complete_end != len(raw):
                os.ftruncate(fd, complete_end)
            os.fsync(fd)
        finally:
            os.close(fd)
        _fsync_directory(j.run_dir)
        return j
