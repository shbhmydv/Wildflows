"""The journal: the run's durable spine AND the single append owner.

An append-only in-memory list mirrored to <run_dir>/events.ndjson (one event per line,
fsynced on append). It is the ONLY durable run state resume and the dashboard consume.
`append` is the one place that assigns a seq, fsyncs, and updates the live
`RunProjection`; `load` replays the ndjson through the same `projection.apply`, so a
running projection and a reloaded one are bit-identical. Parallel dispatch (step 3)
serializes through this owner (DESIGN §6).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import ValidationError

from wildflows.events import Event, parse_event
from wildflows.projection import RunProjection


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


class Journal:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.run_dir / "events.ndjson"
        self._events: list[Event] = []
        self.projection = RunProjection()

    def append(self, event: Event) -> int:
        """Append an event: assign the next seq, fsync, fold into the projection.

        The next seq is one past the last event's (load enforces strictly-increasing
        seqs, so the tail holds the max). Deriving from the max — not the list length —
        keeps seqs collision-free even after a gap-truncated resume, so the strict
        ordering `load` checks always holds.
        """
        seq = self._events[-1].seq + 1 if self._events else 0
        event.seq = seq
        self._events.append(event)
        line = event.model_dump_json() + "\n"
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
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

        Tolerates exactly ONE torn tail: a kill/power-loss during the final `write()`
        can leave the last record unterminated (no trailing `\\n`) or a partial multibyte
        UTF-8 sequence. Only a record that lacks its terminating newline may be dropped,
        and only if it fails to parse. A newline-TERMINATED record durably completed its
        write, so if it is malformed the journal still raises — a complete invalid line
        is corruption, not a torn tail. The file is read as
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
        raws: list[dict[str, object]] = []
        prev_seq = -1
        for i, rec in enumerate(records):
            try:
                data = json.loads(rec.decode("utf-8"))
                event = parse_event(data)
            except (json.JSONDecodeError, ValidationError, UnicodeDecodeError):
                if i == last and not final_terminated:
                    break  # unterminated torn final record — drop it, no durable log
                raise
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
        return j
