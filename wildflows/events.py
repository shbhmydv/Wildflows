"""The single event vocabulary (design invariant 2).

Every primitive execution is one event in ONE vocabulary; resume = replay this log
against the expression tree. `seq` is assigned by the journal on append. All events
share a header (run_id/epoch/node_id/kind + ts/seq) and discriminate on `kind`.
"""
from __future__ import annotations

import time
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, computed_field, model_validator

from wildflows.result import CommitReceipt, reconcile_outcome


class _Header(BaseModel):
    seq: int = -1  # assigned by the journal on append
    ts: float = Field(default_factory=time.time)
    run_id: str
    epoch: int
    node_id: str


class Boundary(_Header):
    """An epoch opened or closed: the re-entry + durability point."""

    kind: Literal["boundary"] = "boundary"
    phase: Literal["opened", "closed"]
    expr: dict[str, Any] | None = None  # the admitted tree (on opened)
    reason: str | None = None  # e.g. deadline / budget / done (on closed)


class Dispatched(_Header):
    """A do/combine/inplace/setup started.

    `pre_head` is the workdir HEAD at the moment the attempt opened (None for an unborn
    repo). It is the DURABLE ATTEMPT PROVENANCE and the reset target on recovery. Two
    disjoint recovery boundaries key off it (hand-9/hand-10):
    - a torn OK `result` (post_head stamped) whose `integrated` was lost reconstructs the
      receipt from EXACTLY `pre_head..post_head` — never `..HEAD`, so a post-crash operator
      commit is out of range by construction;
    - a DISPATCHED-ONLY tail (no result, no completion certificate) is NOT reconstructed:
      its tip is QUARANTINED to a ref and the branch reset to `pre_head`, then the node
      re-runs (PRINCIPLE A — no committed work is destroyed, no unreceipted commit is
      blessed as success).
    A legacy dispatched line lacks this key entirely — the field's absence marks a pre-v1
    journal that cannot be provenance-resumed (see `journal.JournalCompatibilityError`).
    """

    kind: Literal["dispatched"] = "dispatched"
    rig: str | None = None
    task: str | None = None
    cmd: str | None = None
    workdir: str | None = None
    pre_head: str | None = None
    # New engines publish a durable lease before dispatch.  The explicit marker lets
    # recovery distinguish a missing modern record (corruption: fail closed) from a
    # pre-hand-12 journal that needs the conservative no-sweep compatibility path.
    lease_required: bool = False
    # A planned non-empty modern inplace publishes its intent before this dispatch.
    # Recovery therefore treats absence as corruption; false also covers do/legacy and
    # inplace attempts that failed planning before any write could occur.
    intent_required: bool = False


class ResultEvent(_Header):
    """A primitive produced output.

    `outcome` is the single terminal-status source; `ok` is a derived convenience
    (`outcome == "ok"`). A legacy/caller `ok` is folded into `outcome` by the shared
    reconciler (the ok/outcome collapse + old-journal compatibility reader, item 3).
    """

    kind: Literal["result"] = "result"
    text: str = ""
    files: list[str] = Field(default_factory=list)
    exit_code: int | None = None
    # The SECOND durable boundary (hand-9, PROVENANCE-RANGE): the workdir HEAD at the
    # moment the rig returned and this result was recorded (None on a pre-v1 line or an
    # unborn repo). Resume reconstructs a torn receipt from EXACTLY `pre_head..post_head`,
    # never `..HEAD`, so an operator commit made after process death is outside the range
    # by construction and never misattributed to this attempt.
    post_head: str | None = None
    # A loop's final result reuses this event but carries the last integrated
    # iteration's body artifact in text/files; the convergence/cap disposition rides
    # in this SEPARATE field so a downstream `combine` consumes the artifact, never the
    # status prose. None for every non-loop result. Journal-only (the dashboard
    # reads it); replay's Result reconstruction ignores it.
    loop_status: str | None = None
    outcome: Literal["ok", "failed", "busy"] = "ok"
    # A durable failed result must never LIE that the workspace was cleanly handled. When a
    # cleanup/rollback/capture operation fails (PRINCIPLE A), the engine marks the failed
    # result `workspace_unclean=True` and HALTS. Replay retains this marker and retries the
    # durable recovery action; a surviving live effect is never papered over as handled.
    workspace_unclean: bool = False
    # How resume proceeds AFTER checked cleanup clears an unclean halt. `fail` means the
    # attempt had already failed and may close once cleanup succeeds; `retry` means it died
    # without a completion certificate and must dispatch again. None on ordinary results
    # and legacy unclean markers (which fail closed because their disposition is unknown).
    recovery_action: Literal["fail", "retry"] | None = None
    # A commit is an effect even when it changes zero paths (`git commit --allow-empty`).
    # Such a result still requires the following Integrated receipt before it is durable.
    receipt_required: bool = False

    @model_validator(mode="before")
    @classmethod
    def _collapse_ok(cls, data: Any) -> Any:
        return reconcile_outcome(data)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ok(self) -> bool:
        return self.outcome == "ok"


class Integrated(_Header):
    """The core applied+committed a result (mediation proof; core-only).

    Carries EVERY commit the core attributes to this node attempt (a rig may legitimately
    author several commits in `pre..post`), each with its own changed paths, so the full
    range is verifiable (item 3/defect 4). `commit` (the final sha) and `paths` (the
    order-preserving union) are derived. A legacy single-commit line (`commit`+`paths`,
    no `commits`) is migrated by the before-validator — the old-journal compatibility
    reader for this event.
    """

    kind: Literal["integrated"] = "integrated"
    commits: list[CommitReceipt] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _migrate_single_commit(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "commits" in data and data["commits"]:
            # New shape (a dump also carries derived `commit`/`paths`); reject a
            # CONTRADICTORY hand-authored/corrupt line where `commit` disagrees with the
            # last of `commits` (malformed-receipt hardening, hand-8).
            legacy_commit = data.get("commit")
            if legacy_commit:
                last = data["commits"][-1]
                last_sha = last["sha"] if isinstance(last, dict) else getattr(last, "sha", None)
                if last_sha is not None and legacy_commit != last_sha:
                    raise ValueError("integrated: `commit` contradicts `commits`")
            return data
        if "commit" in data:
            out = {k: v for k, v in data.items() if k not in ("commit", "paths")}
            out["commits"] = [{"sha": data["commit"], "paths": data.get("paths", [])}]
            return out
        return data

    @model_validator(mode="after")
    def _require_nonempty_commits(self) -> "Integrated":
        # An `integrated` is the mediation PROOF; an empty receipt proves nothing and
        # would falsely mark an effect durable (malformed-receipt hardening, hand-8). The
        # core only ever emits `integrated` for a non-empty receipt, so this rejects only
        # corrupt/legacy-empty lines.
        if not self.commits:
            raise ValueError("integrated must carry at least one commit")
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def commit(self) -> str:
        return self.commits[-1].sha if self.commits else ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def paths(self) -> list[str]:
        seen: dict[str, None] = {}
        for c in self.commits:
            for p in c.paths:
                seen.setdefault(p, None)
        return list(seen)


class Judged(_Header):
    """A do-as-judge produced a verdict (co-exists with its raw `result`)."""

    kind: Literal["judged"] = "judged"
    verdict: str
    ok: bool
    target_node: str


class LoopIter(_Header):
    """One completed loop iteration (body executed + `until` checked).

    Carries the iteration index, the workdir HEAD after the body integrated, and
    whether `until` converged. Replay folds these to expose, per loop node, the count
    of completed iterations and the last integrated commit (D5 resume rule) — with no
    special case beyond a two-line fold.
    """

    kind: Literal["loop_iter"] = "loop_iter"
    iteration: int
    commit: str | None = None
    converged: bool = False
    # An EXPLICIT reference to this iteration's body outcome: the `seq` of the body
    # leaf's ResultEvent (hand-8, LOOP-OUTCOME-REFERENCE). The fold resolves the body
    # artifact through THIS seq, never the process-global last-folded result — which was
    # only coincidentally correct under serial in-order dispatch and wrong the moment a
    # positional Dispatch completes out of order. `None` on a legacy line (no such field);
    # the projection then falls back to the last ResultEvent before the loop_iter, which
    # is the documented old-journal semantics (its `body_*` payload is ignored either way).
    body_result_seq: int | None = None


class Asked(_Header):
    """An ask parked, awaiting the owner."""

    kind: Literal["asked"] = "asked"
    question: str
    options: list[str] = Field(default_factory=list)


class Answered(_Header):
    """The owner answered (or the ask expired -> synthetic empty answer)."""

    kind: Literal["answered"] = "answered"
    answer: str
    ok: bool


Event = Annotated[
    Union[Boundary, Dispatched, ResultEvent, Integrated, Judged, LoopIter, Asked, Answered],
    Field(discriminator="kind"),
]


class _EventHolder(BaseModel):
    event: Event


def parse_event(data: dict[str, Any]) -> Event:
    """Parse raw data (e.g. one ndjson line) into the discriminated Event union."""
    return _EventHolder.model_validate({"event": data}).event
