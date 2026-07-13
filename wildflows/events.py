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
    """A do/combine/inplace/setup started."""

    kind: Literal["dispatched"] = "dispatched"
    rig: str | None = None
    task: str | None = None
    cmd: str | None = None
    workdir: str | None = None


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
    # A loop's final result reuses this event but carries the last integrated
    # iteration's body artifact in text/files; the convergence/cap disposition rides
    # in this SEPARATE field so a downstream `combine` consumes the artifact, never the
    # status prose. None for every non-loop result. Journal-only (the dashboard
    # reads it); replay's Result reconstruction ignores it.
    loop_status: str | None = None
    outcome: Literal["ok", "failed", "busy"] = "ok"

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
        if not isinstance(data, dict) or "commits" in data:
            return data
        if "commit" in data:
            out = {k: v for k, v in data.items() if k not in ("commit", "paths")}
            out["commits"] = [{"sha": data["commit"], "paths": data.get("paths", [])}]
            return out
        return data

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
    # No body-artifact payload copy (item 3): the loop_iter REFERENCES the body outcome
    # by journal position — the body leaf's ResultEvent is the last result folded before
    # this loop_iter, so the projection recovers the iteration's body from its live
    # last-result at fold time. A pre-collapse line's `body_*` fields are ignored (the
    # preceding ResultEvent still reconstructs the body), so old journals fold unchanged.


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
