"""The single event vocabulary (design invariant 2).

Every primitive execution is one event in ONE vocabulary; resume = replay this log
against the expression tree. `seq` is assigned by the journal on append. All events
share a header (run_id/epoch/node_id/kind + ts/seq) and discriminate on `kind`.
"""
from __future__ import annotations

import time
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field


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
    rails: dict[str, Any] | None = None
    reason: str | None = None  # e.g. deadline / budget / done (on closed)


class Dispatched(_Header):
    """A do/combine/inplace/setup started."""

    kind: Literal["dispatched"] = "dispatched"
    rig: str | None = None
    task: str | None = None
    cmd: str | None = None
    workdir: str | None = None


class ResultEvent(_Header):
    """A primitive produced output."""

    kind: Literal["result"] = "result"
    ok: bool
    text: str = ""
    files: list[str] = Field(default_factory=list)
    exit_code: int | None = None
    # A loop's final result reuses this event but carries the last integrated
    # iteration's body artifact in text/files; the convergence/cap disposition rides
    # in this SEPARATE field so a downstream `combine` consumes the artifact, never the
    # status prose (SF6). None for every non-loop result. Journal-only (the dashboard
    # reads it); replay's Result reconstruction ignores it.
    loop_status: str | None = None
    # Outcome discriminator mirroring Result.outcome. A `busy` result (rate/session
    # wall) journals ok=False AND outcome="busy" so it is NOT confused with a task
    # failure; real backoff/re-entry on it is a later ladder step. Defaults to "ok"
    # so pre-existing journal lines (no field) parse unchanged.
    outcome: Literal["ok", "failed", "busy"] = "ok"


class Integrated(_Header):
    """The core applied+committed a result (mediation proof; core-only)."""

    kind: Literal["integrated"] = "integrated"
    commit: str
    paths: list[str] = Field(default_factory=list)


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
