"""The single journal event vocabulary.

Events describe workflow facts, not workspace rollback transactions.  ``pre_head`` and
``post_head`` are the two cheap provenance anchors needed to reconcile the one crash
window around a run-branch fast-forward.
"""
from __future__ import annotations

import time
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, computed_field, model_validator

from wildflows.result import CommitReceipt, reconcile_outcome


class _Header(BaseModel):
    seq: int = -1
    ts: float = Field(default_factory=time.time)
    run_id: str
    epoch: int
    node_id: str


class Boundary(_Header):
    kind: Literal["boundary"] = "boundary"
    phase: Literal["opened", "closed"]
    expr: dict[str, Any] | None = None
    reason: str | None = None
    # New runs pin both facts on every opened epoch.  Defaults retain replay support for
    # complete pre-worktree journals; an old open run is not executable by this engine.
    run_branch: str | None = None
    base_commit: str | None = None


class Dispatched(_Header):
    kind: Literal["dispatched"] = "dispatched"
    rig: str | None = None
    task: str | None = None
    cmd: str | None = None
    workdir: str | None = None
    pre_head: str | None = None


class ResultEvent(_Header):
    kind: Literal["result"] = "result"
    text: str = ""
    files: list[str] = Field(default_factory=list)
    exit_code: int | None = None
    post_head: str | None = None
    loop_status: str | None = None
    outcome: Literal["ok", "failed", "busy"] = "ok"
    # True when post_head differs from the attempt base, including an empty commit.
    receipt_required: bool = False
    # A failed ResultEvent can invalidate one unverifiable Integrated claim while keeping
    # the event vocabulary append-only.  The node is then rerun from the prior verified tip.
    fallback_for: int | None = None

    @model_validator(mode="before")
    @classmethod
    def _collapse_ok(cls, data: Any) -> Any:
        return reconcile_outcome(data)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ok(self) -> bool:
        return self.outcome == "ok"


class Integrated(_Header):
    """The core fast-forwarded every attributed commit onto the run branch."""

    kind: Literal["integrated"] = "integrated"
    commits: list[CommitReceipt] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _migrate_single_commit(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "commits" in data and data["commits"]:
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
    def _require_commits(self) -> "Integrated":
        if not self.commits:
            raise ValueError("integrated must carry at least one commit")
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def commit(self) -> str:
        return self.commits[-1].sha

    @computed_field  # type: ignore[prop-decorator]
    @property
    def paths(self) -> list[str]:
        seen: dict[str, None] = {}
        for commit in self.commits:
            for path in commit.paths:
                seen.setdefault(path, None)
        return list(seen)


class Judged(_Header):
    kind: Literal["judged"] = "judged"
    verdict: str
    ok: bool
    target_node: str


class LoopIter(_Header):
    kind: Literal["loop_iter"] = "loop_iter"
    iteration: int
    commit: str | None = None
    converged: bool = False
    body_result_seq: int | None = None


class Asked(_Header):
    kind: Literal["asked"] = "asked"
    question: str
    options: list[str] = Field(default_factory=list)


class Answered(_Header):
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
    return _EventHolder.model_validate({"event": data}).event
