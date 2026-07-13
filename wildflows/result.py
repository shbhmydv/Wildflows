"""Core result + effect value types: a primitive's artifact, and the committed effect.

RAZE item 3 separates three concepts that hand-6 conflated:

- `Result` — the AGENT ARTIFACT: `text`, `files` (artifact paths), and an `outcome`
  enum that is the SINGLE source of terminal status. `ok` is a derived convenience
  (`outcome == "ok"`), not a second stored field — the ok/outcome duplication is
  collapsed here. A `mode="before"` validator reconciles a legacy/caller `ok` into
  `outcome`, so it is ALSO the compatibility reader for old journals (a pre-collapse
  line carried `ok` and, on very old lines, no `outcome`).
- `CommitReceipt` / `IntegrationReceipt` — the EFFECT record: every commit the core
  attributes to a node attempt, each with its own changed paths. This is the ownership
  ledger (which commits, which paths), distinct from the artifact `Result.files`.

`Result` is a core domain value (a diff, artifact, free text, or judgment are one
shape), not a rig detail, so it lives here; rigs/engine/projection all depend on it.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, computed_field, model_validator

# Outcome discriminator — the single terminal-status source:
#   ok     — the primitive produced output and succeeded.
#   failed — a real task/transport failure (non-zero exit, timeout, git failure,
#            a loop that hit its cap without converging).
#   busy   — a rate/session/quota wall: NOT a task failure; back off and re-enter
#            (real backoff policy is a later ladder step).
Outcome = Literal["ok", "failed", "busy"]


def reconcile_outcome(data: Any) -> Any:
    """Fold a legacy/convenience `ok` into the authoritative `outcome` and drop `ok`.

    This is the ok/outcome collapse AND the old-journal compatibility reader in one:
    - an explicit `busy`/`failed` outcome is authoritative and kept;
    - otherwise an `ok=False` (a pre-collapse failure OR the old loop-cap drift line
      that carried `ok=False, outcome="ok"`) becomes `failed`;
    - a missing outcome defaults to `ok`.
    `ok` is a computed field, so it is popped from the input either way. Non-dict input
    (already-built model) passes through untouched.
    """
    if not isinstance(data, dict):
        return data
    oc = data.get("outcome")
    ok = data.get("ok")
    if oc not in ("failed", "busy"):
        if ok is False:
            oc = "failed"
        elif oc is None:
            oc = "ok"
    out = {k: v for k, v in data.items() if k != "ok"}
    out["outcome"] = oc
    return out


class Result(BaseModel):
    """A primitive's output artifact: a diff, artifact, free text, or a judgment."""

    text: str = ""
    files: list[str] = Field(default_factory=list)
    exit_code: int | None = None
    outcome: Outcome = "ok"

    @model_validator(mode="before")
    @classmethod
    def _collapse_ok(cls, data: Any) -> Any:
        return reconcile_outcome(data)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ok(self) -> bool:
        return self.outcome == "ok"


class CommitReceipt(BaseModel):
    """One commit the core attributes to a node attempt, with its changed paths."""

    sha: str
    paths: list[str] = Field(default_factory=list)


class IntegrationReceipt(BaseModel):
    """The accumulated effect record for a node attempt: every attributed commit.

    Replay ACCUMULATES commits across a node's `integrated` events (no last-write-wins
    on a single `paths` list). `paths` is the order-preserving union of every commit's
    paths — the disjoint-ownership set — and `shas` names every commit verifiably.
    """

    commits: list[CommitReceipt] = Field(default_factory=list)

    def extend(self, more: list[CommitReceipt]) -> None:
        self.commits.extend(more)

    @property
    def shas(self) -> list[str]:
        return [c.sha for c in self.commits]

    @property
    def paths(self) -> list[str]:
        seen: dict[str, None] = {}
        for c in self.commits:
            for p in c.paths:
                seen.setdefault(p, None)
        return list(seen)
