"""Core result value types: a primitive's output and its terminal outcome.

`Result` is a core domain value (a `do`/`inplace`/`loop` output — a diff, artifact,
free text, or a judgment are all one shape), not a rig implementation detail, so it
lives here rather than in `rig.py`. Rigs, the engine, and the projection all depend on
it. `ok` and `outcome` currently BOTH encode terminal status; unifying them is a later
raze (item 3/9), left intact here.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# Outcome discriminator (extends the plain ok/not-ok Result):
#   ok     — the primitive produced output and succeeded.
#   failed — a real task/transport failure (non-zero exit, timeout, git failure).
#   busy   — a rate/session/quota wall: NOT a task failure; back off and re-enter
#            (real backoff policy is a later ladder step).
Outcome = Literal["ok", "failed", "busy"]


class Result(BaseModel):
    """A primitive's output: a diff, artifact, free text, or a judgment — one shape."""

    text: str = ""
    files: list[str] = Field(default_factory=list)
    ok: bool = True
    exit_code: int | None = None
    outcome: Outcome = "ok"
