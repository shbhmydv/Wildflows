"""Validated planner boundary and typed parked run states."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
class Rails(BaseModel):
    model_config = ConfigDict(extra="forbid")
    deadline_s: float | None = Field(default=None, gt=0)
    max_epochs: int | None = Field(default=None, gt=0)
    budget_notes: str | None = None
class PlannerDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expression: dict[str, Any] | None
    rails: Rails = Field(default_factory=Rails)
    rationale: str
    end: bool
    final_summary: str | None = None
    @model_validator(mode="after")
    def _complete_shape(self) -> "PlannerDecision":
        if self.end:
            if self.expression is not None:
                raise ValueError("an ending decision requires expression=null")
            if not self.final_summary:
                raise ValueError("an ending decision requires final_summary")
        elif self.expression is None:
            raise ValueError("a continuing decision requires expression")
        return self
class PlannerFailure(RuntimeError):
    def __init__(self, message: str, decision_path: Path) -> None:
        super().__init__(message)
        self.decision_path = decision_path
        self.retryable = True
@dataclass(frozen=True)
class OwnerQuestion:
    epoch: int
    node_id: str
    question: str
    options: tuple[str, ...]
class AwaitingOwner(RuntimeError):
    def __init__(self, questions: tuple[OwnerQuestion, ...]) -> None:
        if not questions:
            raise ValueError("AwaitingOwner requires a pending question")
        self.questions = questions
        first = questions[0]
        super().__init__(first.question)
        self.epoch = first.epoch
        self.node_id = first.node_id
        self.question = first.question
        self.options = first.options
class SetupResumeRequired(RuntimeError):
    def __init__(self, epoch: int, node_id: str) -> None:
        self.epoch, self.node_id = epoch, node_id
        super().__init__(f"non-idempotent setup {node_id} requires explicit retry")
class RailStop(RuntimeError):
    def __init__(
        self,
        *,
        run_id: str,
        epoch: int,
        rail: Literal["deadline_s", "max_epochs"],
        limit: float,
        observed: float,
    ) -> None:
        self.run_id, self.epoch, self.rail = run_id, epoch, rail
        self.limit, self.observed = limit, observed
        super().__init__(f"{rail} rail hit: observed {observed:.3f}, limit {limit:.3f}")
