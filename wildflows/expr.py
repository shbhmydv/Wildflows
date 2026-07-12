"""The expression model: a recursive Pydantic union over the 7 primitives.

One expression tree = one epoch. `node_id` is the join key between the tree and the
journal (assigned on admission by `assign_node_ids`); resume = replay the log against
the tree by node_id. Only `do` and `inplace` are executable in the PoC, but all seven
are representable so the model and event vocabulary are proven complete from day one.
"""
from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field


class RigRef(BaseModel):
    """Names a rig implementation and its config; resolved via the rig registry."""

    name: str
    params: dict[str, str] = Field(default_factory=dict)


class CtxRef(BaseModel):
    """A tagged reference the core materializes: a file path or an upstream node id."""

    kind: Literal["file", "node"]
    ref: str


class Edit(BaseModel):
    """A whole-file write authored directly by the planner (inplace)."""

    path: str
    content: str


class Until(BaseModel):
    """A loop predicate: a shell `cmd` (exit-0 = done) or planner-judged `flag`."""

    kind: Literal["cmd", "flag"]
    cmd: str | None = None


class Do(BaseModel):
    kind: Literal["do"] = "do"
    node_id: str = ""
    task: str
    rig: RigRef
    ctx: list[CtxRef] = Field(default_factory=list)


class Dispatch(BaseModel):
    kind: Literal["dispatch"] = "dispatch"
    node_id: str = ""
    children: list["Expr"]


class Combine(BaseModel):
    kind: Literal["combine"] = "combine"
    node_id: str = ""
    task: str
    rig: RigRef
    inputs: list["Expr"]


class Loop(BaseModel):
    kind: Literal["loop"] = "loop"
    node_id: str = ""
    body: "Expr"
    until: Until
    cap: int = Field(gt=0)


class Inplace(BaseModel):
    kind: Literal["inplace"] = "inplace"
    node_id: str = ""
    edits: list[Edit]


class Ask(BaseModel):
    kind: Literal["ask"] = "ask"
    node_id: str = ""
    question: str
    options: list[str] = Field(default_factory=list)


class Setup(BaseModel):
    kind: Literal["setup"] = "setup"
    node_id: str = ""
    cmd: str
    cwd: str | None = None
    idempotent: bool = True


Expr = Annotated[
    Union[Do, Dispatch, Combine, Loop, Inplace, Ask, Setup],
    Field(discriminator="kind"),
]

# Resolve the forward references now that every member exists.
Dispatch.model_rebuild()
Combine.model_rebuild()
Loop.model_rebuild()


class _ExprHolder(BaseModel):
    expr: Expr


def parse_expr(data: dict[str, Any]) -> Expr:
    """Parse raw data into the discriminated Expr union."""
    return _ExprHolder.model_validate({"expr": data}).expr


def children_of(expr: Expr) -> list[Expr]:
    """The direct sub-expressions of a node, in stable order."""
    if isinstance(expr, Dispatch):
        return list(expr.children)
    if isinstance(expr, Combine):
        return list(expr.inputs)
    if isinstance(expr, Loop):
        return [expr.body]
    return []


def assign_node_ids(root: Expr, prefix: str = "n0") -> None:
    """Assign deterministic pre-order path ids in place (root -> n0, child i -> nX.i)."""
    root.node_id = prefix
    for i, child in enumerate(children_of(root)):
        assign_node_ids(child, f"{prefix}.{i}")
