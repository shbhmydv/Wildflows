"""The expression model: a recursive Pydantic union over the eight expression kinds.

One expression tree = one epoch. `node_id` is the join key between the tree and the
journal (assigned on admission by `assign_node_ids`); resume = replay the log against
the tree by node_id. Every kind is executable except planner-judged `loop` flags,
which remain representable but are rejected by admission.

Pydantic proves wire shape and the LOCAL invariants here (lexical path guards, positive
loop cap, unique inplace paths); whole-tree checks (capability, rig names, node refs)
run in `admission.admit_epoch`.
"""
from __future__ import annotations

from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class RigRef(BaseModel):
    """Names a rig implementation and its config; resolved via the rig registry.

    `params` is RESERVED for planner-integration and is NOT consumed by the engine yet
    (it is not passed to the registry or the rig). It is admitted so the wire shape is
    stable, but has no runtime effect until the planner-config seam lands (DESIGN §8).
    """

    name: str
    params: dict[str, str] = Field(default_factory=dict)


class CtxRef(BaseModel):
    """A tagged reference the core materializes: a file path or an upstream node id."""

    kind: Literal["file", "node"]
    ref: str

    @model_validator(mode="after")
    def _reject_unsafe_file_ref(self) -> "CtxRef":
        # A file ctx path is subject to the same lexical containment as an edit; a node
        # ctx ref is a node id (its existence is validated over the whole tree at
        # admission), so it is not path-checked here.
        if self.kind == "file":
            _reject_unsafe_path(self.ref, "ctx file")
        return self


def _reject_unsafe_path(v: str, what: str) -> str:
    """Lexical containment guard shared by `Edit` and file `CtxRef`.

    An edit/ctx path is a literal path INSIDE the workdir, never a git option, an
    absolute path, a `..` escape, or a git admin path. `--all`/`-f` reaching `git add`
    would stage the whole tree; an absolute or `..` path escapes the workdir; a `.git`
    component targets git internals. Symlink escapes cannot be judged lexically and stay
    a use-time `Workspace` check.
    """
    if v.startswith("-"):
        raise ValueError(f"{what} path must not look like an option (leading dash): {v!r}")
    if ".git" in PurePosixPath(v).parts:
        raise ValueError(f"{what} path targets a git admin path: {v!r}")
    if PurePosixPath(v).is_absolute() or ".." in PurePosixPath(v).parts:
        raise ValueError(f"{what} path escapes workdir: {v!r}")
    return v


class Edit(BaseModel):
    """A whole-file write authored directly by the planner (inplace)."""

    path: str
    content: str

    @field_validator("path")
    @classmethod
    def _reject_unsafe(cls, v: str) -> str:
        return _reject_unsafe_path(v, "edit")


class Until(BaseModel):
    """A loop predicate: a shell `cmd` (exit-0 = done) or planner-judged `flag`."""

    kind: Literal["cmd", "flag"]
    cmd: str | None = None
    timeout_s: float = Field(default=30.0, gt=0)

    @model_validator(mode="after")
    def _cmd_requires_command(self) -> "Until":
        # A `cmd` predicate without a command is invalid expression data, not an engine
        # crash after the epoch is already open. A blank/whitespace-only cmd is
        # equally invalid: POSIX `sh -c ""` exits 0, so it would be a predicate that
        # ALWAYS converges with no test at all. Reject both.
        if self.kind == "cmd" and (self.cmd is None or not self.cmd.strip()):
            raise ValueError("until(kind='cmd') requires a non-blank `cmd`")
        return self


class Do(BaseModel):
    kind: Literal["do"] = "do"
    node_id: str = ""
    task: str
    rig: RigRef
    ctx: list[CtxRef] = Field(default_factory=list)


class Dispatch(BaseModel):
    """Parallel `do()`s — children are unordered and disjoint by construction.

    Dispatch declares *no* ordering: executable leaf siblings run through the bounded
    scheduler and the semantics are unordered-parallel. For strictly ordered execution
    use `Seq`.
    """

    kind: Literal["dispatch"] = "dispatch"
    node_id: str = ""
    children: list["Expr"]


class Seq(BaseModel):
    """Strictly ordered execution: run each child in list order, one after another."""

    kind: Literal["seq"] = "seq"
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

    @model_validator(mode="after")
    def _reject_duplicate_paths(self) -> "Inplace":
        # Two edits to one path within a single inplace have ambiguous last-writer
        # semantics and a duplicate commit pathspec; reject at admission.
        paths = [e.path for e in self.edits]
        if len(paths) != len(set(paths)):
            raise ValueError("inplace edits contain duplicate paths")
        return self


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
    Union[Do, Dispatch, Seq, Combine, Loop, Inplace, Ask, Setup],
    Field(discriminator="kind"),
]

# Resolve the forward references now that every member exists.
Dispatch.model_rebuild()
Seq.model_rebuild()
Combine.model_rebuild()
Loop.model_rebuild()


class _ExprHolder(BaseModel):
    expr: Expr


def parse_expr(data: dict[str, Any]) -> Expr:
    """Parse raw data into the discriminated Expr union."""
    return _ExprHolder.model_validate({"expr": data}).expr


def children_of(expr: Expr) -> list[Expr]:
    """The direct sub-expressions of a node, in stable order."""
    if isinstance(expr, (Dispatch, Seq)):
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
