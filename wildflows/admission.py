"""Admission: prove an epoch's tree is admissible BEFORE any execution event.

Pydantic proves wire shape at construction; admission proves the rest deterministically
over the whole tree — dealias, deterministic ids, executor capability, rig-name
resolution, and node-ref validity — so the core rejects an unrunnable plan without ever
opening an epoch. A `NotImplementedError` after a durable open boundary is scar tissue,
not a protocol (DESIGN §3). Environment-dependent checks (symlink escapes, the resolved
gitdir, a not-yet-created context file, git/until results) stay at use time in
`Workspace`.

Lexical path guards (absolute / `..` / literal `.git`) live as `Edit`/`CtxRef` validators
(local Pydantic invariants); this pass owns the checks that need the whole assembled tree.
"""
from __future__ import annotations

from collections.abc import Iterator

from wildflows.expr import (
    Combine,
    Do,
    Expr,
    Loop,
    assign_node_ids,
    children_of,
    parse_expr,
)
from wildflows.projection import RunProjection
from wildflows.rig import RigRegistry


class AdmissionError(ValueError):
    """An epoch tree the core can reject without effects (capability, rig, ref, identity).

    Subclasses ValueError so a caller may catch either; it is raised BEFORE any journal
    event, so no incomplete epoch is ever opened for an inadmissible plan.
    """


def _walk(expr: Expr) -> Iterator[Expr]:
    yield expr
    for child in children_of(expr):
        yield from _walk(child)


def _check_capability(node: Expr) -> None:
    # combine/ask/setup are representable but not executable in the PoC; a `loop` is
    # executable only with a `cmd` predicate (a `flag` predicate needs the planner).
    if isinstance(node, Combine) or node.kind in ("ask", "setup"):
        raise AdmissionError(f"{node.kind} is not executable in the PoC")
    if isinstance(node, Loop) and node.until.kind != "cmd":
        raise AdmissionError(
            "loop `until=flag` is planner-judged; lands with real planner integration"
        )


def admit_epoch(
    tree: Expr, epoch: int, projection: RunProjection, registry: RigRegistry
) -> Expr:
    """Return the admitted (dealiased, id-assigned) tree, or raise `AdmissionError`.

    Steps: dealias round-trip (two positions sharing one Python instance become distinct
    objects, so `assign_node_ids` never collapses two declared nodes onto one journal
    key — also deep-copies, so the caller's tree is untouched); deterministic ids; a
    single whole-tree traversal for capability / rig-name / node-ref; and, on an already
    OPEN (unclosed) epoch, resume-identity against the journalled boundary expr.
    """
    tree = parse_expr(tree.model_dump())
    assign_node_ids(tree)

    node_ids = {node.node_id for node in _walk(tree)}
    for node in _walk(tree):
        _check_capability(node)
        if isinstance(node, Do):
            if node.rig.name not in registry:
                raise AdmissionError(f"unknown rig: {node.rig.name!r}")
            for ref in node.ctx:
                if ref.kind == "node" and ref.ref not in node_ids:
                    raise AdmissionError(
                        f"ctx node ref {ref.ref!r} names no node in the epoch tree"
                    )

    if projection.epoch_opened(epoch) and not projection.epoch_closed(epoch):
        # RESUME IDENTITY: the admitted expr was journalled on the `opened` boundary for
        # replay. The planner re-shapes at epoch BOUNDARIES, never mid-epoch, so a
        # resumed tree that differs from the durable boundary is a caller error.
        admitted = projection.epoch_expr(epoch)
        if admitted is not None and admitted != tree.model_dump():
            raise AdmissionError(
                f"resume tree for epoch {epoch} differs from the admitted boundary expr"
            )
    return tree
