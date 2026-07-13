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
    Dispatch,
    Do,
    Expr,
    Inplace,
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
    if isinstance(node, Loop):
        if node.until.kind != "cmd":
            raise AdmissionError(
                "loop `until=flag` is planner-judged; lands with real planner integration"
            )
        # An empty composite loop body (a Seq/Dispatch with no executable leaf) would
        # iterate forever producing nothing and leave `loop_iter` with no body outcome to
        # reference (hand-8, LOOP-OUTCOME-REFERENCE). A body must contain a runnable leaf.
        if not _has_executable_leaf(node.body):
            raise AdmissionError("loop body has no executable leaf (do/inplace)")


def _has_executable_leaf(node: Expr) -> bool:
    if isinstance(node, (Do, Inplace)):
        return True
    return any(_has_executable_leaf(c) for c in children_of(node))


def _ancestor_paths(tree: Expr) -> dict[str, list[Expr]]:
    """Each node_id -> the list of expressions from root down to (and including) it."""
    paths: dict[str, list[Expr]] = {}

    def rec(node: Expr, acc: list[Expr]) -> None:
        acc2 = [*acc, node]
        paths[node.node_id] = acc2
        for child in children_of(node):
            rec(child, acc2)

    rec(tree, [])
    return paths


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

    order = list(_walk(tree))
    node_ids = {node.node_id for node in order}
    position = {node.node_id: i for i, node in enumerate(order)}
    paths = _ancestor_paths(tree)
    for node in order:
        _check_capability(node)
        if isinstance(node, Do):
            if node.rig.name not in registry:
                raise AdmissionError(f"unknown rig: {node.rig.name!r}")
            for ref in node.ctx:
                if ref.kind == "node":
                    _check_upstream_ctx_ref(node, ref.ref, node_ids, position, paths)
    return _resume_identity(tree, epoch, projection)


def _check_upstream_ctx_ref(
    node: Do,
    target: str,
    node_ids: set[str],
    position: dict[str, int],
    paths: dict[str, list[Expr]],
) -> None:
    """A `ctx` node ref must resolve to an UPSTREAM result (hand-8, ADMISSION-REFERENCE):
    the referenced node must exist, be strictly earlier in pre-order (rejects self- and
    forward-refs), and not be a sibling reachable only across a `Dispatch` (whose children
    complete in non-deterministic order). A ref into an elder `Seq` sibling is fine."""
    if target not in node_ids:
        raise AdmissionError(f"ctx node ref {target!r} names no node in the epoch tree")
    if position[target] >= position[node.node_id]:
        raise AdmissionError(
            f"ctx node ref {target!r} is not upstream of {node.node_id!r} (self/forward ref)"
        )
    pr, pt = paths[node.node_id], paths[target]
    i = 0
    while i < len(pr) and i < len(pt) and pr[i].node_id == pt[i].node_id:
        i += 1
    lca = pr[i - 1]  # i >= 1: both share the root
    if isinstance(lca, Dispatch):
        raise AdmissionError(
            f"ctx node ref {target!r} crosses a Dispatch (concurrent siblings, "
            f"non-deterministic completion order)"
        )


def _resume_identity(tree: Expr, epoch: int, projection: RunProjection) -> Expr:
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
