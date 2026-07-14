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
    Ask,
    Combine,
    Dispatch,
    Do,
    Expr,
    Inplace,
    Loop,
    Seq,
    Setup,
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
    # Combine and planner-judged loop flags remain outside the M4 execution core.
    if isinstance(node, Combine):
        raise AdmissionError("combine is not executable in the M4 core")
    if isinstance(node, Setup) and node.cwd not in (None, "", "."):
        raise AdmissionError("setup always runs at the repository root")
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
    if isinstance(node, (Do, Inplace, Ask, Setup)):
        return True
    return any(_has_executable_leaf(c) for c in children_of(node))


# Executable leaves and loops produce durable results; composites are structural.
_RESULTFUL = (Do, Inplace, Ask, Setup, Loop)


def _is_result_total(node: Expr) -> bool:
    """True if `node`'s LAST positional child chain terminates in a result-producing leaf,
    so `ExecutionOutcome.result_key()` over it is TOTAL (hand-9, LOOP-OUTCOME-TOTALITY).

    A leaf/loop is result-total; a `seq`/`dispatch` is result-total only when it has
    children AND its LAST child is result-total (recursively). An empty composite, or one
    whose last child chain bottoms out at a resultless structural node, is NOT."""
    if isinstance(node, _RESULTFUL):
        return True
    if isinstance(node, (Dispatch, Seq)):
        return bool(node.children) and _is_result_total(node.children[-1])
    return False


def _check_result_total(node: Expr) -> None:
    # Every composite must produce a defined outcome: its last positional child chain must
    # end in an executable result-producing leaf. This makes result_key() total by
    # construction, so an uninterrupted fold and a resumed fold always agree.
    if isinstance(node, (Dispatch, Seq)) and not _is_result_total(node):
        raise AdmissionError(
            f"composite {node.node_id!r} ({node.kind}) has no result-producing last leaf: "
            f"its final positional child chain must terminate in an executable leaf/loop"
        )


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
        _check_result_total(node)
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
    # An ANCESTOR composite's result does not exist before the consumer runs (the consumer
    # is inside it), so a ref to an enclosing node can never resolve (hand-9, finding 6).
    if target in {n.node_id for n in pr[:-1]}:
        raise AdmissionError(
            f"ctx node ref {target!r} is an unfinished ancestor of {node.node_id!r} "
            f"(its result does not exist before the consumer runs)"
        )
    # The target must PRODUCE a result: an executable leaf (do/inplace) or a loop. A
    # structural seq/dispatch (or a non-executable combine/ask/setup) journals no result,
    # so a ref to it would deterministically fail to resolve — reject it at admission.
    target_node = pt[-1]
    if not isinstance(target_node, _RESULTFUL):
        raise AdmissionError(
            f"ctx node ref {target!r} targets a {target_node.kind} which produces no result"
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
