"""The one live journal projection: fold the event log into node/epoch state.

Resume, the loop fold, and (later) the dashboard all read THIS — there is no second
state system and no per-shape resume code. `apply(event)` is the only fold: the append
owner (the journal) calls it on every append, and `load` replays the ndjson through the
same `apply`, so a running projection and a reloaded one are bit-identical.

State is keyed by `(epoch, node_id)` — NOT node_id alone — so a reopened epoch's node
never inherits an earlier epoch's fact. One `NodeProjection` per node; `resume_action`
is the single durability decision. Effects accumulate into one `IntegrationReceipt` per
node (item 3: no last-write-wins on a single `paths` list). `_exec` returns an
`ExecutionOutcome` referencing the journalled result(s); a loop reads its body's outcome
through that reference, not a journal re-scan.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from wildflows.events import (
    Boundary,
    Dispatched,
    Event,
    Integrated,
    LoopIter,
    ResultEvent,
)
from wildflows.result import IntegrationReceipt, Result

NodeKey = tuple[int, str]

# Resume scope for a leaf: any durable state with seq > floor counts.
#   -1   top-level node — any durable state counts.
#   k    a loop's resumed-partial iteration — only inner state journalled AFTER the
#        last loop_iter (seq k) is durable for it.
#   None a fresh loop iteration — nothing counts, the whole body re-runs (replaces the
#        old `_NO_RESUME = sys.maxsize` sentinel with an explicit "no resume" scope).
Floor = int | None


@dataclass(frozen=True)
class ExecutionOutcome:
    """What `_exec` returns: a reference into the journalled projection, never a payload.

    A leaf carries its result `key`; a `seq`/`dispatch` carries its children's outcomes
    positionally aligned with the inputs (preserving order even when completion order
    later becomes nondeterministic). `result_key` resolves the single effective result a
    consumer reads — a leaf's own, or a composite's last child (the `seq` convention a
    loop body relies on).
    """

    key: NodeKey | None = None
    children: tuple["ExecutionOutcome", ...] = ()

    def result_key(self) -> NodeKey | None:
        if self.children:
            return self.children[-1].result_key()
        return self.key


@dataclass
class NodeProjection:
    """Everything the fold knows about one `(epoch, node_id)`."""

    dispatched: bool = False
    result: Result | None = None
    result_seq: int = -1
    receipt: IntegrationReceipt | None = None  # accumulated effect record
    integrated_seq: int = -1  # seq of the LAST integrated event (the resume frontier)
    # Loop-only: completed-iteration count, last integrated commit, the seq of the last
    # loop_iter (the partial-iteration resume frontier), the last iteration's body
    # artifact (recovered by reference from the live last-result), and convergence.
    loop_iterations: int = 0
    loop_last_commit: str | None = None
    loop_last_iter_seq: int = -1
    loop_last_body: Result | None = None
    loop_converged: bool = False


@dataclass
class EpochProjection:
    phase: str = ""  # latest boundary phase for the epoch (opened | closed)
    expr: dict[str, object] | None = None  # the admitted tree (from the opened boundary)


class RunProjection:
    """The single fold of a run's journal into per-node / per-epoch state."""

    def __init__(self) -> None:
        self.nodes: dict[NodeKey, NodeProjection] = {}
        self.epochs: dict[int, EpochProjection] = {}
        # The most recently journalled result. A loop_iter references its body's outcome
        # through this (the body leaf's result is the last one folded before the
        # loop_iter), so no body payload is copied into the loop_iter event (item 3).
        self._last_result: Result | None = None

    # -- the one fold --------------------------------------------------------

    def apply(self, ev: Event) -> None:
        if isinstance(ev, Boundary):
            ep = self.epochs.setdefault(ev.epoch, EpochProjection())
            ep.phase = ev.phase  # latest boundary wins
            if ev.phase == "opened" and ev.expr is not None:
                ep.expr = ev.expr
            return
        key = (ev.epoch, ev.node_id)
        node = self.nodes.setdefault(key, NodeProjection())
        if isinstance(ev, Dispatched):
            node.dispatched = True
        elif isinstance(ev, ResultEvent):
            node.result = Result(
                text=ev.text, files=ev.files, exit_code=ev.exit_code, outcome=ev.outcome,
            )
            node.result_seq = ev.seq
            self._last_result = node.result
        elif isinstance(ev, Integrated):
            if node.receipt is None:
                node.receipt = IntegrationReceipt()
            node.receipt.extend(ev.commits)  # ACCUMULATE — never last-write-wins
            node.integrated_seq = ev.seq
        elif isinstance(ev, LoopIter):
            node.loop_iterations = ev.iteration + 1
            node.loop_last_commit = ev.commit
            node.loop_last_iter_seq = ev.seq
            node.loop_converged = ev.converged
            # Reference (not copy): the body's outcome is the last result folded before
            # this loop_iter — for old journals too (the body ResultEvent precedes it).
            node.loop_last_body = self._last_result

    # -- resume / durability decision ---------------------------------------

    def resume_action(self, key: NodeKey, floor: Floor) -> Literal["run", "done"]:
        """The single leaf durability decision.

        A `do`/`inplace` with DECLARED FILE EFFECTS is durable only once its core
        `integrated` receipt is journalled; an effectless node is durable on its result
        alone. `floor` scopes the decision: durable state at/below it is stale; `None`
        (a fresh loop iteration) is never durable.
        """
        node = self.nodes.get(key)
        if node is None or node.result is None:
            return "run"
        if floor is None or node.result_seq <= floor:
            return "run"
        if node.result.files:
            if node.receipt is None or node.integrated_seq <= floor:
                return "run"
        return "done"

    def node(self, key: NodeKey) -> NodeProjection:
        """The node's projection, or an empty default (never mutating the map)."""
        return self.nodes.get(key, NodeProjection())

    def result(self, key: NodeKey | None) -> Result | None:
        if key is None:
            return None
        node = self.nodes.get(key)
        return node.result if node is not None else None

    def has_result(self, key: NodeKey) -> bool:
        node = self.nodes.get(key)
        return node is not None and node.result is not None

    def result_text(self, key: NodeKey) -> str | None:
        node = self.nodes.get(key)
        return node.result.text if node is not None and node.result is not None else None

    # -- epoch queries -------------------------------------------------------

    def epoch_opened(self, epoch: int) -> bool:
        return epoch in self.epochs

    def epoch_closed(self, epoch: int) -> bool:
        ep = self.epochs.get(epoch)
        return ep is not None and ep.phase == "closed"

    def epoch_expr(self, epoch: int) -> dict[str, object] | None:
        ep = self.epochs.get(epoch)
        return ep.expr if ep is not None else None

    # -- read-only views (dashboard / tests consume the fold, not internals) --

    @property
    def results(self) -> dict[NodeKey, Result]:
        return {k: n.result for k, n in self.nodes.items() if n.result is not None}

    @property
    def integrated(self) -> dict[NodeKey, list[str]]:
        """Per-node union of every attributed commit's paths (the ownership set)."""
        return {k: n.receipt.paths for k, n in self.nodes.items() if n.receipt is not None}

    @property
    def receipts(self) -> dict[NodeKey, IntegrationReceipt]:
        return {k: n.receipt for k, n in self.nodes.items() if n.receipt is not None}

    @property
    def dispatched(self) -> set[NodeKey]:
        return {k for k, n in self.nodes.items() if n.dispatched}

    @property
    def loop_iterations(self) -> dict[NodeKey, int]:
        return {k: n.loop_iterations for k, n in self.nodes.items() if n.loop_iterations}

    @property
    def loop_last_commit(self) -> dict[NodeKey, str | None]:
        return {k: n.loop_last_commit for k, n in self.nodes.items() if n.loop_last_iter_seq >= 0}


def replay(run_dir: Path) -> RunProjection:
    """Reconstruct run state from the ndjson alone — the single resume/dashboard path."""
    from wildflows.journal import Journal  # local import: journal depends on projection

    return Journal.load(run_dir).projection
