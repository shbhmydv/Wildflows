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

from dataclasses import dataclass, field
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
class LoopIterRecord:
    """One folded `loop_iter` event — enough to scope a loop's resume to a floor."""

    seq: int
    commit: str | None
    converged: bool
    body: Result | None


@dataclass
class NodeProjection:
    """Everything the fold knows about one `(epoch, node_id)`."""

    dispatched: bool = False
    dispatch_count: int = 0  # number of Dispatched events = the next attempt's index
    last_dispatch_seq: int = -1
    dispatched_pre_head: str | None = None  # provenance anchor (range START)
    lease_required: bool = False
    result: Result | None = None
    result_seq: int = -1
    result_post_head: str | None = None  # HEAD when the result was recorded (range END)
    workspace_unclean: bool = False
    recovery_action: Literal["fail", "retry"] | None = None
    receipt_required: bool = False
    receipt: IntegrationReceipt | None = None  # accumulated effect record
    integrated_seq: int = -1  # seq of the LAST integrated event (the resume frontier)
    # Loop-only: completed-iteration count, last integrated commit, the seq of the last
    # loop_iter (the partial-iteration resume frontier), the last iteration's body
    # artifact (recovered by reference from the live last-result), and convergence. The
    # per-iteration `loop_iters` list backs floor-scoped resume (nested loops): only iters
    # with seq > floor belong to the CURRENT invocation.
    loop_iterations: int = 0
    loop_last_commit: str | None = None
    loop_last_iter_seq: int = -1
    loop_last_body: Result | None = None
    loop_converged: bool = False
    loop_iters: list[LoopIterRecord] = field(default_factory=list)

    def has_unfinished_dispatch(self, floor: Floor) -> bool:
        """Whether the latest in-scope dispatch has no result from that attempt."""
        return (
            floor is not None
            and self.last_dispatch_seq > floor
            and self.last_dispatch_seq > self.result_seq
        )


@dataclass
class EpochProjection:
    phase: str = ""  # latest boundary phase for the epoch (opened | closed)
    expr: dict[str, object] | None = None  # the admitted tree (from the opened boundary)


class RunProjection:
    """The single fold of a run's journal into per-node / per-epoch state."""

    def __init__(self) -> None:
        self.nodes: dict[NodeKey, NodeProjection] = {}
        self.epochs: dict[int, EpochProjection] = {}
        # Every result by its journal seq, so a loop_iter resolves its body artifact
        # through its EXPLICIT `body_result_seq` reference (hand-8). `_last_result_seq`
        # is the last ResultEvent's seq — used ONLY to fold a legacy loop_iter that
        # predates the explicit reference (old-journal compatibility, not a live path).
        self._results_by_seq: dict[int, Result] = {}
        self._last_result_seq: int = -1

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
            node.dispatch_count += 1
            node.last_dispatch_seq = ev.seq
            node.dispatched_pre_head = ev.pre_head
            node.lease_required = ev.lease_required
        elif isinstance(ev, ResultEvent):
            node.result = Result(
                text=ev.text, files=ev.files, exit_code=ev.exit_code, outcome=ev.outcome,
            )
            node.result_seq = ev.seq
            node.result_post_head = ev.post_head
            node.workspace_unclean = ev.workspace_unclean
            node.recovery_action = ev.recovery_action
            node.receipt_required = ev.receipt_required
            self._results_by_seq[ev.seq] = node.result
            self._last_result_seq = ev.seq
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
            # Resolve the body artifact through the loop_iter's EXPLICIT reference; a
            # legacy line (no reference) falls back to the last result before it — the
            # documented old-journal semantics, never the live fold path.
            ref_seq = ev.body_result_seq if ev.body_result_seq is not None else self._last_result_seq
            node.loop_last_body = self._results_by_seq.get(ref_seq)
            node.loop_iters.append(LoopIterRecord(
                seq=ev.seq, commit=ev.commit, converged=ev.converged, body=node.loop_last_body,
            ))

    # -- resume / durability decision ---------------------------------------

    def resume_action(self, key: NodeKey, floor: Floor) -> Literal["run", "done", "recover"]:
        """The single leaf durability decision, including persistent workspace halts.

        An in-scope ``workspace_unclean`` result is never terminal: resume must first run
        checked recovery from the durable record. A cleared ``retry`` marker also remains
        non-terminal until a new dispatch produces its own result, which closes the crash
        window between recovery and redispatch. Successful declared file effects require
        an integration receipt; failed results are terminal once their cleanup is verified,
        regardless of any artifact names the failed rig reported.
        """
        node = self.nodes.get(key)
        if node is None:
            return "run"
        if node.has_unfinished_dispatch(floor):
            return "run"
        if node.result is None or floor is None or node.result_seq <= floor:
            return "run"
        if node.workspace_unclean:
            return "recover"
        if node.recovery_action == "retry":
            return "run"
        if node.result.ok and (node.result.files or node.receipt_required):
            if (
                node.receipt is None
                or node.integrated_seq <= floor
                or node.integrated_seq <= node.result_seq
            ):
                return "run"
        return "done"

    def loop_resume(
        self, key: NodeKey, floor: Floor
    ) -> tuple[int, Floor, bool, Result | None]:
        """Floor-scoped loop resume state: `(resume_from, partial_floor, converged, body)`.

        Only `loop_iter` events with seq > `floor` belong to THIS invocation, so a nested
        inner loop is scoped to its CURRENT outer iteration and a prior outer iteration's
        inner iterations never make it skip (hand-9, nested-loop resume floor). `floor=None`
        (a fresh iteration from a parent loop) counts none and runs the whole body fresh;
        `floor=-1` (top-level) counts every iteration; `floor=k` counts iters after k.
        `partial_floor` is the last counted iter's seq (marking earlier inner state stale),
        or the incoming `floor` when none were counted.
        """
        if floor is None:
            return 0, None, False, None
        node = self.nodes.get(key)
        counted = [] if node is None else [r for r in node.loop_iters if r.seq > floor]
        if not counted:
            return 0, floor, False, None
        last = counted[-1]
        return len(counted), last.seq, last.converged, last.body

    def node(self, key: NodeKey) -> NodeProjection:
        """The node's projection, or an empty default (never mutating the map)."""
        return self.nodes.get(key, NodeProjection())

    def result(self, key: NodeKey | None) -> Result | None:
        if key is None:
            return None
        node = self.nodes.get(key)
        return node.result if node is not None else None

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
