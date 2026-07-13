"""The one live journal projection: fold the event log into node/epoch state.

Resume, the loop fold, and (later) the dashboard all read THIS — there is no second
state system and no per-shape resume code. `apply(event)` is the only fold: the append
owner (the journal) calls it on every append, and `load` replays the ndjson through the
same `apply`, so a running projection and a reloaded one are bit-identical.

State is keyed by `(epoch, node_id)` — NOT node_id alone — so a reopened epoch's node
never inherits an earlier epoch's fact. One `NodeProjection` per node replaces the
old parallel dictionaries; `resume_action` is the single durability decision.
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
from wildflows.result import Result

NodeKey = tuple[int, str]

# Resume scope for a leaf: any durable state with seq > floor counts.
#   -1   top-level node — any durable state counts.
#   k    a loop's resumed-partial iteration — only inner state journalled AFTER the
#        last loop_iter (seq k) is durable for it.
#   None a fresh loop iteration — nothing counts, the whole body re-runs (replaces the
#        old `_NO_RESUME = sys.maxsize` sentinel with an explicit "no resume" scope).
Floor = int | None


@dataclass
class NodeProjection:
    """Everything the fold knows about one `(epoch, node_id)`."""

    dispatched: bool = False
    result: Result | None = None
    result_seq: int = -1
    integrated_paths: list[str] | None = None
    integrated_seq: int = -1
    # Loop-only: completed-iteration count, last integrated commit, the seq of the last
    # loop_iter (the partial-iteration resume frontier), the last iteration's body
    # artifact, and whether it converged.
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
        # The most recently journalled result (a loop reads its body's output by this
        # rather than by re-scanning a journal slice; a fuller outcome-reference model
        # is a later raze, item 3).
        self._last_result: Result | None = None
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
        elif isinstance(ev, ResultEvent):
            node.result = Result(
                text=ev.text, files=ev.files, ok=ev.ok,
                exit_code=ev.exit_code, outcome=ev.outcome,
            )
            node.result_seq = ev.seq
            self._last_result = node.result
            self._last_result_seq = ev.seq
        elif isinstance(ev, Integrated):
            node.integrated_paths = ev.paths
            node.integrated_seq = ev.seq
        elif isinstance(ev, LoopIter):
            node.loop_iterations = ev.iteration + 1
            node.loop_last_commit = ev.commit
            node.loop_last_iter_seq = ev.seq
            node.loop_last_body = Result(
                text=ev.body_text, files=ev.body_files, ok=True, exit_code=ev.body_exit_code
            )
            node.loop_converged = ev.converged

    # -- resume / durability decision ---------------------------------------

    def resume_action(self, key: NodeKey, floor: Floor) -> Literal["run", "done"]:
        """The single leaf durability decision (was `_is_durable`).

        A `do`/`inplace` with DECLARED FILE EFFECTS is durable only once its core
        `integrated` is journalled; an effectless node is durable on its result
        alone. `floor` scopes the decision: durable state at/below it is stale;
        `None` (a fresh loop iteration) is never durable.
        """
        node = self.nodes.get(key)
        if node is None or node.result is None:
            return "run"
        if floor is None or node.result_seq <= floor:
            return "run"
        if node.result.files:
            if node.integrated_paths is None or node.integrated_seq <= floor:
                return "run"
        return "done"

    def node(self, key: NodeKey) -> NodeProjection:
        """The node's projection, or an empty default (never mutating the map)."""
        return self.nodes.get(key, NodeProjection())

    def has_result(self, key: NodeKey) -> bool:
        node = self.nodes.get(key)
        return node is not None and node.result is not None

    def result_text(self, key: NodeKey) -> str | None:
        node = self.nodes.get(key)
        return node.result.text if node is not None and node.result is not None else None

    def last_result_since(self, floor_seq: int) -> Result | None:
        """The most recent journalled result whose seq >= floor_seq, else None."""
        if self._last_result is not None and self._last_result_seq >= floor_seq:
            return self._last_result
        return None

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
        return {k: n.integrated_paths for k, n in self.nodes.items()
                if n.integrated_paths is not None}

    @property
    def dispatched(self) -> set[NodeKey]:
        return {k for k, n in self.nodes.items() if n.dispatched}

    @property
    def loop_iterations(self) -> dict[NodeKey, int]:
        return {k: n.loop_iterations for k, n in self.nodes.items() if n.loop_iterations}

    @property
    def loop_last_commit(self) -> dict[NodeKey, str | None]:
        return {k: n.loop_last_commit for k, n in self.nodes.items() if n.loop_last_iter_seq >= 0}


def _fold(events: list[Event]) -> RunProjection:
    projection = RunProjection()
    for ev in events:
        projection.apply(ev)
    return projection


def replay(run_dir: Path) -> RunProjection:
    """Reconstruct run state from the ndjson alone — the single resume/dashboard path."""
    from wildflows.journal import Journal  # local import: journal depends on projection

    return Journal.load(run_dir).projection
