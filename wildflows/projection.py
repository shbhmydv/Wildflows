"""The one live fold of the append-only journal.

Running and resumed engines use the same ``RunProjection.apply`` path.  State is keyed
by ``(epoch, node_id)`` and loop floors scope repeated node ids to one iteration.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from wildflows.events import Boundary, Dispatched, Event, Integrated, LoopIter, ResultEvent
from wildflows.result import IntegrationReceipt, Result

NodeKey = tuple[int, str]
Floor = int | None


@dataclass(frozen=True)
class ExecutionOutcome:
    key: NodeKey | None = None
    children: tuple["ExecutionOutcome", ...] = ()

    def result_key(self) -> NodeKey | None:
        if self.children:
            return self.children[-1].result_key()
        return self.key


@dataclass
class LoopIterRecord:
    seq: int
    commit: str | None
    converged: bool
    body: Result | None


@dataclass
class NodeProjection:
    dispatched: bool = False
    dispatch_count: int = 0
    last_dispatch_seq: int = -1
    dispatched_pre_head: str | None = None
    result: Result | None = None
    result_seq: int = -1
    result_post_head: str | None = None
    receipt_required: bool = False
    receipt: IntegrationReceipt | None = None
    integrated_seq: int = -1
    loop_iterations: int = 0
    loop_last_commit: str | None = None
    loop_last_iter_seq: int = -1
    loop_last_body: Result | None = None
    loop_converged: bool = False
    loop_iters: list[LoopIterRecord] = field(default_factory=list)

    def has_unfinished_dispatch(self, floor: Floor) -> bool:
        return (
            floor is not None
            and self.last_dispatch_seq > floor
            and self.last_dispatch_seq > self.result_seq
        )


@dataclass
class EpochProjection:
    phase: str = ""
    expr: dict[str, object] | None = None
    run_branch: str | None = None
    base_commit: str | None = None


class RunProjection:
    def __init__(self) -> None:
        self.nodes: dict[NodeKey, NodeProjection] = {}
        self.epochs: dict[int, EpochProjection] = {}
        self._results_by_seq: dict[int, Result] = {}
        self._last_result_seq = -1

    def apply(self, event: Event) -> None:
        if isinstance(event, Boundary):
            epoch = self.epochs.setdefault(event.epoch, EpochProjection())
            epoch.phase = event.phase
            if event.phase == "opened":
                if event.expr is not None:
                    epoch.expr = event.expr
                epoch.run_branch = event.run_branch
                epoch.base_commit = event.base_commit
            return
        key = (event.epoch, event.node_id)
        node = self.nodes.setdefault(key, NodeProjection())
        if isinstance(event, Dispatched):
            node.dispatched = True
            node.dispatch_count += 1
            node.last_dispatch_seq = event.seq
            node.dispatched_pre_head = event.pre_head
        elif isinstance(event, ResultEvent):
            node.result = Result(
                text=event.text,
                files=event.files,
                exit_code=event.exit_code,
                outcome=event.outcome,
            )
            node.result_seq = event.seq
            node.result_post_head = event.post_head
            node.receipt_required = event.receipt_required
            self._results_by_seq[event.seq] = node.result
            self._last_result_seq = event.seq
        elif isinstance(event, Integrated):
            if node.receipt is None:
                node.receipt = IntegrationReceipt()
            node.receipt.extend(event.commits)
            node.integrated_seq = event.seq
        elif isinstance(event, LoopIter):
            node.loop_iterations = event.iteration + 1
            node.loop_last_commit = event.commit
            node.loop_last_iter_seq = event.seq
            node.loop_converged = event.converged
            ref = event.body_result_seq
            if ref is None:  # compatibility with pre-reference journals
                ref = self._last_result_seq
            node.loop_last_body = self._results_by_seq.get(ref)
            node.loop_iters.append(
                LoopIterRecord(
                    seq=event.seq,
                    commit=event.commit,
                    converged=event.converged,
                    body=node.loop_last_body,
                )
            )

    def resume_action(self, key: NodeKey, floor: Floor) -> str:
        """Return ``run`` or ``done`` for one leaf in the requested loop scope."""
        node = self.nodes.get(key)
        if node is None or floor is None:
            return "run"
        if node.has_unfinished_dispatch(floor):
            return "run"
        if node.result is None or node.result_seq <= floor or not node.result.ok:
            return "run"
        if node.receipt_required and (
            node.integrated_seq <= floor or node.integrated_seq <= node.result_seq
        ):
            return "run"
        return "done"

    def loop_resume(
        self, key: NodeKey, floor: Floor
    ) -> tuple[int, Floor, bool, Result | None]:
        if floor is None:
            return 0, None, False, None
        node = self.nodes.get(key)
        records = [] if node is None else [item for item in node.loop_iters if item.seq > floor]
        if not records:
            return 0, floor, False, None
        last = records[-1]
        return len(records), last.seq, last.converged, last.body

    def node(self, key: NodeKey) -> NodeProjection:
        return self.nodes.get(key, NodeProjection())

    def result(self, key: NodeKey | None) -> Result | None:
        if key is None:
            return None
        node = self.nodes.get(key)
        return node.result if node is not None else None

    def result_text(self, key: NodeKey) -> str | None:
        result = self.result(key)
        return result.text if result is not None else None

    def epoch_opened(self, epoch: int) -> bool:
        return epoch in self.epochs

    def epoch_closed(self, epoch: int) -> bool:
        value = self.epochs.get(epoch)
        return value is not None and value.phase == "closed"

    def epoch_expr(self, epoch: int) -> dict[str, object] | None:
        value = self.epochs.get(epoch)
        return value.expr if value is not None else None

    @property
    def results(self) -> dict[NodeKey, Result]:
        return {key: node.result for key, node in self.nodes.items() if node.result is not None}

    @property
    def integrated(self) -> dict[NodeKey, list[str]]:
        return {
            key: node.receipt.paths
            for key, node in self.nodes.items()
            if node.receipt is not None
        }

    @property
    def receipts(self) -> dict[NodeKey, IntegrationReceipt]:
        return {
            key: node.receipt
            for key, node in self.nodes.items()
            if node.receipt is not None
        }

    @property
    def dispatched(self) -> set[NodeKey]:
        return {key for key, node in self.nodes.items() if node.dispatched}

    @property
    def loop_iterations(self) -> dict[NodeKey, int]:
        return {
            key: node.loop_iterations
            for key, node in self.nodes.items()
            if node.loop_iterations
        }

    @property
    def loop_last_commit(self) -> dict[NodeKey, str | None]:
        return {
            key: node.loop_last_commit
            for key, node in self.nodes.items()
            if node.loop_last_iter_seq >= 0
        }


def replay(run_dir: Path) -> RunProjection:
    from wildflows.journal import Journal

    return Journal.load(run_dir).projection
