from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from wildflows.events import (
    Answered,
    Asked,
    Boundary,
    Dispatched,
    Event,
    Integrated,
    LoopIter,
    ResultEvent,
)
from wildflows.planner import OwnerQuestion, Rails
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
    artifact: str | None = None
    question: str | None = None
    options: tuple[str, ...] = ()
    asked_seq: int = -1
    answered_seq: int = -1
    loop_status: str | None = None
    loop_iterations: int = 0
    loop_last_commit: str | None = None
    loop_last_iter_seq: int = -1
    loop_last_body: Result | None = None
    loop_converged: bool = False
    loop_iters: list[LoopIterRecord] = field(default_factory=list)
    def has_unfinished_dispatch(self, floor: Floor) -> bool:
        return (
            floor is not None and self.last_dispatch_seq > floor
            and self.last_dispatch_seq > self.result_seq
        )
@dataclass
class EpochProjection:
    phase: str = ""
    expr: dict[str, object] | None = None
    run_branch: str | None = None
    base_commit: str | None = None
    rails: Rails | None = None
    rationale: str | None = None
    reason: str | None = None
class RunProjection:
    def __init__(self) -> None:
        self._history: list[Event] = []
        self._clear()
    def _clear(self) -> None:
        self.nodes: dict[NodeKey, NodeProjection] = {}
        self.epochs: dict[int, EpochProjection] = {}
        self._results_by_seq: dict[int, Result] = {}
        self._last_result_seq = -1
    @staticmethod
    def _effective(history: list[Event]) -> list[Event]:
        effective: list[Event] = []
        for event in history:
            if isinstance(event, Boundary) and event.fallback_from is not None:
                effective = [old for old in effective if old.seq < event.fallback_from]
            effective.append(event)
        return effective
    @property
    def effective_events(self) -> list[Event]:
        return self._effective(self._history)
    def apply(self, event: Event) -> None:
        self._history.append(event)
        if isinstance(event, Boundary) and event.fallback_from is not None:
            effective = self._effective(self._history)
            self._clear()
            for retained in effective:
                self._apply_fact(retained)
            return
        self._apply_fact(event)
    def _apply_fact(self, event: Event) -> None:
        if isinstance(event, Boundary):
            epoch = self.epochs.setdefault(event.epoch, EpochProjection())
            epoch.phase = event.phase
            if event.phase == "opened":
                if event.expr is not None:
                    epoch.expr = event.expr
                epoch.run_branch = event.run_branch
                epoch.base_commit = event.base_commit
                epoch.rails = event.rails
                epoch.rationale = event.rationale
            epoch.reason = event.reason
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
                text=event.text, files=event.files, exit_code=event.exit_code,
                outcome=event.outcome,
            )
            node.result_seq = event.seq
            node.result_post_head = event.post_head
            node.receipt_required = event.receipt_required
            node.loop_status = event.loop_status
            node.artifact = event.artifact
            self._results_by_seq[event.seq] = node.result
            self._last_result_seq = event.seq
        elif isinstance(event, Asked):
            node.question = event.question
            node.options = tuple(event.options)
            node.asked_seq = event.seq
        elif isinstance(event, Answered):
            node.result = Result(
                text=event.answer, outcome="ok" if event.ok else "failed"
            )
            node.result_seq = event.seq
            node.answered_seq = event.seq
            node.receipt_required = False
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
            if ref is None:
                ref = self._last_result_seq
            node.loop_last_body = self._results_by_seq.get(ref)
            node.loop_iters.append(LoopIterRecord(
                seq=event.seq, commit=event.commit, converged=event.converged,
                body=node.loop_last_body,
            ))
    def resume_action(self, key: NodeKey, floor: Floor) -> str:
        node = self.nodes.get(key)
        if node is None or floor is None or node.has_unfinished_dispatch(floor):
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
    def epoch_rails(self, epoch: int) -> Rails | None:
        value = self.epochs.get(epoch)
        return value.rails if value is not None else None
    def pending_questions(self) -> list[OwnerQuestion]:
        pending: list[OwnerQuestion] = []
        for (epoch, node_id), node in self.nodes.items():
            if (
                node.question is not None
                and node.asked_seq > node.answered_seq
                and not self.epoch_closed(epoch)
            ):
                pending.append(OwnerQuestion(epoch, node_id, node.question, node.options))
        return pending
    @property
    def results(self) -> dict[NodeKey, Result]:
        return {key: node.result for key, node in self.nodes.items() if node.result is not None}
    @property
    def integrated(self) -> dict[NodeKey, list[str]]:
        return {key: node.receipt.paths for key, node in self.nodes.items() if node.receipt}
    @property
    def receipts(self) -> dict[NodeKey, IntegrationReceipt]:
        return {key: node.receipt for key, node in self.nodes.items() if node.receipt}
    @property
    def dispatched(self) -> set[NodeKey]:
        return {key for key, node in self.nodes.items() if node.dispatched}
    @property
    def loop_iterations(self) -> dict[NodeKey, int]:
        return {key: node.loop_iterations for key, node in self.nodes.items() if node.loop_iterations}
    @property
    def loop_last_commit(self) -> dict[NodeKey, str | None]:
        return {
            key: node.loop_last_commit for key, node in self.nodes.items()
            if node.loop_last_iter_seq >= 0
        }
def replay(run_dir: Path) -> RunProjection:
    from wildflows.journal import Journal
    return Journal.load(run_dir).projection
