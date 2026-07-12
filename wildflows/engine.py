"""The minimal engine (ladder step 1) + replay.

Executes an expression tree of do/inplace nodes sequentially, journalling every event
in the single vocabulary. Effects are core-mediated: inplace writes + commits are the
core's, never the model's. `replay` reconstructs run state from the ndjson alone,
proving resume = fold-the-journal (no per-shape resume code).

Only `do` and `inplace` are executable here; `dispatch` is the sequence container that
walks its children in order. Combine/loop/ask/setup raise NotImplementedError — they
are representable (expr.py) and journal-ready (events.py) but land at later ladder steps.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from wildflows.events import Boundary, Dispatched, Integrated, ResultEvent
from wildflows.expr import Dispatch, Do, Expr, Inplace, assign_node_ids
from wildflows.journal import Journal
from wildflows.rig import RigRegistry, Result


class Engine:
    def __init__(self, run_dir: Path, workdir: Path, registry: RigRegistry) -> None:
        self.run_dir = Path(run_dir)
        self.workdir = Path(workdir)
        self.registry = registry
        self.journal = Journal(self.run_dir)
        self.run_id = self.run_dir.name

    def run_epoch(self, tree: Expr, epoch: int) -> None:
        """Admit the tree (assign node ids), open a boundary, execute, close."""
        assign_node_ids(tree)
        self.journal.append(
            Boundary(
                run_id=self.run_id,
                epoch=epoch,
                node_id=tree.node_id,
                phase="opened",
                expr=tree.model_dump(),
            )
        )
        self._exec(tree, epoch)
        self.journal.append(
            Boundary(
                run_id=self.run_id,
                epoch=epoch,
                node_id=tree.node_id,
                phase="closed",
                reason="done",
            )
        )

    def _exec(self, node: Expr, epoch: int) -> None:
        if isinstance(node, Dispatch):
            for child in node.children:  # PoC: sequential, deterministic order
                self._exec(child, epoch)
        elif isinstance(node, Do):
            self._exec_do(node, epoch)
        elif isinstance(node, Inplace):
            self._exec_inplace(node, epoch)
        else:
            raise NotImplementedError(f"{node.kind} is not executable in the PoC")

    def _exec_do(self, node: Do, epoch: int) -> None:
        self.journal.append(
            Dispatched(
                run_id=self.run_id,
                epoch=epoch,
                node_id=node.node_id,
                rig=node.rig.name,
                task=node.task,
                workdir=str(self.workdir),
            )
        )
        rig = self.registry.resolve(node.rig.name)
        result = rig.run(node.task, self.workdir)
        self._journal_result(node.node_id, epoch, result)

    def _exec_inplace(self, node: Inplace, epoch: int) -> None:
        self.journal.append(
            Dispatched(
                run_id=self.run_id,
                epoch=epoch,
                node_id=node.node_id,
                task=f"inplace: {len(node.edits)} edit(s)",
                workdir=str(self.workdir),
            )
        )
        paths: list[str] = []
        for edit in node.edits:
            target = (self.workdir / edit.path).resolve()
            if not str(target).startswith(str(self.workdir.resolve())):
                raise ValueError(f"inplace edit escapes workdir: {edit.path}")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(edit.content, encoding="utf-8")
            paths.append(edit.path)
        commit = self._commit(paths, f"inplace {node.node_id}")
        if commit is not None:
            self.journal.append(
                Integrated(
                    run_id=self.run_id,
                    epoch=epoch,
                    node_id=node.node_id,
                    commit=commit,
                    paths=paths,
                )
            )
        self._journal_result(
            node.node_id,
            epoch,
            Result(text=f"wrote {', '.join(paths)}", files=paths, ok=commit is not None),
        )

    def _commit(self, paths: list[str], message: str) -> str | None:
        """Core-mediated commit; returns the commit sha, or None if nothing staged."""
        subprocess.run(["git", "add", *paths], cwd=self.workdir, check=True)
        status = subprocess.run(
            ["git", "status", "--porcelain"], cwd=self.workdir, capture_output=True, text=True
        ).stdout
        if not status.strip():
            return None
        subprocess.run(["git", "commit", "-q", "-m", message], cwd=self.workdir, check=True)
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=self.workdir, capture_output=True, text=True
        ).stdout.strip()

    def _journal_result(self, node_id: str, epoch: int, result: Result) -> None:
        self.journal.append(
            ResultEvent(
                run_id=self.run_id,
                epoch=epoch,
                node_id=node_id,
                ok=result.ok,
                text=result.text,
                files=result.files,
                exit_code=result.exit_code,
            )
        )


class ReplayState:
    """Per-node run state folded from the journal (resume + dashboard consume this)."""

    def __init__(self) -> None:
        self.results: dict[str, Result] = {}
        self.integrated: dict[str, list[str]] = {}
        self.dispatched: set[str] = set()
        self._closed_epochs: set[int] = set()

    def epoch_closed(self, epoch: int) -> bool:
        return epoch in self._closed_epochs


def replay(run_dir: Path) -> ReplayState:
    """Reconstruct run state from the ndjson alone — the single resume/dashboard path."""
    journal = Journal.load(run_dir)
    state = ReplayState()
    for ev in journal.events():
        if isinstance(ev, Dispatched):
            state.dispatched.add(ev.node_id)
        elif isinstance(ev, ResultEvent):
            state.results[ev.node_id] = Result(
                text=ev.text, files=ev.files, ok=ev.ok, exit_code=ev.exit_code
            )
        elif isinstance(ev, Integrated):
            state.integrated[ev.node_id] = ev.paths
        elif isinstance(ev, Boundary) and ev.phase == "closed":
            state._closed_epochs.add(ev.epoch)
    return state
