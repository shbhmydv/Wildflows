"""Standalone v2 frame supervisor and the three engine tool implementations."""
from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

from wildflows.admission import AdmissionError, AdmissionPolicy, admit_dispatch
from wildflows.events import (
    Answered,
    Asked,
    DispatchCalled,
    DispatchReturned,
    FrameExited,
    FrameIntegrated,
    FrameIntegrating,
    FramePopped,
    FramePushed,
    GateCalled,
    GateReturned,
    RunFinished,
    RunOpened,
)
from wildflows.frame import (
    AskRequest,
    AskResult,
    ChildResult,
    DispatchRequest,
    DispatchResult,
    FrameOutcome,
    FrameResult,
    FrameRuntime,
    GateRequest,
    GateResult,
    ToolName,
    ToolRequest,
    ToolResponse,
    call_hash,
)
from wildflows.journal import Journal
from wildflows.mcp import MCPServer
from wildflows.projection import FrameProjection, RunProjection
from wildflows.result import CommitReceipt
from wildflows.rig import RigRegistry, run_shell
from wildflows.shim import write_pi_shim
from wildflows.workspace import (
    FrameWorktree,
    IntegrationError,
    Repository,
    RepositoryError,
)


class CallConflictError(RuntimeError):
    """A frame reused a logical call index for different content."""


class FrameNotActiveError(RuntimeError):
    """A token-authenticated request did not name a live caller frame."""


class Engine:
    """One run's append owner, MCP service, frame stack, and serialized integrator."""

    ROOT_FRAME_ID = "f0"

    def __init__(
        self,
        run_dir: Path,
        workdir: Path,
        registry: RigRegistry,
        *,
        run_id: str,
        root_rig: str,
        root_prompt: str,
        run_branch: str | None = None,
        policy: AdmissionPolicy | None = None,
        worktrees_root: Path | None = None,
    ) -> None:
        self.run_dir = Path(run_dir).resolve()
        self.registry = registry
        self.run_id = run_id
        self._active: dict[str, FrameWorktree] = {}
        self._active_lock = threading.RLock()
        self._integration_lock = threading.RLock()
        self._answer_condition = threading.Condition(threading.RLock())
        journal_path = self.run_dir / "events.ndjson"
        continuing = journal_path.exists() and journal_path.stat().st_size > 0
        self.journal = Journal.load(self.run_dir) if continuing else Journal(self.run_dir)
        opened = self.journal.projection.opened
        if continuing:
            if opened is None:
                raise RuntimeError("v2 journal has no run_opened event")
            if opened.run_id != run_id:
                raise ValueError("run id differs from durable run")
            if opened.root_prompt != root_prompt:
                raise ValueError("resumed job spec differs from durable root prompt")
            if opened.root_rig != root_rig:
                raise ValueError("resumed root rig differs from durable run")
            durable_worktrees = Path(opened.worktrees_root)
            if worktrees_root is not None and Path(worktrees_root).resolve() != durable_worktrees:
                raise ValueError("resumed worktrees root differs from durable run")
            self.policy = opened.policy
            self.repository = Repository(
                workdir,
                self.run_dir,
                run_id,
                run_branch=opened.run_branch,
                worktrees_root=durable_worktrees,
            )
            if str(self.repository.root) != opened.repository:
                raise ValueError("resumed repository differs from durable run")
            if run_branch is not None:
                requested = run_branch.removeprefix("refs/heads/")
                if requested != opened.run_branch:
                    raise ValueError("resumed run branch differs from durable run")
            self._verify_integrations()
        else:
            self.policy = policy or AdmissionPolicy()
            self.repository = Repository(
                workdir,
                self.run_dir,
                run_id,
                run_branch=run_branch,
                worktrees_root=worktrees_root,
            )
            if root_rig not in registry:
                raise ValueError(f"root rig {root_rig!r} is not in the rig allowlist")
            started = time.time()
            self.journal.append(RunOpened(
                run_id=run_id,
                repository=str(self.repository.root),
                run_branch=self.repository.branch,
                base_commit=self.repository.branch_tip(),
                root_frame_id=self.ROOT_FRAME_ID,
                root_rig=root_rig,
                root_prompt=root_prompt,
                worktrees_root=str(self.repository.worktrees_root),
                started_at=started,
                policy=self.policy,
            ))
        self.server = MCPServer(self)

    @property
    def projection(self) -> RunProjection:
        return self.journal.projection

    def _verify_integrations(self) -> None:
        for frame in self.journal.projection.frames.values():
            integrated = frame.integrated
            if integrated is None:
                continue
            self.repository.verify_receipt(
                integrated.integration_base, integrated.landed_commits
            )
            if integrated.landed_commits:
                target_ref = (
                    self.repository.ref
                    if integrated.target_frame_id is None
                    else self.journal.projection.frame(integrated.target_frame_id).branch
                )
                target_tip = self.repository.branch_tip(target_ref)
                candidate = integrated.candidate_head
                if self.repository.git(
                    ["merge-base", "--is-ancestor", candidate, target_tip], check=False
                ).returncode:
                    raise RepositoryError(
                        f"integrated frame {frame.frame_id!r} is absent from its target branch"
                    )

    def run(self) -> FrameResult:
        """Start or replay the root stack and return its terminal frame result."""
        finished = self.journal.projection.finished
        if finished is not None:
            return FrameResult(
                outcome=finished.outcome,
                text=finished.text,
                exit_code=0 if finished.outcome == "ok" else 1,
            )
        opened = self.journal.projection.opened
        assert opened is not None
        with self.server:
            root = self.journal.projection.frames.get(self.ROOT_FRAME_ID)
            if root is None or root.outcome is None:
                root = self._launch_frame(
                    frame_id=self.ROOT_FRAME_ID,
                    parent_frame_id=None,
                    parent_call_index=None,
                    task_index=None,
                    depth=0,
                    rig=opened.root_rig,
                    prompt=opened.root_prompt,
                    base_commit=opened.base_commit,
                    subtree_deadline=opened.started_at + self.policy.subtree_timeout_s,
                )
            if root.outcome == "ok" and root.integrated is None:
                try:
                    self._integrate_frame(root.frame_id, target_frame_id=None, owned=set())
                except RepositoryError as exc:
                    root.outcome = "failed"
                    root.text = f"root integration failed: {exc}"
            self._pop_once(root, root.outcome or "failed")
            root_head = root.head or self.repository.branch_tip(root.branch)
            outcome = root.outcome or "failed"
            self.journal.append(RunFinished(
                run_id=self.run_id,
                outcome=outcome,
                root_head=root_head,
                text=root.text,
            ))
            return FrameResult(
                outcome=outcome,
                text=root.text,
                exit_code=root.exit_code,
                stdout=root.stdout,
                stderr=root.stderr,
            )

    def handle_tool(
        self,
        frame_id: str,
        call_index: int,
        tool: ToolName,
        request: ToolRequest,
    ) -> ToolResponse:
        worktree = self._active_worktree(frame_id)
        frame = self.journal.projection.frame(frame_id)
        digest = call_hash(tool, request)
        existing = self.journal.projection.call(frame_id, call_index)
        if existing is not None:
            if existing.tool != tool or existing.call_hash != digest:
                raise CallConflictError(
                    f"call {frame_id}:{call_index} content differs from its durable call"
                )
            if existing.response is not None:
                return existing.response
        elif call_index != self.journal.projection.next_call_index(frame_id):
            raise CallConflictError(
                f"call {frame_id}:{call_index} is not the next logical call"
            )

        if tool == "dispatch":
            if not isinstance(request, DispatchRequest):
                raise TypeError("dispatch received the wrong request model")
            return self._dispatch(frame, worktree, call_index, digest, request, existing is not None)
        if tool == "gate":
            if not isinstance(request, GateRequest):
                raise TypeError("gate received the wrong request model")
            return self._gate(frame, worktree, call_index, digest, request, existing is not None)
        if not isinstance(request, AskRequest):
            raise TypeError("ask received the wrong request model")
        return self._ask(frame, worktree, call_index, digest, request, existing is not None)

    def _active_worktree(self, frame_id: str) -> FrameWorktree:
        with self._active_lock:
            try:
                return self._active[frame_id]
            except KeyError as exc:
                raise FrameNotActiveError(f"frame {frame_id!r} is not active") from exc

    def _dispatch(
        self,
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: DispatchRequest,
        replaying: bool,
    ) -> DispatchResult:
        if replaying:
            self.repository.ensure_clean(worktree.path, frame.branch)
        else:
            caller_head = self.repository.ensure_clean(worktree.path, frame.branch)
            descendants = self.journal.projection.descendants(frame.frame_id)
            spend = sum(self.policy.rig_cost(item.rig) for item in descendants)
            try:
                admit_dispatch(
                    request,
                    caller_depth=frame.depth,
                    subtree_frames=len(descendants),
                    subtree_spend=spend,
                    subtree_deadline=frame.subtree_deadline,
                    policy=self.policy,
                    registry=self.registry,
                )
            except AdmissionError as exc:
                self.journal.append(DispatchCalled(
                    run_id=self.run_id,
                    frame_id=frame.frame_id,
                    call_index=call_index,
                    call_hash=digest,
                    request=request,
                    caller_head=caller_head,
                ))
                refused = DispatchResult(
                    outcome="refused",
                    error_code=exc.code,
                    message=str(exc),
                )
                self.journal.append(DispatchReturned(
                    run_id=self.run_id,
                    frame_id=frame.frame_id,
                    call_index=call_index,
                    call_hash=digest,
                    result=refused,
                ))
                return refused
            self.journal.append(DispatchCalled(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                request=request,
                caller_head=caller_head,
            ))

        results = (
            self._parallel_children(frame, worktree, call_index, request)
            if request.parallel and len(request.tasks) > 1
            else self._serial_children(frame, worktree, call_index, request)
        )
        outcome: FrameOutcome = (
            "ok" if all(result.outcome == "ok" for result in results) else "failed"
        )
        returned = DispatchResult(outcome=outcome, children=results)
        self.journal.append(DispatchReturned(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            result=returned,
        ))
        return returned

    def _serial_children(
        self,
        parent: FrameProjection,
        parent_worktree: FrameWorktree,
        call_index: int,
        request: DispatchRequest,
    ) -> list[ChildResult]:
        results: list[ChildResult] = []
        for task_index, task in enumerate(request.tasks):
            child = self._execute_child(
                parent, call_index, task_index, task, request.rig,
                base_commit=self.repository.branch_tip(parent.branch),
            )
            results.append(self._finish_child(
                child, parent, parent_worktree, owned=set()
            ))
        return results

    def _parallel_children(
        self,
        parent: FrameProjection,
        parent_worktree: FrameWorktree,
        call_index: int,
        request: DispatchRequest,
    ) -> list[ChildResult]:
        base = self.repository.branch_tip(parent.branch)
        by_index: dict[int, ChildResult] = {}
        owned: set[str] = set()
        with ThreadPoolExecutor(max_workers=len(request.tasks)) as executor:
            futures: dict[Future[FrameProjection], int] = {}
            for task_index, task in enumerate(request.tasks):
                future = executor.submit(
                    self._execute_child,
                    parent,
                    call_index,
                    task_index,
                    task,
                    request.rig,
                    base_commit=base,
                )
                futures[future] = task_index
            for future in as_completed(futures):
                task_index = futures[future]
                try:
                    child = future.result()
                    result = self._finish_child(
                        child, parent, parent_worktree, owned=owned
                    )
                    if result.outcome == "ok":
                        owned.update(path for commit in result.commits for path in commit.paths)
                except Exception as exc:
                    frame_id = self._child_id(parent.frame_id, call_index, task_index)
                    result = ChildResult(
                        frame_id=frame_id,
                        outcome="failed",
                        text=f"child execution failed: {exc}",
                    )
                by_index[task_index] = result
        return [by_index[index] for index in range(len(request.tasks))]

    def _execute_child(
        self,
        parent: FrameProjection,
        call_index: int,
        task_index: int,
        task: str,
        rig: str,
        *,
        base_commit: str,
    ) -> FrameProjection:
        frame_id = self._child_id(parent.frame_id, call_index, task_index)
        existing = self.journal.projection.frames.get(frame_id)
        if existing is not None:
            if (
                existing.parent_frame_id != parent.frame_id
                or existing.parent_call_index != call_index
                or existing.task_index != task_index
                or existing.prompt != task
                or existing.rig != rig
            ):
                raise CallConflictError(f"durable child identity differs for {frame_id}")
            if existing.outcome is not None:
                return existing
        return self._launch_frame(
            frame_id=frame_id,
            parent_frame_id=parent.frame_id,
            parent_call_index=call_index,
            task_index=task_index,
            depth=parent.depth + 1,
            rig=rig,
            prompt=task,
            base_commit=existing.base_commit if existing is not None else base_commit,
            subtree_deadline=parent.subtree_deadline,
        )

    @staticmethod
    def _child_id(parent: str, call_index: int, task_index: int) -> str:
        return f"{parent}.c{call_index}.t{task_index}"

    def _finish_child(
        self,
        child: FrameProjection,
        parent: FrameProjection,
        parent_worktree: FrameWorktree,
        *,
        owned: set[str],
    ) -> ChildResult:
        if child.outcome != "ok":
            self._pop_once(child, child.outcome or "failed")
            return self._child_result(child, [])
        try:
            integrated = child.integrated
            if integrated is None:
                integrated = self._integrate_frame(
                    child.frame_id,
                    target_frame_id=parent.frame_id,
                    owned=owned,
                    target_worktree=parent_worktree.path,
                )
            paths = {path for commit in integrated.source_commits for path in commit.paths}
            overlap = paths.intersection(owned)
            if overlap:
                raise IntegrationError(
                    f"parallel sibling path ownership overlaps: {', '.join(sorted(overlap))}"
                )
            self._pop_once(child, "ok")
            return self._child_result(child, integrated.landed_commits)
        except RepositoryError as exc:
            self._pop_once(child, "failed")
            return ChildResult(
                frame_id=child.frame_id,
                outcome="failed",
                text=f"integration failed: {exc}",
                exit_code=child.exit_code,
            )

    def _child_result(
        self, child: FrameProjection, commits: list[CommitReceipt]
    ) -> ChildResult:
        return ChildResult(
            frame_id=child.frame_id,
            outcome=child.outcome or "failed",
            text=child.text,
            exit_code=child.exit_code,
            commits=commits,
        )

    def _launch_frame(
        self,
        *,
        frame_id: str,
        parent_frame_id: str | None,
        parent_call_index: int | None,
        task_index: int | None,
        depth: int,
        rig: str,
        prompt: str,
        base_commit: str,
        subtree_deadline: float,
    ) -> FrameProjection:
        existing = self.journal.projection.frames.get(frame_id)
        branch = existing.branch if existing is not None else self.repository.frame_branch(frame_id)
        resume = existing is not None or self.repository.ref_exists(branch)
        worktree = self.repository.create_frame_worktree(
            frame_id, branch, base_commit, resume=resume
        )
        attempt = 0 if existing is None else existing.push_count
        self.journal.append(FramePushed(
            run_id=self.run_id,
            frame_id=frame_id,
            parent_frame_id=parent_frame_id,
            parent_call_index=parent_call_index,
            task_index=task_index,
            attempt=attempt,
            depth=depth,
            rig=rig,
            prompt=prompt,
            branch=branch,
            base_commit=base_commit,
            worktree=str(worktree.path),
            subtree_deadline=subtree_deadline,
        ))
        with self._active_lock:
            self._active[frame_id] = worktree
        try:
            runtime_dir = self.run_dir / "runtime" / frame_id / f"attempt-{attempt}"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            shim = write_pi_shim(
                runtime_dir,
                self.server.endpoint,
                self.server.token,
                frame_id,
                self.journal.projection.next_call_index(frame_id),
            )
            runtime = FrameRuntime(
                endpoint=self.server.endpoint,
                token=self.server.token,
                frame_id=frame_id,
                shim_path=shim,
                runtime_dir=runtime_dir,
                next_call_index=self.journal.projection.next_call_index(frame_id),
            )
            result = self.registry.resolve(rig).run(
                self._frame_prompt(frame_id, prompt), worktree.path, runtime
            )
            if result.outcome == "ok":
                try:
                    head = self.repository.commit_all(
                        worktree.path, f"wildflows frame {frame_id}"
                    )
                except RepositoryError as exc:
                    result = FrameResult(
                        outcome="failed",
                        text=f"frame commit failed: {exc}",
                        exit_code=result.exit_code,
                        stdout=result.stdout,
                        stderr=result.stderr,
                    )
                    head = self.repository.head(worktree.path)
            else:
                head = self.repository.head(worktree.path)
            self.journal.append(FrameExited(
                run_id=self.run_id,
                frame_id=frame_id,
                attempt=attempt,
                outcome=result.outcome,
                text=result.text,
                exit_code=result.exit_code,
                stdout=result.stdout,
                stderr=result.stderr,
                head=head,
            ))
            return self.journal.projection.frame(frame_id)
        except Exception as exc:
            head = self.repository.head(worktree.path)
            self.journal.append(FrameExited(
                run_id=self.run_id,
                frame_id=frame_id,
                attempt=attempt,
                outcome="failed",
                text=f"frame rig failed: {exc}",
                stderr=str(exc),
                head=head,
            ))
            return self.journal.projection.frame(frame_id)
        finally:
            with self._active_lock:
                self._active.pop(frame_id, None)
            self.repository.remove_worktree(worktree)

    def _frame_prompt(self, frame_id: str, original: str) -> str:
        digest = self.journal.projection.resume_digest(frame_id)
        resume = bool(digest) or self.journal.projection.frame(frame_id).push_count > 1
        preamble = (
            "You are a WILDFLOWS frame. Work only in your CWD. Commit useful changes "
            "before calling an engine tool or exiting. The only engine tools are "
            "wildflows_dispatch, wildflows_gate, and wildflows_ask. Tool calls block; "
            "child commits are present in your branch when dispatch returns.\n"
        )
        if resume:
            preamble += (
                "\nRESUME REPLAY: completed calls below are durable and must not be paid "
                "for again. Do not re-issue completed calls; continue from their results. "
                "Re-issue an exact pending call to reconnect the durable stack. If a "
                "completed call is accidentally re-issued with its original logical index "
                "and content, the engine returns its memoized result.\n"
                f"RESUME_DIGEST={json.dumps(digest, sort_keys=True, separators=(',', ':'))}\n"
            )
        return f"{preamble}\n--- ORIGINAL FRAME PROMPT ---\n{original}"

    def _integrate_frame(
        self,
        frame_id: str,
        *,
        target_frame_id: str | None,
        owned: set[str],
        target_worktree: Path | None = None,
    ) -> FrameIntegrated:
        with self._integration_lock:
            frame = self.journal.projection.frame(frame_id)
            if frame.integrated is not None:
                return frame.integrated
            if frame.head is None:
                raise IntegrationError("cannot integrate a frame with no exit head")
            target_ref = (
                self.repository.ref
                if target_frame_id is None
                else self.journal.projection.frame(target_frame_id).branch
            )
            if target_worktree is None:
                target_worktree = self.repository.checked_out_owner(target_ref)
            intent = frame.integrating
            if intent is None:
                source = self.repository.receipt(frame.base_commit, frame.head)
                overlap = set(source.paths).intersection(owned)
                if overlap:
                    raise IntegrationError(
                        f"parallel sibling path ownership overlaps: {', '.join(sorted(overlap))}"
                    )
                moving_base = self.repository.branch_tip(target_ref)
                if moving_base == frame.base_commit:
                    candidate = frame.head
                    landed = source
                else:
                    candidate, landed = self.repository.reapply(
                        source.commits, moving_base
                    )
                intent = FrameIntegrating(
                    run_id=self.run_id,
                    frame_id=frame_id,
                    target_frame_id=target_frame_id,
                    integration_base=moving_base,
                    candidate_head=candidate,
                    source_commits=source.commits,
                    landed_commits=landed.commits,
                )
                self.journal.append(intent)
            self.repository.advance(
                target_ref,
                intent.integration_base,
                intent.candidate_head,
                target_worktree=target_worktree,
            )
            integrated = FrameIntegrated(
                run_id=self.run_id,
                frame_id=frame_id,
                target_frame_id=target_frame_id,
                integration_base=intent.integration_base,
                candidate_head=intent.candidate_head,
                source_commits=intent.source_commits,
                landed_commits=intent.landed_commits,
            )
            self.journal.append(integrated)
            return integrated

    def _pop_once(self, frame: FrameProjection, outcome: FrameOutcome) -> None:
        if frame.popped:
            return
        self.journal.append(FramePopped(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            attempt=frame.attempt,
            outcome=outcome,
        ))

    def _gate(
        self,
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: GateRequest,
        replaying: bool,
    ) -> GateResult:
        caller_head = self.repository.ensure_clean(worktree.path, frame.branch)
        if not replaying:
            self.journal.append(GateCalled(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                request=request,
                caller_head=caller_head,
            ))
        remaining = max(0.01, frame.subtree_deadline - time.time())
        result = run_shell(request.cmd, worktree.path, remaining)
        if result.timed_out:
            gate = GateResult(
                exit_code=124,
                stdout=result.stdout,
                stderr=result.stderr + f"\n[timeout] gate exceeded {remaining:g}s",
            )
        else:
            assert result.returncode is not None
            gate = GateResult(
                exit_code=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        self.journal.append(GateReturned(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            result=gate,
        ))
        return gate

    def _ask(
        self,
        frame: FrameProjection,
        worktree: FrameWorktree,
        call_index: int,
        digest: str,
        request: AskRequest,
        replaying: bool,
    ) -> AskResult:
        if not replaying:
            self.repository.ensure_clean(worktree.path, frame.branch)
            self.journal.append(Asked(
                run_id=self.run_id,
                frame_id=frame.frame_id,
                call_index=call_index,
                call_hash=digest,
                request=request,
            ))
        answer_path = self._answer_path(frame.frame_id, call_index)
        with self._answer_condition:
            while not answer_path.is_file():
                self._answer_condition.wait(timeout=0.25)
        answer = answer_path.read_text(encoding="utf-8")
        self.journal.append(Answered(
            run_id=self.run_id,
            frame_id=frame.frame_id,
            call_index=call_index,
            call_hash=digest,
            answer=answer,
        ))
        return AskResult(answer=answer)

    def _answer_path(self, frame_id: str, call_index: int) -> Path:
        safe = frame_id.replace("/", "-")
        return self.run_dir / "answers" / f"{safe}-{call_index}.txt"

    def answer(
        self,
        answer: str,
        *,
        frame_id: str | None = None,
        call_index: int | None = None,
    ) -> tuple[str, int]:
        pending = self.journal.projection.pending_questions()
        selected = [
            call for call in pending
            if (frame_id is None or call.frame_id == frame_id)
            and (call_index is None or call.call_index == call_index)
        ]
        if len(selected) != 1:
            raise ValueError(f"answer target is ambiguous or absent ({len(selected)} matches)")
        call = selected[0]
        path = self._answer_path(call.frame_id, call.call_index)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(answer, encoding="utf-8")
        os.replace(temporary, path)
        with self._answer_condition:
            self._answer_condition.notify_all()
        return call.frame_id, call.call_index
