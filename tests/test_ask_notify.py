"""Regression coverage for owner notification after durable asks."""
from __future__ import annotations

import errno
import json
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path
from urllib.request import Request, urlopen

import pytest

from wildflows.engine import Engine
from wildflows.events import Answered, Asked, FramePushed
from wildflows.frame import AskRequest, AskResult, FrameResult, FrameRuntime, call_hash
from wildflows.rig import EchoRig, RigRegistry
from wildflows.run import Run
from wildflows.workspace import FrameWorktree
from tests.conftest import executable


class AskingRig:
    """A resident rig which parks once for every configured owner question."""

    timeout_s = 30.0

    def __init__(self, questions: list[str]) -> None:
        self.questions = questions
        self.answers: list[str] = []

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del prompt, workdir
        for call_index, question in enumerate(self.questions):
            payload = {
                "jsonrpc": "2.0",
                "id": call_index,
                "method": "tools/call",
                "params": {
                    "name": "ask",
                    "arguments": {"question": question},
                    "_meta": {"wildflows": {"callIndex": call_index}},
                },
            }
            request = Request(
                runtime.endpoint,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {runtime.token}",
                    "X-Wildflows-Frame": runtime.frame_id,
                },
            )
            with urlopen(request, timeout=10) as response:  # noqa: S310 - local MCP
                body = json.load(response)
            answer = body["result"]["structuredContent"]["answer"]
            assert isinstance(answer, str)
            self.answers.append(answer)
        return FrameResult(text="owner answered", exit_code=0)


def _wait_until(predicate: object, description: str) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if callable(predicate) and predicate():
            return
        time.sleep(0.01)
    pytest.fail(f"timed out waiting for {description}")


def _pending_question(engine: Engine, question: str) -> tuple[str, int]:
    with engine.journal.projection_transaction() as projection:
        pending = projection.pending_questions()
    if (
        len(pending) != 1
        or not isinstance(pending[0].request, AskRequest)
        or pending[0].request.question != question
    ):
        raise AssertionError(f"expected one pending question {question!r}, got {pending!r}")
    return pending[0].frame_id, pending[0].call_index


def _start(owner: Engine | Run) -> tuple[threading.Thread, list[object], list[BaseException]]:
    results: list[object] = []
    errors: list[BaseException] = []

    def drive() -> None:
        try:
            results.append(owner.run())
        except BaseException as exc:  # keep worker failures in the test thread
            errors.append(exc)

    worker = threading.Thread(target=drive, name="ask-notify-test")
    worker.start()
    return worker, results, errors


def _raise_worker_error(errors: list[BaseException]) -> None:
    if errors:
        raise errors[0]


def _finish_with_answers(
    owner: Engine | Run,
    engine: Engine,
    questions: list[str],
    *,
    before_answer: Callable[[int], None] | None = None,
) -> object:
    worker, results, errors = _start(owner)
    for index, question in enumerate(questions):
        _wait_until(
            lambda: bool(errors) or _has_pending_question(engine, question),
            f"pending ask {question!r}",
        )
        _raise_worker_error(errors)
        if before_answer is not None:
            before_answer(index)
        frame_id, call_index = _pending_question(engine, question)
        assert engine.answer(
            f"answer-{index}", frame_id=frame_id, call_index=call_index
        ) == ("f0", index)
    worker.join(timeout=10)
    assert not worker.is_alive(), "ask/run did not complete after its answers arrived"
    _raise_worker_error(errors)
    assert len(results) == 1
    result = results[0]
    assert getattr(result, "outcome") == "ok"
    return result


def _has_pending_question(engine: Engine, question: str) -> bool:
    with engine.journal.projection_transaction() as projection:
        pending = projection.pending_questions()
        return (
            len(pending) == 1
            and isinstance(pending[0].request, AskRequest)
            and pending[0].request.question == question
        )


class _DetachedProcess:
    """A fake process that makes waiting on the notification a test failure."""

    def wait(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        pytest.fail("owner notification must be fire-and-forget, never waited")

    def communicate(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        pytest.fail("owner notification must be fire-and-forget, never collected")


def _command_from_popen(args: tuple[object, ...], kwargs: dict[str, object]) -> list[str]:
    raw = args[0] if args else kwargs.get("args")
    assert isinstance(raw, (list, tuple))
    assert all(isinstance(part, str) for part in raw)
    return list(raw)


@pytest.mark.parametrize("scope", ["engine", "run"])
def test_notify_command_runs_once_per_new_durable_ask_with_owner_context(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scope: str
) -> None:
    questions = ["Should this land?", "Which release train?"]
    run_id = f"notify-{scope}"
    command = ["owner-notify", "--urgent"]
    rig = AskingRig(questions)
    launches: list[tuple[list[str], dict[str, object]]] = []
    engine: Engine

    def fake_popen(*args: object, **kwargs: object) -> _DetachedProcess:
        argv = _command_from_popen(args, kwargs)
        asks = [event for event in engine.journal.events() if isinstance(event, Asked)]
        # The durable record precedes the external wakeup, and there is one wakeup
        # for this newly appended record only.
        assert len(asks) == len(launches) + 1
        assert kwargs.get("start_new_session") is True
        assert kwargs.get("stdin") is subprocess.DEVNULL
        assert kwargs.get("stdout") is subprocess.DEVNULL
        assert kwargs.get("stderr") is subprocess.DEVNULL
        launches.append((argv, kwargs))
        return _DetachedProcess()

    monkeypatch.setattr("wildflows.engine.subprocess.Popen", fake_popen)
    registry = RigRegistry({"asker": rig})
    if scope == "engine":
        engine = Engine(
            tmp_path / "engine-run",
            repo,
            registry,
            run_id=run_id,
            root_rig="asker",
            root_prompt="ask the owner twice",
            worktrees_root=tmp_path / "worktrees",
            notify_command=command,
        )
        owner: Engine | Run = engine
    else:
        run_owner = Run(
            workdir=repo,
            job_spec="ask the owner twice",
            registry=registry,
            root_rig="asker",
            run_id=run_id,
            notify_command=command,
        )
        owner = run_owner
        engine = run_owner.engine

    def notification_arrived(index: int) -> None:
        _wait_until(
            lambda: len(launches) == index + 1,
            f"notification for ask {index}",
        )

    _finish_with_answers(owner, engine, questions, before_answer=notification_arrived)
    assert rig.answers == ["answer-0", "answer-1"]
    assert len(launches) == 2
    assert [
        event.request.question
        for event in engine.journal.events()
        if isinstance(event, Asked)
    ] == questions
    assert len([event for event in engine.journal.events() if isinstance(event, Answered)]) == 2

    for index, (argv, kwargs) in enumerate(launches):
        assert argv == [*command, questions[index], "f0", run_id]
        environment = kwargs.get("env")
        assert isinstance(environment, dict)
        assert environment["WILDFLOWS_QUESTION"] == questions[index]
        assert environment["WILDFLOWS_FRAME_ID"] == "f0"
        assert environment["WILDFLOWS_RUN_ID"] == run_id
        assert environment["WILDFLOWS_NOTIFY_QUESTION"] == questions[index]
        assert environment["WILDFLOWS_NOTIFY_FRAME_ID"] == "f0"
        assert environment["WILDFLOWS_NOTIFY_RUN_ID"] == run_id


def _durably_pending_ask(
    repo: Path, tmp_path: Path
) -> tuple[Engine, AskRequest, FrameWorktree]:
    run_dir = tmp_path / "pending-run"
    request = AskRequest(question="Replay this exact owner question")
    first = Engine(
        run_dir,
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id="pending-notify",
        root_rig="echo",
        root_prompt="pending ask",
        worktrees_root=tmp_path / "worktrees",
    )
    base = first.repository.branch_tip()
    branch = first.repository.frame_branch("f0")
    worktree = first.repository.create_frame_worktree("f0", branch, base, resume=False)
    first.journal.append(FramePushed(
        run_id=first.run_id,
        frame_id="f0",
        attempt=0,
        depth=0,
        rig="echo",
        prompt="pending ask",
        skills=[],
        branch=branch,
        base_commit=base,
        worktree=str(worktree.path),
    ))
    first.journal.append(Asked(
        run_id=first.run_id,
        frame_id="f0",
        call_index=0,
        call_hash=call_hash("ask", request),
        request=request,
    ))

    resumed = Engine(
        run_dir,
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id="pending-notify",
        root_rig="echo",
        root_prompt="pending ask",
        notify_command=["owner-notify"],
    )
    replay_worktree = resumed.repository.create_frame_worktree(
        "f0", branch, base, resume=True
    )
    with resumed._active_lock:  # noqa: SLF001 - install the durable active frame seam
        resumed._active["f0"] = replay_worktree  # noqa: SLF001
    return resumed, request, replay_worktree


def test_exact_replay_of_pending_ask_does_not_notify_again(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launches: list[list[str]] = []

    def fake_popen(*args: object, **kwargs: object) -> _DetachedProcess:
        del kwargs
        launches.append(_command_from_popen(args, {}))
        return _DetachedProcess()

    monkeypatch.setattr("wildflows.engine.subprocess.Popen", fake_popen)
    engine, request, _ = _durably_pending_ask(repo, tmp_path)
    results: list[AskResult] = []
    errors: list[BaseException] = []

    def replay() -> None:
        try:
            response = engine.handle_tool("f0", 0, "ask", request)
            assert isinstance(response, AskResult)
            results.append(response)
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=replay, name="exact-ask-replay")
    worker.start()
    assert engine.answer("replayed answer", frame_id="f0", call_index=0) == ("f0", 0)
    worker.join(timeout=10)
    assert not worker.is_alive()
    _raise_worker_error(errors)
    assert results == [AskResult(answer="replayed answer")]
    assert launches == []
    events = engine.journal.events()
    assert len([event for event in events if isinstance(event, Asked)]) == 1
    assert len([event for event in events if isinstance(event, Answered)]) == 1


def test_missing_notify_executable_does_not_prevent_ask_or_run_completion(
    repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempts: list[list[str]] = []

    def missing_popen(*args: object, **kwargs: object) -> _DetachedProcess:
        del kwargs
        argv = _command_from_popen(args, {})
        attempts.append(argv)
        raise FileNotFoundError(errno.ENOENT, "missing notify executable", argv[0])

    monkeypatch.setattr("wildflows.engine.subprocess.Popen", missing_popen)
    rig = AskingRig(["Can the missing notifier stop this run?"])
    owner = Run(
        workdir=repo,
        job_spec="prove notification errors are best effort",
        registry=RigRegistry({"asker": rig}),
        root_rig="asker",
        run_id="missing-notify",
        notify_command=["definitely-not-installed"],
    )

    _finish_with_answers(
        owner,
        owner.engine,
        rig.questions,
        before_answer=lambda index: _wait_until(
            lambda: len(attempts) == index + 1,
            "failed notification spawn attempt",
        ),
    )
    assert attempts == [[
        "definitely-not-installed",
        rig.questions[0],
        "f0",
        "missing-notify",
    ]]
    assert rig.answers == ["answer-0"]


def test_nonzero_notify_command_does_not_prevent_ask_or_run_completion(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "nonzero-notify-ran"
    notifier = executable(
        tmp_path / "nonzero-notify",
        "#!/bin/sh\nprintf launched > \"$NOTIFY_MARKER\"\nexit 29\n",
    )
    monkeypatch.setenv("NOTIFY_MARKER", str(marker))
    rig = AskingRig(["Does notifier status matter?"])
    owner = Run(
        workdir=repo,
        job_spec="a nonzero owner notifier is informational only",
        registry=RigRegistry({"asker": rig}),
        root_rig="asker",
        run_id="nonzero-notify",
        notify_command=[str(notifier)],
    )

    _finish_with_answers(
        owner,
        owner.engine,
        rig.questions,
        before_answer=lambda _index: _wait_until(
            marker.is_file, "nonzero notification process launch"
        ),
    )
    assert marker.read_text(encoding="utf-8") == "launched"
    assert rig.answers == ["answer-0"]
