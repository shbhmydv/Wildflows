"""Planner boundary, run-loop, rails, macro, and real-crash scenarios."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.test_engine import init_repo
from wildflows.planner import AwaitingOwner, PlannerFailure, RailStop
from wildflows.result import Result
from wildflows.rig import EchoRig, RigRegistry
from wildflows.run import Run, RunCompleted


class CannedPlanner:
    def __init__(self, decisions: list[str]) -> None:
        self.decisions = decisions
        self.calls = 0
        self.prompts: list[str] = []

    def run(self, prompt: str, workdir: Path) -> Result:
        self.prompts.append(prompt)
        decision = self.decisions[self.calls]
        self.calls += 1
        return Result(text=decision)


def decision(
    expression: dict[str, object] | None,
    *,
    end: bool = False,
    deadline_s: float | None = 60,
    max_epochs: int | None = 5,
    rationale: str = "next",
    final_summary: str | None = None,
) -> str:
    return json.dumps({
        "expression": expression,
        "rails": {
            "deadline_s": deadline_s,
            "max_epochs": max_epochs,
            "budget_notes": "no token accounting in M4",
        },
        "rationale": rationale,
        "end": end,
        "final_summary": final_summary,
    })


def make_run(
    tmp_path: Path, planner: CannedPlanner, *, run_id: str = "run-1"
) -> Run:
    repo = tmp_path / "repo"
    if not repo.exists():
        init_repo(repo)
    registry = RigRegistry({"planner": planner, "echo": EchoRig()})
    return Run(
        workdir=repo,
        job_spec="# Build it\nMake the requested change.",
        registry=registry,
        planner_rig="planner",
        run_id=run_id,
    )


def test_run_dir_rejects_dotdot_and_symlink_escape(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    planner = CannedPlanner([])
    registry = RigRegistry({"planner": planner})
    with pytest.raises(ValueError, match="path component"):
        Run(
            workdir=repo, job_spec="x", registry=registry,
            planner_rig="planner", run_id="..",
        )
    (repo / ".wildflows").symlink_to(tmp_path / "outside", target_is_directory=True)
    with pytest.raises(ValueError, match="escapes"):
        Run(
            workdir=repo, job_spec="x", registry=registry,
            planner_rig="planner", run_id="safe",
        )


def test_scripted_planner_runs_two_epochs_then_ends(tmp_path: Path) -> None:
    planner = CannedPlanner([
        decision({
            "kind": "inplace",
            "edits": [{"path": "built.txt", "content": "epoch zero"}],
        }, rationale="build"),
        decision({
            "kind": "do",
            "task": "inspect epoch zero",
            "rig": {"name": "echo"},
            "ctx": [{"kind": "file", "ref": "built.txt"}],
        }, rationale="inspect"),
        decision(None, end=True, rationale="done", final_summary="shipped"),
    ])
    run = make_run(tmp_path, planner)

    completed = run.run()

    assert completed == RunCompleted(summary="shipped", epochs=2)
    assert planner.calls == 3
    assert run.run_dir == run.workdir / ".wildflows" / "runs" / "run-1"
    assert (run.workdir / "built.txt").read_text(encoding="utf-8") == "epoch zero"
    saved = sorted((run.run_dir / "decisions").glob("*.json"))
    assert [path.read_text(encoding="utf-8") for path in saved] == planner.decisions
    assert '"node_id":"n0"' in planner.prompts[1]
    assert "artifacts/e0-n0/" in planner.prompts[1]


def test_malformed_planner_decision_is_typed_and_retryable(tmp_path: Path) -> None:
    malformed = '{"expression": {"kind": "not-real"}}'
    planner = CannedPlanner([
        malformed,
        decision(None, end=True, final_summary="recovered"),
    ])
    run = make_run(tmp_path, planner)

    with pytest.raises(PlannerFailure) as caught:
        run.run()
    assert caught.value.retryable
    assert caught.value.decision_path.read_text(encoding="utf-8") == malformed

    assert run.resume() == RunCompleted(summary="recovered", epochs=0)
    assert planner.calls == 2


def test_ending_decision_rejects_ignored_expression(tmp_path: Path) -> None:
    planner = CannedPlanner([decision(
        {"kind": "inplace", "edits": []}, end=True, final_summary="not valid"
    )])
    with pytest.raises(PlannerFailure, match="expression=null"):
        make_run(tmp_path, planner).run()


def test_run_resume_answer_file_completes_ask_without_replanning_epoch(
    tmp_path: Path,
) -> None:
    planner = CannedPlanner([
        decision({"kind": "ask", "question": "Proceed?"}),
        decision(None, end=True, final_summary="owner answered"),
    ])
    run = make_run(tmp_path, planner, run_id="ask-run")
    with pytest.raises(AwaitingOwner):
        run.run()
    assert planner.calls == 1

    answer = tmp_path / "answer.txt"
    answer.write_text("yes", encoding="utf-8")
    resumed = make_run(tmp_path, planner, run_id="ask-run")
    assert resumed.resume(answer_file=answer).summary == "owner answered"
    assert planner.calls == 2
    assert resumed.engine.journal.projection.results[(0, "n0")].text == "yes"


def test_macro_names_and_descriptions_reach_planner_input(tmp_path: Path) -> None:
    planner = CannedPlanner([decision(None, end=True, final_summary="done")])
    run = make_run(tmp_path, planner)
    user_macros = run.run_dir.parent / "macros"
    user_macros.mkdir(parents=True)
    (user_macros / "owner.json").write_text(json.dumps({
        "name": "owner-shape",
        "description": "A project-specific nudge.",
        "parameters": {},
        "expression": {"kind": "inplace", "edits": []},
    }), encoding="utf-8")

    run.run()

    prompt = planner.prompts[0]
    assert "owner-shape" in prompt
    assert "A project-specific nudge." in prompt
    assert "senior-loop" in prompt
    assert "swarm-judge" in prompt
    assert '"path":' in prompt


def test_max_epochs_rail_stops_and_replays_without_planner_call(tmp_path: Path) -> None:
    planner = CannedPlanner([
        decision({"kind": "inplace", "edits": []}, max_epochs=1),
    ])
    run = make_run(tmp_path, planner)

    with pytest.raises(RailStop) as first:
        run.run()
    assert first.value.rail == "max_epochs"
    assert first.value.epoch == 1
    assert planner.calls == 1

    run = make_run(tmp_path, planner)
    with pytest.raises(RailStop) as resumed:
        run.resume()
    assert resumed.value.epoch == 1
    assert planner.calls == 1


def test_deadline_stops_open_epoch_and_resume_keeps_same_expression(tmp_path: Path) -> None:
    planner = CannedPlanner([
        decision({
            "kind": "seq",
            "children": [
                {"kind": "setup", "cmd": "sleep 0.2; printf ready > setup-ready"},
                {"kind": "do", "task": "must not start", "rig": {"name": "echo"}},
            ],
        }, deadline_s=0.1),
    ])
    run = make_run(tmp_path, planner)

    with pytest.raises(RailStop) as first:
        run.run()
    assert first.value.rail == "deadline_s"
    assert (run.workdir / "setup-ready").read_text(encoding="utf-8") == "ready"
    assert planner.calls == 1

    run = make_run(tmp_path, planner)
    with pytest.raises(RailStop):
        run.resume()
    assert planner.calls == 1
    assert not any(
        event.kind == "dispatched" and event.node_id == "n0.1"
        for event in run.engine.journal.events()
    )


def test_real_process_crash_resumes_open_expression_before_planning_again(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    calls = tmp_path / "planner-calls"

    class FilePlanner:
        def run(self, prompt: str, workdir: Path) -> Result:
            count = int(calls.read_text(encoding="utf-8")) + 1 if calls.exists() else 1
            calls.write_text(str(count), encoding="utf-8")
            if count == 1:
                return Result(text=decision({
                    "kind": "do", "task": "crash once", "rig": {"name": "worker"},
                }))
            return Result(text=decision(
                None, end=True, final_summary="resumed without replanning epoch zero"
            ))

    class ExitRig:
        def run(self, prompt: str, workdir: Path) -> Result:
            os._exit(77)

    class ResumedRig:
        def run(self, prompt: str, workdir: Path) -> Result:
            (workdir / "resumed-effect").write_text("ran", encoding="utf-8")
            return Result(text="resumed")

    pid = os.fork()
    if pid == 0:
        child = Run(
            workdir=repo,
            job_spec="crash recovery",
            registry=RigRegistry({"planner": FilePlanner(), "worker": ExitRig()}),
            planner_rig="planner",
            run_id="crash-run",
        )
        child.run()
        os._exit(0)
    _, status = os.waitpid(pid, 0)
    assert os.WEXITSTATUS(status) == 77
    assert calls.read_text(encoding="utf-8") == "1"

    resumed = Run(
        workdir=repo,
        job_spec="crash recovery",
        registry=RigRegistry({"planner": FilePlanner(), "worker": ResumedRig()}),
        planner_rig="planner",
        run_id="crash-run",
    )
    assert resumed.resume().epochs == 1
    assert (repo / "resumed-effect").read_text(encoding="utf-8") == "ran"
    # One call emitted epoch zero; the second is the normal planner re-entry that ends.
    assert calls.read_text(encoding="utf-8") == "2"
