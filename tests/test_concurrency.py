"""M2 sibling concurrency, serialized integration, and concurrent resume."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import time
from pathlib import Path

import pytest

from tests.test_engine import git, init_repo, registry
from wildflows.engine import Engine, SiblingOwnershipError
from wildflows.events import Dispatched, Integrated, LoopIter, ResultEvent
from wildflows.expr import Dispatch, Do, Loop, RigRef, Until
from wildflows.result import Result
from wildflows.rig import RigRegistry, ShellRig


class OrderedRig:
    def __init__(self, count: int, *, overlap: bool = False) -> None:
        self.barrier = threading.Barrier(count)
        self.overlap = overlap
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def run(self, prompt: str, workdir: Path) -> Result:
        index = int(prompt)
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        self.barrier.wait(timeout=5)
        time.sleep((2 - index) * 0.04)
        path = "shared" if self.overlap else f"file-{index}"
        (workdir / path).write_text(f"value-{index}", encoding="utf-8")
        with self.lock:
            self.active -= 1
        return Result(text=f"result-{index}")


def dispatch_three() -> Dispatch:
    return Dispatch(
        children=[Do(task=str(index), rig=RigRef(name="ordered")) for index in range(3)]
    )


def test_three_disjoint_siblings_run_together_and_land_by_completion(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    base = init_repo(repo)
    rig = OrderedRig(3)
    engine = Engine(
        tmp_path / "run", repo, registry(ordered=rig), max_workers=3
    )

    engine.run_epoch(dispatch_three(), 0)

    assert rig.max_active == 3
    dispatched = [
        event for event in engine.journal.events() if isinstance(event, Dispatched)
    ]
    assert [event.pre_head for event in dispatched] == [base, base, base]
    integrated = [
        event for event in engine.journal.events() if isinstance(event, Integrated)
    ]
    assert [event.node_id for event in integrated] == ["n0.2", "n0.1", "n0.0"]
    assert [
        engine.journal.projection.results[(0, f"n0.{index}")].text
        for index in range(3)
    ] == ["result-0", "result-1", "result-2"]
    for index in range(3):
        assert git(repo, "show", f"HEAD:file-{index}") == f"value-{index}"


def test_overlapping_later_lander_is_a_typed_failed_attempt(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    rig = OrderedRig(2, overlap=True)
    tree = Dispatch(
        children=[Do(task="1", rig=RigRef(name="ordered")), Do(task="2", rig=RigRef(name="ordered"))]
    )
    engine = Engine(tmp_path / "run", repo, registry(ordered=rig), max_workers=2)

    with pytest.raises(SiblingOwnershipError, match="shared"):
        engine.run_epoch(tree, 0)

    assert git(repo, "show", "HEAD:shared") == "value-2"
    state = engine.journal.projection
    assert state.results[(0, "n0.1")].ok
    assert state.results[(0, "n0.0")].outcome == "failed"
    assert not state.epoch_closed(0)


def test_loop_uses_positional_dispatch_result_not_last_completion(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    rig = OrderedRig(3)
    tree = Loop(
        body=dispatch_three(),
        until=Until(kind="cmd", cmd="true"),
        cap=1,
    )
    engine = Engine(tmp_path / "run", repo, registry(ordered=rig), max_workers=3)

    engine.run_epoch(tree, 0)

    loop_result = engine.journal.projection.results[(0, "n0")]
    assert loop_result.text == "result-2"
    iteration = next(
        event for event in engine.journal.events() if isinstance(event, LoopIter)
    )
    positional = next(
        event
        for event in engine.journal.events()
        if isinstance(event, ResultEvent) and event.node_id == "n0.0.2"
    )
    assert iteration.body_result_seq == positional.seq


def test_max_workers_one_keeps_serial_dispatch_semantics(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    base = init_repo(repo)
    writer = ShellRig("printf '%s' {prompt} > file-{prompt}", 5)
    tree = Dispatch(
        children=[Do(task=str(index), rig=RigRef(name="writer")) for index in range(3)]
    )
    engine = Engine(
        tmp_path / "run", repo, registry(writer=writer), max_workers=1
    )

    engine.run_epoch(tree, 0)

    dispatched = [
        event for event in engine.journal.events() if isinstance(event, Dispatched)
    ]
    integrated = [
        event for event in engine.journal.events() if isinstance(event, Integrated)
    ]
    assert dispatched[0].pre_head == base
    assert [event.pre_head for event in dispatched[1:]] == [
        integrated[0].commit,
        integrated[1].commit,
    ]
    assert [event.node_id for event in integrated] == ["n0.0", "n0.1", "n0.2"]


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork/SIGKILL")
def test_sigkill_mid_group_resume_reruns_only_unintegrated_siblings(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    init_repo(repo)
    run_dir = tmp_path / "run"
    gate = tmp_path / "release-slow"
    calls = tmp_path / "calls"
    script = tmp_path / "rig.py"
    script.write_text(
        "import pathlib, sys, time\n"
        f"gate = pathlib.Path({str(gate)!r})\n"
        f"calls = pathlib.Path({str(calls)!r})\n"
        "task = sys.argv[1]\n"
        "with calls.open('a') as stream: stream.write(task + '\\n'); stream.flush()\n"
        "if task != 'fast':\n"
        "    while not gate.exists(): time.sleep(0.01)\n"
        "pathlib.Path(task).write_text(task)\n",
        encoding="utf-8",
    )
    rig = ShellRig(f"python3 {script} {{prompt}}", 20)
    rigs = RigRegistry({"script": rig})
    tree = Dispatch(
        children=[
            Do(task="fast", rig=RigRef(name="script")),
            Do(task="slow-a", rig=RigRef(name="script")),
            Do(task="slow-b", rig=RigRef(name="script")),
        ]
    )

    pid = os.fork()
    if pid == 0:
        try:
            Engine(run_dir, repo, rigs, max_workers=3).run_epoch(tree, 0)
        except BaseException:
            os._exit(91)
        os._exit(0)

    waited = False
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if (run_dir / "events.ndjson").exists():
                records = [
                    json.loads(line)
                    for line in (run_dir / "events.ndjson").read_text(encoding="utf-8").splitlines()
                ]
                if any(
                    record["kind"] == "integrated" and record["node_id"] == "n0.0"
                    for record in records
                ):
                    break
            exited, _ = os.waitpid(pid, os.WNOHANG)
            if exited:
                waited = True
                pytest.fail("engine exited before one sibling integrated")
            time.sleep(0.02)
        else:
            pytest.fail("fast sibling did not integrate")
        os.kill(pid, signal.SIGKILL)
        _, status = os.waitpid(pid, 0)
        waited = True
        assert os.WIFSIGNALED(status)
    finally:
        if not waited:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)

    gate.touch()
    resumed = Engine(run_dir, repo, rigs, max_workers=3)
    resumed.run_epoch(tree, 0)

    invoked = calls.read_text(encoding="utf-8").splitlines()
    assert invoked.count("fast") == 1
    assert invoked.count("slow-a") == 2
    assert invoked.count("slow-b") == 2
    assert git(repo, "show", "HEAD:fast") == "fast"
    assert git(repo, "show", "HEAD:slow-a") == "slow-a"
    assert git(repo, "show", "HEAD:slow-b") == "slow-b"
    assert resumed.journal.projection.epoch_closed(0)
