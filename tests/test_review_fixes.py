"""Regression tests from the external adversarial review (hand-4).

Each test carries the reviewer's exact name and exercises a failure/restart path the
happy-path PoC suite did not: resume, torn journals, epoch scoping, loop partial
iterations, core-mediated integration, and rig/git failure conversion.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError

from wildflows.engine import Engine, replay
from wildflows.expr import CtxRef, Do, Edit, Inplace, Loop, RigRef, Seq, Until
from wildflows.journal import Journal
from wildflows.rig import EchoRig, Result, RigRegistry


def _git_init(workdir: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=workdir, check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=workdir, check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=workdir, check=True)


class _CountingRig:
    """Writes `<name>.txt` = its call count each run (a fresh diff every call) so the
    core commits every invocation; the counter proves whether a node re-executed."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0

    def run(self, prompt: str, workdir: Path) -> Result:
        self.calls += 1
        (Path(workdir) / f"{self.name}.txt").write_text(str(self.calls), encoding="utf-8")
        return Result(text=f"{self.name} run {self.calls}", ok=True, exit_code=0)


# --------------------------------------------------------------------------- B2

def test_load_ignores_only_an_unterminated_malformed_final_record(tmp_path: Path) -> None:
    j = Journal(tmp_path)
    from wildflows.events import Boundary

    j.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="opened"))
    j.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="closed"))
    # Simulate a kill during the final write: a torn, unterminated last record.
    with open(tmp_path / "events.ndjson", "a", encoding="utf-8") as fh:
        fh.write('{"kind":"result"')  # no closing brace, no newline

    reloaded = Journal.load(tmp_path)
    assert [e.kind for e in reloaded.events()] == ["boundary", "boundary"]
    # The next append safely reuses the torn record's seq (contiguity preserved).
    assert reloaded.append(Boundary(run_id="r", epoch=1, node_id="n0", phase="opened")) == 2


def test_load_still_rejects_a_malformed_middle_record(tmp_path: Path) -> None:
    path = tmp_path / "events.ndjson"
    good = json.dumps({"kind": "boundary", "run_id": "r", "epoch": 0, "node_id": "n0",
                       "phase": "opened"})
    path.write_text(good + "\n" + "{garbage}\n" + good + "\n", encoding="utf-8")
    with pytest.raises((json.JSONDecodeError, ValidationError)):
        Journal.load(tmp_path)


# --------------------------------------------------------------------------- B3

def test_replay_scopes_node_state_to_epoch(tmp_path: Path) -> None:
    from wildflows.events import Boundary, Dispatched, ResultEvent

    j = Journal(tmp_path)
    j.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="opened"))
    j.append(ResultEvent(run_id="r", epoch=0, node_id="n0", ok=True, text="epoch0 result"))
    j.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="closed"))
    j.append(Boundary(run_id="r", epoch=1, node_id="n0", phase="opened"))
    j.append(Dispatched(run_id="r", epoch=1, node_id="n0", rig="echo", task="t"))

    state = replay(tmp_path)
    # epoch 0's n0 result must NOT leak into epoch 1's (killed-after-dispatch) n0.
    assert state.results[(0, "n0")].text == "epoch0 result"
    assert (1, "n0") not in state.results
    assert (1, "n0") in state.dispatched


def test_replay_uses_the_latest_boundary_state_for_an_epoch(tmp_path: Path) -> None:
    from wildflows.events import Boundary

    j = Journal(tmp_path)
    j.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="closed"))
    j.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="opened"))
    # Latest boundary is `opened` -> the epoch is open, not closed.
    assert replay(tmp_path).epoch_closed(0) is False


# --------------------------------------------------------------------------- B1

def test_engine_restart_loads_journal_contiguously_and_resumes_only_inflight_nodes(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    rig = _CountingRig("do")
    reg = RigRegistry({"c": rig})
    tree = Seq(children=[
        Inplace(edits=[Edit(path="planner.txt", content="hi")]),
        Do(task="work", rig=RigRef(name="c")),
    ])

    # Full epoch 0, then a fresh Engine (restart) runs epoch 1.
    Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg).run_epoch(tree, epoch=0)
    assert rig.calls == 1
    Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg).run_epoch(
        Do(task="more", rig=RigRef(name="c")), epoch=1
    )
    seqs = [e.seq for e in Journal.load(tmp_path / "run").events()]
    # No reused sequence numbers across the restart: contiguous, strictly increasing.
    assert seqs == list(range(len(seqs)))

    # Resume-only-inflight: interrupt epoch 0 with the `do` dispatched but not resulted.
    wd2 = tmp_path / "work2"
    wd2.mkdir()
    _git_init(wd2)
    rig2 = _CountingRig("do")
    reg2 = RigRegistry({"c": rig2})
    run2 = tmp_path / "run2"
    Engine(run_dir=run2, workdir=wd2, registry=reg2).run_epoch(tree, epoch=0)
    assert rig2.calls == 1
    lines = (run2 / "events.ndjson").read_text().splitlines()
    seen = 0
    cut = len(lines)
    for i, ln in enumerate(lines):
        if '"kind":"dispatched"' in ln.replace(" ", ""):
            seen += 1
            if seen == 2:  # the `do` dispatch — drop its result/integrated + close
                cut = i + 1
                break
    (run2 / "events.ndjson").write_text("\n".join(lines[:cut]) + "\n")
    # The `do` also committed in git; to simulate a crash BEFORE that commit (a genuine
    # in-flight node, not the NB4 commit-then-crash window which has its own test), drop
    # the do's commit so only the completed inplace commit remains.
    subprocess.run(["git", "reset", "--hard", "HEAD~1"], cwd=wd2, check=True,
                   capture_output=True)

    Engine(run_dir=run2, workdir=wd2, registry=reg2).run_epoch(tree, epoch=0)
    assert rig2.calls == 2  # the in-flight `do` re-ran
    planner_log = subprocess.run(
        ["git", "log", "--oneline", "--", "planner.txt"], cwd=wd2, capture_output=True, text=True
    ).stdout.strip().splitlines()
    assert len(planner_log) == 1  # the completed inplace was NOT re-committed


# --------------------------------------------------------------------------- B4

_NEVER = "false"


def test_resume_loop_ignores_inner_node_state_before_the_last_loop_iter_when_the_next_iteration_is_partial(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    rig_a = _CountingRig("a")
    rig_b = _CountingRig("b")
    reg = RigRegistry({"a": rig_a, "b": rig_b})
    loop = Loop(
        body=Seq(children=[
            Do(task="A", rig=RigRef(name="a")),
            Do(task="B", rig=RigRef(name="b")),
        ]),
        until=Until(kind="cmd", cmd=_NEVER),
        cap=2,
    )
    Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg).run_epoch(loop, epoch=0)
    # Two full iterations ran both A and B.
    assert (rig_a.calls, rig_b.calls) == (2, 2)

    # Truncate mid iteration-1: keep iter-1's A (result+integrated) but drop B and the
    # 2nd loop_iter — a partial iteration whose A is done and B is not.
    lines = (tmp_path / "run" / "events.ndjson").read_text().splitlines()
    seen = 0
    cut = len(lines)
    for i, ln in enumerate(lines):
        if '"kind":"integrated"' in ln.replace(" ", ""):
            seen += 1
            if seen == 3:  # A(it0), B(it0), A(it1) -> cut right after A(it1)
                cut = i + 1
                break
    (tmp_path / "run" / "events.ndjson").write_text("\n".join(lines[:cut]) + "\n")

    rig_a2 = _CountingRig("a")
    rig_b2 = _CountingRig("b")
    reg2 = RigRegistry({"a": rig_a2, "b": rig_b2})
    Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg2).run_epoch(loop, epoch=0)
    # A's iteration-1 result is AFTER the last loop_iter -> durable -> A NOT re-run.
    assert rig_a2.calls == 0
    # B's only result is iteration-0 (BEFORE the last loop_iter) -> stale -> B re-runs.
    assert rig_b2.calls == 1


# --------------------------------------------------------------------------- B5

def _shell_reg(template: str) -> RigRegistry:
    from wildflows.rig import ShellRig

    return RigRegistry({"shell": ShellRig(template=template, timeout_s=30)})


def test_effectful_do_is_core_integrated_and_remains_recoverable_after_worktree_reset(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    reg = _shell_reg("printf 'x' > created.txt")
    eng = Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg)
    eng.run_epoch(Do(task="make it", rig=RigRef(name="shell")), epoch=0)

    committed = subprocess.run(
        ["git", "show", "--name-only", "--pretty=", "HEAD"], cwd=workdir,
        capture_output=True, text=True,
    ).stdout
    assert "created.txt" in committed  # the core committed the rig's artifact
    state = replay(tmp_path / "run")
    assert (0, "n0") in state.integrated
    assert "created.txt" in state.integrated[(0, "n0")]

    # A worktree reset would lose an untracked file; the committed diff survives it.
    (workdir / "created.txt").unlink()
    subprocess.run(["git", "checkout", "HEAD", "--", "created.txt"], cwd=workdir, check=True)
    assert (workdir / "created.txt").read_text() == "x"


# --------------------------------------------------------------------------- B6

def test_inplace_preserves_a_preexisting_staged_index(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    (workdir / "outside.txt").write_text("staged by someone else", encoding="utf-8")
    subprocess.run(["git", "add", "outside.txt"], cwd=workdir, check=True)

    eng = Engine(run_dir=tmp_path / "run", workdir=workdir, registry=RigRegistry({}))
    eng.run_epoch(Inplace(edits=[Edit(path="declared.txt", content="d")]), epoch=0)

    # The commit contains ONLY the declared path.
    committed = subprocess.run(
        ["git", "show", "--name-only", "--pretty=", "HEAD"], cwd=workdir,
        capture_output=True, text=True,
    ).stdout
    assert "declared.txt" in committed
    assert "outside.txt" not in committed
    # outside.txt is still staged, uncommitted — its ownership record is intact.
    staged = subprocess.run(
        ["git", "diff", "--cached", "--name-only"], cwd=workdir, capture_output=True, text=True
    ).stdout
    assert "outside.txt" in staged
    state = replay(tmp_path / "run")
    assert state.integrated[(0, "n0")] == ["declared.txt"]


def test_inplace_treats_option_like_paths_as_literal_paths() -> None:
    # `--all` would become `git add --all`; reject option-like paths at admission (B6).
    with pytest.raises(ValidationError):
        Edit(path="--all", content="x")
    with pytest.raises(ValidationError):
        Inplace(edits=[Edit(path="-rf", content="x")])


# --------------------------------------------------------------------------- SF1

def test_empty_inplace_is_a_noop_result(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    eng = Engine(run_dir=tmp_path / "run", workdir=workdir, registry=RigRegistry({}))
    eng.run_epoch(Inplace(edits=[]), epoch=0)

    state = replay(tmp_path / "run")
    assert state.results[(0, "n0")].ok is True  # no-op is an OK result
    assert (0, "n0") not in state.integrated  # no git commit was made
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=workdir, capture_output=True, text=True)
    assert head.returncode != 0  # no commit exists at all


def test_inplace_without_git_identity_journals_a_failed_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    # A repo with NO identity: block global/system config + author env so commit fails.
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "no-global"))
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", str(tmp_path / "no-system"))
    for var in ("GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME",
                "GIT_COMMITTER_EMAIL"):
        monkeypatch.delenv(var, raising=False)
    subprocess.run(["git", "init", "-q"], cwd=workdir, check=True)

    eng = Engine(run_dir=tmp_path / "run", workdir=workdir, registry=RigRegistry({}))
    eng.run_epoch(Inplace(edits=[Edit(path="f.txt", content="x")]), epoch=0)

    state = replay(tmp_path / "run")
    res = state.results[(0, "n0")]
    assert res.ok is False
    assert res.outcome == "failed"
    assert "integration failed" in res.text  # the git stderr is journalled, not raised
    assert state.epoch_closed(0)  # the epoch closed cleanly despite the failure


# --------------------------------------------------------------------------- SF2

def test_do_materializes_node_and_file_context(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    (workdir / "brief.txt").write_text("FROM-FILE-CTX", encoding="utf-8")
    reg = RigRegistry({"echo": EchoRig()})
    tree = Seq(children=[
        Do(task="produce", rig=RigRef(name="echo")),                       # n0.0
        Do(task="consume", rig=RigRef(name="echo"), ctx=[                   # n0.1
            CtxRef(kind="node", ref="n0.0"),
            CtxRef(kind="file", ref="brief.txt"),
        ]),
    ])
    Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg).run_epoch(tree, epoch=0)

    state = replay(tmp_path / "run")
    consumed = state.results[(0, "n0.1")].text  # echo returns the whole prompt back
    assert "echo: produce" in consumed  # upstream node result was materialized
    assert "FROM-FILE-CTX" in consumed  # file content was materialized


def test_do_with_missing_ctx_node_ref_rejected_at_admission(tmp_path: Path) -> None:
    # A ctx node ref naming no node in the tree is a deterministic error the core can
    # reject over the whole tree BEFORE opening the epoch (item 5), not a runtime result.
    from wildflows.admission import AdmissionError

    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    reg = RigRegistry({"echo": EchoRig()})
    eng = Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg)
    with pytest.raises(AdmissionError):
        eng.run_epoch(
            Do(task="x", rig=RigRef(name="echo"), ctx=[CtxRef(kind="node", ref="n404")]), epoch=0
        )
    assert not (tmp_path / "run" / "events.ndjson").exists()


# --------------------------------------------------------------------------- SF3

class _RaisingRig:
    def run(self, prompt: str, workdir: Path) -> Result:
        raise RuntimeError("transport boom")


def test_rig_exception_is_recoverable_from_a_durable_state(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    reg = RigRegistry({"boom": _RaisingRig()})
    eng = Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg)
    eng.run_epoch(Do(task="x", rig=RigRef(name="boom")), epoch=0)  # must NOT raise

    state = replay(tmp_path / "run")
    res = state.results[(0, "n0")]
    assert res.ok is False
    assert res.outcome == "failed"
    assert "boom" in res.text
    assert state.epoch_closed(0)


# --------------------------------------------------------------------------- SF5

def test_cmd_until_requires_cmd_at_expression_validation_time() -> None:
    with pytest.raises(ValidationError):
        Until(kind="cmd")  # a cmd predicate with no command is invalid expression data


# --------------------------------------------------------------------------- SF6

def test_loop_returns_the_last_integrated_body_result(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    _git_init(workdir)
    reg = RigRegistry({"build": _CountingRig("artifact")})
    # Converge once the counter file reaches 2.
    converge = "c=$(cat n 2>/dev/null||echo 0); c=$((c+1)); echo $c>n; test $c -ge 2"
    loop = Loop(
        body=Do(task="build", rig=RigRef(name="build")),
        until=Until(kind="cmd", cmd=converge),
        cap=5,
    )
    Engine(run_dir=tmp_path / "run", workdir=workdir, registry=reg).run_epoch(loop, epoch=0)

    state = replay(tmp_path / "run")
    final = state.results[(0, "n0")]
    # The loop result carries the body ARTIFACT (files/text), not convergence prose.
    assert "artifact.txt" in final.files
    assert "artifact" in final.text
    assert "converged" not in final.text


# --------------------------------------------------------------------------- N1

def test_inplace_rejects_git_admin_paths_in_a_linked_worktree(tmp_path: Path) -> None:
    main = tmp_path / "main"
    main.mkdir()
    _git_init(main)
    (main / "seed.txt").write_text("seed", encoding="utf-8")
    subprocess.run(["git", "add", "seed.txt"], cwd=main, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "seed"], cwd=main, check=True)
    wt = tmp_path / "wt"
    subprocess.run(["git", "worktree", "add", "-q", str(wt)], cwd=main, check=True)
    assert (wt / ".git").is_file()  # a linked worktree's .git is a FILE (a gitdir pointer)
    before = (wt / ".git").read_text()

    eng = Engine(run_dir=tmp_path / "run", workdir=wt, registry=RigRegistry({}))
    with pytest.raises(ValueError, match="git admin path"):
        eng.run_epoch(Inplace(edits=[Edit(path=".git", content="corrupt")]), epoch=0)
    assert (wt / ".git").read_text() == before  # the gitdir pointer was not overwritten
