"""Regression tests from the external adversarial review, PASS 6 (hand-10).

The consolidation round: recovery state moved from process memory onto durable run_dir
records, and every cleanup/rollback git op made non-destructive + checked. Two principles:

  A — QUARANTINE, NEVER DESTROY: a dead dispatched-only attempt's tip (dead-attempt AND
      post-crash operator commits) is quarantined to a ref and the branch reset to the
      durable pre_head; pre-existing untracked files are left in place; a cleanup git-op
      failure HALTS the epoch with a `workspace_unclean` failed result rather than a
      durable "failed" that lies the effect was handled.
  B — DURABLE TRANSACTION INTENTS: the lease record (pre_head + per-file preexisting
      snapshot) and the inplace intent (per-path original content) are fsynced BEFORE the
      first mutation, so a crash mid-transaction is reversed idempotently on restart.

The crash-window probes use REAL child-process death (`os._exit` after the mutation, no
teardown), matching the reviewer's methodology, so the durable-record recovery is proven
against actual process death, not a mocked boundary.
"""
from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

from wildflows.engine import Engine, replay
from wildflows.expr import Do, Edit, Inplace, RigRef
from wildflows.journal import Journal, JournalCompatibilityError
from wildflows.rig import RigRegistry, Result
from wildflows.workspace import WorkspaceFault

from tests.test_review_fixes import _CountingRig, _git_init
from tests.test_review_fixes_pass4 import _head
from tests.test_review_fixes_pass5 import _base_repo, _commit_file


# --------------------------------------------------------------------------- helpers

def _run_in_child(fn: Callable[[], None]) -> None:
    """Run `fn` in a forked child that really dies (`os._exit`, no teardown) — a genuine
    process death, so only what `fn` fsynced to disk survives into the parent's restart."""
    pid = os.fork()
    if pid == 0:  # child
        try:
            fn()
        except BaseException:
            os._exit(1)
        os._exit(0)
    os.waitpid(pid, 0)


def _boom(*_a: object, **_k: object) -> None:
    os._exit(0)  # real death at an injected point, after the mutation it follows


class _DieAfterLeakRig:
    """Writes an untracked leak, then really dies before returning — a dispatched-only
    crash with the dispatched event + lease record already fsynced to disk."""

    def __init__(self, rel: str, content: str = "leak") -> None:
        self.name = "die"
        self.rel = rel
        self.content = content

    def run(self, prompt: str, workdir: Path) -> Result:
        target = Path(workdir) / self.rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.content, encoding="utf-8")
        os._exit(0)


class _DieAfterCommitRig:
    """Commits an effect, then really dies before returning a result — a dispatched-only
    tail whose mid-rig commit sits at HEAD with no completion certificate."""

    def __init__(self, path: str = "effect.txt") -> None:
        self.name = "die"
        self.path = path

    def run(self, prompt: str, workdir: Path) -> Result:
        (Path(workdir) / self.path).write_text("effect", encoding="utf-8")
        subprocess.run(["git", "add", self.path], cwd=workdir, check=True)
        subprocess.run(["git", "commit", "-qm", "mid-rig commit"], cwd=workdir, check=True)
        os._exit(0)


class _IdempotentRig:
    """Commits effect.txt only if absent; otherwise a success no-op. Under the OLD (retain-
    at-HEAD) policy this would observe a dead attempt's commit and close success with no
    receipt; under quarantine+reset it sees a clean tree and must author its own effect."""

    def __init__(self) -> None:
        self.name = "idem"
        self.calls = 0

    def run(self, prompt: str, workdir: Path) -> Result:
        self.calls += 1
        effect = Path(workdir) / "effect.txt"
        if not effect.exists():
            effect.write_text("effect", encoding="utf-8")
            subprocess.run(["git", "add", "effect.txt"], cwd=workdir, check=True)
            subprocess.run(["git", "commit", "-qm", "effect"], cwd=workdir, check=True)
        return Result(text="idempotent", exit_code=0)


def _quarantined_shas(workdir: Path) -> list[str]:
    out = subprocess.run(
        ["git", "for-each-ref", "--format=%(objectname)", "refs/wildflows/quarantine/"],
        cwd=workdir, capture_output=True, text=True,
    ).stdout
    return out.split()


# ------------------------------------------------ PRINCIPLE A: quarantine, never destroy

def test_dispatched_resume_checks_dirty_before_any_reset(tmp_path: Path) -> None:
    """On resume of a dead dispatched attempt, a post-crash operator COMMIT is never
    silently reset away: it is quarantined (reachable) before the branch resets to the
    durable pre_head, and the node re-runs. Nothing the reset touches is destroyed."""
    workdir = tmp_path / "work"
    pre = _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="die"))

    _run_in_child(lambda: Engine(
        run_dir=run_dir, workdir=workdir,
        registry=RigRegistry({"die": _DieAfterLeakRig("leak.txt")})).run_epoch(tree, epoch=0))

    # An operator lands a tracked hotfix commit after the crash, before restart.
    _commit_file(workdir, "base.txt", "OPERATOR HOTFIX", "operator hotfix")
    op_sha = _head(workdir)

    rig = _CountingRig("die")  # a normal rig for the rerun
    Engine(run_dir=run_dir, workdir=workdir,
           registry=RigRegistry({"die": rig})).run_epoch(tree, epoch=0)

    assert rig.calls == 1  # the node re-ran from a clean pre_head
    assert op_sha in _quarantined_shas(workdir)  # operator commit preserved, never lost
    assert (workdir / "base.txt").read_text() == "base"  # reset to pre_head, not to HEAD
    assert replay(run_dir).integrated[(0, "n0")]  # rerun owns its own receipt


def test_dispatched_resume_preserves_original_lease_untracked_snapshot(tmp_path: Path) -> None:
    """A pre-existing untracked file present at lease open (recorded in the DURABLE lease
    snapshot) survives a dead-attempt resume; only the attempt's own untracked leak is
    swept — the reviewer's original-vs-leak distinction that memory-only state could not
    make across process death."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "notes").mkdir()
    (workdir / "notes" / "keep").write_text("USER NOTES", encoding="utf-8")  # pre-existing
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="die"))

    _run_in_child(lambda: Engine(
        run_dir=run_dir, workdir=workdir,
        registry=RigRegistry({"die": _DieAfterLeakRig("notes/leak")})).run_epoch(tree, epoch=0))

    Engine(run_dir=run_dir, workdir=workdir,
           registry=RigRegistry({"die": _CountingRig("die")})).run_epoch(tree, epoch=0)

    assert (workdir / "notes" / "keep").read_text() == "USER NOTES"  # original preserved
    assert not (workdir / "notes" / "leak").exists()                 # attempt leak swept


def test_crash_mid_failure_cleanup_preserves_preexisting_untracked_on_restart(
    tmp_path: Path,
) -> None:
    """A crash DURING failure cleanup (after the reset, before the sweep) still preserves a
    pre-existing untracked file on restart, because the lease snapshot is durable on disk —
    not the in-memory lease the reviewer showed was gone after death."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "keep").write_text("USER", encoding="utf-8")  # pre-existing untracked
    run_dir = tmp_path / "run"
    tree = Do(task="fail", rig=RigRef(name="shell"))

    def crash() -> None:
        from wildflows.rig import ShellRig
        eng = Engine(run_dir=run_dir, workdir=workdir,
                     registry=RigRegistry({"shell": ShellRig("printf leak > leak; exit 5", 30)}))
        setattr(eng.ws, "_remove_leaks", _boom)  # die mid-cleanup, after the reset
        eng.run_epoch(tree, epoch=0)

    _run_in_child(crash)

    Engine(run_dir=run_dir, workdir=workdir,
           registry=RigRegistry({"shell": _CountingRig("shell")})).run_epoch(tree, epoch=0)

    assert (workdir / "keep").read_text() == "USER"  # preserved across mid-cleanup death
    assert not (workdir / "leak").exists()           # the attempt's leak swept on restart


def test_dead_committed_attempt_idempotent_rerun_must_own_active_commit(tmp_path: Path) -> None:
    """A dead attempt that committed an effect then died: the idempotent rerun must OWN the
    final active commit via a receipt. Quarantine+reset removes the retained commit from the
    branch (kept in a ref), so the idempotent rig sees a clean tree and re-authors + commits
    the effect — killing the reviewer's 'unreceipted retained commit closes success' row."""
    workdir = tmp_path / "work"
    pre = _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="die"))

    _run_in_child(lambda: Engine(
        run_dir=run_dir, workdir=workdir,
        registry=RigRegistry({"die": _DieAfterCommitRig()})).run_epoch(tree, epoch=0))
    dead_sha = _head(workdir)
    assert dead_sha != pre  # the dead attempt committed

    idem = _IdempotentRig()  # same rig NAME "die" for resume identity; idempotent behaviour
    Engine(run_dir=run_dir, workdir=workdir,
           registry=RigRegistry({"die": idem})).run_epoch(tree, epoch=0)

    assert idem.calls == 1
    state = replay(run_dir)
    assert "effect.txt" in state.integrated[(0, "n0")]  # active effect owned by a receipt
    assert (workdir / "effect.txt").exists()            # present in the final worktree
    assert dead_sha in _quarantined_shas(workdir)       # dead attempt's commit not destroyed


def test_inplace_commit_before_result_noop_rerun_preserves_receipt(tmp_path: Path) -> None:
    """An inplace that committed then died before its result: on restart the tip is
    quarantined, the branch reset to pre_head, and the rerun re-applies + commits, so the
    final active commit is owned by a receipt (no unreceipted retained inplace commit)."""
    workdir = tmp_path / "work"
    pre = _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="f.txt", content="hi")])

    def crash() -> None:
        eng = Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({}))
        setattr(eng.rec, "record_success", _boom)  # die after commit, before the result
        eng.run_epoch(tree, epoch=0)

    _run_in_child(crash)
    dead_sha = _head(workdir)
    assert dead_sha != pre  # the inplace committed before dying

    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({})).run_epoch(tree, epoch=0)

    state = replay(run_dir)
    assert "f.txt" in state.integrated[(0, "n0")]  # receipt owns the re-applied commit
    assert (workdir / "f.txt").read_text() == "hi"
    assert dead_sha in _quarantined_shas(workdir)  # the pre-crash commit is preserved


def test_interrupted_effectful_result_without_post_head_is_refused(tmp_path: Path) -> None:
    """An effectful result (non-empty `files`) with NO `post_head` in an interrupted tail is
    an interrupted pre-v1 shape (no completion certificate to reconstruct a receipt from) —
    refused at load, never accepted as a durable receipt-less success."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    stream = [
        {"seq": 0, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "opened"},
        {"seq": 1, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "dispatched",
         "rig": "c", "task": "t", "pre_head": None},
        {"seq": 2, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "result",
         "text": "x", "files": ["x.txt"], "outcome": "ok"},  # effectful, NO post_head
    ]
    (run_dir / "events.ndjson").write_text(
        "\n".join(json.dumps(r) for r in stream) + "\n", encoding="utf-8")
    with pytest.raises(JournalCompatibilityError, match="legacy"):
        Journal.load(run_dir)


def test_failure_reset_error_does_not_record_durable_failure_with_live_effects(
    tmp_path: Path,
) -> None:
    """When the failure revert's `git reset` itself fails (an index.lock the failing rig
    left), the engine must NOT journal a clean durable 'failed'. It records the failure
    marked `workspace_unclean` and HALTS the epoch (WorkspaceFault), so the surviving live
    effect is honest, not papered over."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    from wildflows.rig import ShellRig
    reg = RigRegistry({"shell": ShellRig(
        "printf MUTATED > base.txt; : > .git/index.lock; exit 5", 30)})
    eng = Engine(run_dir=run_dir, workdir=workdir, registry=reg)

    with pytest.raises(WorkspaceFault):
        eng.run_epoch(Do(task="fail", rig=RigRef(name="shell")), epoch=0)

    (workdir / ".git" / "index.lock").unlink()  # release for the read-only assertions below
    assert (workdir / "base.txt").read_text() == "MUTATED"  # live effect survived
    events = [json.loads(ln) for ln in (run_dir / "events.ndjson").read_text().splitlines()]
    last_result = [e for e in events if e["kind"] == "result"][-1]
    assert last_result["outcome"] == "failed" and last_result["workspace_unclean"] is True
    assert not any(e["kind"] == "boundary" and e["phase"] == "closed" for e in events)


# ---------------------------------------------- PRINCIPLE B: durable inplace transaction

def test_inplace_oserror_after_first_write_rolls_back_and_records_failure(tmp_path: Path) -> None:
    """A NON-ValueError post-write failure (a second edit targeting a pre-existing directory
    raises OSError) rolls back the first write and records a durable failure — the reviewer's
    `except ValueError` gap. The pre-existing directory is left untouched (never deleted)."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "dest").mkdir()
    (workdir / "dest" / "inner").write_text("keepme", encoding="utf-8")  # pre-existing dir
    run_dir = tmp_path / "run"

    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({})).run_epoch(
        Inplace(edits=[Edit(path="first", content="1"),
                       Edit(path="dest", content="2")]), epoch=0)

    assert not (workdir / "first").exists()                    # first write rolled back
    assert (workdir / "dest" / "inner").read_text() == "keepme"  # pre-existing dir intact
    state = replay(run_dir)
    assert state.results[(0, "n0")].ok is False
    assert (0, "n0") not in state.integrated


def test_inplace_partial_write_exception_restores_original(tmp_path: Path) -> None:
    """A partial-write failure restores a pre-existing FILE's original content from the
    durable intent (not deletes it as if created)."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "keep").write_text("ORIGINAL", encoding="utf-8")  # pre-existing untracked file
    (workdir / "dest").mkdir()
    (workdir / "dest" / "inner").write_text("x", encoding="utf-8")
    run_dir = tmp_path / "run"

    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({})).run_epoch(
        Inplace(edits=[Edit(path="keep", content="OVERWRITTEN"),
                       Edit(path="dest", content="2")]), epoch=0)

    assert (workdir / "keep").read_text() == "ORIGINAL"  # restored, not left overwritten
    assert replay(run_dir).results[(0, "n0")].ok is False


def test_crash_mid_inplace_rollback_restores_preexisting_untracked_content(tmp_path: Path) -> None:
    """A crash DURING an inplace rollback still restores the pre-existing untracked content
    on restart — the durable intent (fsynced originals) survives process death where the
    in-memory `writes` list the reviewer relied on being present did not."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "keep").write_text("ORIGINAL", encoding="utf-8")  # pre-existing untracked
    (workdir / "dest").mkdir()
    (workdir / "dest" / "inner").write_text("x", encoding="utf-8")
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="keep", content="OVERWRITTEN"),
                          Edit(path="dest", content="2")])  # 2nd edit → OSError → rollback

    def crash() -> None:
        eng = Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({}))
        setattr(eng.ws, "rollback_inplace", _boom)  # die at the very start of rollback
        eng.run_epoch(tree, epoch=0)

    _run_in_child(crash)
    assert (workdir / "keep").read_text() == "OVERWRITTEN"  # died before any restoration

    # Restart: the durable intent reverses the partial write, then the rerun fails the same
    # way and rolls back again — leaving the pre-existing content restored, never lost.
    Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({})).run_epoch(tree, epoch=0)
    assert (workdir / "keep").read_text() == "ORIGINAL"  # preexisting content recovered
    assert replay(run_dir).results[(0, "n0")].ok is False


def test_inplace_unstage_failure_cannot_record_durable_failure(tmp_path: Path) -> None:
    """If the rollback's unstage `git reset` fails, the engine HALTS with a
    `workspace_unclean` failed result (WorkspaceFault) rather than a durable clean failure —
    a lying 'handled' is worse than a halt."""
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "dest").mkdir()
    (workdir / "dest" / "inner").write_text("x", encoding="utf-8")
    run_dir = tmp_path / "run"
    eng = Engine(run_dir=run_dir, workdir=workdir, registry=RigRegistry({}))

    real_git = eng.ws.git

    def flaky_git(*args: str) -> subprocess.CompletedProcess[str]:
        if "reset" in args and "--" in args:  # rollback unstage (global git opts allowed)
            return subprocess.CompletedProcess(list(args), 1, "", "index.lock exists")
        return real_git(*args)

    setattr(eng.ws, "git", flaky_git)

    with pytest.raises(WorkspaceFault):
        eng.run_epoch(Inplace(edits=[Edit(path="a", content="a"),
                                     Edit(path="dest", content="2")]), epoch=0)

    events = [json.loads(ln) for ln in (run_dir / "events.ndjson").read_text().splitlines()]
    last_result = [e for e in events if e["kind"] == "result"][-1]
    assert last_result["outcome"] == "failed" and last_result["workspace_unclean"] is True
    assert not any(e["kind"] == "boundary" and e["phase"] == "closed" for e in events)


# ----------------------------------------------------------- SHOULD-FIX: stale docs

def test_provenance_docs_do_not_describe_dispatched_only_as_recoverable() -> None:
    """Load-bearing protocol comments must match the two-boundary + quarantine model: the
    dispatched provenance doc no longer claims `pre_head..HEAD` reconstruction, and the
    workspace header no longer mentions the deleted marker reconciliation."""
    import wildflows.events as events_mod
    import wildflows.workspace as ws_mod

    events_src = Path(events_mod.__file__).read_text(encoding="utf-8")
    ws_src = Path(ws_mod.__file__).read_text(encoding="utf-8")

    assert "reconstructed from that range instead of re-run" not in events_src  # old claim
    assert "QUARANTINED" in events_src  # dispatched-only tail is quarantined + reset, re-run
    assert "marker reconciliation" not in ws_src  # the deleted path is no longer advertised
