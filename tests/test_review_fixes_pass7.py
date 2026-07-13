"""Pass-7 transaction-hardening regressions (hand-11).

Crash-window tests use real ``fork`` + ``os._exit`` deaths.  They intentionally inspect
only durable state from a freshly constructed Engine after the child is gone.
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
from wildflows.rig import RigRegistry, Result, ShellRig
from wildflows.workspace import WorkspaceFault

from tests.test_review_fixes import _CountingRig
from tests.test_review_fixes_pass5 import _base_repo, _commit_file
from tests.test_review_fixes_pass6 import _DieAfterCommitRig, _DieAfterLeakRig


def _fork(fn: Callable[[], None]) -> int:
    pid = os.fork()
    if pid == 0:
        try:
            fn()
        except BaseException:
            os._exit(91)
        os._exit(0)
    _, status = os.waitpid(pid, 0)
    return os.waitstatus_to_exitcode(status)


def _die(*_args: object, **_kwargs: object) -> None:
    os._exit(0)


def _events(run_dir: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in (run_dir / "events.ndjson").read_text().splitlines()]


def _capture_bytes(run_dir: Path, rel: str) -> list[bytes]:
    """Read exact captured bytes through capture manifests, never from hash summaries."""
    found: list[bytes] = []
    for manifest_path in run_dir.rglob("manifest.json"):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest["entries"]:
            if entry["path"] == rel and entry["kind"] == "file":
                found.append((manifest_path.parent / entry["blob"]).read_bytes())
    return found


def _quarantine_refs(workdir: Path) -> dict[str, str]:
    output = subprocess.run(
        ["git", "for-each-ref", "--format=%(refname) %(objectname)",
         "refs/wildflows/quarantine/"],
        cwd=workdir, check=True, capture_output=True, text=True,
    ).stdout
    return dict(line.split(" ", 1) for line in output.splitlines())


def test_workspace_unclean_resume_remains_halted_until_cleanup_succeeds(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="fail", rig=RigRef(name="shell"))
    rig = ShellRig("printf MUTATED > base.txt; : > .git/index.lock; exit 5", 30)

    with pytest.raises(WorkspaceFault):
        Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(tree, 0)
    assert (workdir / "base.txt").read_text() == "MUTATED"

    # The durable marker must not replay as an ordinary fileless failed result and close.
    with pytest.raises(WorkspaceFault):
        Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(tree, 0)
    assert not replay(run_dir).epoch_closed(0)
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True

    # Releasing the cleanup obstruction lets resume re-run checked cleanup, explicitly
    # clear the halt, and only then close the failed attempt.
    (workdir / ".git" / "index.lock").unlink()
    Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(tree, 0)
    state = replay(run_dir)
    assert (workdir / "base.txt").read_text() == "base"
    assert state.epoch_closed(0)
    assert state.node((0, "n0")).workspace_unclean is False
    assert any(e["kind"] == "result" and e["workspace_unclean"] is False
               and "cleanup recovered" in str(e["text"]) for e in _events(run_dir))


def test_inplace_internal_symlink_alias_never_leaves_unreceipted_target_effect(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    os.symlink("base.txt", workdir / "alias")
    run_dir = tmp_path / "run"

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(
        Inplace(edits=[Edit(path="alias", content="MUTATED")]), 0)

    state = replay(run_dir)
    assert state.results[(0, "n0")].ok
    assert state.integrated[(0, "n0")] == ["base.txt"]
    assert subprocess.run(
        ["git", "diff", "--quiet", "--", "base.txt"], cwd=workdir
    ).returncode == 0
    assert subprocess.run(
        ["git", "show", "HEAD:base.txt"], cwd=workdir,
        check=True, capture_output=True, text=True,
    ).stdout == "MUTATED"


def test_inplace_resolved_target_collision_is_rejected_before_write(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    os.symlink("base.txt", workdir / "alias")
    run_dir = tmp_path / "run"

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(Inplace(edits=[
        Edit(path="alias", content="through alias"),
        Edit(path="base.txt", content="direct"),
    ]), 0)

    state = replay(run_dir)
    assert not state.results[(0, "n0")].ok
    assert "resolved target collision" in state.results[(0, "n0")].text
    assert (workdir / "base.txt").read_text() == "base"


def test_crash_after_quarantine_reset_then_operator_commit_preserves_both_histories(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="die"))

    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": _DieAfterCommitRig("dead.txt")})
    ).run_epoch(tree, 0)) == 0
    dead_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout.strip()

    def die_after_reset() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("rerun")}))
        setattr(engine.ws, "_remove_leaks", _die)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_reset) == 0
    _commit_file(workdir, "operator.txt", "operator", "operator after recovery crash")
    operator_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout.strip()

    Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("final")})).run_epoch(tree, 0)
    tips = set(_quarantine_refs(workdir).values())
    assert dead_sha in tips
    assert operator_sha in tips


def test_unremovable_leak_marks_workspace_unclean_and_halts_resume(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="fail", rig=RigRef(name="shell"))
    rig = ShellRig("mkdir locked; printf secret > locked/f; chmod 000 locked; exit 7", 30)

    try:
        with pytest.raises(WorkspaceFault):
            Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(tree, 0)
        assert (workdir / "locked").exists()
        with pytest.raises(WorkspaceFault):
            Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(tree, 0)
        assert not replay(run_dir).epoch_closed(0)
    finally:
        if (workdir / "locked").exists():
            (workdir / "locked").chmod(0o700)

    Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(tree, 0)
    assert not (workdir / "locked").exists()
    assert replay(run_dir).epoch_closed(0)


def test_dead_attempt_binary_dirt_is_byte_recoverable_after_quarantine(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))
    tracked = b"\xff\x00TRACKED"
    untracked = b"\xff\x00SECRET"
    nested = b"\x00\xfeNESTED"

    class BinaryDeathRig:
        name = "die"

        def run(self, prompt: str, workdir_arg: Path) -> Result:
            wd = Path(workdir_arg)
            (wd / "base.txt").write_bytes(tracked)
            (wd / "bin.dat").write_bytes(untracked)
            nested_dir = wd / "nested"
            nested_dir.mkdir()
            subprocess.run(["git", "init", "-q"], cwd=nested_dir, check=True)
            (nested_dir / "payload.bin").write_bytes(nested)
            os._exit(0)

    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": BinaryDeathRig()})
    ).run_epoch(tree, 0)) == 0

    Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("rerun")})).run_epoch(tree, 0)
    assert tracked in _capture_bytes(run_dir, "base.txt")
    assert untracked in _capture_bytes(run_dir, "bin.dat")
    assert nested in _capture_bytes(run_dir, "nested/payload.bin")
    assert (workdir / "base.txt").read_bytes() == b"base"
    assert not (workdir / "bin.dat").exists()
    assert not (workdir / "nested").exists()


def test_pending_intent_captures_post_crash_operator_edit_before_reverse(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    target = workdir / "keep"
    target.write_text("ORIGINAL", encoding="utf-8")
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="keep", content="ENGINE")])

    def crash_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _die)
        engine.run_epoch(tree, 0)

    assert _fork(crash_after_write) == 0
    operator = b"\xff\x00OPERATOR AFTER CRASH"
    target.write_bytes(operator)

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert operator in _capture_bytes(run_dir, "keep")
    assert target.read_text(encoding="utf-8") == "ENGINE"
    assert replay(run_dir).epoch_closed(0)


def test_torn_lease_record_fails_with_durable_workspace_fault_before_cleanup(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))
    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": _DieAfterLeakRig("leak.txt")})
    ).run_epoch(tree, 0)) == 0
    lease = next((run_dir / "leases").glob("*.json"))
    lease.write_text('{"epoch":0,"node_id":', encoding="utf-8")

    with pytest.raises(WorkspaceFault, match="lease record"):
        Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("die")})).run_epoch(tree, 0)
    assert (workdir / "leak.txt").read_text() == "leak"
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True
    assert not replay(run_dir).epoch_closed(0)


def test_torn_intent_record_fails_with_durable_workspace_fault_without_overwrite(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    target = workdir / "keep"
    target.write_text("ORIGINAL", encoding="utf-8")
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="keep", content="ENGINE")])

    def crash_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _die)
        engine.run_epoch(tree, 0)

    assert _fork(crash_after_write) == 0
    intent = next((run_dir / "intents").glob("*.json"))
    intent.write_text('{"epoch":0,"writes":[', encoding="utf-8")

    with pytest.raises(WorkspaceFault, match="intent record"):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert target.read_text() == "ENGINE"
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True
    assert not replay(run_dir).epoch_closed(0)


def test_lease_record_publication_is_atomic_across_process_death(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="c"))

    def die_before_record_rename() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"c": _CountingRig("c")}))
        setattr(os, "replace", _die)
        engine.run_epoch(tree, 0)

    assert _fork(die_before_record_rename) == 0
    assert not list((run_dir / "leases").glob("*.json"))
    assert [e["kind"] for e in _events(run_dir)] == ["boundary"]

    rig = _CountingRig("c")
    Engine(run_dir, workdir, RigRegistry({"c": rig})).run_epoch(tree, 0)
    assert rig.calls == 1
    assert replay(run_dir).epoch_closed(0)


def test_quarantine_ref_slug_is_valid_for_all_git_forbidden_run_id_chars(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / r"D4 mid~rig^bad:ref?star*[back]\slash..@{.lock"
    tree = Do(task="die", rig=RigRef(name="die"))

    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": _DieAfterCommitRig("dead.txt")})
    ).run_epoch(tree, 0)) == 0
    Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("rerun")})).run_epoch(tree, 0)

    refs = _quarantine_refs(workdir)
    assert refs
    for ref in refs:
        subprocess.run(["git", "check-ref-format", ref], cwd=workdir, check=True)
    assert replay(run_dir).epoch_closed(0)
