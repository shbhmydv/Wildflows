"""Pass-7 transaction-hardening regressions (hand-11).

Crash-window tests use real ``fork`` + ``os._exit`` deaths.  They intentionally inspect
only durable state from a freshly constructed Engine after the child is gone.
"""
from __future__ import annotations

import json
import os
import shlex
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
    should_not_rerun = _CountingRig("should-not-rerun")
    Engine(run_dir, workdir, RigRegistry({"shell": should_not_rerun})).run_epoch(tree, 0)
    state = replay(run_dir)
    assert should_not_rerun.calls == 0
    assert state.node((0, "n0")).dispatch_count == 1
    assert (workdir / "base.txt").read_text() == "base"
    assert state.epoch_closed(0)
    assert state.node((0, "n0")).workspace_unclean is False
    assert any(e["kind"] == "result" and e["workspace_unclean"] is False
               and "cleanup recovered" in str(e["text"]) for e in _events(run_dir))


def test_retry_marker_survives_crash_between_cleanup_and_redispatch(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))
    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": _DieAfterLeakRig("leak.txt")})
    ).run_epoch(tree, 0)) == 0

    (workdir / ".git" / "index.lock").touch()
    with pytest.raises(WorkspaceFault):
        Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("blocked")})).run_epoch(tree, 0)
    (workdir / ".git" / "index.lock").unlink()

    def die_before_redispatch() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("not-run")}))
        setattr(engine.ws, "open_lease", _die)
        engine.run_epoch(tree, 0)

    assert _fork(die_before_redispatch) == 0
    state = replay(run_dir).node((0, "n0"))
    assert state.workspace_unclean is False and state.recovery_action == "retry"
    assert not replay(run_dir).epoch_closed(0)

    rig = _CountingRig("rerun")
    Engine(run_dir, workdir, RigRegistry({"die": rig})).run_epoch(tree, 0)
    assert rig.calls == 1
    assert replay(run_dir).epoch_closed(0)


def test_post_retry_dispatched_only_do_is_quarantined_before_another_retry(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))
    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": _DieAfterLeakRig("first-leak")})
    ).run_epoch(tree, 0)) == 0
    (workdir / ".git" / "index.lock").touch()
    with pytest.raises(WorkspaceFault):
        Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("blocked")})).run_epoch(tree, 0)
    (workdir / ".git" / "index.lock").unlink()

    class MutateTrackedAndDie:
        name = "die"

        def run(self, prompt: str, workdir_arg: Path) -> Result:
            (Path(workdir_arg) / "base.txt").write_text("MUTATED", encoding="utf-8")
            os._exit(0)

    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": MutateTrackedAndDie()})
    ).run_epoch(tree, 0)) == 0
    assert (workdir / "base.txt").read_text() == "MUTATED"

    rerun = _CountingRig("final")
    Engine(run_dir, workdir, RigRegistry({"die": rerun})).run_epoch(tree, 0)
    assert rerun.calls == 1
    assert (workdir / "base.txt").read_text() == "base"
    assert subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=workdir,
        check=True, capture_output=True, text=True,
    ).stdout == ""


def test_post_retry_dispatched_only_inplace_reverses_latest_intent(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="base.txt", content="ENGINE")])

    def die_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _die)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_write) == 0
    (workdir / ".git" / "index.lock").touch()
    with pytest.raises(WorkspaceFault):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    (workdir / ".git" / "index.lock").unlink()

    # Recovery clears the first halt, dispatches attempt 1, writes, and dies again.
    assert _fork(die_after_write) == 0
    assert (workdir / "base.txt").read_text() == "ENGINE"

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    state = replay(run_dir)
    assert state.epoch_closed(0)
    assert (workdir / "base.txt").read_text() == "ENGINE"
    assert state.integrated[(0, "n0")] == ["base.txt"]


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


def test_failed_inplace_removes_parent_directories_created_by_its_writes(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "dest").mkdir()
    run_dir = tmp_path / "run"

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(Inplace(edits=[
        Edit(path="new/deep/file", content="partial"),
        Edit(path="dest", content="fails because dest is a directory"),
    ]), 0)

    assert not (workdir / "new").exists()
    assert (workdir / "dest").is_dir()
    assert not replay(run_dir).results[(0, "n0")].ok


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
        halted = replay(run_dir).node((0, "n0"))
        assert halted.workspace_unclean is True and halted.recovery_action == "fail"
        with pytest.raises(WorkspaceFault):
            Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(tree, 0)
        assert not replay(run_dir).epoch_closed(0)
    finally:
        if (workdir / "locked").exists():
            (workdir / "locked").chmod(0o700)

    Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(tree, 0)
    assert not (workdir / "locked").exists()
    assert replay(run_dir).epoch_closed(0)


def test_failed_and_dead_rig_empty_directories_are_captured_and_removed(tmp_path: Path) -> None:
    for mode in ("failed", "dead"):
        case = tmp_path / mode
        case.mkdir()
        workdir = case / "work"
        _base_repo(workdir)
        run_dir = case / "run"
        tree = Do(task=mode, rig=RigRef(name="rig"))
        command = "mkdir -p empty/deep; exit 7" if mode == "failed" else "mkdir -p empty/deep"
        rig = ShellRig(command, 30)
        if mode == "dead":
            class EmptyDirDeath:
                name = "rig"

                def run(self, prompt: str, workdir_arg: Path) -> Result:
                    (Path(workdir_arg) / "empty" / "deep").mkdir(parents=True)
                    os._exit(0)

            assert _fork(lambda: Engine(
                run_dir, workdir, RigRegistry({"rig": EmptyDirDeath()})
            ).run_epoch(tree, 0)) == 0
            Engine(run_dir, workdir, RigRegistry({"rig": _CountingRig("rerun")})).run_epoch(
                tree, 0
            )
        else:
            Engine(run_dir, workdir, RigRegistry({"rig": rig})).run_epoch(tree, 0)
        assert not (workdir / "empty").exists()
        manifests = [
            json.loads(path.read_text(encoding="utf-8")) for path in run_dir.rglob("manifest.json")
        ]
        assert any(
            entry["path"] == "empty" and entry["kind"] == "directory"
            for manifest in manifests for entry in manifest["entries"]
        )


def test_failed_rig_net_zero_commits_remain_reachable_after_reset(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    rig = ShellRig(
        "printf SECRET > secret.bin; git add secret.bin; git commit -qm add-secret; "
        "git rm -q secret.bin; git commit -qm delete-secret; exit 7",
        30,
    )

    Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(
        Do(task="fail", rig=RigRef(name="shell")), 0
    )

    recovered = False
    for ref in _quarantine_refs(workdir):
        commits = subprocess.run(
            ["git", "rev-list", ref], cwd=workdir, check=True,
            capture_output=True, text=True,
        ).stdout.splitlines()
        for commit in commits:
            shown = subprocess.run(
                ["git", "show", f"{commit}:secret.bin"], cwd=workdir,
                capture_output=True,
            )
            recovered = recovered or shown.stdout == b"SECRET"
    assert recovered
    assert replay(run_dir).epoch_closed(0)


def test_failed_rig_symlink_alias_to_run_dir_is_captured_and_removed(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    command = f"ln -s {shlex.quote(str(run_dir))} run-alias; exit 7"

    Engine(run_dir, workdir, RigRegistry({"shell": ShellRig(command, 30)})).run_epoch(
        Do(task="fail", rig=RigRef(name="shell")), 0
    )

    assert not os.path.lexists(workdir / "run-alias")
    entries = [
        entry
        for manifest_path in run_dir.rglob("manifest.json")
        for entry in json.loads(manifest_path.read_text(encoding="utf-8"))["entries"]
    ]
    assert any(entry["path"] == "run-alias" and entry["kind"] == "symlink"
               for entry in entries)
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


def test_schema_valid_lease_with_wrong_provenance_halts_before_cleanup(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))
    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": _DieAfterLeakRig("leak.txt")})
    ).run_epoch(tree, 0)) == 0
    lease = next((run_dir / "leases").glob("*.json"))
    record = json.loads(lease.read_text(encoding="utf-8"))
    record["pre_head"] = "0" * 40  # schema-valid but contradicts Dispatched.pre_head
    lease.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(WorkspaceFault, match="integrity|pre_head"):
        Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("die")})).run_epoch(tree, 0)
    assert (workdir / "leak.txt").read_text() == "leak"
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True


def test_schema_valid_intent_with_wrong_identity_halts_without_overwrite(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="base.txt", content="ENGINE")])

    def crash_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _die)
        engine.run_epoch(tree, 0)

    assert _fork(crash_after_write) == 0
    intent = next((run_dir / "intents").glob("*.json"))
    record = json.loads(intent.read_text(encoding="utf-8"))
    record["node_id"] = "n999"
    intent.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(WorkspaceFault, match="integrity|identity mismatch"):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert (workdir / "base.txt").read_text() == "ENGINE"
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True


def test_schema_valid_intent_cannot_delete_an_outside_created_directory(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    outside = tmp_path / "victim"
    outside.mkdir()
    (outside / "keep").write_text("SAFE", encoding="utf-8")
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="new/file", content="ENGINE")])

    def crash_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _die)
        engine.run_epoch(tree, 0)

    assert _fork(crash_after_write) == 0
    intent = next((run_dir / "intents").glob("*.json"))
    record = json.loads(intent.read_text(encoding="utf-8"))
    record["created_dirs"] = ["../victim"]
    record["integrity"] = None  # still schema-valid; unsigned records must fail closed
    intent.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(WorkspaceFault, match="integrity|created directory"):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert (outside / "keep").read_text() == "SAFE"
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True


def test_schema_valid_lease_cannot_reclassify_preexisting_user_bytes(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "keep").write_text("USER", encoding="utf-8")
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))
    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": _DieAfterLeakRig("leak")})
    ).run_epoch(tree, 0)) == 0
    lease = next((run_dir / "leases").glob("*.json"))
    record = json.loads(lease.read_text(encoding="utf-8"))
    record["preexisting"] = []
    record["integrity"] = None
    lease.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(WorkspaceFault, match="integrity"):
        Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("die")})).run_epoch(tree, 0)
    assert (workdir / "keep").read_text() == "USER"
    assert (workdir / "leak").read_text() == "leak"
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True


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
