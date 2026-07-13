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
from wildflows.events import Boundary
from wildflows.expr import Do, Edit, Inplace, RigRef
from wildflows.journal import Journal
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


def test_inplace_rejects_internal_and_external_hard_link_aliases(tmp_path: Path) -> None:
    for external in (False, True):
        case = tmp_path / str(external)
        case.mkdir()
        workdir = case / "work"
        _base_repo(workdir)
        source = case / "outside" if external else workdir / "base.txt"
        if external:
            source.write_text("OUTSIDE", encoding="utf-8")
        os.link(source, workdir / "alias")
        run_dir = case / "run"

        Engine(run_dir, workdir, RigRegistry({})).run_epoch(
            Inplace(edits=[Edit(path="alias", content="MUTATED")]), 0
        )

        result = replay(run_dir).results[(0, "n0")]
        assert not result.ok and "hard-link aliases" in result.text
        assert source.read_text() == ("OUTSIDE" if external else "base")
        assert subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"], cwd=workdir,
            check=True, capture_output=True, text=True,
        ).stdout == ""


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


def test_interrupted_legacy_result_without_receipt_requirement_is_refused(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy"
    run_dir.mkdir()
    records = [
        {"seq": 0, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "boundary",
         "phase": "opened"},
        {"seq": 1, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "dispatched",
         "pre_head": "a" * 40},
        {"seq": 2, "run_id": "r", "epoch": 0, "node_id": "n0", "kind": "result",
         "outcome": "ok", "files": [], "post_head": "b" * 40},
    ]
    (run_dir / "events.ndjson").write_text(
        "\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8"
    )
    from wildflows.journal import JournalCompatibilityError

    with pytest.raises(JournalCompatibilityError, match="legacy"):
        Journal.load(run_dir)


def test_allow_empty_commit_result_tear_still_requires_receipt(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="empty", rig=RigRef(name="empty"))

    class EmptyCommitRig:
        name = "empty"

        def run(self, prompt: str, workdir_arg: Path) -> Result:
            subprocess.run(
                ["git", "commit", "--allow-empty", "-qm", "empty effect"],
                cwd=workdir_arg, check=True,
            )
            return Result(text="empty committed")

    def die_after_result() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"empty": EmptyCommitRig()}))

        def result_then_die(
            key: tuple[int, str], result: Result, receipt: object,
            post_head: str | None = None,
        ) -> None:
            engine.rec.record_result(
                key, result, post_head=post_head, receipt_required=True
            )
            os._exit(0)

        setattr(engine.rec, "record_success", result_then_die)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_result) == 0
    should_not_run = _CountingRig("unexpected")
    Engine(run_dir, workdir, RigRegistry({"empty": should_not_run})).run_epoch(tree, 0)
    assert should_not_run.calls == 0
    receipt = replay(run_dir).receipts[(0, "n0")]
    assert len(receipt.commits) == 1 and receipt.paths == []


def test_successful_rig_cannot_move_head_behind_lease_prehead(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    _commit_file(workdir, "second.txt", "second", "second")
    pre = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    run_dir = tmp_path / "run"

    Engine(run_dir, workdir, RigRegistry({
        "shell": ShellRig("git reset --hard HEAD~1", 30),
    })).run_epoch(Do(task="rewind", rig=RigRef(name="shell")), 0)

    assert subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout.strip() == pre
    assert (workdir / "second.txt").read_text() == "second"
    assert not replay(run_dir).results[(0, "n0")].ok


def test_inactive_torn_result_certificate_is_quarantined_and_rerun(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    pre = _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="c"))

    def die_after_result() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"c": _CountingRig("effect")}))

        def result_then_die(
            key: tuple[int, str], result: Result, receipt: object,
            post_head: str | None = None,
        ) -> None:
            engine.rec.record_result(key, result, post_head=post_head)
            os._exit(0)

        setattr(engine.rec, "record_success", result_then_die)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_result) == 0
    dead = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    subprocess.run(["git", "reset", "--hard", pre], cwd=workdir, check=True,
                   capture_output=True)

    rerun = _CountingRig("effect")
    Engine(run_dir, workdir, RigRegistry({"c": rerun})).run_epoch(tree, 0)
    assert rerun.calls == 1
    assert dead in set(_quarantine_refs(workdir).values())
    state = replay(run_dir)
    assert state.epoch_closed(0) and "effect.txt" in state.integrated[(0, "n0")]
    assert (workdir / "effect.txt").exists()


def test_descendant_revert_of_torn_result_is_quarantined_and_rerun(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="work", rig=RigRef(name="c"))

    def die_after_result() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"c": _CountingRig("effect") }))

        def result_then_die(
            key: tuple[int, str], result: Result, receipt: object,
            post_head: str | None = None,
        ) -> None:
            engine.rec.record_result(key, result, post_head=post_head)
            os._exit(0)

        setattr(engine.rec, "record_success", result_then_die)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_result) == 0
    subprocess.run(["git", "rm", "-q", "effect.txt"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", "operator revert"], cwd=workdir, check=True)
    operator = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout.strip()

    rerun = _CountingRig("effect")
    Engine(run_dir, workdir, RigRegistry({"c": rerun})).run_epoch(tree, 0)
    assert rerun.calls == 1 and (workdir / "effect.txt").exists()
    assert operator in set(_quarantine_refs(workdir).values())


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


def test_preexisting_untracked_bytes_and_empty_dirs_restore_on_failure_and_death(
    tmp_path: Path,
) -> None:
    for mode in ("failed", "dead"):
        case = tmp_path / mode
        case.mkdir()
        workdir = case / "work"
        _base_repo(workdir)
        (workdir / "user.bin").write_bytes(b"\xff\x00ORIGINAL")
        (workdir / "keep-empty").mkdir()
        run_dir = case / "run"
        tree = Do(task=mode, rig=RigRef(name="rig"))

        if mode == "dead":
            class OverwriteAndDie:
                name = "rig"

                def run(self, prompt: str, workdir_arg: Path) -> Result:
                    wd = Path(workdir_arg)
                    (wd / "user.bin").write_bytes(b"MUTATED")
                    (wd / "keep-empty").rmdir()
                    os._exit(0)

            assert _fork(lambda: Engine(
                run_dir, workdir, RigRegistry({"rig": OverwriteAndDie()})
            ).run_epoch(tree, 0)) == 0
            Engine(run_dir, workdir, RigRegistry({"rig": _CountingRig("rerun")})).run_epoch(
                tree, 0
            )
        else:
            command = "printf MUTATED > user.bin; rmdir keep-empty; exit 7"
            Engine(run_dir, workdir, RigRegistry({"rig": ShellRig(command, 30)})).run_epoch(
                tree, 0
            )

        assert (workdir / "user.bin").read_bytes() == b"\xff\x00ORIGINAL"
        assert (workdir / "keep-empty").is_dir()
        assert b"\xff\x00ORIGINAL" in _capture_bytes(run_dir, "user.bin")


def test_baseline_restore_never_follows_substituted_parent_symlink(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "dir").mkdir()
    (workdir / "dir" / "file").write_text("ORIGINAL", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "file").write_text("EXTERNAL", encoding="utf-8")
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="rig"))

    class SubstituteAndDie:
        name = "rig"

        def run(self, prompt: str, workdir_arg: Path) -> Result:
            wd = Path(workdir_arg)
            (wd / "dir" / "file").unlink()
            (wd / "dir").rmdir()
            os.symlink(outside, wd / "dir")
            os._exit(0)

    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"rig": SubstituteAndDie()})
    ).run_epoch(tree, 0)) == 0
    with pytest.raises(WorkspaceFault, match="topology"):
        Engine(run_dir, workdir, RigRegistry({"rig": _CountingRig("rerun")})).run_epoch(tree, 0)
    assert (outside / "file").read_text() == "EXTERNAL"
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True


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


def test_successful_noop_does_not_commit_preexisting_untracked_user_file(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "user.txt").write_text("USER", encoding="utf-8")
    run_dir = tmp_path / "run"

    Engine(run_dir, workdir, RigRegistry({"shell": ShellRig("true", 30)})).run_epoch(
        Do(task="noop", rig=RigRef(name="shell")), 0
    )

    assert (workdir / "user.txt").read_text() == "USER"
    assert subprocess.run(
        ["git", "ls-files", "--error-unmatch", "user.txt"], cwd=workdir,
        capture_output=True,
    ).returncode != 0
    assert replay(run_dir).integrated.get((0, "n0"), []) == []


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


def test_corrupt_baseline_blob_is_detected_before_preexisting_bytes_are_deleted(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "one").write_text("ONE", encoding="utf-8")
    (workdir / "two").write_text("TWO", encoding="utf-8")
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))
    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": _DieAfterLeakRig("leak")})
    ).run_epoch(tree, 0)) == 0
    blob = next((run_dir / "lease-baselines").rglob("blobs/*"))
    blob.write_bytes(b"CORRUPT")

    with pytest.raises(WorkspaceFault, match="blob integrity"):
        Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("die")})).run_epoch(tree, 0)
    assert (workdir / "one").read_text() == "ONE"
    assert (workdir / "two").read_text() == "TWO"
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True


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


def test_new_run_and_journal_directory_entries_are_fsynced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import wildflows.journal as journal_module

    calls: list[Path] = []
    real_fsync = journal_module._fsync_directory

    def recording_fsync(path: Path) -> None:
        calls.append(path)
        real_fsync(path)

    monkeypatch.setattr(journal_module, "_fsync_directory", recording_fsync)
    run_dir = tmp_path / "new-parent" / "run"
    journal = Journal(run_dir)
    assert tmp_path in calls and run_dir.parent in calls
    journal.append(Boundary(
        run_id="run", epoch=0, node_id="n0", phase="opened",
    ))
    assert run_dir in calls


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


def test_run_dir_inside_worktree_is_rejected_before_any_rig_or_journal(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = workdir / ".wildflows" / "run"

    with pytest.raises(ValueError, match="run_dir must be outside workdir"):
        Engine(run_dir, workdir, RigRegistry({"c": _CountingRig("never")}))
    assert not run_dir.exists()


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
