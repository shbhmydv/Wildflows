"""Pass-8 recovery-transaction regressions (hand-12).

Crash windows use real ``fork``/``os._exit`` children: restart assertions consume only
journal and record bytes that survived process death.
"""
from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import NoReturn

import pytest

from wildflows.engine import Engine, replay
from wildflows.expr import Do, Edit, Inplace, RigRef
from wildflows.rig import RigRegistry, Result, ShellRig
from wildflows.workspace import InplaceIntent, WorkspaceEffects, WorkspaceFault

from tests.test_review_fixes import _CountingRig
from tests.test_review_fixes_pass5 import _base_repo
from tests.test_review_fixes_pass7 import _capture_bytes, _events, _fork, _quarantine_refs


def _exit_now(*_args: object, **_kwargs: object) -> NoReturn:
    os._exit(0)


def _sha256_repo(path: Path) -> str:
    path.mkdir()
    init = subprocess.run(
        ["git", "init", "-q", "--object-format=sha256"], cwd=path,
        capture_output=True, text=True,
    )
    if init.returncode != 0:
        pytest.skip("installed Git does not support SHA-256 repositories")
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=path, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=path, check=True)
    (path / "base.txt").write_text("base", encoding="utf-8")
    subprocess.run(["git", "add", "base.txt"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=path, check=True)
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=path, check=True,
        capture_output=True, text=True,
    ).stdout.strip()


def test_cleanup_reset_success_but_postcondition_dirty_remains_halted(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    # Configure only after committing the attribute: the pre-lease file is clean, but a
    # later reset runs a deliberately non-invertible smudge and writes different bytes.
    (workdir / ".gitattributes").write_text("base.txt filter=noninvert\n", encoding="utf-8")
    subprocess.run(["git", "add", ".gitattributes"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", "filter attr"], cwd=workdir, check=True)
    subprocess.run(["git", "config", "filter.noninvert.clean", "cat"], cwd=workdir, check=True)
    subprocess.run(
        ["git", "config", "filter.noninvert.smudge", "sed s/base/baseSMUDGE/"],
        cwd=workdir, check=True,
    )
    subprocess.run(["git", "config", "filter.noninvert.required", "true"], cwd=workdir,
                   check=True)
    assert subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=workdir,
        check=True, capture_output=True, text=True,
    ).stdout == ""

    run_dir = tmp_path / "run"
    tree = Do(task="fail", rig=RigRef(name="shell"))
    with pytest.raises(WorkspaceFault, match="postcondition|tracked|clean"):
        Engine(run_dir, workdir, RigRegistry({
            "shell": ShellRig("printf ATTEMPT > base.txt; exit 7", 30),
        })).run_epoch(tree, 0)

    state = replay(run_dir)
    assert state.node((0, "n0")).workspace_unclean is True
    assert not state.epoch_closed(0)
    assert subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=workdir,
        check=True, capture_output=True, text=True,
    ).stdout != ""


def test_post_intent_hardlink_alias_halts_without_leaking_attempt_bytes(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="new.txt", content="ENGINE")])

    def die_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _exit_now)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_write) == 0
    alias = tmp_path / "outside-alias"
    os.link(workdir / "new.txt", alias)

    with pytest.raises(WorkspaceFault, match="hard-link"):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)

    # Reversal must not unlink/overwrite either name. The retained bytes are now captured
    # forensic evidence and the attempt remains durably halted, never silently cleared.
    assert (workdir / "new.txt").read_bytes() == b"ENGINE"
    assert alias.read_bytes() == b"ENGINE"
    assert b"ENGINE" in _capture_bytes(run_dir, "new.txt")
    state = replay(run_dir)
    assert state.node((0, "n0")).workspace_unclean is True
    assert not state.epoch_closed(0)


def test_post_intent_hidden_hardlink_alias_fails_closed(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="new.txt", content="ENGINE")])

    def die_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _exit_now)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_write) == 0
    alias = tmp_path / "hidden-alias"
    os.link(workdir / "new.txt", alias)
    (workdir / "new.txt").unlink()

    with pytest.raises(WorkspaceFault, match="disappeared|ambiguous"):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert alias.read_bytes() == b"ENGINE"
    state = replay(run_dir)
    assert state.node((0, "n0")).workspace_unclean is True
    assert not state.epoch_closed(0)


def test_missing_required_modern_intent_fails_closed(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="new.txt", content="ENGINE")])

    def die_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _exit_now)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_write) == 0
    next((run_dir / "intents").glob("*.json")).unlink()
    alias = tmp_path / "hidden-alias"
    os.link(workdir / "new.txt", alias)
    (workdir / "new.txt").unlink()

    with pytest.raises(WorkspaceFault, match="required.*intent|intent.*required"):
        Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert alias.read_bytes() == b"ENGINE"
    assert replay(run_dir).node((0, "n0")).workspace_unclean is True


def test_inplace_reversal_path_progress_survives_recovery_crash(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "dest").mkdir()
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[
        Edit(path="new/deep/file", content="ENGINE"),
        Edit(path="dest", content="fails because dest is a directory"),
    ])

    def die_after_rollback() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        real_rollback = engine.ws.rollback_inplace

        def rollback_then_die(
            intent: InplaceIntent, preexisting_dirs: set[str] | None = None,
            *, prevalidated: bool = False,
        ) -> NoReturn:
            real_rollback(intent, preexisting_dirs, prevalidated=prevalidated)
            os._exit(0)

        setattr(engine.ws, "rollback_inplace", rollback_then_die)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_rollback) == 0
    intent_path = next((run_dir / "intents").glob("*.json"))
    intent = json.loads(intent_path.read_text(encoding="utf-8"))
    assert intent["writes"][0]["reversed"] is True
    assert intent["reversed"] is False

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert not (workdir / "new").exists()
    assert not replay(run_dir).results[(0, "n0")].ok
    assert replay(run_dir).epoch_closed(0)


def test_inplace_sweep_completion_survives_recovery_crash(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Inplace(edits=[Edit(path="new/deep/file", content="ENGINE")])

    def die_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _exit_now)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_write) == 0

    def die_after_sweep() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        real_remove = engine.ws._remove_leaks

        def remove_then_die(
            leaks: list[str], preexisting_dirs: set[str] | None = None,
            completion: Callable[[], None] | None = None,
        ) -> NoReturn:
            real_remove(leaks, preexisting_dirs, completion)
            os._exit(0)

        setattr(engine.ws, "_remove_leaks", remove_then_die)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_sweep) == 0
    intent_path = next((run_dir / "intents").glob("*.json"))
    assert json.loads(intent_path.read_text(encoding="utf-8"))["swept"] is True
    assert not (workdir / "new").exists()

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert (workdir / "new" / "deep" / "file").read_text(encoding="utf-8") == "ENGINE"
    assert replay(run_dir).epoch_closed(0)


class _CommitThenSucceed:
    def run(self, prompt: str, workdir: Path) -> Result:
        (workdir / "owned").write_text("effect", encoding="utf-8")
        subprocess.run(["git", "add", "owned"], cwd=workdir, check=True)
        subprocess.run(["git", "commit", "-qm", "rig effect"], cwd=workdir, check=True)
        return Result(text="done")


def _assert_receipt_git_read_failure(
    tmp_path: Path, failed_command: str,
) -> None:
    workdir = tmp_path / "work"
    pre = _base_repo(workdir)
    run_dir = tmp_path / "run"
    engine = Engine(run_dir, workdir, RigRegistry({"commit": _CommitThenSucceed()}))
    real_git_bytes = engine.ws.git_bytes

    def failed_read(
        *args: str, input_data: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        if args and args[0] == failed_command:
            return subprocess.CompletedProcess(list(args), 128, b"", b"injected read failure")
        return real_git_bytes(*args, input_data=input_data)

    setattr(engine.ws, "git_bytes", failed_read)
    engine.run_epoch(Do(task="commit", rig=RigRef(name="commit")), 0)

    state = replay(run_dir)
    assert not state.results[(0, "n0")].ok
    assert (0, "n0") not in state.receipts
    assert subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout.strip() == pre
    assert not (workdir / "owned").exists()
    assert _quarantine_refs(workdir)


def test_rev_list_failure_cannot_bless_rig_commit_as_effectless(tmp_path: Path) -> None:
    _assert_receipt_git_read_failure(tmp_path, "rev-list")


def test_diff_tree_failure_cannot_publish_empty_ownership(tmp_path: Path) -> None:
    _assert_receipt_git_read_failure(tmp_path, "diff-tree")


def test_failed_rig_non_utf8_filename_is_captured_or_durably_halted(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"

    class ByteNameFailure:
        def run(self, prompt: str, workdir_arg: Path) -> Result:
            raw = os.path.join(os.fsencode(workdir_arg), b"bad-\xff")
            fd = os.open(raw, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "wb") as fh:
                fh.write(b"BYTE-NAME-EVIDENCE")
            return Result(text="failed", outcome="failed")

    Engine(run_dir, workdir, RigRegistry({"bytes": ByteNameFailure()})).run_epoch(
        Do(task="bytes", rig=RigRef(name="bytes")), 0
    )

    state = replay(run_dir)
    assert not state.results[(0, "n0")].ok
    assert not os.path.lexists(os.path.join(os.fsencode(workdir), b"bad-\xff"))
    captured: list[bytes] = []
    encoded_paths: list[str] = []
    for manifest_path in (run_dir / "failed-diffs").rglob("manifest.json"):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest["entries"]:
            encoded_paths.append(entry["path"])
            if entry["kind"] == "file":
                captured.append((manifest_path.parent / entry["blob"]).read_bytes())
    assert b"BYTE-NAME-EVIDENCE" in captured
    assert any(path.startswith("@wildflows-bytes:") for path in encoded_paths)


def test_missing_required_modern_lease_cannot_take_legacy_fallback(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "user").write_text("ORIGINAL", encoding="utf-8")
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))

    class MutateAndDie:
        def run(self, prompt: str, workdir_arg: Path) -> Result:
            (workdir_arg / "user").write_text("MUTATED", encoding="utf-8")
            os._exit(0)

    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": MutateAndDie()})
    ).run_epoch(tree, 0)) == 0
    next((run_dir / "leases").glob("*.json")).unlink()

    with pytest.raises(WorkspaceFault, match="required.*lease|lease.*required"):
        Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("rerun")})).run_epoch(tree, 0)
    assert (workdir / "user").read_text() == "MUTATED"
    state = replay(run_dir)
    assert state.node((0, "n0")).workspace_unclean is True
    assert not state.epoch_closed(0)


def test_record_settlement_failure_cannot_clear_unclean_halt(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="fail", rig=RigRef(name="shell"))
    rig = ShellRig("printf MUTATED > base.txt; : > .git/index.lock; exit 7", 30)

    with pytest.raises(WorkspaceFault):
        Engine(run_dir, workdir, RigRegistry({"shell": rig})).run_epoch(tree, 0)
    (workdir / ".git" / "index.lock").unlink()
    engine = Engine(run_dir, workdir, RigRegistry({"shell": _CountingRig("not-run")}))

    def fail_settlement(epoch: int, node_id: str, attempt: int) -> None:
        raise WorkspaceFault("injected settlement failure")

    setattr(engine.ws, "settle_records", fail_settlement)
    with pytest.raises(WorkspaceFault, match="settlement"):
        engine.run_epoch(tree, 0)

    state = replay(run_dir)
    assert state.node((0, "n0")).workspace_unclean is True
    assert not state.epoch_closed(0)
    assert list((run_dir / "leases").glob("*.json"))

    Engine(run_dir, workdir, RigRegistry({"shell": _CountingRig("not-run")})).run_epoch(tree, 0)
    assert replay(run_dir).epoch_closed(0)


def test_quarantine_compare_create_in_sha256_repository(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    pre = _sha256_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))

    class CommitAndDie:
        def run(self, prompt: str, workdir_arg: Path) -> Result:
            (workdir_arg / "dead").write_text("dead", encoding="utf-8")
            subprocess.run(["git", "add", "dead"], cwd=workdir_arg, check=True)
            subprocess.run(["git", "commit", "-qm", "dead"], cwd=workdir_arg, check=True)
            os._exit(0)

    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": CommitAndDie()})
    ).run_epoch(tree, 0)) == 0
    dead = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workdir, check=True,
        capture_output=True, text=True,
    ).stdout.strip()

    Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("rerun")})).run_epoch(tree, 0)
    assert subprocess.run(
        ["git", "merge-base", "--is-ancestor", pre, "HEAD"], cwd=workdir
    ).returncode == 0
    assert dead in set(_quarantine_refs(workdir).values())
    assert replay(run_dir).epoch_closed(0)


def test_inplace_case_aliases_are_rejected_on_case_insensitive_workdir(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(Inplace(edits=[
        Edit(path="New", content="one"),
        Edit(path="new", content="two"),
    ]), 0)

    result = replay(run_dir).results[(0, "n0")]
    assert not result.ok
    assert "case" in result.text.lower() and "collision" in result.text.lower()
    assert not (workdir / "New").exists() and not (workdir / "new").exists()


def test_quarantine_capture_manifest_corruption_is_detected(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))

    class LeakAndDie:
        def run(self, prompt: str, workdir_arg: Path) -> Result:
            (workdir_arg / "leak").write_text("evidence", encoding="utf-8")
            os._exit(0)

    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": LeakAndDie()})
    ).run_epoch(tree, 0)) == 0
    Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("rerun")})).run_epoch(tree, 0)

    manifest = next((run_dir / "quarantine").rglob("manifest.json"))
    parsed = json.loads(manifest.read_text(encoding="utf-8"))
    file_entry = next(entry for entry in parsed["entries"] if entry["kind"] == "file")
    blob = manifest.parent / file_entry["blob"]
    original = blob.read_bytes()
    blob.write_bytes(b"CORRUPT")
    with pytest.raises(WorkspaceFault, match="blob integrity"):
        WorkspaceEffects(workdir, run_dir).load_capture_manifest(manifest)

    blob.write_bytes(original)
    parsed["entries"][0]["path"] = "corrupt-mapping"
    manifest.write_text(json.dumps(parsed), encoding="utf-8")
    with pytest.raises(WorkspaceFault, match="integrity"):
        WorkspaceEffects(workdir, run_dir).load_capture_manifest(manifest)


def test_inplace_git_pathspec_magic_is_always_literal(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    (workdir / "victim").write_text("USER", encoding="utf-8")
    run_dir = tmp_path / "run"
    magic = ":(glob)**"

    Engine(run_dir, workdir, RigRegistry({})).run_epoch(
        Inplace(edits=[Edit(path=magic, content="DECLARED")]), 0
    )

    assert replay(run_dir).integrated[(0, "n0")] == [magic]
    assert subprocess.run(
        ["git", "ls-files", "--error-unmatch", "victim"], cwd=workdir,
        capture_output=True,
    ).returncode != 0
    assert (workdir / "victim").read_text(encoding="utf-8") == "USER"
    assert subprocess.run(
        ["git", "--literal-pathspecs", "show", f"HEAD:{magic}"], cwd=workdir,
        check=True, capture_output=True, text=True,
    ).stdout == "DECLARED"


def test_reserved_byte_path_prefix_roundtrips_inplace_recovery(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    literal = "@wildflows-bytes:literal"
    tree = Inplace(edits=[Edit(path=literal, content="ENGINE")])

    def die_after_write() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({}))
        setattr(engine.ws, "integrate_declared", _exit_now)
        engine.run_epoch(tree, 0)

    assert _fork(die_after_write) == 0
    Engine(run_dir, workdir, RigRegistry({})).run_epoch(tree, 0)
    assert (workdir / literal).read_text(encoding="utf-8") == "ENGINE"
    assert replay(run_dir).integrated[(0, "n0")] == [
        "@wildflows-bytes:QHdpbGRmbG93cy1ieXRlczpsaXRlcmFs"
    ]


def test_non_utf8_receipt_certificate_detects_operator_revert(tmp_path: Path) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="bytes", rig=RigRef(name="bytes"))
    raw_name = b"bad-\xff"

    class CommitByteName:
        def run(self, prompt: str, workdir_arg: Path) -> Result:
            raw_workdir = os.fsencode(workdir_arg)
            target = os.path.join(raw_workdir, raw_name)
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "wb") as fh:
                fh.write(b"EFFECT")
            subprocess.run([b"git", b"add", raw_name], cwd=raw_workdir, check=True)
            subprocess.run(
                [b"git", b"commit", b"-qm", b"byte effect"], cwd=raw_workdir, check=True
            )
            return Result(text="committed")

    def die_after_result() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"bytes": CommitByteName()}))

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
    os.unlink(os.path.join(os.fsencode(workdir), raw_name))
    subprocess.run(["git", "add", "-A"], cwd=workdir, check=True)
    subprocess.run(["git", "commit", "-qm", "operator revert"], cwd=workdir, check=True)

    rerun = _CountingRig("rerun")
    Engine(run_dir, workdir, RigRegistry({"bytes": rerun})).run_epoch(tree, 0)
    assert rerun.calls == 1
    receipt_paths = replay(run_dir).receipts[(0, "n0")].paths
    assert not any(path.startswith("@wildflows-bytes:") for path in receipt_paths)
    assert replay(run_dir).epoch_closed(0)


def test_crash_after_recovery_settlement_before_publication_resumes_from_receipt(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "work"
    _base_repo(workdir)
    run_dir = tmp_path / "run"
    tree = Do(task="die", rig=RigRef(name="die"))

    class LeakAndDie:
        def run(self, prompt: str, workdir_arg: Path) -> Result:
            (workdir_arg / "leak").write_text("leak", encoding="utf-8")
            os._exit(0)

    assert _fork(lambda: Engine(
        run_dir, workdir, RigRegistry({"die": LeakAndDie()})
    ).run_epoch(tree, 0)) == 0

    def settle_then_die() -> None:
        engine = Engine(run_dir, workdir, RigRegistry({"die": _CountingRig("not-yet")}))
        setattr(engine.rec, "record_result", _exit_now)
        engine.run_epoch(tree, 0)

    assert _fork(settle_then_die) == 0
    assert not list((run_dir / "leases").glob("*.json"))
    assert list((run_dir / "recoveries").glob("*.json"))

    rerun = _CountingRig("rerun")
    Engine(run_dir, workdir, RigRegistry({"die": rerun})).run_epoch(tree, 0)
    assert rerun.calls == 1
    assert replay(run_dir).epoch_closed(0)
    assert not (workdir / "leak").exists()
    assert any(e["kind"] == "result" for e in _events(run_dir))
