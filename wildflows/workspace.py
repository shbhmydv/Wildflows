"""Durable workspace effect transaction and completion recorder."""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import select
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, ValidationError

from wildflows.events import Integrated, ResultEvent
from wildflows.journal import Journal
from wildflows.projection import NodeKey
from wildflows.result import CommitReceipt, IntegrationReceipt, Result

_EMPTY_TREES = {
    "sha1": "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
    "sha256": "6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321",
}
_PATH_BYTES_PREFIX = "@wildflows-bytes:"


def _wire_path(raw: bytes) -> str:
    """Reversible JSON representation for Git's arbitrary POSIX pathname bytes."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = ""
    if text and not text.startswith(_PATH_BYTES_PREFIX):
        return text
    return _PATH_BYTES_PREFIX + base64.urlsafe_b64encode(raw).decode("ascii")


def _fs_path(wire: str) -> str:
    if not wire.startswith(_PATH_BYTES_PREFIX):
        return wire
    try:
        return os.fsdecode(base64.urlsafe_b64decode(wire[len(_PATH_BYTES_PREFIX):]))
    except (ValueError, binascii.Error) as exc:
        raise WorkspaceFault(f"invalid encoded pathname {wire!r}: {exc}") from exc


class WorkspaceFault(Exception):
    """An unproved effect/recovery; halt unclean and retain `diff_path` evidence."""

    def __init__(self, message: str, diff_path: "Path | None" = None) -> None:
        super().__init__(message)
        self.diff_path = diff_path


class LeaseRecord(BaseModel):
    epoch: int
    node_id: str
    attempt: int
    pre_head: str | None
    preexisting: list[str] = Field(default_factory=list)
    preexisting_dirs: list[str] | None = None
    ts: float
    baseline_manifest: str | None = None
    baseline_digest: str | None = None
    index_snapshot: bool = False
    index_b64: str | None = None
    content_tree: str | None = None
    raw_tree: str | None = None
    integrity: str | None = None


class IntentWrite(BaseModel):
    path: str
    pre_kind: Literal["file", "dir", "absent", "other"]
    original: str | None = None  # legacy text; modern records use exact base64
    original_b64: str | None = None
    content: str | None = None
    started: bool = False
    reversed: bool = False
    swept: bool = False


class InplaceIntent(BaseModel):
    epoch: int
    node_id: str
    attempt: int
    writes: list[IntentWrite]
    created_dirs: list[str] = Field(default_factory=list)
    ts: float
    # Published after complete reversal so a recovery crash after an engine unlink can
    # redo without misclassifying the now-absent target as a hidden external alias.
    reversed: bool = False
    # Set by the checked leak sweep's completion callback after all removals/fsyncs.
    swept: bool = False
    integrity: str | None = None


class CaptureEntry(BaseModel):
    path: str
    kind: Literal["file", "directory", "symlink", "absent"]
    size: int | None = None
    sha256: str | None = None
    blob: str | None = None
    link_target: str | None = None


class CaptureManifest(BaseModel):
    entries: list[CaptureEntry]
    integrity: str | None = None


class RecoveryRecord(BaseModel):
    epoch: int
    node_id: str
    attempt: int
    action: Literal["fail", "retry"]
    lease: LeaseRecord
    result: Result
    integrity: str | None = None


class CompletionSettlement(BaseModel):
    epoch: int
    node_id: str
    attempt: int
    pre_head: str | None
    lease: LeaseRecord | None
    intent: InplaceIntent | None
    recovery_integrity: str | None = None
    integrity: str | None = None


class ProcessRecord(BaseModel):
    epoch: int
    node_id: str
    attempt: int
    pid: int
    pgid: int
    start_time: int
    integrity: str | None = None


@dataclass
class Lease:
    node_key: NodeKey
    pre_head: str | None
    attempt: int = 0
    preexisting: frozenset[str] = frozenset()
    preexisting_dirs: frozenset[str] = frozenset()
    baseline_manifest: str | None = None
    baseline_digest: str | None = None
    index_snapshot: bool = False
    index_b64: str | None = None
    content_tree: str | None = None
    raw_tree: str | None = None


@dataclass
class Integration:
    status: Literal["committed", "noop", "failed"]
    commit: str | None = None
    paths: list[str] = field(default_factory=list)
    stderr: str = ""


@dataclass
class DoIntegration:
    ok: bool
    receipt: IntegrationReceipt = field(default_factory=IntegrationReceipt)
    stderr: str = ""


@dataclass(frozen=True)
class RecoveryRequest:
    node_key: NodeKey
    attempt: int
    expected_pre_head: str | None
    lease_required: bool
    intent_required: bool
    action: Literal["fail", "retry"]
    result: Result
    evidence_kind: Literal["failed-diffs", "quarantine"]
    certified_post_head: str | None = None


@dataclass(frozen=True)
class RecoveryOutcome:
    action: Literal["fail", "retry"]
    result: Result


class WorkspaceEffects:
    def __init__(self, workdir: Path, run_dir: Path) -> None:
        self.workdir = Path(workdir)
        self.run_dir = Path(run_dir)

    @staticmethod
    def encode_path(rel: str) -> str:
        return _wire_path(os.fsencode(rel))

    @staticmethod
    def decode_path(wire: str) -> str:
        return _fs_path(wire)

    def _work_path(self, wire: str) -> Path:
        return self.workdir / _fs_path(wire)

    @staticmethod
    def _path_depth(wire: str) -> int:
        return len(Path(_fs_path(wire)).parts)

    # -- lease ---------------------------------------------------------------

    def open_lease(self, node_key: NodeKey, attempt: int) -> Lease | None:
        """Open a durable baseline, or refuse a dirty tracked/index worktree."""
        if self.tracked_dirty():
            return None
        index = self._index_bytes()
        lease = Lease(
            node_key=node_key, pre_head=self.head_commit(), attempt=attempt,
            preexisting=frozenset(self._untracked_ignored_paths()),
            preexisting_dirs=frozenset(self._workspace_dirs()), index_snapshot=True,
            index_b64=None if index is None else base64.b64encode(index).decode("ascii"),
            content_tree=self._fresh_workspace_tree(), raw_tree=self._raw_tracked_snapshot()[0],
        )
        self._capture_lease_baseline(lease)
        self._write_lease_record(lease)
        return lease

    def tracked_dirty(self) -> bool:
        """True if the worktree has any uncommitted TRACKED or staged change (untracked
        files excluded). The lease-open precondition."""
        proc = self.git_bytes("status", "--porcelain", "--untracked-files=no", "-z")
        self._checked_bytes(proc, "inspect tracked workspace state")
        if proc.stderr.strip():
            raise WorkspaceFault(
                "tracked workspace inspection was incomplete: " + self._output(proc.stderr)
            )
        return bool(proc.stdout)

    def verify_lease_unchanged(self, lease: Lease) -> None:
        """Compare actual content and the real index, restoring index bytes before return."""
        epoch, node_id = lease.node_key
        record = self.load_lease_record(epoch, node_id, lease.attempt)
        if record is None:
            raise WorkspaceFault("predicate lease record disappeared before verification")
        expected = self._record_index_bytes(record)
        try:
            index_changed = self._index_bytes() != expected
            content_changed = (
                self._fresh_workspace_tree() != record.content_tree
                or self._raw_tracked_snapshot()[0] != record.raw_tree
            )
        finally:
            self._restore_index_snapshot(record)
        if index_changed or content_changed:
            raise WorkspaceFault("predicate changed index metadata or workspace content")
        self._verify_recovery_postconditions(record, None)

    # -- durable transaction intents (PRINCIPLE B) ---------------------------

    def _record_path(self, kind: str, epoch: int, node_id: str, attempt: int) -> Path:
        return self.run_dir / kind / f"{epoch}-{node_id}-{attempt}.json"

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        """Durably publish a directory-entry change (rename/unlink/mkdir)."""
        try:
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError as exc:
            raise WorkspaceFault(f"directory fsync failed for {path}: {exc}") from exc

    def _atomic_write(self, path: Path, data: bytes, what: str) -> None:
        """Crash-atomically publish bytes: same-dir temp + fsync + replace + dir fsync."""
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fsync_dir(path.parent.parent)
            fd, raw_tmp = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
            tmp = Path(raw_tmp)
            try:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(data)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, path)
                self._fsync_dir(path.parent)
            except BaseException:
                try:
                    tmp.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
        except WorkspaceFault:
            raise
        except OSError as exc:
            raise WorkspaceFault(f"atomic {what} publication failed for {path}: {exc}") from exc

    def _fsync_json(self, path: Path, model: BaseModel) -> None:
        self._atomic_write(path, model.model_dump_json().encode("utf-8"), "record")

    @staticmethod
    def _record_integrity(model: BaseModel) -> str:
        payload = json.dumps(
            model.model_dump(mode="json", exclude={"integrity"}),
            sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _verify_record_integrity(self, model: BaseModel, path: Path) -> None:
        integrity = getattr(model, "integrity", None)
        if integrity is None:
            raise WorkspaceFault(f"durable record has no integrity digest: {path}")
        if not hmac.compare_digest(integrity, self._record_integrity(model)):
            raise WorkspaceFault(f"durable record integrity mismatch: {path}")

    def _write_lease_record(self, lease: Lease) -> None:
        epoch, node_id = lease.node_key
        self._fsync_json(
            self._record_path("leases", epoch, node_id, lease.attempt),
            self._signed_lease_record(lease),
        )

    def _signed_lease_record(self, lease: Lease) -> LeaseRecord:
        epoch, node_id = lease.node_key
        record = LeaseRecord(
            epoch=epoch, node_id=node_id, attempt=lease.attempt,
            pre_head=lease.pre_head, preexisting=sorted(lease.preexisting),
            preexisting_dirs=sorted(lease.preexisting_dirs), ts=time.time(),
            baseline_manifest=lease.baseline_manifest,
            baseline_digest=lease.baseline_digest, index_snapshot=lease.index_snapshot,
            index_b64=lease.index_b64, content_tree=lease.content_tree,
            raw_tree=lease.raw_tree,
        )
        record.integrity = self._record_integrity(record)
        return record

    def _capture_lease_baseline(self, lease: Lease) -> None:
        if not lease.preexisting:
            return
        epoch, node_id = lease.node_key
        base = "HEAD" if lease.pre_head is not None else self._empty_tree()
        index = self._capture_workspace(
            self.run_dir / "lease-baselines",
            f"{epoch}-{node_id}-{lease.attempt}",
            base,
            sorted(lease.preexisting),
        )
        if index is None:
            raise WorkspaceFault("preexisting lease paths had no recoverable baseline")
        manifest = index.with_suffix(".capture") / "manifest.json"
        try:
            raw = manifest.read_bytes()
            lease.baseline_manifest = manifest.relative_to(self.run_dir).as_posix()
        except (OSError, ValueError) as exc:
            raise WorkspaceFault(f"cannot index lease baseline {manifest}: {exc}") from exc
        lease.baseline_digest = hashlib.sha256(raw).hexdigest()

    def load_lease_record(self, epoch: int, node_id: str, attempt: int) -> LeaseRecord | None:
        return self._load_lease_at(
            self._record_path("leases", epoch, node_id, attempt), epoch, node_id, attempt
        )

    def _load_settled_lease(
        self, epoch: int, node_id: str, attempt: int, pre_head: str | None
    ) -> LeaseRecord | None:
        directory = self.run_dir / "settled-leases"
        matches: list[LeaseRecord] = []
        for path in directory.glob(f"{epoch}-{node_id}-{attempt}-*.json"):
            record = self._load_lease_at(path, epoch, node_id, attempt)
            if record is not None and record.pre_head == pre_head:
                matches.append(record)
        if len(matches) > 1 and any(
            not self._lease_equivalent(matches[0], record) for record in matches[1:]
        ):
            raise WorkspaceFault("ambiguous settled lease records")
        return matches[0] if matches else None

    def _settled_lease_path(self, record: LeaseRecord) -> Path:
        digest = self._record_integrity(record)[:16]
        return self.run_dir / "settled-leases" / (
            f"{record.epoch}-{record.node_id}-{record.attempt}-{digest}.json"
        )

    def _load_lease_at(
        self, path: Path, epoch: int, node_id: str, attempt: int
    ) -> LeaseRecord | None:
        if not os.path.lexists(path):
            return None
        try:
            if not stat.S_ISREG(path.lstat().st_mode) or path.is_symlink():
                raise WorkspaceFault(f"lease record is not a regular file: {path}")
            record = LeaseRecord.model_validate_json(path.read_bytes())
            self._verify_record_integrity(record, path)
            if (record.epoch, record.node_id, record.attempt) != (epoch, node_id, attempt):
                raise WorkspaceFault(
                    f"lease record identity mismatch for {path}: "
                    f"{record.epoch}/{record.node_id}/{record.attempt}"
                )
            return record
        except WorkspaceFault:
            raise
        except (OSError, ValidationError) as exc:
            raise WorkspaceFault(f"lease record is unreadable or corrupt: {path}: {exc}") from exc

    def write_intent(self, intent: InplaceIntent) -> None:
        signed = intent.model_copy(deep=True)
        signed.integrity = self._record_integrity(signed)
        self._fsync_json(
            self._record_path("intents", intent.epoch, intent.node_id, intent.attempt), signed
        )

    def load_intent(self, epoch: int, node_id: str, attempt: int) -> InplaceIntent | None:
        path = self._record_path("intents", epoch, node_id, attempt)
        if not os.path.lexists(path):
            return None
        try:
            if not stat.S_ISREG(path.lstat().st_mode) or path.is_symlink():
                raise WorkspaceFault(f"intent record is not a regular file: {path}")
            record = InplaceIntent.model_validate_json(path.read_bytes())
            self._verify_record_integrity(record, path)
            if (record.epoch, record.node_id, record.attempt) != (epoch, node_id, attempt):
                raise WorkspaceFault(
                    f"intent record identity mismatch for {path}: "
                    f"{record.epoch}/{record.node_id}/{record.attempt}"
                )
            return record
        except WorkspaceFault:
            raise
        except (OSError, ValidationError) as exc:
            raise WorkspaceFault(f"intent record is unreadable or corrupt: {path}: {exc}") from exc

    def settle_records(self, epoch: int, node_id: str, attempt: int) -> None:
        """Archive the signed lease, then unlink/fsync active lease and intent records."""
        lease = self.load_lease_record(epoch, node_id, attempt)
        if lease is not None:
            archive = self._settled_lease_path(lease)
            if not os.path.lexists(archive):
                self._fsync_json(archive, lease)
        for kind in ("leases", "intents"):
            path = self._record_path(kind, epoch, node_id, attempt)
            try:
                if os.path.lexists(path):
                    path.unlink()
                    self._fsync_dir(path.parent)
            except OSError as exc:
                raise WorkspaceFault(f"record settlement failed for {path}: {exc}") from exc

    def settle_completed_attempt(
        self, epoch: int, node_id: str, attempt: int, expected_pre_head: str | None,
        *, lease_required: bool, intent_required: bool,
    ) -> None:
        """Validate required records and durably begin settlement before integration.

        The create-once certificate closes the settlement→Integrated crash window: it
        retains the validated records while active-record unlink is redone idempotently.
        """
        settled = self._load_completion_settlement(epoch, node_id, attempt)
        if settled is None:
            recovery = self._load_recovery_record(epoch, node_id, attempt)
            lease = self.load_lease_record(epoch, node_id, attempt)
            if lease is None:
                lease = self._load_settled_lease(
                    epoch, node_id, attempt, expected_pre_head
                )
            if recovery is not None:
                if recovery.lease.pre_head != expected_pre_head:
                    raise WorkspaceFault(
                        "recovery receipt contradicts dispatched provenance"
                    )
                lease = recovery.lease
            if lease is None and lease_required:
                raise WorkspaceFault("required modern lease record is missing")
            if lease is not None and lease.pre_head != expected_pre_head:
                raise WorkspaceFault("lease record pre_head contradicts dispatched provenance")
            intent = self.load_intent(epoch, node_id, attempt)
            if intent is None and intent_required and recovery is None:
                raise WorkspaceFault("required modern inplace intent record is missing")
            settled = CompletionSettlement(
                epoch=epoch, node_id=node_id, attempt=attempt,
                pre_head=expected_pre_head, lease=lease, intent=intent,
                recovery_integrity=None if recovery is None else recovery.integrity,
            )
            settled.integrity = self._record_integrity(settled)
            self._publish_completion_settlement(settled)
        if settled.pre_head != expected_pre_head:
            raise WorkspaceFault("completion settlement contradicts dispatched provenance")
        if lease_required and settled.lease is None:
            raise WorkspaceFault("required modern lease settlement is missing")
        if intent_required and settled.intent is None and settled.recovery_integrity is None:
            raise WorkspaceFault("required modern inplace intent settlement is missing")
        self.settle_records(epoch, node_id, attempt)

    def _completion_settlement_path(self, epoch: int, node_id: str, attempt: int) -> Path:
        return self._record_path("settlements", epoch, node_id, attempt)

    def _load_completion_settlement(
        self, epoch: int, node_id: str, attempt: int
    ) -> CompletionSettlement | None:
        path = self._completion_settlement_path(epoch, node_id, attempt)
        if not os.path.lexists(path):
            return None
        try:
            if not stat.S_ISREG(path.lstat().st_mode) or path.is_symlink():
                raise WorkspaceFault(f"completion settlement is not a regular file: {path}")
            record = CompletionSettlement.model_validate_json(path.read_bytes())
            self._verify_record_integrity(record, path)
            if (record.epoch, record.node_id, record.attempt) != (epoch, node_id, attempt):
                raise WorkspaceFault(f"completion settlement identity mismatch: {path}")
            return record
        except WorkspaceFault:
            raise
        except (OSError, ValidationError) as exc:
            raise WorkspaceFault(
                f"completion settlement is unreadable or corrupt: {path}: {exc}"
            ) from exc

    def _publish_completion_settlement(self, record: CompletionSettlement) -> None:
        path = self._completion_settlement_path(record.epoch, record.node_id, record.attempt)
        prior = self._load_completion_settlement(record.epoch, record.node_id, record.attempt)
        if prior is not None:
            if prior != record:
                raise WorkspaceFault(f"completion settlement collision: {path}")
            return
        self._fsync_json(path, record)

    @staticmethod
    def _lease_equivalent(left: LeaseRecord, right: LeaseRecord) -> bool:
        """Ignore publication time/path when the same attempt baseline was re-captured."""
        return (
            (left.epoch, left.node_id, left.attempt, left.pre_head)
            == (right.epoch, right.node_id, right.attempt, right.pre_head)
            and left.preexisting == right.preexisting
            and left.preexisting_dirs == right.preexisting_dirs
            and left.baseline_digest == right.baseline_digest
            and (left.index_snapshot, left.index_b64, left.content_tree, left.raw_tree)
            == (right.index_snapshot, right.index_b64, right.content_tree, right.raw_tree)
        )

    # -- the ONE lease-recovery transaction ---------------------------------

    def recover_lease(self, request: RecoveryRequest) -> RecoveryOutcome:
        """Validate → capture → quarantine → reset → restore → verify → settle.

        Every destructive caller uses this transaction.  A create-once recovery receipt
        is published after postconditions and before lease/intent settlement; if the
        process dies before the engine publishes its Result, resume re-verifies from that
        receipt and publishes the same deterministic disposition.
        """
        epoch, node_id = request.node_key
        self.reap_process(epoch, node_id, request.attempt)
        receipt = self._load_recovery_record(epoch, node_id, request.attempt)
        settlement = self._load_completion_settlement(epoch, node_id, request.attempt)
        if settlement is not None and settlement.pre_head != request.expected_pre_head:
            raise WorkspaceFault("completion settlement contradicts dispatched provenance")

        # Phase 1: validate every durable input before mutation.  Recovery/completion
        # receipts replace records that their settlement already removed.
        lease = self.load_lease_record(epoch, node_id, request.attempt)
        if lease is None:
            lease = self._load_settled_lease(
                epoch, node_id, request.attempt, request.expected_pre_head
            )
        if receipt is not None:
            if (
                receipt.action != request.action
                or receipt.lease.pre_head != request.expected_pre_head
            ):
                raise WorkspaceFault("recovery receipt contradicts dispatched provenance")
            lease = receipt.lease
        elif lease is None and settlement is not None:
            lease = settlement.lease
        if lease is None:
            if request.lease_required:
                raise WorkspaceFault("required modern lease record is missing")
            lease = LeaseRecord(
                epoch=epoch, node_id=node_id, attempt=request.attempt,
                pre_head=request.expected_pre_head,
                preexisting=self._untracked_ignored_paths(), preexisting_dirs=None, ts=0.0,
            )
        if lease.pre_head != request.expected_pre_head:
            raise WorkspaceFault("lease record pre_head contradicts dispatched provenance")
        intent = self.load_intent(epoch, node_id, request.attempt)
        if intent is None and settlement is not None:
            intent = settlement.intent
        if intent is None and request.intent_required and receipt is None:
            raise WorkspaceFault("required modern inplace intent record is missing")
        if lease.baseline_manifest is not None:
            self._load_lease_baseline(lease.baseline_manifest, lease.baseline_digest)
        unexpected: list[str] = []
        if intent is not None:
            self._validate_intent_targets(
                intent, None if lease.preexisting_dirs is None else set(lease.preexisting_dirs)
            )
            unexpected = [
                write.path for write in intent.writes
                if not write.reversed
                and not self._matches_expected(write)
                and not self._matches_prestate(write)
            ]

        head = self._cleanup_head()
        diff_path: Path | None = None
        if receipt is None or not self._recovery_postconditions(lease):
            # Phase 2: capture every byte before the first reset/unlink/overwrite.
            snapshot_changes = self._snapshot_changed_paths(lease)
            base = (
                lease.pre_head or self._empty_tree()
                if request.evidence_kind == "failed-diffs"
                else ("HEAD" if head is not None else self._empty_tree())
            )
            leaks = self._attempt_leaks(lease.preexisting, lease.preexisting_dirs)
            diff_path = self._capture_workspace(
                self.run_dir / request.evidence_kind,
                (f"e{epoch}-{node_id}" if request.evidence_kind == "failed-diffs"
                 else f"{epoch}-{node_id}-{request.attempt}"), base,
                sorted(set(leaks) | set(lease.preexisting) | set(snapshot_changes)),
            )
            if unexpected:
                divergent = self._capture_workspace(
                    self.run_dir / "intent-reversal", f"{epoch}-{node_id}-{request.attempt}",
                    "HEAD" if head is not None else self._empty_tree(), unexpected,
                )
                diff_path = diff_path or divergent

            # Phase 3: preserve every observed/certified history before changing HEAD.
            if request.certified_post_head is not None:
                self._checked(
                    self.git("cat-file", "-e", f"{request.certified_post_head}^{{commit}}"),
                    "inactive certificate object lookup", diff_path,
                )
                self._preserve_tip(lease, request.certified_post_head, diff_path)
            if head is not None and head != lease.pre_head:
                self._preserve_tip(lease, head, diff_path)

            # Phase 4: reverse intent, restore exact committed/unborn baseline, sweep leaks.
            if intent is not None and not intent.reversed:
                self.rollback_inplace(
                    intent,
                    None if lease.preexisting_dirs is None else set(lease.preexisting_dirs),
                    prevalidated=True,
                )
                intent.reversed = True
                self.write_intent(intent)
            self._reset_to_pre_head(lease, diff_path)
            preexisting_dirs = (
                None if lease.preexisting_dirs is None else set(lease.preexisting_dirs)
            )

            def prepare_sweep(rel: str) -> None:
                if intent is None:
                    return
                # Recheck aliases immediately before publishing the per-path proof.  A
                # prior swept path may now be absent; an unmarked disappearance still halts.
                self._validate_intent_targets(intent, preexisting_dirs)
                leak = Path(_fs_path(rel))
                changed = False
                for write in intent.writes:
                    target = Path(_fs_path(write.path))
                    if (
                        write.pre_kind == "absent"
                        and not write.swept
                        and (target == leak or target.is_relative_to(leak))
                    ):
                        write.swept = True
                        changed = True
                if changed:
                    self.write_intent(intent)

            def mark_swept() -> None:
                if intent is not None:
                    intent.swept = True
                    self.write_intent(intent)

            self._remove_leaks(
                self._attempt_leaks(lease.preexisting, lease.preexisting_dirs),
                preexisting_dirs, mark_swept if intent is not None else None,
                prepare_sweep if intent is not None else None,
            )

            # Phase 5: byte-restore the user baseline through the shared checked loader.
            self._restore_preexisting(
                lease.preexisting, lease.preexisting_dirs,
                lease.baseline_manifest, lease.baseline_digest,
            )
            # Cleanup Git reads may refresh stat-cache bytes; restore the exact index last.
            self._restore_index_snapshot(lease)

            # Phase 6: commands are not success; the complete end state is.
            self._verify_recovery_postconditions(lease, diff_path)

        final_result = receipt.result if receipt is not None else self._recovery_result(
            request.result, diff_path
        )
        if receipt is None:
            receipt = RecoveryRecord(
                epoch=epoch, node_id=node_id, attempt=request.attempt,
                action=request.action, lease=lease, result=final_result,
            )
            receipt.integrity = self._record_integrity(receipt)
            self._publish_recovery_record(receipt)

        # Phase 7: settlement completes before the engine may publish halt-clear/final.
        self.settle_records(epoch, node_id, request.attempt)
        return RecoveryOutcome(receipt.action, receipt.result)

    def resume_recovery(
        self, node_key: NodeKey, attempt: int, expected_pre_head: str | None
    ) -> RecoveryOutcome | None:
        """Finish a verified recovery whose Result publication was torn."""
        epoch, node_id = node_key
        receipt = self._load_recovery_record(epoch, node_id, attempt)
        if receipt is None:
            return None
        return self.recover_lease(RecoveryRequest(
            node_key=node_key, attempt=attempt, expected_pre_head=expected_pre_head,
            lease_required=True, intent_required=False,
            action=receipt.action, result=receipt.result,
            evidence_kind="failed-diffs" if receipt.action == "fail" else "quarantine",
        ))

    def _recovery_path(self, epoch: int, node_id: str, attempt: int) -> Path:
        return self._record_path("recoveries", epoch, node_id, attempt)

    def _load_recovery_record(
        self, epoch: int, node_id: str, attempt: int
    ) -> RecoveryRecord | None:
        path = self._recovery_path(epoch, node_id, attempt)
        if not os.path.lexists(path):
            return None
        try:
            if not stat.S_ISREG(path.lstat().st_mode) or path.is_symlink():
                raise WorkspaceFault(f"recovery record is not a regular file: {path}")
            record = RecoveryRecord.model_validate_json(path.read_bytes())
            self._verify_record_integrity(record, path)
            if (record.epoch, record.node_id, record.attempt) != (epoch, node_id, attempt):
                raise WorkspaceFault(f"recovery record identity mismatch: {path}")
            return record
        except WorkspaceFault:
            raise
        except (OSError, ValidationError) as exc:
            raise WorkspaceFault(f"recovery record is unreadable or corrupt: {path}: {exc}") from exc

    def _publish_recovery_record(self, record: RecoveryRecord) -> None:
        path = self._recovery_path(record.epoch, record.node_id, record.attempt)
        prior = self._load_recovery_record(record.epoch, record.node_id, record.attempt)
        if prior is not None:
            if prior != record:
                raise WorkspaceFault(f"recovery receipt collision: {path}")
            return
        self._fsync_json(path, record)

    @staticmethod
    def _ref_component(raw: str) -> str:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", raw)
        cleaned = re.sub(r"\.{2,}", ".", cleaned).strip(".")
        changed = cleaned != raw
        if not cleaned:
            return f"q-{digest}"
        if cleaned.endswith(".lock") or len(cleaned) > 80:
            changed = True
        cleaned = cleaned[:80].rstrip(".") or "q"
        return f"{cleaned}-{digest}" if changed else cleaned

    def _quarantine_ref(self, rec: LeaseRecord, head: str) -> str:
        run = self._ref_component(self.run_dir.name)
        node = self._ref_component(rec.node_id)
        return f"refs/wildflows/quarantine/{run}/e{rec.epoch}-{node}-a{rec.attempt}-{head}"

    def _preserve_tip(self, rec: LeaseRecord, head: str, diff_path: Path | None) -> None:
        ref = self._quarantine_ref(rec, head)
        current = self.git("rev-parse", "--verify", "--quiet", ref)
        if current.returncode == 0:
            if current.stdout.strip() != head:
                raise WorkspaceFault(f"quarantine ref collision at {ref}", diff_path)
            return
        if current.returncode != 1:
            raise WorkspaceFault(
                f"quarantine ref lookup failed: {current.stderr.strip()}", diff_path
            )
        self._checked(
            self.git("update-ref", ref, head, self._null_oid()),
            "quarantine append-only update-ref", diff_path,
        )

    def _reset_to_pre_head(self, rec: LeaseRecord, diff_path: Path | None) -> None:
        if rec.pre_head is not None:
            self._checked(
                self.git("reset", "--hard", rec.pre_head), "recovery reset --hard", diff_path
            )
            return
        symbolic = self.git("symbolic-ref", "-q", "HEAD")
        if symbolic.returncode not in (0, 1):
            self._checked(symbolic, "recovery unborn symbolic-ref", diff_path)
        ref = symbolic.stdout.strip()
        if self._cleanup_head() is not None and ref:
            self._checked(
                self.git("update-ref", "-d", ref), "recovery unborn update-ref -d", diff_path
            )
        self._checked(self.git("reset"), "recovery unborn reset", diff_path)

    def _recovery_postconditions(self, rec: LeaseRecord) -> bool:
        try:
            self._verify_recovery_postconditions(rec, None)
            return True
        except WorkspaceFault:
            return False

    def _verify_recovery_postconditions(
        self, rec: LeaseRecord, diff_path: Path | None
    ) -> None:
        if self._cleanup_head() != rec.pre_head:
            raise WorkspaceFault("recovery postcondition failed: HEAD differs from lease", diff_path)
        if rec.index_snapshot:
            if self._index_bytes() != self._record_index_bytes(rec):
                raise WorkspaceFault("recovery postcondition failed: index bytes differ", diff_path)
            if (
                self._fresh_workspace_tree() != rec.content_tree
                or self._raw_tracked_snapshot()[0] != rec.raw_tree
            ):
                raise WorkspaceFault("recovery postcondition failed: workspace content differs", diff_path)
        elif self.tracked_dirty():
            raise WorkspaceFault("recovery postcondition failed: tracked/index state is dirty", diff_path)
        if self._attempt_leaks(rec.preexisting, rec.preexisting_dirs):
            raise WorkspaceFault("recovery postcondition failed: attempt leaks remain", diff_path)
        if rec.preexisting_dirs is not None:
            lease = Lease(
                node_key=(rec.epoch, rec.node_id), pre_head=rec.pre_head, attempt=rec.attempt,
                preexisting=frozenset(rec.preexisting),
                preexisting_dirs=frozenset(rec.preexisting_dirs),
                baseline_manifest=rec.baseline_manifest, baseline_digest=rec.baseline_digest,
                index_snapshot=rec.index_snapshot, index_b64=rec.index_b64,
                content_tree=rec.content_tree, raw_tree=rec.raw_tree,
            )
            if not self._preexisting_baseline_unchanged(lease):
                raise WorkspaceFault(
                    "recovery postcondition failed: baseline bytes differ", diff_path
                )
            for rel in rec.preexisting_dirs:
                target = self._work_path(rel)
                if not target.is_dir() or target.is_symlink():
                    raise WorkspaceFault(
                        f"recovery postcondition failed: baseline directory differs: {rel}",
                        diff_path,
                    )
        if rec.index_snapshot:
            self._restore_index_snapshot(rec)
            if self._index_bytes() != self._record_index_bytes(rec):
                raise WorkspaceFault("recovery postcondition failed: index restore differed", diff_path)

    def _recovery_result(self, result: Result, diff_path: Path | None) -> Result:
        if diff_path is None:
            return result
        return result.model_copy(update={
            "text": f"{result.text}\n[workspace evidence captured: {diff_path}]"
        })

    def _capture_workspace(
        self, index_dir: Path, stem: str, tracked_base: str, leaks: list[str]
    ) -> Path | None:
        """Durably copy exact current bytes plus a binary Git patch before destruction.

        The human-readable ``.diff`` remains an index, while every current regular file
        (tracked binary dirt, untracked/ignored files, and nested-repository contents) is
        copied into an immutable sibling ``.capture`` directory and named by a manifest.
        Any enumerate/read/write/fsync failure raises ``WorkspaceFault`` before reset/sweep.
        """
        diff = self.git_bytes("diff", "--binary", tracked_base)
        self._checked_bytes(diff, "capture git diff")
        names = self.git_bytes("diff", "--name-only", "-z", tracked_base)
        self._checked_bytes(names, "capture git diff --name-only")
        tracked = [_wire_path(name) for name in names.stdout.split(b"\0") if name]
        if not tracked and not leaks:
            return None

        index_path, capture_dir = self._allocate_capture(index_dir, stem)
        entries: list[CaptureEntry] = []
        seen: set[str] = set()
        try:
            for rel in [*tracked, *leaks]:
                self._capture_path(rel, self._work_path(rel), capture_dir, entries, seen)
            manifest = CaptureManifest(entries=entries)
            manifest.integrity = self._record_integrity(manifest)
            self._fsync_json(capture_dir / "manifest.json", manifest)
            parts = [diff.stdout] if diff.stdout.strip() else []
            parts.extend(self._capture_evidence(capture_dir, entry) for entry in entries)
            self._atomic_write(
                index_path, b"\n".join(p for p in parts if p), "capture index"
            )
            self._fsync_dir(capture_dir)
            return index_path
        except WorkspaceFault:
            raise
        except OSError as exc:
            raise WorkspaceFault(f"filesystem capture failed: {exc}", index_path) from exc

    def _allocate_capture(self, index_dir: Path, stem: str) -> tuple[Path, Path]:
        try:
            index_dir.mkdir(parents=True, exist_ok=True)
            self._fsync_dir(index_dir.parent)
            suffix = 0
            while True:
                name = stem if suffix == 0 else f"{stem}-{suffix}"
                capture_dir = index_dir / f"{name}.capture"
                try:
                    capture_dir.mkdir()
                    self._fsync_dir(index_dir)
                    return index_dir / f"{name}.diff", capture_dir
                except FileExistsError:
                    suffix += 1
        except WorkspaceFault:
            raise
        except OSError as exc:
            raise WorkspaceFault(f"capture directory allocation failed: {exc}") from exc

    def _capture_path(
        self,
        rel: str,
        target: Path,
        capture_dir: Path,
        entries: list[CaptureEntry],
        seen: set[str],
    ) -> None:
        rel = rel.rstrip("/")
        if rel in seen:
            return
        seen.add(rel)
        root = self.workdir.resolve()
        expected = root / _fs_path(rel)
        try:
            if target.parent.resolve() != expected.parent:
                raise WorkspaceFault(f"capture path changed topology: {rel}")
        except OSError as exc:
            raise WorkspaceFault(f"cannot resolve capture parent for {rel}: {exc}") from exc
        try:
            info = target.lstat()
        except FileNotFoundError:
            entries.append(CaptureEntry(path=rel, kind="absent"))
            return
        except OSError as exc:
            raise WorkspaceFault(f"cannot stat capture source {rel}: {exc}") from exc

        if stat.S_ISLNK(info.st_mode):
            try:
                link_target = _wire_path(os.fsencode(os.readlink(target)))
            except OSError as exc:
                raise WorkspaceFault(f"cannot read captured symlink {rel}: {exc}") from exc
            entries.append(CaptureEntry(path=rel, kind="symlink", link_target=link_target))
            return
        if stat.S_ISDIR(info.st_mode):
            entries.append(CaptureEntry(path=rel, kind="directory"))
            try:
                children = sorted(os.scandir(target), key=lambda entry: entry.name)
            except OSError as exc:
                raise WorkspaceFault(f"cannot enumerate capture directory {rel}: {exc}") from exc
            for child in children:
                child_fs = Path(child.path).relative_to(self.workdir).as_posix()
                child_rel = _wire_path(os.fsencode(child_fs))
                self._capture_path(child_rel, Path(child.path), capture_dir, entries, seen)
            return
        if not stat.S_ISREG(info.st_mode):
            raise WorkspaceFault(f"cannot byte-capture special filesystem object: {rel}")
        entries.append(self._capture_file(rel, target, capture_dir))

    def _capture_file(self, rel: str, source: Path, capture_dir: Path) -> CaptureEntry:
        blobs = capture_dir / "blobs"
        try:
            blobs.mkdir(exist_ok=True)
            fd, raw_tmp = tempfile.mkstemp(prefix=".blob-", dir=blobs)
            digest = hashlib.sha256()
            size = 0
            try:
                with open(source, "rb") as src, os.fdopen(fd, "wb") as dst:
                    while chunk := src.read(1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                        dst.write(chunk)
                    dst.flush()
                    os.fsync(dst.fileno())
                sha = digest.hexdigest()
                blob = blobs / sha
                os.replace(raw_tmp, blob)
                self._fsync_dir(blobs)
            except BaseException:
                try:
                    Path(raw_tmp).unlink(missing_ok=True)
                except OSError:
                    pass
                raise
        except WorkspaceFault:
            raise
        except OSError as exc:
            raise WorkspaceFault(f"cannot byte-capture file {rel}: {exc}") from exc
        return CaptureEntry(
            path=rel, kind="file", size=size, sha256=sha,
            blob=(Path("blobs") / sha).as_posix(),
        )

    def _capture_evidence(self, capture_dir: Path, entry: CaptureEntry) -> bytes:
        header = f"=== captured: {entry.path} ({entry.kind}) ==="
        if entry.kind != "file" or entry.blob is None:
            return f"{header}\n{entry.link_target or ''}".encode("utf-8")
        data = (capture_dir / entry.blob).read_bytes()
        try:
            body = data.decode("utf-8")
        except UnicodeDecodeError:
            body = (
                f"<binary artifact: {entry.size} bytes, sha256={entry.sha256}, "
                f"blob={entry.blob}>"
            )
        return f"{header}\n{body}".encode("utf-8")

    def load_capture_manifest(self, path: Path) -> CaptureManifest:
        """Checked shared loader for baseline and forensic captures."""
        manifest, _blobs = self._load_capture_manifest(Path(path))
        return manifest

    def _load_capture_manifest(
        self, path: Path
    ) -> tuple[CaptureManifest, dict[str, bytes]]:
        try:
            if not stat.S_ISREG(path.lstat().st_mode) or path.is_symlink():
                raise WorkspaceFault(f"capture manifest is not a regular file: {path}")
            if not path.resolve().is_relative_to(self.run_dir.resolve()):
                raise WorkspaceFault(f"capture manifest escapes run directory: {path}")
            manifest = CaptureManifest.model_validate_json(path.read_bytes())
            self._verify_record_integrity(manifest, path)
        except WorkspaceFault:
            raise
        except (OSError, ValidationError) as exc:
            raise WorkspaceFault(f"capture manifest is unreadable or corrupt: {path}: {exc}") from exc
        blobs: dict[str, bytes] = {}
        seen: set[str] = set()
        root = path.parent.resolve()
        for entry in manifest.entries:
            decoded = Path(_fs_path(entry.path))
            if (
                not entry.path or decoded.is_absolute() or ".." in decoded.parts
                or entry.path in seen
            ):
                raise WorkspaceFault(f"invalid or duplicate capture manifest path: {entry.path!r}")
            seen.add(entry.path)
            if entry.kind == "file":
                if entry.blob is None or entry.sha256 is None or entry.size is None:
                    raise WorkspaceFault(f"incomplete capture file entry: {entry.path}")
                blob = path.parent / entry.blob
                try:
                    if not blob.resolve().is_relative_to(root):
                        raise WorkspaceFault(f"capture blob escapes manifest: {entry.path}")
                    data = blob.read_bytes()
                except WorkspaceFault:
                    raise
                except OSError as exc:
                    raise WorkspaceFault(f"cannot read capture blob {entry.path}: {exc}") from exc
                if len(data) != entry.size or not hmac.compare_digest(
                    hashlib.sha256(data).hexdigest(), entry.sha256
                ):
                    raise WorkspaceFault(f"capture blob integrity mismatch: {entry.path}")
                blobs[entry.path] = data
            elif entry.kind == "symlink" and entry.link_target is None:
                raise WorkspaceFault(f"incomplete capture symlink entry: {entry.path}")
            elif entry.kind not in ("directory", "symlink", "absent"):
                raise WorkspaceFault(f"unknown capture entry kind: {entry.kind}")
        return manifest, blobs

    def _load_lease_baseline(
        self, manifest_rel: str | None, digest: str | None
    ) -> tuple[Path, CaptureManifest, dict[str, bytes]] | None:
        if manifest_rel is None:
            if digest is not None:
                raise WorkspaceFault("lease baseline digest has no manifest")
            return None
        rel = Path(manifest_rel)
        if rel.is_absolute() or ".." in rel.parts:
            raise WorkspaceFault(f"lease baseline escapes run directory: {manifest_rel}")
        path = self.run_dir / rel
        try:
            if path.resolve().parent.parent != (self.run_dir / "lease-baselines").resolve():
                raise WorkspaceFault(f"lease baseline has invalid location: {manifest_rel}")
            raw = path.read_bytes()
            if digest is None or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), digest):
                raise WorkspaceFault(f"lease baseline integrity mismatch: {path}")
            manifest, blobs = self._load_capture_manifest(path)
        except WorkspaceFault:
            raise
        except OSError as exc:
            raise WorkspaceFault(f"lease baseline is unreadable or corrupt: {path}: {exc}") from exc
        return path.parent, manifest, blobs

    def _restore_preexisting(
        self,
        preexisting: list[str],
        preexisting_dirs: list[str] | None,
        manifest_rel: str | None,
        digest: str | None,
    ) -> None:
        loaded = self._load_lease_baseline(manifest_rel, digest)
        if preexisting and loaded is None:
            return  # conservative no-record journal fallback: current paths were untouched
        capture_dir: Path | None = None
        manifest = CaptureManifest(entries=[])
        blob_data: dict[str, bytes] = {}
        if loaded is not None:
            capture_dir, manifest, blob_data = loaded
        # Validate EVERY path and blob before the first delete. A corrupt late entry must
        # never be discovered only after earlier user bytes have already been removed.
        pre_targets = {
            rel: self._contained_record_target(rel, "preexisting path")
            for rel in set(preexisting)
        }
        dir_targets = {
            rel: self._contained_record_target(rel, "preexisting directory")
            for rel in set(preexisting_dirs or [])
        }
        entry_targets = {
            entry.path: self._contained_record_target(entry.path, "baseline entry")
            for entry in manifest.entries
        }
        for rel, target in sorted(pre_targets.items(), key=lambda item: self._path_depth(item[0])):
            if os.path.lexists(target):
                self._remove_rollback_target(target, rel)
        for rel, target in sorted(
            dir_targets.items(), key=lambda item: self._path_depth(item[0])
        ):
            if os.path.lexists(target) and (not target.is_dir() or target.is_symlink()):
                self._remove_rollback_target(target, rel)
            try:
                target.mkdir(parents=True, exist_ok=True)
                self._fsync_dir(target.parent)
            except OSError as exc:
                raise WorkspaceFault(
                    f"cannot restore preexisting directory {rel}: {exc}"
                ) from exc
        if capture_dir is None:
            return
        for entry in sorted(
            manifest.entries,
            key=lambda item: (0 if item.kind == "directory" else 1, self._path_depth(item.path)),
        ):
            target = entry_targets[entry.path]
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                if entry.kind == "directory":
                    target.mkdir(exist_ok=True)
                elif entry.kind == "file":
                    self._atomic_write(target, blob_data[entry.path], "lease baseline restore")
                elif entry.kind == "symlink":
                    assert entry.link_target is not None  # preflight above
                    os.symlink(_fs_path(entry.link_target), target)
                    self._fsync_dir(target.parent)
                elif entry.kind != "absent":
                    raise WorkspaceFault(f"unknown baseline entry kind: {entry.kind}")
            except WorkspaceFault:
                raise
            except OSError as exc:
                raise WorkspaceFault(f"cannot restore lease baseline {entry.path}: {exc}") from exc

    def _contained_record_target(self, rel: str, what: str) -> Path:
        lexical = Path(_fs_path(rel))
        root = self.workdir.resolve()
        if not rel or lexical.is_absolute() or ".." in lexical.parts:
            raise WorkspaceFault(f"invalid {what}: {rel!r}")
        target = root / lexical
        try:
            resolved_parent = target.parent.resolve()
        except OSError as exc:
            raise WorkspaceFault(f"cannot resolve parent for {what} {rel!r}: {exc}") from exc
        if (
            not target.is_relative_to(root)
            or resolved_parent != target.parent
            or not resolved_parent.is_relative_to(root)
        ):
            raise WorkspaceFault(f"{what} escapes workdir through path topology: {rel!r}")
        return target

    def _checked(
        self, proc: subprocess.CompletedProcess[str], what: str, diff_path: Path | None = None
    ) -> None:
        """Every cleanup/rollback git op is checked (PRINCIPLE A): a non-zero exit raises
        `WorkspaceFault` so the engine never journals a durable "failed" claiming a live
        effect was reverted when the revert itself failed."""
        if proc.returncode != 0:
            raise WorkspaceFault(f"{what} failed: {proc.stderr.strip()}", diff_path)

    # -- do finalization -----------------------------------------------------

    def finalize_do_success(self, lease: Lease, message: str) -> DoIntegration:
        """Record every commit the rig authored in `pre..post` (each with its own paths),
        then commit any remaining dirty state, as ONE accumulated receipt.

        A rig legitimately authors several commits; every one is attributed verifiably
        (rev-list + per-commit diff-tree), so no earlier commit is dropped (defect 4).
        A git failure integrating the dirty remainder returns `ok=False` with stderr.
        """
        post = self.head_commit()
        topology_error = self._range_topology_error(lease.pre_head, post)
        if topology_error is not None:
            return DoIntegration(ok=False, stderr=topology_error)
        commits = self._commit_receipts(lease.pre_head, post)
        if any(
            self._is_preexisting_path(path, lease.preexisting)
            for commit in commits for path in commit.paths
        ):
            return DoIntegration(
                ok=False, stderr="rig commit claimed a preexisting untracked/ignored path"
            )
        try:
            if not self._preexisting_baseline_unchanged(lease):
                return DoIntegration(
                    ok=False, stderr="rig modified a preexisting untracked/ignored path"
                )
        except WorkspaceFault as fault:
            return DoIntegration(ok=False, stderr=str(fault))
        integ = self._commit_all(message, lease)
        if integ.status == "failed":
            return DoIntegration(ok=False, stderr=integ.stderr)
        if integ.status == "committed" and integ.commit is not None:
            commits.append(CommitReceipt(sha=integ.commit, paths=integ.paths))
        return DoIntegration(ok=True, receipt=IntegrationReceipt(commits=commits))

    def _workspace_dirs(self) -> list[str]:
        """Snapshot preexisting directories so cleanup may safely prune only new parents."""
        found: list[str] = []
        errors: list[OSError] = []

        def onerror(exc: OSError) -> None:
            errors.append(exc)

        for root, dirs, _files in os.walk(
            self.workdir, topdown=True, onerror=onerror, followlinks=False
        ):
            root_path = Path(root)
            kept: list[str] = []
            for name in dirs:
                path = root_path / name
                rel = _wire_path(os.fsencode(path.relative_to(self.workdir).as_posix()))
                if name == ".git" and root_path == self.workdir:
                    continue
                if path.is_symlink():
                    continue
                found.append(rel)
                kept.append(name)
            dirs[:] = kept
        if errors:
            raise WorkspaceFault(f"cannot snapshot workspace directories: {errors[0]}")
        return found

    def _attempt_leaks(
        self, preexisting: list[str], preexisting_dirs: list[str] | None
    ) -> list[str]:
        paths = set(self._untracked_ignored_paths()) - set(preexisting)
        if preexisting_dirs is not None:
            new_dirs = set(self._workspace_dirs()) - set(preexisting_dirs)
            roots = {
                rel for rel in new_dirs
                if not any(
                    _wire_path(os.fsencode(parent.as_posix())) in new_dirs
                    for parent in Path(_fs_path(rel)).parents
                )
            }
            paths.update(roots)
        return sorted(paths, key=lambda rel: (self._path_depth(rel), rel))

    def _untracked_ignored_paths(self) -> list[str]:
        """The untracked (`??`) and ignored (`!!`) paths git reports PER FILE
        (`--untracked-files=all`), so an addition under a pre-existing untracked directory
        is its own entry (hand-9, FAILURE-LEASE). A
        nested Git repo still appears as ONE `dir/` entry — git never recurses into it —
        and is walked/removed as a directory by the evidence/sweep helpers."""
        status = self.git_bytes(
            "status", "--ignored", "--porcelain", "-z", "--untracked-files=all"
        )
        self._checked_bytes(status, "enumerate untracked/ignored paths")
        if status.stderr.strip():
            raise WorkspaceFault(
                "enumerate untracked/ignored paths was incomplete: " + self._output(status.stderr)
            )
        out: list[str] = []
        for entry in status.stdout.split(b"\0"):
            if not entry or entry[:2] not in (b"??", b"!!"):
                continue
            out.append(_wire_path(entry[3:]))
        return out

    def _remove_leaks(
        self, leaks: list[str], preexisting_dirs: set[str] | None = None,
        completion: Callable[[], None] | None = None,
        prepare: Callable[[str], None] | None = None,
    ) -> None:
        """Checked lease-scoped sweep with durable per-target preparation."""
        removed: list[Path] = []
        for rel in leaks:
            target = self._work_path(rel)
            try:
                info = target.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise WorkspaceFault(f"cannot stat leak before removal {rel}: {exc}") from exc
            if prepare is not None:
                prepare(rel)
            try:
                if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                    shutil.rmtree(target)
                else:
                    target.unlink()
                self._fsync_dir(target.parent)
            except OSError as exc:
                raise WorkspaceFault(f"cannot remove workspace leak {rel}: {exc}") from exc
            if os.path.lexists(target):
                raise WorkspaceFault(f"workspace leak remained after removal: {rel}")
            removed.append(target)
        if preexisting_dirs is None:
            if completion is not None:
                completion()
            return
        # Git's per-file status omits now-empty parent directories. Prune a parent only
        # when the modern lease proved it did not preexist, stopping at any nonempty/user
        # directory and verifying every successful removal.
        for target in removed:
            parent = target.parent
            while parent != self.workdir:
                rel_parent = _wire_path(os.fsencode(parent.relative_to(self.workdir).as_posix()))
                if rel_parent in preexisting_dirs:
                    break
                try:
                    parent.rmdir()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    if parent.exists():  # nonempty is a safe stop; other errors are faults
                        try:
                            next(parent.iterdir())
                        except StopIteration:
                            raise WorkspaceFault(
                                f"cannot remove empty leak directory {rel_parent}: {exc}"
                            ) from exc
                        except OSError as inspect_exc:
                            raise WorkspaceFault(
                                f"cannot verify leak directory {rel_parent}: {inspect_exc}"
                            ) from inspect_exc
                    break
                if os.path.lexists(parent):
                    raise WorkspaceFault(
                        f"workspace leak directory remained after removal: {rel_parent}"
                    )
                self._fsync_dir(parent.parent)
                parent = parent.parent
        if completion is not None:
            completion()

    def certificate_is_active(self, post_head: str | None, paths: list[str]) -> bool:
        if post_head is None:
            return False
        live = self._cleanup_head()
        if live is None:
            return False
        ancestor = self.git("merge-base", "--is-ancestor", post_head, live)
        if ancestor.returncode == 1:
            return False
        self._checked(ancestor, "certificate reachability check")
        if not paths:  # an active allow-empty commit still requires its history receipt
            return True
        active = self.git(
            "--literal-pathspecs", "diff", "--quiet", post_head, "--",
            *[_fs_path(path) for path in paths],
        )
        if active.returncode == 0:
            return True
        if active.returncode == 1:
            return False
        raise WorkspaceFault(f"certificate path check failed: {active.stderr.strip()}")

    def reconstruct_receipt(
        self, pre_head: str | None, post_head: str | None
    ) -> IntegrationReceipt:
        """The receipt for an attempt bounded by its TWO durable heads: EVERY commit in
        EXACTLY `pre_head..post_head`, each with its paths (hand-9, PROVENANCE-RANGE).

        `post_head` is the workdir HEAD the attempt DURABLY recorded on its `result` — the
        completion certificate. Reconstruction never uses live `HEAD`, so an operator commit
        made after process death (above `post_head`) is outside the range by construction
        and can never be misattributed to this attempt. Only a torn result-then-integrated
        window (a durable result whose `integrated` was lost) is reconstructable; a
        dispatched-only tail has no `post_head` and is re-run, not recovered."""
        topology_error = self._range_topology_error(pre_head, post_head)
        if topology_error is not None:
            raise WorkspaceFault(topology_error)
        return IntegrationReceipt(commits=self._commit_receipts(pre_head, post_head))

    def _range_topology_error(self, pre: str | None, post: str | None) -> str | None:
        if pre is None:
            return None  # unborn lease: zero or more first commits are valid
        if post is None:
            return "attempt removed HEAD after opening from a committed base"
        ancestor = self.git("merge-base", "--is-ancestor", pre, post)
        if ancestor.returncode == 0:
            return None
        if ancestor.returncode == 1:
            return f"attempt HEAD {post} does not descend from lease pre_head {pre}"
        return f"attempt provenance check failed: {ancestor.stderr.strip()}"

    # -- inplace finalization ------------------------------------------------

    def integrate_declared(self, declared: list[str], message: str) -> Integration:
        """Commit ONLY the declared paths (an `inplace`), via a `--`-scoped pathspec so
        any pre-existing staged index is preserved. Git failures return
        status="failed"; nothing staged for our scope is a "noop". Never raises."""
        pathspecs = [_fs_path(path) for path in declared]
        add = self.git("--literal-pathspecs", "add", "--", *pathspecs)
        if add.returncode != 0:
            return Integration("failed", stderr=add.stderr)
        diff = self.git(
            "--literal-pathspecs", "diff", "--cached", "--quiet", "--", *pathspecs
        )
        if diff.returncode == 0:
            return Integration("noop")
        if diff.returncode != 1:
            return Integration("failed", stderr=diff.stderr)
        commit = self.git(
            "--literal-pathspecs", "commit", "-q", "-m", message, "--", *pathspecs
        )
        if commit.returncode != 0:
            return Integration("failed", stderr=commit.stderr)
        sha = self._cleanup_head()
        if sha is None:
            return Integration("failed", stderr="commit succeeded but HEAD is unborn")
        return Integration("committed", commit=sha, paths=list(declared))

    def rollback_inplace(
        self, intent: InplaceIntent, preexisting_dirs: set[str] | None = None,
        *, prevalidated: bool = False,
    ) -> None:
        """Reverse a durable intent without destroying post-crash operator bytes.

        Current state matching either the expected engine write or the recorded pre-state
        is an idempotent rollback case. Anything else is byte-captured first; only after
        that manifest is durable are canonical targets restored and unstaged.
        """
        if not prevalidated:
            self._validate_intent_targets(intent, preexisting_dirs)
            unexpected = [
                w.path for w in intent.writes
                if not self._matches_expected(w) and not self._matches_prestate(w)
            ]
            if unexpected:
                self._capture_workspace(
                    self.run_dir / "intent-reversal",
                    f"{intent.epoch}-{intent.node_id}-{intent.attempt}",
                    "HEAD" if self._cleanup_head() is not None else self._empty_tree(),
                    unexpected,
                )
        for write in intent.writes:
            if write.reversed:
                continue
            target = self._work_path(write.path)
            if write.pre_kind == "file":
                self._atomic_write(target, self._original_bytes(write), "inplace rollback")
            elif write.pre_kind == "absent":
                # Leave the checked, singly-linked target for the lease leak sweep.  The
                # per-path marker is durable first; every redo rechecks nlink if it remains.
                pass
            elif write.pre_kind in ("dir", "other"):
                if not self._matches_prestate(write):
                    raise WorkspaceFault(
                        f"legacy inplace intent cannot restore pre-state for {write.path}"
                    )
            write.reversed = True
            self.write_intent(intent)
        if intent.writes:
            self._checked(
                self.git(
                    "--literal-pathspecs", "reset", "-q", "--",
                    *[_fs_path(w.path) for w in intent.writes],
                ),
                "inplace rollback unstage",
            )

    def _validate_intent_targets(
        self, intent: InplaceIntent, preexisting_dirs: set[str] | None
    ) -> None:
        root = self.workdir.resolve()
        allowed_dirs = {
            _wire_path(os.fsencode(parent.as_posix()))
            for write in intent.writes
            for parent in Path(_fs_path(write.path)).parents
            if parent.as_posix() != "."
        }
        if len(intent.created_dirs) != len(set(intent.created_dirs)):
            raise WorkspaceFault("inplace intent contains duplicate created directories")
        for rel in intent.created_dirs:
            lexical = Path(_fs_path(rel))
            if (
                not rel
                or lexical.is_absolute()
                or ".." in lexical.parts
                or ".git" in lexical.parts
                or rel not in allowed_dirs
                or (preexisting_dirs is not None and rel in preexisting_dirs)
            ):
                raise WorkspaceFault(f"invalid created directory in inplace intent: {rel!r}")
            expected_dir = root / lexical
            try:
                actual_dir = self._work_path(rel).resolve()
            except OSError as exc:
                raise WorkspaceFault(
                    f"cannot resolve created inplace directory {rel}: {exc}"
                ) from exc
            if actual_dir != expected_dir or not actual_dir.is_relative_to(root):
                raise WorkspaceFault(f"created inplace directory changed topology: {rel}")
        linked: list[str] = []
        disappeared: list[str] = []
        for write in intent.writes:
            expected = root / _fs_path(write.path)
            try:
                actual = self._work_path(write.path).resolve()
            except OSError as exc:
                raise WorkspaceFault(
                    f"cannot resolve canonical inplace target {write.path}: {exc}"
                ) from exc
            if actual != expected or not actual.is_relative_to(root) or self._in_gitdir(actual):
                raise WorkspaceFault(
                    f"canonical inplace target changed after intent publication: {write.path}"
                )
            try:
                info = actual.lstat()
            except FileNotFoundError:
                if write.started and not (write.swept or intent.swept):
                    disappeared.append(write.path)
                continue
            except OSError as exc:
                raise WorkspaceFault(f"cannot stat inplace target {write.path}: {exc}") from exc
            if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
                linked.append(write.path)
        if disappeared:
            evidence = self._capture_workspace(
                self.run_dir / "intent-reversal",
                f"{intent.epoch}-{intent.node_id}-{intent.attempt}-disappeared",
                "HEAD" if self._cleanup_head() is not None else self._empty_tree(),
                disappeared,
            )
            raise WorkspaceFault(
                "started inplace target disappeared; hidden hard-link alias is ambiguous",
                evidence,
            )
        if linked:
            evidence = self._capture_workspace(
                self.run_dir / "intent-reversal",
                f"{intent.epoch}-{intent.node_id}-{intent.attempt}-hardlink",
                "HEAD" if self._cleanup_head() is not None else self._empty_tree(), linked,
            )
            raise WorkspaceFault(
                "inplace reversal found post-intent hard-link aliases; refusing overwrite/unlink",
                evidence,
            )

    def _original_bytes(self, write: IntentWrite) -> bytes:
        if write.original_b64 is not None:
            try:
                return base64.b64decode(write.original_b64, validate=True)
            except (ValueError, binascii.Error) as exc:
                raise WorkspaceFault(
                    f"inplace intent has invalid original bytes for {write.path}: {exc}"
                ) from exc
        return (write.original or "").encode("utf-8")

    def _matches_expected(self, write: IntentWrite) -> bool:
        if write.content is None:  # legacy intent did not record what the attempt wrote
            return False
        target = self._work_path(write.path)
        try:
            return target.is_file() and not target.is_symlink() and (
                target.read_bytes() == write.content.encode("utf-8")
            )
        except OSError as exc:
            raise WorkspaceFault(f"cannot compare inplace target {write.path}: {exc}") from exc

    def _matches_prestate(self, write: IntentWrite) -> bool:
        target = self._work_path(write.path)
        try:
            if write.pre_kind == "absent":
                return not os.path.lexists(target)
            if write.pre_kind == "file":
                return target.is_file() and not target.is_symlink() and (
                    target.read_bytes() == self._original_bytes(write)
                )
            if write.pre_kind == "dir":
                return target.is_dir() and not target.is_symlink()
            return False
        except OSError as exc:
            raise WorkspaceFault(f"cannot compare inplace pre-state {write.path}: {exc}") from exc

    def _remove_rollback_target(self, target: Path, rel: str) -> None:
        try:
            info = target.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise WorkspaceFault(f"cannot stat inplace rollback target {rel}: {exc}") from exc
        try:
            if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                shutil.rmtree(target)
            else:
                target.unlink()
            self._fsync_dir(target.parent)
        except OSError as exc:
            raise WorkspaceFault(f"cannot remove inplace rollback target {rel}: {exc}") from exc
        if os.path.lexists(target):
            raise WorkspaceFault(f"inplace rollback target remained after removal: {rel}")

    # -- index-independent workspace snapshots ------------------------------

    def _index_path(self) -> Path:
        proc = self.git_bytes("rev-parse", "--git-path", "index")
        self._checked_bytes(proc, "locate repository index")
        path = Path(os.fsdecode(proc.stdout.rstrip(b"\n")))
        return path if path.is_absolute() else self.workdir / path

    def _index_bytes(self) -> bytes | None:
        path = self._index_path()
        if not os.path.lexists(path):
            return None
        try:
            if not stat.S_ISREG(path.lstat().st_mode) or path.is_symlink():
                raise WorkspaceFault(f"repository index is not a regular file: {path}")
            return path.read_bytes()
        except OSError as exc:
            raise WorkspaceFault(f"cannot snapshot repository index {path}: {exc}") from exc

    @staticmethod
    def _record_index_bytes(rec: LeaseRecord | Lease) -> bytes | None:
        if not rec.index_snapshot:
            raise WorkspaceFault("lease has no required index snapshot")
        if rec.index_b64 is None:
            return None
        try:
            return base64.b64decode(rec.index_b64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise WorkspaceFault(f"lease index snapshot is corrupt: {exc}") from exc

    def _restore_index_snapshot(self, rec: LeaseRecord | Lease) -> None:
        if not rec.index_snapshot:
            return
        expected = self._record_index_bytes(rec)
        path = self._index_path()
        try:
            if self._index_bytes() == expected:
                return
        except WorkspaceFault:
            pass
        if expected is not None:
            self._atomic_write(path, expected, "index snapshot")
        else:
            try:
                if os.path.lexists(path):
                    path.unlink()
                    self._fsync_dir(path.parent)
            except OSError as exc:
                raise WorkspaceFault(f"cannot restore absent repository index: {exc}") from exc

    def _fresh_workspace_tree(self) -> str:
        """Hash actual worktree content through a temporary index, never the live index."""
        with tempfile.TemporaryDirectory(prefix="wildflows-index-") as directory:
            env = os.environ.copy()
            env["GIT_INDEX_FILE"] = str(Path(directory) / "index")
            read = ("read-tree", "--empty") if self._cleanup_head() is None else (
                "read-tree", "HEAD"
            )
            self._checked_bytes(self._git_index(env, *read), "seed temporary index")
            self._checked_bytes(
                self._git_index(env, "add", "-u", "--", "."), "snapshot workspace content"
            )
            tree = self._git_index(env, "write-tree")
            self._checked_bytes(tree, "write temporary-index tree")
            try:
                return tree.stdout.strip().decode("ascii")
            except UnicodeDecodeError as exc:
                raise WorkspaceFault(f"temporary tree OID is invalid: {exc}") from exc

    def _raw_tracked_snapshot(self) -> tuple[str, list[str]]:
        head = self._cleanup_head()
        names: list[bytes] = []
        if head is not None:
            listed = self.git_bytes("ls-tree", "-rz", "--name-only", head)
            self._checked_bytes(listed, "enumerate tracked files for byte snapshot")
            names = [name for name in listed.stdout.split(b"\0") if name]
        digest = hashlib.sha256()
        try:
            for name in names:
                target = self.workdir / os.fsdecode(name)
                try:
                    info = target.lstat()
                except FileNotFoundError:
                    kind, data = b"absent", b""
                else:
                    if stat.S_ISREG(info.st_mode):
                        kind, data = b"file" + bytes([info.st_mode & 0o111]), target.read_bytes()
                    elif stat.S_ISLNK(info.st_mode):
                        kind, data = b"link", os.fsencode(os.readlink(target))
                    else:
                        kind, data = b"other", b""
                digest.update(len(name).to_bytes(8, "big") + name + kind)
                digest.update(len(data).to_bytes(8, "big") + data)
        except OSError as exc:
            raise WorkspaceFault(f"cannot byte-snapshot tracked files: {exc}") from exc
        return digest.hexdigest(), [_wire_path(name) for name in names]

    def _snapshot_changed_paths(self, rec: LeaseRecord) -> list[str]:
        if not rec.index_snapshot or rec.content_tree is None:
            return []
        current = self._fresh_workspace_tree()
        raw_tree, raw_paths = self._raw_tracked_snapshot()
        paths = raw_paths if raw_tree != rec.raw_tree else []
        if current != rec.content_tree:
            diff = self._git_index(
                os.environ.copy(), "diff-tree", "--no-commit-id", "--name-only", "-r", "-z",
                rec.content_tree, current,
            )
            self._checked_bytes(diff, "compare temporary-index trees")
            paths += [_wire_path(path) for path in diff.stdout.split(b"\0") if path]
        return sorted(set(paths))

    def _git_index(
        self, env: dict[str, str], *args: str
    ) -> subprocess.CompletedProcess[bytes]:
        try:
            return subprocess.run(
                ["git", *args], cwd=self.workdir, capture_output=True, env=env
            )
        except OSError as exc:
            raise WorkspaceFault(f"cannot launch temporary-index git: {exc}") from exc

    # -- git plumbing --------------------------------------------------------

    def git(self, *args: str) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                ["git", *args], cwd=self.workdir, capture_output=True, text=True,
                encoding="utf-8", errors="strict",
            )
        except (OSError, UnicodeError) as exc:
            raise WorkspaceFault(f"cannot launch/decode git {' '.join(args)}: {exc}") from exc

    def git_bytes(
        self, *args: str, input_data: bytes | None = None
    ) -> subprocess.CompletedProcess[bytes]:
        try:
            return subprocess.run(
                ["git", *args], cwd=self.workdir, capture_output=True, input=input_data,
            )
        except OSError as exc:
            raise WorkspaceFault(f"cannot launch git {' '.join(args)}: {exc}") from exc

    @staticmethod
    def _output(raw: bytes) -> str:
        return raw.decode("utf-8", errors="backslashreplace").strip()

    def _checked_bytes(
        self, proc: subprocess.CompletedProcess[bytes], what: str,
        diff_path: Path | None = None,
    ) -> None:
        if proc.returncode != 0:
            raise WorkspaceFault(f"{what} failed: {self._output(proc.stderr)}", diff_path)

    def head_commit(self) -> str | None:
        return self._cleanup_head()

    def _cleanup_head(self) -> str | None:
        """Checked HEAD lookup: distinguish a valid unborn branch from a Git failure."""
        proc = self.git("rev-parse", "--verify", "HEAD")
        if proc.returncode == 0:
            return proc.stdout.strip()
        symbolic = self.git("symbolic-ref", "-q", "HEAD")
        if symbolic.returncode == 0 and symbolic.stdout.strip():
            return None
        raise WorkspaceFault(
            f"cleanup HEAD lookup failed: {proc.stderr.strip() or symbolic.stderr.strip()}"
        )

    def _object_format(self) -> str:
        proc = self.git("rev-parse", "--show-object-format")
        self._checked(proc, "read repository object format")
        value = proc.stdout.strip()
        if value not in _EMPTY_TREES:
            raise WorkspaceFault(f"unsupported repository object format: {value!r}")
        return value

    def _empty_tree(self) -> str:
        return _EMPTY_TREES[self._object_format()]

    def _null_oid(self) -> str:
        return "0" * (40 if self._object_format() == "sha1" else 64)

    @staticmethod
    def _is_preexisting_path(path: str, preexisting: frozenset[str]) -> bool:
        decoded = _fs_path(path)
        return any(
            decoded == _fs_path(root).rstrip("/")
            or decoded.startswith(_fs_path(root).rstrip("/") + "/")
            for root in preexisting
        )

    def _preexisting_baseline_unchanged(self, lease: Lease) -> bool:
        if not lease.preexisting:
            return True
        loaded = self._load_lease_baseline(lease.baseline_manifest, lease.baseline_digest)
        if loaded is None:
            raise WorkspaceFault("preexisting paths have no lease baseline")
        _capture_dir, manifest, _blobs = loaded
        expected = {entry.path: entry for entry in manifest.entries}
        for root in lease.preexisting:
            current = self._current_tree(root)
            baseline_paths = {
                path for path in expected
                if self._is_preexisting_path(path, frozenset({root}))
            }
            if set(current) != baseline_paths:
                return False
            for path, kind in current.items():
                entry = expected[path]
                if kind != entry.kind:
                    return False
                if kind == "file":
                    data = self._work_path(path).read_bytes()
                    if entry.sha256 is None or not hmac.compare_digest(
                        hashlib.sha256(data).hexdigest(), entry.sha256
                    ):
                        return False
                elif kind == "symlink" and _wire_path(
                    os.fsencode(os.readlink(self._work_path(path)))
                ) != entry.link_target:
                    return False
        return True

    def _current_tree(self, rel: str) -> dict[str, str]:
        found: dict[str, str] = {}

        def visit(path_rel: str) -> None:
            target = self._contained_record_target(path_rel.rstrip("/"), "baseline comparison")
            try:
                info = target.lstat()
            except FileNotFoundError:
                return
            except OSError as exc:
                raise WorkspaceFault(f"cannot inspect preexisting path {path_rel}: {exc}") from exc
            if stat.S_ISLNK(info.st_mode):
                found[path_rel] = "symlink"
            elif stat.S_ISREG(info.st_mode):
                found[path_rel] = "file"
            elif stat.S_ISDIR(info.st_mode):
                found[path_rel] = "directory"
                try:
                    children = sorted(os.scandir(target), key=lambda entry: entry.name)
                except OSError as exc:
                    raise WorkspaceFault(
                        f"cannot enumerate preexisting path {path_rel}: {exc}"
                    ) from exc
                for child in children:
                    child_fs = Path(child.path).relative_to(self.workdir).as_posix()
                    visit(_wire_path(os.fsencode(child_fs)))
            else:
                found[path_rel] = "other"

        visit(rel.rstrip("/"))
        return found

    def _commit_all(self, message: str, lease: Lease) -> Integration:
        """Stage attempt effects, never unchanged preexisting untracked user paths."""
        if lease.preexisting:
            unstage = self.git(
                "--literal-pathspecs", "reset", "-q", "--",
                *[_fs_path(p) for p in sorted(lease.preexisting)],
            )
            if unstage.returncode != 0:
                return Integration("failed", stderr=unstage.stderr)
        add = self.git("add", "-u", "--", ".")
        if add.returncode != 0:
            return Integration("failed", stderr=add.stderr)
        new_paths = sorted(
            set(self._untracked_ignored_paths()) - set(lease.preexisting)
        )
        if new_paths:
            add_new = self.git(
                "--literal-pathspecs", "add", "-f", "-A", "--",
                *[_fs_path(p) for p in new_paths],
            )
            if add_new.returncode != 0:
                return Integration("failed", stderr=add_new.stderr)
        staged = self.git("diff", "--cached", "--quiet")
        if staged.returncode == 0:
            return Integration("noop")
        if staged.returncode != 1:
            return Integration("failed", stderr=staged.stderr)
        commit = self.git("commit", "-q", "-m", message)
        if commit.returncode != 0:
            return Integration("failed", stderr=commit.stderr)
        sha = self._cleanup_head()
        if sha is None:
            return Integration("failed", stderr="commit succeeded but HEAD is unborn")
        return Integration("committed", commit=sha, paths=self._paths_in_commit(sha))

    def _paths_in_commit(self, sha: str) -> list[str]:
        proc = self.git_bytes(
            "diff-tree", "--no-commit-id", "--name-only", "-r", "--root", "-z", sha
        )
        self._checked_bytes(proc, "receipt diff-tree")
        return [_wire_path(path) for path in proc.stdout.split(b"\0") if path]

    def _commit_receipts(self, pre: str | None, post: str | None) -> list[CommitReceipt]:
        """Every checked commit in `pre..post`, oldest first, with byte-safe paths."""
        if post is None or pre == post:
            return []
        args = ("rev-list", "--reverse", post) if pre is None else (
            "rev-list", "--reverse", f"{pre}..{post}"
        )
        rev = self.git_bytes(*args)
        self._checked_bytes(rev, "receipt rev-list")
        try:
            shas = [s.decode("ascii") for s in rev.stdout.splitlines() if s.strip()]
        except UnicodeDecodeError as exc:
            raise WorkspaceFault(f"receipt rev-list decode failed: {exc}") from exc
        return [CommitReceipt(sha=s, paths=self._paths_in_commit(s)) for s in shas]

    # -- containment (one path-safety home; item 5 / defect 5) ---------------

    def _gitdir(self) -> Path | None:
        absgit = self.git("rev-parse", "--absolute-git-dir")
        if absgit.returncode == 0 and absgit.stdout.strip():
            return Path(absgit.stdout.strip()).resolve()
        return None

    def _in_gitdir(self, target: Path) -> bool:
        gitdir = self._gitdir()
        return gitdir is not None and (target == gitdir or target.is_relative_to(gitdir))

    def resolve_safe_path(self, rel: str) -> Path:
        """Resolve an `inplace` edit path under the workdir, raising on an escape.

        Lexical escapes (`..`, absolute, literal `.git`) are rejected at admission; what
        remains is a symlink that resolves outside the workdir or into the (possibly
        linked-worktree) gitdir."""
        target = (self.workdir / rel).resolve()
        if not target.is_relative_to(self.workdir.resolve()):
            raise ValueError(f"inplace edit escapes workdir: {rel}")
        if self._in_gitdir(target):
            raise ValueError(f"inplace edit targets a git admin path: {rel}")
        return target

    def read_contained_file(self, rel: str) -> str | None:
        """Read a `ctx` file resolved under the workdir; None if it escapes, aliases the
        gitdir, or is absent.

        Admission cannot resolve symlinks, so an in-worktree symlink pointing outside
        the workdir OR into the git admin dir (a `.git` alias — defect 5) is caught HERE,
        never a host-file / git-config exfiltration into a rig prompt."""
        target = (self.workdir / rel).resolve()
        if not target.is_relative_to(self.workdir.resolve()) or self._in_gitdir(target):
            return None
        try:
            return target.read_text(encoding="utf-8")
        except OSError:
            return None

    def _process_path(self, epoch: int, node_id: str, attempt: int) -> Path:
        return self._record_path("processes", epoch, node_id, attempt)

    @staticmethod
    def _proc_identity(pid: int) -> tuple[int, str, int] | None:
        try:
            raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise WorkspaceFault(f"cannot inspect process {pid}: {exc}") from exc
        _head, marker, tail = raw.rpartition(")")
        fields = tail.split()
        if not marker or len(fields) < 20:
            raise WorkspaceFault(f"malformed process identity for pid {pid}")
        try:
            return int(fields[19]), fields[0], int(fields[2])
        except ValueError as exc:
            raise WorkspaceFault(f"malformed process identity for pid {pid}") from exc

    def _load_process(self, epoch: int, node_id: str, attempt: int) -> ProcessRecord | None:
        path = self._process_path(epoch, node_id, attempt)
        if not os.path.lexists(path):
            return None
        try:
            if not stat.S_ISREG(path.lstat().st_mode) or path.is_symlink():
                raise WorkspaceFault(f"process record is not regular: {path}")
            record = ProcessRecord.model_validate_json(path.read_bytes())
            self._verify_record_integrity(record, path)
            if (record.epoch, record.node_id, record.attempt) != (epoch, node_id, attempt):
                raise WorkspaceFault(f"process record identity mismatch: {path}")
            return record
        except WorkspaceFault:
            raise
        except (OSError, ValidationError) as exc:
            raise WorkspaceFault(f"process record is corrupt: {path}: {exc}") from exc

    def _group_processes(self, record: ProcessRecord) -> list[int]:
        members: list[int] = []
        try:
            entries = list(Path("/proc").iterdir())
        except OSError as exc:
            raise WorkspaceFault(f"cannot enumerate process group {record.pgid}: {exc}") from exc
        for entry in entries:
            if not entry.name.isdigit():
                continue
            identity = self._proc_identity(int(entry.name))
            if identity is not None and identity[1] != "Z" and (
                identity[2] == record.pgid and identity[0] >= record.start_time
            ):
                members.append(int(entry.name))
        return members

    def _settle_process(self, record: ProcessRecord) -> None:
        path = self._process_path(record.epoch, record.node_id, record.attempt)
        try:
            path.unlink(missing_ok=True)
            self._fsync_dir(path.parent)
        except OSError as exc:
            raise WorkspaceFault(f"cannot settle process record: {exc}") from exc

    def reap_process(self, epoch: int, node_id: str, attempt: int) -> None:
        """Kill only recorded-group members launched no earlier than its guardian."""
        record = self._load_process(epoch, node_id, attempt)
        if record is None:
            return
        leader = self._proc_identity(record.pid)
        if leader is not None:
            if leader[0] != record.start_time:
                # A live different generation proves the old numeric PGID was released;
                # never apply the launch-time lower bound to this recycled group.
                self._settle_process(record)
                return
            if leader[2] != record.pgid:
                raise WorkspaceFault("recorded process leader changed process group")
            try:
                if os.getpgid(record.pid) != record.pgid:
                    raise WorkspaceFault("recorded process leader changed process group")
            except ProcessLookupError:
                pass  # leader exited between /proc and getpgid; enumerate/recheck below
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            members = self._group_processes(record)
            if not members:
                self._settle_process(record)
                return
            for pid in members:
                try:
                    pidfd = os.pidfd_open(pid)
                except ProcessLookupError:
                    continue
                try:
                    current = self._proc_identity(pid)
                    if current is not None and current[1] != "Z" and (
                        current[2] == record.pgid and current[0] >= record.start_time
                    ):
                        signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                finally:
                    os.close(pidfd)
            time.sleep(0.01)
        raise WorkspaceFault("recorded process group did not exit after SIGKILL")

    @staticmethod
    def _write_all(fd: int, data: bytes) -> None:
        while data:
            written = os.write(fd, data)
            if written <= 0:
                raise OSError("short process-result write")
            data = data[written:]

    @staticmethod
    def _supervisor_child(
        gate_r: int, ready_w: int, result_w: int, operation: Callable[[], bytes]
    ) -> None:
        try:
            os.setsid()
        except OSError:
            os._exit(125)
        try:
            WorkspaceEffects._write_all(ready_w, b"1")
            os.close(ready_w)
            gate = os.read(gate_r, 1)
            os.close(gate_r)
            if not gate:
                os._exit(125)
            try:
                payload, ok = operation(), b"1"
            except BaseException as exc:
                payload, ok = f"{type(exc).__name__}: {exc}".encode(
                    "utf-8", errors="backslashreplace"
                ), b"0"
            frame = ok + len(payload).to_bytes(8, "big") + payload
            WorkspaceEffects._write_all(result_w, frame)
            os.close(result_w)
        except BaseException:
            try:
                os.killpg(os.getpgrp(), signal.SIGKILL)
            finally:
                os._exit(126)
        while True:  # successful delivery keeps the group guardian alive until reap
            signal.pause()

    @staticmethod
    def _read_exact(fd: int, size: int, deadline: float) -> bytes:
        data = bytearray()
        while len(data) < size:
            remaining = deadline - time.monotonic()
            ready, _writable, _exceptional = select.select([fd], [], [], max(0.0, remaining))
            if not ready:
                raise WorkspaceFault("supervised process timed out")
            chunk = os.read(fd, size - len(data))
            if not chunk:
                raise WorkspaceFault("supervised process returned a truncated result")
            data.extend(chunk)
        return bytes(data)

    def run_process(
        self, lease: Lease, operation: Callable[[], bytes], timeout_s: float
    ) -> bytes:
        """Gate one process group behind a durable identity, then reap before return."""
        gate_r, gate_w = os.pipe()
        ready_r, ready_w = os.pipe()
        result_r, result_w = os.pipe()
        fds = (gate_r, gate_w, ready_r, ready_w, result_r, result_w)
        try:
            pid = os.fork()
        except OSError as exc:
            for fd in fds:
                os.close(fd)
            raise WorkspaceFault(f"cannot launch process supervisor: {exc}") from exc
        if pid == 0:
            os.close(gate_w); os.close(ready_r); os.close(result_r)
            self._supervisor_child(gate_r, ready_w, result_w, operation)
            os._exit(127)
        os.close(gate_r); os.close(ready_w); os.close(result_w)
        epoch, node_id = lease.node_key
        path = self._process_path(epoch, node_id, lease.attempt)
        try:
            deadline = time.monotonic() + timeout_s
            self._read_exact(ready_r, 1, min(deadline, time.monotonic() + 5))
            identity = self._proc_identity(pid)
            if identity is None or identity[2] != pid:
                raise WorkspaceFault("process supervisor exited before identity publication")
            if os.path.lexists(path):
                raise WorkspaceFault(f"active process record already exists: {path}")
            record = ProcessRecord(
                epoch=epoch, node_id=node_id, attempt=lease.attempt, pid=pid,
                pgid=identity[2], start_time=identity[0],
            )
            record.integrity = self._record_integrity(record)
            self._fsync_json(path, record)
            self._write_all(gate_w, b"1")
            header = self._read_exact(result_r, 9, deadline)
            length = int.from_bytes(header[1:], "big")
            if length > 64 * 1024 * 1024:
                raise WorkspaceFault("supervised process result exceeds 64 MiB")
            payload = self._read_exact(result_r, length, deadline)
            ok = header[:1] == b"1"
        finally:
            for fd in (gate_w, ready_r, result_r):
                try:
                    os.close(fd)
                except OSError:
                    pass
            if os.path.lexists(path):
                self.reap_process(epoch, node_id, lease.attempt)
            else:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
        if not ok:
            raise WorkspaceFault(
                "supervised operation failed: " + payload.decode("utf-8", errors="replace")
            )
        return payload

    def run_predicate(self, cmd: str, lease: Lease, timeout_s: float) -> bool:
        def execute() -> bytes:
            proc = subprocess.run(cmd, shell=True, cwd=self.workdir, stdin=subprocess.DEVNULL)
            return str(proc.returncode).encode("ascii")

        raw = self.run_process(lease, execute, timeout_s)
        try:
            return int(raw) == 0
        except ValueError as exc:
            raise WorkspaceFault("predicate process returned no exit status") from exc


class CompletionRecorder:
    """The ONE completion-event ordering: result THEN integrated, for every path.

    A do/inplace success, an effectless/no-op result, a failure, and a reconciliation
    all flow through here, so ordering + receipt attribution live in one home instead of
    the three inconsistent orderings the engine grew. Result-before-integrated is the
    torn-tail contract: an effectful result without its integrated reads as NOT durable
    (it re-runs / reconciles), never a lost or duplicated effect.
    """

    def __init__(self, journal: Journal, run_id: str) -> None:
        self.journal = journal
        self.run_id = run_id

    def record_result(
        self, key: NodeKey, result: Result, post_head: str | None = None,
        workspace_unclean: bool = False,
        recovery_action: Literal["fail", "retry"] | None = None,
        receipt_required: bool = False,
    ) -> None:
        """A terminal result with no integration (failure, effectless, or no-op).

        `workspace_unclean` marks a failed result whose cleanup git op failed (PRINCIPLE A):
        it is journalled honestly and the engine then HALTS the epoch (WorkspaceFault)."""
        self.journal.append(self._result_event(
            key, result, post_head=post_head, workspace_unclean=workspace_unclean,
            recovery_action=recovery_action, receipt_required=receipt_required))

    def record_success(
        self, key: NodeKey, result: Result, receipt: IntegrationReceipt,
        post_head: str | None = None,
    ) -> None:
        """A successful result and, if it had a committed effect, its integrated receipt
        (one event carrying every attributed commit) — result first, integrated second.
        `post_head` is the workdir HEAD stamped on the result (the completion certificate
        the torn-window receipt reconstruction is bounded by)."""
        self.journal.append(self._result_event(
            key, result, post_head=post_head, receipt_required=bool(receipt.commits)
        ))
        if receipt.commits:
            epoch, node_id = key
            self.journal.append(Integrated(
                run_id=self.run_id, epoch=epoch, node_id=node_id, commits=receipt.commits,
            ))

    def record_integrated(self, key: NodeKey, receipt: IntegrationReceipt) -> None:
        """Journal a recovered integration for a node whose result is ALREADY durable
        (the RECEIPT-TEAR torn window: result written, its integrated lost). Completes
        the receipt without touching the existing result."""
        if receipt.commits:
            epoch, node_id = key
            self.journal.append(Integrated(
                run_id=self.run_id, epoch=epoch, node_id=node_id, commits=receipt.commits,
            ))

    def record_loop_result(self, key: NodeKey, result: Result, loop_status: str) -> None:
        """A loop's final result carries the body artifact plus its convergence/cap
        disposition in the SEPARATE journal-only `loop_status` (no integrated: the body
        iterations already journalled their own)."""
        self.journal.append(self._result_event(key, result, loop_status=loop_status))

    def _result_event(
        self, key: NodeKey, result: Result, loop_status: str | None = None,
        post_head: str | None = None, workspace_unclean: bool = False,
        recovery_action: Literal["fail", "retry"] | None = None,
        receipt_required: bool = False,
    ) -> ResultEvent:
        epoch, node_id = key
        return ResultEvent(
            run_id=self.run_id, epoch=epoch, node_id=node_id,
            text=result.text, files=result.files, exit_code=result.exit_code,
            outcome=result.outcome, loop_status=loop_status, post_head=post_head,
            workspace_unclean=workspace_unclean, recovery_action=recovery_action,
            receipt_required=receipt_required,
        )
