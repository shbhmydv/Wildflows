"""The workspace effect transaction (RAZE item 4) + the completion recorder.

The engine says "run this node in this lease, then finalize success/failure"; it issues
ZERO git commands. `WorkspaceEffects` owns a per-node-attempt lease (pre/post HEAD
capture over the shared workdir — the seam, not yet a worktree), rig-authored commit
discovery, staging/commit, failure evidence capture + revert, and path containment. It
hands back one accumulated `IntegrationReceipt` (every attributed commit, per-commit
paths). `CompletionRecorder` owns the ONE event ordering — result THEN integrated — for
every completion path (do / inplace / recovery), replacing three inconsistent orderings.

Two consolidation principles (hand-10) live here:

- PRINCIPLE A — QUARANTINE, NEVER DESTROY. No cleanup path deletes content irrecoverably.
  A dead dispatched-only attempt's tip (dead-attempt AND post-crash operator commits) is
  moved to a quarantine ref before the branch is reset to the durable `pre_head`;
  uncommitted dirt + non-preexisting untracked leaks are captured to the run_dir; the
  lease's per-file preexisting snapshot is respected (preexisting files are left in place,
  never swept). EVERY git op in any cleanup/rollback path is CHECKED — a failure raises a
  typed `WorkspaceFault` (never a durable "failed" that lies the workspace was handled).

- PRINCIPLE B — DURABLE TRANSACTION INTENTS. The lease record (pre_head + per-file
  preexisting snapshot) is fsynced to `run_dir/leases/` at lease open, BEFORE the first
  mutation, so restart cleanup is idempotent off disk, never process memory. `inplace` is
  a durable intent transaction: the per-path original content is fsynced to
  `run_dir/intents/` before the first write, so a crash mid-edit is reversed on restart.

Per-node worktree leases are a later step; the shared-workdir policy (quarantine + reset
on failure) lives here and is superseded by discard-the-worktree once worktrees land.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import os
import re
import shutil
import stat
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, ValidationError

from wildflows.events import Integrated, ResultEvent
from wildflows.journal import Journal
from wildflows.projection import NodeKey
from wildflows.result import CommitReceipt, IntegrationReceipt, Result

# Git's canonical empty-tree object — the "base" for a failure diff when the lease opened
# on an unborn repo, so a rig's first-commit leak still diffs verbatim.
_EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


class WorkspaceFault(Exception):
    """A cleanup/rollback git op failed, so the workspace was NOT provably handled.

    Raised from any CHECKED cleanup path (hand-10, PRINCIPLE A). The engine records the
    failed result marked `workspace_unclean=True` and re-raises this to HALT the epoch — a
    durable "failed" that lies a live effect was reverted is worse than a crash. `diff_path`
    is the captured evidence, if any.
    """

    def __init__(self, message: str, diff_path: "Path | None" = None) -> None:
        super().__init__(message)
        self.diff_path = diff_path


class LeaseRecord(BaseModel):
    """The durable lease intent (PRINCIPLE B), fsynced to run_dir BEFORE the first mutation.

    Restart cleanup loads THIS (never process memory) to quarantine + reset a dead attempt
    idempotently: `pre_head` is the reset target and quarantine range anchor; `preexisting`
    is the per-file untracked/ignored snapshot the sweep must leave in place.
    """

    epoch: int
    node_id: str
    attempt: int
    pre_head: str | None
    preexisting: list[str] = Field(default_factory=list)
    # None marks a legacy record that did not snapshot directories; recovery then refuses
    # to prune empty parents because it cannot distinguish user-created empty directories.
    preexisting_dirs: list[str] | None = None
    ts: float


class IntentWrite(BaseModel):
    """One canonical inplace target's exact pre-state and expected written state."""

    path: str
    pre_kind: Literal["file", "dir", "absent", "other"]
    # `original` reads hand-10 text intents; modern intents use base64 for exact binary
    # reversal. `content` is what this attempt intended to write, allowing restart to
    # distinguish the engine's bytes from a post-crash operator edit.
    original: str | None = None
    original_b64: str | None = None
    content: str | None = None


class InplaceIntent(BaseModel):
    """The durable inplace transaction intent (PRINCIPLE B): every target's original state,
    fsynced BEFORE the first write so a crash mid-edit is reversed idempotently on restart."""

    epoch: int
    node_id: str
    attempt: int
    writes: list[IntentWrite]
    ts: float


class CaptureEntry(BaseModel):
    """One exactly recoverable filesystem object in an immutable capture."""

    path: str
    kind: Literal["file", "directory", "symlink", "absent"]
    size: int | None = None
    sha256: str | None = None
    blob: str | None = None
    link_target: str | None = None


class CaptureManifest(BaseModel):
    """Durable index for raw blobs copied before a destructive reset/sweep."""

    entries: list[CaptureEntry]


@dataclass
class Lease:
    """A node attempt's workspace lease: the shared workdir + its pre-run HEAD.

    `pre_head` anchors rig-authored-commit discovery (`pre..post`), the reset target on
    resume/failure, and the receipt reconstruction range. `preexisting` is the set of
    untracked+ignored paths present at lease open — cleanup removes ONLY paths NOT in this
    set, never destroying pre-existing user files, the run_dir, or anything the lease did
    not create. `attempt` keys the durable lease/intent records and the quarantine ref so
    repeated dead attempts never clobber one another's forensics.
    """

    node_key: NodeKey
    pre_head: str | None
    attempt: int = 0
    preexisting: frozenset[str] = frozenset()
    preexisting_dirs: frozenset[str] = frozenset()


@dataclass
class Integration:
    """The outcome of a core-mediated declared-path (`inplace`) commit."""

    status: Literal["committed", "noop", "failed"]
    commit: str | None = None
    paths: list[str] = field(default_factory=list)
    stderr: str = ""


@dataclass
class DoIntegration:
    """The outcome of finalizing a successful `do`: an accumulated receipt or a git error."""

    ok: bool
    receipt: IntegrationReceipt = field(default_factory=IntegrationReceipt)
    stderr: str = ""


class WorkspaceEffects:
    """Containment + git mediation + the effect transaction over one shared workdir."""

    def __init__(self, workdir: Path, run_dir: Path) -> None:
        self.workdir = Path(workdir)
        self.run_dir = Path(run_dir)

    # -- lease ---------------------------------------------------------------

    def open_lease(self, node_key: NodeKey, attempt: int) -> Lease | None:
        """Begin a node attempt, or REFUSE (None) on a dirty tracked/index worktree.

        The clean-worktree precondition (hand-9, FAILURE-LEASE): a lease opens only when
        the tracked + index state is clean (untracked files are allowed and snapshotted
        per-file). This is the honest serial-M1 rule. The durable lease record is fsynced
        to run_dir BEFORE any mutation (hand-10, PRINCIPLE B) so restart cleanup loads
        `pre_head` + the per-file `preexisting` snapshot off disk, never process memory.
        (M3 per-node worktree isolation makes the workdir engine-owned and retires this.)
        """
        if self.tracked_dirty():
            return None
        lease = Lease(
            node_key=node_key,
            pre_head=self.head_commit(),
            attempt=attempt,
            preexisting=frozenset(self._untracked_ignored_paths()),
            preexisting_dirs=frozenset(self._workspace_dirs()),
        )
        self._write_lease_record(lease)
        return lease

    def tracked_dirty(self) -> bool:
        """True if the worktree has any uncommitted TRACKED or staged change (untracked
        files excluded). The lease-open precondition."""
        proc = self.git("status", "--porcelain", "--untracked-files=no")
        self._checked(proc, "inspect tracked workspace state")
        if proc.stderr.strip():
            raise WorkspaceFault(f"tracked workspace inspection was incomplete: {proc.stderr.strip()}")
        return bool(proc.stdout.strip())

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

    def _write_lease_record(self, lease: Lease) -> None:
        epoch, node_id = lease.node_key
        self._fsync_json(
            self._record_path("leases", epoch, node_id, lease.attempt),
            LeaseRecord(
                epoch=epoch, node_id=node_id, attempt=lease.attempt,
                pre_head=lease.pre_head, preexisting=sorted(lease.preexisting),
                preexisting_dirs=sorted(lease.preexisting_dirs), ts=time.time(),
            ),
        )

    def load_lease_record(self, epoch: int, node_id: str, attempt: int) -> LeaseRecord | None:
        path = self._record_path("leases", epoch, node_id, attempt)
        if not os.path.lexists(path):
            return None
        try:
            if not stat.S_ISREG(path.lstat().st_mode) or path.is_symlink():
                raise WorkspaceFault(f"lease record is not a regular file: {path}")
            return LeaseRecord.model_validate_json(path.read_bytes())
        except WorkspaceFault:
            raise
        except (OSError, ValidationError) as exc:
            raise WorkspaceFault(f"lease record is unreadable or corrupt: {path}: {exc}") from exc

    def write_intent(self, intent: InplaceIntent) -> None:
        self._fsync_json(
            self._record_path("intents", intent.epoch, intent.node_id, intent.attempt), intent
        )

    def load_intent(self, epoch: int, node_id: str, attempt: int) -> InplaceIntent | None:
        path = self._record_path("intents", epoch, node_id, attempt)
        if not os.path.lexists(path):
            return None
        try:
            if not stat.S_ISREG(path.lstat().st_mode) or path.is_symlink():
                raise WorkspaceFault(f"intent record is not a regular file: {path}")
            return InplaceIntent.model_validate_json(path.read_bytes())
        except WorkspaceFault:
            raise
        except (OSError, ValidationError) as exc:
            raise WorkspaceFault(f"intent record is unreadable or corrupt: {path}: {exc}") from exc

    def settle_records(self, epoch: int, node_id: str, attempt: int) -> None:
        """Remove a settled attempt's durable lease + intent records — only AFTER its
        terminal result (and integrated) are journalled, so a crash before settlement
        redoes an idempotent cleanup rather than losing recovery state."""
        for kind in ("leases", "intents"):
            path = self._record_path(kind, epoch, node_id, attempt)
            try:
                if os.path.lexists(path):
                    path.unlink()
                    self._fsync_dir(path.parent)
            except OSError as exc:
                raise WorkspaceFault(f"record settlement failed for {path}: {exc}") from exc

    # -- dead-attempt recovery: QUARANTINE, NEVER DESTROY (PRINCIPLE A) -------

    @staticmethod
    def _ref_component(raw: str) -> str:
        """Encode arbitrary text as one valid, collision-resistant Git ref component."""
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
        # The observed tip SHA makes recovery append-only: a redo with an operator commit
        # allocates another immutable ref instead of moving the dead attempt's first ref.
        run = self._ref_component(self.run_dir.name)
        node = self._ref_component(rec.node_id)
        return (
            f"refs/wildflows/quarantine/{run}/"
            f"e{rec.epoch}-{node}-a{rec.attempt}-{head}"
        )

    def _preserve_tip(self, rec: LeaseRecord, head: str, diff_path: Path | None) -> None:
        ref = self._quarantine_ref(rec, head)
        current = self.git("rev-parse", "--verify", "--quiet", ref)
        if current.returncode == 0:
            if current.stdout.strip() != head:
                raise WorkspaceFault(
                    f"quarantine ref collision at {ref}: expected {head}, "
                    f"found {current.stdout.strip()}", diff_path,
                )
            return
        if current.returncode != 1:
            raise WorkspaceFault(
                f"quarantine ref lookup failed: {current.stderr.strip()}", diff_path)
        zero = "0" * 40
        self._checked(
            self.git("update-ref", ref, head, zero), "quarantine append-only update-ref", diff_path
        )

    def quarantine_dead_attempt(self, rec: LeaseRecord) -> Path | None:
        """Recover a dead dispatched-only attempt without destroying anything (PRINCIPLE A).

        The current tip (dead-attempt commits AND any post-crash operator commit) is moved
        to a quarantine ref so it stays reachable; uncommitted dirt + non-preexisting
        untracked leaks are captured to run_dir; the branch is reset to the durable
        `pre_head`; then only NON-preexisting untracked leaks are swept (the lease's
        preexisting snapshot is left in place). EVERY git op is CHECKED — a failure raises
        `WorkspaceFault`. Idempotent given the record: quarantine update-ref, reset to
        pre_head, and capture-append all redo safely after a crash mid-cleanup."""
        pre = rec.pre_head
        head = self._cleanup_head()
        diff_path = self._capture_dead_attempt(rec)
        if head is not None and head != pre:
            self._preserve_tip(rec, head, diff_path)
        if pre is not None:
            self._checked(self.git("reset", "--hard", pre), "quarantine reset --hard", diff_path)
        else:
            # Unborn at lease open: drop the branch ref so the dead attempt's first commit
            # cannot survive as durable history (it lives on in the quarantine ref).
            symbolic = self.git("symbolic-ref", "-q", "HEAD")
            if symbolic.returncode not in (0, 1):
                self._checked(symbolic, "quarantine unborn symbolic-ref", diff_path)
            ref = symbolic.stdout.strip()
            if head is not None and ref:
                self._checked(self.git("update-ref", "-d", ref),
                              "quarantine unborn update-ref -d", diff_path)
            self._checked(self.git("reset"), "quarantine unborn reset", diff_path)
        self._remove_leaks(
            sorted(set(self._untracked_ignored_paths()) - set(rec.preexisting)),
            None if rec.preexisting_dirs is None else set(rec.preexisting_dirs),
        )
        return diff_path

    def quarantine_from_journal(
        self, epoch: int, node_id: str, attempt: int, pre_head: str | None
    ) -> Path | None:
        """Quarantine a pre-hand-10 dead attempt that has no durable lease record: build a
        conservative record from the journalled `pre_head` and treat ALL current untracked
        as preexisting, so committed work is quarantined + reset to pre_head but NOTHING is
        swept (never-destroy in the absence of a snapshot)."""
        rec = LeaseRecord(
            epoch=epoch, node_id=node_id, attempt=attempt, pre_head=pre_head,
            preexisting=self._untracked_ignored_paths(), preexisting_dirs=None, ts=0.0,
        )
        return self.quarantine_dead_attempt(rec)

    def _capture_dead_attempt(self, rec: LeaseRecord) -> Path | None:
        """Byte-exactly capture dirt before quarantine reset/sweep."""
        head = self._cleanup_head()
        base = "HEAD" if head is not None else _EMPTY_TREE
        leaks = sorted(set(self._untracked_ignored_paths()) - set(rec.preexisting))
        return self._capture_workspace(
            self.run_dir / "quarantine",
            f"{rec.epoch}-{rec.node_id}-{rec.attempt}",
            base,
            leaks,
        )

    def _capture_workspace(
        self, index_dir: Path, stem: str, tracked_base: str, leaks: list[str]
    ) -> Path | None:
        """Durably copy exact current bytes plus a binary Git patch before destruction.

        The human-readable ``.diff`` remains an index, while every current regular file
        (tracked binary dirt, untracked/ignored files, and nested-repository contents) is
        copied into an immutable sibling ``.capture`` directory and named by a manifest.
        Any enumerate/read/write/fsync failure raises ``WorkspaceFault`` before reset/sweep.
        """
        diff = self.git("diff", "--binary", tracked_base)
        self._checked(diff, "capture git diff")
        names = self.git("diff", "--name-only", "-z", tracked_base)
        self._checked(names, "capture git diff --name-only")
        tracked = [name for name in names.stdout.split("\0") if name]
        if not tracked and not leaks:
            return None

        index_path, capture_dir = self._allocate_capture(index_dir, stem)
        entries: list[CaptureEntry] = []
        seen: set[str] = set()
        try:
            for rel in [*tracked, *leaks]:
                self._capture_path(rel, self.workdir / rel, capture_dir, entries, seen)
            manifest = CaptureManifest(entries=entries)
            self._fsync_json(capture_dir / "manifest.json", manifest)
            parts = [diff.stdout] if diff.stdout.strip() else []
            parts.extend(self._capture_evidence(capture_dir, entry) for entry in entries)
            self._atomic_write(
                index_path, "\n".join(p for p in parts if p).encode("utf-8"), "capture index"
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
        rel = Path(rel).as_posix().rstrip("/")
        if rel in seen:
            return
        seen.add(rel)
        try:
            info = target.lstat()
        except FileNotFoundError:
            entries.append(CaptureEntry(path=rel, kind="absent"))
            return
        except OSError as exc:
            raise WorkspaceFault(f"cannot stat capture source {rel}: {exc}") from exc

        if stat.S_ISLNK(info.st_mode):
            try:
                link_target = os.readlink(target)
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
                child_rel = f"{rel}/{child.name}" if rel else child.name
                self._capture_path(
                    child_rel, Path(child.path), capture_dir, entries, seen)
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

    def _capture_evidence(self, capture_dir: Path, entry: CaptureEntry) -> str:
        header = f"=== captured: {entry.path} ({entry.kind}) ==="
        if entry.kind != "file" or entry.blob is None:
            return f"{header}\n{entry.link_target or ''}"
        data = (capture_dir / entry.blob).read_bytes()
        try:
            body = data.decode("utf-8")
        except UnicodeDecodeError:
            body = (
                f"<binary artifact: {entry.size} bytes, sha256={entry.sha256}, "
                f"blob={entry.blob}>"
            )
        return f"{header}\n{body}"

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
        commits = self._commit_receipts(lease.pre_head, post)
        integ = self._commit_all(message)
        if integ.status == "failed":
            return DoIntegration(ok=False, stderr=integ.stderr)
        if integ.status == "committed" and integ.commit is not None:
            commits.append(CommitReceipt(sha=integ.commit, paths=integ.paths))
        return DoIntegration(ok=True, receipt=IntegrationReceipt(commits=commits))

    def finalize_failure(self, lease: Lease, diff_name: str) -> Path | None:
        """A failed rig's effects are REVERTED and captured, scoped to THIS lease.

        Evidence (committed rig work `pre..post`, uncommitted tracked changes, AND the
        untracked/ignored artifacts THIS attempt created) is captured to the run log dir;
        the workdir is then reset to the lease's PRE-run HEAD (undoing commits the failing
        rig made) and this lease's untracked/ignored leaks are removed. Cleanup is
        LEASE-SCOPED (hand-8): only paths absent from `lease.preexisting` are touched, and
        the run_dir subtree is hard-excluded — so pre-existing user files, the journal
        under a run_dir-inside-workdir, and anything the lease did not create all survive.
        Nested Git repositories the rig left are captured (their file listing) and removed
        recursively (a plain `git clean -fd` would refuse them). Returns the evidence path
        or None if nothing changed. Per-node worktree isolation later replaces this with
        discard-the-worktree.
        """
        pre = lease.pre_head
        # Working tree (incl any commit the failing rig made) vs the lease's PRE base —
        # the empty tree when the lease opened on an unborn repo — captures committed AND
        # staged/tracked leaks; this lease's untracked/ignored are captured separately.
        base = pre if pre is not None else _EMPTY_TREE
        leaks = sorted(set(self._untracked_ignored_paths()) - lease.preexisting)
        diff_path = self._capture_workspace(
            self.run_dir / "failed-diffs", Path(diff_name).stem, base, leaks
        )

        # EVERY revert git op is CHECKED (hand-10, PRINCIPLE A): if reset/update-ref fails,
        # the failing rig's live effect is NOT provably reverted, so raise WorkspaceFault
        # (carrying the captured evidence) rather than let the engine journal a durable
        # "failed" that lies the workspace was handled.
        if pre is not None:
            self._checked(self.git("reset", "--hard", pre), "failure reset --hard", diff_path)
        else:
            # Unborn at lease open: if the failing rig created the first commit, drop the
            # branch ref back to unborn so its effect cannot survive as durable history.
            symbolic = self.git("symbolic-ref", "-q", "HEAD")
            if symbolic.returncode not in (0, 1):
                self._checked(symbolic, "failure unborn symbolic-ref", diff_path)
            ref = symbolic.stdout.strip()
            if self._cleanup_head() is not None and ref:
                self._checked(self.git("update-ref", "-d", ref),
                              "failure unborn update-ref -d", diff_path)
            self._checked(self.git("reset"), "failure unborn reset", diff_path)
        # Recompute leaks AFTER the reset: a leak the failing rig (or a failed core commit)
        # had STAGED only surfaces as untracked once the index is reset. The sweep stays
        # lease-scoped — preexisting user files and the run_dir subtree are never touched.
        self._remove_leaks(
            sorted(set(self._untracked_ignored_paths()) - lease.preexisting),
            set(lease.preexisting_dirs),
        )
        return diff_path

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
                rel = path.relative_to(self.workdir).as_posix()
                if name == ".git" and root_path == self.workdir:
                    continue
                if self._overlaps_run_dir(rel) or path.is_symlink():
                    continue
                found.append(rel)
                kept.append(name)
            dirs[:] = kept
        if errors:
            raise WorkspaceFault(f"cannot snapshot workspace directories: {errors[0]}")
        return found

    def _untracked_ignored_paths(self) -> list[str]:
        """The untracked (`??`) and ignored (`!!`) paths git reports PER FILE
        (`--untracked-files=all`), EXCLUDING the run_dir subtree, so an addition under a
        pre-existing untracked directory is its own entry (hand-9, FAILURE-LEASE). A
        nested Git repo still appears as ONE `dir/` entry — git never recurses into it —
        and is walked/removed as a directory by the evidence/sweep helpers."""
        status = self.git("status", "--ignored", "--porcelain", "-z", "--untracked-files=all")
        self._checked(status, "enumerate untracked/ignored paths")
        if status.stderr.strip():
            raise WorkspaceFault(
                f"enumerate untracked/ignored paths was incomplete: {status.stderr.strip()}"
            )
        out: list[str] = []
        for entry in status.stdout.split("\0"):
            if not entry or entry[:2] not in ("??", "!!"):
                continue
            rel = entry[3:]
            if not self._overlaps_run_dir(rel):
                out.append(rel)
        return out

    def _remove_leaks(
        self, leaks: list[str], preexisting_dirs: set[str] | None = None
    ) -> None:
        """Checked lease-scoped sweep with an explicit absence postcondition."""
        removed: list[Path] = []
        for rel in leaks:
            target = self.workdir / rel
            try:
                info = target.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise WorkspaceFault(f"cannot stat leak before removal {rel}: {exc}") from exc
            try:
                if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                    shutil.rmtree(target)
                else:
                    target.unlink()
            except OSError as exc:
                raise WorkspaceFault(f"cannot remove workspace leak {rel}: {exc}") from exc
            if os.path.lexists(target):
                raise WorkspaceFault(f"workspace leak remained after removal: {rel}")
            removed.append(target)
        if preexisting_dirs is None:
            return
        # Git's per-file status omits now-empty parent directories. Prune a parent only
        # when the modern lease proved it did not preexist, stopping at any nonempty/user
        # directory and verifying every successful removal.
        for target in removed:
            parent = target.parent
            while parent != self.workdir:
                rel_parent = parent.relative_to(self.workdir).as_posix()
                if rel_parent in preexisting_dirs or self._overlaps_run_dir(rel_parent):
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
                parent = parent.parent

    def _overlaps_run_dir(self, rel: str) -> bool:
        """True if a workdir-relative path OVERLAPS the run_dir subtree — the run_dir
        itself, anything inside it, OR an ANCESTOR of it. Git reports an untracked
        directory as one top-level entry (e.g. `.wildflows/`), which is an ANCESTOR of a
        `run_dir=<workdir>/.wildflows/run`; sweeping it would delete the live journal.
        None of these may ever be captured or swept."""
        try:
            resolved = (self.workdir / rel).resolve()
            run = self.run_dir.resolve()
        except OSError:
            return True  # fail conservative: never capture/sweep a path we cannot classify
        return resolved == run or resolved.is_relative_to(run) or run.is_relative_to(resolved)

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
        return IntegrationReceipt(commits=self._commit_receipts(pre_head, post_head))

    # -- inplace finalization ------------------------------------------------

    def integrate_declared(self, declared: list[str], message: str) -> Integration:
        """Commit ONLY the declared paths (an `inplace`), via a `--`-scoped pathspec so
        any pre-existing staged index is preserved. Git failures return
        status="failed"; nothing staged for our scope is a "noop". Never raises."""
        add = self.git("add", "--", *declared)
        if add.returncode != 0:
            return Integration("failed", stderr=add.stderr)
        diff = self.git("diff", "--cached", "--quiet", "--", *declared)
        if diff.returncode == 0:
            return Integration("noop")
        commit = self.git("commit", "-q", "-m", message, "--", *declared)
        if commit.returncode != 0:
            return Integration("failed", stderr=commit.stderr)
        sha = self.git("rev-parse", "HEAD").stdout.strip()
        return Integration("committed", commit=sha, paths=list(declared))

    def rollback_inplace(self, intent: InplaceIntent) -> None:
        """Reverse a durable intent without destroying post-crash operator bytes.

        Current state matching either the expected engine write or the recorded pre-state
        is an idempotent rollback case. Anything else is byte-captured first; only after
        that manifest is durable are canonical targets restored and unstaged.
        """
        self._validate_intent_targets(intent)
        unexpected = [
            w.path for w in intent.writes
            if not self._matches_expected(w) and not self._matches_prestate(w)
        ]
        if unexpected:
            self._capture_workspace(
                self.run_dir / "intent-reversal",
                f"{intent.epoch}-{intent.node_id}-{intent.attempt}",
                "HEAD" if self._cleanup_head() is not None else _EMPTY_TREE,
                unexpected,
            )
        for write in intent.writes:
            target = self.workdir / write.path
            if write.pre_kind == "file":
                self._atomic_write(target, self._original_bytes(write), "inplace rollback")
            elif write.pre_kind == "absent":
                self._remove_rollback_target(target, write.path)
            elif write.pre_kind in ("dir", "other"):
                if not self._matches_prestate(write):
                    raise WorkspaceFault(
                        f"legacy inplace intent cannot restore pre-state for {write.path}"
                    )
        if intent.writes:
            self._checked(
                self.git("reset", "-q", "--", *[w.path for w in intent.writes]),
                "inplace rollback unstage",
            )

    def _validate_intent_targets(self, intent: InplaceIntent) -> None:
        root = self.workdir.resolve()
        for write in intent.writes:
            expected = root / write.path
            try:
                actual = (self.workdir / write.path).resolve()
            except OSError as exc:
                raise WorkspaceFault(
                    f"cannot resolve canonical inplace target {write.path}: {exc}"
                ) from exc
            if actual != expected or not actual.is_relative_to(root) or self._in_gitdir(actual):
                raise WorkspaceFault(
                    f"canonical inplace target changed after intent publication: {write.path}"
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
        target = self.workdir / write.path
        try:
            return target.is_file() and not target.is_symlink() and (
                target.read_bytes() == write.content.encode("utf-8")
            )
        except OSError as exc:
            raise WorkspaceFault(f"cannot compare inplace target {write.path}: {exc}") from exc

    def _matches_prestate(self, write: IntentWrite) -> bool:
        target = self.workdir / write.path
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
        except OSError as exc:
            raise WorkspaceFault(f"cannot remove inplace rollback target {rel}: {exc}") from exc
        if os.path.lexists(target):
            raise WorkspaceFault(f"inplace rollback target remained after removal: {rel}")

    # -- git plumbing --------------------------------------------------------

    def git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["git", *args], cwd=self.workdir, capture_output=True, text=True)

    def head_commit(self) -> str | None:
        proc = self.git("rev-parse", "HEAD")
        return proc.stdout.strip() if proc.returncode == 0 else None

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

    def _commit_all(self, message: str) -> Integration:
        """Stage + commit ALL worktree changes (a `do`'s dirty remainder)."""
        add = self.git("add", "-A", "--", ".")
        if add.returncode != 0:
            return Integration("failed", stderr=add.stderr)
        if self.git("diff", "--cached", "--quiet").returncode == 0:
            return Integration("noop")
        commit = self.git("commit", "-q", "-m", message)
        if commit.returncode != 0:
            return Integration("failed", stderr=commit.stderr)
        sha = self.git("rev-parse", "HEAD").stdout.strip()
        return Integration("committed", commit=sha, paths=self._paths_in_commit(sha))

    def _paths_in_commit(self, sha: str) -> list[str]:
        out = self.git(
            "diff-tree", "--no-commit-id", "--name-only", "-r", "--root", "-z", sha
        ).stdout
        return [p for p in out.split("\0") if p]

    def _commit_receipts(self, pre: str | None, post: str | None) -> list[CommitReceipt]:
        """Every commit in `pre..post`, oldest first, each with its own changed paths."""
        if post is None or pre == post:
            return []
        if pre is None:
            rev = self.git("rev-list", "--reverse", post)  # unborn base: all commits
        else:
            rev = self.git("rev-list", "--reverse", f"{pre}..{post}")
        shas = [s for s in rev.stdout.splitlines() if s.strip()]
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

    def run_predicate(self, cmd: str) -> bool:
        """Run a loop `until` predicate in the workdir; exit 0 means converged."""
        return subprocess.run(cmd, shell=True, cwd=self.workdir).returncode == 0


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
    ) -> None:
        """A terminal result with no integration (failure, effectless, or no-op).

        `workspace_unclean` marks a failed result whose cleanup git op failed (PRINCIPLE A):
        it is journalled honestly and the engine then HALTS the epoch (WorkspaceFault)."""
        self.journal.append(self._result_event(
            key, result, post_head=post_head, workspace_unclean=workspace_unclean,
            recovery_action=recovery_action))

    def record_success(
        self, key: NodeKey, result: Result, receipt: IntegrationReceipt,
        post_head: str | None = None,
    ) -> None:
        """A successful result and, if it had a committed effect, its integrated receipt
        (one event carrying every attributed commit) — result first, integrated second.
        `post_head` is the workdir HEAD stamped on the result (the completion certificate
        the torn-window receipt reconstruction is bounded by)."""
        self.journal.append(self._result_event(key, result, post_head=post_head))
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
    ) -> ResultEvent:
        epoch, node_id = key
        return ResultEvent(
            run_id=self.run_id, epoch=epoch, node_id=node_id,
            text=result.text, files=result.files, exit_code=result.exit_code,
            outcome=result.outcome, loop_status=loop_status, post_head=post_head,
            workspace_unclean=workspace_unclean, recovery_action=recovery_action,
        )
