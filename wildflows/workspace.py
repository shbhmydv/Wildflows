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

import hashlib
import json
import os
import shutil
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
    ts: float


class IntentWrite(BaseModel):
    """One inplace target's pre-state, enough to reverse the write on restart/failure."""

    path: str
    pre_kind: Literal["file", "dir", "absent", "other"]
    original: str | None = None  # the file's content when pre_kind == "file"


class InplaceIntent(BaseModel):
    """The durable inplace transaction intent (PRINCIPLE B): every target's original state,
    fsynced BEFORE the first write so a crash mid-edit is reversed idempotently on restart."""

    epoch: int
    node_id: str
    attempt: int
    writes: list[IntentWrite]
    ts: float


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
        )
        self._write_lease_record(lease)
        return lease

    def tracked_dirty(self) -> bool:
        """True if the worktree has any uncommitted TRACKED or staged change (untracked
        files excluded). The lease-open precondition."""
        proc = self.git("status", "--porcelain", "--untracked-files=no")
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

    def _fsync_json(self, path: Path, model: BaseModel) -> None:
        """Crash-atomically publish a record: temp + file fsync + replace + dir fsync."""
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fsync_dir(path.parent.parent)
            fd, raw_tmp = tempfile.mkstemp(prefix=f".{path.name}.tmp-", dir=path.parent)
            tmp = Path(raw_tmp)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(model.model_dump_json())
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
            raise WorkspaceFault(f"atomic record publication failed for {path}: {exc}") from exc

    def _write_lease_record(self, lease: Lease) -> None:
        epoch, node_id = lease.node_key
        self._fsync_json(
            self._record_path("leases", epoch, node_id, lease.attempt),
            LeaseRecord(
                epoch=epoch, node_id=node_id, attempt=lease.attempt,
                pre_head=lease.pre_head, preexisting=sorted(lease.preexisting),
                ts=time.time(),
            ),
        )

    def load_lease_record(self, epoch: int, node_id: str, attempt: int) -> LeaseRecord | None:
        path = self._record_path("leases", epoch, node_id, attempt)
        if not os.path.lexists(path):
            return None
        try:
            return LeaseRecord.model_validate_json(path.read_bytes())
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
            return InplaceIntent.model_validate_json(path.read_bytes())
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

    def _quarantine_ref(self, rec: LeaseRecord) -> str:
        slug = f"{self.run_dir.name}-{rec.epoch}-{rec.node_id}-{rec.attempt}".replace("/", "-")
        return f"refs/wildflows/quarantine/{slug}"

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
        head = self.head_commit()
        diff_path = self._capture_dead_attempt(rec)
        if head is not None and head != pre:
            self._checked(self.git("update-ref", self._quarantine_ref(rec), head),
                          "quarantine update-ref", diff_path)
        if pre is not None:
            self._checked(self.git("reset", "--hard", pre), "quarantine reset --hard", diff_path)
        else:
            # Unborn at lease open: drop the branch ref so the dead attempt's first commit
            # cannot survive as durable history (it lives on in the quarantine ref).
            ref = self.git("symbolic-ref", "-q", "HEAD").stdout.strip()
            if head is not None and ref:
                self._checked(self.git("update-ref", "-d", ref),
                              "quarantine unborn update-ref -d", diff_path)
            self._checked(self.git("reset"), "quarantine unborn reset", diff_path)
        self._remove_leaks(
            sorted(set(self._untracked_ignored_paths()) - set(rec.preexisting))
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
            preexisting=self._untracked_ignored_paths(), ts=0.0,
        )
        return self.quarantine_dead_attempt(rec)

    def _capture_dead_attempt(self, rec: LeaseRecord) -> Path | None:
        """Capture a dead attempt's uncommitted dirt + non-preexisting untracked leaks to
        run_dir/quarantine/ (committed work is already reachable via the quarantine ref)."""
        parts: list[str] = []
        diff = self.git("diff", "HEAD")
        if diff.returncode == 0 and diff.stdout.strip():
            parts.append(diff.stdout)
        parts.extend(self._leak_evidence(
            sorted(set(self._untracked_ignored_paths()) - set(rec.preexisting))
        ))
        if not parts:
            return None
        cap_dir = self.run_dir / "quarantine"
        cap_dir.mkdir(parents=True, exist_ok=True)
        path = cap_dir / f"{rec.epoch}-{rec.node_id}-{rec.attempt}.diff"
        path.write_text("\n".join(parts), encoding="utf-8")
        return path

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
        parts: list[str] = []
        diff = self.git("diff", base)
        if diff.stdout.strip():
            parts.append(diff.stdout)
        parts.extend(self._leak_evidence(sorted(set(self._untracked_ignored_paths()) - lease.preexisting)))

        diff_path: Path | None = None
        if parts:
            leak_dir = self.run_dir / "failed-diffs"
            leak_dir.mkdir(parents=True, exist_ok=True)
            diff_path = leak_dir / diff_name
            diff_path.write_text("\n".join(parts), encoding="utf-8")

        # EVERY revert git op is CHECKED (hand-10, PRINCIPLE A): if reset/update-ref fails,
        # the failing rig's live effect is NOT provably reverted, so raise WorkspaceFault
        # (carrying the captured evidence) rather than let the engine journal a durable
        # "failed" that lies the workspace was handled.
        if pre is not None:
            self._checked(self.git("reset", "--hard", pre), "failure reset --hard", diff_path)
        else:
            # Unborn at lease open: if the failing rig created the first commit, drop the
            # branch ref back to unborn so its effect cannot survive as durable history.
            ref = self.git("symbolic-ref", "-q", "HEAD").stdout.strip()
            if self.head_commit() is not None and ref:
                self._checked(self.git("update-ref", "-d", ref),
                              "failure unborn update-ref -d", diff_path)
            self._checked(self.git("reset"), "failure unborn reset", diff_path)
        # Recompute leaks AFTER the reset: a leak the failing rig (or a failed core commit)
        # had STAGED only surfaces as untracked once the index is reset. The sweep stays
        # lease-scoped — preexisting user files and the run_dir subtree are never touched.
        self._remove_leaks(sorted(set(self._untracked_ignored_paths()) - lease.preexisting))
        return diff_path

    def _untracked_ignored_paths(self) -> list[str]:
        """The untracked (`??`) and ignored (`!!`) paths git reports PER FILE
        (`--untracked-files=all`), EXCLUDING the run_dir subtree, so an addition under a
        pre-existing untracked directory is its own entry (hand-9, FAILURE-LEASE). A
        nested Git repo still appears as ONE `dir/` entry — git never recurses into it —
        and is walked/removed as a directory by the evidence/sweep helpers."""
        status = self.git("status", "--ignored", "--porcelain", "-z", "--untracked-files=all")
        out: list[str] = []
        for entry in status.stdout.split("\0"):
            if not entry or entry[:2] not in ("??", "!!"):
                continue
            rel = entry[3:]
            if not self._overlaps_run_dir(rel):
                out.append(rel)
        return out

    def _leak_evidence(self, leaks: list[str]) -> list[str]:
        """Dump each leaked path's content — git omits untracked/ignored from `diff`. A
        directory (e.g. a nested Git repo) is WALKED so its file listing + contents are
        recorded verbatim, never a bare `<unreadable>` (hand-8, defect 3)."""
        out: list[str] = []
        for rel in leaks:
            target = self.workdir / rel
            if target.is_dir() and not target.is_symlink():
                for sub in sorted(p for p in target.rglob("*") if p.is_file()):
                    srel = sub.relative_to(self.workdir).as_posix()
                    out.append(f"=== untracked/ignored: {srel} ===\n{self._read_text(sub)}")
            else:
                out.append(f"=== untracked/ignored: {rel} ===\n{self._read_text(target)}")
        return out

    @staticmethod
    def _read_text(path: Path) -> str:
        """Capture a leaked file's content, tolerating BINARY (hand-9, FAILURE-LEASE): a
        decode failure records size + sha256 instead of raising, so no exception escapes
        after `dispatched`. An unreadable path (permission/gone) is noted, not fatal."""
        try:
            return path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            try:
                data = path.read_bytes()
            except OSError:
                return "<binary or unreadable>"
            digest = hashlib.sha256(data).hexdigest()
            return f"<binary artifact: {len(data)} bytes, sha256={digest}>"
        except OSError:
            return "<binary or unreadable>"

    def _remove_leaks(self, leaks: list[str]) -> None:
        """Remove this lease's untracked/ignored leaks — files via unlink, directories
        (incl nested Git repos, which `git clean -fd` refuses without `-ff`) recursively."""
        for rel in leaks:
            target = self.workdir / rel
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target, ignore_errors=True)
            else:
                target.unlink(missing_ok=True)

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
            return False
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

    def rollback_inplace(self, writes: list[IntentWrite]) -> None:
        """Reverse an inplace from its DURABLE intent — on a failure after the first write
        (OSError/symlink/commit failure) OR on restart of a crashed inplace (hand-10,
        PRINCIPLE B). Idempotent given the intent record.

        Each target is restored to its recorded pre-state: a pre-existing FILE is rewritten
        with its original content; a path this inplace CREATED (pre_kind `absent`) is
        unlinked; a pre-existing DIRECTORY/other is left untouched (a whole-file write could
        not have mutated it — the write failed). The declared paths are then unstaged with a
        CHECKED reset so a durable failed result leaves NO partial effect for a later node to
        claim (an unstage failure raises WorkspaceFault, never a lying durable failure)."""
        for w in writes:
            target = self.workdir / w.path
            if w.pre_kind == "file":
                target.write_text(w.original or "", encoding="utf-8")
            elif w.pre_kind == "absent":
                if target.is_file() or target.is_symlink():
                    target.unlink(missing_ok=True)
            # pre_kind dir/other: the write never mutated it; leave it in place.
        if writes:
            self._checked(
                self.git("reset", "-q", "--", *[w.path for w in writes]), "inplace rollback unstage"
            )

    # -- git plumbing --------------------------------------------------------

    def git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["git", *args], cwd=self.workdir, capture_output=True, text=True)

    def head_commit(self) -> str | None:
        proc = self.git("rev-parse", "HEAD")
        return proc.stdout.strip() if proc.returncode == 0 else None

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
