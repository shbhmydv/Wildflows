"""The workspace effect transaction (RAZE item 4) + the completion recorder.

The engine says "run this node in this lease, then finalize success/failure"; it issues
ZERO git commands. `WorkspaceEffects` owns a per-node-attempt lease (pre/post HEAD
capture over the shared workdir — the seam, not yet a worktree), rig-authored commit
discovery, staging/commit, failure evidence capture + revert, marker reconciliation
(reachable-from-HEAD only), and path containment. It hands back one accumulated
`IntegrationReceipt` (every attributed commit, per-commit paths). `CompletionRecorder`
owns the ONE event ordering — result THEN integrated — for every completion path
(do / inplace / reconciliation), replacing three inconsistent orderings.

Per-node worktree leases are a later step; the shared-workdir policy (revert + clean on
failure) lives here and is superseded by discard-the-worktree once worktrees land.
"""
from __future__ import annotations

import hashlib
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from wildflows.events import Integrated, ResultEvent
from wildflows.journal import Journal
from wildflows.projection import NodeKey
from wildflows.result import CommitReceipt, IntegrationReceipt, Result

# Git's canonical empty-tree object — the "base" for a failure diff when the lease opened
# on an unborn repo, so a rig's first-commit leak still diffs verbatim.
_EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


@dataclass
class Lease:
    """A node attempt's workspace lease: the shared workdir + its pre-run HEAD.

    `pre_head` anchors rig-authored-commit discovery (`pre..post`), the provenance
    range `pre_head..HEAD` used on resume, and the failure revert. `preexisting` is the
    set of untracked+ignored paths present at lease open — failure cleanup removes ONLY
    paths NOT in this set, so it never destroys pre-existing user files, the run_dir, or
    anything the lease did not create (hand-8 lease-scoping). A per-node worktree is the
    later backend behind this same seam.
    """

    node_key: NodeKey
    pre_head: str | None
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

    def open_lease(self, node_key: NodeKey) -> Lease | None:
        """Begin a node attempt, or REFUSE (None) on a dirty tracked/index worktree.

        The clean-worktree precondition (hand-9, FAILURE-LEASE): a lease opens only when
        the tracked + index state is clean (untracked files are allowed and snapshotted
        per-file). This is the honest serial-M1 rule — it removes the reset-`--hard`
        destruction class, because pre-existing tracked/staged user work can no longer
        exist at open, so failure revert only ever undoes THIS attempt's own effects.
        (M3 per-node worktree isolation makes the workdir engine-owned and retires this
        precondition.) `preexisting` snapshots untracked+ignored PER FILE so an addition
        under a pre-existing untracked directory is a distinct entry the sweep can detect.
        """
        if self.tracked_dirty():
            return None
        return Lease(
            node_key=node_key,
            pre_head=self.head_commit(),
            preexisting=frozenset(self._untracked_ignored_paths()),
        )

    def tracked_dirty(self) -> bool:
        """True if the worktree has any uncommitted TRACKED or staged change (untracked
        files excluded). The lease-open precondition."""
        proc = self.git("status", "--porcelain", "--untracked-files=no")
        return bool(proc.stdout.strip())

    def clean_dispatched_residue(self) -> None:
        """Clean a dead dispatched-only attempt's leftover DIRT before re-running (hand-9,
        PROVENANCE-RANGE). Reverts uncommitted tracked changes to the CURRENT HEAD and
        sweeps untracked/ignored leaks — but KEEPS committed history (`reset --hard HEAD`,
        not `pre_head`): mid-rig checkpoint commits are accepted forensic residue (reachable
        and identifiable via `pre_head` lineage), never reset away, since an operator commit
        may sit above them. In the serial PoC the engine owns the workdir mid-run, so the
        untracked leftovers ARE the dead attempt's; per-node worktrees (M3) retire this."""
        self.git("reset", "--hard", "HEAD")
        self._remove_leaks(self._untracked_ignored_paths())

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

        if pre is not None:
            self.git("reset", "--hard", pre)  # undo the failing rig's own commits
        else:
            # Unborn at lease open: if the failing rig created the first commit, drop the
            # branch ref back to unborn so its effect cannot survive as durable history.
            ref = self.git("symbolic-ref", "-q", "HEAD").stdout.strip()
            if self.head_commit() is not None and ref:
                self.git("update-ref", "-d", ref)
            self.git("reset")  # unstage so staged leaks become untracked for the sweep
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

    def rollback_inplace(self, writes: list[tuple[str, str | None]]) -> None:
        """Undo an inplace's partial writes on a failure AFTER the first write (a late
        symlink rejection or a failed declared commit) — hand-9, INPLACE-TRANSACTIONAL.

        `writes` is every path this inplace already wrote, in order, each with the content
        that PRE-EXISTED (None if the file was created). Pre-existing files are restored to
        that content; created files are deleted; then the declared paths are unstaged, so a
        durable failed result leaves NO partial effect for a later node to claim."""
        for rel, original in writes:
            target = self.workdir / rel
            if original is None:
                target.unlink(missing_ok=True)
            else:
                target.write_text(original, encoding="utf-8")
        if writes:
            self.git("reset", "-q", "--", *[rel for rel, _ in writes])

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
        self, key: NodeKey, result: Result, post_head: str | None = None
    ) -> None:
        """A terminal result with no integration (failure, effectless, or no-op)."""
        self.journal.append(self._result_event(key, result, post_head=post_head))

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
        post_head: str | None = None,
    ) -> ResultEvent:
        epoch, node_id = key
        return ResultEvent(
            run_id=self.run_id, epoch=epoch, node_id=node_id,
            text=result.text, files=result.files, exit_code=result.exit_code,
            outcome=result.outcome, loop_status=loop_status, post_head=post_head,
        )
