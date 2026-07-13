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

    `pre_head` anchors rig-authored-commit discovery (`pre..post`) and the failure
    revert. A per-node worktree is the later backend behind this same seam.
    """

    node_key: NodeKey
    pre_head: str | None


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

    def open_lease(self, node_key: NodeKey) -> Lease:
        """Begin a node attempt: snapshot pre-run HEAD (None on an unborn repo)."""
        return Lease(node_key=node_key, pre_head=self.head_commit())

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
        """A failed rig's effects are REVERTED and captured (shared-workdir policy).

        Evidence (committed rig work `pre..post`, uncommitted tracked changes, AND
        untracked/ignored artifacts the rig created — defect 3) is captured to the run
        log dir; the workdir is then reset to the lease's PRE-run HEAD (undoing commits
        the failing rig made — defect 2) and `git clean -fdxq` removes untracked AND
        ignored leftovers, so no later node can stage or observe the leak. Returns the
        evidence path (journalled in the failed result) or None if nothing changed.

        Per-node worktree isolation later replaces this with discard-the-worktree.
        """
        pre = lease.pre_head
        # Working tree (incl any commit the failing rig made) vs the lease's PRE base —
        # the empty tree when the lease opened on an unborn repo — captures committed AND
        # uncommitted tracked leaks; untracked/ignored are captured separately.
        base = pre if pre is not None else _EMPTY_TREE
        parts: list[str] = []
        diff = self.git("diff", base)
        if diff.stdout.strip():
            parts.append(diff.stdout)
        parts.extend(self._untracked_and_ignored_evidence())

        diff_path: Path | None = None
        if parts:
            leak_dir = self.run_dir / "failed-diffs"
            leak_dir.mkdir(parents=True, exist_ok=True)
            diff_path = leak_dir / diff_name
            diff_path.write_text("\n".join(parts), encoding="utf-8")

        if pre is not None:
            self.git("reset", "--hard", pre)  # undo the failing rig's own commits (defect 2)
        else:
            # Unborn at lease open: if the failing rig created the first commit, drop the
            # branch ref back to unborn so its effect cannot survive as durable history.
            ref = self.git("symbolic-ref", "-q", "HEAD").stdout.strip()
            if self.head_commit() is not None and ref:
                self.git("update-ref", "-d", ref)
            self.git("reset")  # unstage, then clean removes the files
        self.git("clean", "-fdxq")  # -x also removes ignored artifacts
        return diff_path

    def _untracked_and_ignored_evidence(self) -> list[str]:
        """Dump every untracked (`??`) and ignored (`!!`) file's content — git omits
        these from `diff`, so a failing rig's ignored/untracked leak would otherwise
        vanish uncaptured (defect 3)."""
        status = self.git("status", "--ignored", "--porcelain", "-z")
        out: list[str] = []
        for entry in status.stdout.split("\0"):
            if not entry or entry[:2] not in ("??", "!!"):
                continue
            rel = entry[3:]
            try:
                content = (self.workdir / rel).read_text(encoding="utf-8")
            except OSError:
                content = "<binary or unreadable>"
            out.append(f"=== untracked/ignored: {rel} ===\n{content}")
        return out

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

    # -- reconciliation (reachable-from-HEAD only) ---------------------------

    def reconcile_committed(self, marker: str) -> IntegrationReceipt | None:
        """A commit carrying this marker AND reachable from current HEAD (a crash after
        the core committed but before it journalled) yields a retro receipt; an
        unreachable marked commit (e.g. on a side branch, absent from the worktree) is
        NOT retro-integrated — false durable attribution (defect 1)."""
        # rev-list walks ONLY HEAD's ancestry, so a match is reachable by construction.
        log = self.git(
            "rev-list", "--max-count=1", "--fixed-strings", f"--grep={marker}", "HEAD"
        )
        sha = log.stdout.strip().splitlines()[0] if log.returncode == 0 and log.stdout.strip() else None
        if sha is None:
            return None
        return IntegrationReceipt(commits=[CommitReceipt(sha=sha, paths=self._paths_in_commit(sha))])

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

    def record_result(self, key: NodeKey, result: Result) -> None:
        """A terminal result with no integration (failure, effectless, or no-op)."""
        self.journal.append(self._result_event(key, result))

    def record_success(self, key: NodeKey, result: Result, receipt: IntegrationReceipt) -> None:
        """A successful result and, if it had a committed effect, its integrated receipt
        (one event carrying every attributed commit) — result first, integrated second."""
        self.journal.append(self._result_event(key, result))
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
        self, key: NodeKey, result: Result, loop_status: str | None = None
    ) -> ResultEvent:
        epoch, node_id = key
        return ResultEvent(
            run_id=self.run_id, epoch=epoch, node_id=node_id,
            text=result.text, files=result.files, exit_code=result.exit_code,
            outcome=result.outcome, loop_status=loop_status,
        )
