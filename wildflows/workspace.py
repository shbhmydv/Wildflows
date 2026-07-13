"""Filesystem + git effect mediation for a run's workdir.

The engine says "run this node in this workspace, then finalize"; it never issues git
commands itself. This home owns containment guards, staging/commit, changed-path
parsing, reconciliation-marker lookup, and failed-diff capture. Per-node worktree
leases and one accumulated integration receipt are a later raze (item 4); the PoC keeps
the single shared-workdir policy, moved here verbatim (defects included).

Lexical path rejection (absolute / `..` / literal `.git`) is now an admission-time
validator on `Edit`/`CtxRef`; what remains here is the ENVIRONMENT-dependent resolution
a validator cannot do: symlink escapes and a linked worktree's resolved gitdir.
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal


@dataclass
class Integration:
    """The outcome of a core-mediated git integration."""

    status: Literal["committed", "noop", "failed"]
    commit: str | None = None
    paths: list[str] = field(default_factory=list)
    stderr: str = ""


class Workspace:
    """Containment + git mediation over one workdir (shared across a run's nodes)."""

    def __init__(self, workdir: Path, run_dir: Path) -> None:
        self.workdir = Path(workdir)
        self.run_dir = Path(run_dir)

    # -- git plumbing --------------------------------------------------------

    def git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args], cwd=self.workdir, capture_output=True, text=True
        )

    def head_commit(self) -> str | None:
        """The workdir's current HEAD sha, or None if it has no commits yet."""
        proc = self.git("rev-parse", "HEAD")
        return proc.stdout.strip() if proc.returncode == 0 else None

    def integrate(self, declared: list[str] | None, message: str) -> Integration:
        """Core-mediated commit. `declared=None` -> all worktree changes (a `do`); a
        path list -> ONLY those declared paths, even if other changes are staged (an
        `inplace`, via git's `--`-scoped pathspec commit). Git failures return
        status="failed" with stderr; never raises."""
        if declared is None:
            add = self.git("add", "-A", "--", ".")
        else:
            add = self.git("add", "--", *declared)
        if add.returncode != 0:
            return Integration("failed", stderr=add.stderr)
        if declared is None:
            diff = self.git("diff", "--cached", "--quiet")
        else:
            diff = self.git("diff", "--cached", "--quiet", "--", *declared)
        if diff.returncode == 0:  # nothing staged for our scope -> no-op
            return Integration("noop")
        if declared is None:
            commit = self.git("commit", "-q", "-m", message)
        else:
            # Pathspec commit: commits ONLY these paths, leaving any other staged
            # changes staged-and-uncommitted.
            commit = self.git("commit", "-q", "-m", message, "--", *declared)
        if commit.returncode != 0:
            return Integration("failed", stderr=commit.stderr)
        sha = self.git("rev-parse", "HEAD").stdout.strip()
        changed = self.paths_in_commit(sha) if declared is None else list(declared)
        return Integration("committed", commit=sha, paths=changed)

    def paths_in_commit(self, sha: str) -> list[str]:
        """The paths a commit changed, NUL-parsed so whitespace filenames survive."""
        out = self.git(
            "diff-tree", "--no-commit-id", "--name-only", "-r", "--root", "-z", sha
        ).stdout
        return [p for p in out.split("\0") if p]

    def paths_in_range(self, pre: str | None, post: str) -> list[str]:
        """Paths changed by pre..post (rig-made commits); NUL-parsed."""
        if pre is None:
            return self.paths_in_commit(post)
        out = self.git("diff", "--name-only", "-z", pre, post).stdout
        return [p for p in out.split("\0") if p]

    def find_marked_commit(self, marker: str) -> str | None:
        """The newest commit carrying a reconciliation marker in its message, or None."""
        log = self.git("log", "--all", "--format=%H", "--fixed-strings", f"--grep={marker}")
        if log.returncode != 0 or not log.stdout.strip():
            return None
        return log.stdout.strip().splitlines()[0]

    def capture_and_reset_dirty(self, diff_name: str) -> Path | None:
        """Capture a failed rig's dirty working-tree diff to the run log dir and reset the
        workdir to HEAD, so a later node cannot stage + claim the leak. Returns the diff
        file path (journalled in the failed result) or None if the tree is clean.

        PoC policy (single shared workdir): superseded by per-node worktrees at step 4.
        """
        self.git("add", "-A", "--", ".")
        diff = self.git("diff", "--cached")
        diff_path: Path | None = None
        if diff.stdout.strip():
            leak_dir = self.run_dir / "failed-diffs"
            leak_dir.mkdir(parents=True, exist_ok=True)
            diff_path = leak_dir / diff_name
            diff_path.write_text(diff.stdout, encoding="utf-8")
        if self.head_commit() is not None:
            self.git("reset", "--hard", "HEAD")
        else:
            self.git("reset")  # no commit yet: unstage, then clean removes the files
        self.git("clean", "-fdq")
        return diff_path

    # -- containment (environment-dependent; lexical guards are admission-time) --

    def resolve_safe_path(self, rel: str) -> Path:
        """Resolve an `inplace` edit path under the workdir, raising on an escape.

        Lexical escapes (`..`, absolute, literal `.git`) are already rejected at
        admission; what remains is a symlink that resolves outside the workdir or into
        the (possibly linked-worktree) gitdir.
        """
        target = (self.workdir / rel).resolve()
        if not target.is_relative_to(self.workdir.resolve()):
            raise ValueError(f"inplace edit escapes workdir: {rel}")
        absgit = self.git("rev-parse", "--absolute-git-dir")
        if absgit.returncode == 0 and absgit.stdout.strip():
            gitdir = Path(absgit.stdout.strip()).resolve()
            if target == gitdir or target.is_relative_to(gitdir):
                raise ValueError(f"inplace edit targets a git admin path: {rel}")
        return target

    def read_contained_file(self, rel: str) -> str | None:
        """Read a `ctx` file resolved under the workdir; None if it escapes or is absent.

        Admission cannot resolve symlinks, so an in-worktree symlink pointing outside is
        caught HERE (a failed result), never a host-file exfiltration.
        """
        target = (self.workdir / rel).resolve()
        if not target.is_relative_to(self.workdir.resolve()):
            return None
        try:
            return target.read_text(encoding="utf-8")
        except OSError:
            return None

    def run_predicate(self, cmd: str) -> bool:
        """Run a loop `until` predicate in the workdir; exit 0 means converged."""
        return subprocess.run(cmd, shell=True, cwd=self.workdir).returncode == 0
