from __future__ import annotations
import ctypes
import os
import re
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from uuid import uuid4
from wildflows.result import CommitReceipt, IntegrationReceipt

_PRCTL: Callable[[int, int, int, int, int], int] | None = None
if sys.platform == "linux":
    _LIBC = ctypes.CDLL(None, use_errno=True)
    _RAW_PRCTL = _LIBC.prctl
    _RAW_PRCTL.argtypes = [
        ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong,
    ]
    _RAW_PRCTL.restype = ctypes.c_int
    _PRCTL = _RAW_PRCTL
_PR_SET_PDEATHSIG = 1


def _die_with_parent(parent_pid: int) -> None:
    if _PRCTL is None or _PRCTL(_PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0:
        os._exit(127)
    if os.getppid() != parent_pid:
        os.kill(os.getpid(), signal.SIGKILL)
        os._exit(127)


class RepositoryError(RuntimeError):
    """A repository or plain Git operation failed."""
class RepositoryTransientError(RepositoryError): pass
class BranchDivergedError(RepositoryError):
    """The run branch no longer has the exact journalled tip."""
class IntegrationError(RepositoryError):
    """A candidate could not be integrated without changing the run branch."""
@dataclass(frozen=True)
class NodeWorktree:
    path: Path
    base_commit: str
class Repository:
    """The target repository, run branch, and disposable-worktree operations."""
    def __init__(self, workdir: Path, run_dir: Path, run_branch: str | None = None) -> None:
        requested = Path(workdir).resolve()
        root = self._run(
            ["git", "rev-parse", "--show-toplevel"], cwd=requested
        ).stdout.strip()
        self.root = Path(root).resolve()
        self.run_dir = Path(run_dir).resolve()
        if self.run_dir.is_relative_to(self.root):
            raise ValueError("run_dir must be outside the target repository worktree")
        self.worktrees_dir = self.run_dir / "worktrees"
        self.ref = self._resolve_branch(run_branch)
    @staticmethod
    def _run(
        argv: list[str], *, cwd: Path, check: bool = True,
        parent_lifetime: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        parent_pid = os.getpid()
        preexec_fn = (
            (lambda: _die_with_parent(parent_pid)) if parent_lifetime else None
        )
        try:
            proc = subprocess.run(
                argv, cwd=cwd, capture_output=True, text=True, preexec_fn=preexec_fn
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise RepositoryError(f"could not launch {' '.join(argv)!r}: {exc}") from exc
        if check and proc.returncode != 0:
            detail = proc.stderr.strip() or proc.stdout.strip()
            raise RepositoryError(f"{' '.join(argv)!r} failed: {detail}")
        return proc
    def git(
        self, args: list[str], *, cwd: Path | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return self._run(["git", *args], cwd=cwd or self.root, check=check)
    def _resolve_branch(self, requested: str | None) -> str:
        if requested is None:
            proc = self.git(["symbolic-ref", "--quiet", "HEAD"], check=False)
            if proc.returncode != 0:
                raise RepositoryError("target workdir is detached; pass run_branch explicitly")
            ref = proc.stdout.strip()
        else:
            ref = requested if requested.startswith("refs/heads/") else f"refs/heads/{requested}"
        short = ref.removeprefix("refs/heads/")
        if not short or self.git(["check-ref-format", "--branch", short], check=False).returncode:
            raise RepositoryError(f"invalid run branch: {requested!r}")
        if self.git(["rev-parse", "--verify", ref], check=False).returncode:
            raise RepositoryError(f"run branch does not exist: {short!r}")
        return ref
    @property
    def branch(self) -> str:
        return self.ref.removeprefix("refs/heads/")
    def branch_claim(self) -> str:
        proc = self.git(["rev-parse", "--verify", self.ref], check=False)
        if proc.returncode != 0:
            raise BranchDivergedError(f"run branch {self.branch!r} has no claimed tip")
        return proc.stdout.strip()
    def commit_exists(self, commit: str) -> bool:
        return self.git(["cat-file", "-e", f"{commit}^{{commit}}"], check=False).returncode == 0
    def branch_tip(self) -> str:
        claim = self.branch_claim()
        if not self.commit_exists(claim):
            raise BranchDivergedError(f"run branch {self.branch!r} has no commit tip")
        return claim
    def head(self, worktree: Path) -> str:
        return self.git(["rev-parse", "--verify", "HEAD^{commit}"], cwd=worktree).stdout.strip()
    def create_worktree(self, epoch: int, node_id: str, attempt: int, base: str) -> NodeWorktree:
        self.worktrees_dir.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"[^A-Za-z0-9._-]", "-", node_id)[:80] or "node"
        path = self.worktrees_dir / f"e{epoch}-{safe}-a{attempt}-{uuid4().hex}"
        self.git(["worktree", "add", "--detach", str(path), base])
        return NodeWorktree(path=path, base_commit=base)
    def remove_worktree(self, worktree: NodeWorktree) -> None:
        self.git(["worktree", "remove", "--force", str(worktree.path)], check=False)
    def ensure_tip(self, expected: str) -> None:
        actual = self.branch_tip()
        if actual != expected:
            raise BranchDivergedError(
                f"run branch {self.branch!r} moved outside the journal: "
                f"expected {expected}, found {actual}"
            )
    def _paths(self, args: list[str], *, cwd: Path) -> list[str]:
        proc = subprocess.run(
            ["git", *args], cwd=cwd, capture_output=True, text=False
        )
        if proc.returncode != 0:
            detail = proc.stderr.decode("utf-8", "replace").strip()
            raise RepositoryError(f"git {' '.join(args)!r} failed: {detail}")
        try:
            return [part.decode("utf-8") for part in proc.stdout.split(b"\0") if part]
        except UnicodeDecodeError as exc:
            raise RepositoryError("non-UTF-8 repository paths are unsupported") from exc
    def changed_paths(self, worktree: Path) -> list[str]:
        tracked = self._paths(["diff", "--name-only", "-z", "HEAD"], cwd=worktree)
        untracked = self._paths(
            ["ls-files", "--others", "--exclude-standard", "-z"], cwd=worktree
        )
        return list(dict.fromkeys([*tracked, *untracked]))
    def commit_all(self, worktree: Path, message: str) -> None:
        self.git(["add", "-A", "--", "."], cwd=worktree)
        staged = self.git(["diff", "--cached", "--quiet"], cwd=worktree, check=False)
        if staged.returncode not in (0, 1):
            raise IntegrationError(staged.stderr.strip() or "could not inspect staged changes")
        if staged.returncode == 1:
            self.git(["commit", "-m", message], cwd=worktree)
        if self.git(["status", "--porcelain"], cwd=worktree).stdout:
            raise IntegrationError("node worktree remained dirty after core commit")
    def commit_declared(self, worktree: Path, paths: list[str], message: str) -> None:
        declared = set(paths)
        unexpected = [p for p in self.changed_paths(worktree) if p not in declared]
        if unexpected:
            raise IntegrationError(
                f"inplace changed undeclared paths: {', '.join(unexpected)}"
            )
        if not paths:
            return
        self.git(["add", "-A", "--", *paths], cwd=worktree)
        staged = self.git(["diff", "--cached", "--quiet"], cwd=worktree, check=False)
        if staged.returncode not in (0, 1):
            raise IntegrationError(staged.stderr.strip() or "could not inspect staged changes")
        if staged.returncode == 1:
            self.git(["commit", "-m", message], cwd=worktree)
        if self.git(["status", "--porcelain"], cwd=worktree).stdout:
            raise IntegrationError("inplace worktree remained dirty after core commit")
    def receipt(self, base: str, candidate: str) -> IntegrationReceipt:
        if base == candidate:
            return IntegrationReceipt()
        if self.git(["merge-base", "--is-ancestor", base, candidate], check=False).returncode:
            raise IntegrationError("candidate HEAD does not descend from its run-branch base")
        shas = self.git(["rev-list", "--reverse", f"{base}..{candidate}"]).stdout.splitlines()
        commits: list[CommitReceipt] = []
        parent = base
        for sha in shas:
            parents = self.git(["show", "-s", "--format=%P", sha]).stdout.split()
            if parents != [parent]:
                raise IntegrationError("node commit range must be a linear fast-forward chain")
            paths = self._paths(["diff", "--name-only", "--no-renames", "-z", parent, sha], cwd=self.root)
            commits.append(CommitReceipt(sha=sha, paths=paths))
            parent = sha
        if parent != candidate:
            raise IntegrationError("candidate commit range is incomplete")
        return IntegrationReceipt(commits=commits)
    def verify_receipt(
        self, base: str, commits: list[CommitReceipt]
    ) -> IntegrationReceipt:
        if not commits:
            raise RepositoryError("an integrated claim has no commits")
        for commit in commits:
            if not self.commit_exists(commit.sha):
                raise RepositoryError(f"receipt commit does not exist: {commit.sha}")
        actual = self.receipt(base, commits[-1].sha)
        if actual.model_dump() != IntegrationReceipt(commits=commits).model_dump():
            raise RepositoryError("receipt commit range or changed paths do not match Git")
        return actual
    def _branch_worktrees(self) -> list[Path]:
        fields = self.git(["worktree", "list", "--porcelain", "-z"]).stdout.split("\0")
        owners: list[Path] = []
        worktree: Path | None = None
        for field in fields:
            if field.startswith("worktree "):
                worktree = Path(field.removeprefix("worktree ")).resolve()
            elif field == f"branch {self.ref}" and worktree is not None:
                owners.append(worktree)
            elif not field:
                worktree = None
        return owners
    def _move_ref(self, args: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        return self._run(
            ["git", *args], cwd=cwd, check=False, parent_lifetime=True
        )
    @staticmethod
    def _git_writer_is_live(uid: int) -> bool:
        procfs = Path("/proc")
        if not procfs.is_dir(): return True
        try: processes = list(procfs.iterdir())
        except OSError: return True
        for process in (path for path in processes if path.name.isdigit()):
            try:
                if process.stat().st_uid != uid: continue
                fields = (process / "stat").read_text(encoding="utf-8").split()
                command = (process / "comm").read_text(encoding="utf-8").strip()
            except (FileNotFoundError, ProcessLookupError): continue
            except (OSError, UnicodeError): return True
            if len(fields) > 2 and fields[2] != "Z" and (command == "git" or command.startswith("git-")):
                return True
        return False
    @staticmethod
    def _sync_path(path: Path, *, directory: bool = False) -> None:
        flags = os.O_RDONLY | (getattr(os, "O_DIRECTORY", 0) if directory else 0)
        try:
            descriptor = os.open(path, flags)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)
        except OSError as exc:
            raise RepositoryTransientError(f"could not sync recovered path: {exc}") from exc
    def _lock_path(self, name: str) -> Path:
        raw = self.git(["rev-parse", "--git-path", name]).stdout.strip()
        return Path(raw) if Path(raw).is_absolute() else self.root / raw
    def _recover_lock(self, lock: Path, *, root_owned: bool, is_index: bool) -> bool:
        try:
            observed = lock.stat()
        except FileNotFoundError:
            if lock.parent.is_dir():
                self._sync_path(lock.parent, directory=True)
            return False
        except OSError as exc:
            raise RepositoryTransientError(f"could not inspect Git lock: {exc}") from exc
        if is_index and not root_owned:
            raise RepositoryTransientError("run worktree no longer owns the branch with the interrupted index lock")
        if observed.st_uid != os.geteuid() or self._git_writer_is_live(observed.st_uid):
            raise RepositoryTransientError("repository lock is owned by a live writer")
        try:
            current = lock.stat()
            if (current.st_dev, current.st_ino) != (observed.st_dev, observed.st_ino):
                raise RepositoryTransientError("Git lock changed during recovery")
            lock.unlink()
        except FileNotFoundError:
            self._sync_path(lock.parent, directory=True)
            return False
        except OSError as exc:
            raise RepositoryTransientError(f"could not remove Git lock: {exc}") from exc
        self._sync_path(lock.parent, directory=True)
        return True
    def recover_interrupted_locks(self) -> bool:
        root_owned = self.root in self._branch_worktrees()
        locks = (("index.lock", True), ("ORIG_HEAD.lock", False), (f"{self.ref}.lock", False))
        cleaned = False
        for name, is_index in locks:
            cleaned = self._recover_lock(self._lock_path(name), root_owned=root_owned, is_index=is_index) or cleaned
        return cleaned
    def _matches_resume_tree(self, base: str, candidate: str, path: str) -> bool:
        literal = ["--literal-pathspecs"]
        target = self.root / path
        if target.parent.resolve() != target.parent: return False
        status = self.git([*literal, "status", "--porcelain", "--untracked-files=all", "--ignored=matching", "--", path]).stdout
        if not status: return True
        index_states = [self.git([
            *literal, "diff", "--cached", "--quiet", tree, "--", path
        ], check=False).returncode for tree in (base, candidate)]
        if 0 not in index_states: return False
        entry = self.git([*literal, "ls-tree", candidate, "--", path]).stdout.split()
        if not os.path.lexists(target): return True
        if len(entry) >= 2 and entry[0] == "040000" and target.is_dir(): return True
        if len(entry) >= 3 and entry[0] == "120000" and target.is_symlink():
            if os.readlink(target) == self.git(["cat-file", "blob", entry[2]]).stdout: return True
        if len(entry) >= 3 and not target.is_symlink() and target.is_file():
            mode = "100755" if os.access(target, os.X_OK) else "100644"
            hashed = self.git(["hash-object", f"--path={path}", "--", path], check=False)
            if entry[0] == mode and hashed.returncode == 0 and entry[2] == hashed.stdout.strip(): return True
        if status.startswith(("?? ", "!! ")): return False
        worktrees = [self.git(
            [*literal, "diff", "--quiet", tree, "--", path], check=False
        ).returncode for tree in (base, candidate)]
        return 0 in worktrees
    def sync_run_ref(self) -> None:
        ref_path = self._lock_path(self.ref)
        if not ref_path.exists(): ref_path = self._lock_path("packed-refs")
        self._sync_path(ref_path)
        self._sync_path(ref_path.parent, directory=True)
    def _sync_checkout(self, paths: list[str]) -> None:
        for target in [self._lock_path("index"), *(self.root / path for path in paths)]:
            if target.is_file() and not target.is_symlink(): self._sync_path(target)
            if target.parent.is_dir(): self._sync_path(target.parent, directory=True)
    def restore_interrupted_integration(self, base: str, candidate: str) -> bool:
        cleaned = self.recover_interrupted_locks()
        owners = self._branch_worktrees()
        if not owners: return cleaned
        if self.root not in owners:
            raise RepositoryTransientError("run branch acquired another worktree owner")
        paths = self.receipt(base, candidate).paths
        if any(not self._matches_resume_tree(base, candidate, path) for path in paths):
            raise RepositoryTransientError("run worktree changed outside the interrupted integration")
        if not paths: return cleaned
        self.ensure_tip(base)
        reset = self._move_ref(["--literal-pathspecs", "reset", "-q", base, "--", *paths], cwd=self.root)
        if reset.returncode != 0: raise RepositoryTransientError("could not restore interrupted integration index")
        base_paths = [path for path in paths if self.git(
            ["--literal-pathspecs", "ls-tree", base, "--", path]).stdout]
        for path in set(paths) - set(base_paths):
            target = self.root / path
            if target.parent.resolve() != target.parent:
                raise RepositoryTransientError("interrupted integration path gained a symlink parent")
            if os.path.lexists(target):
                try:
                    target.unlink()
                except OSError as exc:
                    raise RepositoryTransientError(f"could not remove interrupted integration path: {exc}") from exc
        for path in base_paths:
            target = self.root / path
            if target.is_dir():
                try: target.rmdir()
                except OSError as exc: raise RepositoryTransientError(f"could not restore path type: {exc}") from exc
        if base_paths:
            restored = self._move_ref([
                "--literal-pathspecs", "restore", f"--source={base}", "--worktree", "--", *base_paths
            ], cwd=self.root)
            if restored.returncode != 0:
                raise RepositoryTransientError("could not restore interrupted integration tree")
        dirty = self.git([
            "--literal-pathspecs", "status", "--porcelain", "--untracked-files=all", "--", *paths
        ]).stdout
        if dirty: raise RepositoryTransientError("run worktree remains dirty after integration recovery")
        self._sync_checkout(paths)
        return cleaned
    def restore_missing_claim(self, missing: str, fallback: str) -> bool:
        owners = self._branch_worktrees()
        other = [owner for owner in owners if owner != self.root]
        if other: raise IntegrationError(f"run branch {self.branch!r} is checked out in linked worktree {other[0]}")
        cleaned = self.recover_interrupted_locks()
        if self.root in owners:
            before = self._paths(["ls-files", "-z"], cwd=self.root)
            tree = self._move_ref(["read-tree", "--reset", "-u", fallback], cwd=self.root)
            if tree.returncode != 0:
                detail = tree.stderr.strip() or tree.stdout.strip() or "index update refused"
                raise RepositoryTransientError(f"could not restore checked-out run tree; retry: {detail}")
            after = self._paths(["ls-files", "-z"], cwd=self.root)
            self._sync_checkout(list(dict.fromkeys([*before, *after])))
        proc = self._move_ref(["update-ref", self.ref, fallback, missing], cwd=self.root)
        actual = self.branch_claim()
        if actual == fallback:
            self.sync_run_ref()
            return cleaned
        detail = proc.stderr.strip() or proc.stdout.strip() or "compare-and-swap refused"
        if actual == missing:
            raise RepositoryTransientError(f"could not restore missing run-branch claim; retry: {detail}")
        raise BranchDivergedError(f"could not restore missing run-branch claim: {detail}")
    def integrate(self, base: str, candidate: str) -> None:
        """Fast-forward the run branch, or leave it exactly at ``base``."""
        self.ensure_tip(base)
        if candidate == base:
            return
        if self.git(["merge-base", "--is-ancestor", base, candidate], check=False).returncode:
            raise IntegrationError("candidate is not a fast-forward of the run branch")
        owners = self._branch_worktrees()
        other = [owner for owner in owners if owner != self.root]
        if other:
            raise IntegrationError(
                f"run branch {self.branch!r} is checked out in linked worktree {other[0]}"
            )
        if self.root in owners:
            proc = self._move_ref(
                ["merge", "--ff-only", "--no-edit", candidate], cwd=self.root
            )
        else:
            proc = self._move_ref(
                ["update-ref", self.ref, candidate, base], cwd=self.root
            )
        actual = self.branch_tip()
        if actual == candidate:
            return
        if actual != base:
            raise BranchDivergedError(
                f"run branch changed during integration: expected {base} or {candidate}, "
                f"found {actual}"
            )
        detail = proc.stderr.strip() or proc.stdout.strip() or "fast-forward refused"
        raise IntegrationError(detail)
    def safe_path(self, worktree: Path, relative: str, *, for_write: bool) -> Path:
        root = worktree.resolve()
        lexical = root / relative
        resolved = lexical.resolve(strict=False)
        if not resolved.is_relative_to(root):
            raise ValueError(f"path escapes worktree through a symlink: {relative!r}")
        rel = resolved.relative_to(root)
        if ".git" in rel.parts:
            raise ValueError(f"path targets worktree git administration: {relative!r}")
        if for_write and resolved != lexical:
            raise ValueError(f"inplace edit path uses a symbolic-link alias: {relative!r}")
        if for_write and os.path.lexists(resolved) and not resolved.is_file():
            raise ValueError(f"inplace target is not a regular file: {relative!r}")
        return resolved
    def read_file(self, worktree: Path, relative: str) -> str | None:
        try:
            path = self.safe_path(worktree, relative, for_write=False)
            if not path.is_file():
                return None
            return path.read_text(encoding="utf-8")
        except (OSError, UnicodeError, ValueError):
            return None
