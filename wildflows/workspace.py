"""Git authority for stacked v2 frame branches and external worktrees."""
from __future__ import annotations

import hashlib
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from wildflows.result import CommitReceipt, IntegrationReceipt


class RepositoryError(RuntimeError):
    """A repository or plain Git operation failed."""


class BranchDivergedError(RepositoryError):
    """A frame or run branch moved outside the journalled transition."""


class IntegrationError(RepositoryError):
    """A candidate could not be integrated without changing its target branch."""


class FrameOwnershipError(RepositoryError):
    """A frame branch exists without durable ownership by this run."""


class RootIntegrationOwnershipError(IntegrationError):
    """The run branch is checked out by a worktree this run does not own."""


@dataclass(frozen=True)
class FrameWorktree:
    frame_id: str
    path: Path
    branch: str
    base_commit: str


class Repository:
    """The target repository plus engine-owned frame branch/worktree operations."""

    def __init__(
        self,
        workdir: Path,
        run_dir: Path,
        run_id: str,
        *,
        run_branch: str | None = None,
        worktrees_root: Path | None = None,
    ) -> None:
        requested = Path(workdir).resolve()
        root = self._run(
            ["git", "rev-parse", "--show-toplevel"], cwd=requested
        ).stdout.strip()
        self.root = Path(root).resolve()
        self.run_dir = Path(run_dir).resolve()
        self.run_id = run_id
        self.ref = self._resolve_branch(run_branch)
        if worktrees_root is None:
            digest = hashlib.sha256(str(self.root).encode("utf-8")).hexdigest()[:12]
            worktrees_root = (
                Path(tempfile.gettempdir())
                / "wildflows-worktrees"
                / digest
                / run_id
            )
        candidate = Path(worktrees_root).resolve(strict=False)
        if candidate.is_relative_to(self.root):
            candidate = self.root.parent / f".{self.root.name}-wildflows-worktrees" / run_id
            candidate = candidate.resolve(strict=False)
        if candidate.is_relative_to(self.root):
            raise RepositoryError("frame worktrees must live outside the target repository")
        self.worktrees_root = candidate

    @staticmethod
    def _run(
        argv: list[str], *, cwd: Path, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        try:
            process = subprocess.run(argv, cwd=cwd, capture_output=True, text=True)
        except (OSError, subprocess.SubprocessError) as exc:
            raise RepositoryError(f"could not launch {' '.join(argv)!r}: {exc}") from exc
        if check and process.returncode != 0:
            detail = process.stderr.strip() or process.stdout.strip()
            raise RepositoryError(f"{' '.join(argv)!r} failed: {detail}")
        return process

    def git(
        self, args: list[str], *, cwd: Path | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return self._run(["git", *args], cwd=cwd or self.root, check=check)

    def _resolve_branch(self, requested: str | None) -> str:
        if requested is None:
            process = self.git(["symbolic-ref", "--quiet", "HEAD"], check=False)
            if process.returncode != 0:
                raise RepositoryError("target repository is detached; pass run_branch")
            ref = process.stdout.strip()
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

    def frame_branch(self, frame_id: str) -> str:
        safe_run = re.sub(r"[^A-Za-z0-9._-]", "-", self.run_id)[:60] or "run"
        safe_frame = re.sub(r"[^A-Za-z0-9._-]", "-", frame_id)[:120] or "frame"
        return f"refs/heads/wildflows/{safe_run}/{safe_frame}"

    @property
    def _integration_ref_prefix(self) -> str:
        run_key = hashlib.sha256(self.run_id.encode("utf-8")).hexdigest()[:24]
        return f"refs/wildflows/integration/{run_key}/"

    def integration_ref(self, frame_id: str) -> str:
        frame_key = hashlib.sha256(frame_id.encode("utf-8")).hexdigest()
        return f"{self._integration_ref_prefix}{frame_key}"

    def integration_refs(self) -> list[str]:
        output = self.git(
            ["for-each-ref", "--format=%(refname)", self._integration_ref_prefix]
        ).stdout
        return [ref for ref in output.splitlines() if ref]

    def publish_integration_ref(self, ref: str, candidate: str) -> None:
        if not ref.startswith(self._integration_ref_prefix):
            raise RepositoryError(f"temporary integration ref is outside this run: {ref}")
        if self.ref_exists(ref):
            if self.branch_tip(ref) == candidate:
                return
            raise RepositoryError(f"temporary integration ref already exists: {ref}")
        process = self.git(
            ["update-ref", ref, candidate, "0" * 40], check=False
        )
        if process.returncode != 0:
            detail = process.stderr.strip() or process.stdout.strip()
            raise RepositoryError(
                f"could not publish temporary integration ref {ref!r}: {detail}"
            )

    def delete_integration_ref(self, ref: str) -> None:
        if not ref.startswith(self._integration_ref_prefix):
            raise RepositoryError(f"temporary integration ref is outside this run: {ref}")
        self.git(["update-ref", "-d", ref], check=False)

    def ref_exists(self, ref: str) -> bool:
        return self.git(["show-ref", "--verify", "--quiet", ref], check=False).returncode == 0

    def branch_tip(self, ref: str | None = None) -> str:
        target = ref or self.ref
        process = self.git(["rev-parse", "--verify", f"{target}^{{commit}}"], check=False)
        if process.returncode != 0:
            raise BranchDivergedError(f"branch {target!r} has no commit tip")
        return process.stdout.strip()

    def head(self, worktree: Path) -> str:
        return self.git(
            ["rev-parse", "--verify", "HEAD^{commit}"], cwd=worktree
        ).stdout.strip()

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        if ancestor == descendant:
            return True
        return self.git(
            ["merge-base", "--is-ancestor", ancestor, descendant], check=False
        ).returncode == 0

    def _branch_owners(self, ref: str) -> list[Path]:
        fields = self.git(["worktree", "list", "--porcelain", "-z"]).stdout.split("\0")
        owners: list[Path] = []
        worktree: Path | None = None
        for field in fields:
            if field.startswith("worktree "):
                worktree = Path(field.removeprefix("worktree ")).resolve()
            elif field == f"branch {ref}" and worktree is not None:
                owners.append(worktree)
            elif not field:
                worktree = None
        return owners

    def _remove_stale_owners(self, ref: str) -> None:
        for owner in self._branch_owners(ref):
            if owner == self.root or not owner.is_relative_to(self.worktrees_root):
                raise RepositoryError(
                    f"frame branch {ref!r} is checked out outside this run: {owner}"
                )
            self.git(["worktree", "remove", "--force", str(owner)], check=False)
        self.git(["worktree", "prune", "--expire", "now"], check=False)

    def create_frame_worktree(
        self,
        frame_id: str,
        branch: str,
        base: str,
        *,
        resume: bool,
    ) -> FrameWorktree:
        self.worktrees_root.mkdir(parents=True, exist_ok=True)
        self._remove_stale_owners(branch)
        safe = re.sub(r"[^A-Za-z0-9._-]", "-", frame_id)[:100] or "frame"
        path = self.worktrees_root / f"{safe}-{uuid4().hex}"
        short = branch.removeprefix("refs/heads/")
        if resume:
            if not self.ref_exists(branch):
                raise BranchDivergedError(f"resumed frame branch is missing: {branch}")
            self.git(["worktree", "add", str(path), short])
        else:
            if self.ref_exists(branch):
                raise FrameOwnershipError(
                    f"new frame branch already exists without durable ownership: {branch}"
                )
            self.git(["worktree", "add", "-b", short, str(path), base])
        return FrameWorktree(frame_id, path, branch, base)

    def remove_worktree(self, worktree: FrameWorktree) -> None:
        self.git(["worktree", "remove", "--force", str(worktree.path)], check=False)
        self.git(["worktree", "prune", "--expire", "now"], check=False)

    def status_porcelain(self, worktree: Path) -> str:
        return self.git(
            ["status", "--porcelain", "--untracked-files=all"], cwd=worktree
        ).stdout

    def ensure_clean(self, worktree: Path, branch: str) -> str:
        head = self.head(worktree)
        if self.branch_tip(branch) != head:
            raise BranchDivergedError(f"worktree HEAD differs from frame branch {branch!r}")
        status = self.status_porcelain(worktree)
        if status:
            raise IntegrationError(
                "caller frame worktree is dirty; commit or clean the changes below, "
                "then retry the engine tool.\n"
                f"git status --porcelain:\n{status.rstrip()}"
            )
        return head

    def append_excludes(self, worktree: Path, paths: list[str]) -> None:
        """Append anchored link patterns to the repository exclude file once."""
        raw_path = self.git(
            ["rev-parse", "--git-path", "info/exclude"], cwd=worktree
        ).stdout.strip()
        exclude = Path(raw_path)
        if not exclude.is_absolute():
            exclude = worktree / exclude
        try:
            current = exclude.read_bytes() if exclude.exists() else b""
            existing = set(current.splitlines())
            additions = [
                f"/{path}".encode("utf-8")
                for path in paths
                if f"/{path}".encode("utf-8") not in existing
            ]
            if not additions:
                return
            separator = b"" if not current or current.endswith(b"\n") else b"\n"
            with exclude.open("ab") as stream:
                stream.write(separator + b"\n".join(additions) + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as exc:
            raise RepositoryError(
                f"could not update repository exclude file {exclude}: {exc}"
            ) from exc

    def is_tracked(self, worktree: Path, path: str) -> bool:
        return self.git(
            ["ls-files", "--error-unmatch", "--", path],
            cwd=worktree,
            check=False,
        ).returncode == 0

    def commit_all(self, worktree: Path, message: str) -> str:
        self.git(["add", "-A", "--", "."], cwd=worktree)
        staged = self.git(["diff", "--cached", "--quiet"], cwd=worktree, check=False)
        if staged.returncode not in (0, 1):
            raise IntegrationError(staged.stderr.strip() or "could not inspect staged changes")
        if staged.returncode == 1:
            self.git(["commit", "-m", message], cwd=worktree)
        if self.git(
            ["status", "--porcelain", "--untracked-files=all"], cwd=worktree
        ).stdout:
            raise IntegrationError("frame worktree remained dirty after core commit")
        return self.head(worktree)

    def _paths(self, args: list[str], *, cwd: Path) -> list[str]:
        process = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=False)
        if process.returncode != 0:
            detail = process.stderr.decode("utf-8", "replace").strip()
            raise RepositoryError(f"git {' '.join(args)!r} failed: {detail}")
        try:
            return [part.decode("utf-8") for part in process.stdout.split(b"\0") if part]
        except UnicodeDecodeError as exc:
            raise RepositoryError("non-UTF-8 repository paths are unsupported") from exc

    def diffstat(self, base: str, candidate: str, *, limit: int = 8192) -> str:
        """Return a bounded committed diffstat for a frame salvage range."""
        if limit <= 0:
            raise ValueError("diffstat limit must be positive")
        output = self.git([
            "diff",
            "--stat",
            "--no-ext-diff",
            "--no-textconv",
            base,
            candidate,
            "--",
        ]).stdout.rstrip()
        encoded = output.encode("utf-8")
        if len(encoded) <= limit:
            return output
        marker = b"\n[... DIFFSTAT TRUNCATED ...]"
        prefix = encoded[:max(0, limit - len(marker))]
        return (prefix + marker).decode("utf-8", errors="ignore")

    def receipt(self, base: str, candidate: str) -> IntegrationReceipt:
        if base == candidate:
            return IntegrationReceipt()
        if self.git(["merge-base", "--is-ancestor", base, candidate], check=False).returncode:
            raise IntegrationError("candidate HEAD does not descend from its frame base")
        shas = self.git(["rev-list", "--reverse", f"{base}..{candidate}"]).stdout.splitlines()
        commits: list[CommitReceipt] = []
        parent = base
        for sha in shas:
            parents = self.git(["show", "-s", "--format=%P", sha]).stdout.split()
            if parents != [parent]:
                raise IntegrationError("frame commit range must be a linear chain")
            paths = self._paths(
                ["diff", "--name-only", "--no-renames", "-z", parent, sha],
                cwd=self.root,
            )
            commits.append(CommitReceipt(sha=sha, paths=paths))
            parent = sha
        if parent != candidate:
            raise IntegrationError("candidate commit range is incomplete")
        return IntegrationReceipt(commits=commits)

    def verify_receipt(
        self, base: str, commits: list[CommitReceipt]
    ) -> IntegrationReceipt:
        if not commits:
            return IntegrationReceipt()
        actual = self.receipt(base, commits[-1].sha)
        expected = IntegrationReceipt(commits=commits)
        if actual.model_dump() != expected.model_dump():
            raise RepositoryError("receipt commit range or changed paths do not match Git")
        return actual

    def advance(
        self,
        target_ref: str,
        base: str,
        candidate: str,
        *,
        target_worktree: Path | None,
    ) -> None:
        actual = self.branch_tip(target_ref)
        if actual == candidate:
            return
        if actual != base:
            raise BranchDivergedError(
                f"branch {target_ref!r} moved: expected {base} or {candidate}, found {actual}"
            )
        if candidate == base:
            return
        if self.git(["merge-base", "--is-ancestor", base, candidate], check=False).returncode:
            raise IntegrationError("candidate is not a fast-forward of its target")
        if target_worktree is not None:
            symbolic = self.git(
                ["symbolic-ref", "--quiet", "HEAD"], cwd=target_worktree, check=False
            )
            if symbolic.returncode != 0 or symbolic.stdout.strip() != target_ref:
                raise IntegrationError("integration target worktree does not own target branch")
            process = self.git(
                ["merge", "--ff-only", "--no-edit", candidate],
                cwd=target_worktree,
                check=False,
            )
        else:
            if self._branch_owners(target_ref):
                raise IntegrationError("target branch is checked out in another worktree")
            process = self.git(
                ["update-ref", target_ref, candidate, base], check=False
            )
        landed = self.branch_tip(target_ref)
        if landed == candidate:
            return
        if landed != base:
            raise BranchDivergedError(
                f"branch {target_ref!r} changed during integration: {landed}"
            )
        detail = process.stderr.strip() or process.stdout.strip() or "fast-forward refused"
        raise IntegrationError(detail)

    def checked_out_owner(self, ref: str) -> Path | None:
        owners = self._branch_owners(ref)
        if not owners:
            return None
        if len(owners) != 1:
            raise IntegrationError(f"branch {ref!r} has multiple worktree owners")
        return owners[0]

    def reapply(
        self,
        source: list[CommitReceipt],
        moving_base: str,
        *,
        temporary_ref: str | None = None,
    ) -> tuple[str, IntegrationReceipt]:
        path = self.worktrees_root / f"integrate-{uuid4().hex}"
        self.git(["worktree", "add", "--detach", str(path), moving_base])
        temporary = FrameWorktree("integrator", path, "", moving_base)
        try:
            for commit in source:
                process = self.git(
                    ["cherry-pick", "--allow-empty", commit.sha], cwd=path, check=False
                )
                if process.returncode != 0:
                    detail = process.stderr.strip() or process.stdout.strip() or "conflict"
                    raise IntegrationError(f"sibling reapply failed: {detail}")
            candidate = self.head(path)
            receipt = self.receipt(moving_base, candidate)
            if [item.paths for item in receipt.commits] != [item.paths for item in source]:
                raise IntegrationError("sibling reapply changed its owned path set")
            if temporary_ref is not None:
                self.publish_integration_ref(temporary_ref, candidate)
            return candidate, receipt
        finally:
            self.remove_worktree(temporary)
