"""Warm-relaunch prompt evidence from the immediately preceding frame attempt."""
from __future__ import annotations

from pathlib import Path

from tests.conftest import executable
from wildflows.engine import Engine
from wildflows.events import FramePushed, WorkerReaped
from wildflows.frame import FrameResult, FrameRuntime
from wildflows.rig import RigRegistry, ScriptRig



class _PromptRig:
    timeout_s = 30.0

    def __init__(self) -> None:
        self.prompts: list[str] = []

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del workdir, runtime
        self.prompts.append(prompt)
        return FrameResult(text="done", exit_code=0)


def _interrupted_script_frame(
    repo: Path,
    tmp_path: Path,
    *,
    run_id: str,
    reaped_reason: str,
) -> tuple[Path, Path, Path, ScriptRig]:
    run_dir = tmp_path / f"{run_id}-run"
    worktrees = tmp_path / f"{run_id}-worktrees"
    log_root = tmp_path / f"{run_id}-logs"
    captured_prompt = tmp_path / f"{run_id}-prompt.txt"
    adapter = executable(
        tmp_path / f"{run_id}-adapter",
        """#!/usr/bin/env python3
import os
from pathlib import Path
import sys

arguments = sys.argv[1:]
prompt = Path(arguments[arguments.index("--prompt") + 1])
Path(os.environ["CAPTURED_PROMPT"]).write_text(
    prompt.read_text(encoding="utf-8"), encoding="utf-8"
)
print("relaunched")
""",
    )
    rig = ScriptRig(
        adapter,
        log_root,
        timeout_s=10,
        env={"CAPTURED_PROMPT": str(captured_prompt)},
    )
    initial = Engine(
        run_dir,
        repo,
        RigRegistry({"script": rig}),
        run_id=run_id,
        root_rig="script",
        root_prompt="recover the interrupted job",
        worktrees_root=worktrees,
    )
    base = initial.repository.branch_tip()
    branch = initial.repository.frame_branch(Engine.ROOT_FRAME_ID)
    interrupted = initial.repository.create_frame_worktree(
        Engine.ROOT_FRAME_ID, branch, base, resume=False
    )
    initial.journal.append(FramePushed(
        run_id=run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        attempt=0,
        depth=0,
        rig="script",
        prompt="recover the interrupted job",
        skills=[],
        branch=branch,
        base_commit=base,
        worktree=str(interrupted.path),
    ))
    initial.journal.append(WorkerReaped(
        run_id=run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        attempt=0,
        pid=123,
        process_group_id=123,
        session_id=123,
        reason=reaped_reason,
        escalated=False,
    ))
    attempt_logs = log_root / interrupted.path.name
    attempt_logs.mkdir(parents=True)
    # ScriptRig's capture logs are a bounded fallback. Adapter-owned Pi logs are
    # the richer source and must win when both exist.
    (attempt_logs / "agent.stdout.log").write_text(
        "do not use the adapter stdout fallback\n", encoding="utf-8"
    )
    (attempt_logs / "agent.stderr.log").write_text(
        "do not use the adapter stderr fallback\n", encoding="utf-8"
    )
    return run_dir, interrupted.path, attempt_logs, rig


def test_first_launch_prompt_has_no_earlier_attempt_block(
    repo: Path, tmp_path: Path
) -> None:
    rig = _PromptRig()
    engine = Engine(
        tmp_path / "fresh-run",
        repo,
        RigRegistry({"prompt": rig}),
        run_id="fresh-prompt",
        root_rig="prompt",
        root_prompt="first launch stays cold",
        worktrees_root=tmp_path / "fresh-worktrees",
    )

    assert engine.run().outcome == "ok"
    assert len(rig.prompts) == 1
    assert "--- EARLIER ATTEMPT ---" not in rig.prompts[0]
    assert "relaunch attempt" not in rig.prompts[0]


def test_relaunch_prompt_includes_prior_log_tails_dirty_diff_and_reap_reason(
    repo: Path, tmp_path: Path
) -> None:
    run_dir, interrupted, attempt_logs, rig = _interrupted_script_frame(
        repo,
        tmp_path,
        run_id="warm-evidence",
        reaped_reason="engine_resume_sweep",
    )
    (attempt_logs / "pi.stdout.log").write_text(
        "".join(f"stdout-{line:03d}\n" for line in range(110)),
        encoding="utf-8",
    )
    (attempt_logs / "pi.stderr.log").write_text(
        "".join(f"stderr-{line:03d}\n" for line in range(110)),
        encoding="utf-8",
    )
    (interrupted / "base.txt").write_text("unfinished evidence\n", encoding="utf-8")
    (interrupted / "new-evidence.txt").write_text(
        "untracked finding\n", encoding="utf-8"
    )

    resumed = Engine(
        run_dir,
        repo,
        RigRegistry({"script": rig}),
        run_id="warm-evidence",
        root_rig="script",
        root_prompt="recover the interrupted job",
    )
    assert resumed.run().outcome == "ok"

    prompt = (tmp_path / "warm-evidence-prompt.txt").read_text(encoding="utf-8")
    assert "This is relaunch attempt 2 for frame f0." in prompt
    assert "Earlier attempt 1 died: crash" in prompt
    assert "worker_reaped reason=engine_resume_sweep" in prompt
    assert "--- PRIOR STDOUT (pi.stdout.log) ---" in prompt
    assert "--- PRIOR STDERR (pi.stderr.log) ---" in prompt
    assert "stdout-009" not in prompt
    assert "stderr-009" not in prompt
    assert "stdout-010" in prompt
    assert "stderr-010" in prompt
    assert "stdout-109" in prompt
    assert "stderr-109" in prompt
    assert "do not use the adapter stdout fallback" not in prompt
    assert "do not use the adapter stderr fallback" not in prompt
    assert "--- UNCOMMITTED WORKTREE DIFF ---" in prompt
    assert "-base" in prompt
    assert "+unfinished evidence" in prompt
    assert "new-evidence.txt" in prompt
    assert "+untracked finding" in prompt
    assert prompt.endswith("--- END EARLIER ATTEMPT ---")


def test_relaunch_prompt_summarizes_huge_diff_and_bounds_evidence(
    repo: Path, tmp_path: Path
) -> None:
    run_dir, interrupted, attempt_logs, rig = _interrupted_script_frame(
        repo,
        tmp_path,
        run_id="bounded-evidence",
        reaped_reason="worker_timeout",
    )
    (attempt_logs / "pi.stdout.log").write_text(
        "".join(f"out-{line:04d}-{'x' * 300}\n" for line in range(200)),
        encoding="utf-8",
    )
    (attempt_logs / "pi.stderr.log").write_text(
        "".join(f"err-{line:04d}-{'y' * 300}\n" for line in range(200)),
        encoding="utf-8",
    )
    (interrupted / "base.txt").write_text("z" * 100_000, encoding="utf-8")

    resumed = Engine(
        run_dir,
        repo,
        RigRegistry({"script": rig}),
        run_id="bounded-evidence",
        root_rig="script",
        root_prompt="recover the interrupted job",
    )
    assert resumed.run().outcome == "ok"

    prompt = (tmp_path / "bounded-evidence-prompt.txt").read_text(encoding="utf-8")
    block = prompt.split("--- EARLIER ATTEMPT ---", maxsplit=1)[1]
    assert "Earlier attempt 1 died: timeout" in block
    assert "worker_reaped reason=worker_timeout" in block
    assert "PRIOR STDOUT TRUNCATED" in block
    assert "PRIOR STDERR TRUNCATED" in block
    assert "UNCOMMITTED DIFF OMITTED" in block
    assert "git diff --stat HEAD:" in block
    assert "base.txt" in block
    assert "z" * 1_000 not in block
    assert len(block.encode("utf-8")) <= 64 * 1024
