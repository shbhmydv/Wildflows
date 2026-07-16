from __future__ import annotations

import os
import subprocess
from pathlib import Path

from tests.conftest import executable


def _make_pi_stub(root: Path) -> tuple[Path, Path]:
    bin_dir = root / "bin"
    bin_dir.mkdir()
    executable(
        bin_dir / "pi",
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" > "$PI_ARGS_OUT"
provider=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --provider) provider="$2"; shift 2 ;;
    *) shift ;;
  esac
done
printf '%s\n' "$provider" > "$PI_PROVIDER_OUT"
""",
    )
    extension = root / "extension.ts"
    extension.write_text("extension", encoding="utf-8")
    return bin_dir, extension


def _run_worker(
    root: Path,
    *,
    engine_provider: str | None,
    operator_provider: str | None = None,
) -> tuple[str, str]:
    bin_dir, extension = _make_pi_stub(root)
    worktree = root / "worktree"
    worktree.mkdir()
    prompt = root / "prompt"
    prompt.write_text("test prompt", encoding="utf-8")
    provider_out = root / "provider"
    args_out = root / "args"
    env = os.environ.copy()
    for key in (
        "GRINDSTONE_SENIOR_PROVIDER",
        "GRINDSTONE_SENIOR_MODEL",
        "GRINDSTONE_SENIOR_EFFORT",
        "WILDFLOWS_PROVIDER_OVERRIDE",
    ):
        env.pop(key, None)
    env.update({
        "PATH": f"{bin_dir}{os.pathsep}{env['PATH']}",
        "WILDFLOWS_PI_EXTENSION": str(extension),
        "PI_PROVIDER_OUT": str(provider_out),
        "PI_ARGS_OUT": str(args_out),
    })
    if engine_provider is not None:
        env["WILDFLOWS_PROVIDER_OVERRIDE"] = engine_provider
    if operator_provider is not None:
        env["GRINDSTONE_SENIOR_PROVIDER"] = operator_provider
    process = subprocess.run(
        [
            str(Path("rigs/worker-local.sh").resolve()),
            "--worktree", str(worktree),
            "--prompt", str(prompt),
            "--log-dir", str(root / "logs"),
            "--handle-out", str(root / "handle"),
            "--timeout", "10",
        ],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    assert process.returncode == 0
    return (
        provider_out.read_text(encoding="utf-8").strip(),
        args_out.read_text(encoding="utf-8"),
    )


def test_engine_provider_override_reaches_local_adapter(tmp_path: Path) -> None:
    provider, args = _run_worker(
        tmp_path, engine_provider="local-reviewer-8082"
    )
    assert provider == "local-reviewer-8082"
    assert "--model qwen-3-6-27b-dense" in args
    assert "--thinking medium" in args


def test_operator_provider_override_wins_over_engine_lane(tmp_path: Path) -> None:
    provider, _ = _run_worker(
        tmp_path,
        engine_provider="local-reviewer-8082",
        operator_provider="operator-provider",
    )
    assert provider == "operator-provider"


def test_unscheduled_direct_adapter_has_deterministic_fallback(
    tmp_path: Path,
) -> None:
    provider, _ = _run_worker(tmp_path, engine_provider=None)
    assert provider == "local-reviewer-8081"
