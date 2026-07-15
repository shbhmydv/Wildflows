from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

from tests.conftest import executable


def _wait_for_file(path: Path, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {path}")


def _finish(process: subprocess.Popen[str]) -> None:
    stdout, stderr = process.communicate(timeout=3)
    assert process.returncode == 0, (stdout, stderr)


def _kill_group(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=3)


def _start_worker(
    root: Path,
    name: str,
    bin_dir: Path,
    extension: Path,
    lock_dir: Path,
    provider_out: Path,
    *,
    release_file: Path | None = None,
    provider_override: str | None = None,
) -> subprocess.Popen[str]:
    worktree = root / f"{name}-worktree"
    worktree.mkdir()
    prompt = root / f"{name}.prompt"
    prompt.write_text("test prompt", encoding="utf-8")
    env = os.environ.copy()
    for key in (
        "GRINDSTONE_SENIOR_PROVIDER",
        "GRINDSTONE_SENIOR_MODEL",
        "GRINDSTONE_SENIOR_EFFORT",
    ):
        env.pop(key, None)
    env.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{env['PATH']}",
            "WILDFLOWS_PI_EXTENSION": str(extension),
            "WILDFLOWS_PIN_LOCK_DIR": str(lock_dir),
            "PI_PROVIDER_OUT": str(provider_out),
            "PI_ARGS_OUT": str(provider_out.with_suffix(".args")),
            "PI_SLEEP": "0.05",
        }
    )
    if release_file is not None:
        env["PI_RELEASE_FILE"] = str(release_file)
    if provider_override is not None:
        env["GRINDSTONE_SENIOR_PROVIDER"] = provider_override
    return subprocess.Popen(
        [
            str(Path("rigs/worker-local.sh").resolve()),
            "--worktree",
            str(worktree),
            "--prompt",
            str(prompt),
            "--log-dir",
            str(root / f"{name}-logs"),
            "--handle-out",
            str(root / f"{name}.handle"),
            "--timeout",
            "10",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        start_new_session=True,
    )


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
if [[ -n "${PI_RELEASE_FILE:-}" ]]; then
  while [[ ! -e "$PI_RELEASE_FILE" ]]; do sleep 0.02; done
else
  sleep "${PI_SLEEP:-0.05}"
fi
""",
    )
    extension = root / "extension.ts"
    extension.write_text("extension", encoding="utf-8")
    return bin_dir, extension


def test_two_local_workers_pin_distinct_backends(tmp_path: Path) -> None:
    bin_dir, extension = _make_pi_stub(tmp_path)
    lock_dir = tmp_path / "locks"
    first_out = tmp_path / "first.provider"
    second_out = tmp_path / "second.provider"
    first_release = tmp_path / "first.release"
    second_release = tmp_path / "second.release"
    processes: list[subprocess.Popen[str]] = []
    try:
        processes = [
            _start_worker(
                tmp_path,
                "first",
                bin_dir,
                extension,
                lock_dir,
                first_out,
                release_file=first_release,
            ),
            _start_worker(
                tmp_path,
                "second",
                bin_dir,
                extension,
                lock_dir,
                second_out,
                release_file=second_release,
            ),
        ]
        _wait_for_file(first_out)
        _wait_for_file(second_out)
        assert {first_out.read_text().strip(), second_out.read_text().strip()} == {
            "local-reviewer-8081",
            "local-reviewer-8082",
        }
        first_release.touch()
        second_release.touch()
        for process in processes:
            _finish(process)
    finally:
        for process in processes:
            _kill_group(process)


def test_third_local_worker_waits_then_reuses_freed_backend(tmp_path: Path) -> None:
    bin_dir, extension = _make_pi_stub(tmp_path)
    lock_dir = tmp_path / "locks"
    outputs = [tmp_path / f"worker-{index}.provider" for index in range(3)]
    releases = [tmp_path / f"worker-{index}.release" for index in range(3)]
    processes: list[subprocess.Popen[str]] = []
    try:
        for index in range(2):
            processes.append(
                _start_worker(
                    tmp_path,
                    f"worker-{index}",
                    bin_dir,
                    extension,
                    lock_dir,
                    outputs[index],
                    release_file=releases[index],
                )
            )
            _wait_for_file(outputs[index])

        third = _start_worker(
            tmp_path,
            "worker-2",
            bin_dir,
            extension,
            lock_dir,
            outputs[2],
            release_file=releases[2],
        )
        processes.append(third)
        time.sleep(0.15)
        assert third.poll() is None
        assert not outputs[2].exists()

        freed_provider = outputs[0].read_text(encoding="utf-8")
        releases[0].touch()
        _finish(processes[0])
        _wait_for_file(outputs[2])
        assert outputs[2].read_text(encoding="utf-8") == freed_provider

        releases[1].touch()
        releases[2].touch()
        _finish(processes[1])
        _finish(processes[2])
    finally:
        for release in releases:
            release.touch(exist_ok=True)
        for process in processes:
            _kill_group(process)


def test_sigkill_releases_local_worker_lock(tmp_path: Path) -> None:
    bin_dir, extension = _make_pi_stub(tmp_path)
    lock_dir = tmp_path / "locks"
    killed_out = tmp_path / "killed.provider"
    killed_release = tmp_path / "killed.release"
    killed = _start_worker(
        tmp_path,
        "killed",
        bin_dir,
        extension,
        lock_dir,
        killed_out,
        release_file=killed_release,
    )
    follow: subprocess.Popen[str] | None = None
    try:
        _wait_for_file(killed_out)
        killed.kill()  # Kill only the wrapper; its still-running pi child must not own the fd.
        killed.wait(timeout=3)

        follow_out = tmp_path / "follow.provider"
        follow = _start_worker(
            tmp_path,
            "follow",
            bin_dir,
            extension,
            lock_dir,
            follow_out,
        )
        _wait_for_file(follow_out, timeout=1)
        assert follow_out.read_text(encoding="utf-8") == killed_out.read_text(
            encoding="utf-8"
        )
        _finish(follow)
    finally:
        # The killed wrapper's orphaned stub deliberately remains in its old group.
        try:
            os.killpg(killed.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if follow is not None:
            _kill_group(follow)


def test_provider_override_skips_local_backend_pinning(tmp_path: Path) -> None:
    bin_dir, extension = _make_pi_stub(tmp_path)
    lock_dir = tmp_path / "locks-that-must-not-exist"
    provider_out = tmp_path / "override.provider"
    process = _start_worker(
        tmp_path,
        "override",
        bin_dir,
        extension,
        lock_dir,
        provider_out,
        provider_override="operator-provider",
    )
    try:
        _finish(process)
        assert provider_out.read_text(encoding="utf-8").strip() == "operator-provider"
        assert not lock_dir.exists()
        args = provider_out.with_suffix(".args").read_text(encoding="utf-8")
        assert "--model qwen-3-6-27b-dense" in args
        assert "--thinking medium" in args
    finally:
        _kill_group(process)
