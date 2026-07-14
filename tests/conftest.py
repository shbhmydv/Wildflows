from __future__ import annotations

import os
import subprocess
from pathlib import Path
from collections.abc import Iterator

import pytest


def git(repo: Path, *args: str) -> str:
    process = subprocess.run(
        ["git", *args], cwd=repo, capture_output=True, text=True, check=True
    )
    return process.stdout.strip()


@pytest.fixture
def repo(tmp_path: Path) -> Iterator[Path]:
    target = tmp_path / "target"
    target.mkdir()
    git(target, "init", "-b", "main")
    git(target, "config", "user.email", "tests@example.invalid")
    git(target, "config", "user.name", "Wildflows Tests")
    (target / "base.txt").write_text("base\n", encoding="utf-8")
    git(target, "add", "base.txt")
    git(target, "commit", "-m", "base")
    yield target


def executable(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    os.chmod(path, 0o755)
    return path
