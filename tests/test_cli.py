from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_cli_rig_validation_error_is_one_clear_line_without_traceback(
    repo: Path, tmp_path: Path
) -> None:
    job = tmp_path / "job.md"
    job.write_text("Do the root job.\n", encoding="utf-8")
    rigs = tmp_path / "rigs.yaml"
    rigs.write_text(
        "kinds:\n  review: local\nrigs:\n  echo:\n    kind: echo\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "wildflows",
            "run",
            str(job),
            "--repo",
            str(repo),
            "--rigs",
            str(rigs),
            "--root-rig",
            "echo",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert result.stdout == ""
    assert result.stderr.count("\n") == 1
    assert str(rigs) in result.stderr
    assert "kinds routing was removed; pass rig per task in dispatch" in result.stderr
    assert "Traceback" not in result.stderr


def test_cli_starts_root_frame_and_resume_reuses_finished_run(
    repo: Path, tmp_path: Path
) -> None:
    job = tmp_path / "job.md"
    job.write_text("Do the root job.\n", encoding="utf-8")
    rigs = tmp_path / "rigs.yaml"
    rigs.write_text("rigs:\n  echo:\n    kind: echo\n", encoding="utf-8")
    base = [
        sys.executable,
        "-m",
        "wildflows",
        "run",
        str(job),
        "--repo",
        str(repo),
        "--rigs",
        str(rigs),
        "--root-rig",
        "echo",
        "--run-id",
        "cli-test",
        "--notify",
        "true",
    ]
    first = subprocess.run(base, capture_output=True, text=True, check=True)
    payload = json.loads(first.stdout.splitlines()[-1])
    assert payload["outcome"] == "ok"
    assert payload["frames"] == 1
    journal = repo / ".wildflows" / "runs" / "cli-test" / "events.ndjson"
    assert journal.is_file()
    assert all(json.loads(line)["version"] == 2 for line in journal.read_text().splitlines())

    resumed = subprocess.run(
        [
            *base[:3],
            "resume",
            *base[4:],
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(resumed.stdout.splitlines()[-1])["outcome"] == "ok"
