from __future__ import annotations

import os
import threading
from pathlib import Path

import pytest

from wildflows.rig import EchoRig, RigRegistry
from wildflows.run import LifecycleLockError, Run


def _run(repo: Path, run_id: str) -> Run:
    return Run(
        workdir=repo,
        job_spec="hold the lifecycle lock",
        registry=RigRegistry({"echo": EchoRig()}),
        root_rig="echo",
        run_id=run_id,
    )


def test_concurrent_run_constructors_allow_exactly_one_owner(repo: Path) -> None:
    """Lifecycle ownership begins during construction, not when ``run`` starts."""
    start = threading.Barrier(3)
    constructed: list[Run] = []
    failures: list[BaseException] = []

    def construct() -> None:
        try:
            start.wait(timeout=5)
            constructed.append(_run(repo, "constructor-race"))
        except BaseException as exc:
            failures.append(exc)

    workers = [threading.Thread(target=construct) for _ in range(2)]
    for worker in workers:
        worker.start()
    start.wait(timeout=5)
    for worker in workers:
        worker.join(timeout=10)

    assert not [worker for worker in workers if worker.is_alive()]
    assert len(constructed) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], LifecycleLockError)


def test_locked_resume_constructor_never_repairs_live_unterminated_journal(
    repo: Path,
) -> None:
    owner = _run(repo, "live-tail")
    events = owner.run_dir / "events.ndjson"
    with events.open("ab") as stream:
        stream.write(b'{"version":2,"seq":1')
        stream.flush()
        os.fsync(stream.fileno())

    before_bytes = events.read_bytes()
    before_mtime_ns = events.stat().st_mtime_ns

    with pytest.raises(LifecycleLockError):
        _run(repo, "live-tail")

    assert events.read_bytes() == before_bytes
    assert events.stat().st_mtime_ns == before_mtime_ns
    assert not before_bytes.endswith(b"\n")
