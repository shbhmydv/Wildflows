from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from wildflows.admission import AdmissionPolicy
from wildflows.engine import Engine
from wildflows.events import RunFinished, RunOpened
from wildflows.journal import IncompatibleJournalError, Journal
from wildflows.rig import EchoRig, RigRegistry


def _opened(run_id: str = "run") -> RunOpened:
    return RunOpened(
        run_id=run_id,
        repository="/repo",
        run_branch="main",
        base_commit="a" * 40,
        root_frame_id="f0",
        root_rig="fake",
        root_prompt="job",
        worktrees_root="/tmp/worktrees",
        started_at=1.0,
        policy=AdmissionPolicy(),
    )


@pytest.mark.parametrize("version", [None, True, 0, 1, 3])
def test_v2_load_refuses_other_or_missing_versions(
    tmp_path: Path, version: object
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    record: dict[str, object] = {
        "seq": 0,
        "ts": 1,
        "run_id": "old",
        "kind": "run_finished",
        "outcome": "ok",
        "root_head": "deadbeef",
        "text": "old",
    }
    if version is not None:
        record["version"] = version
    (run_dir / "events.ndjson").write_text(
        json.dumps(record) + "\n", encoding="utf-8"
    )
    with pytest.raises(IncompatibleJournalError, match="requires v2"):
        Journal.load(run_dir)


def test_v1_event_shape_is_not_accepted_when_falsely_stamped_v2(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "events.ndjson").write_text(
        json.dumps({
            "version": 2,
            "seq": 0,
            "ts": 1,
            "run_id": "old",
            "epoch": 0,
            "node_id": "n0",
            "kind": "boundary",
            "phase": "opened",
        }) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        Journal.load(run_dir)


def test_load_durably_drops_any_unterminated_tail(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "run")
    journal.append(_opened())
    path = journal.path
    with path.open("ab") as stream:
        stream.write(b'{"version":2,"seq":1')
    loaded = Journal.load(tmp_path / "run")
    assert loaded.n_events == 1
    assert path.read_bytes().endswith(b"\n")


def test_engine_treats_torn_first_run_opened_as_no_durable_run(
    repo: Path, tmp_path: Path
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "events.ndjson").write_bytes(b'{"version":2,"seq":0')
    engine = Engine(
        run_dir,
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id="fresh-after-tear",
        root_rig="echo",
        root_prompt="job",
        worktrees_root=tmp_path / "worktrees",
    )
    assert engine.projection.opened is not None
    assert engine.projection.opened.run_id == "fresh-after-tear"
    assert engine.journal.events()[0].seq == 0


def test_parallel_append_owner_assigns_contiguous_sequences(tmp_path: Path) -> None:
    journal = Journal(tmp_path / "run")
    journal.append(_opened())

    def append(index: int) -> None:
        journal.append(RunFinished(
            run_id="run",
            outcome="ok",
            root_head=str(index),
            text=str(index),
        ))

    threads = [threading.Thread(target=append, args=(index,)) for index in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert [event.seq for event in journal.events()] == list(range(21))
    assert [event.seq for event in Journal.load(tmp_path / "run").events()] == list(range(21))
