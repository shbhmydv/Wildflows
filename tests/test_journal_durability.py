"""Durability regressions: torn tails, poisoning, and sequence integrity."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal

import pytest

from wildflows.events import Boundary
from wildflows.journal import (
    Journal,
    IncompatibleJournalError,
    JournalCompatibilityError,
    JournalExistsError,
    JournalPoisonedError,
)


def boundary(phase: Literal["opened", "closed"] = "opened") -> Boundary:
    return Boundary(run_id="run", epoch=0, node_id="n0", phase=phase)


def test_unterminated_tail_is_durably_removed_and_sequence_reused(tmp_path: Path) -> None:
    journal = Journal(tmp_path)
    journal.append(boundary())
    path = tmp_path / "events.ndjson"
    with open(path, "ab") as stream:
        stream.write(b'{"kind":"result","seq":1')

    loaded = Journal.load(tmp_path)
    assert loaded.n_events == 1
    assert path.read_bytes().endswith(b"\n")
    assert loaded.append(boundary("closed")) == 1
    again = Journal.load(tmp_path)
    assert [event.seq for event in again.events()] == [0, 1]
    assert len(path.read_text(encoding="utf-8").splitlines()) == 2


def test_complete_json_without_newline_is_still_discarded(tmp_path: Path) -> None:
    journal = Journal(tmp_path)
    journal.append(boundary())
    path = tmp_path / "events.ndjson"
    complete = boundary("closed").model_copy(update={"seq": 1}).model_dump_json()
    with open(path, "ab") as stream:
        stream.write(complete.encode("utf-8"))
    loaded = Journal.load(tmp_path)
    assert loaded.n_events == 1
    assert loaded.append(boundary("closed")) == 1


def test_complete_malformed_record_is_corruption(tmp_path: Path) -> None:
    journal = Journal(tmp_path)
    journal.append(boundary())
    with open(tmp_path / "events.ndjson", "ab") as stream:
        stream.write(b"not-json\n")
    with pytest.raises(json.JSONDecodeError):
        Journal.load(tmp_path)


def test_failed_fsync_poisons_owner_until_fresh_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = Journal(tmp_path)
    real_fsync = os.fsync

    def fail_fsync(descriptor: int) -> None:
        raise OSError("disk refused sync")

    monkeypatch.setattr(os, "fsync", fail_fsync)
    with pytest.raises(OSError, match="disk refused"):
        journal.append(boundary())
    with pytest.raises(JournalPoisonedError):
        journal.append(boundary())

    monkeypatch.setattr(os, "fsync", real_fsync)
    loaded = Journal.load(tmp_path)
    assert loaded.n_events == 1  # complete residue is adopted only by fresh load


def test_creation_refuses_to_continue_nonempty_journal(tmp_path: Path) -> None:
    Journal(tmp_path).append(boundary())
    with pytest.raises(JournalExistsError):
        Journal(tmp_path)


@pytest.mark.parametrize("version", [None, 0, 2, "1", True])
def test_load_refuses_unversioned_or_incompatible_journal(
    tmp_path: Path, version: object
) -> None:
    record: dict[str, object] = {
        "seq": 0, "kind": "boundary", "run_id": "r", "epoch": 0,
        "node_id": "n0", "phase": "opened",
    }
    if version is not None:
        record["version"] = version
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / "events.ndjson").write_text(
        json.dumps(record) + "\n", encoding="utf-8"
    )
    with pytest.raises(IncompatibleJournalError, match="requires v1"):
        Journal.load(tmp_path)


def test_load_refuses_non_object_record(tmp_path: Path) -> None:
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / "events.ndjson").write_text("[]\n", encoding="utf-8")
    with pytest.raises(IncompatibleJournalError, match="not a v1 event object"):
        Journal.load(tmp_path)


def test_load_refuses_gapped_sequence(tmp_path: Path) -> None:
    journal = Journal(tmp_path)
    journal.append(boundary())
    path = tmp_path / "events.ndjson"
    record = boundary("closed").model_copy(update={"seq": 2}).model_dump_json()
    with open(path, "a", encoding="utf-8") as stream:
        stream.write(record + "\n")
    with pytest.raises(JournalCompatibilityError, match="contiguous"):
        Journal.load(tmp_path)
