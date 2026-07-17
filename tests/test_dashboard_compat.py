"""Forward-compatibility and engine/dashboard schema coverage contracts."""
from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from fastapi.testclient import TestClient
from httpx import Response

from wildflows.admission import AdmissionPolicy
from wildflows.dashboard.app import create_app
from wildflows.dashboard.journal import (
    DASHBOARD_EVENT_DISPOSITIONS,
    DashboardEventDisposition,
)
from wildflows.dashboard.model import DashboardModel
from wildflows.events import (
    EVENT_TYPES_BY_KIND,
    Answered,
    Asked,
    CallFailed,
    CallRefused,
    DispatchCalled,
    DispatchReturned,
    Event,
    FrameCommitWarning,
    FrameExited,
    FrameIntegrated,
    FrameIntegrating,
    FramePopped,
    FramePushed,
    FrameRelaunchBlocked,
    FrameSlotAcquired,
    FrameSlotQueued,
    FrameSlotReleased,
    GateCalled,
    GateReturned,
    RunFinished,
    RunInterrupted,
    RunOpened,
    WorkerReaped,
    WorktreeProvisioned,
    event_kind,
)
from wildflows.frame import (
    AskRequest,
    DispatchRequest,
    DispatchResult,
    GateRequest,
    GateResult,
    ToolFailure,
)


_FIXTURE_JOURNAL = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "dashboard-fixture"
    / ".wildflows"
    / "runs"
    / "frame-stack-demo"
    / "events.ndjson"
)
_RUN_ID = "frame-stack-demo"


def _object(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _objects(value: object) -> list[dict[str, object]]:
    assert isinstance(value, list)
    return [_object(item) for item in value]


def _payload(response: Response) -> dict[str, object]:
    assert response.status_code == 200
    return _object(response.json())


def _write_records(repo: Path, records: list[dict[str, object]]) -> Path:
    run_dir = repo / ".wildflows" / "runs" / _RUN_ID
    run_dir.mkdir(parents=True)
    journal = run_dir / "events.ndjson"
    journal.write_text(
        "".join(
            json.dumps(record, separators=(",", ":")) + "\n" for record in records
        ),
        encoding="utf-8",
    )
    return run_dir


def _fixture_records() -> list[dict[str, object]]:
    return [
        _object(json.loads(line))
        for line in _FIXTURE_JOURNAL.read_text(encoding="utf-8").splitlines()
    ]


def _run_url(client: TestClient) -> str:
    runs = _objects(_payload(client.get("/api/runs"))["runs"])
    assert len(runs) == 1
    run = runs[0]
    return f"/api/repos/{run['repo_id']}/runs/{run['run_id']}"


def test_newer_journal_degrades_visibly_without_hiding_understood_state(
    tmp_path: Path,
) -> None:
    records = _fixture_records()
    records[0]["future_engine_field"] = {"safe": True}
    records[1]["version"] = 3
    records.insert(2, {
        "version": 2,
        "seq": -1,
        "ts": 1_720_000_000.5,
        "run_id": _RUN_ID,
        "kind": "future_quantum_checkpoint",
        "payload": {"opaque": True},
    })
    for seq, record in enumerate(records):
        record["seq"] = seq
    repo = tmp_path / "repo"
    _write_records(repo, records)
    client = TestClient(create_app(repo))

    listing = _objects(_payload(client.get("/api/runs"))["runs"])
    assert len(listing) == 1
    assert listing[0]["state"] == "parked"
    assert listing[0]["frames"] == 9
    assert listing[0]["event_count"] == 51
    assert listing[0]["understood_event_count"] == 50
    assert listing[0]["not_understood_count"] == 2
    assert listing[0]["newer_journal_versions"] == [3]

    detail = _payload(client.get(_run_url(client)))
    events = _objects(detail["events"])
    frames = _object(detail["frames"])
    assert detail["state"] == "parked"
    assert detail["event_count"] == 51
    assert detail["understood_event_count"] == 50
    assert detail["not_understood_count"] == 2
    assert detail["newer_journal_versions"] == [3]
    assert detail["last_event_seq"] == 50
    assert len(events) == 50
    assert "future_quantum_checkpoint" not in {event["kind"] for event in events}
    assert "future_engine_field" not in events[0]
    assert events[1]["version"] == 2
    assert "f0" in frames
    assert "f0.c0.t0.c0.t0.c0.t0.c0.t0" in frames

    javascript = client.get("/static/app.js").text
    assert "not understood" in javascript
    assert "dashboard may be out of date" in javascript


def test_malformed_known_event_counts_as_not_understood_and_replay_continues(
    tmp_path: Path,
) -> None:
    records = _fixture_records()
    malformed = next(record for record in records if record["kind"] == "gate_returned")
    malformed.pop("result")
    repo = tmp_path / "repo"
    _write_records(repo, records)
    client = TestClient(create_app(repo))

    listing = _objects(_payload(client.get("/api/runs"))["runs"])
    assert listing[0]["state"] == "parked"
    assert listing[0]["not_understood_count"] == 1
    detail = _payload(client.get(_run_url(client)))
    assert detail["not_understood_count"] == 1
    assert detail["understood_event_count"] == 49
    assert len(_objects(detail["events"])) == 49
    assert len(_object(detail["frames"])) == 9


def test_wholly_newer_journal_version_is_one_visible_compatibility_issue(
    tmp_path: Path,
) -> None:
    records = _fixture_records()
    for record in records:
        record["version"] = 3
    repo = tmp_path / "repo"
    _write_records(repo, records)
    client = TestClient(create_app(repo))

    listing = _objects(_payload(client.get("/api/runs"))["runs"])
    assert listing[0]["state"] == "parked"
    assert listing[0]["not_understood_count"] == 1
    detail = _payload(client.get(_run_url(client)))
    assert detail["not_understood_count"] == 1
    assert detail["newer_journal_versions"] == [3]
    assert detail["understood_event_count"] == 50
    assert len(_object(detail["frames"])) == 9


def _every_dashboard_event() -> list[Event]:
    run_id = "schema-coverage"
    frame_id = "f0"
    dispatch = DispatchRequest(tasks=["child"])
    gate = GateRequest(cmd="true")
    ask = AskRequest(question="continue?")
    events: list[Event] = [
        RunOpened(
            run_id=run_id,
            repository="/repo",
            run_branch="wildflows/run",
            base_commit="a" * 40,
            root_frame_id=frame_id,
            root_rig="echo",
            root_prompt="cover the dashboard schema",
            worktrees_root="/worktrees",
            started_at=1.0,
            policy=AdmissionPolicy(),
        ),
        FramePushed(
            run_id=run_id,
            frame_id=frame_id,
            attempt=1,
            depth=0,
            rig="echo",
            prompt="root",
            branch="wildflows/frame",
            base_commit="a" * 40,
            worktree="/worktrees/f0",
            subtree_deadline=100.0,
        ),
        WorktreeProvisioned(
            run_id=run_id,
            frame_id=frame_id,
            attempt=1,
            worktree="/worktrees/f0",
            mechanism="setup",
            duration_s=0.1,
            outcome="ok",
        ),
        FrameSlotQueued(run_id=run_id, frame_id=frame_id, attempt=1, rig="echo"),
        FrameSlotAcquired(
            run_id=run_id, frame_id=frame_id, attempt=1, rig="echo", slot=0
        ),
        FrameSlotReleased(
            run_id=run_id,
            frame_id=frame_id,
            attempt=1,
            rig="echo",
            slot=0,
            active_s=0.5,
            reason="dispatch",
        ),
        DispatchCalled(
            run_id=run_id,
            frame_id=frame_id,
            call_index=0,
            call_hash="dispatch-hash",
            request=dispatch,
            caller_head="a" * 40,
        ),
        DispatchReturned(
            run_id=run_id,
            frame_id=frame_id,
            call_index=0,
            call_hash="dispatch-hash",
            result=DispatchResult(outcome="ok"),
        ),
        GateCalled(
            run_id=run_id,
            frame_id=frame_id,
            call_index=1,
            call_hash="gate-hash",
            request=gate,
            caller_head="a" * 40,
        ),
        GateReturned(
            run_id=run_id,
            frame_id=frame_id,
            call_index=1,
            call_hash="gate-hash",
            result=GateResult(exit_code=0, stdout="ok\n", stderr=""),
        ),
        Asked(
            run_id=run_id,
            frame_id=frame_id,
            call_index=2,
            call_hash="ask-hash",
            request=ask,
            caller_head="a" * 40,
        ),
        Answered(
            run_id=run_id,
            frame_id=frame_id,
            call_index=2,
            call_hash="ask-hash",
            answer="yes",
        ),
        CallRefused(
            run_id=run_id,
            frame_id=frame_id,
            call_index=3,
            call_hash="refused-hash",
            tool="gate",
            request=gate,
            reason="pre-effect refusal",
        ),
        CallFailed(
            run_id=run_id,
            frame_id=frame_id,
            call_index=4,
            call_hash="failed-hash",
            tool="gate",
            request=gate,
            result=ToolFailure(error_code="test", message="failed"),
        ),
        WorkerReaped(
            run_id=run_id,
            frame_id=frame_id,
            attempt=1,
            pid=10,
            process_group_id=10,
            session_id=10,
            reason="test",
            escalated=False,
        ),
        FrameRelaunchBlocked(
            run_id=run_id,
            frame_id=frame_id,
            expected_tip="a" * 40,
            found_tip="b" * 40,
            message="blocked",
        ),
        FrameCommitWarning(
            run_id=run_id,
            frame_id=frame_id,
            attempt=1,
            skipped_paths=["cache"],
            message="unsafe symlink skipped",
        ),
        FrameExited(
            run_id=run_id,
            frame_id=frame_id,
            attempt=1,
            outcome="ok",
            text="done",
            head="b" * 40,
        ),
        FrameIntegrating(
            run_id=run_id,
            frame_id=frame_id,
            integration_base="a" * 40,
            candidate_head="b" * 40,
        ),
        FrameIntegrated(
            run_id=run_id,
            frame_id=frame_id,
            integration_base="a" * 40,
            candidate_head="b" * 40,
        ),
        FramePopped(
            run_id=run_id, frame_id=frame_id, attempt=1, outcome="ok"
        ),
        RunInterrupted(run_id=run_id, reason="schema test"),
        RunFinished(
            run_id=run_id,
            outcome="ok",
            root_head="b" * 40,
            text="complete",
        ),
    ]
    return [event.model_copy(update={"seq": seq}) for seq, event in enumerate(events)]


def _dashboard_must_learn(kinds: set[str]) -> str:
    names = ", ".join(sorted(kinds))
    return f"event kind(s) {names}: the dashboard must learn them before the suite can pass"


def test_dashboard_projection_covers_every_registered_engine_event(
    tmp_path: Path,
) -> None:
    events = _every_dashboard_event()
    registered_kinds = set(EVENT_TYPES_BY_KIND)
    fixture_kinds = {event.kind for event in events}
    assert fixture_kinds == registered_kinds, _dashboard_must_learn(
        registered_kinds - fixture_kinds
    )

    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "events.ndjson").write_text(
        "".join(
            event.model_dump_json(exclude_computed_fields=True) + "\n"
            for event in events
        ),
        encoding="utf-8",
    )
    snapshot = DashboardModel.snapshot(run_dir)

    assert snapshot.not_understood_count == 0, _dashboard_must_learn(
        set(snapshot.not_understood_event_kinds)
    )
    assert set(snapshot.understood_event_kinds) == registered_kinds
    registered_dispositions = {
        event_kind(event_type): disposition
        for event_type, disposition in DASHBOARD_EVENT_DISPOSITIONS.items()
    }
    assert set(registered_dispositions) == registered_kinds, _dashboard_must_learn(
        registered_kinds - set(registered_dispositions)
    )
    projected_kinds = {event.kind for event in snapshot.projection.effective_events}
    assert projected_kinds == {
        kind
        for kind, disposition in registered_dispositions.items()
        if disposition is DashboardEventDisposition.PROJECT
    }
    assert set(snapshot.no_op_event_kinds) == {
        kind
        for kind, disposition in registered_dispositions.items()
        if disposition is DashboardEventDisposition.NO_OP
    }
