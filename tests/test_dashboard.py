"""Dashboard projections, SSE, containment, controls, and frontend tree logic."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import time
from typing import Any, cast

from fastapi import HTTPException, Request
from fastapi.testclient import TestClient
import pytest

from tests.test_engine import init_repo
from wildflows.dashboard.app import Dashboard, _tail_events, create_app
from wildflows.events import Asked
from wildflows.journal import Journal
from wildflows.planner import AwaitingOwner
from wildflows.projection import NodeProjection
from wildflows.result import Result
from wildflows.rigconfig import load_rigs
from wildflows.run import Run

TOKEN = "dashboard-test-token"


def _script(path: Path, body: str) -> None:
    path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _run_files(repo: Path, mode: str) -> tuple[Path, Path]:
    job = repo / "job.md"
    job.write_text(f"dashboard {mode} job", encoding="utf-8")
    calls = repo / "planner-calls"
    planner = repo / "planner.sh"
    _script(planner, r'''
prompt=""; handle=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prompt) prompt="$2"; shift 2 ;;
    --handle-out) handle="$2"; shift 2 ;;
    --worktree|--log-dir|--timeout) shift 2 ;;
    *) exit 2 ;;
  esac
done
echo "$$" > "$handle"
count=$(cat "${DASH_CALLS}" 2>/dev/null || echo 0)
count=$((count + 1)); echo "$count" > "${DASH_CALLS}"
if [[ "$count" -eq 1 && "${DASH_MODE}" == "ask" ]]; then
  echo '{"expression":{"kind":"ask","question":"Ship it?","options":["yes","no"]},"rails":{"deadline_s":60,"max_epochs":3},"rationale":"owner choice","end":false,"final_summary":null}'
elif [[ "$count" -eq 1 ]]; then
  echo '{"expression":{"kind":"do","task":"produce dashboard artifact","rig":{"name":"worker"}},"rails":{"deadline_s":60,"max_epochs":3},"rationale":"build artifact","end":false,"final_summary":null}'
else
  echo '{"expression":null,"rails":{"deadline_s":60,"max_epochs":3},"rationale":"done","end":true,"final_summary":"dashboard fixture complete"}'
fi
''')
    worker = repo / "worker.sh"
    _script(worker, r'''
worktree=""; prompt=""; handle=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worktree) worktree="$2"; shift 2 ;;
    --prompt) prompt="$2"; shift 2 ;;
    --handle-out) handle="$2"; shift 2 ;;
    --log-dir|--timeout) shift 2 ;;
    *) exit 2 ;;
  esac
done
echo "$$" > "$handle"
if [[ "${DASH_MODE}" == "slow" ]]; then sleep 30; fi
printf 'rendered artifact' > "$worktree/render.txt"
echo 'worker result text'
''')
    config = repo / "rigs.yaml"
    config.write_text(
        "rigs:\n"
        "  planner:\n"
        "    kind: script\n"
        "    script: planner.sh\n"
        "    log_dir: planner-logs\n"
        "    timeout_s: 40\n"
        "    env:\n"
        f"      DASH_CALLS: {calls}\n"
        f"      DASH_MODE: {mode}\n"
        "  worker:\n"
        "    kind: script\n"
        "    script: worker.sh\n"
        "    log_dir: worker-logs\n"
        "    timeout_s: 40\n"
        "    env:\n"
        f"      DASH_MODE: {mode}\n",
        encoding="utf-8",
    )
    return job, config


def _make_repo(tmp_path: Path, mode: str) -> tuple[Path, Path, Path]:
    repo = tmp_path / "repo"
    init_repo(repo)
    job, config = _run_files(repo, mode)
    return repo, job, config


@pytest.fixture
def completed_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo, job, config = _make_repo(tmp_path, "complete")
    Run(
        workdir=repo,
        job_spec=job,
        registry=load_rigs(config),
        planner_rig="planner",
        run_id="completed-run",
    ).run()
    return repo, repo / ".wildflows" / "runs" / "completed-run"


def _json(client_call: Callable[[], Any]) -> dict[str, Any]:
    response = client_call()
    assert response.status_code < 400, response.text
    value = response.json()
    assert isinstance(value, dict)
    return cast(dict[str, Any], value)


def _wait_action(client: TestClient, action_id: str, timeout: float = 8) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = _json(lambda: client.get(f"/api/actions/{action_id}"))
        if value["state"] == "finished":
            return value
        time.sleep(0.05)
    raise AssertionError(f"action {action_id} did not finish")


def test_list_detail_and_node_projection_from_real_completed_run(
    completed_repo: tuple[Path, Path],
) -> None:
    repo, _ = completed_repo
    client = TestClient(create_app(repo, TOKEN))

    listing = _json(lambda: client.get("/api/runs"))
    assert listing["runs"][0]["run_id"] == "completed-run"
    detail = _json(lambda: client.get("/api/runs/completed-run"))
    assert detail["state"] == "completed"
    assert detail["epoch_count"] == 1
    assert detail["rationale"] == "build artifact"
    node = detail["nodes"]["n0"]
    assert node["task"] == "produce dashboard artifact"
    assert node["rig"] == "worker"
    assert node["state"] == "integrated"
    assert node["result"]["text"].strip() == "worker result text"
    assert node["receipts"][0]["paths"] == ["render.txt"]
    assert node["artifacts"]
    artifact = client.get(node["artifacts"][0]["url"])
    assert artifact.status_code == 200
    assert artifact.json()["text"].strip() == "worker result text"
    assert client.get("/").status_code == 200


def test_new_dispatch_overrides_an_older_integrated_result_state() -> None:
    node = NodeProjection(
        result=Result(text="old"), result_seq=3, receipt_required=False,
        last_dispatch_seq=4,
    )
    assert Dashboard._node_state(node) == "running"


def test_sse_tailer_sees_an_event_appended_after_subscription(
    completed_repo: tuple[Path, Path],
) -> None:
    _, run_dir = completed_repo
    journal = Journal.load(run_dir)
    after = journal.events()[-1].seq

    class Connected:
        async def is_disconnected(self) -> bool:
            return False

    async def scenario() -> str:
        request = cast(Request, Connected())
        stream = _tail_events(request, run_dir / "events.ndjson", after)
        pending: asyncio.Future[str] = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0.05)
        journal.append(Asked(
            run_id="completed-run", epoch=1, node_id="n0",
            question="tail visible?",
        ))
        value = await asyncio.wait_for(pending, timeout=2)
        await stream.aclose()
        return value

    message = asyncio.run(scenario())
    assert "event: journal" in message
    assert '"question":"tail visible?"' in message


def test_file_containment_rejects_dotdot_and_symlink_escape(
    completed_repo: tuple[Path, Path], tmp_path: Path,
) -> None:
    repo, run_dir = completed_repo
    dashboard = Dashboard(repo)
    with pytest.raises(HTTPException) as escaped:
        dashboard.public_file(run_dir, "artifacts/../../job.md")
    assert escaped.value.status_code == 404

    outside = tmp_path / "owner-secret"
    outside.write_text("do not serve", encoding="utf-8")
    link = run_dir / "artifacts" / "escape"
    link.symlink_to(outside)
    client = TestClient(create_app(repo, TOKEN))
    assert client.get("/api/runs/completed-run/files/artifacts/escape").status_code == 404
    svg = run_dir / "artifacts" / "active.svg"
    svg.write_text("<svg xmlns='http://www.w3.org/2000/svg'><script/></svg>", encoding="utf-8")
    response = client.get("/api/runs/completed-run/files/artifacts/active.svg")
    assert response.status_code == 200
    assert response.headers["content-security-policy"].startswith("sandbox;")


def test_mutating_controls_reject_missing_and_bad_tokens(
    completed_repo: tuple[Path, Path],
) -> None:
    repo, _ = completed_repo
    client = TestClient(create_app(repo, TOKEN))
    url = "/api/runs/completed-run/resume"
    assert client.post(url, json={}).status_code == 403
    assert client.post(url, json={}, headers={"X-Wildflows-Token": "wrong"}).status_code == 403


def test_answer_action_resumes_parked_scripted_run_to_completion(tmp_path: Path) -> None:
    repo, job, config = _make_repo(tmp_path, "ask")
    run = Run(
        workdir=repo,
        job_spec=job,
        registry=load_rigs(config),
        planner_rig="planner",
        run_id="ask-run",
    )
    with pytest.raises(AwaitingOwner):
        run.run()
    client = TestClient(create_app(repo, TOKEN))
    parked = _json(lambda: client.get("/api/runs/ask-run"))
    assert parked["state"] == "parked"

    launched = _json(lambda: client.post(
        "/api/runs/ask-run/answer",
        json={"answer": "yes", "node_id": "n0"},
        headers={"X-Wildflows-Token": TOKEN},
    ))
    action = _wait_action(client, launched["action_id"])
    assert action["returncode"] == 0, action["log"]
    detail = _json(lambda: client.get("/api/runs/ask-run"))
    assert detail["state"] == "completed"
    events = Journal.load(repo / ".wildflows" / "runs" / "ask-run").events()
    assert [(event.kind, event.node_id) for event in events if event.kind == "answered"] == [
        ("answered", "n0")
    ]


def test_kill_terminates_slow_scripted_run_and_leaves_resumable_tail(
    tmp_path: Path,
) -> None:
    repo, _, _ = _make_repo(tmp_path, "slow")
    client = TestClient(create_app(repo, TOKEN))
    launched = _json(lambda: client.post(
        "/api/runs",
        json={"job": "job.md", "rigs": "rigs.yaml", "run_id": "slow-run"},
        headers={"X-Wildflows-Token": TOKEN},
    ))
    deadline = time.monotonic() + 8
    detail: dict[str, Any] = {}
    while time.monotonic() < deadline:
        detail = _json(lambda: client.get("/api/runs/slow-run"))
        dispatched = [
            event for event in detail["events"]
            if event["kind"] == "dispatched" and event["node_id"] == "n0"
        ]
        if detail["active"] and dispatched:
            break
        time.sleep(0.05)
    else:
        raise AssertionError(f"slow run did not dispatch: {detail}")

    killed = _json(lambda: client.post(
        "/api/runs/slow-run/kill",
        json={},
        headers={"X-Wildflows-Token": TOKEN},
    ))
    action = _wait_action(client, launched["action_id"])
    assert action["returncode"] != 0
    with pytest.raises(ProcessLookupError):
        os.kill(killed["pid"], 0)

    run_dir = repo / ".wildflows" / "runs" / "slow-run"
    projection = Journal.load(run_dir).projection
    assert projection.epoch_opened(0) and not projection.epoch_closed(0)
    node = projection.node((0, "n0"))
    assert node.last_dispatch_seq > node.result_seq
    assert _json(lambda: client.get("/api/runs/slow-run"))["state"] == "crashed"


def test_frontend_declares_binding_light_and_dark_theme_tokens() -> None:
    root = Path(__file__).resolve().parents[1]
    index = (root / "wildflows/dashboard/static/index.html").read_text(encoding="utf-8")
    light = {
        "--ground": "#fbfcfd", "--surface": "#ffffff", "--hairline": "#e6e8eb",
        "--ink": "#17181a", "--muted": "#6e7378", "--violet-chip": "#e4e1ff",
        "--violet": "#514bd8", "--blue-chip": "#dcecff", "--blue": "#1768b0",
        "--teal-chip": "#d7f3ed", "--teal": "#006f64", "--amber-chip": "#ffebc7",
        "--amber": "#965400", "--failure": "#c74652", "--success": "#2e9b5f",
        "--dot-grid": "#dfdef2",
    }
    dark = {
        "--ground": "#101114", "--surface": "#17181b", "--hairline": "#26282c",
        "--ink": "#e8eaed", "--muted": "#9aa0a6", "--violet-chip": "#302c68",
        "--violet": "#aaa7ff", "--blue-chip": "#173c65", "--blue": "#7fb9f4",
        "--teal-chip": "#123f3a", "--teal": "#65d3c2", "--amber-chip": "#503716",
        "--amber": "#f0b45f", "--failure": "#c76f75", "--success": "#55a977",
        "--dot-grid": "#2a2939",
    }
    assert '<style id="theme-tokens">' in index
    assert '[data-theme="dark"]' in index
    assert "@media (prefers-color-scheme: dark)" in index
    for token, value in light.items():
        assert f"{token}: {value};" in index
    for token, value in dark.items():
        assert f"{token}: {value};" in index


def test_frontend_tree_builder_is_pure_and_aggregates_state() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    script = r'''
import { buildTree, phaseLayout } from "./wildflows/dashboard/static/tree.js";
const expr = {kind:"combine",node_id:"n0",task:"judge",rig:{name:"senior"},inputs:[
  {kind:"dispatch",node_id:"n0.0",children:[
    {kind:"do",node_id:"n0.0.0",task:"done",rig:{name:"x"}},
    {kind:"ask",node_id:"n0.0.1",question:"choose"}
  ]}
]};
const tree = buildTree(expr,{"n0.0.0":{state:"integrated"},"n0.0.1":{state:"parked-ask"}});
const layout = phaseLayout(tree);
if (tree.state !== "parked-ask" || tree.children[0].children[0].label !== "done") process.exit(2);
if (JSON.stringify(layout.lanes.map(lane => lane.nodes.map(item => item.id))) !== JSON.stringify([["n0.0"],["n0.0.0","n0.0.1"],["n0"]])) process.exit(3);
console.log(JSON.stringify({tree,layout}));
'''
    root = Path(__file__).resolve().parents[1]
    syntax = subprocess.run(
        [node, "--check", "wildflows/dashboard/static/app.js"],
        cwd=root, capture_output=True, text=True, check=False,
    )
    assert syntax.returncode == 0, syntax.stderr
    result = subprocess.run(
        [node, "--input-type=module", "-e", script],
        cwd=root, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["tree"]["children"][0]["children"][1]["state"] == "parked-ask"
