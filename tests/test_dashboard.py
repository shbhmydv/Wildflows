"""Server and static-asset contract coverage for the v2 dashboard."""
from __future__ import annotations

from pathlib import Path
import re
from typing import cast

from fastapi.testclient import TestClient
from httpx import Response

from wildflows.__main__ import _parser
from wildflows.dashboard.app import DEFAULT_PORT, _event_cursor, create_app


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
_LIGHT_TOKENS = {
    "--ground": "#fbfcfd",
    "--surface": "#ffffff",
    "--hairline": "#e6e8eb",
    "--ink": "#17181a",
    "--muted": "#6e7378",
    "--violet-chip": "#e4e1ff",
    "--violet": "#514bd8",
    "--blue-chip": "#dcecff",
    "--blue": "#1768b0",
    "--teal-chip": "#d7f3ed",
    "--teal": "#006f64",
    "--amber-chip": "#ffebc7",
    "--amber": "#965400",
    "--failure": "#c74652",
    "--success": "#2e9b5f",
    "--dot-grid": "#dfdef2",
    "--shadow": "#17181a0a",
    "--transparent": "#ffffff00",
    "--result-inset": "#17181a",
    "--result-ink": "#e8eaed",
}
_DARK_TOKENS = {
    "--ground": "#101114",
    "--surface": "#17181b",
    "--hairline": "#26282c",
    "--ink": "#e8eaed",
    "--muted": "#9aa0a6",
    "--violet-chip": "#302c68",
    "--violet": "#aaa7ff",
    "--blue-chip": "#173c65",
    "--blue": "#7fb9f4",
    "--teal-chip": "#123f3a",
    "--teal": "#65d3c2",
    "--amber-chip": "#503716",
    "--amber": "#f0b45f",
    "--failure": "#c76f75",
    "--success": "#55a977",
    "--dot-grid": "#2a2939",
    "--shadow": "#00000038",
    "--transparent": "#10111400",
    "--result-inset": "#101114",
    "--result-ink": "#e8eaed",
}


def _json_object(response: Response) -> dict[str, object]:
    value: object = response.json()
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _objects(value: object) -> list[dict[str, object]]:
    assert isinstance(value, list)
    return [_object(item) for item in value]


def _object(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _text(value: object) -> str:
    assert isinstance(value, str)
    return value


def _call(frame: dict[str, object], call_index: int) -> dict[str, object]:
    for call in _objects(frame["calls"]):
        if call["call_index"] == call_index:
            return call
    raise AssertionError(f"call {call_index} was not projected")


def _write_fixture_run(repo: Path, *, tail: bytes = b"") -> Path:
    assert _FIXTURE_JOURNAL.is_file()
    run_dir = repo / ".wildflows" / "runs" / _RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "events.ndjson").write_bytes(_FIXTURE_JOURNAL.read_bytes() + tail)
    return run_dir


def _run_url(client: TestClient) -> str:
    response = client.get("/api/runs")
    assert response.status_code == 200
    payload = _json_object(response)
    runs = _objects(payload["runs"])
    assert len(runs) == 1
    run = runs[0]
    return f"/api/repos/{_text(run['repo_id'])}/runs/{_text(run['run_id'])}"


def _css_tokens(css: str, selector: str) -> dict[str, str]:
    match = re.search(
        rf"{re.escape(selector)}\s*\{{(?P<body>.*?)\}}", css, flags=re.DOTALL
    )
    assert match is not None
    return {
        name: value.strip()
        for name, value in re.findall(
            r"^\s*(--[\w-]+):\s*([^;]+);", match.group("body"), flags=re.MULTILINE
        )
    }


def test_dashboard_port_and_dash_cli_default_are_8181() -> None:
    args = _parser().parse_args(["dash"])
    watched = _parser().parse_args([
        "dash", "--repo", "one", "--repo", "two", "--watchlist", "repos.txt"
    ])

    assert DEFAULT_PORT == 8181
    assert args.port == 8181
    assert args.repo == []
    assert watched.repo == [Path("one"), Path("two")]
    assert watched.watchlist == Path("repos.txt")
    assert _event_cursor(12, None) == 12
    assert _event_cursor(12, "15") == 15


def test_app_lists_deduplicated_repositories_with_qualified_run_keys(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    _write_fixture_run(first)
    _write_fixture_run(second)

    client = TestClient(create_app([first, second, first.resolve()]))
    response = client.get("/api/runs")

    assert response.status_code == 200
    payload = _json_object(response)
    repositories = _objects(payload["repositories"])
    runs = _objects(payload["runs"])
    assert len(repositories) == 2
    assert len(runs) == 2
    assert {_text(repo["name"]) for repo in repositories} == {"first", "second"}
    repo_ids = {_text(repo["id"]) for repo in repositories}
    assert {_text(run["repo_id"]) for run in runs} == repo_ids
    assert all(run["run_id"] == _RUN_ID for run in runs)
    assert {_text(run["key"]) for run in runs} == {
        f"{_text(run['repo_id'])}:{_RUN_ID}" for run in runs
    }


def test_detail_projects_fixture_state_and_ignores_torn_tail_read_only(
    tmp_path: Path,
) -> None:
    run_dir = _write_fixture_run(tmp_path / "repo", tail=b'{"version":2,"seq":49')
    journal = run_dir / "events.ndjson"
    before = journal.read_bytes()
    client = TestClient(create_app(tmp_path / "repo"))

    detail_response = client.get(_run_url(client))

    assert detail_response.status_code == 200
    detail = _json_object(detail_response)
    events = _objects(detail["events"])
    assert detail["state"] == "parked"
    assert len(events) == 49
    assert {event["version"] for event in events} == {2}
    gate_event = next(event for event in events if event["kind"] == "gate_returned")
    assert _object(gate_event["result"]) == {
        "exit_code": 2,
        "stdout": "19 checks collected; 18 passed\n",
        "stderr": "contract snapshot differs at route /settings\n",
    }
    assert journal.read_bytes() == before

    pending_questions = _objects(detail["pending_questions"])
    assert pending_questions == [{
        "frame_id": "f0.c2.t2",
        "frame_path": "f0 › c2 › t2",
        "call_index": 0,
        "question": "May the rollout proceed with the stale ownership flag?",
    }]

    frames = _object(detail["frames"])
    root = _object(frames["f0"])
    failed = _object(frames["f0.c2.t1"])
    parked = _object(frames["f0.c2.t2"])
    depth_four = _object(frames["f0.c0.t0.c0.t0.c0.t0.c0.t0"])
    assert root["state"] == "banked"
    assert failed["state"] == "failed"
    assert failed["reason"] == "migration boundary missing"
    assert parked["state"] == "parked"
    assert depth_four["depth"] == 4
    assert depth_four["state"] == "done"

    gate = _call(root, 1)
    gate_result = _object(gate["result"])
    assert gate["tool"] == "gate"
    assert gate["status"] == "completed"
    assert gate["gate_language"] == "gate: FAIL (exit 2)"
    assert gate_result["exit_code"] == 2
    assert gate_result["stdout"] == "19 checks collected; 18 passed\n"
    assert gate_result["stderr"] == "contract snapshot differs at route /settings\n"

    fanout = _call(root, 2)
    assert fanout["tool"] == "dispatch"
    assert fanout["status"] == "pending"
    assert fanout["parallel"] is True
    assert fanout["requested"] == 20
    assert fanout["queued"] == 15
    children = fanout["children"]
    assert isinstance(children, list)
    assert {_text(value) for value in children} == {
        "f0.c2.t0",
        "f0.c2.t1",
        "f0.c2.t2",
        "f0.c2.t3",
        "f0.c2.t4",
    }
    assert _object(fanout["counts"]) == {
        "done": 2,
        "failed": 1,
        "parked": 1,
        "running": 1,
    }


def test_unfinished_run_stays_live_after_a_child_failure(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    run_dir = repo / ".wildflows" / "runs" / _RUN_ID
    run_dir.mkdir(parents=True)
    records = _FIXTURE_JOURNAL.read_bytes().splitlines(keepends=True)
    (run_dir / "events.ndjson").write_bytes(b"".join(records[:44]))
    client = TestClient(create_app(repo))

    listing = _objects(_json_object(client.get("/api/runs"))["runs"])
    assert listing[0]["state"] == "running"
    detail = _json_object(client.get(_run_url(client)))
    assert detail["state"] == "running"
    assert detail["active"] is True
    assert _object(_object(detail["frames"])["f0.c2.t1"])["state"] == "failed"


def test_artifacts_are_listed_and_contained_inside_the_run(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    run_dir = _write_fixture_run(repo)
    (run_dir / "report.txt").write_text("public evidence\n", encoding="utf-8")
    (run_dir / "nested").mkdir()
    (run_dir / "nested" / "detail.json").write_text("{}\n", encoding="utf-8")
    (run_dir / "answers").mkdir()
    (run_dir / "answers" / "owner.txt").write_text("private\n", encoding="utf-8")
    private = tmp_path / "private.txt"
    private.write_text("outside\n", encoding="utf-8")
    (run_dir / "outside.txt").symlink_to(private)
    client = TestClient(create_app(repo))
    base = _run_url(client)

    detail_response = client.get(base)
    assert detail_response.status_code == 200
    artifacts = _objects(_json_object(detail_response)["artifacts"])
    assert {artifact["path"] for artifact in artifacts} == {
        "nested/detail.json",
        "report.txt",
    }
    report = next(artifact for artifact in artifacts if artifact["path"] == "report.txt")
    report_response = client.get(_text(report["url"]))
    assert report_response.status_code == 200
    assert report_response.text == "public evidence\n"

    assert client.get(f"{base}/artifacts/events.ndjson").status_code == 404
    assert client.get(f"{base}/artifacts/answers/owner.txt").status_code == 404
    assert client.get(f"{base}/artifacts/outside.txt").status_code == 404
    assert client.get(f"{base}/artifacts/%2e%2e/%2e%2e/private.txt").status_code == 404


def test_static_assets_are_local_and_keep_exact_theme_tokens(tmp_path: Path) -> None:
    client = TestClient(create_app(tmp_path))
    index_response = client.get("/")
    css_response = client.get("/static/style.css")
    js_response = client.get("/static/app.js")

    assert index_response.status_code == 200
    assert css_response.status_code == 200
    assert js_response.status_code == 200
    index = index_response.text
    css = css_response.text
    javascript = js_response.text
    assert 'href="/static/style.css"' in index
    assert 'src="/static/app.js"' in index
    for asset in (index, css, javascript):
        assert "cdn" not in asset.lower()
        assert "http://" not in asset.lower()
        assert "https://" not in asset.lower()

    light = _css_tokens(css, ":root")
    dark = _css_tokens(css, '[data-theme="dark"]')
    automatic_dark = _css_tokens(css, ":root:not([data-theme])")
    assert set(light) == {*_LIGHT_TOKENS, "--mono", "--sans"}
    assert {name: light[name] for name in _LIGHT_TOKENS} == _LIGHT_TOKENS
    assert dark == _DARK_TOKENS
    assert automatic_dark == _DARK_TOKENS
    assert "border-left" not in css
    assert "const FRAME_COLUMN_MIN = 260;" in javascript
    assert (
        'const node = el("section", `frame-node frame-card ${frame.state}'
        '${collapsed ? " collapsed-container" : ""}`);'
    ) in javascript
    assert 'node.append(calls);' in javascript
    assert "if (collapsed) return node;" in javascript
    assert ".canvas { min-height: 0; overflow-x: auto; overflow-y: auto;" in css
    assert (
        "grid-template-columns: repeat(var(--call-columns), "
        "minmax(260px, 1fr));"
    ) in css
    assert ".frame-card.collapsed-container { padding: 0; }" in css
    assert ".ask-card { position: absolute" not in css
