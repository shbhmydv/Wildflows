"""The journal: full typed event vocabulary, in-memory list + ndjson, reloadable."""
from __future__ import annotations

from pathlib import Path

from wildflows.events import (
    Answered,
    Asked,
    Boundary,
    Dispatched,
    Integrated,
    Event,
    Judged,
    LoopIter,
    ResultEvent,
    parse_event,
)
from wildflows.journal import Journal
from wildflows.result import CommitReceipt


def test_append_assigns_increasing_seq(tmp_path: Path) -> None:
    j = Journal(tmp_path)
    s0 = j.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="opened"))
    s1 = j.append(Dispatched(run_id="r", epoch=0, node_id="n0", rig="echo", task="t"))
    assert (s0, s1) == (0, 1)
    assert len(j.events()) == 2


def test_all_event_types_roundtrip_through_ndjson(tmp_path: Path) -> None:
    j = Journal(tmp_path)
    events: list[Event] = [
        Boundary(run_id="r", epoch=0, node_id="n0", phase="opened"),
        Dispatched(run_id="r", epoch=0, node_id="n0.0", rig="echo", task="t"),
        ResultEvent(run_id="r", epoch=0, node_id="n0.0", text="done"),
        Integrated(run_id="r", epoch=0, node_id="n0.0",
                   commits=[CommitReceipt(sha="abc", paths=["a.txt"])]),
        Judged(run_id="r", epoch=0, node_id="n0.1", verdict="pass", ok=True, target_node="n0.0"),
        LoopIter(run_id="r", epoch=0, node_id="n0", iteration=0, commit="abc", converged=True),
        Asked(run_id="r", epoch=0, node_id="n0.2", question="which?"),
        Answered(run_id="r", epoch=0, node_id="n0.2", answer="left", ok=True),
        Boundary(run_id="r", epoch=0, node_id="n0", phase="closed"),
    ]
    for e in events:
        j.append(e)

    reloaded = Journal.load(tmp_path)
    assert len(reloaded.events()) == len(events)
    # types survive the ndjson round-trip
    kinds = [e.kind for e in reloaded.events()]
    assert kinds == [e.kind for e in events]
    assert isinstance(reloaded.events()[0], Boundary)


def test_ndjson_is_line_per_event(tmp_path: Path) -> None:
    j = Journal(tmp_path)
    j.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="opened"))
    j.append(Boundary(run_id="r", epoch=0, node_id="n0", phase="closed"))
    text = (tmp_path / "events.ndjson").read_text()
    assert len([ln for ln in text.splitlines() if ln.strip()]) == 2


def test_parse_event_discriminates() -> None:
    ev = parse_event({"kind": "result", "run_id": "r", "epoch": 0, "node_id": "n0", "ok": False})
    assert isinstance(ev, ResultEvent)
    assert ev.ok is False
