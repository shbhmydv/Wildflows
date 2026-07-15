# Dashboard (v2 frame console)

The optional dashboard is a local FastAPI/Uvicorn operator console with a
framework-free static frontend over v2 `events.ndjson` journals. It discovers the call
stack from frame and tool-call facts;
it does not forecast or write engine state. The canvas reads top-down: a frame card,
then that frame's dispatch rows in call order, with parallel siblings across each row.
Completed sibling rows collapse together, an in-flight caller is **banked**, and only a
running leaf breathes. Frame state follows the frame's own exit: an ok frame with failed
children remains **done** and shows an `N failed children` chip.

The call stack keeps its natural intrinsic width on an unbounded surface. Parallel
siblings never shrink below 280px at 100% and never wrap; the canvas pane scrolls in
both axes. Drag empty dotted canvas to pan, use an ordinary wheel/trackpad to scroll,
and use Ctrl/Cmd+wheel to zoom around the pointer. The fixed bottom-right controls
provide zoom out, percentage, zoom in, and fit-to-width. Calls wider than five tasks
still start as five cards plus the counted ghost; expanding the ghost widens the canvas.

```bash
pip install -e '.[dash]'
python3 -m wildflows dash \
  --repo /path/to/first-target \
  --repo /path/to/second-target
# http://127.0.0.1:8181
```

`--repo` is repeatable. `--watchlist paths.txt` adds one repository path per nonblank,
non-comment line and can be combined with `--repo`. `--port` overrides the 8181
default. The run picker and **LIVE NOW** rail qualify every run with its repository.
Useful deep-link parameters are `?repo=<name>&run=<id-prefix>&theme=light|dark`; add
`&frame=<full-frame-id>` to rebase the canvas at a deep frame.

The server tails complete journal records with SSE and serves run-local artifacts
read-only. It ignores an unterminated tail and never repairs or truncates a watched
journal. Gate rows expose captured stdout and stderr separately. An unanswered `ask`
can use the engine's existing `Run.deliver_live_answer` seam; this is the dashboard's
only control action and requires the random token printed at startup. No pause, kill,
retry, or launch API is invented here.

## Development fixture

The synthetic repository at `examples/dashboard-fixture` includes the states that
small live runs rarely exhibit: a failed frame, parked ask, 20-way fan-out with queued
slots, a completed sibling row, a nonzero two-stream gate, and depth four.

```bash
python3 -m wildflows dash --repo examples/dashboard-fixture
# http://127.0.0.1:8181/?repo=dashboard-fixture&run=frame-stack-demo&theme=light
```

For a manual canvas pass, inspect that link at 1440px and 900px in both themes. Check
50%, 100%, and 150%; at each zoom, scroll both axes, drag the empty dotted area, and
confirm cards neither overlap nor narrow as the viewport changes. Expand the 20-way
fan-out ghost and verify that the scroll width grows, then use fit-to-width. Expand a
long result and failed gate streams and verify that each scrolls inside its card.
For the finished owner run, watch the read-only repository and open
`?repo=wf-selfaudit-target&run=a07cba96`: expand `f0`, then inspect `f0.c0.t0` at 50%
and 100%. It must be DONE with `1 failed child`, while its failed child alone has a red
FAILED border.
