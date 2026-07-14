# Dashboard (v2 frame console)

The optional dashboard is a framework-free, local operator console over v2
`events.ndjson` journals. It discovers the call stack from frame and tool-call facts;
it does not forecast or write engine state. The canvas reads top-down: a frame card,
then that frame's dispatch rows in call order, with parallel siblings across each row.
Completed sibling rows collapse together, an in-flight caller is **banked**, and only a
running leaf breathes.

```bash
pip install -e '.[dash]'
python3 -m wildflows dash \
  --repo /path/to/first-target \
  --repo /path/to/second-target
# http://127.0.0.1:8181
```

`--repo` is repeatable. `--watchlist paths.txt` adds one repository path per nonblank,
non-comment line and can be combined with `--repo`. `--port` overrides the fixed 8181
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
