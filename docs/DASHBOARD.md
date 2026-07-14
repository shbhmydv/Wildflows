# Dashboard

WILDFLOWS ships a local control room for live runs. It shows the current expression
tree, folded node state, rails and planner rationale, the append-only journal, node
results and receipts, and run artifacts. It also operates runs through the same CLI
surfaces an operator uses.

## Install and launch

The dashboard is optional; the core install still depends only on Pydantic and PyYAML.

```bash
python3 -m pip install -e '.[dash]'
python3 -m wildflows dash --repo /path/to/target --port 8765
```

The server binds **only `127.0.0.1`** and prints both its URL and a random control
token. Open the URL, click the link icon in the toolbar, and paste that token. The
browser retains it only in the current tab's `sessionStorage`.

## Controls

- **New run** accepts a `job.md` path, `rigs.yaml` path, planner rig, worker limit,
  and optional run id. It launches `python3 -m wildflows run` in a managed process.
- **Resume / retry** launches `python3 -m wildflows resume` for the durable `job.md`.
  Failed or interrupted nodes follow the core's existing replay rules; a successful,
  integrated node is never forcibly replayed.
- **Answer & resume** appears for a pending `ask(owner)`. It passes `--answer` and
  `--answer-node` to that same resume command. The engine remains the sole writer of
  the resulting `answered` event.
- **Kill expression** terminates the active dashboard-managed runner and its observed
  descendant processes through identity-checked Linux pidfds. It is available only when `run.lock` is held and
  `run.json.active` identifies a live process whose PID, PGID, session, and Linux
  process start tick still match. The dashboard refuses stale records and runners
  that were not launched in their own process group. Kill writes no synthetic event:
  a dispatch interrupted before its result remains the ordinary torn tail that resume
  already reconciles and retries in a fresh worktree.

Managed action output is available live under **Journal stream → Action log**. A
normal parked run may produce a non-zero command exit because `AwaitingOwner` is the
CLI's parked-state signal; the journal and run header are authoritative.

There is deliberately no separate `pause` or “retry successful hand” action in v1.
Neither has a CLI/journal meaning in the frozen event vocabulary. Adding either only
in the dashboard would create a second engine and violate replay semantics.

## Pure-consumer boundary

Read paths never instantiate the engine and never append, repair, truncate, or fsync
`events.ndjson`. The backend copies complete newline-terminated records, parses the v1
events, and applies each one to `wildflows.projection.RunProjection`—the same fold the
engine uses. An incomplete live tail is ignored until its terminating newline appears.
SSE polls by byte offset and sends newly complete records; the browser then refreshes
the server-folded projection rather than recreating event semantics in JavaScript.

Mutations are not alternate state transitions. They are managed `python3 -m wildflows
run|resume` subprocesses. The only core addition is the active lock-holder identity in
`run.json`, which makes a kill targetable without changing the journal.

## Files and artifact rendering

Only regular files under a run's `artifacts/`, `decisions/`, or `handoffs/` directory
are served. `..`, absolute paths, and symlinks that resolve outside the run directory
are rejected. Images render in `<img>` elements, text/JSON in `<pre>`, and HTML in a
sandboxed iframe; every preview also has a direct link.

## Token model

Every mutating endpoint requires `X-Wildflows-Token` and compares it with the random
token printed at startup. Read endpoints are unauthenticated. This is intentionally a
same-host operator token, not multi-user authentication: the server is loopback-only,
uses no cookies, and has no remote-bind option. Anyone who can read the token can run
commands through the configured WILDFLOWS rigs, so do not relay it or proxy this server.
