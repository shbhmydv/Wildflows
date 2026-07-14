# Dashboard frame-stack fixture

This is a **read-only synthetic repository** for dashboard development. Point the
dashboard at `examples/dashboard-fixture`; do not run or resume it. Its only run,
`frame-stack-demo`, is deliberately unfinished and its paths, timestamps, and Git
hashes are fixed fixture data rather than execution output.

From the repository root, serve it with:

```bash
python3 -m wildflows dash --repo examples/dashboard-fixture
```

The journal contains a completed dispatch beside an active 20-task parallel dispatch:
five child slots are launched (terminal, parked, and running) and the other fifteen
remain queued. It also includes a nonzero gate with both output streams and a
completed depth-4 frame chain for drill-in.
