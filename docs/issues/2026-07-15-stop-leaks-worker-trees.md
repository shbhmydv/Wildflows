# SEVERITY: critical — engine stop/crash leaks live worker process trees

**Observed** during the U4ME dogfood (run `09d266b4`, 2026-07-15), and again on
engine crash (run `8d62364a`).

## What happened

SIGINT to a running engine exited it "cleanly", but **9 in-flight worker frames
(pi processes) and their descendants kept running** — including pi-spawned
subagents holding local llama backends at ~95% GPU. With the engine dead, their
results could never be journalled: pure quota/GPU burn. The same leak happens on
an engine *crash* (see the FrameCallJoinTimeoutError issue): the orphaned worker
later hit its rig `timeout --signal=KILL` with nobody left to record the
failure or relaunch.

Operator cleanup required manually killing the PGIDs from each rig log-dir
`handle` file, then hunting escaped grandchildren (pi subagents re-parented to
pid 1, found only via their TCP connections to the llama router).

## Why it's extremely bad

- Detached tmux is the recommended launch mode precisely so runs survive; but it
  also means nobody notices multi-hour orphan burn.
- pi subagents escape the recorded PGID (they create their own process groups),
  so even a correct PGID sweep misses them.

## Suggested fix

- On SIGINT/SIGTERM and on any fatal engine exception, reap every outstanding
  rig handle (SIGTERM, grace, SIGKILL the process group) **before** the process
  exits — the engine already owns the handle files.
- Belt-and-braces: run each rig adapter under a session leader (`setsid`) and
  record the session id, or use a cgroup/pidfd so descendants that change
  process group are still findable.
- Journal a `worker_reaped` event per kill so resume knows those attempts died.
