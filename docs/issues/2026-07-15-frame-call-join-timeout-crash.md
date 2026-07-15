# SEVERITY: high — FrameCallJoinTimeoutError crashes the whole engine

**Observed:** U4ME dogfood run `8d62364a`, 2026-07-15 ~23:50 IST.

## What happened

While (re)launching root `f0` with a pending third call, the engine raised

```
wildflows.engine.FrameCallJoinTimeoutError: frame call join exceeded 5s
without confirmed execution stop: f0:2   (engine.py:475, via _launch_frame:1151)
```

and the whole run process died. Consequences compounded: the just-dispatched
worker `f0.c2.t0` was orphaned (see the stop-leak issue), its pi hung after
inference (known pi agent-end bug), the rig timeout SIGKILLed it 20 minutes
later, and nothing was journalled — the operator discovered the crash only by
noticing the missing timeout event.

## Why it's bad

A 5s bookkeeping join on one frame's call state should never be fatal to the
run. Anything slow on the box (GPU prefill hogging IO, a wedged worker) turns
into a full engine death plus orphan leak.

## Suggested fix

- Retry the join with backoff; escalate to failing *that call* (journalled
  `call_failed`), then *that frame* — never the engine.
- If the engine must die, run the reap-and-journal path first (same fix as the
  stop-leak issue).
