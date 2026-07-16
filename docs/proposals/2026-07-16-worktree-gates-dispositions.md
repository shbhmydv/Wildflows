# Proposal: independent gate timers, worktree provisioning, and failed-child disposition

Status: ACCEPTED 2026-07-16

## Evidence and problem

Live U4ME and React Native target runs exposed four coupled failure modes: a legitimate
11-minute repository gate exhausted its caller's self-time and made every later gate
instant-timeout; fresh Git worktrees lacked ignored dependencies; a failed child hid a
mostly useful committed branch; and bounded repair loops had no honest exhaustion
policy.

## Decisions

### A. Gate waits are resident, not self-time

A gate pauses its caller's self-time timer from `gate_called` through `gate_returned` but
retains the rig slot for the resident worker. The eventual `frame_slot_released.active_s`
contains only active thinking intervals; replay therefore folds the same authoritative
self-time. On resume, an orphaned active interval subtracts journalled gate wait time, so
the existing gate timestamps are sufficient and no second gate-clock vocabulary exists.

Each rig may set an optional positive `gate_timeout_s`. It bounds only gates called by
that rig. Unset means no Wildflows gate ceiling; adapter, subtree, run, and operator
backstops remain separate. A configured timeout returns exit 124 with
`[timeout] gate exceeded <seconds>s` on stderr.

### B. Repository-wide fresh-worktree provisioning

`rigs.yaml` accepts a top-level repository property:

```yaml
worktree:
  setup: python3 -m project_bootstrap --worktree
  link:
    - .cache/dependencies
```

`setup` runs once after each checkout and before adapter launch. Nonzero exit records
bounded output, terminalizes the frame, and removes the checkout. Each `link` source is a
validated repository-relative path in the primary checkout; existing sources are
symlinked at the same destination and missing sources become journalled warnings. Shared
mutable links suit caches/dependencies, not source or build outputs.

A `worktree_provisioned` event records frame/attempt/path, mechanism, duration, outcome,
and bounded details. `run_opened` pins the provisioning config. A completed/replayed
worktree mechanism is not rerun; a genuinely new replacement checkout is provisioned
once.

### C. Failed children expose and reuse salvage

Every failed pushed child result includes its short frame branch, exit head, and an
8-KiB-bounded committed diffstat against its original branch point. Successful children
still auto-integrate exactly as before.

`wildflows_dispatch(retry_frame=<failed-direct-child-id>)` is exclusive with new-task
fields. A fresh call accepts only a failed direct child of the caller. It relaunches the
same frame id and branch through the existing warm-relaunch path: attempt number, prior
commits, bounded logs, saved dirty diff, and death reason all survive. The request is
journalled and pending-call replay may reconnect an in-progress or just-completed retry.
Non-children and successful children return durable, explicit refusals.

Doctrine supplies the other dispositions: retry a plausible transient; merge the
salvage branch and finish inline when little remains; escalate an under-tiered task with
failure evidence and branch; or ask/park when only the owner can unblock it.

### D. Bounded loops fail deliberately

A retry budget is a budget, not a suggestion. When it exhausts with a failing gate, do
not silently extend it: finish a small residual inline, re-tier under-specced work with
concrete evidence, or fail honestly upward with that evidence. A truthful failure is
more useful to a parent than fake success.

## Compatibility and non-goals

All fields/events are additive within journal v2. Normal dispatch hashes omit absent
`retry_frame`, and old run records default to no provisioning and unbounded gates.
Provisioning is generic; Wildflows hardcodes no target dependency path or setup command.
There is no automatic failed-branch merge, automatic escalation, hidden retry, or
hardcoded gate ceiling.
