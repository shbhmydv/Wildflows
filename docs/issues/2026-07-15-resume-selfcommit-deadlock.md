# SEVERITY: critical — resume deadlocks when a frame self-commits before dispatching

**Observed:** U4ME dogfood run `09d266b4` became permanently unresumable.

## What happened

Root frame `f0` committed directly on its own frame branch (`4c14452`, a docs
commit) *before* its first dispatch. Child `f0.c0.t0` later integrated with
`integration_base=4c14452`, `candidate_head=4cb05d6` (journalled correctly).

On resume, two checks contradict each other:

- `Engine._explained_frame_tips` walks **only integration events** starting from
  `frame.base_commit` (`ab153ee`). The integration's base `4c14452` never equals
  the walk's tip, so the walk never advances: expected tip stays `ab153ee`.
  With the branch at any real tip, `_guard_frame_relaunch` parks:
  `frame_relaunch_blocked, expected ab153ee`.
- Reset the branch to `ab153ee` to satisfy the guard, and `_verify_integrations`
  raises instead: `integrated frame 'f0.c0.t0' is absent from its target branch`.

No branch tip satisfies both. The operator seam ("inspect, disposition, resume")
cannot help; the run is dead. We abandoned replay and merged the branches by
hand.

## Why it's extremely bad

Frames are *instructed* to "commit useful work before every tool call" — so any
frame that follows instructions and then needs a relaunch (crash, stop, kill)
poisons the run. Resume is the headline durability feature; this breaks it on
the common path, silently, at the worst time (after a crash).

## Suggested fix

`_explained_frame_tips` must model frame self-commits: treat a `FrameIntegrating`
/`FrameIntegrated` whose `integration_base` is a **descendant** of the current
walk tip (and an ancestor of the branch) as advancing the walk through the
self-committed range, or journal the frame's own tip movement (e.g. a
`frame_committed` event at each engine call boundary — the engine already
observes the worktree at every dispatch/gate). Add a regression test: frame
commits, dispatches, child integrates, engine killed, resume must relaunch.
