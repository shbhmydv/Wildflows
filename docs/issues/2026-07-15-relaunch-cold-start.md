# ENHANCEMENT — relaunched frames restart cold; give them the prior attempt's evidence

**Observed:** U4ME dogfood, relaunch of `f0.c2.t0` (run `8d62364a`).

Resume/relaunch restores *effects* (journalled calls replay free, branch keeps
commits) but not *context*: the frame restarts from its original prompt plus the
call digest and re-derives everything — re-reads the repo, re-thinks ~90k tokens.
A frame that crashed with zero completed calls re-runs at full price.

Grindstone's equivalent was warmer: same worktree, plus a nudge — "you are a
relaunch; read the previous attempt's logs" — so attempt 2 starts from evidence.

## Suggestion

On relaunch, append an "earlier attempt" block to the rebuilt prompt:

- tail of the previous attempt's rig stdout/stderr logs (the adapters already
  write `pi.stdout.log`/`pi.stderr.log` per attempt);
- the worktree's uncommitted diff (if any) at relaunch time;
- the attempt count and why it died (timeout / crash / operator reset).

Cheap to implement (engine already knows the log dir and worktree), and turns a
full re-derivation into a resume-from-notes. Pairs well with backend pinning,
which already keeps the KV cache warm for the prompt prefix.
