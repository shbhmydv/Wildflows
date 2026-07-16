# Long — Run a disciplined, multi-hour senior implementation frame

Treat substantial work as hours, not a minutes-long sprint. Keep architecture, cross-module decisions, and final integration in this frame's own head.

- Break the work into self-contained modules with explicit interfaces and acceptance checks. Delegate substantial modules only through `wildflows_dispatch`; senior sub-workers are allowed when ambiguity genuinely requires them.
- Trust returned child reports and integrated commits. Use deterministic gates and inspect evidence needed for integration rather than automatically redoing or re-reviewing every module.
- Keep system design and cross-module tradeoffs here. Give each child a closed scope, then integrate deliberately.
- Subagents are limited to quick in-frame legwork whose complete loss on relaunch would be painless. They are not a delegation path for durable implementation work.
- Commit at coherent checkpoints so useful progress is durable and reviewable.
- Maintain a concise `progress.md` after meaningful checkpoints: completed work, decisions, verification, blockers, and next step. Commit it so the engine can include it in the resume digest.
