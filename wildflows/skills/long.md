# Long — Run a disciplined, multi-hour senior implementation frame

Treat substantial work as hours, not a minutes-long sprint. Keep the architecture and cross-module decisions in this frame's own head.

- Break the work into self-contained modules with clear interfaces and acceptance checks; delegate those modules through agent-harness subagents.
- Review every returned module before accepting it: inspect the diff, run its relevant checks, and reject or repair work that does not meet the interface or task.
- Integrate deliberately while retaining architectural ownership; do not delegate the system design or blindly merge child output.
- Commit early and often at coherent checkpoints so progress is durable and reviewable.
- Maintain a concise `progress.md` in the worktree after each meaningful checkpoint: completed work, decisions, verification, blockers, and next step. Commit it so the engine can include it in the resume digest.
