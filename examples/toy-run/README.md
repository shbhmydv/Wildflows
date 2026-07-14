# Live frame toy run

From the WILDFLOWS checkout, with `pi` authenticated, the local OpenAI-compatible
server listening on `127.0.0.1:8080`, and `<target>` a clean Git repository:

```bash
python3 -m wildflows run examples/toy-run/job.md \
  --repo <target> --rigs examples/toy-run/rigs.yaml --root-rig senior
```

The senior is the root resident frame. It dispatches two one-shot local leaf frames
through the per-run authenticated endpoint and synthesizes their returned text itself.
Adapter logs stay under `/tmp/wildflows-toy-run/`; generated Pi shims stay in the run
directory; frame worktrees use the external system worktree root.

Resume the durable stack with:

```bash
python3 -m wildflows resume examples/toy-run/job.md \
  --repo <target> --rigs examples/toy-run/rigs.yaml --root-rig senior \
  --run-id <id>
```

Add `--answer 'yes'` when the journal shows one pending owner question.
