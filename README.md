# wildflows

**Resident agent frames with deterministic, journalled hands.**

Wildflows v2 is a standalone supervisor for long-running agent work. A run starts one
root frame in an external Git worktree. That resident agent owns strategy and ordinary
control flow; when it needs help it calls one of three authenticated engine tools:

- `dispatch(tasks[], rig, parallel?, skills?)` pushes child frames and blocks until
  their committed work is integrated into the caller's frame branch; `skills` is one
  ordered skill-name list per task;
- `gate(cmd)` runs a deterministic check in the caller's worktree and returns the exit
  code plus complete stdout **and** stderr;
- `ask(question)` parks the frame until its owner supplies an answer.

There is no planner/expression/epoch executor. Sequences, loops, and result synthesis
are normal agent control flow. See [`docs/DESIGN.md` §12](docs/DESIGN.md) for the design
of record.

## Durability model

Every run has an incompatible v2 append-only journal at
`<repo>/.wildflows/runs/<run-id>/events.ndjson`. Child branches start at their parent
frame's branch, never at the run branch. Child commits integrate only upward; the run
branch advances once, when the root frame unwinds. All frame worktrees live outside the
target repository.

Resume replays the frame stack from original prompts with one engine-generated digest
of completed and pending calls. Calls are keyed by frame, logical call index, and
canonical content hash. A completed call reissued with the same identity returns its
journalled result without launching another agent or rerunning a gate. Only the
frontier frame's uncommitted work is disposable.

Dispatch admission is enforced before child effects: depth, breadth, subtree frame,
spend/time, and rig-allowlist rails. Skills are prompt data and do not change admission.
Repository Markdown skills in `.wildflows/skills/` shadow bundled stock skills. Every
frame receives its assigned skill texts, job, the full skill manifest, then the engine
tool preamble.

The per-run MCP-compatible JSON-RPC endpoint binds an ephemeral `127.0.0.1` port and
requires its random bearer token. Banked tool calls use HTTP/1.1 chunked whitespace
heartbeats. A disconnected Pi shim retries the same hidden call identity, allowing the
engine's durable single flight to return the original result without recomposed work.

## Run

Declare frame rigs in YAML (see [`docs/RIGS.md`](docs/RIGS.md)), then:

```bash
python3 -m wildflows run job.md \
  --repo /path/to/target \
  --rigs rigs.yaml \
  --root-rig senior
```

Resume a stopped stack with the printed run id:

```bash
python3 -m wildflows resume job.md --repo /path/to/target \
  --rigs rigs.yaml --root-rig senior --run-id <id>
```

A live parked ask can be answered by adding `--answer '...'`; the resident engine
observes the durable answer file and resumes the blocked tool call.

The optional dashboard renders the live v2 frame call stack and journal. It watches
multiple repositories on fixed port 8181; `--repo` is repeatable and `--watchlist`
accepts a file with one repository path per line:

```bash
python3 -m wildflows dash \
  --repo /path/to/target \
  --repo /path/to/another-target
# http://127.0.0.1:8181
```

See [docs/DASHBOARD.md](docs/DASHBOARD.md) for deep links, the synthetic visual fixture,
and the token-guarded owner-answer seam.

## Develop

```bash
pip install -e '.[dev]'
python3 -m pytest -q
python3 -m mypy --strict wildflows tests
bash -n rigs/*.sh
```

Repository tests use fake agent binaries; they do not invoke real models.
