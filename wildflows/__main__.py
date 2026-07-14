"""Command line entry point for v2 root-frame runs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from wildflows.admission import AdmissionPolicy
from wildflows.rigconfig import load_rigs
from wildflows.run import Run


def _common(parser: argparse.ArgumentParser, *, resume: bool) -> None:
    parser.add_argument("job", type=Path)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--rigs", type=Path)
    parser.add_argument("--root-rig", default="senior")
    parser.add_argument("--run-id", required=resume)
    parser.add_argument("--run-branch")
    parser.add_argument("--worktrees-root", type=Path)
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--max-breadth", type=int, default=8)
    parser.add_argument("--max-subtree-frames", type=int, default=64)
    parser.add_argument("--max-subtree-spend", type=float, default=64.0)
    parser.add_argument("--subtree-timeout", type=float, default=3600.0)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python3 -m wildflows")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="start a root frame")
    _common(run, resume=False)
    resume = commands.add_parser("resume", help="replay a durable frame stack")
    _common(resume, resume=True)
    resume.add_argument("--answer")
    resume.add_argument("--answer-frame")
    resume.add_argument("--answer-call", type=int)
    dash = commands.add_parser("dash", help="serve the v2 frame-call-stack console")
    dash.add_argument("--repo", type=Path, action="append", default=[])
    dash.add_argument("--watchlist", type=Path)
    dash.add_argument("--port", type=int, default=8181)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "dash":
        try:
            from wildflows.dashboard.app import serve
        except ModuleNotFoundError as exc:
            if exc.name in {"fastapi", "starlette", "uvicorn"}:
                raise RuntimeError(
                    "dashboard dependencies missing; install wildflows[dash]"
                ) from exc
            raise
        serve(args.repo, args.port, watchlist=args.watchlist)
        return 0

    if (
        args.command == "resume"
        and args.answer is not None
        and Run.deliver_live_answer(
            args.repo,
            args.run_id,
            args.answer,
            frame_id=args.answer_frame,
            call_index=args.answer_call,
        )
    ):
        print(f"wildflows run_id={args.run_id}")
        print(json.dumps({
            "summary": "owner answer delivered; resident run continues",
            "frames": 0,
            "outcome": "ok",
        }))
        return 0

    job: Path = args.job
    rigs: Path = args.rigs or job.parent / "rigs.yaml"
    policy = AdmissionPolicy(
        max_depth=args.max_depth,
        max_breadth=args.max_breadth,
        max_subtree_frames=args.max_subtree_frames,
        max_subtree_spend=args.max_subtree_spend,
        subtree_timeout_s=args.subtree_timeout,
    )
    run = Run(
        workdir=args.repo,
        job_spec=job,
        registry=load_rigs(rigs),
        root_rig=args.root_rig,
        run_id=args.run_id,
        run_branch=args.run_branch,
        policy=policy,
        worktrees_root=args.worktrees_root,
    )
    print(f"wildflows run_id={run.run_id}")
    if args.command == "resume":
        completed = run.resume(
            answer=args.answer,
            frame_id=args.answer_frame,
            call_index=args.answer_call,
        )
    else:
        completed = run.run()
    print(json.dumps({
        "summary": completed.summary,
        "frames": completed.frames,
        "outcome": completed.outcome,
    }))
    return 0 if completed.outcome == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
