"""Thin command line entry point for planner-driven runs."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from wildflows.rigconfig import load_rigs
from wildflows.run import Run
def _save_operator_config(
    run: Run,
    rigs: Path,
    planner_rig: str,
    max_workers: int,
    run_branch: str | None,
) -> None:
    directory = run.run_dir / "control"
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / ".config.json.tmp"
    temporary.write_text(json.dumps({
        "rigs": str(rigs.resolve()),
        "planner_rig": planner_rig,
        "max_workers": max_workers,
        "run_branch": run_branch,
    }, sort_keys=True), encoding="utf-8")
    temporary.replace(directory / "config.json")
def _common(parser: argparse.ArgumentParser, *, resume: bool) -> None:
    parser.add_argument("job", type=Path)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--rigs", type=Path)
    parser.add_argument("--planner-rig", default="planner")
    parser.add_argument("--run-id", required=resume)
    parser.add_argument("--run-branch")
    parser.add_argument("--max-workers", type=int, default=1)
def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python3 -m wildflows")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="start or drive a planner run")
    _common(run, resume=False)
    resume = commands.add_parser("resume", help="resume a durable run")
    _common(resume, resume=True)
    resume.add_argument("--answer")
    resume.add_argument("--answer-node")
    resume.add_argument("--retry-setups", action="store_true")
    dash = commands.add_parser("dash", help="serve the local live dashboard")
    dash.add_argument("--repo", type=Path, required=True)
    dash.add_argument("--port", type=int, default=8765)
    return parser
def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "dash":
        try:
            from wildflows.dashboard.app import serve
        except ModuleNotFoundError as exc:
            if exc.name in {"fastapi", "starlette"}:
                raise RuntimeError(
                    "dashboard dependencies missing; install wildflows[dash]"
                ) from exc
            raise
        serve(args.repo, args.port)
        return 0
    job: Path = args.job
    rigs: Path = args.rigs or job.parent / "rigs.yaml"
    run = Run(
        workdir=args.repo,
        job_spec=job,
        registry=load_rigs(rigs),
        planner_rig=args.planner_rig,
        run_id=args.run_id,
        run_branch=args.run_branch,
        max_workers=args.max_workers,
    )
    _save_operator_config(
        run, rigs, args.planner_rig, args.max_workers, args.run_branch
    )
    print(f"wildflows run_id={run.run_id}")
    if args.command == "resume":
        completed = run.resume(
            answer=args.answer,
            answer_node=args.answer_node,
            retry_setups=args.retry_setups,
        )
    else:
        completed = run.run()
    print(json.dumps({"summary": completed.summary, "epochs": completed.epochs}))
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
