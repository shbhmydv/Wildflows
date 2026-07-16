"""Focused coverage for layered skill routing, prompts, and resume state."""
from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest
from pydantic import ValidationError

from tests.conftest import git
from wildflows.admission import AdmissionPolicy
from wildflows.engine import Engine
from wildflows.events import DispatchCalled, DispatchReturned, FramePushed
from wildflows.frame import ChildResult, DispatchRequest, DispatchResult, FrameResult, FrameRuntime, call_hash
from wildflows.rig import RigRegistry
from wildflows.skill import SkillLibrary, SkillLibraryError


class CapturingRig:
    """A no-model rig which retains the exact frame prompt it was given."""

    timeout_s = 1.0

    def __init__(self) -> None:
        self.prompts: list[str] = []

    def run(
        self, prompt: str, workdir: Path, runtime: FrameRuntime
    ) -> FrameResult:
        del workdir, runtime
        self.prompts.append(prompt)
        return FrameResult(text="captured", exit_code=0)


def _engine(repo: Path, tmp_path: Path, rig: CapturingRig) -> Engine:
    return Engine(
        tmp_path / "run",
        repo,
        RigRegistry({"capture": rig}),
        run_id="skills",
        root_rig="capture",
        root_prompt="root job",
        worktrees_root=tmp_path / "worktrees",
    )


def _push_root(engine: Engine, worktree: Path) -> str:
    base = engine.repository.branch_tip()
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        attempt=0,
        depth=0,
        rig="capture",
        prompt="root job",
        skills=[],
        branch=engine.repository.frame_branch(Engine.ROOT_FRAME_ID),
        base_commit=base,
        worktree=str(worktree),
        subtree_deadline=9_999_999_999.0,
    ))
    return base


def test_bundled_skills_are_discovered_with_heading_descriptions(repo: Path) -> None:
    library = SkillLibrary(repo)

    assert library.names == (
        "dispatch-economy",
        "long",
        "orchestration-shapes",
        "plan-compress-execute",
        "skill-selection",
    )
    skills = library.resolve(library.names)
    assert [skill.description for skill in skills] == [
        skill.text.splitlines()[0].removeprefix("# ") for skill in skills
    ]
    assert library.manifest() == "\n".join([
        "SKILL MANIFEST:",
        "- dispatch-economy: Dispatch economy — Route bounded work by determinacy and spend review once",
        "- long: Long — Run a disciplined, multi-hour senior implementation frame",
        "- orchestration-shapes: Orchestration shapes — Compose serial work, independent fan-out, bounded loops, and reviews",
        "- plan-compress-execute: Plan, compress, execute — Turn a bounded junior task into verified delivery",
        "- skill-selection: Skill selection — Route a small, role-appropriate bundle to each frame",
    ])


def test_failed_child_and_loop_exhaustion_doctrine_is_shipped() -> None:
    library = SkillLibrary(Path("wildflows/skills").parent.parent)
    economy = library.resolve(["dispatch-economy"])[0].text
    shapes = library.resolve(["orchestration-shapes"])[0].text

    assert "retry_frame" in economy
    assert "merge the salvage branch" in economy
    assert "wildflows_ask" in economy
    assert "same task" in economy
    assert "stronger rig" in economy
    assert "failure evidence" in economy
    assert "cheapest rig" in economy
    assert "Never silently extend the loop" in shapes
    assert "fail honestly upward with the concrete evidence" in shapes


def test_root_gets_default_dispatch_skills_with_repository_overrides(
    repo: Path, tmp_path: Path
) -> None:
    skills_dir = repo / ".wildflows" / "skills"
    skills_dir.mkdir(parents=True)
    local = skills_dir / "dispatch-economy.md"
    local.write_text(
        "# Local economy — Repository dispatch policy\n\nUse this local policy.\n",
        encoding="utf-8",
    )
    rig = CapturingRig()
    engine = _engine(repo, tmp_path, rig)

    assert engine.run().outcome == "ok"
    assert rig.prompts[0].startswith(local.read_text(encoding="utf-8"))
    pushed = [
        event for event in engine.journal.events() if isinstance(event, FramePushed)
    ]
    assert pushed[0].skills == ["dispatch-economy", "orchestration-shapes"]


def test_repository_skills_shadow_bundled_stems_and_add_custom_skills(repo: Path) -> None:
    skills_dir = repo / ".wildflows" / "skills"
    skills_dir.mkdir(parents=True)
    replacement = skills_dir / "long.md"
    replacement.write_text("# Local long — Repository replacement\n\nUse local rules.\n", encoding="utf-8")
    custom = skills_dir / "review.md"
    custom.write_text("# Review — Check the delivered change\n\nReview it.\n", encoding="utf-8")

    library = SkillLibrary(repo)

    assert library.names == (
        "dispatch-economy",
        "long",
        "orchestration-shapes",
        "plan-compress-execute",
        "review",
        "skill-selection",
    )
    assert library.resolve(["long"])[0].source == replacement
    assert library.resolve(["long"])[0].text == replacement.read_text(encoding="utf-8")
    assert "- long: Local long — Repository replacement" in library.manifest()
    assert "- review: Review — Check the delivered change" in library.manifest()
    assert "Run a disciplined, multi-hour" not in library.manifest()


@pytest.mark.parametrize(
    "contents",
    [
        "# A heading without its required description\n",
        "# A title — \n",
    ],
)
def test_skill_heading_requires_a_title_and_one_line_description(
    repo: Path, contents: str
) -> None:
    skills_dir = repo / ".wildflows" / "skills"
    skills_dir.mkdir(parents=True)
    (skills_dir / "invalid.md").write_text(contents, encoding="utf-8")

    with pytest.raises(SkillLibraryError, match="must start"):
        SkillLibrary(repo)


def test_dispatch_request_canonicalizes_and_validates_per_task_skill_bundles() -> None:
    omitted = DispatchRequest(tasks=["first", "second"], rig="capture")
    explicit_empty = DispatchRequest(
        tasks=["first", "second"], rig="capture", skills=[]
    )
    assigned = DispatchRequest(
        tasks=["first", "second"],
        rig="capture",
        skills=[["long"], ["skill-selection", "long"]],
    )

    assert omitted.skills == [[], []]
    assert explicit_empty.skills == [[], []]
    assert assigned.skill_bundle(0) == ["long"]
    assert assigned.skill_bundle(1) == ["skill-selection", "long"]
    with pytest.raises(ValidationError, match="one list per task"):
        DispatchRequest(tasks=["first", "second"], rig="capture", skills=[["long"]])
    with pytest.raises(ValidationError, match="skill names must be non-blank"):
        DispatchRequest(tasks=["first"], rig="capture", skills=[["  "]])


def test_dispatch_call_hash_includes_each_skill_bundle_canonically() -> None:
    omitted = DispatchRequest(tasks=["first", "second"], rig="capture")
    explicit_empty = DispatchRequest(
        tasks=["first", "second"], rig="capture", skills=[]
    )
    changed_second_task = DispatchRequest(
        tasks=["first", "second"], rig="capture", skills=[[], ["long"]]
    )

    assert call_hash("dispatch", omitted) == call_hash("dispatch", explicit_empty)
    assert call_hash("dispatch", omitted) != call_hash("dispatch", changed_second_task)


def test_frame_prompt_orders_assigned_skill_texts_before_job_and_resources(
    repo: Path, tmp_path: Path
) -> None:
    rig = CapturingRig()
    engine = _engine(repo, tmp_path, rig)
    assigned = ["skill-selection", "long"]
    base = engine.repository.branch_tip()

    with engine.server:
        engine._launch_frame(  # noqa: SLF001 - exercise the frame launch boundary
            frame_id=Engine.ROOT_FRAME_ID,
            parent_frame_id=None,
            parent_call_index=None,
            task_index=None,
            depth=0,
            rig="capture",
            prompt="implement the assigned task",
            skills=assigned,
            base_commit=base,
            subtree_deadline=9_999_999_999.0,
        )

    preamble = (
        "--- RESOURCES ---\n"
        "You are a WILDFLOWS frame. Work only in your CWD. Commit useful changes "
        "before calling an engine tool or exiting.\n\n"
        "RIGS:\n"
        "- capture\n"
        "Rig names are these registry keys; script filenames are not rig names.\n\n"
        "LIMITS:\n"
        "- Remaining depth below this frame: 4.\n"
        "- Maximum parallel width: 8 tasks per dispatch.\n"
        "- Remaining descendant frame capacity: 64.\n"
        "- Remaining subtree spend capacity: 64 admission units.\n\n"
        "SKILL MANIFEST:\n"
        "- dispatch-economy: Dispatch economy — Route bounded work by determinacy "
        "and spend review once\n"
        "- long: Long — Run a disciplined, multi-hour senior implementation frame\n"
        "- orchestration-shapes: Orchestration shapes — Compose serial work, "
        "independent fan-out, bounded loops, and reviews\n"
        "- plan-compress-execute: Plan, compress, execute — Turn a bounded junior "
        "task into verified delivery\n"
        "- skill-selection: Skill selection — Route a small, role-appropriate bundle "
        "to each frame\n\n"
        "TOOLS:\n"
        "The only engine tools are wildflows_dispatch, wildflows_gate, and "
        "wildflows_ask. Tool calls block; successful child commits are present in "
        "your branch when dispatch returns. A failed child result includes its "
        "salvage branch, head, and diffstat; pass retry_frame alone to relaunch a "
        "failed direct child on that branch. Dispatch skills is optional and contains one "
        "ordered skill-name list per task. Dispatch rig accepts one registry key for "
        "every task or a parallel list; omission and null list entries inherit this "
        "frame's rig. Dispatch kinds is an optional parallel list describing the "
        "nature of each task; kinds are journalled hints with no routing power. "
        "Shapes are your control flow: a sequence "
        "is consecutive dispatch calls, a loop is redispatching until your own "
        "criterion is met, a fan-out is one dispatch with many tasks (parallel: "
        "true); combine these freely and choose per task. Prefer sequential "
        "dispatches when each task depends on the previous result; prefer parallel "
        "when tasks are independent. Admission refusals are durable, no-effect "
        "tool results: nothing was launched, and replay returns the same refusal for "
        "that call. Correct the request before making a new dispatch. Use "
        "wildflows_ask only when progress requires owner-only information or a "
        "decision; never use it to discover rigs, limits, or skills listed here.\n"
    )
    library = SkillLibrary(repo)
    expected = "\n\n".join([
        library.resolve(["skill-selection"])[0].text,
        library.resolve(["long"])[0].text,
        "--- FRAME JOB ---\nimplement the assigned task",
        preamble,
    ])
    assert rig.prompts == [expected]
    pushed = [
        event for event in engine.journal.events() if isinstance(event, FramePushed)
    ]
    assert [event.skills for event in pushed] == [assigned]


def test_child_resource_preamble_uses_effective_attenuated_rigs_and_limits(
    repo: Path, tmp_path: Path
) -> None:
    rig = CapturingRig()
    registry = RigRegistry(
        {"senior": rig, "local": rig},
        descriptions={
            "senior": "deep architecture and review lane",
            "local": "pooled dual-GPU Qwen lane for concretely-specced junior work",
        },
    )
    engine = Engine(
        tmp_path / "run",
        repo,
        registry,
        run_id="resources",
        root_rig="senior",
        root_prompt="root job",
        policy=AdmissionPolicy(
            max_depth=3,
            max_breadth=2,
            max_subtree_frames=4,
            max_subtree_spend=2,
            rig_costs={"senior": 2, "local": 1},
        ),
        worktrees_root=tmp_path / "worktrees",
    )
    base = engine.repository.branch_tip()
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        attempt=0,
        depth=0,
        rig="senior",
        prompt="root job",
        skills=[],
        branch=engine.repository.frame_branch(Engine.ROOT_FRAME_ID),
        base_commit=base,
        worktree=str(repo),
        subtree_deadline=9_999_999_999.0,
    ))
    root_prompt = engine._frame_prompt(  # noqa: SLF001 - prompt contract regression
        Engine.ROOT_FRAME_ID, "root job", [], repo
    )
    assert "RIGS:\n- senior: deep architecture and review lane\n- local: " in root_prompt
    assert "Rig names are these registry keys; script filenames are not rig names." in root_prompt

    child_id = "f0.c0.t0"
    engine.journal.append(FramePushed(
        run_id=engine.run_id,
        frame_id=child_id,
        parent_frame_id=Engine.ROOT_FRAME_ID,
        parent_call_index=0,
        task_index=0,
        attempt=0,
        depth=1,
        rig="local",
        prompt="bounded child task",
        skills=[],
        branch=engine.repository.frame_branch(child_id),
        base_commit=base,
        worktree=str(repo),
        subtree_deadline=9_999_999_999.0,
    ))
    child_prompt = engine._frame_prompt(  # noqa: SLF001 - attenuation regression
        child_id, "bounded child task", [], repo
    )
    resources = child_prompt.split("--- RESOURCES ---", maxsplit=1)[1]
    rigs, limits = resources.split("LIMITS:", maxsplit=1)
    assert rigs == (
        "\nYou are a WILDFLOWS frame. Work only in your CWD. Commit useful changes "
        "before calling an engine tool or exiting.\n\n"
        "RIGS:\n"
        "- local: pooled dual-GPU Qwen lane for concretely-specced junior work\n"
        "Rig names are these registry keys; script filenames are not rig names.\n\n"
    )
    assert "- Remaining depth below this frame: 2." in limits
    assert "- Maximum parallel width: 2 tasks per dispatch." in limits
    assert "- Remaining descendant frame capacity: 3." in limits
    assert "- Remaining subtree spend capacity: 1 admission unit." in limits


def test_pre_resources_journal_fixture_replays_without_call_hash_conflict(
    repo: Path, tmp_path: Path
) -> None:
    fixture = Path(__file__).with_name("fixtures") / "pre_resources_journal.ndjson"
    records = [
        cast(dict[str, object], json.loads(line))
        for line in fixture.read_text(encoding="utf-8").splitlines()
    ]
    base = git(repo, "rev-parse", "HEAD")
    worktrees = tmp_path / "replay-worktrees"
    for record in records:
        if record["kind"] == "run_opened":
            record["repository"] = str(repo)
            record["base_commit"] = base
            record["worktrees_root"] = str(worktrees)
        elif record["kind"] == "frame_pushed":
            record["base_commit"] = base
            record["worktree"] = str(tmp_path / "lost-root-worktree")
        elif record["kind"] == "dispatch_called":
            record["caller_head"] = base
    run_dir = tmp_path / "pre-resources-run"
    run_dir.mkdir()
    (run_dir / "events.ndjson").write_text(
        "".join(f"{json.dumps(record, separators=(',', ':'))}\n" for record in records),
        encoding="utf-8",
    )
    git(repo, "branch", "wildflows/pre-resources/f0", base)

    rig = CapturingRig()
    engine = Engine(
        run_dir,
        repo,
        RigRegistry({"capture": rig}),
        run_id="pre-resources",
        root_rig="capture",
        root_prompt="root job",
    )
    request = DispatchRequest(
        tasks=["bounded child"], rig="capture", skills=[["long"]]
    )
    durable_call = engine.projection.call(Engine.ROOT_FRAME_ID, 0)
    assert durable_call is not None
    assert durable_call.call_hash == call_hash("dispatch", request)

    assert engine.run().outcome == "ok"
    assert len(rig.prompts) == 1
    assert "--- RESOURCES ---" in rig.prompts[0]
    assert "RESUME REPLAY:" in rig.prompts[0]


def test_completed_dispatch_resume_digest_includes_skills_and_committed_progress(
    repo: Path, tmp_path: Path
) -> None:
    (repo / "progress.md").write_text("completed the first checkpoint\n", encoding="utf-8")
    git(repo, "add", "progress.md")
    git(repo, "commit", "-m", "record progress")
    rig = CapturingRig()
    engine = _engine(repo, tmp_path, rig)
    base = _push_root(engine, repo)
    request = DispatchRequest(
        tasks=["first", "second"],
        rig="capture",
        skills=[["long"], ["skill-selection"]],
    )
    digest = call_hash("dispatch", request)
    engine.journal.append(DispatchCalled(
        run_id=engine.run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        call_index=0,
        call_hash=digest,
        request=request,
        caller_head=base,
    ))
    engine.journal.append(DispatchReturned(
        run_id=engine.run_id,
        frame_id=Engine.ROOT_FRAME_ID,
        call_index=0,
        call_hash=digest,
        result=DispatchResult(
            outcome="ok",
            children=[ChildResult(frame_id="f0.c0.t0", outcome="ok", text="done")],
        ),
    ))

    expected_calls = engine.projection.resume_digest(Engine.ROOT_FRAME_ID)
    assert expected_calls[0]["status"] == "completed"
    assert expected_calls[0]["skills"] == [["long"], ["skill-selection"]]
    prompt = engine._frame_prompt(  # noqa: SLF001 - verify resume prompt materialization
        Engine.ROOT_FRAME_ID, "root job", [], repo
    )
    encoded = prompt.split("RESUME_DIGEST=", maxsplit=1)[1].split("\n", maxsplit=1)[0]
    assert json.loads(encoded) == {
        "calls": expected_calls,
        "progress_note": "completed the first checkpoint\n",
    }
