"""Focused coverage for layered skill routing, prompts, and resume state."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from tests.conftest import git
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

    assert library.names == ("long", "plan-compress-execute", "skill-selection")
    skills = library.resolve(library.names)
    assert [skill.description for skill in skills] == [
        skill.text.splitlines()[0].removeprefix("# ") for skill in skills
    ]
    assert library.manifest() == "\n".join([
        "SKILL MANIFEST:",
        "- long: Long — Run a disciplined, multi-hour senior implementation frame",
        "- plan-compress-execute: Plan, compress, execute — Turn a bounded junior task into verified delivery",
        "- skill-selection: Skill selection — Route a small, role-appropriate bundle to each frame",
    ])


def test_repository_skills_shadow_bundled_stems_and_add_custom_skills(repo: Path) -> None:
    skills_dir = repo / ".wildflows" / "skills"
    skills_dir.mkdir(parents=True)
    replacement = skills_dir / "long.md"
    replacement.write_text("# Local long — Repository replacement\n\nUse local rules.\n", encoding="utf-8")
    custom = skills_dir / "review.md"
    custom.write_text("# Review — Check the delivered change\n\nReview it.\n", encoding="utf-8")

    library = SkillLibrary(repo)

    assert library.names == (
        "long",
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


def test_frame_prompt_orders_assigned_skill_texts_before_job_manifest_and_preamble(
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
        "You are a WILDFLOWS frame. Work only in your CWD. Commit useful changes "
        "before calling an engine tool or exiting. The only engine tools are "
        "wildflows_dispatch, wildflows_gate, and wildflows_ask. Tool calls block; "
        "child commits are present in your branch when dispatch returns. Dispatch "
        "skills is optional and contains one ordered skill-name list per task.\n"
    )
    library = SkillLibrary(repo)
    expected = "\n\n".join([
        library.resolve(["skill-selection"])[0].text,
        library.resolve(["long"])[0].text,
        "--- FRAME JOB ---\nimplement the assigned task",
        library.manifest(),
        f"--- TOOL PREAMBLE ---\n{preamble}",
    ])
    assert rig.prompts == [expected]
    pushed = [
        event for event in engine.journal.events() if isinstance(event, FramePushed)
    ]
    assert [event.skills for event in pushed] == [assigned]


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
