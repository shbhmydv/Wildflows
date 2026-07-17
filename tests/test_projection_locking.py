from __future__ import annotations

import threading
from pathlib import Path

import pytest

from wildflows.admission import AdmissionPolicy
from wildflows.engine import Engine
from wildflows.events import FramePushed
from wildflows.frame import DispatchRequest
from wildflows.projection import FrameProjection
from wildflows.rig import EchoRig, RigRegistry


class _BlockingReservations(dict[str, int]):
    """Pause the final ancestor reservation while the projection is locked."""

    def __init__(
        self,
        root_frame_id: str,
        write_started: threading.Event,
        release_write: threading.Event,
        write_committed: threading.Event,
    ) -> None:
        super().__init__()
        self._root_frame_id = root_frame_id
        self._write_started = write_started
        self._release_write = release_write
        self._write_committed = write_committed

    def __setitem__(self, key: str, value: int) -> None:
        if key == self._root_frame_id and not self._write_started.is_set():
            self._write_started.set()
            if not self._release_write.wait(timeout=5):
                raise TimeoutError("test did not release the reservation write")
        super().__setitem__(key, value)
        if key == self._root_frame_id:
            self._write_committed.set()


def test_nested_admission_holds_projection_lock_through_reservation(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A frame push cannot splice itself into an admission snapshot.

    The pause is inside ``frames.values()`` rather than before the read: without a
    common projection transaction, the racing append changes that dictionary during
    the walk.  The second pause proves that the lock remains held through the final
    ancestor reservation, not merely while a snapshot is copied.
    """
    root_frame_id = "f0"
    nested_frame_id = "f0.c0.t0"
    racing_frame_id = "f0.c1.t0"
    policy = AdmissionPolicy(
        max_depth=3,
        max_breadth=1,
        max_subtree_frames=3,
        max_subtree_spend=3.0,
    )
    engine = Engine(
        tmp_path / "run",
        repo,
        RigRegistry({"echo": EchoRig()}),
        run_id="projection-lock",
        root_rig="echo",
        root_prompt="job",
        policy=policy,
        worktrees_root=tmp_path / "worktrees",
    )
    base = engine.repository.branch_tip()

    def push(frame_id: str, parent_frame_id: str | None, depth: int) -> None:
        engine.journal.append(FramePushed(
            run_id="projection-lock",
            frame_id=frame_id,
            parent_frame_id=parent_frame_id,
            parent_call_index=0 if parent_frame_id is not None else None,
            task_index=0 if parent_frame_id is not None else None,
            attempt=0,
            depth=depth,
            rig="echo",
            prompt=frame_id,
            branch=engine.repository.frame_branch(frame_id),
            base_commit=base,
            worktree=str(tmp_path / frame_id),
        ))

    push(root_frame_id, None, 0)
    push(nested_frame_id, root_frame_id, 1)

    walk_started = threading.Event()
    release_walk = threading.Event()
    reservation_write_started = threading.Event()
    release_reservation_write = threading.Event()
    reservation_committed = threading.Event()
    append_attempted = threading.Event()
    append_finished = threading.Event()
    admission_finished = threading.Event()
    admission_results: list[None] = []
    admission_errors: list[BaseException] = []
    append_errors: list[BaseException] = []

    engine._reservation_frames = _BlockingReservations(  # noqa: SLF001 - race seam
        root_frame_id,
        reservation_write_started,
        release_reservation_write,
        reservation_committed,
    )

    def paused_descendants(frame_id: str) -> list[FrameProjection]:
        descendants: list[FrameProjection] = []
        for position, candidate in enumerate(engine.projection.frames.values()):
            if frame_id == root_frame_id and position == 0 and not walk_started.is_set():
                walk_started.set()
                if not release_walk.wait(timeout=5):
                    raise TimeoutError("test did not release the descendant walk")
            parent = candidate.parent_frame_id
            while parent is not None:
                if parent == frame_id:
                    descendants.append(candidate)
                    break
                ancestor = engine.projection.frames.get(parent)
                parent = None if ancestor is None else ancestor.parent_frame_id
        return descendants

    monkeypatch.setattr(engine.projection, "descendants", paused_descendants)

    def admit_nested() -> None:
        try:
            engine._admit_and_reserve(  # noqa: SLF001 - admission transaction seam
                engine.projection.frame(nested_frame_id),
                0,
                DispatchRequest(tasks=["grandchild"], rig="echo"),
            )
            admission_results.append(None)
        except BaseException as exc:  # asserted below, after both threads are joined
            admission_errors.append(exc)
        finally:
            admission_finished.set()

    def append_racing_frame() -> None:
        append_attempted.set()
        try:
            push(racing_frame_id, root_frame_id, 1)
        except BaseException as exc:  # asserted below, after both threads are joined
            append_errors.append(exc)
        finally:
            append_finished.set()

    admission_thread = threading.Thread(target=admit_nested)
    append_thread = threading.Thread(target=append_racing_frame)
    admission_thread.start()
    try:
        assert walk_started.wait(timeout=5)
        append_thread.start()
        assert append_attempted.wait(timeout=5)

        # The writer has attempted Journal.append while the reader is stopped in
        # dict.values().  It must be waiting on the very projection transaction.
        assert not append_finished.wait(timeout=0.2)

        release_walk.set()
        assert reservation_write_started.wait(timeout=5)

        # A lock limited to descendants() is insufficient: reserve is part of the
        # same check-and-reserve transaction and the append must still be excluded.
        assert not append_finished.wait(timeout=0.2)

        release_reservation_write.set()
        assert admission_finished.wait(timeout=5)
        assert append_finished.wait(timeout=5)
    finally:
        release_walk.set()
        release_reservation_write.set()
        admission_thread.join(timeout=5)
        if append_thread.ident is not None:
            append_thread.join(timeout=5)

    assert not admission_thread.is_alive()
    assert not append_thread.is_alive()
    assert admission_results == [None]
    assert not admission_errors
    assert not append_errors
    assert reservation_committed.is_set()

    # The only admissible serial order is a one-frame nested reservation plus the
    # racing push, exactly consuming (and never exceeding) the root subtree budget.
    descendants = engine.projection.descendants(root_frame_id)
    frames = len(descendants) + engine._reservation_frames[root_frame_id]  # noqa: SLF001
    spend = sum(policy.rig_cost(frame.rig) for frame in descendants)
    spend += engine._reservation_spend[root_frame_id]  # noqa: SLF001
    assert frames == policy.max_subtree_frames
    assert spend == policy.max_subtree_spend
