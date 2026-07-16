"""Engine-owned active-period slot scheduling for configured rigs."""
from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable


class SlotSchedulerStopped(RuntimeError):
    """A slot waiter cannot run because the engine is shutting down."""


class RigSlotScheduler:
    """FIFO per-rig slot pools; rigs without a capacity remain unlimited."""

    def __init__(self, capacities: dict[str, int]) -> None:
        if any(capacity <= 0 for capacity in capacities.values()):
            raise ValueError("rig slot capacities must be positive")
        self._capacities = dict(capacities)
        self._condition = threading.Condition(threading.RLock())
        self._owners: dict[str, dict[int, str]] = {
            rig: {} for rig in capacities
        }
        self._waiters: dict[str, list[object]] = {
            rig: [] for rig in capacities
        }
        self._stopped = False

    @staticmethod
    def _stable_lane(frame_id: str, capacity: int) -> int:
        digest = hashlib.sha256(frame_id.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % capacity

    def acquire(
        self,
        rig: str,
        frame_id: str,
        *,
        previous: int | None,
        cancellation: threading.Event | None,
        on_queued: Callable[[], None],
    ) -> int | None:
        capacity = self._capacities.get(rig)
        if capacity is None:
            return None
        token = object()
        with self._condition:
            if self._stopped:
                raise SlotSchedulerStopped("rig slot scheduler is shut down")
            owners = self._owners[rig]
            waiters = self._waiters[rig]
            if not waiters and len(owners) < capacity:
                lane = self._choose_lane(
                    frame_id, capacity, owners, previous
                )
                owners[lane] = frame_id
                return lane
            waiters.append(token)
        try:
            on_queued()
        except BaseException:
            with self._condition:
                self._discard_waiter(rig, token)
            raise

        with self._condition:
            while True:
                if self._stopped:
                    self._discard_waiter(rig, token)
                    raise SlotSchedulerStopped("rig slot scheduler is shut down")
                if cancellation is not None and cancellation.is_set():
                    self._discard_waiter(rig, token)
                    raise SlotSchedulerStopped("slot wait was cancelled")
                owners = self._owners[rig]
                waiters = self._waiters[rig]
                if waiters and waiters[0] is token and len(owners) < capacity:
                    lane = self._choose_lane(
                        frame_id, capacity, owners, previous
                    )
                    owners[lane] = frame_id
                    waiters.pop(0)
                    return lane
                self._condition.wait(timeout=0.05)

    def _choose_lane(
        self,
        frame_id: str,
        capacity: int,
        owners: dict[int, str],
        previous: int | None,
    ) -> int:
        free = set(range(capacity)).difference(owners)
        preferred = (
            previous
            if previous is not None and 0 <= previous < capacity
            else self._stable_lane(frame_id, capacity)
        )
        return preferred if preferred in free else min(free)

    def _discard_waiter(self, rig: str, token: object) -> None:
        waiters = self._waiters[rig]
        if token in waiters:
            waiters.remove(token)
            self._condition.notify_all()

    def release(self, rig: str, frame_id: str, lane: int | None) -> None:
        if lane is None:
            return
        with self._condition:
            owner = self._owners[rig].get(lane)
            if owner != frame_id:
                raise RuntimeError(
                    f"slot {rig}:{lane} belongs to {owner!r}, not {frame_id!r}"
                )
            del self._owners[rig][lane]
            self._condition.notify_all()

    def shutdown(self) -> None:
        """Wake every waiter; active owners release through frame cleanup."""
        with self._condition:
            self._stopped = True
            self._condition.notify_all()
