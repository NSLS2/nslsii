"""RunEngine suspenders."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable
from bluesky import RunEngine
from bluesky.suspenders import SuspendFloor

from nslsii.ophyd_async.devices.machine import NSLS2OpsMode, NSLS2StorageRing


def _call_on_loop(loop, func: Callable[[], None]) -> None:
    """Run ``func`` on the RunEngine's event loop and wait for it."""
    if threading.get_ident() == getattr(loop, "_thread_id", None):
        func()
        return
    done = threading.Event()
    errors: list[BaseException] = []

    def call():
        try:
            func()
        except BaseException as err:
            errors.append(err)
        finally:
            done.set()

    loop.call_soon_threadsafe(call)
    done.wait()
    if errors:
        raise errors[0]


class SuspendFloorUnlessOpsMode(SuspendFloor):
    """Suspend when the ring's beam current falls below a threshold, unless in a skipped ring mode.

    Install it whatever the ring's mode: the mode is checked every time the
    suspender is evaluated, so a ring entering maintenance releases a tripped
    suspender, and a ring returning to operations with beam still low trips it.
    Until the mode has been read, the suspender acts like a plain `SuspendFloor`.

    Parameters
    ----------
    storage_ring : NSLS2StorageRing
        The storage ring, whose ``beam_current`` is watched and whose
        ``operating_mode`` decides whether a low current suspends.
    suspend_thresh : float
        Suspend if the beam current falls below this value, in mA.
    skip_modes : Iterable[NSLS2OpsMode]
        Modes in which a low beam current does not suspend the RunEngine.
    **kwargs
        Passed to `SuspendFloor` (``resume_thresh``, ``sleep``, ...).
    """

    def __init__(
        self,
        storage_ring: NSLS2StorageRing,
        suspend_thresh: float,
        *,
        skip_modes: Iterable[NSLS2OpsMode],
        **kwargs,
    ):
        super().__init__(storage_ring.beam_current, suspend_thresh, **kwargs)
        self._ops_mode = storage_ring.operating_mode
        self._skip_modes = frozenset(skip_modes)
        self._mode = None
        # The base _lock is not reentrant, and _set_mode calls back into __call__.
        self._eval_lock = threading.RLock()

    @property
    def _skipping(self) -> bool:
        return self._mode in self._skip_modes

    def _should_suspend(self, value):
        return not self._skipping and super()._should_suspend(value)

    def _should_resume(self, value):
        return self._skipping or super()._should_resume(value)

    def _on_mode_reading(self, reading):
        self._set_mode(reading[self._ops_mode.name]["value"])

    def __call__(self, value, **kwargs):
        with self._eval_lock:
            super().__call__(value, **kwargs)

    def _set_mode(self, mode):
        with self._eval_lock:
            self._mode = mode
            if self._last_value is None:
                return
            value = self._last_value
            if self._implements_protocol:
                value = {self._sig.name: {"value": value}}
            self(value=value)

    def install(self, RE: RunEngine, *, event_type=None):
        # The mode first, so the first beam reading is judged against it.
        _call_on_loop(
            RE.loop, lambda: self._ops_mode.subscribe_reading(self._on_mode_reading)
        )
        super().install(RE, event_type=event_type)

    def remove(self):
        # The base remove() clears self.RE, so take it first
        RE = self.RE
        super().remove()
        if RE is not None:
            _call_on_loop(
                RE.loop, lambda: self._ops_mode.clear_sub(self._on_mode_reading)
            )

    def _get_justification(self):
        just = super()._get_justification()
        if just and self._mode is not None:
            just = f"{just} (ring mode: {getattr(self._mode, 'value', self._mode)})"
        return just
