"""Clocks used for lease decisions and for human-facing timestamps.

Two different clocks are used on purpose:

* :func:`mono_now` wraps ``CLOCK_MONOTONIC`` (Python's :func:`time.monotonic`).
  It advances with the real elapsed time, cannot go backwards, and is **not**
  affected when an operator corrects the wall clock (a jump either forwards or
  backwards).  Heartbeat leases are therefore always measured against elapsed
  time.  The monotonic clock is a kernel-wide clock, so both API worker
  processes on the same host read comparable values; readings are additionally
  scoped by :func:`boot_id`, because monotonic values from a previous host boot
  are not comparable.
* :func:`wall_now` wraps the adjustable wall clock.  It is used only for the
  UTC timestamps shown by the status API, never for lease arithmetic.

The test/demo helpers in this module let a demonstration shift the wall clock
or pause a worker **without touching the host clock**.  They are inert in
normal operation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import time

_BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")
_UNKNOWN_BOOT = "unknown-boot"

# Test/demo only.  Production code leaves the offset at zero and the hook unset.
_wall_offset_seconds = 0.0
_pause_hook: Callable[[str], None] | None = None


def boot_id() -> str:
    """Stable identifier of the current host boot cycle.

    Monotonic readings taken before a reboot are not comparable to readings
    taken afterwards, so lease records carry the boot id alongside them.
    """

    try:
        value = _BOOT_ID_PATH.read_text().strip()
    except OSError:
        return _UNKNOWN_BOOT
    return value or _UNKNOWN_BOOT


def mono_now() -> float:
    """Monotonic elapsed-time reading (``CLOCK_MONOTONIC``); wall-jump immune."""

    return time.monotonic()


def wall_now() -> float:
    """Adjustable wall-clock epoch, used solely for UTC display fields."""

    return time.time() + _wall_offset_seconds


def set_wall_offset(seconds: float) -> None:
    """Shift the in-process wall clock for demos; monotonic time is untouched."""

    global _wall_offset_seconds
    _wall_offset_seconds = float(seconds)


def set_pause_hook(hook: Callable[[str], None] | None) -> None:
    """Install a worker pause hook for the takeover demonstration."""

    global _pause_hook
    _pause_hook = hook


def run_pause_hook(job_id: str) -> None:
    """Invoke the demo pause hook at a safe worker batch boundary."""

    if _pause_hook is not None:
        _pause_hook(job_id)


def reset_test_controls() -> None:
    """Remove every test/demo clock adjustment."""

    global _wall_offset_seconds, _pause_hook
    _wall_offset_seconds = 0.0
    _pause_hook = None
