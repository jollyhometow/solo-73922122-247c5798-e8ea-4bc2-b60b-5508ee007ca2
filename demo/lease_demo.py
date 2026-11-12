"""Heartbeat-lease demonstrations that never modify the host clock.

Run locally:

    python3 -m demo.lease_demo

Run inside the Compose container:

    docker compose exec api python -m demo.lease_demo

Three scenarios are shown:

1. Wall-clock **forward jump** while a rebuild is active: the healthy job is
   never judged expired and no competing rebuild is created.
2. Wall-clock **backward jump** with a frozen owner: once the real elapsed
   lease runs out the job is reclaimed even though the wall clock is in the
   past.
3. **Paused owner takeover**: a SIGSTOP-style frozen worker (PID alive, no
   heartbeats) is taken over after the lease; when the old worker resumes its
   heartbeats, failure bookkeeping and index activation are fenced off and
   cannot affect the new job.

The wall-clock jumps move only the in-process display clock
(:func:`app.clock.set_wall_offset`); ``CLOCK_MONOTONIC`` and the host clock are
untouched.
"""

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

from fastapi.testclient import TestClient

from app import clock
from app.config import SETTINGS
from app.main import app
from app.rebuild import cancel_local_rebuilds

LEASE_SECONDS = 1.0
PAGE_COUNT = 8


def _banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def _seed(client: TestClient) -> None:
    for index in range(PAGE_COUNT):
        response = client.put(
            f"/api/pages/demo-{index:02d}",
            json={
                "title": f"Crème brûlée {index:02d}",
                "body": f"Le café numéro {index:02d}; le señor lit un naïve résumé.",
            },
        )
        response.raise_for_status()


def _trigger(client: TestClient) -> dict:
    response = client.post("/api/index/rebuild", json={"accent_policy": "fold"})
    response.raise_for_status()
    return response.json()


def _wait_terminal(client: TestClient, job_id: str, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = client.get(f"/api/index/jobs/{job_id}").json()
        if job["state"] in {"succeeded", "failed", "interrupted"}:
            return job
        time.sleep(0.02)
    raise RuntimeError(f"job {job_id} did not finish")


def _wait_active(client: TestClient, job_id: str) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        job = client.get(f"/api/index/jobs/{job_id}").json()
        if job["state"] in {"building", "catching_up", "activating"}:
            return
        time.sleep(0.02)
    raise RuntimeError("rebuild never became active")


class _PauseGate:
    """Blocks the rebuild worker at a batch boundary, like SIGSTOP."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self, hook_job_id: str) -> None:
        if hook_job_id == self.job_id:
            self.entered.set()
            self.release.wait()


def scenario_forward_jump(client: TestClient) -> None:
    _banner("Scenario 1 — wall clock jumps FORWARD 30 days during a rebuild")
    _seed(client)
    body = _trigger(client)
    job_id = body["job"]["job_id"]
    print(f"rebuild started:            job={job_id} state=queued")
    _wait_active(client, job_id)
    print("worker heartbeating:        state=building (monotonic lease)")

    clock.set_wall_offset(60 * 60 * 24 * 30)
    print("wall clock shifted:         +30 days (CLOCK_MONOTONIC untouched)")

    duplicate = _trigger(client)
    assert duplicate["reused_existing_rebuild"] is True
    assert duplicate["job"]["job_id"] == job_id
    print("re-trigger during jump:     reused_existing_rebuild=True, same job id")

    job = _wait_terminal(client, job_id)
    assert job["state"] == "succeeded"
    print(f"rebuild outcome:            {job['state']} — no false expiry, no duplicate job")


def scenario_backward_jump(client: TestClient) -> None:
    _banner("Scenario 2 — wall clock jumps BACKWARD 30 days, owner frozen")
    _seed(client)
    body = _trigger(client)
    job_id = body["job"]["job_id"]
    _wait_active(client, job_id)

    gate = _PauseGate(job_id)
    clock.set_pause_hook(gate)
    assert gate.entered.wait(timeout=5)
    print("owner frozen:               PID alive, heartbeats stopped")

    clock.set_wall_offset(-60 * 60 * 24 * 30)
    print("wall clock shifted:         -30 days (CLOCK_MONOTONIC untouched)")

    early = _trigger(client)
    assert early["reused_existing_rebuild"] is True
    print("re-trigger within lease:    still returns the original job")

    print(f"waiting out real lease:     {LEASE_SECONDS:.0f}s of elapsed time ...")
    time.sleep(LEASE_SECONDS + 0.5)
    takeover = _trigger(client)
    assert takeover["created"] is True
    new_job_id = takeover["job"]["job_id"]
    print(f"re-trigger after lease:     NEW job claimed: {new_job_id}")

    old_job = client.get(f"/api/index/jobs/{job_id}").json()
    assert old_job["state"] == "failed"
    print(f"old job outcome:            {old_job['state']} ({old_job['error']})")

    gate.release.set()
    new_job = _wait_terminal(client, new_job_id)
    assert new_job["state"] == "succeeded"
    print(f"new job outcome:            {new_job['state']} despite the past-set wall clock")


def scenario_paused_takeover(client: TestClient) -> None:
    _banner("Scenario 3 — paused owner is taken over; resumed owner is fenced")
    _seed(client)
    body = _trigger(client)
    job_id = body["job"]["job_id"]
    _wait_active(client, job_id)

    gate = _PauseGate(job_id)
    clock.set_pause_hook(gate)
    assert gate.entered.wait(timeout=5)
    print("old worker paused:          blocked at a batch boundary, no lock held")

    print(f"waiting out real lease:     {LEASE_SECONDS:.0f}s ...")
    time.sleep(LEASE_SECONDS + 0.5)
    takeover = _trigger(client)
    assert takeover["created"] is True
    new_job_id = takeover["job"]["job_id"]
    print(f"takeover job claimed:       {new_job_id}")

    new_job = _wait_terminal(client, new_job_id)
    assert new_job["state"] == "succeeded"
    print("takeover rebuild succeeded: new generation active, old index retained until swap")

    print("resuming the old worker ...")
    gate.release.set()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        old_job = client.get(f"/api/index/jobs/{job_id}").json()
        if old_job["state"] in {"failed", "interrupted"}:
            break
        time.sleep(0.02)
    assert old_job["state"] in {"failed", "interrupted"}
    print(f"resumed old worker fenced:  old job={old_job['state']}, claim token rejected")

    final_new = client.get(f"/api/index/jobs/{new_job_id}").json()
    assert final_new["state"] == "succeeded"
    status = client.get("/api/index/status").json()
    assert status["active_policy"] == "fold"
    print("new job/index untouched:    new job still 'succeeded', active_policy=fold")

    # Corrections/deletes still converge on the new index after the takeover.
    client.put(
        "/api/pages/demo-00",
        json={"title": "Corrigé", "body": "Le café remplacé; le résumé corrigé."},
    )
    client.delete("/api/pages/demo-01")
    hits = client.get("/api/search", params={"q": "cafe"}).json()
    ids = {hit["page_id"] for hit in hits["hits"]}
    assert "demo-00" in ids and "demo-01" not in ids
    print("post-takeover writes:       correction searchable, deletion stays deleted")


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="lease-demo-"))
    object.__setattr__(SETTINGS, "db_path", workdir / "demo.db")
    object.__setattr__(SETTINGS, "rebuild_lease_seconds", LEASE_SECONDS)
    object.__setattr__(SETTINGS, "rebuild_batch_size", 2)
    object.__setattr__(SETTINGS, "rebuild_batch_delay_ms", 60)

    with TestClient(app) as client:
        try:
            scenario_forward_jump(client)
            clock.reset_test_controls()
            scenario_backward_jump(client)
            clock.reset_test_controls()
            scenario_paused_takeover(client)
        finally:
            clock.reset_test_controls()
            cancel_local_rebuilds()

    _banner("All lease scenarios passed — wall-clock jumps never alter the lease.")


if __name__ == "__main__":
    main()
