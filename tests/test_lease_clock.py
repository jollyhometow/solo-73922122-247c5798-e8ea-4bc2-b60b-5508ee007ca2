"""Heartbeat lease behaviour under wall-clock corrections and process pauses.

These tests never change the host clock: wall-clock forward/backward jumps are
simulated through the in-process clock seam in :mod:`app.clock`, and a frozen
owner process is simulated by blocking the rebuild worker at a safe batch
boundary while its PID stays alive.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import clock
from app.config import SETTINGS
from conftest import configure_database

LEASE_SECONDS = 3.0


@pytest.fixture
def client(tmp_path: Path):
    configure_database(tmp_path / "lease.db")
    from app.main import app
    from app.rebuild import cancel_local_rebuilds

    with TestClient(app) as test_client:
        yield test_client
    cancel_local_rebuilds()


def _start_paced_rebuild(client: TestClient) -> str:
    response = client.post("/api/index/rebuild", json={"accent_policy": "fold"})
    assert response.status_code == 202, response.text
    payload = response.json()
    assert payload["created"] is True
    return payload["job"]["job_id"]


def _wait_state(client: TestClient, job_id: str, states: set[str], timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = client.get(f"/api/index/jobs/{job_id}").json()
        if job["state"] in states:
            return job
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} never reached {states}")


def _seed_pages(client: TestClient, count: int = 20) -> None:
    for index in range(count):
        response = client.put(
            f"/api/pages/lease-{index:02d}",
            json={
                "title": f"Crème brûlée {index:02d}",
                "body": f"Le café numéro {index:02d} et le señor naïve résumé.",
            },
        )
        assert response.status_code == 200


class _FreezeGate:
    """Blocks a rebuild worker at a batch boundary, like SIGSTOP.

    The gate is installed before the rebuild is triggered, and it captures the
    very first job that reaches a batch boundary.  The worker is therefore
    guaranteed to stop before its first batch commits, even for a tiny dataset.
    """

    def __init__(self) -> None:
        self.job_id: str | None = None
        self.entered = threading.Event()
        self.release = threading.Event()
        self._lock = threading.Lock()

    def __call__(self, hook_job_id: str) -> None:
        with self._lock:
            if self.job_id is None:
                self.job_id = hook_job_id
            should_block = hook_job_id == self.job_id
        if should_block:
            self.entered.set()
            self.release.wait()


def _start_paused_rebuild(client: TestClient) -> tuple[str, _FreezeGate]:
    """Trigger a rebuild whose worker is frozen before its first batch commits."""

    gate = _FreezeGate()
    # Install the hook first; no rebuild thread exists yet, so it cannot be
    # bypassed by a fast-finishing worker.
    clock.set_pause_hook(gate)
    response = client.post("/api/index/rebuild", json={"accent_policy": "fold"})
    assert response.status_code == 202, response.text
    payload = response.json()
    assert payload["created"] is True
    job_id = payload["job"]["job_id"]
    assert gate.entered.wait(timeout=10), "worker never reached the pause point"
    assert gate.job_id == job_id
    return job_id, gate


def test_forward_wall_jump_does_not_expire_healthy_lease(client: TestClient):
    _seed_pages(client)
    job_id = _start_paced_rebuild(client)
    _wait_state(client, job_id, {"building", "catching_up", "activating"})

    # Operator corrects the server clock far into the future.  The monotonic
    # lease is unchanged, so the active job must be returned rather than
    # reclaimed.
    clock.set_wall_offset(60 * 60 * 24 * 30)
    duplicate = client.post("/api/index/rebuild", json={"accent_policy": "fold"})
    assert duplicate.status_code == 202
    body = duplicate.json()
    assert body["reused_existing_rebuild"] is True
    assert body["job"]["job_id"] == job_id

    status = client.get("/api/index/status").json()
    assert status["rebuild_in_progress"] is True
    assert status["current_rebuild_job"]["job_id"] == job_id

    job = _wait_state(client, job_id, {"succeeded"})
    assert job["error"] is None
    status = client.get("/api/index/status").json()
    assert status["active_policy"] == "fold"
    # Only one rebuild generation was ever created: no competing job spawned.
    assert len([g for g in status["generations"] if g["state"] == "failed"]) == 0


def test_backward_wall_jump_still_allows_takeover_after_real_lease(client: TestClient):
    _seed_pages(client)
    job_id, gate = _start_paused_rebuild(client)

    # A backwards jump must not make a stuck heartbeat look permanently fresh.
    clock.set_wall_offset(-60 * 60 * 24 * 30)

    # While frozen, even a backwards wall clock reports the same job (lease not
    # yet elapsed in monotonic time).
    early = client.post("/api/index/rebuild", json={"accent_policy": "fold"}).json()
    assert early["reused_existing_rebuild"] is True
    assert early["job"]["job_id"] == job_id

    # Real elapsed time passes; the monotonic lease expires despite the wall
    # clock being 30 days in the past.
    time.sleep(LEASE_SECONDS + 1.0)
    takeover = client.post("/api/index/rebuild", json={"accent_policy": "fold"})
    assert takeover.status_code == 202
    new_body = takeover.json()
    assert new_body["created"] is True
    new_job_id = new_body["job"]["job_id"]
    assert new_job_id != job_id

    old_job = client.get(f"/api/index/jobs/{job_id}").json()
    assert old_job["state"] == "failed"
    assert "lease expired" in (old_job["error"] or "")

    # Release the old worker now that the new job owns the slot.
    gate.release.set()
    new_job = _wait_state(client, new_job_id, {"succeeded"})
    assert new_job["state"] == "succeeded"

    status = client.get("/api/index/status").json()
    assert status["active_policy"] == "fold"
    assert status["rebuild_in_progress"] is False

    # The old worker, once resumed, must not have flipped the new job's state.
    assert client.get(f"/api/index/jobs/{new_job_id}").json()["state"] == "succeeded"
    assert client.get(f"/api/index/jobs/{job_id}").json()["state"] == "failed"


def test_paused_owner_is_taken_over_and_resumed_owner_is_fenced(client: TestClient):
    _seed_pages(client)
    job_id, gate = _start_paused_rebuild(client)

    # The owner PID is alive (this very process) but heartbeats have stopped.
    time.sleep(LEASE_SECONDS + 1.0)
    takeover = client.post("/api/index/rebuild", json={"accent_policy": "fold"})
    assert takeover.status_code == 202
    new_job_id = takeover.json()["job"]["job_id"]
    assert new_job_id != job_id

    old_job = client.get(f"/api/index/jobs/{job_id}").json()
    assert old_job["state"] == "failed"

    # The new worker must not hit the old worker's pause hook.
    new_job = _wait_state(client, new_job_id, {"succeeded"})
    assert new_job["state"] == "succeeded"

    status = client.get("/api/index/status").json()
    assert status["active_policy"] == "fold"

    # Now resume the frozen old worker.  Its claim token no longer matches an
    # active job, so every heartbeat/finalise/activation path must be fenced.
    gate.release.set()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        old_after = client.get(f"/api/index/jobs/{job_id}").json()
        if old_after["state"] in {"interrupted", "failed"}:
            break
        time.sleep(0.02)
    else:
        raise AssertionError("resumed old worker never reached a terminal state")

    # The new job and the active index are untouched by the resumed worker.
    assert client.get(f"/api/index/jobs/{new_job_id}").json()["state"] == "succeeded"
    status = client.get("/api/index/status").json()
    assert status["active_policy"] == "fold"
    assert status["rebuild_in_progress"] is False

    # Correctness still holds: corrected and deleted pages are reflected by the
    # new generation, and the old index was retained until the successful swap.
    corrected = client.put(
        "/api/pages/lease-00",
        json={"title": "Corrigé", "body": "Le café remplacé et le résumé corrigé."},
    )
    assert corrected.status_code == 200
    assert client.delete("/api/pages/lease-01").status_code == 200
    hits = client.get("/api/search", params={"q": "cafe"}).json()
    ids = {hit["page_id"] for hit in hits["hits"]}
    assert "lease-00" in ids
    assert "lease-01" not in ids


def test_non_owner_process_restart_neither_reclaims_nor_resets_lease(
    client: TestClient, tmp_path: Path
):
    _seed_pages(client)
    job_id = _start_paced_rebuild(client)
    job = _wait_state(client, job_id, {"building", "catching_up", "activating"})
    owner_pid = job["owner_pid"]

    # Simulate the *other* API worker restarting while this process keeps
    # running the rebuild: init_db() must not touch the live job or its
    # heartbeat columns.
    from app.db import connect, init_db

    before = _job_row(job_id)
    init_db()
    init_db()
    after = _job_row(job_id)

    assert after["state"] == before["state"]
    assert after["heartbeat_mono"] == before["heartbeat_mono"]
    assert after["claim_token"] == before["claim_token"]
    assert after["owner_pid"] == owner_pid

    # A trigger from the restarted worker returns the same job before expiry.
    duplicate = client.post("/api/index/rebuild", json={"accent_policy": "fold"}).json()
    assert duplicate["reused_existing_rebuild"] is True
    assert duplicate["job"]["job_id"] == job_id

    finished = _wait_state(client, job_id, {"succeeded"})
    assert finished["state"] == "succeeded"


def _job_row(job_id: str) -> sqlite3.Row:
    from app.db import connect

    conn = connect()
    try:
        return conn.execute(
            "SELECT * FROM rebuild_jobs WHERE id = ?", (job_id,)
        ).fetchone()
    finally:
        conn.close()


def test_database_forward_compatible_with_legacy_schema(tmp_path: Path):
    db_path = tmp_path / "legacy.db"
    _create_legacy_database(db_path)

    configure_database(db_path)
    from app.archive import search, upsert_page
    from app.db import claim_rebuild, get_status, init_db

    init_db()

    # The monotonic lease columns were added without dropping any data.
    conn = sqlite3.connect(db_path)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(rebuild_jobs)")}
    finally:
        conn.close()
    assert {"heartbeat_mono", "created_mono", "boot_id"} <= columns

    upsert_page("legacy-1", "Café", "Le café et le señor.")

    # The legacy active job is reclaimable through the documented paths.
    status = get_status()
    assert status["active_policy"] == "keep"
    assert status["latest_rebuild_job"]["state"] in {"interrupted", "failed"}

    claimed = claim_rebuild("fold")
    assert claimed["created"] is True
    job_id = claimed["job"]["job_id"]

    from app.rebuild import start_rebuild_thread

    start_rebuild_thread(
        job_id=job_id,
        generation_id=claimed["job"]["generation_id"],
        policy="fold",
        claim_token=claimed["worker_token"],
    )
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        from app.db import get_job

        job = get_job(job_id)
        if job["state"] in {"succeeded", "failed", "interrupted"}:
            break
        time.sleep(0.03)
    else:
        raise AssertionError("rebuild on upgraded database did not finish")
    assert job["state"] == "succeeded"
    assert search("cafe", limit=10, offset=0)["hits"][0]["page_id"] == "legacy-1"


def test_status_keeps_utc_string_and_epoch_fields(client: TestClient):
    _seed_pages(client)
    job_id = _start_paced_rebuild(client)
    _wait_state(client, job_id, {"succeeded"})

    status = client.get("/api/index/status").json()
    job = status["latest_rebuild_job"]
    assert job["job_id"] == job_id
    # Original response format: ISO UTC strings and a numeric heartbeat epoch.
    for field in ("created_at", "started_at", "finished_at"):
        assert isinstance(job[field], str) and job[field].endswith("Z")
    assert isinstance(job["heartbeat_at"], float)
    assert isinstance(status["server_time"], str) and status["server_time"].endswith("Z")

    # Wall-clock jumps move only the display clock; monotonic lease columns
    # remain internal and are never exposed in the status payload.
    clock.set_wall_offset(3600)
    again = client.get("/api/index/status").json()
    assert "heartbeat_mono" not in again["latest_rebuild_job"]
    assert "boot_id" not in again["latest_rebuild_job"]


def _create_legacy_database(db_path: Path) -> None:
    """Build a database with the pre-fix schema plus one active legacy job."""

    from app.db import fts_table_name
    import socket

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(
            """
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE pages (
                page_id TEXT PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL,
                rev INTEGER NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE changes (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, page_id TEXT NOT NULL,
                page_ref INTEGER, op TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
                title TEXT, body TEXT, rev INTEGER NOT NULL, changed_at TEXT NOT NULL
            );
            CREATE TABLE generations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, policy TEXT NOT NULL,
                state TEXT NOT NULL, fts_table TEXT NOT NULL DEFAULT '',
                base_seq INTEGER NOT NULL DEFAULT 0, applied_seq INTEGER NOT NULL DEFAULT 0,
                doc_count INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
                activated_at TEXT, finished_at TEXT, error TEXT
            );
            CREATE TABLE rebuild_jobs (
                id TEXT PRIMARY KEY, generation_id INTEGER NOT NULL,
                from_policy TEXT NOT NULL, to_policy TEXT NOT NULL,
                state TEXT NOT NULL, claim_token TEXT NOT NULL,
                owner_host TEXT, owner_pid INTEGER, owner_proc_start TEXT,
                created_at TEXT NOT NULL, created_epoch REAL NOT NULL,
                started_at TEXT, heartbeat_at REAL, finished_at TEXT,
                base_seq INTEGER NOT NULL DEFAULT 0, applied_seq INTEGER NOT NULL DEFAULT 0,
                processed_documents INTEGER NOT NULL DEFAULT 0,
                total_documents INTEGER NOT NULL DEFAULT 0, error TEXT, message TEXT
            );
            """
        )
        table = fts_table_name(1)
        conn.execute(
            f"CREATE VIRTUAL TABLE {table} USING fts5("
            "page_id UNINDEXED, title, body, tokenize = 'unicode61 remove_diacritics 0')"
        )
        conn.execute(
            "INSERT INTO generations(policy, state, fts_table, created_at, activated_at) "
            "VALUES ('keep', 'active', ?, '2026-01-01T00:00:00.000Z', "
            "'2026-01-01T00:00:00.000Z')",
            (table,),
        )
        conn.execute("INSERT INTO settings VALUES ('active_generation_id', '1')")
        conn.execute("INSERT INTO settings VALUES ('active_policy', 'keep')")
        # An active job owned by an impossible PID on this host: the startup
        # orphan sweep must mark it interrupted even with a fresh heartbeat.
        conn.execute(
            """
            INSERT INTO rebuild_jobs
                (id, generation_id, from_policy, to_policy, state, claim_token,
                 owner_host, owner_pid, owner_proc_start, created_at, created_epoch,
                 heartbeat_at, message)
            VALUES ('legacy-job', 1, 'keep', 'fold', 'building', 'legacy-token',
                    ?, 999999, NULL, '2026-01-01T00:00:00.000Z', ?, ?,
                    'legacy active job without monotonic columns')
            """,
            (socket.gethostname(), time.time(), time.time()),
        )
        conn.commit()
    finally:
        conn.close()
