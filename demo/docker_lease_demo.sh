#!/usr/bin/env bash
#
# Heartbeat-lease walkthrough against the running two-worker service.
#
# It never changes the host clock.  A rebuild owner worker is frozen with
# SIGSTOP (its PID stays alive but heartbeats stop); after the real elapsed
# lease another worker takes over; SIGCONT then resumes the old worker, whose
# heartbeats, failure bookkeeping and index activation are fenced off.
#
# Usage:
#   docker compose up --build -d
#   REBUILD_LEASE_SECONDS=5 ./demo/docker_lease_demo.sh
#
# Optional: BASE=http://host:port LEASE=5 ./demo/docker_lease_demo.sh
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:${API_PORT:-8000}}"
LEASE="${LEASE:-${REBUILD_LEASE_SECONDS:-5}}"
CURL=(curl -sS)

echo "==> service health"
"${CURL[@]}" "$BASE/healthz"; echo

echo "==> seed a few accented pages"
for i in 01 02 03 04 05 06; do
  "${CURL[@]}" -X PUT "$BASE/api/pages/lease-$i" \
    -H 'Content-Type: application/json' \
    -d "{\"title\":\"Crème brûlée $i\",\"body\":\"Le café $i et le señor naïve résumé.\"}" >/dev/null
done

echo "==> trigger a rebuild"
TRIGGER=$("${CURL[@]}" -X POST "$BASE/api/index/rebuild" \
  -H 'Content-Type: application/json' -d '{"accent_policy":"fold"}')
echo "$TRIGGER" | python3 -m json.tool
JOB=$(echo "$TRIGGER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["job"]["job_id"])')

sleep 1
OWNER=$("${CURL[@]}" "$BASE/api/index/jobs/$JOB" | python3 -c 'import sys,json; print(json.load(sys.stdin)["owner_pid"])')
echo "==> owner worker PID is $OWNER; freezing it with SIGSTOP (no host clock change)"
kill -STOP "$OWNER"
trap 'kill -CONT "$OWNER" 2>/dev/null || true' EXIT

echo "==> re-trigger inside the lease: the same job must come back"
"${CURL[@]}" -X POST "$BASE/api/index/rebuild" \
  -H 'Content-Type: application/json' -d '{"accent_policy":"fold"}' \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print("reused:", d["reused_existing_rebuild"], "job:", d["job"]["job_id"])'

echo "==> waiting ${LEASE}s of real elapsed time for the heartbeat lease to expire"
sleep "$((LEASE + 1))"

echo "==> re-trigger after the lease: a new job must take over"
TAKEOVER=$("${CURL[@]}" -X POST "$BASE/api/index/rebuild" \
  -H 'Content-Type: application/json' -d '{"accent_policy":"fold"}')
NEW_JOB=$(echo "$TAKEOVER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["job"]["job_id"])')
echo "created: $(echo "$TAKEOVER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["created"])')  new job: $NEW_JOB"
"${CURL[@]}" "$BASE/api/index/jobs/$JOB" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print("old job:", d["state"], "-", d["error"])'

echo "==> resume the frozen old worker with SIGCONT; it must be fenced"
kill -CONT "$OWNER"
trap - EXIT

# Wait for the takeover job to finish.
for _ in $(seq 1 60); do
  STATE=$("${CURL[@]}" "$BASE/api/index/jobs/$NEW_JOB" | python3 -c 'import sys,json; print(json.load(sys.stdin)["state"])')
  [ "$STATE" = "succeeded" ] && break
  sleep 0.5
done

echo "==> final status"
"${CURL[@]}" "$BASE/api/index/status" | python3 -m json.tool
echo "new job state:   $("${CURL[@]}" "$BASE/api/index/jobs/$NEW_JOB" | python3 -c 'import sys,json; print(json.load(sys.stdin)["state"])')"
echo "old job state:   $("${CURL[@]}" "$BASE/api/index/jobs/$JOB" | python3 -c 'import sys,json; print(json.load(sys.stdin)["state"])')"
echo "folded search:   $("${CURL[@]}" --get "$BASE/api/search" --data-urlencode 'q=cafe' | python3 -c 'import sys,json; print(sorted(h["page_id"] for h in json.load(sys.stdin)["hits"]))')"

echo
echo "In-process forward/backward wall-jump demo (also host-clock free):"
echo "  docker compose exec api python -m demo.lease_demo"
