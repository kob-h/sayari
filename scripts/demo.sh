#!/usr/bin/env bash
# End-to-end demo of all six required scenarios against a running stack.
#
#   ./start.sh          # bring the stack up first
#   ./scripts/demo.sh
#
# Requires: curl, python3 (for JSON formatting; ships with macOS/most Linux).
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8080}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"

# --- helpers ----------------------------------------------------------------
jqf() { python3 -c 'import sys,json; d=json.load(sys.stdin); print(json.dumps(d,indent=2))'; }
field() { python3 -c "import sys,json;print(json.load(sys.stdin).get('$1',''))"; }
nested() { python3 -c "import sys,json;d=json.load(sys.stdin);print(d['$1']['$2'])"; }

post_doc() { # id, file, mode
  local id="$1" file="$2" mode="${3:-partial}"
  python3 - "$id" "$file" "$mode" <<'PY' | curl -fsS -X POST "$BASE/process" -H 'Content-Type: application/json' -d @-
import json,sys
doc_id, path, mode = sys.argv[1], sys.argv[2], sys.argv[3]
print(json.dumps({"document_id": doc_id, "text": open(path).read(), "mode": mode}))
PY
}
export BASE

status()      { curl -fsS "$BASE/documents/$1/status"; }
status_field(){ status "$1" | field "$2"; }

wait_until_completed() { # id, timeout_seconds
  local id="$1" timeout="${2:-60}"
  for _ in $(seq 1 "$timeout"); do
    [ "$(status "$id" | field status)" = "COMPLETED" ] && return 0
    sleep 1
  done
  echo "!! timed out waiting for $id to complete" >&2; return 1
}

hr() { printf '\n=====================================================================\n'; }
title() { hr; echo "  $1"; hr; }

# ---------------------------------------------------------------------------
title "SCENARIO 1 — Happy path (process end-to-end, query results)"
post_doc happy-1 "$DIR/testdata/docs/small.txt" | jqf
wait_until_completed happy-1 30
echo "--- final status ---"; status happy-1 | jqf
echo "--- all tokens ---"; curl -fsS "$BASE/documents/happy-1/tokens" | jqf

title "SCENARIO 2 — Progress visibility (live classified/total)"
post_doc progress-1 "$DIR/testdata/docs/large.txt" | jqf
echo "--- polling progress ---"
for _ in $(seq 1 20); do
  s=$(status progress-1)
  st=$(echo "$s" | field status)
  cl=$(echo "$s" | nested progress classified)
  tot=$(echo "$s" | nested progress total)
  pct=$(echo "$s" | nested progress percent)
  printf "status=%-11s progress=%s/%s (%s%%)\n" "$st" "$cl" "$tot" "$pct"
  [ "$st" = "COMPLETED" ] && break
  sleep 1
done

title "SCENARIO 3 — Partial rerun (kill mid-way, resume from checkpoint)"
post_doc partial-1 "$DIR/testdata/docs/large.txt" | jqf
echo "--- waiting until classification is underway, then killing the classifier ---"
for _ in $(seq 1 300); do
  s=$(status partial-1)
  st=$(echo "$s" | field status)
  cl=$(echo "$s" | nested progress classified)
  tot=$(echo "$s" | nested progress total)
  [ "$st" = "COMPLETED" ] && break
  if [ "${tot:-0}" -gt 0 ] && [ "${cl:-0}" -gt 0 ]; then
    docker compose kill classifier >/dev/null 2>&1
    echo ">>> killed classifier mid-run at ${cl}/${tot} tokens classified"
    break
  fi
  sleep 0.1
done
s=$(status partial-1)
echo "after kill: status=$(echo "$s" | field status) classified=$(echo "$s" | nested progress classified)/$(echo "$s" | nested progress total)"
echo "--- restarting the classifier; it should RESUME from the checkpoint, not restart ---"
docker compose up -d classifier >/dev/null 2>&1
wait_until_completed partial-1 90
echo "--- final status (classification picked up where it left off; counter never exceeded total) ---"
status partial-1 | jqf

title "SCENARIO 4 — Full rerun (reprocess from scratch, replace data)"
echo "current run_version: $(status_field happy-1 run_version)"
post_doc happy-1 "$DIR/testdata/docs/small.txt" full | jqf
wait_until_completed happy-1 30
echo "new run_version (should be incremented): $(status_field happy-1 run_version)"
status happy-1 | jqf

title "SCENARIO 5 — Concurrent documents (process 3 at once)"
post_doc concur-1 "$DIR/testdata/docs/small.txt"  >/dev/null
post_doc concur-2 "$DIR/testdata/docs/medium.txt" >/dev/null
post_doc concur-3 "$DIR/testdata/docs/large.txt"  >/dev/null
echo "submitted concur-1, concur-2, concur-3"
for id in concur-1 concur-2 concur-3; do wait_until_completed "$id" 60; done
for id in concur-1 concur-2 concur-3; do
  s=$(status "$id")
  printf "%-9s status=%-10s tokens=%s extraction=%ss classification=%ss\n" \
    "$id" "$(echo "$s" | field status)" "$(echo "$s" | nested progress total)" \
    "$(echo "$s" | nested durations extraction_seconds)" \
    "$(echo "$s" | nested durations classification_seconds)"
done

title "SCENARIO 6 — Query tokens (filter by classification / type / page)"
echo "--- concur-3 PERSON tokens (count only) ---"
curl -fsS "$BASE/documents/concur-3/tokens?classification=PERSON" | field count
echo "--- concur-3 COMPANY tokens (first few) ---"
curl -fsS "$BASE/documents/concur-3/tokens?classification=COMPANY&limit=3" | jqf
echo "--- concur-2 DATE tokens ---"
curl -fsS "$BASE/documents/concur-2/tokens?classification=DATE" | jqf

hr; echo "  Demo complete — all six scenarios exercised."; hr
