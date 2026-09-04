#!/usr/bin/env bash
#
# backfill.sh - replay a date range through the pipeline, one chunk at a time.
#
#   bash scripts/backfill.sh --from 2025-01-01 --to 2026-06-30
#   bash scripts/backfill.sh --from 2025-01-01 --to 2025-12-31 --chunk-days 7
#   bash scripts/backfill.sh --from 2025-01-01 --to 2025-03-31 --dry-run
#   bash scripts/backfill.sh --from 2025-01-01 --to 2026-06-30 --airflow
#
# WHY CHUNKING IS THE WHOLE POINT
# -------------------------------
# The default profile spans eighteen months and roughly a million sessions.
# Running that as ONE pipeline invocation means one enormous transaction: it
# holds locks for hours, builds a redo mountain the database has to keep, and
# if it fails at hour six it rolls back all six hours and starts again from
# nothing.
#
# Chunking turns that into a sequence of small, independently committed runs.
# Each chunk advances the watermark inside its OWN transaction, so a crash
# resumes from the last committed chunk rather than from the beginning. That
# is the difference between a backfill you can run on a Tuesday afternoon and
# one you have to babysit overnight.
#
# THIS IS ALSO THE BOOTSTRAP TOOL
# -------------------------------
# The initial load of an empty warehouse and a backfill of an existing one are
# the same operation: the watermarks start at the beginning-of-time sentinel,
# so the first chunk's predicate simply matches everything up to its upper
# bound. There is deliberately no separate bootstrap_history.py - two scripts
# doing the same thing is two scripts to keep correct, and the one that gets
# run less often is the one that quietly rots.
#
# DIMENSIONS ARE NOT BACKFILLED, AND THAT IS CORRECT
# --------------------------------------------------
# --backfill is passed to every chunk, which makes run_dimensions SKIP the
# Type 2 merges. The source exposes current state plus updated_at, so
# re-running a merge for a past date would stamp TODAY's attributes with a
# HISTORICAL effective_from and corrupt the history the dimension exists to
# preserve.
#
# Facts are re-resolved against the dimension history that already exists,
# which is right. Rebuilding true dimension history is a separate, explicit
# and far more destructive operation:
#
#     python scripts/rebuild_dimension_history.py --dim charge_point --dry-run
#
# ORDER OF CHUNKS
# ---------------
# Oldest first, always. Facts resolve against dimension versions by effective
# date, and the inferred-member logic promotes a device the first time it is
# seen. Running newest-first would create inferred members for devices whose
# real CMS rows are in a chunk that has not run yet.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DATE_FROM=""
DATE_TO=""
CHUNK_DAYS=30
LOOKBACK_DAYS=3
DRY_RUN=0
USE_AIRFLOW=0
DAG_ID="volthive_ingest_sessions"
STAGES="ingest,stage,core,dq,mart"

usage() {
    cat <<'USAGE'
Usage: bash scripts/backfill.sh --from YYYY-MM-DD --to YYYY-MM-DD [options]

  --from DATE          first business date to replay (required)
  --to DATE            last business date to replay (required)
  --chunk-days N       days per chunk (default 30). Smaller chunks commit more
                       often and resume more finely; larger chunks are faster.
  --lookback-days N    lookback passed to each chunk (default 3)
  --stages LIST        comma-separated pipeline stages (default all five)
  --airflow            trigger `airflow dags backfill` instead of running the
                       pipeline directly. Use this when you want the run to
                       appear in the Airflow UI with its retries and logs.
  --dag-id ID          DAG to backfill with --airflow (default volthive_ingest_sessions)
  --dry-run            print the chunk plan and exit without running anything
  -h, --help           this message
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --from)          DATE_FROM="$2"; shift 2 ;;
        --to)            DATE_TO="$2"; shift 2 ;;
        --chunk-days)    CHUNK_DAYS="$2"; shift 2 ;;
        --lookback-days) LOOKBACK_DAYS="$2"; shift 2 ;;
        --stages)        STAGES="$2"; shift 2 ;;
        --dag-id)        DAG_ID="$2"; shift 2 ;;
        --airflow)       USE_AIRFLOW=1; shift ;;
        --dry-run)       DRY_RUN=1; shift ;;
        -h|--help)       usage; exit 0 ;;
        *) echo "ERROR: unknown argument '$1'" >&2; usage >&2; exit 2 ;;
    esac
done

if [ -z "$DATE_FROM" ] || [ -z "$DATE_TO" ]; then
    echo "ERROR: --from and --to are both required." >&2
    usage >&2
    exit 2
fi

# GNU date is required for arithmetic. macOS ships BSD date, whose -d flag
# means something else entirely and would produce silently wrong windows -
# which is far worse than refusing to start.
if ! date -d "$DATE_FROM" +%Y-%m-%d >/dev/null 2>&1; then
    echo "ERROR: GNU date is required (coreutils). On macOS: brew install coreutils," >&2
    echo "       then run this script with gdate on PATH as 'date'." >&2
    exit 2
fi

START_EPOCH=$(date -d "$DATE_FROM" +%s)
END_EPOCH=$(date -d "$DATE_TO" +%s)
if [ "$END_EPOCH" -lt "$START_EPOCH" ]; then
    echo "ERROR: --to ($DATE_TO) is before --from ($DATE_FROM)." >&2
    exit 2
fi
if [ "$CHUNK_DAYS" -lt 1 ]; then
    echo "ERROR: --chunk-days must be at least 1." >&2
    exit 2
fi

TOTAL_DAYS=$(( (END_EPOCH - START_EPOCH) / 86400 + 1 ))
TOTAL_CHUNKS=$(( (TOTAL_DAYS + CHUNK_DAYS - 1) / CHUNK_DAYS ))

echo "Backfill plan"
echo "  window       ${DATE_FROM} .. ${DATE_TO}  (${TOTAL_DAYS} days)"
echo "  chunk size   ${CHUNK_DAYS} days  ->  ${TOTAL_CHUNKS} chunks"
echo "  lookback     ${LOOKBACK_DAYS} days"
echo "  stages       ${STAGES}"
if [ "$USE_AIRFLOW" -eq 1 ]; then
    echo "  executor     airflow dags backfill ${DAG_ID}"
else
    echo "  executor     python scripts/run_pipeline.py"
fi
echo "  dimensions   SKIPPED - see the header of this script"
echo

# --airflow delegates the whole range to the scheduler in one command. The DAG
# declares catchup=True and max_active_runs=1, so Airflow itself serialises the
# runs and each one gets its own retries, logs and UI entry. Chunking is not
# needed on this path - Airflow's data intervals already are the chunks.
if [ "$USE_AIRFLOW" -eq 1 ]; then
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "[dry-run] airflow dags backfill --start-date ${DATE_FROM} --end-date ${DATE_TO} ${DAG_ID}"
        exit 0
    fi
    echo "Handing the range to Airflow. Each data interval becomes one run."
    exec docker compose exec -T airflow-scheduler \
        airflow dags backfill --start-date "$DATE_FROM" --end-date "$DATE_TO" "$DAG_ID"
fi

CHUNK=0
FAILED_AT=""
CURSOR="$DATE_FROM"

while [ "$(date -d "$CURSOR" +%s)" -le "$END_EPOCH" ]; do
    CHUNK=$((CHUNK + 1))
    CHUNK_END=$(date -d "${CURSOR} +$((CHUNK_DAYS - 1)) days" +%Y-%m-%d)
    if [ "$(date -d "$CHUNK_END" +%s)" -gt "$END_EPOCH" ]; then
        CHUNK_END="$DATE_TO"
    fi

    printf '[%d/%d] %s .. %s\n' "$CHUNK" "$TOTAL_CHUNKS" "$CURSOR" "$CHUNK_END"

    if [ "$DRY_RUN" -eq 0 ]; then
        # Deliberately NOT `set -e` territory: a failing chunk must stop the
        # loop with a message naming the chunk, so the operator can resume with
        # --from that date rather than re-running everything that succeeded.
        if ! python scripts/run_pipeline.py \
                --from "$CURSOR" --to "$CHUNK_END" \
                --stages "$STAGES" \
                --lookback-days "$LOOKBACK_DAYS" \
                --backfill; then
            FAILED_AT="$CURSOR"
            break
        fi
    fi

    CURSOR=$(date -d "${CHUNK_END} +1 day" +%Y-%m-%d)
done

echo

if [ -n "$FAILED_AT" ]; then
    echo "FAILED on the chunk starting ${FAILED_AT}."
    echo "Every earlier chunk COMMITTED and its watermark advanced, so resume with:"
    echo "  bash scripts/backfill.sh --from ${FAILED_AT} --to ${DATE_TO} --chunk-days ${CHUNK_DAYS}"
    exit 1
fi

if [ "$DRY_RUN" -eq 1 ]; then
    echo "Dry run: nothing was executed."
    exit 0
fi

echo "Backfill complete: ${TOTAL_CHUNKS} chunks over ${DATE_FROM} .. ${DATE_TO}."
echo
echo "Dimension history was NOT rebuilt - facts were re-resolved against the"
echo "history that already existed. If the merge logic itself changed, run:"
echo "  python scripts/rebuild_dimension_history.py --dim all --dry-run"
echo
echo "Check the result before trusting it:"
echo "  python scripts/dq_report.py"
