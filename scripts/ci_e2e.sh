#!/usr/bin/env bash
# Fresh-deploy path: unpausing the DAG schedules its latest interval, which finds an empty
# lakehouse and bootstraps the full history. Then the asset-triggered consumer must follow.
set -euo pipefail

airflow() { docker compose exec -T airflow-scheduler airflow "$@"; }

run_field() {  # dag_id run_id_prefix column
  airflow dags list-runs "$1" -o plain 2>/dev/null | awk -v id="^$2" -v col="$3" '$2 ~ id {print $col; exit}'
}

wait_for() {  # dag_id run_id_prefix timeout_seconds
  local deadline=$((SECONDS + $3)) state=""
  while (( SECONDS < deadline )); do
    state=$(run_field "$1" "$2" 3)
    echo "$(date +%T) $1 ${2}*: ${state:-not created yet}"
    case "$state" in
      success) return 0 ;;
      failed) airflow tasks states-for-dag-run "$1" "$(run_field "$1" "$2" 2)" -o plain || true; return 1 ;;
    esac
    sleep 20
  done
  echo "Timed out waiting for $1"; return 1
}

airflow dags unpause denials_worklist >/dev/null
airflow dags unpause claims_lakehouse >/dev/null

wait_for claims_lakehouse scheduled__ 1800
wait_for denials_worklist asset_triggered__ 300

echo "--- quarantine gate report and task tries ---"
airflow tasks states-for-dag-run claims_lakehouse "$(run_field claims_lakehouse scheduled__ 2)" -o plain
docker compose exec -T airflow-scheduler bash -c 'ls -l /opt/airflow/lakehouse/exports/ && head -5 /opt/airflow/lakehouse/exports/*.csv'
