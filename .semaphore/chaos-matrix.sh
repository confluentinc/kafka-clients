#!/usr/bin/env bash
# Runs the chaos matrix (scenarios 1-16, 18-19) against the Rust client.
# Scenario 17 (all brokers down) is manual only and is skipped.
# Runs all scenarios, prints a PASS/FAIL tally, and exits non-zero on any failure.

set -uo pipefail
cd "$(dirname "$0")/.."

pass=0
fail=0
declare -a RESULTS

run() {
  local label="$1"
  shift
  echo
  echo "=================================================================="
  echo ">>> ${label}"
  echo ">>> cargo xtask chaos $*"
  echo "=================================================================="
  local start
  start=$(date +%s)
  if cargo xtask chaos "$@"; then
    result="PASS"
    pass=$((pass + 1))
  else
    result="FAIL"
    fail=$((fail + 1))
  fi
  local duration=$(( $(date +%s) - start ))
  echo ">>> ${label}: ${result} (${duration}s)"
  RESULTS+=("${label}: ${result} (${duration}s)")
}

COMMON="--up-wait-s 120 --drain-s 60 --idle-threshold-s 0 --reports"
RECREATE="--up-wait-s 120 --drain-s 180 --reports"

run "S1  rolling restart, unclean"          --brokers 5 --partitions 6 --cycles 10 --unclean ${COMMON}
run "S2  rolling restart, clean"            --brokers 5 --partitions 6 --cycles 10 ${COMMON}
run "S3  fast tempo saturated, unclean"     --brokers 3 --partitions 12 --cycles 12 --unclean --stop-s 3 ${COMMON}
run "S4  fast tempo saturated, clean"       --brokers 3 --partitions 12 --cycles 12 --stop-s 3 ${COMMON}
run "S5  reassign-partitions"               --no-broker-roll --reassign-partitions --brokers 5 --partitions 8 --cycles 8 ${COMMON}
run "S6  change-leader"                      --no-broker-roll --change-leader --brokers 5 --partitions 6 --cycles 10 ${COMMON}
run "S7  perm-dead broker, unclean"         --leave-broker-down 2 --brokers 5 --num-topics 1 --partitions 6 --replication-factor 3 --cycles 8 --unclean ${COMMON}
run "S8  perm-dead broker, clean"           --leave-broker-down 2 --brokers 5 --num-topics 1 --partitions 6 --replication-factor 3 --cycles 8 ${COMMON}
run "S9  rebalance-mid-roll, unclean"       --consumers 3 --rebalance-mid-roll --rebalance-add-cycle 3 --rebalance-remove-cycle 6 --brokers 5 --num-topics 1 --partitions 6 --replication-factor 3 --cycles 10 --unclean ${COMMON}
run "S10 rebalance-mid-roll, clean"         --consumers 3 --rebalance-mid-roll --rebalance-add-cycle 3 --rebalance-remove-cycle 6 --brokers 5 --num-topics 1 --partitions 6 --replication-factor 3 --cycles 10 ${COMMON}

run "S11 recreate-delayed, unclean"         --topic-recreate --dwell-s 5 --brokers 5 --num-topics 2 --partitions 6 --replication-factor 3 --cycles 10 --unclean ${RECREATE}
run "S12 recreate-delayed, clean"           --topic-recreate --dwell-s 5 --brokers 5 --num-topics 2 --partitions 6 --replication-factor 3 --cycles 10 ${RECREATE}
run "S13 recreate-immediate, unclean"       --topic-recreate --brokers 5 --num-topics 2 --partitions 6 --replication-factor 3 --cycles 10 --unclean ${RECREATE}
run "S14 recreate-immediate, clean"         --topic-recreate --brokers 5 --num-topics 2 --partitions 6 --replication-factor 3 --cycles 10 ${RECREATE}

run "S15 everything-stacked, unclean"       --topic-recreate --dwell-s 5 --rebalance-mid-roll --rebalance-add-cycle 4 --rebalance-remove-cycle 8 --consumers 3 --brokers 5 --num-topics 2 --partitions 6 --replication-factor 3 --cycles 12 --unclean ${RECREATE}
run "S16 everything-stacked, clean"         --topic-recreate --dwell-s 5 --rebalance-mid-roll --rebalance-add-cycle 4 --rebalance-remove-cycle 8 --consumers 3 --brokers 4 --num-topics 2 --partitions 6 --replication-factor 3 --cycles 12 --up-wait-s 120 --drain-s 300 --reports

echo
echo ">>> S17 all-brokers-down: SKIP (manual)"
RESULTS+=("S17 all-brokers-down: SKIP (manual)")

run "S18 churn + recreate-immediate, unclean" --consumer-churn-min 1 --consumer-churn-max 15 --topic-recreate --brokers 3 --num-topics 2 --partitions 3 --replication-factor 3 --cycles 10 --unclean ${RECREATE}
run "S19 churn + recreate-immediate, clean"   --consumer-churn-min 1 --consumer-churn-max 15 --topic-recreate --brokers 3 --num-topics 2 --partitions 3 --replication-factor 3 --cycles 10 ${RECREATE}

echo
echo "=================================================================="
echo "CHAOS MATRIX: ${pass} PASS / ${fail} FAIL (S17 skipped)"
echo "=================================================================="
for line in "${RESULTS[@]}"; do
  echo "  ${line}"
done

[ "${fail}" -eq 0 ]
