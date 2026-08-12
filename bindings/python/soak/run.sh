#!/usr/bin/env bash
#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Supervise one soak client: keep it alive, and keep its log bounded.
#
# Usage:
#   TESTID=<id> ./run.sh <profile.env> <client.config>
#
# Modelled on confluent-kafka-python/tests/soak/run.sh, with two deliberate
# fixes:
#
#  * It tracks the REAL child PID. The Python version kills its child with
#    `ps --ppid $PID -f | grep soakclient.py | xargs kill`, because its child
#    is the tail of a pipeline. Four soaks share one EC2 box, where that grep
#    matches siblings.
#  * No `stat -c%s` (GNU-only). The log size comes from `wc -c`.
#
# The child therefore writes straight to a plain log file instead of streaming
# through `tee /dev/tty | bzip2`; compression happens at rotation. Live output
# is `tail -f "$LOGFILE"` from the tmux pane.

set -uo pipefail

usage() {
    cat <<'EOF'
Usage: TESTID=<id> ./run.sh <profile.env> <client.config>

Environment:
  TESTID        Required. Test id; tags every metric and names the log file.
  SOAK_TOPIC    Topic. Default: rustsoak-$TESTID-$SOAK_VARIANT
  SOAK_LOG_DIR  Log directory. Default: the current directory.
  SOAK_LOG_LIMIT_BYTES
                Rotate above this size. Default: 52428800 (50 MB).
  SOAK_BROKERS  Overrides bootstrap.servers from the config file.
  SOAK_PYTHON   Python interpreter. Default: python3 (from the active venv).
  SOAK_RESTART_DELAY
                Seconds to wait before restarting a child that exited.
                Default: 5.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ $# -ne 2 ]]; then
    usage >&2
    exit 2
fi

PROFILE_FILE="$1"
CLIENT_CONFIG="$2"

if [[ -z "${TESTID:-}" ]]; then
    echo "ERROR: TESTID must be set" >&2
    exit 2
fi
for f in "$PROFILE_FILE" "$CLIENT_CONFIG"; do
    if [[ ! -f "$f" ]]; then
        echo "ERROR: no such file: $f" >&2
        exit 2
    fi
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOAKCLIENT="$SCRIPT_DIR/soakclient.py"

# Profile defaults, overridable from the environment.
SOAK_VARIANT="$(basename "$PROFILE_FILE" .env)"
SOAK_RATE=80
SOAK_PAYLOAD_SIZE=50
SOAK_PARTITIONS=2
SOAK_REPLICATION_FACTOR=-1
SOAK_ROLLING=false
SOAK_EXTRA_ARGS=""
SOAK_CLIENT_CONFIG=""
# shellcheck disable=SC1090
source "$PROFILE_FILE"

TOPIC="${SOAK_TOPIC:-rustsoak-$TESTID-$SOAK_VARIANT}"
LOG_DIR="${SOAK_LOG_DIR:-$(pwd)}"
LOGFILE="$LOG_DIR/${TESTID}-${SOAK_VARIANT}.log"
PREVLOG="$LOG_DIR/${TESTID}-${SOAK_VARIANT}.log.prev.bz2"
METRICS_FILE="$LOG_DIR/soak-metrics-${SOAK_VARIANT}-${TESTID}.jsonl"
LIMIT="${SOAK_LOG_LIMIT_BYTES:-$((50 * 1024 * 1024))}"
PYTHON="${SOAK_PYTHON:-python3}"
RESTART_DELAY="${SOAK_RESTART_DELAY:-5}"

mkdir -p "$LOG_DIR"

# The effective client config: the operator's file plus the profile's own
# settings, which take precedence (a profile must be able to guarantee
# group.protocol=consumer).
EFFECTIVE_CONFIG="$(mktemp "${TMPDIR:-/tmp}/soak-config.XXXXXX")"
cat "$CLIENT_CONFIG" > "$EFFECTIVE_CONFIG"
if [[ -n "$SOAK_CLIENT_CONFIG" ]]; then
    printf '\n%s\n' "$SOAK_CLIENT_CONFIG" >> "$EFFECTIVE_CONFIG"
fi

ARGS=(
    -i "$TESTID"
    -t "$TOPIC"
    -r "$SOAK_RATE"
    -f "$EFFECTIVE_CONFIG"
    --variant "$SOAK_VARIANT"
    --payload-size "$SOAK_PAYLOAD_SIZE"
    --partitions "$SOAK_PARTITIONS"
    --replication-factor "$SOAK_REPLICATION_FACTOR"
    --metrics-file "$METRICS_FILE"
)
if [[ -n "${SOAK_BROKERS:-}" ]]; then
    ARGS+=(-b "$SOAK_BROKERS")
fi
if [[ -n "$SOAK_EXTRA_ARGS" ]]; then
    # Word-split on purpose: the profiles carry a flag string.
    # shellcheck disable=SC2206
    ARGS+=($SOAK_EXTRA_ARGS)
fi

run=true
CHILD=""

log() {
    echo "[run.sh $(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
}

log_size() {
    if [[ -f "$LOGFILE" ]]; then
        # `wc -c` is POSIX; `stat -c%s` is GNU-only and absent on macOS/BSD.
        wc -c < "$LOGFILE" | tr -d ' '
    else
        echo 0
    fi
}

stop_child() {
    # Terminate the tracked PID only. Never `ps | grep soakclient.py`: four
    # soaks share one box and that matches siblings.
    [[ -n "$CHILD" ]] || return 0
    kill -0 "$CHILD" 2>/dev/null || return 0

    log "Stopping soak client (pid $CHILD)"
    kill -TERM "$CHILD" 2>/dev/null
    # soakclient.py arms its own hard-exit watchdog (--shutdown-timeout,
    # default 60 s) because flush()/close() are uninterruptible; allow for it
    # before escalating.
    for _ in $(seq 1 90); do
        kill -0 "$CHILD" 2>/dev/null || return 0
        sleep 1
    done
    log "Soak client did not exit after SIGTERM; sending SIGKILL"
    kill -KILL "$CHILD" 2>/dev/null
}

rotate_log() {
    log "Rotating log: $LOGFILE -> $PREVLOG"
    rm -f "$PREVLOG"
    bzip2 -c "$LOGFILE" > "$PREVLOG" && rm -f "$LOGFILE"
}

# shellcheck disable=SC2329  # invoked via trap
on_signal() {
    run=false
    log "Termination signal received; stopping soak client using topic $TOPIC"
    stop_child
}
trap on_signal INT TERM

# shellcheck disable=SC2329  # invoked via trap
cleanup() {
    rm -f "$EFFECTIVE_CONFIG"
}
trap cleanup EXIT

log "Starting soak client: variant=$SOAK_VARIANT topic=$TOPIC rate=$SOAK_RATE"
log "  payload=${SOAK_PAYLOAD_SIZE}B rolling=$SOAK_ROLLING"
log "  log=$LOGFILE metrics=$METRICS_FILE limit=${LIMIT}B"

ret=0
while [[ "$run" == true ]]; do
    "$PYTHON" "$SOAKCLIENT" "${ARGS[@]}" >> "$LOGFILE" 2>&1 &
    CHILD=$!
    log "Soak client started (pid $CHILD); follow it with: tail -f $LOGFILE"

    while [[ "$run" == true ]]; do
        # A foreground `sleep` defers trap handling by at most one second.
        sleep 1
        if ! kill -0 "$CHILD" 2>/dev/null; then
            break
        fi
        if (( $(log_size) >= LIMIT )); then
            log "Log reached ${LIMIT} bytes"
            stop_child
            break
        fi
    done

    wait "$CHILD"
    ret=$?
    CHILD=""
    log "Soak client exited with status $ret"

    if [[ "$run" == true ]]; then
        rotate_log
        log "Restarting in ${RESTART_DELAY}s"
        sleep "$RESTART_DELAY"
    fi
done

log "Ending soak client using topic $TOPIC"
exit "$ret"
