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
#   TESTID=<id> [HI=true] ./run.sh <client.config>
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
Usage: TESTID=<id> [HI=true] ./run.sh <client.config>

  TESTID=x ./run.sh ccloud.config                        # 80 msg/s, 50 B
  HI=true TESTID=x ./run.sh ccloud.config                # 1000 msg/s, 10240 B
  SOAK_RATE=200 HI=true TESTID=x ./run.sh ccloud.config  # override any tunable

HI=true is the high-throughput mode: 1000 msg/s at a 10240 B payload (~10 MB/s),
plus the client tuning 10 KB records need (larger fetches, a 1 MiB producer batch
and lz4), appended to your client config.

There is no "rolling" switch: rolling is a property of the CLUSTER, driven by an
external CronJob. Point bootstrap.servers at the rolled cluster and set
SOAK_VARIANT to label it.

Environment:
  TESTID        Required. Test id; tags every metric and names the log file.
  SOAK_VARIANT  Metric tag separating concurrent soaks.
                Default: 848-hi-throughput if HI=true, else 848-normal.
  SOAK_TOPIC    Topic. Default: rustsoak-$TESTID-$SOAK_VARIANT
  SOAK_LOG_DIR  Log directory. Default: the current directory.
  SOAK_RATE     Messages per second. Default: 1000 if HI=true, else 80.
  SOAK_PAYLOAD_SIZE
                Serialized record size. Default: 10240 if HI=true, else 50.
  SOAK_PARTITIONS
                Partitions to create the topic with. Default: 2.
  SOAK_REPLICATION_FACTOR
                Default: -1 (broker default).
  SOAK_EXTRA_ARGS
                Extra soakclient.py arguments, appended verbatim.
  SOAK_LOG_LIMIT_BYTES
                Rotate above this size. Default: 52428800 (50 MB).
  SOAK_BROKERS  Overrides bootstrap.servers from the config file.
  SOAK_PYTHON   Python interpreter. Default: python3 (from the active venv).
  SOAK_RESTART_DELAY
                Seconds before restarting a child that exited. Doubles, up to
                SOAK_RESTART_DELAY_MAX, while restarts keep being rapid.
                Default: 5.
  SOAK_RESTART_DELAY_MAX
                Cap for the backoff. Default: 300.
  SOAK_RAPID_FAILURE_SECONDS
                A child that lived less than this counts as a rapid failure.
                Default: 60.
  SOAK_MAX_RAPID_FAILURES
                Consecutive rapid failures before giving up and leaving
                everything on disk. Default: 5.
  SOAK_JEMALLOC
                true: preload jemalloc for the soak client. Off by default.
                See "Memory allocator" below.

Memory allocator: under a producer stall (a broker that stops answering with
its connections still open — NOT a graceful restart, which this does not
reproduce), glibc can permanently strand freed memory: interleaved payload and
Python allocations fragment the heap, and glibc can only return memory from the
top. Investigated in design/current/soak-rss-spike-explainer.md. jemalloc's
decay-based purging returns that memory on its own, with no forced trim:
measured 449 -> 109 MiB after a stall, vs. glibc's 449 -> 449 (no recovery at
all). It does NOT reduce the peak during the stall itself — that is live memory,
addressed separately by the BatchNode sizing fix in
fix/python-binding-batchnode-memory. Set SOAK_JEMALLOC=true to enable it:

  SOAK_JEMALLOC=true TESTID=x ./run.sh ccloud.config

Requires libjemalloc installed on the host (Debian/Ubuntu: `apt-get install
libjemalloc2`); run.sh discovers the path via `ldconfig` rather than assuming
one, since it varies by architecture. If SOAK_JEMALLOC=true is set and the
library cannot be found, the soak refuses to start (exit 2) rather than
silently running on glibc, so a graph never looks like an unexplained
regression when the real cause is a missing package.

The default stays OFF. This is a deliberate control arm, not caution for its
own sake: jemalloc's purging can mask a *future* oversized-allocation
regression the same way it would have masked this one before it was fixed —
run at least one variant without it so the soak keeps showing what it exists to
show.

Exit codes of the child (see soakclient.py) drive the restart policy:
  0 clean   1 message loss   2 FATAL, never restarted   3 transient startup
  4 consumer wedged (restarted)

Telemetry: the child inherits this environment, and soakclient.py configures the
OpenTelemetry SDK itself, so the standard OTEL_* variables work here with no
`opentelemetry-instrument` wrapper:
  OTEL_METRICS_EXPORTER=otlp OTEL_EXPORTER_OTLP_ENDPOINT=http://host:4317 \
    TESTID=... ./run.sh ccloud.config
The startup log states whether the pipeline was installed, reused or disabled.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOAKCLIENT="$SCRIPT_DIR/soakclient.py"

is_true() { [[ "${1:-}" =~ ^([Tt]rue|TRUE|1|[Yy]es|YES)$ ]]; }

HI_MODE=false
is_true "${HI:-}" && HI_MODE=true

if [[ $# -ne 1 ]]; then
    usage >&2
    exit 2
fi
CLIENT_CONFIG="$1"

if [[ -z "${TESTID:-}" ]]; then
    echo "ERROR: TESTID must be set" >&2
    exit 2
fi
if [[ ! -f "$CLIENT_CONFIG" ]]; then
    echo "ERROR: no such file: $CLIENT_CONFIG" >&2
    exit 2
fi

# Every tunable is `${VAR:-default}`, so anything exported by the operator wins.
# There is no profile file to source, and therefore no way for a default to
# clobber the environment.
if [[ "$HI_MODE" == true ]]; then
    SOAK_VARIANT="${SOAK_VARIANT:-848-hi-throughput}"
    SOAK_PAYLOAD_SIZE="${SOAK_PAYLOAD_SIZE:-10240}"
    SOAK_RATE="${SOAK_RATE:-1000}"
else
    SOAK_VARIANT="${SOAK_VARIANT:-848-normal}"
    SOAK_PAYLOAD_SIZE="${SOAK_PAYLOAD_SIZE:-50}"
fi
SOAK_RATE="${SOAK_RATE:-80}"
SOAK_PARTITIONS="${SOAK_PARTITIONS:-2}"
SOAK_REPLICATION_FACTOR="${SOAK_REPLICATION_FACTOR:--1}"
SOAK_EXTRA_ARGS="${SOAK_EXTRA_ARGS:-}"

TOPIC="${SOAK_TOPIC:-rustsoak-$TESTID-$SOAK_VARIANT}"
LOG_DIR="${SOAK_LOG_DIR:-$(pwd)}"
LOGFILE="$LOG_DIR/${TESTID}-${SOAK_VARIANT}.log"
PREVLOG="$LOG_DIR/${TESTID}-${SOAK_VARIANT}.log.prev.bz2"
METRICS_FILE="$LOG_DIR/soak-metrics-${SOAK_VARIANT}-${TESTID}.jsonl"
FAILED_MARKER="$LOG_DIR/${TESTID}-${SOAK_VARIANT}.FAILED"
LIMIT="${SOAK_LOG_LIMIT_BYTES:-$((50 * 1024 * 1024))}"
PYTHON="${SOAK_PYTHON:-python3}"
# How often the supervisor checks whether the child is alive and how big the log
# is. Also the upper bound on how long a SIGINT waits to be handled. One second
# is right for a two-week run; the test suite turns it down so its ~30 cases do
# not each pay a second.
POLL_INTERVAL="${SOAK_POLL_INTERVAL:-1}"
RESTART_DELAY="${SOAK_RESTART_DELAY:-5}"
RESTART_DELAY_MAX="${SOAK_RESTART_DELAY_MAX:-300}"
RAPID_FAILURE_SECONDS="${SOAK_RAPID_FAILURE_SECONDS:-60}"
MAX_RAPID_FAILURES="${SOAK_MAX_RAPID_FAILURES:-5}"

# soakclient.py's exit-code contract. Only this one means "a restart cannot
# possibly help", so it is the only one that stops the supervisor dead.
EXIT_FATAL=2

mkdir -p "$LOG_DIR"

# The effective client config: the operator's file, plus — in HI mode — the
# client tuning a 10 KB payload needs. These four are what make the
# high-throughput variant perform sensibly rather than just carrying bigger
# records: without them the consumer fetches a handful of records per poll and
# the producer sends a batch per record.
EFFECTIVE_CONFIG="$(mktemp "${TMPDIR:-/tmp}/soak-config.XXXXXX")"
cat "$CLIENT_CONFIG" > "$EFFECTIVE_CONFIG"
if [[ "$HI_MODE" == true ]]; then
    cat >> "$EFFECTIVE_CONFIG" <<'HIEOF'

# --- appended by run.sh for HI=true (high-throughput mode) ---
consumer.fetch.max.bytes=52428800
consumer.max.partition.fetch.bytes=10485760
producer.batch.size=1048576
producer.compression.type=lz4
HIEOF
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
    # Word-split on purpose: SOAK_EXTRA_ARGS is a flag string.
    # shellcheck disable=SC2206
    ARGS+=($SOAK_EXTRA_ARGS)
fi

# Memory allocator (see "Memory allocator" in --help). Resolved once, before
# the supervise loop, so a missing library fails the whole run rather than
# failing silently on every restart.
JEMALLOC_PRELOAD=""
if is_true "${SOAK_JEMALLOC:-}"; then
    # The path is architecture- and distro-dependent (e.g.
    # /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 vs .../aarch64-linux-gnu/...),
    # so ask the dynamic linker's cache rather than guessing one.
    JEMALLOC_PRELOAD="$(ldconfig -p 2>/dev/null | awk '/libjemalloc\.so/{print $NF; exit}')"
    if [[ -z "$JEMALLOC_PRELOAD" ]]; then
        echo "ERROR: SOAK_JEMALLOC=true but no libjemalloc.so.* was found by" \
             "ldconfig. Install it (Debian/Ubuntu: apt-get install" \
             "libjemalloc2) or unset SOAK_JEMALLOC to run on glibc." >&2
        exit "$EXIT_FATAL"
    fi
    # log() (timestamped, tee'd to $LOGFILE) isn't defined yet at this point in
    # the script; this one line goes to stderr only, mirroring the messages
    # above it.
    echo "Preloading jemalloc: $JEMALLOC_PRELOAD" >&2
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

# Rotate only when the log is actually at the limit. A crash-restart must never
# rotate: it would bzip2 a few-KB fragment over the single .prev.bz2 and delete
# the log, destroying the evidence of why the child died — the one thing an
# operator needs. This gate is the difference between a diagnosable failure and
# a soak that has quietly been dead for a week.
maybe_rotate_log() {
    if (( $(log_size) >= LIMIT )); then
        rotate_log
    fi
}

# Terminal state: loud in the log, greppable in the process list's absence, and
# discoverable by `ls` (four soaks share a box, and nobody reads 50 MB of log to
# find out one of them stopped). Everything is left exactly as it is on disk.
give_up() {
    local reason="$1"
    log "================================================================"
    log "SOAK STOPPED — $reason"
    log "  variant=$SOAK_VARIANT testid=$TESTID topic=$TOPIC"
    log "  log:     $LOGFILE"
    log "  metrics: $METRICS_FILE"
    log "  Nothing has been rotated or deleted; the failure is above."
    log "================================================================"
    {
        echo "SOAK STOPPED at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "reason:  $reason"
        echo "variant: $SOAK_VARIANT"
        echo "testid:  $TESTID"
        echo "topic:   $TOPIC"
        echo "log:     $LOGFILE"
        echo "metrics: $METRICS_FILE"
    } > "$FAILED_MARKER"
    run=false
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
log "  payload=${SOAK_PAYLOAD_SIZE}B hi=$HI_MODE"
log "  log=$LOGFILE metrics=$METRICS_FILE limit=${LIMIT}B"

# A stale marker from a previous run would be misleading.
rm -f "$FAILED_MARKER"

# --- pre-flight ------------------------------------------------------------
# Import the soak client once, with the interpreter the supervisor is about to
# use, before entering the restart loop.
#
# An import-time failure — a venv missing psutil, a syntax error — kills the
# child before main() runs, so the child never gets to choose an exit code and
# the interpreter's own exit 1 arrives here as EXIT_MESSAGE_LOSS. The supervisor
# would then restart it with backoff and eventually write a .FAILED marker
# saying "message loss", which is a lie: nothing was produced, let alone lost.
# An incomplete venv on a fresh box is a likely first run, so one subprocess to
# diagnose it properly is cheap.
#
# This does NOT replace the exit-code contract: a crash after startup still goes
# through the normal restart logic below.
if ! preflight_output="$(SOAK_PREFLIGHT_DIR="$SCRIPT_DIR" "$PYTHON" -c '
import os
import sys

sys.path.insert(0, os.environ["SOAK_PREFLIGHT_DIR"])
import soakclient  # noqa: F401
' 2>&1)"; then
    # The interpreter error is the single most useful line for whoever fixes
    # this (it names the missing package), so print it verbatim rather than
    # summarising it. `tee -a` puts one copy on the supervisor's own output and
    # one in the log file, which is otherwise never created for a failure this
    # early — an operator tailing the log would find nothing at all.
    log "Pre-flight failed: $PYTHON cannot import soakclient." | tee -a "$LOGFILE"
    printf '%s\n' "$preflight_output" | tee -a "$LOGFILE"
    give_up "the soak client could not be imported by $PYTHON (the interpreter's \
own error is above). This is a broken installation, not a soak failure: check \
that the venv is activated and that build.sh completed. Nothing was started."
    exit "$EXIT_FATAL"
fi

ret=0
delay="$RESTART_DELAY"
rapid_failures=0

while [[ "$run" == true ]]; do
    stopped_for_rotation=false
    started_at=$(date +%s)

    if [[ -n "$JEMALLOC_PRELOAD" ]]; then
        LD_PRELOAD="$JEMALLOC_PRELOAD" "$PYTHON" "$SOAKCLIENT" "${ARGS[@]}" >> "$LOGFILE" 2>&1 &
    else
        "$PYTHON" "$SOAKCLIENT" "${ARGS[@]}" >> "$LOGFILE" 2>&1 &
    fi
    CHILD=$!
    log "Soak client started (pid $CHILD); follow it with: tail -f $LOGFILE"

    while [[ "$run" == true ]]; do
        # A foreground `sleep` defers trap handling by at most this interval.
        sleep "$POLL_INTERVAL"
        if ! kill -0 "$CHILD" 2>/dev/null; then
            break
        fi
        if (( $(log_size) >= LIMIT )); then
            log "Log reached ${LIMIT} bytes"
            stopped_for_rotation=true
            stop_child
            break
        fi
    done

    wait "$CHILD"
    ret=$?
    CHILD=""
    lifetime=$(( $(date +%s) - started_at ))
    log "Soak client exited with status $ret after ${lifetime}s"

    # A signal for us: stop without rotating or touching anything.
    [[ "$run" == true ]] || break

    if (( ret == EXIT_FATAL )); then
        give_up "child exited $ret (fatal: rejected config, missing bindings or \
failed authentication). Fix the cause and start the soak again; restarting it \
unchanged would only repeat this."
        break
    fi

    if [[ "$stopped_for_rotation" == true ]]; then
        # We asked it to stop, so this is not a failure: rotate and reset the
        # backoff.
        rotate_log
        rapid_failures=0
        delay="$RESTART_DELAY"
    else
        maybe_rotate_log
        if (( lifetime < RAPID_FAILURE_SECONDS )); then
            rapid_failures=$(( rapid_failures + 1 ))
            log "Rapid failure ${rapid_failures}/${MAX_RAPID_FAILURES}" \
                "(lived ${lifetime}s < ${RAPID_FAILURE_SECONDS}s)"
            if (( rapid_failures >= MAX_RAPID_FAILURES )); then
                give_up "$rapid_failures consecutive restarts each lasting under \
${RAPID_FAILURE_SECONDS}s (last exit status $ret). This is a crash loop, not a \
soak; the cause is in the log above."
                break
            fi
        else
            # It ran for a while, so whatever happened was not a startup
            # failure: treat it as an isolated one and restart promptly.
            rapid_failures=0
            delay="$RESTART_DELAY"
        fi
    fi

    log "Restarting in ${delay}s"
    sleep "$delay"

    # Back off only while failures keep being rapid, so a persistent problem
    # gets slower and more visible instead of hammering the broker.
    if (( rapid_failures > 0 )); then
        delay=$(( delay * 2 ))
        (( delay > RESTART_DELAY_MAX )) && delay="$RESTART_DELAY_MAX"
    fi
done

log "Ending soak client using topic $TOPIC"
exit "$ret"
