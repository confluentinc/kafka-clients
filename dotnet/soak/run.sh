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
# Ported from python/soak/run.sh. The supervisor logic is UNCHANGED —
# real child-PID tracking, the EXIT_FATAL never-restart rule, the rotation gate
# that protects crash evidence, the backoff doubling, `wc -c` instead of GNU
# `stat -c%s`, the HI-mode config append and the jemalloc discovery are all
# identical. Three things differ, and only three:
#
#   1. The child is `dotnet <SoakClient.dll>`, not a Python interpreter.
#   2. Configuration is passed as exported SOAK_* environment variables rather
#      than an argparse flag array (the .NET convention in this repo, as used by
#      PerfV2/PerfV3). SOAK_EXTRA_ARGS is therefore gone; see below.
#   3. The pre-flight is `dotnet <dll> --check` rather than a Python import.
#
# The child writes straight to a plain log file instead of streaming through
# `tee /dev/tty | bzip2`; compression happens at rotation. Live output is
# `tail -f "$LOGFILE"` from the tmux pane.
#
# Rotation never stops the child: the log is copied aside and truncated in
# place while the child keeps writing to it (see rotate_log). The copies are
# compressed in the background, one at a time, and deleted once they are older
# than SOAK_LOG_KEEP_DAYS or beyond SOAK_LOG_KEEP_BYTES in all.

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
  SOAK_LOG_LIMIT_BYTES
                Rotate the log above this size. Rotation does not stop the
                client: the log is copied to <log>.<UTC time>, emptied in
                place, and the copy compressed in the background.
                Default: 52428800 (50 MB). Sizes are plain numbers of bytes:
                1073741824, not 1G.
  SOAK_LOG_KEEP_DAYS
                Delete rotated logs older than this many days. Default: 14.
  SOAK_LOG_KEEP_BYTES
                Delete the oldest rotated logs once together they are larger
                than this; the newest is always kept. Default: twice
                SOAK_LOG_LIMIT_BYTES, and at least 2147483648 (2 GiB).
  SOAK_LOG_MIN_FREE_BYTES
                Postpone a rotation that would leave less than this much free
                space on the log's disk; the log grows until there is room.
                Default: 1073741824 (1 GiB).
  SOAK_BROKERS  Overrides bootstrap.servers from the config file.
  SOAK_DOTNET   The dotnet host. Default: dotnet (from PATH).
  SOAK_DOTNET_TFM
                Target framework of the built client. Default: net10.0.
  SOAK_DOTNET_CONFIG
                Build configuration of the built client. Default: Release.
  SOAK_CLIENT_DLL
                Full path to SoakClient.dll, overriding the two above.
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

Any other SOAK_* tunable the client reads (SOAK_COMMIT_INTERVAL,
SOAK_POLL_TIMEOUT, SOAK_STALL_THRESHOLD, SOAK_MAX_SEND_ATTEMPTS,
SOAK_MAX_POLL_FAILURES, SOAK_METRICS_INTERVAL, SOAK_RUNTIME_SECONDS,
SOAK_SHUTDOWN_TIMEOUT, SOAK_LOG_LEVEL, SOAK_RECREATE_TOPIC) is inherited from
this environment as-is -- export it before invoking run.sh. The Python soak's
SOAK_EXTRA_ARGS does NOT exist here, because the client takes no flags; run.sh
fails loudly if it is set rather than silently dropping your tuning.

Memory allocator: under a producer stall (a broker that stops answering with
its connections still open -- NOT a graceful restart, which this does not
reproduce), glibc can permanently strand freed memory: interleaved payload
allocations fragment the heap, and glibc can only return memory from the top.
jemalloc's decay-based purging returns that memory on its own, with no forced
trim. It does NOT reduce the peak during the stall itself -- that is live
memory. The strand is a property of the NATIVE allocator, which the Rust core
still uses here, so this applies to the .NET soak exactly as it does to the
Python one. Set SOAK_JEMALLOC=true to enable it:

  SOAK_JEMALLOC=true TESTID=x ./run.sh ccloud.config

Requires libjemalloc installed on the host (Debian/Ubuntu: `apt-get install
libjemalloc2`); run.sh discovers the path via `ldconfig` rather than assuming
one, since it varies by architecture. If SOAK_JEMALLOC=true is set and the
library cannot be found, the soak refuses to start (exit 2) rather than
silently running on glibc, so a graph never looks like an unexplained
regression when the real cause is a missing package.

The default stays OFF. This is a deliberate control arm, not caution for its
own sake: jemalloc's purging can mask a *future* oversized-allocation
regression -- run at least one variant without it so the soak keeps showing
what it exists to show.

Exit codes of the child (see SoakExitCodes.cs) drive the restart policy:
  0 clean   1 message loss   2 FATAL, never restarted   3 transient startup
  4 consumer wedged (restarted)

Telemetry: the child inherits this environment and configures the OpenTelemetry
SDK itself, so the standard OTEL_* variables work here with no wrapper:
  OTEL_METRICS_EXPORTER=otlp OTEL_EXPORTER_OTLP_ENDPOINT=http://host:4317 \
    TESTID=... ./run.sh ccloud.config
The startup log states whether the pipeline was installed or disabled, and why.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

# The Python soak appended SOAK_EXTRA_ARGS to an argparse flag array. This client
# takes no flags, so silently ignoring it would drop whatever tuning an operator
# copied over from the Python runbook -- for a two-week run, exactly the class of
# silent-default failure this soak exists to catch. Fail instead, and say what to
# do.
if [[ -n "${SOAK_EXTRA_ARGS:-}" ]]; then
    echo "ERROR: SOAK_EXTRA_ARGS is not supported by the .NET soak client, which" \
         "takes no command-line flags. Export the equivalent SOAK_* variables" \
         "instead (e.g. SOAK_COMMIT_INTERVAL=5 SOAK_POLL_TIMEOUT=1.0); run.sh" \
         "passes the whole environment through. See ./run.sh --help." >&2
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

TOPIC="${SOAK_TOPIC:-rustsoak-$TESTID-$SOAK_VARIANT}"
LOG_DIR="${SOAK_LOG_DIR:-$(pwd)}"
LOG_NAME="${TESTID}-${SOAK_VARIANT}.log"
LOGFILE="$LOG_DIR/$LOG_NAME"
METRICS_FILE="$LOG_DIR/soak-metrics-${SOAK_VARIANT}-${TESTID}.jsonl"
FAILED_MARKER="$LOG_DIR/${TESTID}-${SOAK_VARIANT}.FAILED"
LIMIT="${SOAK_LOG_LIMIT_BYTES:-$((50 * 1024 * 1024))}"
KEEP_DAYS="${SOAK_LOG_KEEP_DAYS:-14}"
MIN_FREE="${SOAK_LOG_MIN_FREE_BYTES:-$((1024 * 1024 * 1024))}"

DOTNET="${SOAK_DOTNET:-dotnet}"
DOTNET_TFM="${SOAK_DOTNET_TFM:-net10.0}"
DOTNET_CONFIG="${SOAK_DOTNET_CONFIG:-Release}"
SOAKCLIENT_DLL="${SOAK_CLIENT_DLL:-$SCRIPT_DIR/SoakClient/bin/$DOTNET_CONFIG/$DOTNET_TFM/SoakClient.dll}"

# How often the supervisor checks whether the child is alive and how big the log
# is. Also the upper bound on how long a SIGINT waits to be handled. One second
# is right for a two-week run.
POLL_INTERVAL="${SOAK_POLL_INTERVAL:-1}"
RESTART_DELAY="${SOAK_RESTART_DELAY:-5}"
RESTART_DELAY_MAX="${SOAK_RESTART_DELAY_MAX:-300}"
RAPID_FAILURE_SECONDS="${SOAK_RAPID_FAILURE_SECONDS:-60}"
MAX_RAPID_FAILURES="${SOAK_MAX_RAPID_FAILURES:-5}"

# The client's exit-code contract (SoakExitCodes.cs). Only this one means "a
# restart cannot possibly help", so it is the only one that stops the supervisor
# dead. SoakExitCodeTests reads this very line to confirm the two agree.
EXIT_FATAL=2
# After a rotation or a compression that failed or had to wait (a nearly full
# disk, say), wait this long before trying again, rather than on every poll.
# The test suite turns it down.
ROTATE_RETRY_SECONDS="${SOAK_ROTATE_RETRY_SECONDS:-60}"

# These settings are used in bash arithmetic, which fails on 08 or 1G and reads
# 010 as octal 8, so they are checked here, once, rather than at the first
# rotation.
require_number() {
    local name="$1" value="$2" min="${3:-0}"
    if ! [[ "$value" =~ ^(0|[1-9][0-9]*)$ ]]; then
        echo "ERROR: $name must be a whole number in plain digits (no" \
             "leading zero, sign, decimal point or unit such as G)," \
             "not '$value'" >&2
        exit "$EXIT_FATAL"
    fi
    if (( value < min )); then
        echo "ERROR: $name must be at least $min, not $value" >&2
        exit "$EXIT_FATAL"
    fi
}
require_number SOAK_LOG_LIMIT_BYTES "$LIMIT" 1
require_number SOAK_LOG_KEEP_DAYS "$KEEP_DAYS"
require_number SOAK_LOG_MIN_FREE_BYTES "$MIN_FREE"
require_number SOAK_ROTATE_RETRY_SECONDS "$ROTATE_RETRY_SECONDS"
# Room for the newest rotated log while it is still uncompressed, plus as much
# again of compressed history, and never under 2 GiB.
KEEP_BYTES="${SOAK_LOG_KEEP_BYTES:-$(( 2 * LIMIT > 2147483648 ? 2 * LIMIT : 2147483648 ))}"
require_number SOAK_LOG_KEEP_BYTES "$KEEP_BYTES"
# rotate_log finishes its copy and truncates the log in one perl process. perl
# is part of every Debian/Ubuntu base system and of macOS, so this only trips
# on an unusually minimal image.
if ! command -v perl >/dev/null 2>&1; then
    echo "ERROR: run.sh needs perl to rotate the log, and none is on PATH" >&2
    exit "$EXIT_FATAL"
fi

mkdir -p "$LOG_DIR"

if [[ ! -f "$SOAKCLIENT_DLL" ]]; then
    echo "ERROR: no SoakClient.dll at $SOAKCLIENT_DLL. Run build.sh (or set" \
         "SOAK_CLIENT_DLL / SOAK_DOTNET_TFM / SOAK_DOTNET_CONFIG)." >&2
    exit "$EXIT_FATAL"
fi

# The effective client config: the operator's file, plus -- in HI mode -- the
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

# The child's whole configuration, as environment. Exported rather than passed as
# flags because the .NET client is env-driven (the PerfV2/PerfV3 convention).
export SOAK_TESTID="$TESTID"
export SOAK_TOPIC="$TOPIC"
export SOAK_RATE
export SOAK_VARIANT
export SOAK_PAYLOAD_SIZE
export SOAK_PARTITIONS
export SOAK_REPLICATION_FACTOR
export SOAK_CONFIG_FILE="$EFFECTIVE_CONFIG"
export SOAK_METRICS_FILE="$METRICS_FILE"
if [[ -n "${SOAK_BROKERS:-}" ]]; then
    export SOAK_BROKERS
fi
# build.sh writes the manifest next to this script (the stable location an
# operator can `cat`), not into the build output the client would otherwise
# probe. Point the client at it unless the operator already has.
export SOAK_BUILD_MANIFEST="${SOAK_BUILD_MANIFEST:-$SCRIPT_DIR/build-manifest.json}"

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
# The background job compressing rotated logs, while one runs (see
# start_compressor).
COMPRESSOR=""
low_disk_warned=false

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
    # Terminate the tracked PID only. Never `ps | grep SoakClient`: four soaks
    # share one box and that matches siblings.
    [[ -n "$CHILD" ]] || return 0
    kill -0 "$CHILD" 2>/dev/null || return 0

    log "Stopping soak client (pid $CHILD)"
    kill -TERM "$CHILD" 2>/dev/null
    # The client arms its own hard-exit watchdog (SOAK_SHUTDOWN_TIMEOUT, default
    # 60 s) because Flush/Close are uninterruptible; allow for it before
    # escalating.
    for _ in $(seq 1 90); do
        kill -0 "$CHILD" 2>/dev/null || return 0
        sleep 1
    done
    log "Soak client did not exit after SIGTERM; sending SIGKILL"
    kill -KILL "$CHILD" 2>/dev/null
}

# Appends the log, from byte offset $4 to its end, onto the copy $2, renames
# the copy to its final name $3, and truncates the log straight after. Only a
# write landing in the microseconds between the last read and the truncate can
# be lost. Exits non-zero without truncating on any error.
# shellcheck disable=SC2016  # perl's variables, not the shell's
CATCH_UP_AND_TRUNCATE='
    my ($log, $partial, $part, $offset) = @ARGV;
    open(my $in, "+<", $log) or die "$log: $!\n";
    open(my $out, ">>", $partial) or die "$partial: $!\n";
    binmode $in;
    binmode $out;
    $out->autoflush(1);
    seek($in, $offset, 0) or die "$log: seek: $!\n";
    while (1) {
        my $n = read($in, my $buf, 1 << 20);
        defined $n or die "$log: read: $!\n";
        last if $n == 0;
        print {$out} $buf or die "$partial: write: $!\n";
    }
    rename($partial, $part) or die "$partial: rename: $!\n";
    truncate($in, 0) or die "$log: truncate: $!\n";
'

# Free bytes on the log's filesystem; fails if df cannot say. `df -P` output is
# POSIX, and the arithmetic is bash's because Ubuntu's mawk prints large
# numbers in floating point.
free_bytes() {
    local kb
    kb=$(df -Pk -- "$LOG_DIR" 2>/dev/null | awk 'NR == 2 { print $4 }')
    [[ "$kb" =~ ^[0-9]+$ ]] || return 1
    echo $(( kb * 1024 ))
}

# Rotate the log without stopping the child. The child's stdout/stderr were
# opened with `>>` (O_APPEND), and that is load-bearing: once the log is
# truncated, the child's next write lands at the new end of the file. Opened
# with `>`, it would go on writing at its old offset and leave a sparse file
# full of NULs as large as the old log.
#
# The log goes to "$LOGFILE.<UTC time>", compressed in the background to
# "$LOGFILE.<UTC time>.bz2". The names sort in rotation order.
rotate_log() {
    local part size free
    # 1. Make sure there is room. The copy needs as much space again as the
    #    log, on a disk the other soaks share, so wait while it would leave
    #    less than SOAK_LOG_MIN_FREE_BYTES free rather than fill the disk for
    #    all of them. Waiting loses nothing: the log just grows.
    size=$(log_size)
    if free=$(free_bytes) && (( free < size + MIN_FREE )); then
        if [[ "$low_disk_warned" != true ]]; then
            log "Log rotation postponed: copying $LOGFILE ($size bytes) would" \
                "leave less than $MIN_FREE bytes free on its disk, which has" \
                "$free. The log grows until there is room; checking again" \
                "every ${ROTATE_RETRY_SECONDS}s"
            low_disk_warned=true
        fi
        return 1
    fi

    part="$LOGFILE.$(date -u +%Y%m%dT%H%M%SZ)"
    # Two rotations within one second (only with a tiny limit) would share a
    # name.
    while [[ -e "$part" || -e "$part.bz2" ]]; do
        sleep 1
        part="$LOGFILE.$(date -u +%Y%m%dT%H%M%SZ)"
    done

    # 2. Bulk copy while the child keeps writing, under a name that marks the
    #    copy incomplete: if run.sh is killed before step 3 ends, everything in
    #    the copy is still in the log, so the next run deletes the copy rather
    #    than keep its lines twice. The log is truncated only once all of it
    #    has been copied, so a failure costs a rotation, never output.
    if ! cp -- "$LOGFILE" "$part.partial"; then
        rm -f -- "$part.partial"
        log "Log rotation failed: could not copy $LOGFILE. It was left as it" \
            "is; retrying in ${ROTATE_RETRY_SECONDS}s"
        return 1
    fi
    # 3. Append what the child wrote during the copy, give the copy its final
    #    name, and truncate.
    if ! perl -MIO::Handle -e "$CATCH_UP_AND_TRUNCATE" "$LOGFILE" \
            "$part.partial" "$part" "$(wc -c < "$part.partial" | tr -d ' ')"; then
        rm -f -- "$part.partial"
        log "Log rotation failed while finishing the copy of $LOGFILE. It was" \
            "left as it is; retrying in ${ROTATE_RETRY_SECONDS}s"
        return 1
    fi
    low_disk_warned=false
    log "Log rotated at $(wc -c < "$part" | tr -d ' ') bytes to" \
        "$part.bz2${CHILD:+; the soak client (pid $CHILD) kept running}" \
        | tee -a "$LOGFILE"

    # 4. Compress it in the background.
    start_compressor
}

# Compress one rotated log to "$1.bz2" through a temporary file, so that a
# compression cut short (run.sh killed, a full disk) never leaves a truncated
# .bz2 under the name of a complete one. The .bz2 keeps the rotated log's
# modification time, which pruning goes by, and the rotated log is deleted
# only once its .bz2 is in place. $$ is run.sh's PID even in the background
# job, so a compression that a previous run.sh left running never shares the
# temporary file. A run.sh started meanwhile deletes that file, though, as left
# incomplete: `touch -c` keeps it deleted, so the `mv` fails and the rotated log
# stays for the new run.sh to compress, rather than an empty file taking the
# .bz2's name and the log being deleted.
compress_part() {
    local part="$1" tmp="$1.bz2.tmp.$$"
    if nice -n 19 bzip2 -c -- "$part" > "$tmp" &&
            touch -c -r "$part" "$tmp" && mv -f -- "$tmp" "$part.bz2"; then
        rm -f -- "$part"
    else
        rm -f -- "$tmp"
        log "Could not compress $part; it was left as it is. Retrying in" \
            "${ROTATE_RETRY_SECONDS}s"
        return 1
    fi
}

# Compress every rotated log not yet compressed, oldest first, including any
# rotated meanwhile. Stops at the first failure.
compress_parts() {
    local part found=true
    while [[ "$found" == true ]]; do
        found=false
        for part in "$LOGFILE".[0-9]*Z; do
            [[ -f "$part" ]] || continue
            compress_part "$part" || return 1
            found=true
        done
    done
}

# Start the background job that compresses the rotated logs and then prunes
# them, unless one is running or there is nothing to compress. bzip2 runs at
# the lowest priority, so it never competes with the soak client, and only one
# job runs at a time: one per rotation could pile up on a busy box. The poll
# loop calls this every ROTATE_RETRY_SECONDS, which picks up a log rotated just
# as the job was finishing and retries a compression that failed.
start_compressor() {
    local part
    if [[ -n "$COMPRESSOR" ]] && kill -0 "$COMPRESSOR" 2>/dev/null; then
        return 0
    fi
    for part in "$LOGFILE".[0-9]*Z; do
        if [[ -f "$part" ]]; then
            ( compress_parts; prune_log_parts ) &
            COMPRESSOR=$!
            return 0
        fi
    done
}

# Delete rotated logs once they are older than SOAK_LOG_KEEP_DAYS, or once the
# newer ones already add up to SOAK_LOG_KEEP_BYTES. A .prev.bz2 left by the old
# stop-and-restart rotation counts as one of them. The newest is never deleted
# for its size: it may not be compressed yet (a log that grew while the disk was
# full, with run.sh restarted before compressing it, say), and it is the one
# most worth keeping. Only this soak's own files match: their names start with
# its TESTID and variant.
# shellcheck disable=SC2016  # perl's variables, not the shell's
PRUNE_LOG_PARTS='
    my ($keep_bytes, $keep_days, @parts) = @ARGV;
    my $oldest = time() - $keep_days * 86400;
    my (%size, %mtime);
    for (@parts) {
        my @st = stat or next;
        ($size{$_}, $mtime{$_}) = @st[7, 9];
    }
    my ($total, $count) = (0, 0);
    for (sort { $mtime{$b} <=> $mtime{$a} || $b cmp $a } keys %size) {
        $total += $size{$_};
        next if ($count++ == 0 || $total <= $keep_bytes)
            && $mtime{$_} >= $oldest;
        unlink or warn "$_: $!\n";
    }
'
prune_log_parts() {
    perl -e "$PRUNE_LOG_PARTS" "$KEEP_BYTES" "$KEEP_DAYS" \
        "$LOGFILE".[0-9]*Z "$LOGFILE".[0-9]*Z.bz2 "$LOGFILE".prev.bz2
}

# Rotate only when the log is actually at the limit. A crash-restart must never
# rotate: the crash's own few KB of output belong in the log, the first place an
# operator looks for why the child died, not in a compressed part beside it.
# This gate is the difference between a diagnosable failure and a soak that has
# quietly been dead for a week.
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
    log "SOAK STOPPED -- $reason"
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
log "  dll=$SOAKCLIENT_DLL"
log "  log=$LOGFILE metrics=$METRICS_FILE limit=${LIMIT}B" \
    "keep=${KEEP_DAYS}d/${KEEP_BYTES}B min_free=${MIN_FREE}B"

# A stale marker from a previous run would be misleading.
rm -f "$FAILED_MARKER"

# --- pre-flight ------------------------------------------------------------
# Run the client's own --check once, with the host the supervisor is about to
# use, before entering the restart loop. It proves the assembly resolves, the
# native library loads (through the MOCK clients -- no network, no DNS, no
# timeout risk) and the configuration is acceptable.
#
# A startup failure this early -- a missing native, an unknown config key --
# would otherwise be diagnosed only after a restart loop, and a host-level
# failure (a missing runtime) never gets to choose an exit code at all: the
# host's own exit 1 would arrive here as "message loss", which is a lie, since
# nothing was produced, let alone lost.
#
# This does NOT replace the exit-code contract: a crash after startup still goes
# through the normal restart logic below.
if ! preflight_output="$("$DOTNET" "$SOAKCLIENT_DLL" --check 2>&1)"; then
    # The client's error is the single most useful line for whoever fixes this
    # (it names the offending key or the missing library), so print it verbatim
    # rather than summarising it. `tee -a` puts one copy on the supervisor's own
    # output and one in the log file, which is otherwise never created for a
    # failure this early -- an operator tailing the log would find nothing.
    log "Pre-flight failed: $DOTNET $SOAKCLIENT_DLL --check" | tee -a "$LOGFILE"
    printf '%s\n' "$preflight_output" | tee -a "$LOGFILE"
    give_up "the soak client's --check preflight failed (its own error is above). \
This is a broken installation or a rejected configuration, not a soak failure: \
check that build.sh completed and that the client config is valid. Nothing was \
started."
    exit "$EXIT_FATAL"
fi
log "Pre-flight OK: $preflight_output"

# What a previous run.sh may have left: a copy or a compression it was stopped
# in the middle of (incomplete, so deleted), rotated logs past their retention,
# and rotated logs it did not get to compress.
for leftover in "$LOGFILE".[0-9]*Z.partial "$LOGFILE".[0-9]*Z.bz2.tmp.*; do
    [[ -f "$leftover" ]] || continue
    log "Deleting $leftover, left incomplete by a previous run.sh"
    rm -f -- "$leftover"
done
prune_log_parts
start_compressor

ret=0
delay="$RESTART_DELAY"
rapid_failures=0
# When to next try a rotation that failed or was postponed, and check on
# compression, in bash's SECONDS.
next_rotation=0
next_compress_check=0

while [[ "$run" == true ]]; do
    started_at=$(date +%s)

    if [[ -n "$JEMALLOC_PRELOAD" ]]; then
        LD_PRELOAD="$JEMALLOC_PRELOAD" "$DOTNET" "$SOAKCLIENT_DLL" >> "$LOGFILE" 2>&1 &
    else
        "$DOTNET" "$SOAKCLIENT_DLL" >> "$LOGFILE" 2>&1 &
    fi
    CHILD=$!
    log "Soak client started (pid $CHILD); follow it with: tail -f $LOGFILE"
    # A signal that came after the loop checked `run`, but before CHILD was
    # set, found no client to stop: stop it now, or the `wait` below would
    # wait for the client to exit by itself.
    [[ "$run" == true ]] || stop_child

    while [[ "$run" == true ]]; do
        # A foreground `sleep` defers trap handling by at most this interval.
        sleep "$POLL_INTERVAL"
        if ! kill -0 "$CHILD" 2>/dev/null; then
            break
        fi
        # Rotation leaves the child running (see rotate_log).
        if (( SECONDS >= next_rotation )) && (( $(log_size) >= LIMIT )); then
            rotate_log || next_rotation=$(( SECONDS + ROTATE_RETRY_SECONDS ))
        fi
        if (( SECONDS >= next_compress_check )); then
            start_compressor
            next_compress_check=$(( SECONDS + ROTATE_RETRY_SECONDS ))
        fi
    done

    wait "$CHILD"
    ret=$?
    CHILD=""
    lifetime=$(( $(date +%s) - started_at ))
    log "Soak client exited with status $ret after ${lifetime}s"

    # A signal for us: stop without rotating or touching anything.
    [[ "$run" == true ]] || break

    # SIGKILL gives the child no chance to log anything, and run.sh sends it
    # only while stopping (handled above), so point at the usual sender.
    if (( ret == 128 + 9 )); then
        log "  It was killed by SIGKILL, which run.sh did not send. On Linux" \
            "that is usually the kernel's OOM killer: journalctl -k | grep -i oom"
    fi

    if (( ret == EXIT_FATAL )); then
        give_up "child exited $ret (fatal: rejected config, missing native or \
failed authentication). Fix the cause and start the soak again; restarting it \
unchanged would only repeat this."
        break
    fi

    if (( lifetime < RAPID_FAILURE_SECONDS )); then
        # A crash/rapid-restart must never rotate (see maybe_rotate_log's
        # comment): if the log was already near LIMIT, the crash's own output
        # can push it over, and rotating here would move that output out of
        # the log an operator reads first.
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
        # It ran for a while, so whatever happened was not a startup failure:
        # treat it as an isolated one, rotate if the log has reached the limit,
        # and restart promptly.
        maybe_rotate_log
        rapid_failures=0
        delay="$RESTART_DELAY"
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
