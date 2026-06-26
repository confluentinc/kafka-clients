#!/usr/bin/env python3
"""
Deploy the producer performance tests to a remote host over SSH, install every
dependency (including librdkafka-dev from the Confluent apt repository), build
the Rust + C tests, and run them inside a tmux session using parameters from a
.env file. Optionally copy the resulting metrics back and plot them.

Zip handling:
  * With --recreate-zip: (re)create and OVERWRITE the local zip from --repo-dir
    (copy the repo, strip git-ignored files, drop the bundled `kafka` dir),
    upload it, and EXTRACT it on the server, replacing the existing folder
    there (a clean rm -rf + unzip, so a fresh code + full rebuild).
  * Without --recreate-zip: reuse the repo already present on the server from a
    previous run (incremental build); the zip is neither rebuilt nor uploaded.

The remote host must be Debian/Ubuntu with passwordless or interactive sudo.

This script lives at <repo>/tools/deploy_and_run_perf/. By default it deploys
the repo it belongs to (repo root = two levels up), reads <repo-parent>/.env,
writes the zip to <repo-parent>/<repo-name>.zip, and plots with the repo's
tools/performance_metrics_plot/plot_metrics.py.

One test runs per invocation (--test), because each test can need a different
.env. Run it once per test, e.g. librdkafka then the Rust client.

Examples (paths shown relative to the repo root):
  # First deployment of the librdkafka test (creates zip, uploads, extracts,
  # builds, runs, plots):
  python3 tools/deploy_and_run_perf/deploy_and_run_perf.py admin@host \\
      --test c-v2 --recreate-zip --env-file ../librdkafka.env --results-dir ./perf-results

  # Then the Rust-native test on the already-deployed server (no re-upload):
  python3 tools/deploy_and_run_perf/deploy_and_run_perf.py admin@host \\
      --test rust-native --env-file ../rust.env --results-dir ./perf-results

  # Python binding perf tests (backend via CLIENT_VERSION in the .env; sync
  # unless --async). Producer:
  python3 tools/deploy_and_run_perf/deploy_and_run_perf.py admin@host \\
      --test python-producer --recreate-zip --env-file ../rust.env --results-dir ./perf-results

  # Async consumer (bootstrap installs a JRE + Kafka so it self-spawns load):
  python3 tools/deploy_and_run_perf/deploy_and_run_perf.py admin@host \\
      --test python-consumer --async --env-file ../rust.env --results-dir ./perf-results
"""

import argparse
import datetime
import glob
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
# This script lives at <repo>/tools/deploy_and_run_perf/. The repo it deploys is
# the repo root (two levels up). The zip artifact and the .env default to the
# repo's PARENT folder (outside the repo), where they are conventionally kept.
REPO_ROOT = os.path.abspath(os.path.join(HERE, os.pardir, os.pardir))
WORKSPACE = os.path.dirname(REPO_ROOT)
DEFAULT_PLOT = os.path.join(REPO_ROOT, "tools", "performance_metrics_plot", "plot_metrics.py")

# --------------------------------------------------------------------------- #
# Remote scripts (run on the Debian/Ubuntu server). Extraction of the repo is
# handled separately (only when --recreate-zip); bootstrap just installs deps
# and builds the repo recorded in .repo_path.
# --------------------------------------------------------------------------- #

BOOTSTRAP_SH = r"""#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TEST="${1:?usage: bootstrap.sh <rust-native|c-v2|c-v3|java|python-producer|python-consumer>}"
REPO="$(cat "$SCRIPT_DIR/.repo_path")"

echo "== apt: base packages =="
# zip is required by the sdkman installer (java path); unzip by the repo extract.
sudo apt update && sudo apt install -y wget curl git unzip zip build-essential cmake pkg-config \
  tmux gnupg ca-certificates

if [ "$TEST" = "java" ]; then
  # Java toolchain via sdkman: Corretto JDK + Gradle, then build the perf jar.
  export SDKMAN_DIR="$HOME/.sdkman"
  if [ ! -s "$SDKMAN_DIR/bin/sdkman-init.sh" ]; then
    curl -s "https://get.sdkman.io?rcupdate=false" | bash
  fi
  mkdir -p "$SDKMAN_DIR/etc"
  grep -q sdkman_auto_answer "$SDKMAN_DIR/etc/config" 2>/dev/null \
    || echo "sdkman_auto_answer=true" >> "$SDKMAN_DIR/etc/config"
  set +u
  source "$SDKMAN_DIR/bin/sdkman-init.sh"
  # Newest Corretto 21 sdkman offers (Gradle 8.x runs on <= 21; the perf jar
  # targets Java 17). Fall back to a known version if listing fails.
  JAVA_ID=$(sdk list java 2>/dev/null | tr -d '\r' | grep -oE '21\.[0-9]+\.[0-9]+-amzn' | head -n1)
  [ -z "${JAVA_ID:-}" ] && JAVA_ID="21.0.5-amzn"
  echo "Installing Corretto $JAVA_ID + Gradle 8.4 via sdkman ..."
  sdk install java "$JAVA_ID"
  sdk use java "$JAVA_ID"
  sdk install gradle 8.4
  set -u
  echo "== Building Java perf jar in $REPO/tools/java-perf-test =="
  ( cd "$REPO/tools/java-perf-test" && gradle shadowJar --console=plain )
  echo "== Bootstrap complete (java) =="
  exit 0
fi

if [[ "$TEST" == python-* ]]; then
  # Python binding perf tests. Build the Rust lib (release) + the _confluentkafka
  # extension into a venv; install confluent-kafka (v2 backend) + psutil. No kafka
  # submodule needed (only the Rust build + the C extension). For python-consumer
  # also install a JRE + Apache Kafka so the test self-spawns load via KAFKA_BIN.
  sudo apt install -y python3 python3-venv python3-pip rustup
  rustup default stable
  [ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
  export PATH="$HOME/.cargo/bin:$PATH"
  echo "== Building Rust (FFI, release) in $REPO =="
  cd "$REPO"
  RUSTFLAGS="-C target-cpu=native" cargo build --features ffi --release
  echo "== Creating venv + building the _confluentkafka extension =="
  python3 -m venv "$REPO/venv"
  set +u; . "$REPO/venv/bin/activate"; set -u
  pip install --upgrade pip setuptools wheel
  ( cd "$REPO/bindings/python" && \
    CONFLUENT_KAFKA_LIB_DIR="$REPO/target/release" CFLAGS="-O2 -march=native" pip install -e . )
  pip install "confluent-kafka>=2.13.0" psutil
  if [ "$TEST" = "python-consumer" ]; then
    KVER="${KAFKA_VERSION:-4.2.0}"
    if [ ! -x /opt/kafka/bin/kafka-producer-perf-test.sh ]; then
      echo "== Installing JRE + Apache Kafka $KVER (kafka-producer-perf-test.sh) =="
      sudo apt install -y default-jre
      TGZ="kafka_2.13-${KVER}.tgz"
      curl -fsSL -o "/tmp/$TGZ" "https://archive.apache.org/dist/kafka/${KVER}/${TGZ}"
      sudo rm -rf /opt/kafka && sudo mkdir -p /opt/kafka
      sudo tar -xzf "/tmp/$TGZ" -C /opt/kafka --strip-components=1
    fi
    echo "Kafka bin: /opt/kafka/bin"
  fi
  echo "== Bootstrap complete (python) =="
  exit 0
fi

# Rust / C backends: rustup + librdkafka (Confluent) + build the Rust lib & C test.
sudo apt install -y rustup
rustup default stable

echo "== Confluent clients apt repo + librdkafka-dev =="
wget -qO - https://packages.confluent.io/clients/deb/archive.key \
  | sudo gpg --dearmor --yes -o /usr/share/keyrings/confluent-clients-archive-keyring.gpg
. /etc/os-release
# The Confluent clients repo serves Ubuntu/Debian codenames but not the very
# newest Debian (e.g. trixie). Use the host codename if available, else fall
# back to the newest Debian codename the repo serves (bookworm).
CONFLUENT_CODENAME="${VERSION_CODENAME}"
if ! wget -q --spider "https://packages.confluent.io/clients/deb/dists/${CONFLUENT_CODENAME}/Release"; then
  echo "Confluent repo has no '${CONFLUENT_CODENAME}' dist; falling back to 'bookworm'"
  CONFLUENT_CODENAME="bookworm"
fi
echo "deb [signed-by=/usr/share/keyrings/confluent-clients-archive-keyring.gpg] https://packages.confluent.io/clients/deb/ ${CONFLUENT_CODENAME} main" \
  | sudo tee /etc/apt/sources.list.d/confluent-clients.list >/dev/null
printf 'Package: librdkafka*\nPin: origin packages.confluent.io\nPin-Priority: 1001\n' \
  | sudo tee /etc/apt/preferences.d/confluent-librdkafka >/dev/null
sudo apt update && sudo apt install -y librdkafka-dev
apt-cache policy librdkafka-dev

echo "== Building Rust (FFI, release) + C producer_perf_test in $REPO =="
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
export PATH="$HOME/.cargo/bin:$PATH"
cd "$REPO"
RUSTFLAGS="-C target-cpu=native" cargo build --features ffi --release
cmake -S bindings/c -B bindings/c/build -DRUST_PROJECT_ROOT="$REPO" -DCMAKE_C_FLAGS="-O2 -march=native"
cmake --build bindings/c/build --target producer_perf_test
echo "== Bootstrap complete =="
"""

RUN_PERF_SH = r"""#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TEST="${1:?usage: run-perf.sh <rust-native|c-v2|c-v3|java|python-producer|python-consumer>}"
RESULTS="$SCRIPT_DIR/results"
mkdir -p "$RESULTS"
exec > >(tee "$RESULTS/run.log") 2>&1   # mirror all output to the log

REPO="$(cat "$SCRIPT_DIR/.repo_path")"
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
export PATH="$HOME/.cargo/bin:$PATH"

# Parameters from the .env file. Only one test runs per invocation, so each run
# may use a different .env: BOOTSTRAP_SERVERS, TOPIC_NAME, VALUE_SIZE, LIMIT_RPS,
# TEST_DURATION_SECONDS, WARMUP_SECONDS, P99_LIMIT_MS, SECURITY_PROTOCOL/SASL_*, etc.
set -a
[ -f "$SCRIPT_DIR/perf-test.env" ] && . "$SCRIPT_DIR/perf-test.env"
set +a

# The --async flag (python tests) is delivered as RUN_ASYNC=1 in the launch env;
# apply it AFTER sourcing the .env so the flag wins over any ASYNC there.
if [ "${RUN_ASYNC:-0}" = "1" ]; then export ASYNC=True; fi

cd "$REPO"
case "$TEST" in
  rust-native)
    echo "######## Rust native producer perf test ########"
    METRICS_FILE="$RESULTS/rust-native.jsonl" cargo xtask producer-perf-test --test-threads=1
    ;;
  c-v3)
    echo "######## C v3 (Rust client via C FFI) ########"
    mkdir -p "$RESULTS/c-v3"
    ( cd "$RESULTS/c-v3" && CLIENT_VERSION=3 "$REPO/bindings/c/build/producer_perf_test" )
    ;;
  c-v2)
    echo "######## C v2 (librdkafka baseline) ########"
    mkdir -p "$RESULTS/c-v2"
    ( cd "$RESULTS/c-v2" && CLIENT_VERSION=2 "$REPO/bindings/c/build/producer_perf_test" )
    ;;
  java)
    echo "######## Java producer perf test (Apache Kafka client) ########"
    export SDKMAN_DIR="$HOME/.sdkman"
    set +u
    [ -s "$SDKMAN_DIR/bin/sdkman-init.sh" ] && source "$SDKMAN_DIR/bin/sdkman-init.sh"
    set -u
    mkdir -p "$RESULTS/java"
    ( cd "$RESULTS/java" && java -jar "$REPO/tools/java-perf-test/build/libs/java-perf-test-all.jar" )
    ;;
  python-producer)
    echo "######## Python producer perf test (ASYNC=${ASYNC:-False}, CLIENT_VERSION=${CLIENT_VERSION:-3}) ########"
    set +u; . "$REPO/venv/bin/activate"; set -u
    mkdir -p "$RESULTS/python-producer"
    ( cd "$RESULTS/python-producer" && \
      python "$REPO/bindings/python/test/performance/producer_performance_test.py" )
    ;;
  python-consumer)
    echo "######## Python consumer perf test (ASYNC=${ASYNC:-False}, CLIENT_VERSION=${CLIENT_VERSION:-3}) ########"
    set +u; . "$REPO/venv/bin/activate"; set -u
    # Self-spawn load via kafka-producer-perf-test.sh from the installed Kafka
    # (KAFKA_BIN in the .env overrides). Without it the test is consume-only.
    export KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
    mkdir -p "$RESULTS/python-consumer"
    ( cd "$RESULTS/python-consumer" && \
      python "$REPO/bindings/python/test/performance/consumer_performance_test.py" )
    ;;
  *)
    echo "unknown test: $TEST (expected rust-native|c-v2|c-v3|java|python-producer|python-consumer)" >&2
    exit 2
    ;;
esac
echo "######## Done: $TEST. Metrics under $RESULTS ########"
"""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def die(msg):
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def run(cmd, **kw):
    """Run a local command, echoing it; raises on non-zero exit."""
    print("+ " + " ".join(shlex.quote(c) for c in cmd))
    return subprocess.run(cmd, check=True, **kw)


def ssh(host, remote_cmd, ssh_opts, tty=False, stdin=None, check=True):
    cmd = ["ssh", *ssh_opts]
    if tty:
        cmd.append("-t")
    cmd += [host, remote_cmd]
    print(f"+ ssh {host} {remote_cmd!r}" if not tty else f"+ ssh -t {host} {remote_cmd!r}")
    return subprocess.run(cmd, input=stdin, text=True, check=check)


def wait_for_session(host, session, ssh_opts, poll_seconds=20):
    """Block until the remote tmux `session` is gone, polling with a fresh
    short-lived ssh each iteration. Long runs can outlast a single ssh
    connection (a transient "server not responding" would otherwise kill the
    whole wait), so we reconnect every poll and only stop on a *definitive*
    "gone": ssh succeeded (exit 0) AND tmux reported the session absent. A
    failed poll (ssh non-zero / empty output) is treated as "keep waiting"."""
    probe = f"tmux has-session -t {shlex.quote(session)} 2>/dev/null && echo ALIVE || echo GONE"
    while True:
        r = subprocess.run(["ssh", *ssh_opts, host, probe],
                           text=True, capture_output=True)
        if r.returncode == 0 and r.stdout.strip() == "GONE":
            return
        time.sleep(poll_seconds)


def scp(src, dst, ssh_opts, recursive=False, check=True):
    cmd = ["scp", *ssh_opts]
    if recursive:
        cmd.append("-r")
    cmd += [src, dst]
    print("+ " + " ".join(shlex.quote(c) for c in cmd))
    subprocess.run(cmd, check=check)


def create_zip(repo_dir, zip_path):
    """Replicate clean-and-zip.sh: copy the repo to a temp dir, strip git-ignored
    files (keeping .git + tracked + untracked files), drop the bundled `kafka`
    dir, and (over)write the zip. The original repo is never modified."""
    repo_dir = os.path.abspath(repo_dir)
    zip_path = os.path.abspath(zip_path)
    repo_name = os.path.basename(repo_dir)
    if not os.path.isdir(os.path.join(repo_dir, ".git")):
        die(f"'{repo_dir}' is not a git repository")
    if shutil.which("zip") is None:
        die("'zip' is not installed locally")

    with tempfile.TemporaryDirectory(prefix="deploy-perf.") as work:
        copy = os.path.join(work, repo_name)
        print(f"==> Creating zip from {repo_dir}")
        run(["cp", "-a", repo_dir, copy])
        # -d recurse, -X ignored only, -f force; keeps .git + tracked + untracked.
        run(["git", "-C", copy, "clean", "-dXf"])
        shutil.rmtree(os.path.join(copy, "kafka"), ignore_errors=True)
        if os.path.exists(zip_path):
            os.remove(zip_path)
        run(["zip", "-rq", zip_path, repo_name], cwd=work)
    print(f"==> Wrote {zip_path}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    p = argparse.ArgumentParser(
        description="Deploy + build + run the producer perf tests on a remote host over SSH.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("host", help="SSH target, e.g. user@host")
    p.add_argument("--test", required=True,
                   choices=["rust-native", "c-v2", "c-v3", "java",
                            "python-producer", "python-consumer"],
                   help="which single test to run. Only one runs per invocation, since the "
                        "env vars (in --env-file) can differ per test: "
                        "rust-native = Rust client in-process; "
                        "c-v2 = librdkafka; c-v3 = Rust client via C FFI; "
                        "java = Apache Kafka Java client (tools/java-perf-test, built with "
                        "Corretto + Gradle via sdkman); "
                        "python-producer / python-consumer = the Python binding perf tests "
                        "(backend via CLIENT_VERSION in --env-file: 3 = Rust binding, "
                        "2 = confluent-kafka; sync unless --async).")
    p.add_argument("--async", dest="run_async", action="store_true",
                   help="for python-producer / python-consumer, drive the asyncio-native "
                        "client (sets ASYNC=True, overriding the env-file). Ignored by the "
                        "non-Python tests.")
    p.add_argument("--kafka-version", default="4.2.0",
                   help="for python-consumer, the Apache Kafka version whose "
                        "kafka-producer-perf-test.sh is installed to /opt/kafka to generate "
                        "load (default: 4.2.0). KAFKA_BIN in --env-file overrides the path.")
    p.add_argument("--env-file", default=os.path.join(WORKSPACE, ".env"),
                   help="parameters file sourced for the run (default: <repo-parent>/.env)")
    p.add_argument("--repo-dir", default=REPO_ROOT,
                   help="local repo directory to zip (default: the repo this script lives in)")
    p.add_argument("--zip", default=os.path.join(WORKSPACE, os.path.basename(REPO_ROOT) + ".zip"),
                   help="zip path (default: <repo-parent>/<repo-name>.zip)")
    p.add_argument("--recreate-zip", action="store_true",
                   help="recreate+overwrite the local zip, upload it, and re-extract on the "
                        "server replacing the existing folder (fresh code + full rebuild). "
                        "Without this, the repo already on the server is reused.")
    p.add_argument("--results-dir",
                   help="if set, wait for the run, then copy remote metrics into "
                        "<results-dir>/<UTC-ISO-timestamp>/")
    p.add_argument("--plot-script", default=DEFAULT_PLOT,
                   help="with --results-dir, render each metrics .jsonl to a .md via "
                        "`python3 <plot-script> <jsonl> <md>` (default: the repo's "
                        "tools/performance_metrics_plot/plot_metrics.py)")
    p.add_argument("--remote-base", default="perf-test",
                   help="remote working dir, relative to $HOME (default: perf-test)")
    p.add_argument("--ssh-opts", default="",
                   help='extra ssh/scp options, e.g. "-i ~/.ssh/key -p 2222"')
    args = p.parse_args()

    ssh_opts = ["-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=30", *shlex.split(args.ssh_opts)]
    host = args.host
    base = args.remote_base
    repo_name = os.path.basename(os.path.abspath(args.repo_dir))

    if not os.path.isfile(args.env_file):
        die(f"env file not found: {args.env_file}")
    if args.plot_script and not os.path.isfile(args.plot_script):
        die(f"plot script not found: {args.plot_script}")

    # 1. (Re)create the zip locally if requested.
    if args.recreate_zip:
        create_zip(args.repo_dir, args.zip)
    elif not os.path.isfile(args.zip):
        # Not recreating and no local zip: that's fine only if the repo is
        # already on the server. We verify that below.
        pass

    # 2. Remote working dir + env file (always refreshed).
    print(f"==> [1/5] Preparing remote dir '{base}' on {host}")
    ssh(host, f"mkdir -p {shlex.quote(base)}", ssh_opts)
    print("==> [2/5] Copying env file")
    scp(args.env_file, f"{host}:{base}/perf-test.env", ssh_opts)

    # 3. Upload + extract the repo, replacing the server folder (only on recreate).
    if args.recreate_zip:
        if not os.path.isfile(args.zip):
            die(f"zip not found after creation: {args.zip}")
        print("==> [3/5] Uploading zip and replacing the server folder")
        scp(args.zip, f"{host}:{base}/", ssh_opts)
        zip_base = os.path.basename(args.zip)
        extract = (
            # Ensure unzip exists before extracting: on a fresh host the
            # bootstrap (which installs it) has not run yet.
            "command -v unzip >/dev/null 2>&1 || "
            "{ sudo apt-get update -qq && sudo apt-get install -y unzip; } && "
            f"cd {shlex.quote(base)} && "
            f"rm -rf {shlex.quote(repo_name)} && "
            f"unzip -oq {shlex.quote(zip_base)} && "
            f"readlink -f {shlex.quote(repo_name)} > .repo_path && "
            f"echo 'repo: '$(cat .repo_path)"
        )
        ssh(host, extract, ssh_opts)
    else:
        print("==> [3/5] Reusing repo already on the server (no zip upload)")
        r = ssh(host, f"test -f {shlex.quote(base)}/.repo_path", ssh_opts, check=False)
        if r.returncode != 0:
            die(f"no repo on {host}:{base} (.repo_path missing). "
                f"Run once with --recreate-zip first.")

    # 4. Write the remote bootstrap + run scripts, then bootstrap (install+build).
    print("==> [4/5] Writing remote scripts and installing + building (sudo may prompt)")
    ssh(host, f"cat > {shlex.quote(base)}/bootstrap.sh", ssh_opts, stdin=BOOTSTRAP_SH)
    ssh(host, f"cat > {shlex.quote(base)}/run-perf.sh", ssh_opts, stdin=RUN_PERF_SH)
    ssh(host, f"chmod +x {shlex.quote(base)}/bootstrap.sh {shlex.quote(base)}/run-perf.sh", ssh_opts)
    # KAFKA_VERSION is only consumed by the python-consumer bootstrap (installs
    # the matching kafka-producer-perf-test.sh); harmless for the other tests.
    ssh(host, f"KAFKA_VERSION={shlex.quote(args.kafka_version)} "
              f"bash {shlex.quote(base)}/bootstrap.sh {shlex.quote(args.test)}", ssh_opts, tty=True)

    # 5. Launch the selected test in a detached tmux session.
    print(f"==> [5/5] Launching '{args.test}' in detached tmux session 'perftest'"
          + (" (async)" if args.run_async else ""))
    # --async is delivered as RUN_ASYNC=1 in the run's environment (run-perf.sh
    # turns it into ASYNC=True after sourcing the .env, so it overrides the file).
    async_prefix = "RUN_ASYNC=1 " if args.run_async else ""
    launch = (
        f"mkdir -p {shlex.quote(base)}/results; "
        f"tmux kill-session -t perftest 2>/dev/null; "
        f"tmux new-session -d -s perftest "
        f"'{async_prefix}bash {shlex.quote(base)}/run-perf.sh {shlex.quote(args.test)}'"
    )
    ssh(host, launch, ssh_opts)

    if not args.results_dir:
        print(f"""
Deployed and started. The '{args.test}' test is running in tmux on {host}.
  Watch live : ssh {args.ssh_opts} -t {host} 'tmux attach -t perftest'
  Tail log   : ssh {args.ssh_opts} {host} 'tail -f {base}/results/run.log'
  Results    : {base}/results/  (metrics for '{args.test}')
  (pass --results-dir to auto-copy + plot when the run finishes.)""")
        return

    # 6. Wait for the run to finish (resilient poll: reconnect each iteration so
    #    a transient ssh drop doesn't abort a long run), then fetch.
    print(f"==> Waiting for the test run to finish on {host} ...")
    wait_for_session(host, "perftest", ssh_opts)

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    dest = os.path.join(args.results_dir, ts)
    os.makedirs(dest, exist_ok=True)
    print(f"==> Copying '{args.test}' metrics to {dest}")
    # Copy only THIS test's output, not the whole remote results/ dir (which
    # accumulates other tests' files across runs on the same server).
    if args.test == "rust-native":
        scp(f"{host}:{base}/results/rust-native.jsonl", dest + "/", ssh_opts)
    else:  # c-v2/c-v3/java/python-* write metrics.jsonl inside results/<test>/
        scp(f"{host}:{base}/results/{args.test}", dest + "/", ssh_opts, recursive=True)
    # The run log is per-run (truncated each run); copy it best-effort.
    scp(f"{host}:{base}/results/run.log", dest + "/", ssh_opts, check=False)

    if args.plot_script:
        print(f"==> Plotting metrics with {args.plot_script}")
        for jsonl in sorted(glob.glob(os.path.join(dest, "**", "*.jsonl"), recursive=True)):
            out = jsonl[: -len(".jsonl")] + ".md"
            r = subprocess.run([sys.executable, args.plot_script, jsonl, out],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(f"  {'plotted' if r.returncode == 0 else 'WARN failed'} {jsonl} -> {out}")

    print(f"\nDone. Metrics + plots saved under: {dest}")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        die(f"command failed (exit {e.returncode}): {' '.join(map(str, e.cmd))}")
    except KeyboardInterrupt:
        sys.exit(130)
