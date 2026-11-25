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

"""Tests for run.sh: HI mode, override precedence, argument handling and log
rotation.

These drive the real run.sh with a stub interpreter in place of Python, so what
is asserted is the arguments and the client config the supervisor *would* hand
the soak client — no broker, no bindings, no OpenTelemetry. The stub exits 2
(the "fatal, never restart" code), which makes run.sh stop after exactly one
child instead of looping. The rotation tests use stubs of their own.

They need only bash (and the perl and bzip2 run.sh itself needs), and run on
macOS and Linux alike.
"""

import bz2
import contextlib
import os
import re
import shutil
import signal
import subprocess
import time

import pytest

SOAK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN_SH = os.path.join(SOAK_DIR, "run.sh")

pytestmark = pytest.mark.skipif(shutil.which("bash") is None,
                                reason="run.sh needs bash")


def run_supervisor(tmp_path, args, env=None, config_body=None):
    """Invoke run.sh with a stub child.

    Returns `(result, argv, effective_config)` — the argv run.sh assembled and
    the contents of the client config it actually passed with `-f`.
    """
    stub = tmp_path / "stub-python"
    argv_file = tmp_path / "argv.txt"
    config_copy = tmp_path / "effective-config.txt"
    calls_file = tmp_path / "calls.txt"
    stub.write_text(
        "#!/bin/sh\n"
        "echo call >> " + str(calls_file) + "\n"
        "# run.sh's pre-flight invokes the interpreter as `-c <script>` to check\n"
        "# that soakclient imports; succeed there so the supervise loop is\n"
        "# reached.\n"
        'if [ "$1" = "-c" ]; then exit 0; fi\n'
        "# Otherwise record the argv run.sh assembled and the config it points\n"
        "# at, then exit 2 so the supervisor treats it as fatal and stops.\n"
        'printf "%s\\n" "$@" > ' + str(argv_file) + "\n"
        "while [ $# -gt 0 ]; do\n"
        '  if [ "$1" = "-f" ]; then cp "$2" ' + str(config_copy) + "; fi\n"
        "  shift\n"
        "done\n"
        "exit 2\n")
    stub.chmod(0o755)

    config = tmp_path / "client.config"
    config.write_text(config_body or "bootstrap.servers=localhost:9092\n")

    full_env = dict(os.environ)
    full_env.pop("HI", None)
    for key in list(full_env):
        if key.startswith("SOAK_"):
            del full_env[key]
    full_env.update({
        "TESTID": "t1",
        "SOAK_PYTHON": str(stub),
        "SOAK_LOG_DIR": str(tmp_path),
        # The supervisor's liveness poll; 1 s in production, and these cases
        # should not each pay for it.
        "SOAK_POLL_INTERVAL": "0.05",
    })
    full_env.update(env or {})

    result = subprocess.run(
        ["bash", RUN_SH] + args, cwd=str(tmp_path), env=full_env,
        capture_output=True, text=True, timeout=120)
    argv = argv_file.read_text().splitlines() if argv_file.exists() else []
    effective = config_copy.read_text() if config_copy.exists() else ""
    result.interpreter_calls = (len(calls_file.read_text().splitlines())
                                if calls_file.exists() else 0)
    return result, argv, effective


def arg_value(argv, flag):
    """The value following `flag` in an argv list."""
    return argv[argv.index(flag) + 1] if flag in argv else None


# ---------------------------------------------------------------------------
# HI mode
# ---------------------------------------------------------------------------
#: The client tuning HI mode appends. Without these the 10 KB variant just
#: carries bigger records: the consumer fetches a handful per poll and the
#: producer sends a batch per record.
HI_TUNING_KEYS = (
    "consumer.fetch.max.bytes=52428800",
    "consumer.max.partition.fetch.bytes=10485760",
    "producer.batch.size=1048576",
    "producer.compression.type=lz4",
)


def test_default_mode_is_normal(tmp_path):
    _result, argv, config = run_supervisor(tmp_path, ["client.config"])
    assert arg_value(argv, "--variant") == "848-normal"
    assert arg_value(argv, "--payload-size") == "50"
    assert arg_value(argv, "-r") == "80"
    for key in HI_TUNING_KEYS:
        assert key not in config


def test_hi_sets_rate_payload_and_appends_the_tuning_keys(tmp_path):
    _result, argv, config = run_supervisor(tmp_path, ["client.config"],
                                           {"HI": "true"})
    assert arg_value(argv, "--variant") == "848-hi-throughput"
    assert arg_value(argv, "--payload-size") == "10240"
    # High throughput raises the rate too: 1000 msg/s at a 10 KB payload.
    assert arg_value(argv, "-r") == "1000"
    for key in HI_TUNING_KEYS:
        assert key in config, "HI mode dropped {}".format(key)


def test_hi_preserves_the_operators_own_config(tmp_path):
    body = ("bootstrap.servers=broker:9092\n"
            "consumer.group.protocol=consumer\n"
            "sasl.jaas.config=org.apache...PlainLoginModule required "
            'username="k" password="s";\n')
    _result, _argv, config = run_supervisor(tmp_path, ["client.config"],
                                            {"HI": "true"}, config_body=body)
    for line in body.strip().splitlines():
        assert line in config
    for key in HI_TUNING_KEYS:
        assert key in config


@pytest.mark.parametrize("flag", ["true", "TRUE", "True", "1", "yes"])
def test_hi_accepts_the_usual_spellings_of_true(tmp_path, flag):
    _result, argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {"HI": flag})
    assert arg_value(argv, "--payload-size") == "10240"


@pytest.mark.parametrize("flag", ["false", "no", "0", ""])
def test_hi_off_keeps_the_normal_payload(tmp_path, flag):
    _result, argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {"HI": flag})
    assert arg_value(argv, "--payload-size") == "50"
    assert arg_value(argv, "--variant") == "848-normal"


# ---------------------------------------------------------------------------
# Pre-flight: a broken install must be diagnosed, not restarted
# ---------------------------------------------------------------------------
def test_preflight_refuses_an_interpreter_that_cannot_import_soakclient(tmp_path):
    """A venv missing a dependency must fail fatally, with the real reason.

    An import-time crash happens before main() runs, so the child cannot choose
    its exit code and the interpreter's exit 1 would otherwise be read as
    "message loss" and restarted with backoff — eventually writing a .FAILED
    marker that names the wrong problem. This is a likely first run on a fresh
    box, so it is diagnosed up front instead.
    """
    calls_file = tmp_path / "calls.txt"
    broken = tmp_path / "broken-python"
    broken.write_text(
        "#!/bin/sh\n"
        "echo call >> " + str(calls_file) + "\n"
        "cat >&2 <<'ERR'\n"
        "Traceback (most recent call last):\n"
        '  File "<string>", line 4, in <module>\n'
        '  File "/w/python/soak/soakclient.py", line 59, in <module>\n'
        "    import psutil\n"
        "ModuleNotFoundError: No module named 'psutil'\n"
        "ERR\n"
        "exit 1\n")
    broken.chmod(0o755)

    config = tmp_path / "client.config"
    config.write_text("bootstrap.servers=localhost:9092\n")

    env = dict(os.environ)
    for key in list(env):
        if key.startswith("SOAK_") or key == "HI":
            del env[key]
    env.update({"TESTID": "t1", "SOAK_PYTHON": str(broken),
                "SOAK_LOG_DIR": str(tmp_path), "SOAK_POLL_INTERVAL": "0.05",
                # Would allow several restarts if the pre-flight did not stop it.
                "SOAK_RESTART_DELAY": "0", "SOAK_MAX_RAPID_FAILURES": "5"})

    result = subprocess.run(["bash", RUN_SH, "client.config"], cwd=str(tmp_path),
                            env=env, capture_output=True, text=True, timeout=120)
    output = result.stdout + result.stderr

    # Fatal, so the supervisor never restarts it.
    assert result.returncode == 2, output
    # Exactly one interpreter invocation: the pre-flight. No restart loop.
    calls = (len(calls_file.read_text().splitlines())
             if calls_file.exists() else 0)
    assert calls == 1, "interpreter ran {} times, expected 1".format(calls)
    # The underlying error is visible, not summarised away.
    assert "ModuleNotFoundError" in output
    assert "psutil" in output
    # Diagnosed as an installation problem, not as message loss.
    assert "could not be imported" in output
    assert "message loss" not in output.lower()
    # Nothing was rotated, and the operator gets the marker.
    assert not list(tmp_path.glob("t1-848-normal.log.*"))
    marker = tmp_path / "t1-848-normal.FAILED"
    assert marker.exists()
    assert "could not be imported" in marker.read_text()


def test_preflight_passes_for_a_working_interpreter(tmp_path):
    """The pre-flight must not add an invocation to the happy path.

    One pre-flight plus one real child = two.
    """
    result, argv, _config = run_supervisor(tmp_path, ["client.config"])
    assert argv, "the supervise loop was never reached"
    assert result.interpreter_calls == 2, \
        "expected pre-flight + one child, got {}".format(result.interpreter_calls)


# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
def test_missing_config_argument_prints_usage(tmp_path):
    result, _argv, _config = run_supervisor(tmp_path, [])
    assert result.returncode == 2
    assert "Usage:" in result.stderr


def test_extra_arguments_print_usage(tmp_path):
    """The old two-positional profile form must not silently half-work."""
    result, _argv, _config = run_supervisor(
        tmp_path, ["client.config", "client.config"])
    assert result.returncode == 2
    assert "Usage:" in result.stderr


def test_a_missing_config_file_is_refused(tmp_path):
    result, argv, _config = run_supervisor(tmp_path, ["nope.config"])
    assert result.returncode == 2
    assert "no such file" in result.stderr
    assert argv == []


def test_missing_testid_is_refused(tmp_path):
    result, _argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {"TESTID": ""})
    assert result.returncode == 2
    assert "TESTID" in result.stderr


# ---------------------------------------------------------------------------
# Override precedence: the environment wins
# ---------------------------------------------------------------------------
def test_env_rate_overrides_the_default(tmp_path):
    """The original regression: profiles used bare assignments, so `source`
    clobbered the operator's exported value back to the default — SOAK_RATE=200
    appeared to work and produced at 80. With the profiles gone the default can
    no longer win, and this pins that."""
    _result, argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {"SOAK_RATE": "200"})
    assert arg_value(argv, "-r") == "200"


@pytest.mark.parametrize("var,flag,value", [
    ("SOAK_RATE", "-r", "250"),
    ("SOAK_PAYLOAD_SIZE", "--payload-size", "4096"),
    ("SOAK_PARTITIONS", "--partitions", "6"),
    ("SOAK_REPLICATION_FACTOR", "--replication-factor", "3"),
    ("SOAK_VARIANT", "--variant", "848-hi-throughput-rolling"),
])
def test_every_scalar_tunable_is_overridable(tmp_path, var, flag, value):
    _result, argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {var: value})
    assert arg_value(argv, flag) == value


def test_env_overrides_apply_in_hi_mode_too(tmp_path):
    _result, argv, config = run_supervisor(
        tmp_path, ["client.config"],
        {"HI": "true", "SOAK_RATE": "200", "SOAK_PAYLOAD_SIZE": "2048"})
    assert arg_value(argv, "-r") == "200"
    # An explicit payload size wins over HI's default...
    assert arg_value(argv, "--payload-size") == "2048"
    # ...but the tuning keys still apply, because HI mode is still on.
    for key in HI_TUNING_KEYS:
        assert key in config


def test_variant_override_labels_a_rolled_cluster(tmp_path):
    """There is no rolling switch; labelling is how a rolled run is identified."""
    _result, argv, _config = run_supervisor(
        tmp_path, ["client.config"],
        {"HI": "true", "SOAK_VARIANT": "848-hi-throughput-rolling"})
    assert arg_value(argv, "--variant") == "848-hi-throughput-rolling"
    assert arg_value(argv, "--payload-size") == "10240"
    assert arg_value(argv, "-t") == "rustsoak-t1-848-hi-throughput-rolling"


def test_extra_args_are_appended(tmp_path):
    _result, argv, _config = run_supervisor(
        tmp_path, ["client.config"],
        {"SOAK_EXTRA_ARGS": "--stall-threshold 42 --log-level DEBUG"})
    assert arg_value(argv, "--stall-threshold") == "42"
    assert arg_value(argv, "--log-level") == "DEBUG"


def test_soak_topic_override(tmp_path):
    _result, argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {"SOAK_TOPIC": "my-topic"})
    assert arg_value(argv, "-t") == "my-topic"


def test_default_topic_includes_testid_and_variant(tmp_path):
    _result, argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {"HI": "true"})
    assert arg_value(argv, "-t") == "rustsoak-t1-848-hi-throughput"


def test_brokers_override_is_passed_through(tmp_path):
    _result, argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {"SOAK_BROKERS": "broker:9092"})
    assert arg_value(argv, "-b") == "broker:9092"


# ---------------------------------------------------------------------------
# Log rotation must never destroy crash evidence
# ---------------------------------------------------------------------------
def test_a_rapid_failure_that_pushes_the_log_past_the_limit_is_not_rotated(
        tmp_path):
    """maybe_rotate_log must not run on the rapid-failure path.

    Reproduces the exact scenario: the log is already near LIMIT, then a
    child that crashes almost immediately (lifetime < RAPID_FAILURE_SECONDS)
    prints enough output to push the log over LIMIT on its way out. Rotating
    here would move that crash's own few-KB fragment out of the log, the first
    place an operator looks for why it died, which is exactly what run.sh's
    own comment on maybe_rotate_log says must never happen.
    """
    stub = tmp_path / "stub-python"
    # Rapid, non-fatal failure (not EXIT_FATAL=2) that itself prints enough to
    # push a near-limit log over LIMIT before the supervisor even measures it.
    stub.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "-c" ]; then exit 0; fi\n'
        "head -c 200 /dev/zero | tr '\\0' 'x'\n"
        "exit 1\n")
    stub.chmod(0o755)

    config = tmp_path / "client.config"
    config.write_text("bootstrap.servers=localhost:9092\n")

    logfile = tmp_path / "t1-848-normal.log"
    # Already near the limit before the crashing child ever runs.
    logfile.write_text("x" * 900)

    full_env = dict(os.environ)
    for key in list(full_env):
        if key.startswith("SOAK_") or key == "HI":
            del full_env[key]
    full_env.update({
        "TESTID": "t1",
        "SOAK_PYTHON": str(stub),
        "SOAK_LOG_DIR": str(tmp_path),
        "SOAK_POLL_INTERVAL": "0.05",
        "SOAK_LOG_LIMIT_BYTES": "1000",
        # One rapid failure is enough to trip give_up deterministically,
        # rather than looping.
        "SOAK_MAX_RAPID_FAILURES": "1",
        "SOAK_RAPID_FAILURE_SECONDS": "60",
        "SOAK_RESTART_DELAY": "0",
    })

    result = subprocess.run(
        ["bash", RUN_SH, "client.config"], cwd=str(tmp_path), env=full_env,
        capture_output=True, text=True, timeout=60)

    # run.sh exits with the last child's own status (1, here) once give_up
    # stops the loop -- the crash-loop detection is what we are testing, not
    # the exit code, so assert on the log line give_up emits.
    assert result.returncode == 1, result.stdout + result.stderr
    assert "crash loop" in (result.stdout + result.stderr)

    # The log grew past LIMIT (the scenario is real)...
    assert logfile.exists()
    assert logfile.stat().st_size >= 1000
    # ...but must not have been rotated away.
    assert not list(tmp_path.glob("t1-848-normal.log.*"))
    assert "x" * 900 in logfile.read_text()


# ---------------------------------------------------------------------------
# Rotation never stops the client
# ---------------------------------------------------------------------------
def write_stub(tmp_path, body):
    """A stub interpreter that passes run.sh's pre-flight, then runs `body`."""
    stub = tmp_path / "stub-python"
    stub.write_text("#!/bin/sh\n"
                    'if [ "$1" = "-c" ]; then exit 0; fi\n' + body)
    stub.chmod(0o755)
    return stub


def supervisor_env(tmp_path, stub, extra=None):
    """The environment for running run.sh against `stub` in `tmp_path`."""
    (tmp_path / "client.config").write_text("bootstrap.servers=localhost:9092\n")
    env = dict(os.environ)
    for key in list(env):
        if key.startswith("SOAK_") or key == "HI":
            del env[key]
    env.update({"TESTID": "t1", "SOAK_PYTHON": str(stub),
                "SOAK_LOG_DIR": str(tmp_path), "SOAK_POLL_INTERVAL": "0.05",
                # Rotation waits for free space; these tests should not depend
                # on how full the disk of the machine running them is.
                "SOAK_LOG_MIN_FREE_BYTES": "0"})
    env.update(extra or {})
    return env


@contextlib.contextmanager
def supervising(tmp_path, env, out_name="supervisor.out"):
    """Run run.sh in the background for the duration of the block, then stop
    it with SIGTERM, as an operator would. Yields the path of its output."""
    out_path = tmp_path / out_name
    with open(str(out_path), "w") as out:
        proc = subprocess.Popen(["bash", RUN_SH, "client.config"],
                                cwd=str(tmp_path), env=env, stdout=out,
                                stderr=subprocess.STDOUT)
        try:
            yield out_path
        finally:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=120)


def fake_tool(tmp_path, name, body):
    """Put a stand-in for the command `name` first on run.sh's PATH, and
    return that PATH."""
    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir(exist_ok=True)
    tool = bin_dir / name
    tool.write_text("#!/bin/sh\n" + body)
    tool.chmod(0o755)
    return str(bin_dir) + os.pathsep + os.environ["PATH"]


def wait_for(condition, timeout=30.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.05)
    return True


#: A rotated log, compressed or not. Not a copy or a compression in progress.
ROTATED_PART = re.compile(r"t1-848-normal\.log\.\d{8}T\d{6}Z(\.bz2)?$")


def rotated_parts(tmp_path):
    """Names of rotated logs, ignoring whether each is compressed yet."""
    names = {p.name[:-len(".bz2")] if p.name.endswith(".bz2") else p.name
             for p in tmp_path.glob("t1-848-normal.log.[0-9]*")
             if ROTATED_PART.match(p.name)}
    return sorted(names)


def compression_finished(tmp_path):
    """Nothing left to compress, and no copy or compression in progress."""
    return not any(not p.name.endswith(".bz2")
                   for p in tmp_path.glob("t1-848-normal.log.[0-9]*"))


def test_rotation_keeps_the_client_running_and_loses_nothing(tmp_path):
    """The log is rotated under a client that keeps writing, and the client is
    never stopped: one start, one PID, and every line it wrote lands, in order,
    in exactly one rotated part or the live log.

    The child's `>>` redirection is what makes truncating the log under it
    safe; with `>` the live log would fill with NULs, which this also checks.
    """
    stub = write_stub(tmp_path, (
        "i=0\n"
        'trap \'echo "pid=$$ last=$i"; exit 0\' TERM\n'
        "while :; do\n"
        '  echo "pid=$$ seq=$i padding-padding-padding-padding"\n'
        "  i=$((i + 1))\n"
        "  sleep 0.01\n"
        "done\n"))
    env = supervisor_env(tmp_path, stub, {"SOAK_LOG_LIMIT_BYTES": "4096"})
    with supervising(tmp_path, env) as out_path:
        assert wait_for(lambda: len(rotated_parts(tmp_path)) >= 3), \
            "the log was never rotated"
    assert wait_for(lambda: compression_finished(tmp_path), timeout=30)
    output = out_path.read_text()

    # Never restarted: one start, and every rotation said the client kept
    # running.
    assert output.count("Soak client started") == 1, output
    parts = rotated_parts(tmp_path)
    assert output.count("kept running") == len(parts), output

    seqs, pids, stopped_cleanly = [], set(), False
    for name in parts + ["t1-848-normal.log"]:
        path = tmp_path / name
        data = (bz2.open(str(path) + ".bz2").read() if name != "t1-848-normal.log"
                else path.read_bytes())
        assert b"\0" not in data, "{} has NULs: was it opened with >?".format(name)
        for line in data.decode().splitlines():
            m = re.match(r"pid=(\d+) (seq|last)=(\d+)", line)
            if m:
                pids.add(m.group(1))
                if m.group(2) == "seq":
                    seqs.append(int(m.group(3)))
                else:
                    stopped_cleanly = True
    assert len(pids) == 1, pids
    assert stopped_cleanly, "the client's own shutdown line is missing"
    # In order and no duplicates, across every part boundary...
    assert seqs == sorted(set(seqs))
    # ...and nothing missing. Copy-then-truncate can only lose a write landing
    # in the microseconds between rotate_log's last read and its truncate, so
    # allow at most one line per rotation rather than make this test flaky.
    missing = set(range(seqs[-1] + 1)) - set(seqs)
    assert len(missing) <= len(parts), sorted(missing)


def test_rotated_logs_older_than_keep_days_are_deleted(tmp_path):
    """Retention is by age, 14 days by default, and touches nothing but this
    soak's own logs, including a .prev.bz2 left by the old stop-and-restart
    rotation."""
    now = time.time()
    ages = {
        "t1-848-normal.log.20250101T000000Z.bz2": 15,   # this soak, too old
        "t1-848-normal.log.prev.bz2": 15,               # old scheme, too old
        "t1-848-normal.log.20261101T000000Z.bz2": 13,   # this soak, kept
        "t2-848-normal.log.20250101T000000Z.bz2": 15,   # another soak's
        "soak-metrics-848-normal-t1.jsonl": 15,         # never rotated
    }
    for name, days in ages.items():
        path = tmp_path / name
        path.write_text(name)
        os.utime(str(path), (now - days * 86400, now - days * 86400))

    result, _argv, _config = run_supervisor(tmp_path, ["client.config"])
    assert result.returncode == 2, result.stdout + result.stderr

    assert not (tmp_path / "t1-848-normal.log.20250101T000000Z.bz2").exists()
    assert not (tmp_path / "t1-848-normal.log.prev.bz2").exists()
    assert (tmp_path / "t1-848-normal.log.20261101T000000Z.bz2").exists()
    assert (tmp_path / "t2-848-normal.log.20250101T000000Z.bz2").exists()
    assert (tmp_path / "soak-metrics-848-normal-t1.jsonl").exists()


def test_a_rotated_log_left_uncompressed_is_compressed_at_startup(tmp_path):
    """A run.sh stopped while it was compressing a rotated log leaves that log
    uncompressed; the next run compresses it."""
    leftover = tmp_path / "t1-848-normal.log.20261122T000000Z"
    leftover.write_text("leftover output\n")

    run_supervisor(tmp_path, ["client.config"])

    compressed = tmp_path / "t1-848-normal.log.20261122T000000Z.bz2"
    assert wait_for(lambda: compressed.exists() and not leftover.exists())
    assert bz2.open(str(compressed)).read() == b"leftover output\n"


def test_a_compression_cut_short_leaves_no_truncated_bz2(tmp_path):
    """bzip2 killed half-way (run.sh stopped, the box rebooted) must not leave
    a truncated .bz2 under the name of a complete one. The rotated log stays,
    uncompressed, for a later attempt."""
    path = fake_tool(tmp_path, "bzip2", (
        "# Some output, then death, like a bzip2 killed half-way through.\n"
        "for arg; do file=$arg; done\n"
        'case " $* " in\n'
        '  *" -c "*) printf BZh9 ;;\n'
        '  *) printf BZh9 > "$file.bz2" ;;\n'
        "esac\n"
        "kill -KILL $$\n"))
    (tmp_path / "t1-848-normal.log").write_text("x" * 5000 + "\n")
    stub = write_stub(tmp_path, "exec sleep 600\n")
    env = supervisor_env(tmp_path, stub, {"SOAK_LOG_LIMIT_BYTES": "4096",
                                          "PATH": path})
    with supervising(tmp_path, env) as out_path:
        assert wait_for(lambda: "Could not compress" in out_path.read_text()
                        or list(tmp_path.glob("t1-848-normal.log.*.bz2"))), \
            "no compression was attempted"

    parts = rotated_parts(tmp_path)
    assert len(parts) == 1, parts
    assert wait_for(lambda: not list(tmp_path.glob("*.tmp*")), timeout=10)
    assert not list(tmp_path.glob("t1-848-normal.log.*.bz2"))
    assert (tmp_path / parts[0]).read_text() == "x" * 5000 + "\n"


def test_a_truncated_bz2_beside_its_rotated_log_is_redone_at_startup(tmp_path):
    """What a bzip2 killed half-way left when it compressed in place: the
    rotated log, and a truncated .bz2 beside it. The next run compresses the
    log again rather than trust that .bz2."""
    part = tmp_path / "t1-848-normal.log.20261122T000000Z"
    content = b"complete output\n" * 1000
    part.write_bytes(content)
    compressed = tmp_path / (part.name + ".bz2")
    compressed.write_bytes(bz2.compress(content)[:40])

    run_supervisor(tmp_path, ["client.config"])

    assert wait_for(lambda: not part.exists())
    assert bz2.open(str(compressed)).read() == content


def test_a_restart_during_a_compression_loses_nothing(tmp_path):
    """A compression goes on after its run.sh is stopped, and a run.sh started
    meanwhile deletes its temporary file as left incomplete. The old
    compression, ending first, must then leave the rotated log alone, not put
    an empty .bz2 in its place, so that the new run.sh compresses all of it."""
    path = fake_tool(tmp_path, "bzip2", (
        "# Each run.sh's compression waits for its own go-ahead.\n"
        'while [ ! -e "$BZIP2_GO" ]; do sleep 0.05; done\n'
        "exec '{}' \"$@\"\n").format(shutil.which("bzip2")))
    part = tmp_path / "t1-848-normal.log.20261122T000000Z"
    content = b"rotated output\n" * 1000
    part.write_bytes(content)
    compressed = tmp_path / (part.name + ".bz2")
    stub = write_stub(tmp_path, "exec sleep 600\n")
    go_old, go_new = tmp_path / "go-old", tmp_path / "go-new"
    env = supervisor_env(tmp_path, stub, {"PATH": path,
                                          "BZIP2_GO": str(go_old)})
    try:
        with supervising(tmp_path, env, "old.out") as old_out:
            assert wait_for(lambda: list(tmp_path.glob("*.bz2.tmp.*"))), \
                "the compression never started"
            # Restart a soak that is running, not one still starting up.
            assert wait_for(
                lambda: "Soak client started" in old_out.read_text())
        # run.sh is stopped, and its compression still waits.
        with supervising(tmp_path, dict(env, BZIP2_GO=str(go_new)),
                         "new.out") as new_out:
            assert wait_for(lambda: "left incomplete" in new_out.read_text())
            go_old.touch()
            assert wait_for(lambda: "Could not compress" in old_out.read_text()
                            or not part.exists()), \
                "the old compression never ended"
            go_new.touch()
            assert wait_for(lambda: compressed.exists() and not part.exists()
                            and not list(tmp_path.glob("*.bz2.tmp.*"))
                            or "Could not compress" in new_out.read_text())
    finally:
        # Never leave a compression waiting after the test.
        go_old.touch()
        go_new.touch()

    assert compressed.exists() and not part.exists(), \
        sorted(p.name for p in tmp_path.iterdir())
    assert compressed.stat().st_size > 0, \
        "the .bz2 is empty, and the rotated log is gone"
    assert bz2.open(str(compressed)).read() == content


def test_a_signal_just_before_the_client_starts_still_stops_it(tmp_path):
    """A signal that arrives after run.sh's loop checks `run`, but before the
    client starts, finds no client to stop. run.sh must stop the client it
    then starts, rather than wait for it to exit by itself."""
    # run.sh's first `date +%s` is in that window: hold it there while the
    # signal arrives.
    held, go = tmp_path / "date-held", tmp_path / "date-go"
    path = fake_tool(tmp_path, "date", (
        'if [ "$1" = "+%s" ] && [ ! -e "{held}" ]; then\n'
        '    : > "{held}"\n'
        '    while [ ! -e "{go}" ]; do sleep 0.05; done\n'
        "fi\n"
        "exec '{date}' \"$@\"\n").format(
            held=held, go=go, date=shutil.which("date")))
    client_pid = tmp_path / "client.pid"
    stub = write_stub(tmp_path,
                      'echo $$ > "{}"\nexec sleep 600\n'.format(client_pid))
    env = supervisor_env(tmp_path, stub, {"PATH": path})
    out_path = tmp_path / "supervisor.out"
    with open(str(out_path), "w") as out:
        proc = subprocess.Popen(["bash", RUN_SH, "client.config"],
                                cwd=str(tmp_path), env=env, stdout=out,
                                stderr=subprocess.STDOUT)
        try:
            assert wait_for(held.exists), "run.sh never got to its loop"
            proc.send_signal(signal.SIGTERM)
            go.touch()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                pytest.fail("run.sh is still waiting for a soak client it"
                            " never stopped")
        finally:
            go.touch()
            if proc.poll() is None:
                # It hung: stop the client it waits for, then run.sh itself.
                try:
                    os.kill(int(client_pid.read_text()), signal.SIGKILL)
                except (OSError, ValueError):
                    pass
                proc.kill()
                proc.wait()
    output = out_path.read_text()
    # The signal came first, so the test did hit the window.
    assert (output.index("Termination signal received")
            < output.index("Soak client started")), output
    assert "Stopping soak client" in output, output


def test_a_rotation_cut_short_duplicates_nothing(tmp_path):
    """run.sh killed after copying the log but before truncating it leaves the
    log as it was, plus a copy of it. The next run must not keep that copy as
    a rotated log, or every line in it would be kept twice."""
    copied = tmp_path / "copied"
    path = fake_tool(tmp_path, "cp", (
        "'{}' \"$@\" || exit\n"
        ": > '{}'\n"
        "exec sleep 600\n").format(shutil.which("cp"), copied))
    log = tmp_path / "t1-848-normal.log"
    lines = ["line {}".format(i) for i in range(200)]
    log.write_text("\n".join(lines) + "\n")
    stub = write_stub(tmp_path, "exec sleep 600\n")
    env = supervisor_env(tmp_path, stub, {"SOAK_LOG_LIMIT_BYTES": "1024",
                                          "PATH": path})
    # A session of its own, so one SIGKILL stops run.sh, the client and the
    # copy together, as a reboot would.
    proc = subprocess.Popen(["bash", RUN_SH, "client.config"],
                            cwd=str(tmp_path), env=env,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        assert wait_for(copied.exists), "the log was never copied"
    finally:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=60)

    # The next run, with the real cp and a limit the log is under.
    run_supervisor(tmp_path, ["client.config"])

    assert wait_for(lambda: compression_finished(tmp_path))
    kept = [line for line in log.read_text().splitlines()
            if line.startswith("line ")]
    for name in rotated_parts(tmp_path):
        kept += bz2.open(str(tmp_path / (name + ".bz2"))).read() \
            .decode().splitlines()
    assert sorted(kept) == sorted(lines)


def test_rotated_logs_beyond_keep_bytes_are_deleted_oldest_first(tmp_path):
    """Age alone does not bound the disk: a log that compresses badly or grows
    faster could fill it within SOAK_LOG_KEEP_DAYS. Once the rotated logs add
    up to SOAK_LOG_KEEP_BYTES, the oldest go first, a .prev.bz2 included."""
    now = time.time()
    names = ["t1-848-normal.log.2026112{}T000000Z.bz2".format(day)
             for day in range(5)]
    for days_old, name in zip([5, 4, 3, 2, 1], names):
        path = tmp_path / name
        path.write_bytes(b"x" * 1000)
        os.utime(str(path), (now - days_old * 86400, now - days_old * 86400))
    prev = tmp_path / "t1-848-normal.log.prev.bz2"
    prev.write_bytes(b"x" * 10)
    os.utime(str(prev), (now - 6 * 86400, now - 6 * 86400))
    other_soaks = tmp_path / "t2-848-normal.log.20261120T000000Z.bz2"
    other_soaks.write_bytes(b"x" * 1000)

    result, _argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {"SOAK_LOG_KEEP_BYTES": "2500"})
    assert result.returncode == 2, result.stdout + result.stderr

    assert [n for n in names if (tmp_path / n).exists()] == names[-2:]
    assert not prev.exists()
    assert other_soaks.exists()


def test_the_newest_rotated_log_is_kept_whatever_its_size(tmp_path):
    """The newest rotated log may not be compressed yet, and so be larger than
    SOAK_LOG_KEEP_BYTES on its own: a log that grew while the disk was full,
    say, with run.sh restarted before compressing it. It is kept and
    compressed; older ones are deleted to stay under the cap."""
    now = time.time()
    older = ["t1-848-normal.log.2026112{}T000000Z.bz2".format(day)
             for day in range(3)]
    for days_old, name in zip([3, 2, 1], older):
        path = tmp_path / name
        path.write_bytes(b"x" * 600)
        os.utime(str(path), (now - days_old * 86400, now - days_old * 86400))
    newest = tmp_path / "t1-848-normal.log.20261123T000000Z"
    content = b"the newest output\n" * 300
    newest.write_bytes(content)

    result, _argv, _config = run_supervisor(tmp_path, ["client.config"],
                                            {"SOAK_LOG_KEEP_BYTES": "1000"})
    assert result.returncode == 2, result.stdout + result.stderr

    assert wait_for(lambda: compression_finished(tmp_path))
    compressed = tmp_path / (newest.name + ".bz2")
    assert compressed.exists(), sorted(p.name for p in tmp_path.iterdir())
    assert bz2.open(str(compressed)).read() == content
    assert not (tmp_path / older[0]).exists()
    assert not (tmp_path / older[1]).exists()


@pytest.mark.parametrize("limit,keep_bytes", [
    ("52428800", "2147483648"),     # the default limit: the 2 GiB floor
    ("2147483648", "4294967296"),   # a larger limit: twice it
])
def test_keep_bytes_defaults_to_twice_the_limit_and_at_least_2_gib(
        tmp_path, limit, keep_bytes):
    result, argv, _config = run_supervisor(tmp_path, ["client.config"],
                                           {"SOAK_LOG_LIMIT_BYTES": limit})
    assert argv, result.stdout + result.stderr
    assert "/{}B ".format(keep_bytes) in result.stdout, result.stdout


def test_rotated_logs_are_compressed_one_at_a_time(tmp_path):
    """One bzip2 per rotation could pile up on a busy box, so one compression
    runs at a time, and every rotated log is still compressed."""
    running = tmp_path / "bzip2.running"
    overlaps = tmp_path / "bzip2.overlaps"
    path = fake_tool(tmp_path, "bzip2", (
        "mkdir '{running}' 2>/dev/null || echo overlap >> '{overlaps}'\n"
        "sleep 0.5\n"
        "'{bzip2}' \"$@\"\n"
        "status=$?\n"
        "rmdir '{running}' 2>/dev/null\n"
        "exit $status\n").format(running=running, overlaps=overlaps,
                                 bzip2=shutil.which("bzip2")))
    stub = write_stub(tmp_path, (
        "chunk=$(head -c 2000 /dev/zero | tr '\\0' x)\n"
        'while :; do echo "$chunk"; sleep 0.05; done\n'))
    env = supervisor_env(tmp_path, stub, {"SOAK_LOG_LIMIT_BYTES": "4096",
                                          "SOAK_ROTATE_RETRY_SECONDS": "1",
                                          "PATH": path})
    with supervising(tmp_path, env):
        assert wait_for(lambda: len(rotated_parts(tmp_path)) >= 4), \
            "the log was never rotated"

    assert wait_for(lambda: compression_finished(tmp_path), timeout=60)
    assert not overlaps.exists(), "two compressions ran at once"
    for name in rotated_parts(tmp_path):
        bz2.open(str(tmp_path / (name + ".bz2"))).read()


def test_a_rotation_that_would_fill_the_disk_waits_for_room(tmp_path):
    """The copy needs as much space again as the log, on a disk the other
    soaks share. Rather than fill it, rotation waits, warning once, until the
    copy would leave SOAK_LOG_MIN_FREE_BYTES free."""
    available_kb = tmp_path / "df-available-kb"
    available_kb.write_text("1\n")
    df_calls = tmp_path / "df-calls"
    path = fake_tool(tmp_path, "df", (
        "echo >> '{calls}'\n"
        'echo "Filesystem 1024-blocks Used Available Capacity Mounted on"\n'
        'echo "/dev/fake 100000000 99999999 $(cat \'{kb}\') 100% /"\n'
    ).format(calls=df_calls, kb=available_kb))
    log = tmp_path / "t1-848-normal.log"
    log.write_text("x" * 5000 + "\n")
    stub = write_stub(tmp_path, "exec sleep 600\n")
    env = supervisor_env(tmp_path, stub, {
        "SOAK_LOG_LIMIT_BYTES": "4096", "SOAK_LOG_MIN_FREE_BYTES": "1048576",
        "SOAK_ROTATE_RETRY_SECONDS": "1", "PATH": path})

    with supervising(tmp_path, env) as out_path:
        assert wait_for(lambda: df_calls.exists()
                        and len(df_calls.read_text()) >= 3), \
            "rotation never checked the free space, or never tried again"
        assert out_path.read_text().count("Log rotation postponed") == 1
        assert rotated_parts(tmp_path) == []
        assert log.read_text() == "x" * 5000 + "\n"

        available_kb.write_text("100000000\n")
        assert wait_for(lambda: rotated_parts(tmp_path)), \
            "the log was not rotated once there was room"

    output = out_path.read_text()
    assert output.count("Soak client started") == 1, output
    assert output.count("kept running") == 1, output


def test_a_client_killed_by_sigkill_points_at_the_oom_killer(tmp_path):
    """A SIGKILLed child cannot log why it died. run.sh never sends SIGKILL
    while the soak runs, so it names the usual sender."""
    stub = write_stub(tmp_path, "kill -KILL $$\n")
    env = supervisor_env(tmp_path, stub, {"SOAK_MAX_RAPID_FAILURES": "1",
                                          "SOAK_RESTART_DELAY": "0"})
    result = subprocess.run(["bash", RUN_SH, "client.config"],
                            cwd=str(tmp_path), env=env, capture_output=True,
                            text=True, timeout=60)
    output = result.stdout + result.stderr
    assert "exited with status 137" in output, output
    assert "SIGKILL" in output
    assert "OOM killer" in output


@pytest.mark.parametrize("var,value", [
    ("SOAK_LOG_KEEP_DAYS", "a month"),
    ("SOAK_LOG_KEEP_DAYS", "08"),        # bash arithmetic fails on it...
    ("SOAK_LOG_KEEP_DAYS", "010"),       # ...or reads it as octal 8
    ("SOAK_LOG_LIMIT_BYTES", "1G"),      # the log would never rotate
    ("SOAK_LOG_LIMIT_BYTES", "0"),       # it would rotate on every poll
    ("SOAK_LOG_KEEP_BYTES", "-1"),
    ("SOAK_LOG_MIN_FREE_BYTES", "1.5"),
])
def test_log_settings_must_be_plain_whole_numbers(tmp_path, var, value):
    result, argv, _config = run_supervisor(tmp_path, ["client.config"],
                                           {var: value})
    assert result.returncode == 2, result.stdout + result.stderr
    assert var in result.stderr
    assert argv == []
