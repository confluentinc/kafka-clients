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

"""Tests for run.sh: HI mode, override precedence and argument handling.

These drive the real run.sh with a stub interpreter in place of Python, so what
is asserted is the arguments and the client config the supervisor *would* hand
the soak client — no broker, no bindings, no OpenTelemetry. The stub exits 2
(the "fatal, never restart" code), which makes run.sh stop after exactly one
child instead of looping.

They need only bash, and run on macOS and Linux alike.
"""

import os
import shutil
import subprocess

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
        '  File "/w/bindings/python/soak/soakclient.py", line 59, in <module>\n'
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
    assert not (tmp_path / "t1-848-normal.log.prev.bz2").exists()
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
    here would bzip2 that crash's own few-KB fragment over the single
    .prev.bz2 and delete the log -- destroying the one piece of evidence an
    operator needs, which is exactly what run.sh's own comment on
    maybe_rotate_log says must never happen.
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
    assert not (tmp_path / "t1-848-normal.log.prev.bz2").exists()
    assert "x" * 900 in logfile.read_text()
