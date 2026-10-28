# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""VerifiableProducer. Java has no test of its own; these are the Rust tool's
(rust/tools/verifiable-clients/src/verifiable_producer.rs) over MockProducer,
plus the script run end to end against an unreachable broker."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from typing import Any

import pytest

from _common import ArgumentParserError
from throughput_throttler import ThroughputThrottler
from verifiable_producer import (VerifiableProducer, arg_parser, create_from_args, failed_send,
                                 parse_args, shutdown_complete, startup_complete, successful_send,
                                 tool_data)

from confluent_kafka.common import KafkaError, TopicPartition
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.serialization import string_serializer
from confluent_kafka.producer import MockProducer, RecordMetadata

TOOLS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _mock_producer(auto_complete: bool = True) -> MockProducer[str, str]:
    return MockProducer(auto_complete=auto_complete, partitioner=None,
                        key_serializer=string_serializer(), value_serializer=string_serializer())


def _verifiable(producer: MockProducer[str, str] | None = None, *, throughput: int = -1,
                max_messages: int = -1, value_prefix: int | None = None,
                create_time: int | None = None,
                repeating_keys: int | None = None) -> VerifiableProducer:
    return VerifiableProducer(producer or _mock_producer(), "t", throughput, max_messages,
                              value_prefix, create_time, repeating_keys)


def _lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def _without_timestamp(data: dict[str, Any]) -> dict[str, Any]:
    assert isinstance(data.pop("timestamp"), int)
    return data


# ---- values and keys --------------------------------------------------------

def test_get_value_without_prefix_is_just_the_number() -> None:
    producer = _verifiable()
    assert producer.get_value(0) == "0"
    assert producer.get_value(42) == "42"


def test_get_value_with_prefix_uses_dot_separator() -> None:
    producer = _verifiable(value_prefix=7)
    assert producer.get_value(0) == "7.0"
    assert producer.get_value(42) == "7.42"


def test_get_key_is_none_without_repeating_keys() -> None:
    producer = _verifiable()
    assert producer.get_key() is None
    assert producer.get_key() is None


def test_get_key_wraps_around_at_repeating_keys() -> None:
    producer = _verifiable(repeating_keys=3)
    assert [producer.get_key() for _ in range(5)] == ["0", "1", "2", "0", "1"]


# ---- send -------------------------------------------------------------------

def test_send_increments_sent_and_acked_via_callback(capsys: pytest.CaptureFixture[str]) -> None:
    producer = _verifiable()
    assert (producer.num_sent, producer.num_acked) == (0, 0)
    producer.send("k", "0")
    assert (producer.num_sent, producer.num_acked) == (1, 1)
    producer.send(None, "1")
    assert (producer.num_sent, producer.num_acked) == (2, 2)
    assert [_without_timestamp(line) for line in _lines(capsys)] == [
        {"name": "producer_send_success", "key": "k", "value": "0", "topic": "t",
         "partition": 0, "offset": 0},
        {"name": "producer_send_success", "key": None, "value": "1", "topic": "t",
         "partition": 0, "offset": 1},
    ]


def test_send_failure_from_the_callback_is_a_failed_send(
        capsys: pytest.CaptureFixture[str]) -> None:
    mock = _mock_producer(auto_complete=False)
    producer = _verifiable(mock)
    producer.send("k", "0")
    assert mock.error_next(e=KafkaTimeoutError(message="Expiring 1 record(s)"))
    assert (producer.num_sent, producer.num_acked) == (1, 0)
    assert [_without_timestamp(line) for line in _lines(capsys)] == [
        {"name": "producer_send_error", "key": "k", "value": "0", "topic": "t",
         "exception": "class confluent_kafka.common.errors.TimeoutError",
         "message": "Expiring 1 record(s)"},
    ]


def test_send_failure_raised_by_send_is_a_failed_send(capsys: pytest.CaptureFixture[str]) -> None:
    mock = _mock_producer()
    mock.set_send_exception(send_exception=KafkaError(message="send failed"))
    producer = _verifiable(mock)
    producer.send(None, "0")
    # Java counts the attempt before send() throws.
    assert (producer.num_sent, producer.num_acked) == (1, 0)
    assert [_without_timestamp(line) for line in _lines(capsys)] == [
        {"name": "producer_send_error", "key": None, "value": "0", "topic": "t",
         "exception": "class confluent_kafka.common.KafkaError", "message": "send failed"},
    ]


def test_create_time_is_the_record_timestamp_and_advances(
        capsys: pytest.CaptureFixture[str]) -> None:
    mock = _mock_producer()
    producer = _verifiable(mock, create_time=1_000)
    producer.start_time -= 5  # five milliseconds since the producer started
    producer.send(None, "0")
    producer.send(None, "1")
    first, second = [record.timestamp() for record in mock.history()]
    assert first == 1_000
    assert second is not None and second >= 1_005
    assert producer.create_time is not None and producer.create_time >= second + 5


def test_record_without_create_time_has_no_timestamp() -> None:
    mock = _mock_producer()
    _verifiable(mock).send("k", "0")
    record = mock.history()[0]
    assert (record.topic(), record.partition(), record.timestamp()) == ("t", None, None)
    assert (record.key(), record.value()) == ("k", "0")


def test_negative_create_time_is_a_failed_send_not_counted(
        capsys: pytest.CaptureFixture[str]) -> None:
    mock = _mock_producer()
    producer = _verifiable(mock, create_time=-5)
    producer.send(None, "0")
    assert (producer.num_sent, mock.history()) == (0, [])
    assert [_without_timestamp(line) for line in _lines(capsys)] == [
        {"name": "producer_send_error", "key": None, "value": "0", "topic": "t",
         "exception": "class confluent_kafka.IllegalArgumentError",
         "message": "Invalid timestamp: -5. Timestamp should always be non-negative or None."},
    ]


# ---- run and close ----------------------------------------------------------

def test_run_produces_max_messages(capsys: pytest.CaptureFixture[str]) -> None:
    mock = _mock_producer()
    producer = _verifiable(mock, max_messages=3)
    producer.run(ThroughputThrottler(-1, 0))
    assert [record.value() for record in mock.history()] == ["0", "1", "2"]
    lines = _lines(capsys)
    assert [line["name"] for line in lines] == ["startup_complete"] + ["producer_send_success"] * 3
    assert (producer.num_sent, producer.num_acked) == (3, 3)


def test_run_values_carry_the_prefix_and_keys_repeat() -> None:
    mock = _mock_producer()
    producer = _verifiable(mock, max_messages=3, value_prefix=2, repeating_keys=2)
    producer.run(ThroughputThrottler(-1, 0))
    assert [(r.key(), r.value()) for r in mock.history()] == [
        ("0", "2.0"), ("1", "2.1"), ("0", "2.2")]


def test_run_stops_when_stop_producing_is_set(capsys: pytest.CaptureFixture[str]) -> None:
    mock = _mock_producer()
    producer = _verifiable(mock)
    producer.stop_producing.set()
    producer.run(ThroughputThrottler(-1, 0))
    assert mock.history() == []
    assert [line["name"] for line in _lines(capsys)] == ["startup_complete"]


def test_close_closes_the_producer_and_prints_shutdown_complete(
        capsys: pytest.CaptureFixture[str]) -> None:
    mock = _mock_producer()
    _verifiable(mock).close()
    assert mock.closed()
    assert [_without_timestamp(line) for line in _lines(capsys)] == [{"name": "shutdown_complete"}]


# ---- the JSON events --------------------------------------------------------

def test_lifecycle_events_put_timestamp_then_name_first() -> None:
    assert list(startup_complete()) == ["timestamp", "name"]
    assert startup_complete()["name"] == "startup_complete"
    assert list(shutdown_complete()) == ["timestamp", "name"]
    assert shutdown_complete()["name"] == "shutdown_complete"


def test_successful_send_fields_in_java_order() -> None:
    metadata = RecordMetadata(topic_partition=TopicPartition(topic="t", partition=2),
                              base_offset=40, batch_index=2, timestamp=-1,
                              serialized_key_size=1, serialized_value_size=1)
    data = successful_send("k", "v", metadata)
    assert list(data) == ["timestamp", "name", "key", "value", "topic", "partition", "offset"]
    assert _without_timestamp(data) == {"name": "producer_send_success", "key": "k",
                                        "value": "v", "topic": "t", "partition": 2, "offset": 42}


def test_failed_send_fields_in_java_order() -> None:
    data = failed_send(None, "v", "t", KafkaError(message="boom"))
    assert list(data) == ["timestamp", "name", "key", "value", "topic", "exception", "message"]
    assert _without_timestamp(data) == {
        "name": "producer_send_error", "key": None, "value": "v", "topic": "t",
        "exception": "class confluent_kafka.common.KafkaError", "message": "boom"}


def test_failed_send_with_no_message_is_null() -> None:
    # Java's getMessage() is null for KafkaException(); Jackson writes null.
    assert failed_send("k", "v", "t", KafkaError())["message"] is None


def test_tool_data_fields_in_java_order() -> None:
    data = tool_data(10, 9, 100, 4.5)
    assert list(data) == ["timestamp", "name", "sent", "acked", "target_throughput",
                          "avg_throughput"]
    assert _without_timestamp(data) == {"name": "tool_data", "sent": 10, "acked": 9,
                                        "target_throughput": 100, "avg_throughput": 4.5}


# ---- arguments --------------------------------------------------------------

def _parse(*args: str) -> tuple[Any, dict[str, Any]]:
    return parse_args(arg_parser(), list(args))


def _parse_error(*args: str) -> str:
    with pytest.raises(ArgumentParserError) as error:
        _parse(*args)
    return str(error.value)


def test_parse_args_minimal() -> None:
    res, props = _parse("--topic", "t", "--bootstrap-server", "b:9092")
    assert (res.topic, res.bootstrap_server, res.max_messages, res.throughput, res.acks) == (
        "t", "b:9092", -1, -1, -1)
    assert (res.create_time, res.value_prefix, res.repeating_keys) == (-1, None, None)
    assert props == {"bootstrap.servers": "b:9092", "acks": "-1", "retries": "0"}


def test_parse_args_every_option() -> None:
    res, props = _parse("--topic", "t", "--bootstrap-server", "b", "--max-messages", "5",
                        "--throughput", "100", "--acks", "1", "--message-create-time", "1234",
                        "--value-prefix", "3", "--repeating-keys", "4")
    assert (res.max_messages, res.throughput, res.create_time, res.value_prefix,
            res.repeating_keys) == (5, 100, 1234, 3, 4)
    assert props["acks"] == "1"


def test_parse_args_valid_acks_choices() -> None:
    for acks in ("0", "1", "-1"):
        res, props = _parse("--topic", "t", "--bootstrap-server", "b", "--acks", acks)
        assert (res.acks, props["acks"]) == (int(acks), acks)


def test_parse_args_invalid_acks_choice() -> None:
    message = _parse_error("--topic", "t", "--bootstrap-server", "b", "--acks", "5")
    # Python 3.12 started quoting the choices: "'5' (choose from '0', '1', '-1')"
    # there, "5 (choose from 0, 1, -1)" on 3.10.
    assert message in ("argument --acks: invalid choice: 5 (choose from 0, 1, -1)",
                       "argument --acks: invalid choice: '5' (choose from '0', '1', '-1')")


def test_parse_args_missing_topic() -> None:
    assert _parse_error("--bootstrap-server", "b") == (
        "the following arguments are required: --topic")


def test_parse_args_missing_bootstrap_server() -> None:
    assert _parse_error("--topic", "t") == "one of the arguments --bootstrap-server is required"


def test_parse_args_missing_value() -> None:
    assert _parse_error("--topic") == "argument --topic: expected one argument"


def test_parse_args_not_an_integer() -> None:
    assert _parse_error("--topic", "t", "--bootstrap-server", "b", "--max-messages", "x") == (
        "argument --max-messages: invalid int value: 'x'")


def test_parse_args_unrecognized_argument() -> None:
    assert _parse_error("--topic", "t", "--bootstrap-server", "b", "--bogus") == (
        "unrecognized arguments: --bogus")


def test_parse_args_config_files_are_mutually_exclusive(tmp_path: Any) -> None:
    config = tmp_path / "p.properties"
    config.write_text("")
    assert _parse_error("--topic", "t", "--bootstrap-server", "b", "--producer.config",
                        str(config), "--command-config", str(config)) == (
        "Options --producer.config and --command-config are mutually exclusive.")


def test_command_config_overrides_the_command_line(tmp_path: Any) -> None:
    config = tmp_path / "p.properties"
    config.write_text("# written by the system test\nacks=1\nretries = 5\n"
                      "linger.ms: 10\nclient.id   tool\n")
    _, props = _parse("--topic", "t", "--bootstrap-server", "b", "--command-config", str(config))
    assert props == {"bootstrap.servers": "b", "acks": "1", "retries": "5", "linger.ms": "10",
                     "client.id": "tool"}


def test_deprecated_producer_config_still_loads(tmp_path: Any,
                                                capsys: pytest.CaptureFixture[str]) -> None:
    config = tmp_path / "p.properties"
    config.write_text("linger.ms=10\n")
    _, props = _parse("--topic", "t", "--bootstrap-server", "b", "--producer.config", str(config))
    assert props["linger.ms"] == "10"
    assert capsys.readouterr().out == (
        "Option --producer.config has been deprecated and will be removed in a future version. "
        "Use --command-config instead.\n")


def test_unreadable_config_file_is_an_argument_error(tmp_path: Any) -> None:
    missing = str(tmp_path / "missing.properties")
    assert missing in _parse_error("--topic", "t", "--bootstrap-server", "b",
                                   "--command-config", missing)


def test_create_from_args_builds_a_kafka_producer() -> None:
    # The configuration the tool builds is one the client accepts.
    producer = create_from_args(arg_parser(), ["--topic", "t", "--bootstrap-server",
                                               "localhost:1", "--message-create-time", "-1",
                                               "--throughput", "7"])
    try:
        assert (producer.topic, producer.throughput, producer.create_time) == ("t", 7, None)
    finally:
        producer.producer.close()


# ---- the script -------------------------------------------------------------

def _run_script(*args: str, timeout: float = 60, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, os.path.join(TOOLS_DIR, "verifiable_producer.py"),
                           *args], capture_output=True, text=True, timeout=timeout,
                          env=_script_env(), **kwargs)


def _script_env() -> dict[str, str]:
    # The confluent_kafka package and its extension, from wherever this test
    # imports them.
    import confluent_kafka

    package_root = os.path.dirname(os.path.dirname(confluent_kafka.__file__))
    # Without colour: from Python 3.14 argparse colours its help when
    # FORCE_COLOR is set, as many CI environments set it.
    env = {key: value for key, value in os.environ.items() if key != "FORCE_COLOR"}
    return {**env, "PYTHON_COLORS": "0", "NO_COLOR": "1",
            "PYTHONPATH": os.pathsep.join(filter(None, [package_root, env.get("PYTHONPATH")]))}


def test_script_without_arguments_prints_help() -> None:
    result = _run_script()
    assert result.returncode == 0
    assert result.stdout.startswith("usage: verifiable-producer")


def test_script_with_bad_arguments_exits_1() -> None:
    result = _run_script("--topic", "t")
    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.endswith(
        "verifiable-producer: error: one of the arguments --bootstrap-server is required\n")


def test_script_reports_unacked_sends_then_its_summary(tmp_path: Any) -> None:
    # No broker: each send fails after max.block.ms.
    config = tmp_path / "p.properties"
    config.write_text("max.block.ms=200\n")
    result = _run_script("--topic", "t", "--bootstrap-server", "localhost:1", "--max-messages",
                         "2", "--command-config", str(config))
    assert result.returncode == 0, result.stderr
    lines = [json.loads(line) for line in result.stdout.splitlines()]
    assert [line["name"] for line in lines] == [
        "startup_complete", "producer_send_error", "producer_send_error", "shutdown_complete",
        "tool_data"]
    assert [line["value"] for line in lines[1:3]] == ["0", "1"]
    assert {key: lines[-1][key] for key in ("sent", "acked", "target_throughput")} == {
        "sent": 2, "acked": 0, "target_throughput": -1}


def test_script_shuts_down_cleanly_on_sigterm(tmp_path: Any) -> None:
    # --throughput 0 parks the loop in throttle() after the second send, until
    # the shutdown hook wakes it. It parks only after the second send because
    # the first send blocks for max.block.ms, so the throttler then sees time
    # has passed (should_throttle(1, ...)); the blocked-send case is the next
    # test.
    config = tmp_path / "p.properties"
    config.write_text("max.block.ms=200\n")
    process = subprocess.Popen(
        [sys.executable, os.path.join(TOOLS_DIR, "verifiable_producer.py"), "--topic", "t",
         "--bootstrap-server", "localhost:1", "--throughput", "0", "--command-config",
         str(config)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env=_script_env())
    try:
        assert process.stdout is not None
        names: list[str] = []
        while len(names) < 3:
            names.append(json.loads(process.stdout.readline())["name"])
        assert names == ["startup_complete", "producer_send_error", "producer_send_error"]
        process.send_signal(signal.SIGTERM)
        out, err = process.communicate(timeout=60)
    finally:
        process.kill()
    assert process.returncode == 0, err
    lines = [json.loads(line) for line in out.splitlines()]
    assert [line["name"] for line in lines] == ["shutdown_complete", "tool_data"]
    assert (lines[1]["sent"], lines[1]["acked"], lines[1]["target_throughput"]) == (2, 0, 0)


def test_script_sigterm_releases_a_blocked_send(tmp_path: Any) -> None:
    # The first send waits for metadata that never comes, for up to
    # max.block.ms. As Java's shutdown hook, the tool's hook closes the
    # producer itself, which fails that send at once instead of letting it
    # hold the shutdown until it times out.
    config = tmp_path / "p.properties"
    config.write_text("max.block.ms=60000\n")
    process = subprocess.Popen(
        [sys.executable, os.path.join(TOOLS_DIR, "verifiable_producer.py"), "--topic", "t",
         "--bootstrap-server", "localhost:1", "--command-config", str(config)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=_script_env())
    try:
        assert process.stdout is not None
        assert json.loads(process.stdout.readline())["name"] == "startup_complete"
        time.sleep(0.5)
        signalled = time.monotonic()
        process.send_signal(signal.SIGTERM)
        out, err = process.communicate(timeout=60)
        shutdown_seconds = time.monotonic() - signalled
    finally:
        process.kill()
    assert process.returncode == 0, err
    assert shutdown_seconds < 20, err
    lines = [json.loads(line) for line in out.splitlines()]
    names = [line["name"] for line in lines]
    # The failed send is printed by the main thread while the hook closes, so
    # it may come before or after the hook's lines.
    assert sorted(names) == ["producer_send_error", "shutdown_complete", "tool_data"], names
    assert names.index("shutdown_complete") < names.index("tool_data")
    error = lines[names.index("producer_send_error")]
    assert (error["value"], error["message"]) == ("0", "Producer closed while send in progress")
    summary = lines[names.index("tool_data")]
    assert (summary["sent"], summary["acked"]) == (1, 0)
