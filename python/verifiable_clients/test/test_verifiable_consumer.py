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

"""VerifiableConsumer. Java has no test of its own; these are the Rust tool's
(rust/tools/verifiable-clients/src/verifiable_consumer.rs), over MockConsumer
where they need a consumer, plus the script run end to end against an
unreachable broker."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Mapping
from typing import Any

import pytest

import verifiable_consumer
from _common import ArgumentParserError
from verifiable_consumer import (VerifiableConsumer, arg_parser, create_from_args,
                                 group_protocol_of, offsets_committed, partitions_assigned,
                                 partitions_revoked, record_data, records_consumed,
                                 shutdown_complete, shutdown_requested, startup_complete)

from confluent_kafka import IllegalArgumentError, UnsupportedOperationError
from confluent_kafka.common import KafkaError, TopicPartition
from confluent_kafka.common.errors import FencedInstanceIdError, WakeupError
from confluent_kafka.consumer import (CloseOptions, ConsumerRecord, ConsumerRecords,
                                      MockConsumer, OffsetAndMetadata)

TOOLS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TP0 = TopicPartition(topic="t", partition=0)
TP1 = TopicPartition(topic="t", partition=1)


@pytest.fixture(autouse=True)
def _no_startup_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(verifiable_consumer, "MIN_STARTUP_VISIBLE_SECONDS", 0.0)


def _verifiable(consumer: Any = None, *, max_messages: int = -1, verbose: bool = False,
                use_auto_commit: bool = False, close_timeout_ms: int = 30000) -> VerifiableConsumer:
    return VerifiableConsumer(consumer or MockConsumer(offset_reset_strategy="earliest"), "t",
                              max_messages, use_auto_commit, False, verbose, close_timeout_ms)


def _record(tp: TopicPartition, offset: int, key: str | None = None,
            value: str | None = "v") -> ConsumerRecord[str, str]:
    return ConsumerRecord(topic=tp.topic(), partition=tp.partition(), offset=offset, key=key,
                          value=value)


def _records(by_partition: Mapping[TopicPartition, list[int]]) -> ConsumerRecords[str, str]:
    return ConsumerRecords(
        records={tp: [_record(tp, offset) for offset in offsets]
                 for tp, offsets in by_partition.items()},
        next_offsets={})


def _lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def _without_timestamp(data: dict[str, Any]) -> dict[str, Any]:
    assert isinstance(data.pop("timestamp"), int)
    return data


# ---- the message limit and the records received ------------------------------

def test_has_message_limit_false_for_negative_max() -> None:
    consumer = _verifiable(max_messages=-1)
    assert not consumer.has_message_limit()
    assert not consumer.is_finished()


def test_has_message_limit_true_for_zero_or_positive_max() -> None:
    assert _verifiable(max_messages=0).has_message_limit()
    assert _verifiable(max_messages=5).has_message_limit()
    assert _verifiable(max_messages=0).is_finished()
    assert not _verifiable(max_messages=5).is_finished()


def test_on_records_received_commits_max_offset_plus_one(
        capsys: pytest.CaptureFixture[str]) -> None:
    consumer = _verifiable()
    offsets = consumer.on_records_received(_records({TP0: [10, 11, 12]}))
    assert offsets == {TP0: OffsetAndMetadata(offset=13)}
    assert consumer.consumed_messages == 3
    assert [_without_timestamp(line) for line in _lines(capsys)] == [
        {"name": "records_consumed", "count": 3, "partitions": [
            {"topic": "t", "partition": 0, "count": 3, "minOffset": 10, "maxOffset": 12}]}]


def test_on_records_received_truncates_at_max_messages(
        capsys: pytest.CaptureFixture[str]) -> None:
    consumer = _verifiable(max_messages=2)
    offsets = consumer.on_records_received(_records({TP0: [100, 101, 102, 103, 104]}))
    assert offsets == {TP0: OffsetAndMetadata(offset=102)}
    assert consumer.consumed_messages == 2
    assert consumer.is_finished()
    # The count is the whole poll's; the summary is the records kept.
    assert [_without_timestamp(line) for line in _lines(capsys)] == [
        {"name": "records_consumed", "count": 5, "partitions": [
            {"topic": "t", "partition": 0, "count": 2, "minOffset": 100, "maxOffset": 101}]}]


def test_on_records_received_stops_at_the_partition_that_finishes(
        capsys: pytest.CaptureFixture[str]) -> None:
    consumer = _verifiable(max_messages=1)
    offsets = consumer.on_records_received(_records({TP0: [5], TP1: [7]}))
    assert len(offsets) == 1
    assert consumer.consumed_messages == 1
    assert consumer.is_finished()
    (line,) = _lines(capsys)
    assert line["count"] == 2
    assert len(line["partitions"]) == 1


def test_on_records_received_after_the_limit_keeps_nothing(
        capsys: pytest.CaptureFixture[str]) -> None:
    consumer = _verifiable(max_messages=1)
    consumer.consumed_messages = 1
    assert consumer.on_records_received(_records({TP0: [1, 2]})) == {}
    (line,) = _lines(capsys)
    assert (line["count"], line["partitions"]) == (2, [])


def test_verbose_prints_each_record_kept(capsys: pytest.CaptureFixture[str]) -> None:
    consumer = _verifiable(max_messages=1, verbose=True)
    consumer.on_records_received(ConsumerRecords(
        records={TP0: [_record(TP0, 3, key="k", value="3"), _record(TP0, 4)]}, next_offsets={}))
    lines = [_without_timestamp(line) for line in _lines(capsys)]
    assert lines[0] == {"name": "record_data", "key": "k", "value": "3", "topic": "t",
                        "partition": 0, "offset": 3}
    assert [line["name"] for line in lines] == ["record_data", "records_consumed"]


# ---- the JSON events --------------------------------------------------------

def test_lifecycle_events_put_timestamp_then_name_first() -> None:
    for build, name in ((startup_complete, "startup_complete"),
                        (shutdown_requested, "shutdown_requested"),
                        (shutdown_complete, "shutdown_complete")):
        data = build()
        assert list(data) == ["timestamp", "name"]
        assert data["name"] == name


def test_partition_events_write_topic_partitions_as_objects() -> None:
    assert _without_timestamp(partitions_assigned([TP1])) == {
        "name": "partitions_assigned", "partitions": [{"topic": "t", "partition": 1}]}
    assert _without_timestamp(partitions_revoked([TP0])) == {
        "name": "partitions_revoked", "partitions": [{"topic": "t", "partition": 0}]}


def test_records_consumed_fields_in_java_order() -> None:
    data = records_consumed(3, [])
    assert list(data) == ["timestamp", "name", "count", "partitions"]


def test_record_data_fields_in_java_order() -> None:
    data = record_data(_record(TP1, 9, key=None, value="x"))
    assert list(data) == ["timestamp", "name", "key", "value", "topic", "partition", "offset"]
    assert _without_timestamp(data) == {"name": "record_data", "key": None, "value": "x",
                                        "topic": "t", "partition": 1, "offset": 9}


def test_offsets_committed_success_omits_error() -> None:
    data = offsets_committed({TP0: OffsetAndMetadata(offset=13)}, None, True)
    assert list(data) == ["timestamp", "name", "offsets", "success"]
    assert _without_timestamp(data) == {
        "name": "offsets_committed",
        "offsets": [{"topic": "t", "partition": 0, "offset": 13}], "success": True}


def test_offsets_committed_failure_includes_error() -> None:
    data = offsets_committed({TP0: OffsetAndMetadata(offset=13)}, "boom", False)
    assert list(data) == ["timestamp", "name", "offsets", "error", "success"]
    assert (data["error"], data["success"]) == ("boom", False)


def test_on_complete_reports_the_error_message(capsys: pytest.CaptureFixture[str]) -> None:
    consumer = _verifiable()
    consumer.on_complete(offsets={TP0: OffsetAndMetadata(offset=1)},
                         exception=KafkaError(message="boom"))
    consumer.on_complete(offsets={TP0: OffsetAndMetadata(offset=1)}, exception=KafkaError())
    consumer.on_complete(offsets=None, exception=KafkaError(message="async"))
    lines = [_without_timestamp(line) for line in _lines(capsys)]
    assert lines[0] == {"name": "offsets_committed",
                        "offsets": [{"topic": "t", "partition": 0, "offset": 1}],
                        "error": "boom", "success": False}
    # Java's null message is left out.
    assert "error" not in lines[1] and lines[1]["success"] is False
    assert lines[2]["offsets"] == []


def test_listener_methods_print_the_rebalance(capsys: pytest.CaptureFixture[str]) -> None:
    consumer = _verifiable()
    consumer.on_partitions_assigned({TP0})
    consumer.on_partitions_revoked({TP0})
    # Not overridden: the default reports lost partitions as revoked.
    consumer.on_partitions_lost({TP1})
    assert [(line["name"], line["partitions"]) for line in _lines(capsys)] == [
        ("partitions_assigned", [{"topic": "t", "partition": 0}]),
        ("partitions_revoked", [{"topic": "t", "partition": 0}]),
        ("partitions_revoked", [{"topic": "t", "partition": 1}]),
    ]


# ---- commit_sync ------------------------------------------------------------

class _CommitScript:
    """A consumer whose commit() raises the errors given, one per call, then
    succeeds; only commit() is used by _commit_sync."""

    def __init__(self, *errors: BaseException) -> None:
        self.errors = list(errors)
        self.commits: list[dict[TopicPartition, OffsetAndMetadata]] = []

    def commit(self, *, offsets: dict[TopicPartition, OffsetAndMetadata]) -> None:
        self.commits.append(dict(offsets))
        if self.errors:
            raise self.errors.pop(0)


OFFSETS = {TP0: OffsetAndMetadata(offset=13)}


def test_commit_sync_reports_success(capsys: pytest.CaptureFixture[str]) -> None:
    script = _CommitScript()
    _verifiable(script)._commit_sync(OFFSETS)
    assert script.commits == [OFFSETS]
    assert [line["success"] for line in _lines(capsys)] == [True]


def test_commit_sync_retries_after_wakeup_then_raises_it(
        capsys: pytest.CaptureFixture[str]) -> None:
    wakeup = WakeupError()
    script = _CommitScript(wakeup)
    with pytest.raises(WakeupError) as raised:
        _verifiable(script)._commit_sync(OFFSETS)
    assert raised.value is wakeup
    assert script.commits == [OFFSETS, OFFSETS]
    assert [line["success"] for line in _lines(capsys)] == [True]


def test_commit_sync_raises_fenced_instance_id(capsys: pytest.CaptureFixture[str]) -> None:
    script = _CommitScript(FencedInstanceIdError(message="fenced"))
    with pytest.raises(FencedInstanceIdError):
        _verifiable(script)._commit_sync(OFFSETS)
    assert _lines(capsys) == []


def test_commit_sync_reports_any_other_error(capsys: pytest.CaptureFixture[str]) -> None:
    script = _CommitScript(KafkaError(message="commit failed"))
    _verifiable(script)._commit_sync(OFFSETS)
    (line,) = _lines(capsys)
    assert (line["success"], line["error"]) == (False, "commit failed")


# ---- run over MockConsumer --------------------------------------------------

def _assign_and_add(mock: MockConsumer[str, str], offsets: list[int]) -> None:
    mock.rebalance(new_assignment=[TP0])
    mock.update_beginning_offsets(new_offsets={TP0: 0})
    for offset in offsets:
        mock.add_record(record=_record(TP0, offset, value=str(offset)))


class _StateAtClose(MockConsumer[str, str]):
    """A MockConsumer that keeps what was committed and subscribed when it was
    closed, since neither can be read afterwards."""

    def close(self, **kwargs: Any) -> None:
        self.committed_at_close = self.committed(partitions=[TP0])
        self.subscription_at_close = self.subscription()
        super().close(**kwargs)


def test_run_consumes_commits_and_shuts_down(capsys: pytest.CaptureFixture[str]) -> None:
    mock = _StateAtClose(offset_reset_strategy="earliest")
    mock.schedule_poll_task(task=lambda: _assign_and_add(mock, [0, 1, 2]))
    consumer = _verifiable(mock, max_messages=3)
    consumer.run()
    lines = [_without_timestamp(line) for line in _lines(capsys)]
    assert [line["name"] for line in lines] == [
        "startup_complete", "partitions_assigned", "records_consumed", "offsets_committed",
        "shutdown_complete"]
    assert lines[3] == {"name": "offsets_committed",
                        "offsets": [{"topic": "t", "partition": 0, "offset": 3}],
                        "success": True}
    assert mock.committed_at_close == {TP0: OffsetAndMetadata(offset=3)}
    assert mock.subscription_at_close == {"t"}
    assert mock.closed()


def test_run_with_auto_commit_does_not_commit(capsys: pytest.CaptureFixture[str]) -> None:
    mock: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    mock.schedule_poll_task(task=lambda: _assign_and_add(mock, [0]))
    _verifiable(mock, max_messages=1, use_auto_commit=True).run()
    # The whole sequence, so the test cannot pass with a loop that never ran.
    assert [line["name"] for line in _lines(capsys)] == [
        "startup_complete", "partitions_assigned", "records_consumed", "shutdown_complete"]


class _WakeupCounter(MockConsumer[str, str]):
    def wakeup(self) -> None:
        self.wakeups = getattr(self, "wakeups", 0) + 1
        super().wakeup()


def test_close_wakes_the_consumer_even_if_printing_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # The main thread waits in poll() with no time limit, so the hook must
    # wake it whatever happens to its own output.
    def failing_print(data: Any) -> None:
        raise RuntimeError("stdout is gone")

    mock = _WakeupCounter(offset_reset_strategy="earliest")
    monkeypatch.setattr(verifiable_consumer, "print_json", failing_print)
    with pytest.raises(RuntimeError):
        _verifiable(mock).close()
    assert mock.wakeups == 1


def test_run_ends_quietly_on_wakeup(capsys: pytest.CaptureFixture[str],
                                    caplog: pytest.LogCaptureFixture) -> None:
    mock: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    mock.schedule_poll_task(task=mock.wakeup)
    _verifiable(mock).run()
    assert [line["name"] for line in _lines(capsys)] == ["startup_complete", "shutdown_complete"]
    assert not [r for r in caplog.records if r.levelname == "ERROR"]
    assert mock.closed()


def test_run_logs_any_other_error_and_still_shuts_down(
        capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture) -> None:
    mock: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    mock.set_poll_exception(exception=KafkaError(message="poll failed"))
    _verifiable(mock).run()
    assert [line["name"] for line in _lines(capsys)] == ["startup_complete", "shutdown_complete"]
    (record,) = [r for r in caplog.records if r.levelname == "ERROR"]
    assert record.getMessage() == "Error during processing, terminating consumer process: "
    assert str(record.exc_info[1]) == "poll failed"  # type: ignore[index]
    assert mock.closed()


class _FakeClock:
    """The ``time`` functions ``run()`` uses, on a scripted monotonic clock:
    comparing the events' wall-clock timestamps instead would race the tool's
    timer, which starts just before ``startup_complete`` is stamped."""

    def __init__(self, *readings: float) -> None:
        self._readings = list(readings)
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self._readings.pop(0)

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)


def test_run_keeps_startup_visible_for_the_minimum(monkeypatch: pytest.MonkeyPatch,
                                                   capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(verifiable_consumer, "MIN_STARTUP_VISIBLE_SECONDS", 3.0)
    clock = _FakeClock(100.0, 100.5)
    monkeypatch.setattr(verifiable_consumer, "time", clock)
    mock: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    mock.schedule_poll_task(task=mock.wakeup)
    _verifiable(mock).run()
    assert clock.sleeps == [2.5]
    assert [line["name"] for line in _lines(capsys)] == ["startup_complete", "shutdown_complete"]


def test_run_does_not_wait_once_the_minimum_has_passed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(verifiable_consumer, "MIN_STARTUP_VISIBLE_SECONDS", 3.0)
    clock = _FakeClock(100.0, 103.5)
    monkeypatch.setattr(verifiable_consumer, "time", clock)
    mock: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    mock.schedule_poll_task(task=mock.wakeup)
    _verifiable(mock).run()
    assert clock.sleeps == []


def test_run_closes_with_the_close_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's consumer.close(CloseOptions.timeout(Duration.ofMillis(closeTimeoutMs))).
    options: list[CloseOptions] = []
    close = MockConsumer.close

    def recording_close(self: MockConsumer[str, str], *, option: CloseOptions) -> None:
        options.append(option)
        close(self, option=option)

    monkeypatch.setattr(MockConsumer, "close", recording_close)
    mock: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    mock.schedule_poll_task(task=mock.wakeup)
    _verifiable(mock, close_timeout_ms=15000).run()
    (option,) = options
    assert option.timeout() == 15.0
    assert option.group_membership_operation() == CloseOptions.GroupMembershipOperation.DEFAULT
    assert mock.closed()


def test_close_announces_the_shutdown_and_wakes_the_consumer(
        capsys: pytest.CaptureFixture[str]) -> None:
    mock: MockConsumer[str, str] = MockConsumer(offset_reset_strategy="earliest")
    mock.subscribe(topics=["t"])
    _verifiable(mock).close()
    assert [line["name"] for line in _lines(capsys)] == ["shutdown_requested"]
    with pytest.raises(WakeupError):
        mock.poll(timeout=0)


# ---- the group protocol -----------------------------------------------------

def test_group_protocol_of_ignores_case() -> None:
    assert group_protocol_of("consumer") == "CONSUMER"
    assert group_protocol_of("Classic") == "CLASSIC"


def test_group_protocol_of_rejects_an_unknown_protocol() -> None:
    with pytest.raises(IllegalArgumentError) as error:
        group_protocol_of("bogus")
    assert str(error.value) == "No enum constant org.apache.kafka.clients.consumer.GroupProtocol.BOGUS"


# ---- arguments --------------------------------------------------------------

MINIMAL = ["--bootstrap-server", "b", "--topic", "t", "--group-id", "g"]


def _parse(*args: str) -> tuple[Any, dict[str, Any]]:
    return verifiable_consumer.parse_args(arg_parser(), list(args))


def _parse_error(*args: str) -> str:
    with pytest.raises(ArgumentParserError) as error:
        _parse(*args)
    return str(error.value)


def test_parse_args_minimal_is_classic_by_default() -> None:
    res, props = _parse(*MINIMAL)
    assert (res.group_protocol, res.group_remote_assignor, res.group_instance_id,
            res.max_messages, res.session_timeout, res.verbose, res.use_auto_commit,
            res.reset_policy) == ("classic", None, None, -1, None, False, False, "earliest")
    assert props == {
        "group.protocol": "CLASSIC",
        "partition.assignment.strategy": "org.apache.kafka.clients.consumer.RangeAssignor",
        "group.id": "g", "bootstrap.servers": "b", "enable.auto.commit": False,
        "auto.offset.reset": "earliest"}


def test_parse_args_consumer_protocol() -> None:
    res, props = _parse(*MINIMAL, "--group-protocol", "consumer", "--group-remote-assignor",
                        "uniform", "--group-instance-id", "inst-1", "--max-messages", "100",
                        "--session-timeout", "30000", "--verbose", "--enable-autocommit",
                        "--reset-policy", "latest")
    assert (res.max_messages, res.verbose, res.use_auto_commit) == (100, True, True)
    assert props == {
        "group.protocol": "CONSUMER", "group.remote.assignor": "uniform",
        "session.timeout.ms": "30000", "group.id": "g", "group.instance.id": "inst-1",
        "bootstrap.servers": "b", "enable.auto.commit": True, "auto.offset.reset": "latest"}


def test_parse_args_close_timeout() -> None:
    # Kafka 4.4's system tests always pass it; it is not a consumer config.
    res, props = _parse(*MINIMAL)
    assert res.close_timeout == 30000
    res, props_with_timeout = _parse(*MINIMAL, "--close-timeout", "15000")
    assert res.close_timeout == 15000
    assert props_with_timeout == props


def test_parse_args_close_timeout_must_be_an_integer() -> None:
    assert _parse_error(*MINIMAL, "--close-timeout", "15s") == (
        "argument --close-timeout: invalid int value: '15s'")


def test_parse_args_consumer_protocol_ignores_the_assignment_strategy() -> None:
    _, props = _parse(*MINIMAL, "--group-protocol", "CONSUMER", "--assignment-strategy", "x")
    assert "partition.assignment.strategy" not in props
    assert "group.remote.assignor" not in props


def test_parse_args_last_repeated_option_wins() -> None:
    # The ducktape service repeats --session-timeout: it formats the option and
    # then multiplies the string by 1000.
    _, props = _parse(*MINIMAL, *(["--session-timeout", "10"] * 3), "--session-timeout", "20")
    assert props["session.timeout.ms"] == "20"


def test_parse_args_command_line_overrides_the_config_file(tmp_path: Any) -> None:
    config = tmp_path / "c.properties"
    config.write_text("group.id=from-file\nclient.id=tool\nauto.offset.reset=none\n")
    _, props = _parse(*MINIMAL, "--command-config", str(config))
    assert (props["group.id"], props["client.id"], props["auto.offset.reset"]) == (
        "g", "tool", "earliest")


def test_parse_args_deprecated_consumer_config_still_loads(
        tmp_path: Any, capsys: pytest.CaptureFixture[str]) -> None:
    config = tmp_path / "c.properties"
    config.write_text("client.id=tool\n")
    _, props = _parse(*MINIMAL, "--consumer.config", str(config))
    assert props["client.id"] == "tool"
    assert capsys.readouterr().out == (
        "Option --consumer.config has been deprecated and will be removed in a future version. "
        "Use --command-config instead.\n")


def test_parse_args_config_files_are_mutually_exclusive(tmp_path: Any) -> None:
    config = tmp_path / "c.properties"
    config.write_text("")
    assert _parse_error(*MINIMAL, "--consumer.config", str(config), "--command-config",
                        str(config)) == (
        "Options --consumer.config and --command-config are mutually exclusive.")


def test_parse_args_missing_bootstrap_server() -> None:
    assert _parse_error("--topic", "t", "--group-id", "g") == (
        "one of the arguments --bootstrap-server is required")


def test_parse_args_missing_topic_and_group_id() -> None:
    assert _parse_error("--bootstrap-server", "b") == (
        "the following arguments are required: --topic, --group-id")


def test_parse_args_missing_value() -> None:
    assert _parse_error(*MINIMAL, "--topic") == "argument --topic: expected one argument"


def test_parse_args_unrecognized_argument() -> None:
    assert _parse_error(*MINIMAL, "--bogus") == "unrecognized arguments: --bogus"


def test_parse_args_unknown_group_protocol() -> None:
    with pytest.raises(IllegalArgumentError):
        _parse(*MINIMAL, "--group-protocol", "bogus")


def test_create_from_args_builds_a_kafka_consumer() -> None:
    consumer = create_from_args(arg_parser(), [
        "--bootstrap-server", "localhost:1", "--topic", "t", "--group-id", "g",
        "--group-protocol", "consumer", "--max-messages", "4", "--verbose",
        "--close-timeout", "15000"])
    try:
        assert (consumer.topic, consumer.max_messages, consumer.verbose,
                consumer.use_auto_commit, consumer.use_async_commit,
                consumer.close_timeout_ms) == ("t", 4, True, False, False, 15000)
    finally:
        consumer.consumer.close()


def test_create_from_args_classic_protocol_is_unsupported() -> None:
    # The default --group-protocol is Java's, classic, which the client does
    # not implement.
    with pytest.raises(UnsupportedOperationError) as e:
        create_from_args(arg_parser(), ["--bootstrap-server", "localhost:1", "--topic", "t",
                                        "--group-id", "g"])
    assert str(e.value) == ("Classic group protocol is not yet supported in this client; set "
                            "group.protocol=consumer (KIP-848).")


# ---- the script -------------------------------------------------------------

def _script_env() -> dict[str, str]:
    import confluent_kafka

    package_root = os.path.dirname(os.path.dirname(confluent_kafka.__file__))
    # Without colour: from Python 3.14 argparse colours its help when
    # FORCE_COLOR is set, as many CI environments set it.
    env = {key: value for key, value in os.environ.items() if key != "FORCE_COLOR"}
    return {**env, "PYTHON_COLORS": "0", "NO_COLOR": "1",
            "PYTHONPATH": os.pathsep.join(filter(None, [package_root, env.get("PYTHONPATH")]))}


def _script(*args: str) -> list[str]:
    return [sys.executable, os.path.join(TOOLS_DIR, "verifiable_consumer.py"), *args]


def test_script_without_arguments_prints_help() -> None:
    result = subprocess.run(_script(), capture_output=True, text=True, timeout=60,
                            env=_script_env())
    assert result.returncode == 0
    assert result.stdout.startswith("usage: verifiable-consumer")


def test_script_with_bad_arguments_exits_1() -> None:
    result = subprocess.run(_script("--bootstrap-server", "b"), capture_output=True, text=True,
                            timeout=60, env=_script_env())
    assert result.returncode == 1
    assert result.stderr.endswith("verifiable-consumer: error: the following arguments are "
                                  "required: --topic, --group-id\n")


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_script_shuts_down_cleanly_on_a_signal(signum: int) -> None:
    # No broker: the consumer polls until the shutdown hook wakes it.
    process = subprocess.Popen(
        _script("--bootstrap-server", "localhost:1", "--topic", "t", "--group-id", "g",
                "--group-protocol", "consumer"),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=_script_env())
    try:
        assert process.stdout is not None
        startup = json.loads(process.stdout.readline())
        assert startup["name"] == "startup_complete"
        started = time.monotonic()
        process.send_signal(signum)
        out, err = process.communicate(timeout=60)
    finally:
        process.kill()
    assert process.returncode == 0, err
    lines = [json.loads(line) for line in out.splitlines()]
    assert [line["name"] for line in lines] == ["shutdown_requested", "shutdown_complete"]
    # The startup stays visible for the script's minimum of 3 s (the fixture
    # above lowers it only in this process), less the moment between the
    # tool's start and its first event, and no longer than the close.
    assert lines[1]["timestamp"] - startup["timestamp"] >= 2900
    assert time.monotonic() - started < 30
    assert "Error during processing" not in err
