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

"""Translated from ``org.apache.kafka.tools.VerifiableConsumer``, over the
``confluent_kafka`` Python client.

Command line consumer designed for system testing. It outputs consumer events
to STDOUT as JSON formatted objects. The "name" field in each JSON event
identifies the event type. The following events are currently supported:

- ``partitions_revoked``: outputs the partitions revoked through
  ``ConsumerRebalanceListener.on_partitions_revoked``.
- ``partitions_assigned``: outputs the partitions assigned through
  ``ConsumerRebalanceListener.on_partitions_assigned``.
- ``records_consumed``: contains a summary of records consumed in a single
  call to ``KafkaConsumer.poll``.
- ``record_data``: contains the key, value and offset of every record
  consumed, with ``--verbose``.
- ``offsets_committed``: the result of every offset commit (only applicable
  when auto-commit is not enabled).
- ``startup_complete``, ``shutdown_requested``, ``shutdown_complete``: the
  tool's lifecycle.

The JSON is Java's: the same event names, field names and field order, since
that is what the ducktape ``VerifiableConsumer`` service parses. The client
implements only the KIP-848 ``consumer`` group protocol, so
``--group-protocol classic`` fails when the consumer is built. Run as
``python3 verifiable_consumer.py --bootstrap-server host:9092 --topic t
--group-id g --group-protocol consumer``.
"""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Collection, Mapping
from datetime import timedelta
from typing import Any

from _common import (ArgumentParser, ArgumentParserError, ShutdownHook, configure_logging,
                     event, load_props, print_json)

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common import TopicPartition
from confluent_kafka.common.errors import FencedInstanceIdError, WakeupError
from confluent_kafka.common.serialization import string_deserializer
from confluent_kafka.consumer import (CloseOptions, Consumer, ConsumerRebalanceListener,
                                      ConsumerRecord, ConsumerRecords, KafkaConsumer,
                                      OffsetAndMetadata)

__all__ = ["VerifiableConsumer", "arg_parser", "create_from_args", "main"]

_LOG = logging.getLogger("verifiable_consumer")

# Java's ConsumerConfig.DEFAULT_GROUP_PROTOCOL, GroupProtocol's constants, and
# the class names of the classic assignors, which this client does not have.
_DEFAULT_GROUP_PROTOCOL = "classic"
_GROUP_PROTOCOLS = ("CLASSIC", "CONSUMER")
_RANGE_ASSIGNOR = "org.apache.kafka.clients.consumer.RangeAssignor"
_ROUND_ROBIN_ASSIGNOR = "org.apache.kafka.clients.consumer.RoundRobinAssignor"

# Java polls with Duration.ofMillis(Long.MAX_VALUE). The longest Duration this
# client takes exactly is timedelta.max (about 2.7 million years): a float near
# Long.MAX_VALUE milliseconds rounds past it and raises OverflowError.
_POLL_TIMEOUT = timedelta.max

# How long `startup_complete` stays visible before `shutdown_complete` at the
# least (the Rust tool's MIN_STARTUP_VISIBLE; Java has no such delay). The
# ducktape service counts a node as started only between the two events, and
# polls for it. A consumer fenced on its first heartbeat (a static member whose
# group.instance.id is in use, as in test_fencing_static_consumer) closes in a
# few milliseconds, too fast for the poll to see, and the start gate times
# out; Java leaves a window of about 250 ms only through its slower shutdown.
# The consumer is closed before the wait, so it adds no group activity.
MIN_STARTUP_VISIBLE_SECONDS = 3.0


def group_protocol_of(name: str) -> str:
    """Java's ``GroupProtocol.of(name)``: the constant's name for ``name`` in
    any case, raising Java's ``valueOf`` error for an unknown one."""
    constant = name.upper()
    if constant not in _GROUP_PROTOCOLS:
        raise IllegalArgumentError(
            message=f"No enum constant org.apache.kafka.clients.consumer.GroupProtocol.{constant}")
    return constant


# The events (Java's `ConsumerEvent` subclasses), as their JSON objects. Java's
# custom serializer writes a TopicPartition as {"topic": ..., "partition": ...}.

def _partition_data(tp: TopicPartition) -> dict[str, Any]:
    return {"topic": tp.topic(), "partition": tp.partition()}


def startup_complete() -> dict[str, Any]:
    return event("startup_complete")


def shutdown_requested() -> dict[str, Any]:
    return event("shutdown_requested")


def shutdown_complete() -> dict[str, Any]:
    return event("shutdown_complete")


def partitions_revoked(partitions: Collection[TopicPartition]) -> dict[str, Any]:
    return event("partitions_revoked", partitions=[_partition_data(tp) for tp in partitions])


def partitions_assigned(partitions: Collection[TopicPartition]) -> dict[str, Any]:
    return event("partitions_assigned", partitions=[_partition_data(tp) for tp in partitions])


def record_set_summary(tp: TopicPartition, count: int, min_offset: int,
                       max_offset: int) -> dict[str, Any]:
    # Java's getter-derived property names: minOffset, maxOffset.
    return {**_partition_data(tp), "count": count, "minOffset": min_offset,
            "maxOffset": max_offset}


def records_consumed(count: int, partition_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    return event("records_consumed", count=count, partitions=partition_summaries)


def record_data(record: ConsumerRecord[str, str]) -> dict[str, Any]:
    return event("record_data", key=record.key(), value=record.value(), topic=record.topic(),
                 partition=record.partition(), offset=record.offset())


def offsets_committed(offsets: Mapping[TopicPartition, OffsetAndMetadata],
                      error: str | None, success: bool) -> dict[str, Any]:
    data = event("offsets_committed",
                 offsets=[{**_partition_data(tp), "offset": om.offset()}
                          for tp, om in offsets.items()])
    # Java's error() is @JsonInclude(NON_NULL): a null error is left out.
    if error is not None:
        data["error"] = error
    data["success"] = success
    return data


class VerifiableConsumer(ConsumerRebalanceListener):
    """The consuming loop, and its rebalance listener and commit callback, as
    Java's ``VerifiableConsumer implements OffsetCommitCallback,
    ConsumerRebalanceListener``."""

    def __init__(self, consumer: Consumer[str, str], topic: str, max_messages: int,
                 use_auto_commit: bool, use_async_commit: bool, verbose: bool,
                 close_timeout_ms: int) -> None:
        # Java also takes the PrintStream to print to, always System.out.
        self.consumer = consumer
        self.topic = topic
        self.use_auto_commit = use_auto_commit
        self.use_async_commit = use_async_commit
        self.verbose = verbose
        self.max_messages = max_messages
        self.consumed_messages = 0
        self.close_timeout_ms = close_timeout_ms

    def has_message_limit(self) -> bool:
        return self.max_messages >= 0

    def is_finished(self) -> bool:
        return self.has_message_limit() and self.consumed_messages >= self.max_messages

    def on_records_received(self, records: ConsumerRecords[str, str]
                            ) -> dict[TopicPartition, OffsetAndMetadata]:
        offsets: dict[TopicPartition, OffsetAndMetadata] = {}
        summaries: list[dict[str, Any]] = []
        for tp in records.partitions():
            partition_records = records.records(partition=tp)

            if (self.has_message_limit()
                    and self.consumed_messages + len(partition_records) > self.max_messages):
                partition_records = partition_records[:self.max_messages - self.consumed_messages]

            if not partition_records:
                continue

            min_offset = partition_records[0].offset()
            max_offset = partition_records[-1].offset()

            offsets[tp] = OffsetAndMetadata(offset=max_offset + 1)
            summaries.append(record_set_summary(tp, len(partition_records), min_offset,
                                                max_offset))

            if self.verbose:
                for record in partition_records:
                    print_json(record_data(record))

            self.consumed_messages += len(partition_records)
            if self.is_finished():
                break

        # The count is the whole poll's, not the records kept above.
        print_json(records_consumed(len(records), summaries))
        return offsets

    def on_complete(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None,
                    exception: Exception | None, **_: Any) -> None:
        """Java's ``OffsetCommitCallback.onComplete``. The client passes
        ``None`` offsets for a failed asynchronous commit, where Java's would
        dereference them; ``create_from_args`` never commits asynchronously."""
        if exception is None:
            success, error = True, None
        else:
            # Java's exception.getMessage(), left out when null (a client
            # error's str() is "" for a null message).
            success, error = False, str(exception) or None
        print_json(offsets_committed(offsets or {}, error, success))

    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        print_json(partitions_assigned(partitions))

    def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        print_json(partitions_revoked(partitions))

    # on_partitions_lost is not overridden, as in Java: the listener's default
    # reports lost partitions through on_partitions_revoked.

    def _commit_sync(self, offsets: dict[TopicPartition, OffsetAndMetadata]) -> None:
        try:
            self.consumer.commit(offsets=offsets)
            self.on_complete(offsets=offsets, exception=None)
        except WakeupError:
            # we only call wakeup() once to close the consumer, so this recursion should be safe
            self._commit_sync(offsets)
            raise
        except FencedInstanceIdError:
            raise
        except Exception as e:
            self.on_complete(offsets=offsets, exception=e)

    def run(self) -> None:
        started = time.monotonic()
        try:
            print_json(startup_complete())
            self.consumer.subscribe(topics=[self.topic], callback=self)

            while not self.is_finished():
                records = self.consumer.poll(timeout=_POLL_TIMEOUT)
                offsets = self.on_records_received(records)

                if not self.use_auto_commit:
                    if self.use_async_commit:
                        self.consumer.commit_nowait(offsets=offsets, callback=self.on_complete)
                    else:
                        self._commit_sync(offsets)
        except WakeupError:
            # ignore, we are closing
            _LOG.debug("Caught WakeupError because consumer is shutdown, ignore and terminate.")
        except Exception:
            # Log the error so it goes to the service log and not stdout
            _LOG.exception("Error during processing, terminating consumer process: ")
        finally:
            # Java lets an error closing end run() without shutdown_complete;
            # it is logged here and the shutdown still announced, as by the
            # Rust tool.
            try:
                self.consumer.close(option=CloseOptions.timeout(
                    timeout=timedelta(milliseconds=self.close_timeout_ms)))
            except Exception:
                _LOG.exception("Error closing consumer: ")
            remaining = MIN_STARTUP_VISIBLE_SECONDS - (time.monotonic() - started)
            if remaining > 0:
                time.sleep(remaining)
            print_json(shutdown_complete())

    def close(self) -> None:
        """Java's ``close()``, which the shutdown hook runs: announce the
        shutdown and wake the consumer out of its poll. Java then waits for
        ``run()`` to finish; here the main thread runs ``run()`` to its end
        before it exits."""
        try:
            print_json(shutdown_requested())
        finally:
            # The main thread waits in poll() with no time limit: nothing must
            # keep the hook from waking it.
            self.consumer.wakeup()


def arg_parser() -> ArgumentParser:
    """Get the command-line argument parser."""
    parser = ArgumentParser(
        "verifiable-consumer",
        "This tool consumes messages from a specific topic and emits consumer events (e.g. group "
        "rebalances, received messages, and offsets committed) as JSON objects to STDOUT.")

    connection_group = parser.add_argument_group(
        "Connection Group", "Group of arguments for connection to brokers"
    ).add_mutually_exclusive_group(required=True)
    connection_group.add_argument(
        "--bootstrap-server", dest="bootstrap_server", metavar="HOST1:PORT1[,HOST2:PORT2[...]]",
        help="The server(s) to connect to. Comma-separated list of Kafka brokers in the form "
        "HOST1:PORT1,HOST2:PORT2,...")
    parser.add_argument("--topic", required=True, metavar="TOPIC",
                        help="Consumes messages from this topic.")
    parser.add_argument(
        "--group-protocol", default=_DEFAULT_GROUP_PROTOCOL, dest="group_protocol",
        metavar="GROUP-PROTOCOL",
        help=f"Group protocol (must be one of {', '.join(_GROUP_PROTOCOLS)})")
    parser.add_argument(
        "--group-remote-assignor", dest="group_remote_assignor", metavar="GROUP-REMOTE-ASSIGNOR",
        help="Group remote assignor; only used if the group protocol is CONSUMER")
    parser.add_argument("--group-id", required=True, dest="group_id", metavar="GROUP-ID",
                        help="The group id of the consumer group")
    parser.add_argument("--group-instance-id", dest="group_instance_id",
                        metavar="GROUP-INSTANCE-ID",
                        help="A unique identifier of the consumer instance")
    parser.add_argument(
        "--max-messages", type=int, default=-1, dest="max_messages", metavar="MAX-MESSAGES",
        help="Consume this many messages. If -1 (the default), the consumer will consume until "
        "the process is killed externally")
    parser.add_argument(
        "--session-timeout", type=int, dest="session_timeout", metavar="TIMEOUT-MS",
        help="Set the consumer's session timeout, note that this configuration is not supported "
        "when group protocol is consumer")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable to log individual consumed records")
    parser.add_argument("--enable-autocommit", action="store_true", dest="use_auto_commit",
                        help="Enable offset auto-commit on consumer")
    parser.add_argument(
        "--close-timeout", type=int, default=30000, dest="close_timeout",
        metavar="CLOSE-TIMEOUT-MS",
        help="Timeout in milliseconds for closing the consumer (default: %(default)s)")
    parser.add_argument(
        "--reset-policy", default="earliest", dest="reset_policy", metavar="RESET-POLICY",
        help="Set reset policy (must be either 'earliest', 'latest', or 'none')")
    parser.add_argument(
        "--assignment-strategy", default=_RANGE_ASSIGNOR, dest="assignment_strategy",
        metavar="ASSIGNMENT-STRATEGY",
        help=f"Set assignment strategy (e.g. {_ROUND_ROBIN_ASSIGNOR}); only used if the group "
        "protocol is CLASSIC")
    parser.add_argument(
        "--consumer.config", dest="consumer_config", metavar="CONFIG-FILE",
        help="(DEPRECATED) Consumer config properties file. This option will be removed in a "
        "future version. Use --command-config instead")
    parser.add_argument(
        "--command-config", dest="command_config_file", metavar="CONFIG-FILE",
        help="Config properties file (config options shared with command line parameters will "
        "be overridden).")
    return parser


def parse_args(parser: ArgumentParser, args: list[str]) -> tuple[Any, dict[str, Any]]:
    """The parsed arguments and the consumer's configuration: Java's
    ``createFromArgs`` up to building the consumer."""
    res = parser.parse_args(args)
    if res.consumer_config is not None and res.command_config_file is not None:
        raise ArgumentParserError(
            "Options --consumer.config and --command-config are mutually exclusive.")

    consumer_props: dict[str, Any] = {}
    if res.consumer_config is not None:
        print("Option --consumer.config has been deprecated and will be removed in a future "
              "version. Use --command-config instead.", flush=True)
        consumer_props.update(load_props(res.consumer_config))
    if res.command_config_file is not None:
        consumer_props.update(load_props(res.command_config_file))

    group_protocol = group_protocol_of(res.group_protocol)
    consumer_props["group.protocol"] = group_protocol

    # 3.7.0 includes support for KIP-848 which introduced a new implementation of the consumer
    # group protocol. The two implementations use slightly different configuration, hence these
    # arguments are conditional.
    #
    # See the Python class/method VerifiableConsumer.start_cmd() in verifiable_consumer.py for
    # how the command line arguments are passed in by the system test framework.
    if group_protocol == "CONSUMER":
        if res.group_remote_assignor is not None:
            consumer_props["group.remote.assignor"] = res.group_remote_assignor
    else:
        # This means we're using the CLASSIC consumer group protocol.
        consumer_props["partition.assignment.strategy"] = res.assignment_strategy

    if res.session_timeout is not None:
        consumer_props["session.timeout.ms"] = str(res.session_timeout)

    consumer_props["group.id"] = res.group_id

    if res.group_instance_id is not None:
        consumer_props["group.instance.id"] = res.group_instance_id

    consumer_props["bootstrap.servers"] = res.bootstrap_server
    consumer_props["enable.auto.commit"] = res.use_auto_commit
    consumer_props["auto.offset.reset"] = res.reset_policy
    return res, consumer_props


def create_from_args(parser: ArgumentParser, args: list[str]) -> VerifiableConsumer:
    """Construct a ``VerifiableConsumer`` from command-line arguments."""
    res, props = parse_args(parser, args)
    consumer: KafkaConsumer[str, str] = KafkaConsumer(
        configs=props, key_deserializer=string_deserializer(),
        value_deserializer=string_deserializer())
    return VerifiableConsumer(consumer, res.topic, res.max_messages, res.use_auto_commit,
                              False, res.verbose, res.close_timeout)


def main(args: list[str]) -> int:
    configure_logging()
    parser = arg_parser()
    if not args:
        parser.print_help()
        return 0

    try:
        consumer = create_from_args(parser, args)
    except ArgumentParserError as e:
        # argparse4j's handleError: the usage and the error, then exit 1. Any
        # other error, as from Java's main, ends the tool with its traceback.
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: {e}", file=sys.stderr)
        return 1

    hook = ShutdownHook(consumer.close, "verifiable-consumer-shutdown-hook")
    try:
        consumer.run()
    finally:
        hook.finish()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
