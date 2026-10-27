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

"""Translated from ``org.apache.kafka.tools.VerifiableProducer``, over the
``confluent_kafka`` Python client.

Primarily intended for use with system testing, this producer prints metadata
in the form of JSON to stdout on each "send" request. For example, this helps
with end-to-end correctness tests by making externally visible which messages
have been acked and which have not.

When used as a command-line tool, it produces increasing integers. It will
produce a fixed number of messages unless the default max-messages -1 is used,
in which case it produces indefinitely.

If logging is left enabled, log output on stdout can be easily ignored by
checking whether a given line is valid JSON.

The JSON printed on stdout is Java's: the same event names, field names and
field order, since that is what the ducktape ``VerifiableProducer`` service
parses. Run as ``python3 verifiable_producer.py --topic t --bootstrap-server
host:9092``.
"""

from __future__ import annotations

import sys
import threading
from typing import Any

from _common import (OUTPUT_LOCK, ArgumentParser, ArgumentParserError, ShutdownHook,
                     configure_logging, event, load_props, now_millis, print_json)
from throughput_throttler import ThroughputThrottler

from confluent_kafka.common.serialization import string_serializer
from confluent_kafka.producer import (Callback, KafkaProducer, Producer, ProducerRecord,
                                     RecordMetadata)

__all__ = ["VerifiableProducer", "arg_parser", "create_from_args", "main"]


def _class_name(e: BaseException) -> str:
    """Java's ``Exception.getClass().toString()``, ``"class "`` and the class's
    full name: here the Python class's module and name."""
    return f"class {type(e).__module__}.{type(e).__qualname__}"


def _message(e: BaseException) -> str | None:
    """Java's ``Exception.getMessage()``: the error's message, ``None`` for
    none (a client error's ``str()`` is ``""`` when Java's message is null)."""
    return str(e) or None


# The events (Java's `ProducerEvent` subclasses), as their JSON objects.

def startup_complete() -> dict[str, Any]:
    return event("startup_complete")


def shutdown_complete() -> dict[str, Any]:
    return event("shutdown_complete")


def successful_send(key: str | None, value: str, record_metadata: RecordMetadata) -> dict[str, Any]:
    return event("producer_send_success", key=key, value=value, topic=record_metadata.topic(),
                 partition=record_metadata.partition(), offset=record_metadata.offset())


def failed_send(key: str | None, value: str, topic: str, e: BaseException) -> dict[str, Any]:
    return event("producer_send_error", key=key, value=value, topic=topic,
                 exception=_class_name(e), message=_message(e))


def tool_data(sent: int, acked: int, target_throughput: int,
              avg_throughput: float) -> dict[str, Any]:
    return event("tool_data", sent=sent, acked=acked, target_throughput=target_throughput,
                 avg_throughput=avg_throughput)


class VerifiableProducer:
    """The producing loop, its counters and the send callback."""

    def __init__(self, producer: Producer[str, str], topic: str, throughput: int,
                 max_messages: int, value_prefix: int | None, create_time: int | None,
                 repeating_keys: int | None) -> None:
        self.topic = topic
        self.producer = producer
        # If max_messages < 0, produce until the process is killed externally
        self.max_messages = max_messages
        # Number of messages for which acks were received
        self.num_acked = 0
        # Number of send attempts
        self.num_sent = 0
        # Throttle message throughput if this is set >= 0
        self.throughput = throughput
        # Hook to trigger producing thread to stop sending messages
        self.stop_producing = threading.Event()
        # Prefix (plus a dot separator) added to every value produced by verifiable producer;
        # if None, then values are produced without a prefix
        self.value_prefix = value_prefix
        # Send messages with a key of 0 incrementing by 1 for each message produced when
        # number specified is reached key is reset to 0
        self.repeating_keys = repeating_keys
        self.key_counter = 0
        # The create time to set in messages, in milliseconds since epoch
        self.create_time = create_time
        self.start_time = now_millis()

    def send(self, key: str | None, value: str) -> None:
        """Produce a message with given key and value."""
        # Older versions of ProducerRecord don't include the message create time
        # in the constructor, so Java only includes it when it is explicitly
        # specified; this client always has it, but the two forms are kept.
        try:
            if self.create_time is not None:
                record: ProducerRecord[str, str] = ProducerRecord(
                    topic=self.topic, partition=None, timestamp=self.create_time,
                    key=key, value=value)
                self.create_time += now_millis() - self.start_time
            else:
                record = ProducerRecord(topic=self.topic, key=key, value=value)
        except Exception as e:
            # Java builds the record outside its try, so a negative create time
            # ends the producing thread; it is reported as a failed send
            # instead, as the Rust tool does, and not counted as sent.
            print_json(failed_send(key, value, self.topic, e))
            return

        self.num_sent += 1
        try:
            self.producer.send(record=record, callback=self._print_info_callback(key, value))
        except Exception as e:
            print_json(failed_send(key, value, self.topic, e))

    def _print_info_callback(self, key: str | None, value: str) -> Callback:
        """Java's ``PrintInfoCallback``: exactly one line per completed send.
        It runs on the producer's completion thread, so the counter it bumps
        is guarded by the output lock, as Java's by ``synchronized
        (System.out)``."""
        def on_completion(*, metadata: RecordMetadata, exception: Exception | None,
                          **_: Any) -> None:
            with OUTPUT_LOCK:
                if exception is None:
                    self.num_acked += 1
                    print_json(successful_send(key, value, metadata))
                else:
                    print_json(failed_send(key, value, self.topic, exception))
        return on_completion

    def get_value(self, val: int) -> str:
        """Returns a string to publish: either ``"value_prefix.val"`` or
        ``"val"``."""
        if self.value_prefix is not None:
            return f"{self.value_prefix}.{val}"
        return f"{val}"

    def get_key(self) -> str | None:
        """Returns the next key: ``None`` without ``--repeating-keys``, else
        counting from 0 and back to 0 at ``repeating_keys``."""
        key = None
        if self.repeating_keys is not None:
            key = str(self.key_counter)
            self.key_counter += 1
            if self.key_counter == self.repeating_keys:
                self.key_counter = 0
        return key

    def close(self) -> None:
        """Close the producer to flush any remaining messages."""
        self.producer.close()
        print_json(shutdown_complete())

    def run(self, throttler: ThroughputThrottler) -> None:
        print_json(startup_complete())
        # negative max_messages (-1) means "infinite"
        max_messages = 2**63 - 1 if self.max_messages < 0 else self.max_messages

        i = 0
        while i < max_messages:
            if self.stop_producing.is_set():
                break
            send_start_ms = now_millis()

            self.send(self.get_key(), self.get_value(i))

            if throttler.should_throttle(i, send_start_ms):
                throttler.throttle()
            i += 1


def arg_parser() -> ArgumentParser:
    """Get the command-line argument parser."""
    parser = ArgumentParser(
        "verifiable-producer",
        "This tool produces increasing integers to the specified topic and prints JSON metadata "
        "to stdout on each \"send\" request, making externally visible which messages have been "
        "acked and which have not.")

    parser.add_argument("--topic", required=True, metavar="TOPIC",
                        help="Produce messages to this topic.")
    connection_group = parser.add_argument_group(
        "Connection Group", "Group of arguments for connection to brokers"
    ).add_mutually_exclusive_group(required=True)
    connection_group.add_argument(
        "--bootstrap-server", dest="bootstrap_server", metavar="HOST1:PORT1[,HOST2:PORT2[...]]",
        help="REQUIRED: The server(s) to connect to. Comma-separated list of Kafka brokers in "
        "the form HOST1:PORT1,HOST2:PORT2,...")
    parser.add_argument(
        "--max-messages", type=int, default=-1, dest="max_messages", metavar="MAX-MESSAGES",
        help="Produce this many messages. If -1, produce messages until the process is killed "
        "externally.")
    parser.add_argument(
        "--throughput", type=int, default=-1, metavar="THROUGHPUT",
        help="If set >= 0, throttle maximum message throughput to *approximately* THROUGHPUT "
        "messages/sec.")
    parser.add_argument(
        "--acks", type=int, default=-1, choices=(0, 1, -1), metavar="ACKS",
        help="Acks required on each produced message. See Kafka docs on acks for details.")
    parser.add_argument(
        "--producer.config", dest="producer_config", metavar="CONFIG-FILE",
        help="(DEPRECATED) Producer config properties file. This option will be removed in a "
        "future version. Use --command-config instead.")
    parser.add_argument(
        "--message-create-time", type=int, default=-1, dest="create_time", metavar="CREATE-TIME",
        help="Send messages with creation time starting at the arguments value, in milliseconds "
        "since epoch")
    parser.add_argument(
        "--value-prefix", type=int, dest="value_prefix", metavar="VALUE-PREFIX",
        help="If specified, each produced value will have this prefix with a dot separator")
    parser.add_argument(
        "--repeating-keys", type=int, dest="repeating_keys", metavar="REPEATING-KEYS",
        help="If specified, each produced record will have a key starting at 0 increment by 1 "
        "up to the number specified (exclusive), then the key is set to 0 again")
    parser.add_argument(
        "--command-config", dest="command_config_file", metavar="CONFIG-FILE",
        help="Config properties file (config options shared with command line parameters will "
        "be overridden).")
    return parser


def parse_args(parser: ArgumentParser, args: list[str]) -> tuple[Any, dict[str, Any]]:
    """The parsed arguments and the producer's configuration: Java's
    ``createFromArgs`` up to building the producer."""
    res = parser.parse_args(args)
    if res.producer_config is not None and res.command_config_file is not None:
        raise ArgumentParserError(
            "Options --producer.config and --command-config are mutually exclusive.")

    producer_props: dict[str, Any] = {
        "bootstrap.servers": res.bootstrap_server,
        "acks": str(res.acks),
        # No producer retries
        "retries": "0",
    }
    # Java also sets key.serializer and value.serializer to StringSerializer;
    # here the serializers are given to the constructor.
    if res.producer_config is not None:
        print("Option --producer.config has been deprecated and will be removed in a future "
              "version. Use --command-config instead.", flush=True)
        producer_props.update(load_props(res.producer_config))
    if res.command_config_file is not None:
        producer_props.update(load_props(res.command_config_file))
    return res, producer_props


def create_from_args(parser: ArgumentParser, args: list[str]) -> VerifiableProducer:
    """Construct a ``VerifiableProducer`` from command-line arguments."""
    res, props = parse_args(parser, args)
    # -1 means no explicit create time
    create_time = None if res.create_time == -1 else res.create_time
    producer: KafkaProducer[str, str] = KafkaProducer(
        configs=props, key_serializer=string_serializer(), value_serializer=string_serializer())
    return VerifiableProducer(producer, res.topic, res.throughput, res.max_messages,
                              res.value_prefix, create_time, res.repeating_keys)


def main(args: list[str]) -> int:
    configure_logging()
    parser = arg_parser()
    if not args:
        parser.print_help()
        return 0

    try:
        producer = create_from_args(parser, args)
    except ArgumentParserError as e:
        # argparse4j's handleError: the usage and the error, then exit 1. Any
        # other error, as from Java's main, ends the tool with its traceback.
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: {e}", file=sys.stderr)
        return 1

    start_ms = now_millis()
    throttler = ThroughputThrottler(producer.throughput, start_ms)

    # Java's shutdown hook body, which the JVM runs at the signal or, after a
    # normal end, at exit. Here the signal's hook thread or the main thread
    # runs it, whichever comes first, and the other waits for it: once.
    finish_lock = threading.Lock()
    finished = False

    def close_and_summarize() -> None:
        nonlocal finished
        with finish_lock:
            if finished:
                return
            finished = True

            # Flush any remaining messages. An error closing ends the tool
            # with its traceback and no summary, as it ends Java's hook.
            producer.close()

            # Print a summary
            stop_ms = now_millis()
            elapsed_ms = stop_ms - start_ms
            # When start and stop fall in one millisecond Java's division
            # gives Infinity (NaN with nothing acked), which Jackson prints as
            # the string "Infinity". Reporting 0.0 instead is this tool's
            # choice: the system tests never read avg_throughput.
            avg_throughput = 1000 * (producer.num_acked / elapsed_ms) if elapsed_ms else 0.0
            print_json(tool_data(producer.num_sent, producer.num_acked, producer.throughput,
                                 avg_throughput))

    def stop() -> None:
        # Trigger main thread to stop producing messages, and wake a
        # throttle() that would otherwise keep it parked.
        producer.stop_producing.set()
        throttler.wakeup()
        # Then close the producer from here, as Java's hook does: a send
        # blocked on metadata or buffer space fails at once ("Producer closed
        # while send in progress") instead of holding the shutdown for up to
        # max.block.ms.
        close_and_summarize()

    hook = ShutdownHook(stop, "verifiable-producer-shutdown-hook")
    try:
        producer.run(throttler)
        close_and_summarize()
        return 0
    finally:
        hook.finish()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
