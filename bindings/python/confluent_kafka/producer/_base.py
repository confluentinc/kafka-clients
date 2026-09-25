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

"""State and helpers shared by the FFI-backed producers (private).

``Producer`` / ``KafkaProducer`` and ``AsyncProducer`` / ``AsyncKafkaProducer``
call the C FFI through the C extension (``_confluentkafka``), whose batching
engine owns two background threads: the send thread hands accepted records to
the Rust producer (``kafka_producer_Producer_send_batch``) and the poll thread
waits on their futures and runs each record's completion. Both families reuse
the same entry points and differ only in the future type a completion resolves
and where the user's callback runs (CLAUDE.md, Python Binding Conventions,
Implementation over the FFI).
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._config import duration_to_ms, log_unused, prepare
from confluent_kafka._errors import from_ffi_error
from confluent_kafka.common.metric_name import MetricName
from confluent_kafka.common.node import Node
from confluent_kafka.common.partition_info import PartitionInfo
from confluent_kafka.common.serialization import bytes_serializer
from confluent_kafka.common.serialization._supply import close_if_defined, resolve_serde
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.serialization import Serializer
    from confluent_kafka.consumer import ConsumerGroupMetadata, OffsetAndMetadata

    from .producer_record import ProducerRecord

# Java's KafkaProducer.throwIfProducerClosed() message.
CLOSED_MESSAGE = "Cannot perform operation after producer has been closed"

# Java's KafkaProducer.flush() message for a flush from a delivery callback.
FLUSH_IN_CALLBACK_MESSAGE = ("KafkaProducer.flush() invocation inside a callback is not "
                             "permitted because it may lead to deadlock.")

# Java's KafkaProducer.throwIfInvalidGroupMetadata() message for a null argument;
# the core cannot receive a null, so the binding checks it.
NULL_GROUP_METADATA_MESSAGE = "Consumer group metadata could not be null"

# Java's ProducerConfig keys of a serializer given through the config route.
_KEY_SERIALIZER = "key.serializer"
_VALUE_SERIALIZER = "value.serializer"

# Java's close() is close(Duration.ofMillis(Long.MAX_VALUE)).
LONG_MAX_VALUE = (1 << 63) - 1


class _SnapshotMetric:
    """A point-in-time metric value returned from ``metrics()``: the
    ``Metric`` protocol over the ``MetricName`` and the value the FFI read
    (``metric_value()`` is Java's ``Object``)."""

    __slots__ = ("_name", "_value")

    def __init__(self, name: MetricName, value: object) -> None:
        self._name = name
        self._value = value

    def metric_name(self) -> MetricName:
        return self._name

    def metric_value(self) -> Any:
        return self._value


def to_metrics_map(raw: list[dict[str, Any]] | None) -> dict[MetricName, Metric]:
    """``dict[MetricName, Metric]`` from the FFI metrics snapshot, a list of
    ``{name, group, description, tags, value, kind}`` dicts (the key is the
    whole ``MetricName``, as Java's ``Map<MetricName, Metric>``)."""
    out: dict[MetricName, Metric] = {}
    for entry in raw or ():
        name = MetricName(name=entry["name"], group=entry["group"],
                          description=entry["description"], tags=entry["tags"])
        out[name] = _SnapshotMetric(name, entry["value"])
    return out


def _to_node(raw: tuple[int, str, int, str | None]) -> Node:
    return Node(id=raw[0], host=raw[1], port=raw[2], rack=raw[3])


def to_partition_info(raw: tuple[Any, ...]) -> PartitionInfo:
    topic, partition, leader, replicas, isr, offline = raw
    return PartitionInfo(
        topic=topic,
        partition=partition,
        leader=None if leader is None else _to_node(leader),
        replicas=tuple(_to_node(n) for n in replicas),
        in_sync_replicas=tuple(_to_node(n) for n in isr),
        offline_replicas=tuple(_to_node(n) for n in offline),
    )


def offsets_to_spec(
        offsets: Mapping[TopicPartition, OffsetAndMetadata]) -> list[tuple[str, int, int, int, str]]:
    """``{TopicPartition: OffsetAndMetadata}`` as the ``(topic, partition,
    offset, leader_epoch, metadata)`` tuples the FFI takes (a missing leader
    epoch is -1)."""
    return [(tp.topic(), tp.partition(), oam.offset(),
             -1 if oam.leader_epoch() is None else cast(int, oam.leader_epoch()),
             oam.metadata())
            for tp, oam in offsets.items()]


def group_metadata_fields(group_metadata: ConsumerGroupMetadata) -> tuple[str, int, str, str | None]:
    """The four fields the FFI builds its ``ConsumerGroupMetadata`` from."""
    return (group_metadata.group_id(), group_metadata.generation_id(),
            group_metadata.member_id(), group_metadata.group_instance_id())


def close_timeout_ms(timeout: Duration | None) -> int | None:
    """``close(Duration)``'s timeout in milliseconds (``None`` for ``close()``);
    a negative one raises ``IllegalArgumentError`` with Java's message before
    anything else, as ``KafkaProducer.close(Duration, boolean)`` does."""
    return None if timeout is None else duration_to_ms(timeout, default_ms=0)


def serialize(serializer: Serializer[Any], topic: str, data: object,
              headers: object) -> bytes | None:
    """One serializer call, Java's ``serialize(topic, headers, data)``: a
    ``None`` ``data`` is passed too (the serializer decides what Java's null
    maps to), and a ``None`` result is a null key or value."""
    out = serializer(topic, data, cast(Any, headers))
    if out is None or type(out) is bytes:
        return out
    if isinstance(out, (bytes, bytearray, memoryview)):
        return bytes(out)
    raise TypeError(f"a serializer must return bytes or None, not {type(out).__name__}")


def await_payload(submit: Callable[[Callable[..., None]], None]) -> tuple[Any, ...]:
    """Submit an ``_async`` FFI operation and wait for its completion payload
    on a ``threading.Event``, which releases the GIL (so the dispatcher thread
    can run the completion) and stays interruptible by Ctrl+C."""
    box: dict[str, tuple[Any, ...]] = {}
    done = threading.Event()

    def cb(*payload: Any) -> None:
        box["payload"] = payload
        done.set()

    submit(cb)
    done.wait()
    return box["payload"]


def run_sync(submit: Callable[[Callable[..., None]], None]) -> None:
    """A void ``_async`` FFI operation, waited for; raises its typed error."""
    (error,) = await_payload(submit)
    if error:
        raise from_ffi_error(error)


def raise_if_error(error: int) -> None:
    """Raise the typed error of a plain FFI entry point's result handle."""
    if error:
        raise from_ffi_error(error)


class _ProducerState:
    """The native producer handle, its serializers and the closed flag."""

    def __init__(self) -> None:
        self._c_producer: int = 0
        self._closed = False
        # Futures of the records sent and not yet completed: flush() waits for
        # the ones sent before it (their callbacks have run by then, as in Java).
        self._futures: set[Any] = set()
        self._key_serializer: Serializer[Any] = bytes_serializer()
        self._value_serializer: Serializer[Any] = bytes_serializer()

    def _start(self, configs: dict[str, Any], key_serializer: Serializer[Any] | None,
               value_serializer: Serializer[Any] | None) -> None:
        """Java's ``KafkaProducer(configs, keySerializer, valueSerializer)``:
        parse ``configs``, take the serializers (an argument wins over the
        config key; neither means ``bytes_serializer()``), build the core
        producer, then log the unused configs. A given serializer argument
        replaces its config key, which is then not parsed
        (``ProducerConfig.appendSerializerToConfig``). A construction failure
        raises its typed error (Java's ``KafkaException("Failed to construct
        kafka producer", cause)``) after closing the serializers built so far."""
        given = [key for key, argument in ((_KEY_SERIALIZER, key_serializer),
                                           (_VALUE_SERIALIZER, value_serializer))
                 if argument is not None]
        originals, native = prepare(configs, client="producer", given_serdes=given)
        key = cast("Serializer[Any]", resolve_serde(
            key_serializer, originals, _KEY_SERIALIZER, is_key=True, default=bytes_serializer()))
        value: Serializer[Any] | None = None
        try:
            value = cast("Serializer[Any]", resolve_serde(
                value_serializer, originals, _VALUE_SERIALIZER, is_key=False,
                default=bytes_serializer()))
            handle, error = _lib.KafkaProducer_new(native, self)
        except BaseException:
            close_if_defined(key)
            if value is not None:
                close_if_defined(value)
            raise
        if error:
            close_if_defined(key)
            close_if_defined(value)
            raise from_ffi_error(error)
        self._key_serializer = key
        self._value_serializer = value
        self._c_producer = handle
        log_unused(originals, client="producer")

    def _check_not_closed(self) -> None:
        if self._closed:
            raise IllegalStateError(message=CLOSED_MESSAGE)

    def _native_record(self, record: ProducerRecord[Any, Any]) -> Any:
        """Serialize the record on the caller's thread and build the native
        ``_confluentkafka.ProducerRecord`` the send path reads: it holds the
        serialized ``bytes`` and the record's header values without copying
        them (CLAUDE.md §12)."""
        topic = record.topic()
        headers = record.headers()
        key = serialize(self._key_serializer, topic, record.key(), headers)
        value = serialize(self._value_serializer, topic, record.value(), headers)
        partition = record.partition()
        timestamp = record.timestamp()
        return _lib.ProducerRecord(topic, value, key,
                                   -1 if partition is None else partition,
                                   -1 if timestamp is None else timestamp,
                                   headers)

    def _track(self, future: Any) -> None:
        self._futures.add(future)
        future.add_done_callback(self._futures.discard)

    def _drain_sync(self, timeout_s: float | None = None) -> bool:
        """Wait until every record sent so far is with the Rust producer, so a
        send that returned belongs to the flush, transaction-control operation
        or close that follows (producer-transactions.md §13); at most
        ``timeout_s`` seconds when given. Returns whether they are."""
        done = threading.Event()
        if _lib.Producer_drain(self._c_producer, done.set):
            return True
        return done.wait(timeout_s)

    def _close_serializers(self) -> None:
        """Java closes both serializers at producer close; a failing
        ``close()`` is logged, never raised."""
        close_if_defined(self._key_serializer)
        close_if_defined(self._value_serializer)


def check_group_metadata(group_metadata: object) -> None:
    """Java's ``throwIfInvalidGroupMetadata`` null arm; the core checks the
    generation / member id arm."""
    if group_metadata is None:
        raise IllegalArgumentError(message=NULL_GROUP_METADATA_MESSAGE)
