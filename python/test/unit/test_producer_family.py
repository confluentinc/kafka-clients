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

"""The producer family's surface and contracts (CLAUDE.md, Python Binding
Conventions): the members Java gives each class, keyword-only parameters named
by Java, the ``java_forms`` combinations, ``async def`` iff Java waits, the send
path, the callback contract, close, and the drain that gives a returned send to
the flush or transaction-control call after it (producer-transactions.md §13).

Java's own tests are in ``test_mock_producer.py`` (``MockProducerTest``) and
``test_kafka_producer.py`` (``KafkaProducerTest``). A real ``KafkaProducer``
here talks to an unreachable bootstrap address, so its sends fail on the
metadata wait (``max.block.ms``); the broker round trip is covered by the
integration tests.
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import itertools
import logging
import math
import os
import signal
import subprocess
import sys
import threading
import time
from types import FrameType
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka._args import UNSET
from confluent_kafka.common import KafkaError, MetricName, PartitionInfo, TopicPartition
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.errors import UnsupportedVersionError
from confluent_kafka.common.serialization import bytes_serializer, string_serializer
from confluent_kafka.consumer import ConsumerGroupMetadata, KafkaConsumer, OffsetAndMetadata
from confluent_kafka.producer import (
    AsyncKafkaProducer,
    AsyncMockProducer,
    AsyncProducer,
    Callback,
    KafkaProducer,
    MockProducer,
    Partitioner,
    Producer,
    ProducerRecord,
    RecordMetadata,
)
from confluent_kafka.producer._base import NO_GROUP_METADATA_HANDLE_MESSAGE, raise_if_error

TOPIC = "topic"
RECORD = ProducerRecord(topic=TOPIC, key=b"k", value=b"v")
# Sends fail on the metadata wait after max.block.ms.
UNREACHABLE = {"bootstrap.servers": "127.0.0.1:59999", "max.block.ms": 100}
CLOSED = "Cannot perform operation after producer has been closed"
# Java's messages for a transactional method on an idempotent producer (the
# default: it has a transaction manager, TransactionManager.ensureTransactional)
# and on a producer with neither idempotence nor transactions
# (KafkaProducer.throwIfNoTransactionManager).
NOT_TRANSACTIONAL = "Transactional method invoked on a non-transactional producer."
NO_TXN = ("Cannot use transactional methods without enabling transactions by setting the "
          "transactional.id configuration property")

INTERFACE = ["init_transactions", "begin_transaction", "send_offsets_to_transaction",
             "commit_transaction", "abort_transaction", "send", "flush", "partitions_for",
             "metrics", "close"]
MOCK_OWN = ["set_init_transaction_exception", "set_begin_transaction_exception",
            "set_send_offsets_to_transaction_exception", "set_commit_transaction_exception",
            "set_abort_transaction_exception", "set_send_exception", "set_flush_exception",
            "set_partitions_for_exception", "set_close_exception", "set_mock_metrics", "closed",
            "fence_producer", "transaction_initialized", "transaction_in_flight",
            "transaction_committed", "transaction_aborted", "flushed", "sent_offsets",
            "commit_count", "history", "uncommitted_records", "consumer_group_offsets_history",
            "uncommitted_offsets", "clear", "complete_next", "error_next"]
# The entry points of these have an _async completion form in the FFI header,
# so they are async def (CLAUDE.md, Python Binding Conventions, Class family).
WAITING = {"init_transactions", "begin_transaction", "send_offsets_to_transaction",
           "commit_transaction", "abort_transaction", "send", "flush", "partitions_for",
           "close"}

MOCK_FORMS = ("MockProducer() takes one of (cluster, auto_complete, partitioner, "
              "key_serializer, value_serializer), (auto_complete, partitioner, key_serializer, "
              "value_serializer), (); got ")


def _group_metadata() -> ConsumerGroupMetadata:
    """Metadata without the core's handle, as a mock consumer's (Java's
    deprecated constructor is not offered): enough where a check fails first."""
    return ConsumerGroupMetadata._of(group_id="g", generation_id=-1, member_id="",
                                     group_instance_id=None)


def _consumer_group_metadata() -> ConsumerGroupMetadata:
    """A KafkaConsumer's metadata: it carries the core handle that
    send_offsets_to_transaction passes on, and outlives the consumer."""
    with KafkaConsumer(configs={"bootstrap.servers": "localhost:1",
                                "group.protocol": "consumer", "group.id": "g"}) as consumer:
        return consumer.group_metadata()


def _public(cls: type) -> set[str]:
    return {n for n in dir(cls) if not n.startswith("_")}


class _Cluster:
    """Stands in for Java's ``Cluster`` (a placeholder alias of ``object``)."""

    def __init__(self, partitions: list[PartitionInfo]) -> None:
        self._partitions = partitions

    def partitions_for_topic(self, topic: str) -> list[PartitionInfo]:
        return [p for p in self._partitions if p.topic() == topic]


def _partition_info(partition: int) -> PartitionInfo:
    return PartitionInfo(topic=TOPIC, partition=partition, leader=None, replicas=(),
                         in_sync_replicas=())


# ===========================================================================
# Class family and module layout
# ===========================================================================

def test_bases_are_non_instantiable() -> None:
    with pytest.raises(TypeError) as err:
        Producer()
    assert str(err.value) == ("Producer is a non-instantiable base; use KafkaProducer or "
                              "MockProducer")
    with pytest.raises(TypeError) as err:
        AsyncProducer()
    assert str(err.value) == ("AsyncProducer is a non-instantiable base; use "
                              "AsyncKafkaProducer or AsyncMockProducer")


def test_members_are_javas() -> None:
    # The interface's methods; registerMetricForSubscription,
    # unregisterMetricFromSubscription and clientInstanceId are not generated
    # (FFI entry points missing), nor the mock helpers serving only them.
    for base in (Producer, AsyncProducer):
        assert _public(base) == set(INTERFACE), base
    for kafka in (KafkaProducer, AsyncKafkaProducer):
        assert _public(kafka) == set(INTERFACE) | {"NETWORK_THREAD_PREFIX",
                                                    "PRODUCER_METRIC_GROUP_NAME"}, kafka
        assert kafka.NETWORK_THREAD_PREFIX == "kafka-producer-network-thread"
        assert kafka.PRODUCER_METRIC_GROUP_NAME == "producer-metrics"
    for mock in (MockProducer, AsyncMockProducer):
        assert _public(mock) == set(INTERFACE) | set(MOCK_OWN), mock
    assert issubclass(KafkaProducer, Producer) and issubclass(MockProducer, Producer)
    assert issubclass(AsyncKafkaProducer, AsyncProducer)
    assert issubclass(AsyncMockProducer, AsyncProducer)


def test_async_def_iff_java_waits() -> None:
    for cls in (AsyncProducer, AsyncKafkaProducer, AsyncMockProducer):
        for name in INTERFACE:
            assert inspect.iscoroutinefunction(getattr(cls, name)) == (name in WAITING), (
                cls, name)
        assert inspect.iscoroutinefunction(cls.__aenter__)
        assert inspect.iscoroutinefunction(cls.__aexit__)
    for name in MOCK_OWN:
        assert not inspect.iscoroutinefunction(getattr(AsyncMockProducer, name)), name
    for cls in (Producer, KafkaProducer, MockProducer):
        for name in INTERFACE + (MOCK_OWN if cls is MockProducer else []):
            assert not inspect.iscoroutinefunction(getattr(cls, name)), (cls, name)


def test_transaction_methods_take_no_timeout() -> None:
    for cls in (Producer, AsyncProducer):
        for name in ("init_transactions", "begin_transaction", "send_offsets_to_transaction",
                     "commit_transaction", "abort_transaction"):
            assert "timeout" not in inspect.signature(getattr(cls, name)).parameters


def test_send_callback_defaults_to_unset() -> None:
    # Java's send(record) passes null to send(record, callback) (CLAUDE.md,
    # Signatures, "'Given' means ..."): callback defaults to UNSET. Both sets
    # are Java overloads, so there is no check, and send fills an omitted one.
    for cls in (Producer, AsyncProducer, MockProducer, AsyncMockProducer):
        assert inspect.signature(cls.send).parameters["callback"].default is UNSET, cls
    p = MockProducer(auto_complete=True, partitioner=None)
    assert p.send(record=RECORD).result().offset() == 0
    assert p.send(record=RECORD, callback=None).result().offset() == 1


def test_parameter_names_and_order_are_javas() -> None:
    def names(fn: Any) -> list[str]:
        return [p.name for p in inspect.signature(fn).parameters.values()
                if p.kind is p.KEYWORD_ONLY]

    for cls in (Producer, AsyncProducer, MockProducer, AsyncMockProducer):
        assert names(cls.send_offsets_to_transaction) == ["offsets", "group_metadata"]
        assert names(cls.send) == ["record", "callback"]
        assert names(cls.partitions_for) == ["topic"]
        assert names(cls.close) == ["timeout"]
    for cls in (KafkaProducer, AsyncKafkaProducer):
        assert names(cls.__init__) == ["configs", "key_serializer", "value_serializer"]
    for cls in (MockProducer, AsyncMockProducer):
        assert names(cls.__init__) == ["cluster", "auto_complete", "partitioner",
                                       "key_serializer", "value_serializer"]
        assert names(cls.error_next) == ["e"]
        assert names(cls.set_mock_metrics) == ["name", "metric"]
        for setter in MOCK_OWN[:9]:
            field = setter[len("set_"):]
            assert names(getattr(cls, setter)) == [field], setter


def test_module_exports() -> None:
    import confluent_kafka.producer as module

    assert module.__all__ == ["AsyncKafkaProducer", "AsyncMockProducer", "AsyncProducer",
                              "Callback", "KafkaProducer", "MockProducer", "Partitioner",
                              "Producer", "ProducerRecord", "RecordMetadata",
                              "BufferExhaustedError"]
    assert not hasattr(module, "DeliveryCallback")
    assert Partitioner is object
    assert Callback.__args__[0] is RecordMetadata  # type: ignore[attr-defined]


# ===========================================================================
# Keyword-only parameters
# ===========================================================================

def test_positional_calls_are_type_errors_on_the_mock() -> None:
    p = MockProducer(auto_complete=True, partitioner=None, key_serializer=None,
                     value_serializer=None)
    calls = [
        lambda: MockProducer(None, True, None, None, None),  # type: ignore[misc]
        lambda: p.send(RECORD),  # type: ignore[misc]
        lambda: p.send_offsets_to_transaction({}, _group_metadata()),  # type: ignore[misc]
        lambda: p.partitions_for(TOPIC),  # type: ignore[misc]
        lambda: p.close(1.0),  # type: ignore[misc]
        lambda: p.error_next(IllegalStateError()),  # type: ignore[misc]
        lambda: p.set_mock_metrics(None, None),  # type: ignore[misc]
    ] + [lambda s=s: getattr(p, s)(None) for s in MOCK_OWN[:9]]
    for call in calls:
        with pytest.raises(TypeError):
            call()


def test_positional_calls_are_type_errors_on_the_kafka_producer() -> None:
    with pytest.raises(TypeError):
        KafkaProducer(UNREACHABLE)  # type: ignore[misc]
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        for call in (lambda: p.send(RECORD),  # type: ignore[misc]
                     lambda: p.send_offsets_to_transaction({}, _group_metadata()),  # type: ignore[misc]
                     lambda: p.partitions_for(TOPIC),  # type: ignore[misc]
                     lambda: p.close(1.0)):  # type: ignore[misc]
            with pytest.raises(TypeError):
                call()
    finally:
        p.close(timeout=0)


async def test_positional_calls_are_type_errors_on_the_async_classes() -> None:
    with pytest.raises(TypeError):
        AsyncKafkaProducer(UNREACHABLE)  # type: ignore[misc]
    with pytest.raises(TypeError):
        AsyncMockProducer(None, True, None, None, None)  # type: ignore[misc]
    for p in (AsyncKafkaProducer(configs=UNREACHABLE), AsyncMockProducer()):
        for coro in (lambda: p.send(RECORD),  # type: ignore[misc]
                     lambda: p.send_offsets_to_transaction({}, _group_metadata()),  # type: ignore[misc]
                     lambda: p.partitions_for(TOPIC),  # type: ignore[misc]
                     lambda: p.close(1.0)):  # type: ignore[misc]
            with pytest.raises(TypeError):
                await coro()
        await p.close(timeout=0)


# ===========================================================================
# Constructors: java_forms and defaults
# ===========================================================================

@pytest.mark.parametrize("cls", [MockProducer, AsyncMockProducer])
def test_mock_constructor_accepts_exactly_javas_overloads(cls: type) -> None:
    values = {"cluster": _Cluster([]), "auto_complete": True, "partitioner": None,
              "key_serializer": bytes_serializer(), "value_serializer": bytes_serializer()}
    rest = ["auto_complete", "partitioner", "key_serializer", "value_serializer"]
    serdes = {"key_serializer", "value_serializer"}
    prefix = MOCK_FORMS.replace("MockProducer()", f"{cls.__name__}()")
    for with_cluster in (False, True):
        for n in range(len(rest) + 1):
            for subset in itertools.combinations(rest, n):
                given = (["cluster"] if with_cluster else []) + list(subset)
                kwargs = {k: values[k] for k in given}
                # Exactly one of Java's three constructors, a serializer of the
                # first two excepted, which may be left out (Signatures).
                others = [k for k in given if k not in serdes]
                if not given or others in (["cluster", "auto_complete", "partitioner"],
                                           ["auto_complete", "partitioner"]):
                    cls(**kwargs)
                else:
                    with pytest.raises(IllegalArgumentError) as err:
                        cls(**kwargs)
                    assert str(err.value) == prefix + "(" + ", ".join(given) + ")"


def test_mock_constructor_defaults_are_javas() -> None:
    # MockProducer(): Cluster.empty(), autoComplete=false, no partitioner, and
    # *(deviation)* bytes_serializer() for the serializers.
    p = MockProducer()
    f = p.send(record=RECORD)
    assert not f.done()
    assert p.partitions_for(topic=TOPIC) == []
    assert p.complete_next()
    assert f.result().offset() == 0
    # auto_complete given as False still counts as given: the four-argument form.
    p2 = MockProducer(auto_complete=False, partitioner=None, key_serializer=None,
                      value_serializer=None)
    assert not p2.send(record=RECORD).done()


def test_kafka_producer_constructor_takes_every_combination() -> None:
    # (configs) passes null serializers to (configs, keySerializer,
    # valueSerializer), so every combination is a Java overload.
    for kwargs in ({}, {"key_serializer": bytes_serializer()},
                   {"value_serializer": bytes_serializer()},
                   {"key_serializer": bytes_serializer(), "value_serializer": bytes_serializer()},
                   {"key_serializer": None, "value_serializer": None}):
        KafkaProducer(configs=UNREACHABLE, **kwargs).close(timeout=0)


def test_kafka_producer_applies_javas_config_validators() -> None:
    # Critic 74 N8: ProducerConfig's atLeast(0) on batch.size.
    with pytest.raises(ConfigError) as err:
        KafkaProducer(configs={**UNREACHABLE, "batch.size": -1})
    assert str(err.value) == ("Invalid value -1 for configuration batch.size: "
                              "Value must be at least 0")


def test_kafka_producer_config_route_serializer() -> None:
    # Critic 74 R2-N1 / ruling 33: a serializer argument defaults to None, so a
    # serializer named by config is constructed and configured (is_key as the
    # key says); a given argument wins over the config key, which is then not
    # constructed.
    route = "test.unit.test_producer_family._UpperSerializer"
    for cls in (KafkaProducer, AsyncKafkaProducer):
        _UpperSerializer.configured.clear()
        p = cls(configs={**UNREACHABLE, "key.serializer": route, "value.serializer": route})
        try:
            native = p._native_record(ProducerRecord(topic=TOPIC, key="k", value="abc"))  # noqa: SLF001
            assert (native.key, native.value) == (b"K", b"ABC")
            assert _UpperSerializer.configured == [True, False]
        finally:
            _close_now(p)
        _UpperSerializer.configured.clear()
        p = cls(configs={**UNREACHABLE, "key.serializer": route, "value.serializer": route},
                value_serializer=string_serializer())
        try:
            native = p._native_record(ProducerRecord(topic=TOPIC, key="k", value="abc"))  # noqa: SLF001
            assert (native.key, native.value) == (b"K", b"abc")
            assert _UpperSerializer.configured == [True]
        finally:
            _close_now(p)
    _UpperSerializer.configured.clear()


def test_serializer_argument_wins_over_its_config_key() -> None:
    # Critic 74 F1 (Actor 74): the argument wins and the config key is ignored,
    # not even parsed: Java's ProducerConfig.appendSerializerToConfig replaces
    # the key with the argument's class before ConfigDef parses it
    # (ProducerConfig.java:666).
    class NotAClass:
        pass

    serializer = string_serializer()
    for cls in (KafkaProducer, AsyncKafkaProducer):
        p = cls(configs={**UNREACHABLE, "key.serializer": NotAClass(), "value.serializer": 5},
                key_serializer=serializer, value_serializer=serializer)
        _close_now(p)


def _close_now(p: KafkaProducer[Any, Any] | AsyncKafkaProducer[Any, Any]) -> None:
    if isinstance(p, AsyncKafkaProducer):
        asyncio.run(p.close(timeout=0))
    else:
        p.close(timeout=0)


class _UpperSerializer:
    configured: list[bool] = []

    def configure(self, configs: dict[str, Any], is_key: bool) -> None:
        _UpperSerializer.configured.append(is_key)

    def __call__(self, topic: str, value: str | None, headers: Any = None) -> bytes | None:
        return None if value is None else value.upper().encode()


# ===========================================================================
# The send path
# ===========================================================================

def _native(record: ProducerRecord[Any, Any], **kwargs: Any) -> Any:
    p = KafkaProducer(configs=UNREACHABLE, **kwargs)
    try:
        return p._native_record(record)  # noqa: SLF001 - the struct the FFI reads
    finally:
        p.close(timeout=0)


def test_headers_reach_the_ffi_struct() -> None:
    native = _native(ProducerRecord(topic=TOPIC, partition=None, key=b"k", value=b"v",
                                    headers=[("trace-id", b"abc"), ("null-header", None)]))
    assert native.headers == [("trace-id", b"abc"), ("null-header", None)]
    assert _native(ProducerRecord(topic=TOPIC, key=b"k", value=b"v")).headers == []


def _utf8_address(text: str) -> int:
    """The address of ``text``'s cached UTF-8 buffer (PyUnicode_AsUTF8AndSize)."""
    import ctypes

    as_utf8 = ctypes.pythonapi.PyUnicode_AsUTF8AndSize
    as_utf8.restype = ctypes.c_void_p
    as_utf8.argtypes = [ctypes.py_object, ctypes.c_void_p]
    address = as_utf8(text, None)
    assert address
    return int(address)


def _bytes_address(data: bytes) -> int:
    import ctypes

    address = ctypes.cast(ctypes.c_char_p(data), ctypes.c_void_p).value
    assert address
    return int(address)


def test_the_send_path_copies_no_topic_key_value_or_header_bytes() -> None:
    # DoD #10 / CLAUDE.md §11/§12: the FFI struct the send path hands to
    # kafka_producer_Producer_send_batch points into the Python objects' own
    # buffers: the topic and header keys into their str's cached UTF-8, the key,
    # the value and the header values into the serialized bytes.
    topic = "".join(["top", "ic-", "zero-copy"])  # a str not interned elsewhere
    key, value = b"the-key", b"the-value"
    header_values = [b"v1", b"v2"]
    record = ProducerRecord(topic=topic, partition=None, key=key, value=value,
                            headers=[("h-one", header_values[0]), ("h-two", header_values[1]),
                                     ("h-null", None)])
    native = _native(record)
    topic_address, key_address, value_address, headers = native.ffi_addresses
    assert native.topic is record.topic()
    assert topic_address == _utf8_address(record.topic())
    assert (key_address, value_address) == (_bytes_address(key), _bytes_address(value))
    expected = [(_utf8_address(k), 0 if v is None else _bytes_address(original))
                for (k, v), original in zip(record.headers(), [*header_values, None])]
    assert headers == expected


def test_the_native_record_holds_what_it_points_into() -> None:
    # The native record keeps the topic, the header tuple (so the key strs) and
    # the header value exports alive; a header list the caller changes later
    # does not reach it.
    import gc

    headers = [("".join(["k", str(i)]), bytes([65 + i]) * 3) for i in range(3)]
    native = _lib.ProducerRecord("".join(["t", "o", "p"]), b"v", None, -1, -1, headers)
    headers[0] = ("changed", b"zzz")
    del headers
    gc.collect()
    assert native.topic == "top"
    assert native.headers == [("k0", b"AAA"), ("k1", b"BBB"), ("k2", b"CCC")]
    with pytest.raises(ValueError, match="embedded null character"):
        _lib.ProducerRecord("to\0pic", b"v")
    with pytest.raises(ValueError, match="embedded null character"):
        _lib.ProducerRecord("t", b"v", None, -1, -1, (("k\0", b"v"),))
    with pytest.raises(TypeError):
        _lib.ProducerRecord("t", b"v", None, -1, -1, ((1, b"v"),))


def test_null_value_is_a_tombstone_and_empty_bytes_is_not() -> None:
    native = _native(ProducerRecord(topic=TOPIC, key=b"k", value=None))
    assert native.value is None and native.value_len == -1
    native = _native(ProducerRecord(topic=TOPIC, key=b"k", value=b""))
    assert native.value == b"" and native.value_len == 0


def test_serializer_is_called_on_a_null_value_with_the_headers() -> None:
    # Java's doSend serializes a null value too; the serializer decides what
    # null maps to.
    calls: list[tuple[str, object, object]] = []

    def serializer(topic: str, value: str | None, headers: Any = None) -> bytes | None:
        calls.append((topic, value, headers))
        return b"<null>" if value is None else value.encode()

    record = ProducerRecord(topic=TOPIC, partition=None, key=None, value=None,
                            headers=[("h", b"1")])
    native = _native(record, key_serializer=serializer, value_serializer=serializer)
    assert native.value == b"<null>" and native.key == b"<null>"
    assert calls == [(TOPIC, None, record.headers()), (TOPIC, None, record.headers())]


def test_serializer_must_return_bytes() -> None:
    with pytest.raises(TypeError) as err:
        _native(ProducerRecord(topic=TOPIC, value=b"v"),
                value_serializer=lambda topic, value, headers=None: "text")
    assert str(err.value) == "a serializer must return bytes or None, not str"


def test_send_failure_metadata_callback_and_future() -> None:
    # Java's AppendCallbacks hands the callback RecordMetadata(tp, -1, -1,
    # NO_TIMESTAMP, -1, -1), never null; the callback runs off the caller's
    # thread and before the future completes; the future cannot be cancelled.
    caller = threading.get_ident()
    seen: list[tuple[int, bool, RecordMetadata, Exception | None]] = []
    done = threading.Event()
    holder: dict[str, Any] = {}

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        seen.append((threading.get_ident(), holder["f"].done(), md, e))
        done.set()

    with KafkaProducer(configs=UNREACHABLE) as p:
        holder["f"] = f = p.send(record=ProducerRecord(topic=TOPIC, partition=3, key=None,
                                                       value=b"v"),
                                 callback=callback)
        assert not f.cancel()
        assert done.wait(30)
        with pytest.raises(KafkaTimeoutError) as err:
            f.result(timeout=30)
    ((thread, future_done, md, e),) = seen
    assert thread != caller
    assert not future_done
    assert e is err.value
    assert str(e) == "Topic topic not present in metadata after 100 ms."
    assert (md.topic(), md.partition(), md.offset(), md.timestamp(),
            md.serialized_key_size(), md.serialized_value_size()) == (TOPIC, 3, -1, -1, -1, -1)
    assert not md.has_offset() and not md.has_timestamp()


@pytest.mark.parametrize("cls", [KafkaProducer, AsyncKafkaProducer])
def test_a_transactional_send_waits_for_its_handover_and_keeps_api_errors_in_the_future(
        cls: type[KafkaProducer[Any, Any]] | type[AsyncKafkaProducer[Any, Any]]) -> None:
    # Critic 75 F2: with a transactional.id, send() returns once the record is
    # with the Rust producer, so it can raise what Java's doSend rethrows (the
    # broker tests in test/integration). The metadata wait's TimeoutError is an
    # ApiException: Java gives it to the callback and returns a failed future
    # (KafkaProducer.java:1056-1068), after send() waited max.block.ms.
    configs = {**UNREACHABLE, "max.block.ms": 300, "transactional.id": "txn"}
    seen: list[Exception | None] = []

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        seen.append(e)

    async def send_async() -> tuple[float, BaseException | None]:
        p = AsyncKafkaProducer(configs=configs)
        try:
            start = time.monotonic()
            f = await p.send(record=RECORD, callback=callback)
            elapsed = time.monotonic() - start
            await asyncio.wait([f], timeout=30)
            return elapsed, f.exception()
        finally:
            await p.close(timeout=0)

    if cls is AsyncKafkaProducer:
        elapsed, error = asyncio.run(send_async())
    else:
        p = KafkaProducer(configs=configs)
        try:
            start = time.monotonic()
            f = p.send(record=RECORD, callback=callback)
            elapsed = time.monotonic() - start
            error = f.exception(timeout=30)
        finally:
            p.close(timeout=0)
    assert elapsed >= 0.3, elapsed
    assert type(error) is KafkaTimeoutError
    assert str(error) == "Topic topic not present in metadata after 300 ms."
    assert seen == [error]


_NOT_IN_A_TRANSACTION = "Cannot add partition topic-0 to transaction while in state  READY"


def _fake_queued_send(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fakes the queued send of a record not handed over yet: the delivery
    callback and the handover callback send() passed are kept."""
    captured: dict[str, Any] = {}

    def producer_send_cb(c_producer: int, native: Any, on_completion: Any, resolve: Any) -> int:
        captured["on_completion"] = on_completion
        captured["resolve"] = resolve
        return 0

    monkeypatch.setattr(_lib, "Producer_send_cb", producer_send_cb)
    return captured


def _hand_over(captured: dict[str, Any], rethrown: bool) -> None:
    """The record reaches the core: with ``rethrown`` its send() returns the
    error (an OWNED handle, as the extension passes it), else it is accepted
    and then fails its delivery (a BORROWED handle, freed after the call)."""
    error = _lib.KafkaError_new(IllegalStateError._ffi_id, _NOT_IN_A_TRANSACTION)
    if rethrown:
        captured["resolve"](error)
        return
    captured["resolve"](0)
    try:
        # Callback id 0: the faked send registered no report with the core.
        captured["on_completion"](None, error, 0)
    finally:
        _lib.KafkaError_destroy(error)


@pytest.mark.parametrize("rethrown", [False, True])
def test_ctrl_c_while_a_transactional_send_waits_for_its_handover_raises_after_it(
        monkeypatch: pytest.MonkeyPatch, rethrown: bool) -> None:
    # Updated for the branch C API: send() no longer waits for buffer space
    # (backpressure is the core's max.block.ms, a deviation), and a sync wait
    # lets the operation finish on Ctrl+C, raising KeyboardInterrupt after it.
    # With no transaction started, send() waits for the handover to raise what
    # Java's doSend rethrows. A Ctrl+C meanwhile is raised once the record is
    # handed over: a rethrown error then reaches neither the callback nor the
    # future (as in Java; the callback runs only if the record completes later,
    # see test_a_rethrown_send_runs_its_callback_once_if_the_record_completes),
    # an accepted record's later failure reaches both, and either way flush()
    # has nothing left to wait for (Copilot review, PR #187).
    p = KafkaProducer(configs={**UNREACHABLE, "transactional.id": "txn"})
    captured = _fake_queued_send(monkeypatch)
    seen: list[Exception | None] = []
    called = threading.Event()

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        seen.append(e)
        called.set()

    armed = True
    handed_over_at: list[float] = []

    def on_sigint(signum: int, frame: FrameType | None) -> None:
        if armed:
            raise KeyboardInterrupt

    def hand_over() -> None:
        handed_over_at.append(time.monotonic())
        _hand_over(captured, rethrown)

    previous = signal.signal(signal.SIGINT, on_sigint)
    interrupt = threading.Timer(0.2, os.kill, (os.getpid(), signal.SIGINT))
    handover = threading.Timer(0.5, hand_over)
    try:
        interrupt.start()
        handover.start()
        with pytest.raises(KeyboardInterrupt):
            p.send(record=RECORD, callback=callback)
        raised_at = time.monotonic()
    finally:
        interrupt.cancel()
        handover.cancel()
        interrupt.join()
        handover.join()
        armed = False
        time.sleep(0.05)  # a signal still pending runs the disarmed handler
        signal.signal(signal.SIGINT, previous)
    # The interrupt was raised once the send's wait ended, not at the signal.
    assert handed_over_at and raised_at >= handed_over_at[0]
    monkeypatch.undo()
    try:
        if rethrown:
            assert not called.wait(0.2)
        else:
            assert called.wait(5)
            assert type(seen[0]) is IllegalStateError
            assert str(seen[0]) == _NOT_IN_A_TRANSACTION
        assert not p._futures  # noqa: SLF001
        flush = threading.Thread(target=p.flush, daemon=True)
        flush.start()
        flush.join(5)
        assert not flush.is_alive(), "flush() waits for the interrupted send"
    finally:
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        p.close(timeout=0)


@pytest.mark.parametrize("rethrown", [False, True])
async def test_cancelling_a_transactional_send_waiting_for_its_handover_raises_after_it(
        monkeypatch: pytest.MonkeyPatch, rethrown: bool) -> None:
    # The async peer of the test above (updated the same way): cancelling the
    # task awaiting send() lets the wait end, then raises CancelledError.
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "transactional.id": "txn"})
    captured = _fake_queued_send(monkeypatch)
    seen: list[Exception | None] = []

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        seen.append(e)

    task = asyncio.ensure_future(p.send(record=RECORD, callback=callback))
    while "resolve" not in captured:
        await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.sleep(0.2)
    assert not task.done()  # the cancellation waits for the handover
    _hand_over(captured, rethrown)
    with pytest.raises(asyncio.CancelledError):
        await task
    monkeypatch.undo()
    try:
        if rethrown:
            await asyncio.sleep(0.2)
            assert seen == []
        else:
            for _ in range(500):
                if seen:
                    break
                await asyncio.sleep(0.01)
            assert type(seen[0]) is IllegalStateError
            assert str(seen[0]) == _NOT_IN_A_TRANSACTION
        assert not p._futures  # noqa: SLF001
        await asyncio.wait_for(p.flush(), 5)
    finally:
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        await p.close(timeout=0)


# A later completion of a record whose send() raised: Java's doSend appends the
# record, its appendCallbacks carrying the Callback, before maybeAddPartition
# throws (KafkaProducer.java:1027-1039), so the record completes with its batch
# and its Callback runs then, with the batch's outcome: the metadata of a
# committed record, or the error that aborted it, with the -1 metadata of
# KafkaProducer.AppendCallbacks.onCompletion (:1589-1592).
_LATE_OUTCOMES = {
    "committed": (("topic", 0, 7, 1234, 1, 1), None),
    "aborted": (None, "Producer is closed forcefully."),
}


def _complete_late(captured: dict[str, Any], outcome: str) -> None:
    """The core fires the record's delivery callback (a BORROWED error handle,
    freed after the call; callback id 0, as the faked send registered no report
    with the core)."""
    metadata, message = _LATE_OUTCOMES[outcome]
    error = 0 if message is None else _lib.KafkaError_new(KafkaError._ffi_id, message)
    try:
        captured["on_completion"](metadata, error, 0)
    finally:
        if error:
            _lib.KafkaError_destroy(error)


def _check_late(seen: list[tuple[int, Exception | None]], outcome: str) -> None:
    if outcome == "committed":
        assert seen == [(7, None)]
    else:
        assert len(seen) == 1
        offset, error = seen[0]
        assert offset == -1
        assert type(error) is KafkaError
        assert str(error) == "Producer is closed forcefully."


@pytest.mark.parametrize("outcome", sorted(_LATE_OUTCOMES))
def test_a_rethrown_send_runs_its_callback_once_if_the_record_completes(
        monkeypatch: pytest.MonkeyPatch, outcome: str) -> None:
    # send() raises the handover's error and returns no future; flush() does
    # not wait for the record, whose callback may never run (a record rejected
    # before the append). When the record does complete, its callback runs,
    # once, and the internal future is set once (a second set would raise
    # InvalidStateError from on_completion).
    p = KafkaProducer(configs={**UNREACHABLE, "transactional.id": "txn"})
    captured = _fake_queued_send(monkeypatch)
    seen: list[tuple[int, Exception | None]] = []

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        seen.append((md.offset(), e))

    def hand_over() -> None:
        while "resolve" not in captured:
            time.sleep(0.01)
        _hand_over(captured, rethrown=True)

    handover = threading.Thread(target=hand_over, daemon=True)
    handover.start()
    try:
        with pytest.raises(IllegalStateError) as raised:
            p.send(record=RECORD, callback=callback)
        assert str(raised.value) == _NOT_IN_A_TRANSACTION
        handover.join(5)
        monkeypatch.undo()
        assert seen == []
        assert not p._futures  # noqa: SLF001
        flush = threading.Thread(target=p.flush, daemon=True)
        flush.start()
        flush.join(5)
        assert not flush.is_alive(), "flush() waits for the rethrown send"

        _complete_late(captured, outcome)
        _check_late(seen, outcome)
        _complete_late(captured, outcome)  # never twice
        _check_late(seen, outcome)
    finally:
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        p.close(timeout=0)


@pytest.mark.parametrize("outcome", sorted(_LATE_OUTCOMES))
async def test_a_rethrown_async_send_runs_its_callback_once_if_the_record_completes(
        monkeypatch: pytest.MonkeyPatch, outcome: str) -> None:
    # The async peer of the test above. The future send() did not return is
    # cancelled, so a later failure is not logged as never retrieved.
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "transactional.id": "txn"})
    captured = _fake_queued_send(monkeypatch)
    seen: list[tuple[int, Exception | None]] = []

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        seen.append((md.offset(), e))

    try:
        task = asyncio.ensure_future(p.send(record=RECORD, callback=callback))
        while "resolve" not in captured:
            await asyncio.sleep(0.01)
        _hand_over(captured, rethrown=True)
        with pytest.raises(IllegalStateError) as raised:
            await task
        assert str(raised.value) == _NOT_IN_A_TRANSACTION
        monkeypatch.undo()
        assert seen == []
        assert not p._futures  # noqa: SLF001
        await asyncio.wait_for(p.flush(), 5)

        for _ in range(2):  # never twice
            _complete_late(captured, outcome)
            await asyncio.sleep(0.05)
            _check_late(seen, outcome)
        # Drop every reference to send()'s frame (the raised error's traceback
        # holds it), so an unretrieved error would be logged now.
        del task, raised
        captured.clear()
        gc.collect()
        await asyncio.sleep(0)
        assert loop_errors == []
    finally:
        loop.set_exception_handler(previous_handler)
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        await p.close(timeout=0)


def _core_callback_errors(capfd: pytest.CaptureFixture[str]) -> list[str]:
    """The core's log lines for a raising send() callback, from stderr, the
    record prefix of its default log backend left out."""
    return [line.split("] ", 1)[1] for line in capfd.readouterr().err.splitlines()
            if "Error executing user-provided callback" in line]


def test_callbacks_run_in_completion_order_and_a_raising_one_is_reported(
        caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str]) -> None:
    # Java logs a throwing onCompletion with its exception and fails nothing
    # (ProducerBatch.completeFutureAndFireCallbacks, ProducerBatch.java:318-320).
    # Python reports it to the core, which logs it once with Java's message and
    # the exception's text; Python logs nothing.
    order: list[int] = []
    all_done = threading.Event()

    def make(i: int) -> Callback:
        def cb(md: RecordMetadata, e: Exception | None) -> None:
            order.append(i)
            if i == 3:
                all_done.set()
            if i == 1:
                raise RuntimeError("callback blew up")
        return cb

    capfd.readouterr()
    with caplog.at_level(logging.DEBUG, logger="confluent_kafka"):
        with KafkaProducer(configs=UNREACHABLE) as p:
            futures = [p.send(record=RECORD, callback=make(i)) for i in range(4)]
            assert all_done.wait(30)
            for f in futures:
                assert isinstance(f.exception(timeout=30), KafkaTimeoutError)
    assert order == [0, 1, 2, 3]
    assert _core_callback_errors(capfd) == [f"{CALLBACK_ERROR}: LocalCallbackError: callback blew up"]
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []


@pytest.mark.parametrize("asynchronous", [False, True])
async def test_a_raising_callback_of_a_record_rejected_before_the_accumulator_is_logged_once(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
        capfd: pytest.CaptureFixture[str], asynchronous: bool) -> None:
    # A record the Rust send() rejects before the accumulator has no delivery
    # callback, so Python completes it itself and its callback's exception has
    # no core report to go to: Python logs it, once, with the message the core
    # logs a reported one with.
    p: Any = AsyncKafkaProducer(configs=UNREACHABLE) if asynchronous else KafkaProducer(
        configs=UNREACHABLE)
    captured = _fake_queued_send(monkeypatch)
    raised = RuntimeError("callback blew up")

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        raise raised

    capfd.readouterr()
    try:
        with caplog.at_level(logging.ERROR, logger="confluent_kafka.producer"):
            f = (await p.send(record=RECORD, callback=callback) if asynchronous
                 else p.send(record=RECORD, callback=callback))
            captured["resolve"](_lib.KafkaError_new(IllegalStateError._ffi_id,
                                                    _NOT_IN_A_TRANSACTION))
            if asynchronous:
                with pytest.raises(IllegalStateError):
                    await asyncio.wait_for(f, 5)
            else:
                assert type(f.exception(timeout=5)) is IllegalStateError
    finally:
        monkeypatch.undo()
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        if asynchronous:
            await p.close(timeout=0)
        else:
            p.close(timeout=0)
    records = [r for r in caplog.records if r.name == "confluent_kafka.producer"]
    assert [r.getMessage() for r in records] == [CALLBACK_ERROR]
    assert records[0].exc_info is not None
    assert records[0].exc_info[1] is raised
    assert _core_callback_errors(capfd) == []


def test_flush_waits_for_the_records_sent_before_it() -> None:
    # Java's flush returns once the records sent before it have completed
    # (their callbacks run first).
    with KafkaProducer(configs=UNREACHABLE) as p:
        seen: list[int] = []
        futures = [p.send(record=RECORD, callback=lambda md, e, i=i: seen.append(i))
                   for i in range(3)]
        p.flush()
        assert all(f.done() for f in futures)
        assert seen == [0, 1, 2]


def test_flush_in_a_callback_raises_and_close_in_a_callback_does_not_deadlock(
        caplog: pytest.LogCaptureFixture) -> None:
    p = KafkaProducer(configs=UNREACHABLE)
    out: dict[str, Any] = {}
    done = threading.Event()

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        try:
            p.flush()
        except KafkaError as error:
            out["flush"] = error
        p.close()  # Java: close(0) from the I/O thread, which must not join itself
        done.set()

    with caplog.at_level(logging.WARNING, logger="confluent_kafka.producer"):
        p.send(record=RECORD, callback=callback)
        assert done.wait(30)
    assert str(out["flush"]) == ("KafkaProducer.flush() invocation inside a callback is not "
                                 "permitted because it may lead to deadlock.")
    assert any("Overriding close timeout 9223372036854775807 ms to 0 ms" in r.getMessage()
               for r in caplog.records)
    with pytest.raises(IllegalStateError):
        p.send(record=RECORD)


def test_flush_in_a_callback_of_a_closed_producer_reports_the_closed_state() -> None:
    # Order (CLAUDE.md, Python Binding Conventions, Implementation over the
    # FFI): the closed check comes first on an FFI-backed class, then Java's
    # in-callback check (KafkaProducer.java:1217-1219).
    p = KafkaProducer(configs=UNREACHABLE)
    out: dict[str, Any] = {}
    done = threading.Event()

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        p.close()
        try:
            p.flush()
        except Exception as error:  # noqa: BLE001 - the test reads which one
            out["flush"] = error
        done.set()

    p.send(record=RECORD, callback=callback)
    assert done.wait(30)
    assert type(out["flush"]) is IllegalStateError and str(out["flush"]) == CLOSED


# ===========================================================================
# Close
# ===========================================================================

def test_close_waits_for_in_flight_sends_and_does_not_cancel_them() -> None:
    # Carried into P4: close() used to cancel the in-flight futures
    # (CancelledError, returning at once); Java's close() waits for them.
    p = KafkaProducer(configs=UNREACHABLE)
    futures = [p.send(record=RECORD) for _ in range(3)]
    p.close()
    for f in futures:
        assert f.done() and not f.cancelled()
        assert isinstance(f.exception(), KafkaTimeoutError)


def test_timed_close_is_bounded_and_fails_what_it_could_not_send() -> None:
    # Updated for the branch C API: it has no test pause; the 60 s max.block.ms
    # alone keeps the first record waiting for metadata.
    p = KafkaProducer(configs={"bootstrap.servers": "127.0.0.1:59999"})  # max.block.ms 60 s
    futures = [p.send(record=RECORD) for _ in range(50)]
    start = time.monotonic()
    p.close(timeout=1.0)
    assert time.monotonic() - start < 20
    # The record waiting for metadata fails with the close; the ones after it as
    # sent after close.
    errors = [f.exception() for f in futures]
    assert all(f.done() and not f.cancelled() for f in futures)
    assert isinstance(errors[0], KafkaError)
    assert all(isinstance(e, (KafkaError, IllegalStateError)) for e in errors)
    assert str(errors[-1]) == CLOSED


def test_close_is_idempotent_then_every_call_raises() -> None:
    p = KafkaProducer(configs=UNREACHABLE)
    p.close()
    p.close()
    calls = [lambda: p.send(record=RECORD), p.flush, p.init_transactions,
             p.begin_transaction, p.commit_transaction, p.abort_transaction, p.metrics,
             lambda: p.partitions_for(topic=TOPIC),
             lambda: p.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata())]
    for call in calls:
        with pytest.raises(IllegalStateError) as err:
            call()
        assert str(err.value) == CLOSED


def test_a_send_racing_close_is_refused_after_its_serializer() -> None:
    # Critic 75 B1: a send whose serializer is still running when close()
    # frees the handle must not use it afterwards; the record is refused as
    # Java's RecordAccumulator.append refuses one after close.
    started = threading.Event()

    def slow(topic: str, value: bytes | None, headers: Any = None) -> bytes | None:
        started.set()
        time.sleep(0.5)
        return value

    p = KafkaProducer(configs=UNREACHABLE, value_serializer=slow)
    raised: list[BaseException] = []

    def send() -> None:
        try:
            p.send(record=RECORD)
        except BaseException as error:  # noqa: BLE001
            raised.append(error)

    t = threading.Thread(target=send)
    t.start()
    assert started.wait(10)
    p.close(timeout=0)
    t.join(10)
    ((error,),) = (raised,)
    assert type(error) is KafkaError
    assert str(error) == "Producer closed while send in progress"


def test_calls_racing_close_never_touch_a_freed_handle() -> None:
    # Every native call is a use the close waits for; after it, the calls see
    # the closed producer.
    stop = threading.Event()
    outcomes: set[str] = set()

    for _ in range(5):
        p = KafkaProducer(configs=UNREACHABLE)
        stop.clear()

        def hammer() -> None:
            while not stop.is_set():
                try:
                    p.metrics()
                    outcomes.add("metrics")
                except IllegalStateError as error:
                    assert str(error) == CLOSED
                    outcomes.add("closed")
                    return

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for t in threads:
            t.start()
        time.sleep(0.05)
        p.close(timeout=0)
        stop.set()
        for t in threads:
            t.join(10)
    assert outcomes == {"metrics", "closed"}


def _guard_freed_handles(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Wrap every ``_lib.Producer_*`` entry point: a call with a handle that
    ``Producer_destroy`` has freed is recorded (by name) and not run, so a
    use-after-free fails the test instead of reading freed memory."""
    freed: set[int] = set()
    violations: list[str] = []
    for name in dir(_lib):
        if not name.startswith("Producer_"):
            continue
        real = getattr(_lib, name)

        def guarded(*args: Any, _name: str = name, _real: Any = real) -> Any:
            if args and isinstance(args[0], int) and args[0] in freed:
                violations.append(_name)
                raise RuntimeError(f"{_name} called with a freed producer handle")
            result = _real(*args)
            if _name == "Producer_destroy":
                freed.add(args[0])
            return result

        monkeypatch.setattr(_lib, name, guarded)
    return violations


# Every method that checks for a closed producer, then calls into the native
# one (the drain of flush and the four waiting transaction ops included).
CALLS_AFTER_THE_CLOSED_CHECK: dict[str, dict[str, Any]] = {
    "flush": {},
    "init_transactions": {},
    "begin_transaction": {},
    "send_offsets_to_transaction": {"offsets": {}, "group_metadata": None},
    "commit_transaction": {},
    "abort_transaction": {},
    "partitions_for": {"topic": TOPIC},
    "metrics": {},
}


def _arguments(operation: str) -> dict[str, Any]:
    arguments = dict(CALLS_AFTER_THE_CLOSED_CHECK[operation])
    if "group_metadata" in arguments:
        arguments["group_metadata"] = _consumer_group_metadata()
    return arguments


@pytest.mark.parametrize("operation", CALLS_AFTER_THE_CLOSED_CHECK)
def test_a_call_that_passed_its_closed_check_before_close_never_touches_the_freed_handle(
        operation: str, monkeypatch: pytest.MonkeyPatch) -> None:
    # Critic 75 R2-B1: a call that passed its closed check just before a
    # concurrent close() must still not use the handle close() frees (the
    # drain took close()'s owner path whenever _closed was set). The whole
    # close runs right after the call's closed check passes.
    violations = _guard_freed_handles(monkeypatch)
    p = KafkaProducer(configs=UNREACHABLE)
    passed = p._check_not_closed  # noqa: SLF001

    def check_then_close() -> None:
        passed()
        p._check_not_closed = passed  # type: ignore[method-assign]  # noqa: SLF001
        p.close(timeout=0)

    p._check_not_closed = check_then_close  # type: ignore[method-assign]  # noqa: SLF001
    with pytest.raises(IllegalStateError) as err:
        getattr(p, operation)(**_arguments(operation))
    assert str(err.value) == CLOSED
    assert violations == []


@pytest.mark.parametrize("operation", CALLS_AFTER_THE_CLOSED_CHECK)
async def test_an_async_call_that_passed_its_closed_check_before_close_never_touches_the_freed_handle(  # noqa: E501
        operation: str, monkeypatch: pytest.MonkeyPatch) -> None:
    violations = _guard_freed_handles(monkeypatch)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    passed = p._check_not_closed  # noqa: SLF001

    def check_then_close() -> None:
        # The close runs to its end, on its own loop, before the call goes on.
        passed()
        p._check_not_closed = passed  # type: ignore[method-assign]  # noqa: SLF001
        closer = threading.Thread(target=lambda: asyncio.run(p.close(timeout=0)))
        closer.start()
        closer.join(30)
        assert not closer.is_alive()

    p._check_not_closed = check_then_close  # type: ignore[method-assign]  # noqa: SLF001
    with pytest.raises(IllegalStateError) as err:
        result = getattr(p, operation)(**_arguments(operation))
        if inspect.isawaitable(result):
            await result
    assert str(err.value) == CLOSED
    assert violations == []


def _count_teardowns(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[int]]:
    """Count the close entry points (Producer_close_cb /
    Producer_close_with_timeout_cb) and Producer_destroy per handle; a second
    destroy of one handle is counted, not run (it would free the struct twice).
    Updated for the branch C API, which has no Producer_shutdown: the close
    entry point is the first step of a teardown."""
    calls: dict[str, list[int]] = {"close": [], "destroy": []}
    real_close = _lib.Producer_close_cb
    real_close_with_timeout = _lib.Producer_close_with_timeout_cb
    real_destroy = _lib.Producer_destroy

    def close(c_producer: int, cb: Any) -> None:
        calls["close"].append(c_producer)
        real_close(c_producer, cb)

    def close_with_timeout(c_producer: int, timeout_ms: int, cb: Any) -> None:
        calls["close"].append(c_producer)
        real_close_with_timeout(c_producer, timeout_ms, cb)

    def destroy(c_producer: int) -> None:
        calls["destroy"].append(c_producer)
        if calls["destroy"].count(c_producer) == 1:
            real_destroy(c_producer)

    monkeypatch.setattr(_lib, "Producer_close_cb", close)
    monkeypatch.setattr(_lib, "Producer_close_with_timeout_cb", close_with_timeout)
    monkeypatch.setattr(_lib, "Producer_destroy", destroy)
    return calls


def _close_from_two_threads(p: KafkaProducer[Any, Any]) -> None:
    barrier = threading.Barrier(2)

    def close() -> None:
        barrier.wait()
        p.close(timeout=0)

    threads = [threading.Thread(target=close) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)


def test_concurrent_closes_tear_down_once(monkeypatch: pytest.MonkeyPatch) -> None:
    # Critic 75 B1: close() checks and sets the closed flag at once, so two
    # threads closing together tear the producer down once. Deterministic: the
    # in_callback() check that followed the old check-then-set now sleeps, so
    # both closes would reach the teardown if the flag were set after it.
    import confluent_kafka.producer.producer as module

    calls = _count_teardowns(monkeypatch)

    def slow_in_callback(producer: object) -> bool:
        time.sleep(0.2)
        return False

    monkeypatch.setattr(module, "in_callback", slow_in_callback)
    p = KafkaProducer(configs=UNREACHABLE)
    _close_from_two_threads(p)
    assert calls == {"close": [p._c_producer], "destroy": [p._c_producer]}  # noqa: SLF001


def test_concurrent_closes_tear_down_once_at_a_short_switch_interval(
        monkeypatch: pytest.MonkeyPatch) -> None:
    import sys

    calls = _count_teardowns(monkeypatch)
    previous = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for _ in range(30):
            calls["close"].clear()
            calls["destroy"].clear()
            p = KafkaProducer(configs=UNREACHABLE)
            _close_from_two_threads(p)
            assert len(calls["close"]) == 1 and len(calls["destroy"]) == 1
    finally:
        sys.setswitchinterval(previous)


def test_concurrent_async_closes_tear_down_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _count_teardowns(monkeypatch)

    async def main() -> int:
        p = AsyncKafkaProducer(configs=UNREACHABLE)
        await asyncio.gather(p.close(timeout=0), p.close(timeout=0))
        c_producer: int = p._c_producer  # noqa: SLF001
        return c_producer

    c_producer = asyncio.run(main())
    assert calls == {"close": [c_producer], "destroy": [c_producer]}


def test_an_async_send_racing_close_is_refused_after_its_serializer() -> None:
    started = threading.Event()

    def slow(topic: str, value: bytes | None, headers: Any = None) -> bytes | None:
        started.set()
        time.sleep(0.5)
        return value

    async def main() -> BaseException:
        p = AsyncKafkaProducer(configs=UNREACHABLE, value_serializer=slow)
        loop = asyncio.get_running_loop()
        send = loop.run_in_executor(None, lambda: asyncio.run(p.send(record=RECORD)))
        await loop.run_in_executor(None, started.wait, 10)
        await p.close(timeout=0)
        try:
            await send
        except BaseException as error:  # noqa: BLE001
            return error
        raise AssertionError("send() returned")

    error = asyncio.run(main())
    assert type(error) is KafkaError
    assert str(error) == "Producer closed while send in progress"


def test_a_cancelled_async_close_is_forced_and_raises_promptly() -> None:
    # Critic 75 F1, the async analog of KafkaProducerTest's
    # shouldCloseProperlyAndThrowIfInterrupted (see test_kafka_producer.py):
    # cancelling the task awaiting close() force-closes the producer
    # (KafkaProducer.java:1419-1437) and re-raises CancelledError, instead of
    # waiting for the pending record's max.block.ms.
    async def main() -> None:
        p = AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 60000})
        future = await p.send(record=RECORD)
        task = asyncio.ensure_future(p.close())
        await asyncio.sleep(0.1)
        assert not task.done()
        loop = asyncio.get_running_loop()
        start = loop.time()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert loop.time() - start < 10
        assert future.done()
        assert isinstance(future.exception(), KafkaError)
        with pytest.raises(IllegalStateError) as err:
            await p.send(record=RECORD)
        assert str(err.value) == CLOSED
        await p.close()  # closing again is harmless

    asyncio.run(main())


def test_close_rejects_a_negative_timeout_and_stays_open() -> None:
    p = KafkaProducer(configs=UNREACHABLE)
    with pytest.raises(IllegalArgumentError) as err:
        p.close(timeout=-1)
    assert str(err.value) == "The timeout cannot be negative."
    assert p.metrics() is not None
    p.close(timeout=0)


# A close timeout beyond Long.MAX_VALUE milliseconds (CLAUDE.md, Python Binding
# Conventions, Signatures, Timeouts): OverflowError, and NaN ValueError, before
# the FFI, so before close() marks the producer closed.
TOO_LONG = f"timeout of {math.floor(1e300 * 1000.0)} ms does not fit a signed 64-bit integer"


def test_close_rejects_a_timeout_beyond_a_long_and_stays_open() -> None:
    p = KafkaProducer(configs=UNREACHABLE)
    with pytest.raises(OverflowError) as err:
        p.close(timeout=1e300)
    assert str(err.value) == TOO_LONG
    with pytest.raises(OverflowError):
        p.close(timeout=float("inf"))
    with pytest.raises(ValueError):
        p.close(timeout=float("nan"))
    # Still open: a send and a flush work, and a plain close() closes it.
    f = p.send(record=RECORD)
    p.flush()
    assert isinstance(f.exception(), KafkaTimeoutError)
    p.close()
    with pytest.raises(IllegalStateError) as err:
        p.send(record=RECORD)
    assert str(err.value) == CLOSED


async def test_async_close_rejects_a_timeout_beyond_a_long_and_stays_open() -> None:
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    with pytest.raises(OverflowError) as err:
        await p.close(timeout=1e300)
    assert str(err.value) == TOO_LONG
    with pytest.raises(OverflowError):
        await p.close(timeout=float("inf"))
    with pytest.raises(ValueError):
        await p.close(timeout=float("nan"))
    f = await p.send(record=RECORD)
    await p.flush()
    assert isinstance(f.exception(), KafkaTimeoutError)
    await p.close()
    with pytest.raises(IllegalStateError) as err:
        await p.send(record=RECORD)
    assert str(err.value) == CLOSED


def test_closed_first_then_the_argument_checks() -> None:
    # Java checks these arguments before the closed state (partitionsFor's
    # requireNonNull, throwIfInvalidGroupMetadata, close(Duration)'s timeout);
    # here the closed state comes first (CLAUDE.md, Python Binding Conventions,
    # Implementation over the FFI, Order), so a second close() returns silently.
    p = KafkaProducer(configs=UNREACHABLE)
    p.close(timeout=0)
    with pytest.raises(IllegalStateError) as err:
        p.partitions_for(topic=None)  # type: ignore[arg-type]
    assert str(err.value) == CLOSED
    with pytest.raises(IllegalStateError) as err:
        p.send_offsets_to_transaction(offsets={}, group_metadata=None)  # type: ignore[arg-type]
    assert str(err.value) == CLOSED
    p.close(timeout=-1)


async def test_async_closed_first_then_the_argument_checks() -> None:
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    await p.close(timeout=0)
    with pytest.raises(IllegalStateError) as err:
        await p.partitions_for(topic=None)  # type: ignore[arg-type]
    assert str(err.value) == CLOSED
    with pytest.raises(IllegalStateError) as err:
        await p.send_offsets_to_transaction(offsets={}, group_metadata=None)  # type: ignore[arg-type]
    assert str(err.value) == CLOSED
    await p.close(timeout=-1)


def test_context_manager_flushes_then_closes() -> None:
    with KafkaProducer(configs=UNREACHABLE) as p:
        f = p.send(record=RECORD)
    assert f.done()
    with pytest.raises(IllegalStateError):
        p.flush()


# ===========================================================================
# The drain before flush and the transaction-control operations
# ===========================================================================

# Updated for the branch C API: it has no test pause and no Producer_drain.
# A send to the unreachable broker holds the core's submission task for
# max.block.ms (its metadata wait), which is what these tests hold instead.
HELD_MS = 500


def test_drain_waits_until_the_accumulation_is_handed_over() -> None:
    # The queued flush drains the submission queue first, so its completion
    # waits for the send before it, whose future completes first (the
    # callbacks run in completion order).
    p = KafkaProducer(configs={**UNREACHABLE, "max.block.ms": HELD_MS})
    try:
        f = p.send(record=RECORD)
        fired = threading.Event()
        future_done: list[bool] = []

        def flushed(error: int) -> None:
            if error:
                _lib.KafkaError_destroy(error)
            future_done.append(f.done())
            fired.set()

        _lib.Producer_flush_cb(p._c_producer, flushed)  # noqa: SLF001
        assert not fired.wait(0.2)
        assert fired.wait(30)
        assert future_done == [True]
    finally:
        p.close(timeout=0)


@pytest.mark.parametrize("operation", ["commit_transaction", "abort_transaction",
                                       "init_transactions", "flush"])
def test_control_operations_run_after_the_sends_that_returned(operation: str) -> None:
    # producer-transactions.md §13: a send that returned belongs to the
    # control call after it, so the call waits until the record is with the
    # Rust producer.
    p = KafkaProducer(configs={**UNREACHABLE, "max.block.ms": HELD_MS})
    try:
        p.send(record=RECORD)
        result: dict[str, Any] = {}

        def run() -> None:
            try:
                getattr(p, operation)()
                result["ok"] = True
            except BaseException as error:  # noqa: BLE001
                result["error"] = error

        t = threading.Thread(target=run)
        t.start()
        t.join(0.3)
        assert t.is_alive(), f"{operation} must wait for the handover"
        t.join(30)
        assert not t.is_alive()
        if operation == "flush":
            assert result == {"ok": True}
        else:
            assert isinstance(result["error"], IllegalStateError)
            assert str(result["error"]) == NOT_TRANSACTIONAL
    finally:
        p.close(timeout=0)


@pytest.mark.parametrize("transactional", [False, True])
def test_begin_transaction_waits_for_earlier_sends(transactional: bool) -> None:
    # Updated for the branch C API (was: does not wait): its beginTransaction,
    # like every transaction-control operation, drains the submission queue
    # first (producer-transactions.md §13), so begin_transaction() returns only
    # once the send before it is with the Rust producer. A transactional send
    # with no transaction started itself returns only then.
    configs = {**UNREACHABLE, "max.block.ms": HELD_MS}
    if transactional:
        configs["transactional.id"] = "txn"
    p = KafkaProducer(configs=configs)
    try:
        sent = threading.Event()

        def send() -> None:
            p.send(record=RECORD)
            sent.set()

        start = time.monotonic()
        t = threading.Thread(target=send)
        t.start()
        assert sent.wait(0.3) is not transactional
        with pytest.raises(IllegalStateError) as err:
            p.begin_transaction()
        assert time.monotonic() - start >= (HELD_MS - 50) / 1000.0
        assert str(err.value) == (
            "TransactionalId txn: Invalid transition attempted from state UNINITIALIZED to "
            "state IN_TRANSACTION" if transactional else NOT_TRANSACTIONAL)
        t.join(30)
        assert sent.is_set()
    finally:
        p.close(timeout=0)


# The sync KafkaProducer never calls a blocking C entry point (branch commit
# 2942d0921): every blocking-in-Java operation is submitted through its _cb
# twin and waited for in Python in 100 ms slices, so Ctrl+C is honoured.
_BLOCKING_ENTRY_POINTS = (
    "Producer_send", "Producer_flush", "Producer_partitions_for",
    "Producer_init_transactions", "Producer_begin_transaction",
    "Producer_send_offsets_to_transaction",
    "Producer_commit_transaction", "Producer_abort_transaction", "Producer_close",
)


def _forbid_blocking_entry_points(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(name: str) -> Any:
        def entry(*args: Any) -> Any:
            raise AssertionError(f"the sync KafkaProducer called the blocking {name}")
        return entry

    for name in _BLOCKING_ENTRY_POINTS:
        monkeypatch.setattr(_lib, name, forbidden(name))


def test_sync_producer_never_calls_a_blocking_entry_point(
        monkeypatch: pytest.MonkeyPatch) -> None:
    _forbid_blocking_entry_points(monkeypatch)
    got: list[Exception | None] = []
    p = KafkaProducer(configs=UNREACHABLE)
    future = p.send(record=RECORD, callback=lambda md, e: got.append(e))
    for call in (p.init_transactions, p.begin_transaction, p.commit_transaction,
                 p.abort_transaction):
        with pytest.raises(IllegalStateError) as err:
            call()
        assert str(err.value) == NOT_TRANSACTIONAL
    p.flush()
    assert isinstance(future.exception(timeout=0), KafkaTimeoutError)
    with pytest.raises(KafkaTimeoutError):
        p.partitions_for(topic=TOPIC)
    assert p.metrics() is not None
    p.close()
    assert len(got) == 1 and isinstance(got[0], KafkaTimeoutError)


def test_sync_send_offsets_to_transaction_never_calls_its_blocking_entry_point(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Apart from the test above: it needs a KafkaConsumer's group metadata.
    group_metadata = _consumer_group_metadata()
    _forbid_blocking_entry_points(monkeypatch)
    with KafkaProducer(configs=UNREACHABLE) as p:
        # Non-empty offsets: Java's empty map is a no-op on an idempotent producer
        # (see test_send_offsets_to_transaction_checks_group_metadata_first).
        offsets = {TopicPartition(topic=TOPIC, partition=0): OffsetAndMetadata(offset=1)}
        with pytest.raises(IllegalStateError) as err:
            p.send_offsets_to_transaction(offsets=offsets, group_metadata=group_metadata)
        assert str(err.value) == NOT_TRANSACTIONAL


def test_sigint_while_waiting_lets_the_operation_finish_then_reraises() -> None:
    # Ctrl+C during a waiting call (CLAUDE.md, Python Binding Conventions,
    # Threads and callbacks): the producer cannot abort the operation in
    # flight, so it waits for its completion (here partitions_for's metadata
    # wait of max.block.ms, 500 ms), then re-raises KeyboardInterrupt, and the
    # producer stays usable. pytest runs the test on the main thread, the only
    # one that runs Python signal handlers.
    p = KafkaProducer(configs={**UNREACHABLE, "max.block.ms": 500})
    try:
        timer = threading.Timer(0.15, os.kill, (os.getpid(), signal.SIGINT))
        start = time.monotonic()
        timer.start()
        try:
            with pytest.raises(KeyboardInterrupt):
                p.partitions_for(topic=TOPIC)
            elapsed = time.monotonic() - start
        finally:
            timer.cancel()
            timer.join()
        assert elapsed >= 0.45, elapsed  # not before the operation ended
        # Still usable: the next calls reach the core.
        with pytest.raises(KafkaTimeoutError) as err:
            p.partitions_for(topic=TOPIC)
        assert str(err.value) == f"Topic {TOPIC} not present in metadata after 500 ms."
        assert isinstance(p.send(record=RECORD).exception(timeout=30), KafkaTimeoutError)
    finally:
        p.close(timeout=0)


def test_send_offsets_to_transaction_checks_group_metadata_first() -> None:
    # Java: throwIfInvalidGroupMetadata, throwIfNoTransactionManager,
    # throwIfProducerClosed, then nothing to send for empty offsets. The core
    # makes the transaction-manager check (it takes a KafkaConsumer's metadata).
    with KafkaProducer(configs={**UNREACHABLE, "enable.idempotence": False}) as p:
        with pytest.raises(IllegalArgumentError) as err:
            p.send_offsets_to_transaction(offsets={}, group_metadata=None)  # type: ignore[arg-type]
        assert str(err.value) == "Consumer group metadata could not be null"
        with pytest.raises(IllegalStateError) as err2:
            p.send_offsets_to_transaction(offsets={}, group_metadata=_consumer_group_metadata())
        assert str(err2.value) == NO_TXN
        with pytest.raises(IllegalStateError) as err2:
            p.begin_transaction()
        assert str(err2.value) == NO_TXN
    with KafkaProducer(configs=UNREACHABLE) as idempotent:
        idempotent.send_offsets_to_transaction(offsets={},
                                               group_metadata=_consumer_group_metadata())


def test_send_offsets_to_transaction_needs_a_kafka_consumers_group_metadata() -> None:
    # The core takes the KafkaConsumer's own metadata handle, which a mock
    # consumer's metadata lacks; Java takes any ConsumerGroupMetadata.
    with KafkaProducer(configs=UNREACHABLE) as p:
        with pytest.raises(UnsupportedVersionError) as err:
            p.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata())
        assert str(err.value) == NO_GROUP_METADATA_HANDLE_MESSAGE


async def test_async_send_offsets_to_transaction_needs_a_kafka_consumers_group_metadata() -> None:  # noqa: E501
    async with AsyncKafkaProducer(configs=UNREACHABLE) as p:
        with pytest.raises(UnsupportedVersionError) as err:
            await p.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata())
        assert str(err.value) == NO_GROUP_METADATA_HANDLE_MESSAGE


# Configs on top of UNREACHABLE, and whether Java's producer built from them has
# a transaction manager: KafkaProducer.configureTransactionState creates one iff
# idempotence is on after ProducerConfig.postProcessAndValidateIdempotenceConfigs,
# which turns the default off for retries=0 or acks other than all / -1.
TRANSACTION_MANAGER_CASES: list[tuple[dict[str, Any], bool]] = [
    ({}, True),
    ({"enable.idempotence": True}, True),
    ({"enable.idempotence": "false"}, False),
    ({"enable.idempotence": None}, True),  # left out of the core's configs: the default
    ({"acks": "all"}, True),
    ({"acks": "-1"}, True),
    ({"acks": "1"}, False),
    ({"acks": "0"}, False),
    ({"retries": 0}, False),
    ({"retries": 3}, True),
    ({"enable.idempotence": False, "acks": "all"}, False),
    ({"enable.idempotence": False, "max.in.flight.requests.per.connection": 6}, False),
    ({"transactional.id": "txn"}, True),
]

TRANSACTIONAL_OPERATIONS = ["init_transactions", "begin_transaction",
                            "send_offsets_to_transaction", "commit_transaction",
                            "abort_transaction"]


def _transactional_arguments(operation: str) -> dict[str, Any]:
    if operation == "send_offsets_to_transaction":
        return {"offsets": {}, "group_metadata": _group_metadata()}
    return {}


@pytest.mark.parametrize(("extra", "manager"), TRANSACTION_MANAGER_CASES)
def test_the_transaction_manager_follows_java_idempotence_post_processing(
        extra: dict[str, Any], manager: bool) -> None:
    # The core decides whether the producer has one; asked directly it
    # reports NO_TXN iff it has none, and the open producer reports the same.
    p = KafkaProducer(configs={**UNREACHABLE, **extra})
    try:
        with pytest.raises(IllegalStateError) as core:
            raise_if_error(_lib.Producer_begin_transaction(p._c_producer))  # noqa: SLF001
        assert (str(core.value) != NO_TXN) is manager
        with pytest.raises(IllegalStateError) as err:
            p.begin_transaction()
        assert str(err.value) == str(core.value)
    finally:
        p.close(timeout=0)


# A producer with a transaction manager and one without.
CLOSED_CASES: list[dict[str, Any]] = [{}, {"enable.idempotence": False}]


@pytest.mark.parametrize("operation", TRANSACTIONAL_OPERATIONS)
@pytest.mark.parametrize("extra", CLOSED_CASES)
def test_a_closed_producer_reports_the_closed_state(operation: str, extra: dict[str, Any]) -> None:
    # Java's transactional methods call throwIfNoTransactionManager() before
    # throwIfProducerClosed() (KafkaProducer.java:661-662, 687-688, 746-747,
    # 791-792, 825-826); here the core makes that check, which a closed producer
    # can no longer reach (CLAUDE.md, Implementation over the FFI, exception 3).
    p = KafkaProducer(configs={**UNREACHABLE, **extra})
    p.close(timeout=0)
    with pytest.raises(IllegalStateError) as err:
        getattr(p, operation)(**_transactional_arguments(operation))
    assert str(err.value) == CLOSED


@pytest.mark.parametrize("operation", TRANSACTIONAL_OPERATIONS)
@pytest.mark.parametrize("extra", CLOSED_CASES)
async def test_a_closed_async_producer_reports_the_closed_state(
        operation: str, extra: dict[str, Any]) -> None:
    p = AsyncKafkaProducer(configs={**UNREACHABLE, **extra})
    await p.close(timeout=0)
    with pytest.raises(IllegalStateError) as err:
        result = getattr(p, operation)(**_transactional_arguments(operation))
        if inspect.isawaitable(result):
            await result
    assert str(err.value) == CLOSED


# ===========================================================================
# Backpressure: the core's max.block.ms (why the async send is awaited)
# ===========================================================================

# The bound the no-memory children are handed (their prelude reads it).
BACKPRESSURE_BOUND = 1000

# Updated for the branch C API (was: a send crossing the extension's bound of
# queued records waits for space until close releases it): the branch queues
# each record on the core's submission task, whose producer.send() waits
# max.block.ms for metadata and buffer space as Java's does (a deviation the
# producer module docstring records), so a non-transactional send returns at
# once and close() completes every queued send with the core's error.
QUEUED_SENDS = 3


def test_sync_send_returns_at_once_and_close_completes_the_queued_sends() -> None:
    p = KafkaProducer(configs={**UNREACHABLE, "max.block.ms": 200})
    start = time.monotonic()
    futures = [p.send(record=RECORD) for _ in range(QUEUED_SENDS)]
    assert time.monotonic() - start < 0.2, "a send must not wait for the core"
    assert not any(f.done() for f in futures)
    p.close(timeout=30.0)
    for f in futures:
        error = f.exception(timeout=0)
        assert isinstance(error, KafkaTimeoutError), error
        assert str(error) == f"Topic {TOPIC} not present in metadata after 200 ms."


async def test_async_send_returns_at_once_and_close_completes_the_queued_sends() -> None:
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 200})
    start = time.monotonic()
    futures = [await p.send(record=RECORD) for _ in range(QUEUED_SENDS)]
    assert time.monotonic() - start < 0.2, "a send must not wait for the core"
    assert not any(f.done() for f in futures)
    assert await asyncio.sleep(0, result=True)  # the loop stays responsive
    await asyncio.wait_for(p.close(timeout=30.0), timeout=60)
    for f in futures:
        error = f.exception()
        assert isinstance(error, KafkaTimeoutError), error
        assert str(error) == f"Topic {TOPIC} not present in metadata after 200 ms."


async def test_cancelling_close_waits_for_its_teardown_before_closing_the_serializers() -> None:
    # Cancelling the task awaiting close() lets the teardown (forcing an
    # unfinished close, waiting for the calls in flight, joining the threads)
    # end before the serializers close and close() raises CancelledError,
    # however often the task is cancelled meanwhile (Copilot review, PR #187).
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    order: list[str] = []
    started = threading.Event()
    release = threading.Event()
    real_teardown = p._teardown  # noqa: SLF001
    real_close_serializers = p._close_serializers  # noqa: SLF001

    def teardown(c_producer: int, force: bool) -> None:
        started.set()
        release.wait(10)
        real_teardown(c_producer, force)
        order.append("teardown")

    def close_serializers() -> None:
        order.append("serializers")
        real_close_serializers()

    p._teardown = teardown  # type: ignore[method-assign]  # noqa: SLF001
    p._close_serializers = close_serializers  # type: ignore[method-assign]  # noqa: SLF001
    task = asyncio.ensure_future(p.close())
    assert await asyncio.to_thread(started.wait, 10)
    task.cancel()
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.sleep(0.1)
    assert not task.done(), "close() returned while its teardown still runs"
    assert order == []
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert order == ["teardown", "serializers"]


# ===========================================================================
# Byte lengths and allocation failures in the C extension's send path
#
# Master's tests of the CPython fixes of PR #207, on this package's send path:
# ``MockProducer`` is a Python translation here and never reaches the C
# extension, so a ``KafkaProducer`` with an unreachable broker stands for the
# FFI-backed mock they used.
# ===========================================================================

def test_producer_record_rejects_a_2_gib_buffer(two_gib_bytes: bytes) -> None:
    # The C API takes int32_t lengths. A plain cast turned 2**31 into a
    # negative length, which the C API reads as a null value, so the bytes were
    # silently dropped; the length is now checked before anything is stored.
    # The check is in the native record a send builds (the default
    # bytes_serializer passes the buffer through without a copy).
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        with pytest.raises(OverflowError, match=r"^value exceeds 2 GiB$"):
            p.send(record=ProducerRecord(topic=TOPIC, value=two_gib_bytes))
        with pytest.raises(OverflowError, match=r"^key exceeds 2 GiB$"):
            p.send(record=ProducerRecord(topic=TOPIC, key=two_gib_bytes, value=b"v"))
    finally:
        p.close(timeout=0)


def test_producer_record_rejects_a_2_gib_header_value(two_gib_bytes: bytes) -> None:
    # The ninth int32_t length of the C extension, which only this branch has:
    # a header value of a ProducerRecord. A plain cast turned 2**31 into a
    # negative length, which the C API reads as a null header value. The
    # record keeps its header values as views, so the buffer is not copied.
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        with pytest.raises(OverflowError, match=r"^header value exceeds 2 GiB$"):
            p.send(record=ProducerRecord(topic=TOPIC, partition=None, key=None, value=b"v",
                                         headers=[("k", two_gib_bytes)]))
    finally:
        p.close(timeout=0)


# `_testcapi.set_nomemory` makes every CPython allocation fail, including the
# PyMem_Raw* calls the extension makes with the GIL released. Each check runs
# in a child interpreter, as CPython's own tests of it do, because an
# allocation failure the extension does not handle crashes the process.
_NO_MEMORY_PRELUDE = """
import sys
BACKPRESSURE_BOUND = int(sys.argv[1])
FUTURE_TIMEOUT = float(sys.argv[2])
sys.path[:0] = sys.argv[3:]
import _testcapi
import _confluentkafka as _lib
from confluent_kafka.producer import KafkaProducer, ProducerRecord
from confluent_kafka.producer._send import completion_to_python
UNREACHABLE = {"bootstrap.servers": "127.0.0.1:59999", "max.block.ms": 100}


def without_memory(call, *args):
    # Return call(*args), or the MemoryError it raised, with every allocation
    # failing for the duration of the call. `call(*args)` hands the `args`
    # tuple to a METH_VARARGS function as it is, and the frame object is
    # created up front, so only the call itself asks for memory meanwhile.
    sys._getframe()
    outcome = None
    _testcapi.set_nomemory(0)
    try:
        outcome = call(*args)
    except MemoryError as e:
        outcome = e
    finally:
        _testcapi.remove_mem_hooks()
    return outcome


def on_complete(metadata, error):
    completion_to_python(metadata, error, "t", None)  # reads the borrowed error
"""

# How long the child waits for a future: a send to the unreachable broker
# fails after max.block.ms (100 ms).
_NO_MEMORY_FUTURE_TIMEOUT = 30.0


def _run_without_memory(body: str) -> None:
    pytest.importorskip("_testcapi")
    import confluent_kafka

    module_dirs = dict.fromkeys(
        os.path.dirname(os.path.abspath(path))
        for path in (_lib.__file__, os.path.dirname(os.path.abspath(confluent_kafka.__file__))))
    proc = subprocess.run(
        [sys.executable, "-c", _NO_MEMORY_PRELUDE + body,
         str(BACKPRESSURE_BOUND), str(_NO_MEMORY_FUTURE_TIMEOUT), *module_dirs],
        capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, (proc.returncode, proc.stderr)
    assert proc.stdout.splitlines()[-1:] == ["ok"], proc.stdout


def test_send_without_memory_for_a_new_batch_raises_memory_error() -> None:
    # The first record of a fresh producer needs a new batch node. Its
    # allocation was unchecked, so a failure crashed on a NULL dereference.
    # Updated for the branch C API: the queued send is Producer_send_cb, whose
    # first allocation is the context it hands the C API.
    _run_without_memory("""
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
p = KafkaProducer(configs=UNREACHABLE)
record = p._native_record(ProducerRecord(topic="t", value=b"v"))
resolve = lambda error: None
refs = sys.getrefcount(record), sys.getrefcount(on_complete), sys.getrefcount(resolve)
outcome = without_memory(_lib.Producer_send_cb, p._c_producer, record, on_complete, resolve)
assert isinstance(outcome, MemoryError), outcome
# Not queued: every reference taken for the queue was given back...
assert (sys.getrefcount(record), sys.getrefcount(on_complete), sys.getrefcount(resolve)) == refs
# ...and no half-built batch was left behind: the next record still goes
# through the batch list to the producer, whose metadata wait it fails.
error = p.send(record=ProducerRecord(topic="t", value=b"v")).exception(timeout=FUTURE_TIMEOUT)
assert isinstance(error, KafkaTimeoutError), error
p.close(timeout=0)
print("ok")
""")


@pytest.mark.skip(reason="Subject not on this branch: the extension has no list of senders "
                         "waiting for space (no Producer_on_space_available); backpressure is "
                         "the core's max.block.ms, decided for the branch C API.")
def test_waiting_for_space_without_memory_does_not_wait() -> None:
    # The list of senders waiting for space grows on demand. A failed realloc
    # was assigned over the list and then written through; now the sender is
    # told not to wait, because its record is already queued.
    _run_without_memory("""
p = KafkaProducer(configs=UNREACHABLE)
_lib.Producer_test_set_paused(p._c_producer, True)
futures = [p.send(record=ProducerRecord(topic="t", value=b"v")) for _ in range(BACKPRESSURE_BOUND - 1)]
record = p._native_record(ProducerRecord(topic="t", value=b"v"))
assert _lib.Producer_send(p._c_producer, record, on_complete) is True
space_cb = lambda: None
refs = sys.getrefcount(space_cb)
outcome = without_memory(_lib.Producer_on_space_available, p._c_producer, space_cb)
assert outcome is True, outcome
assert sys.getrefcount(space_cb) == refs  # not registered
_lib.Producer_test_set_paused(p._c_producer, False)
# The queued records are not lost: the close completes every one of them.
p.close(timeout=0)
assert all(future.done() for future in futures)
print("ok")
""")


# ===========================================================================
# The async real producer
# ===========================================================================

async def test_async_send_callback_runs_on_the_loop_before_the_future() -> None:
    loop_thread = threading.get_ident()
    seen: list[tuple[int, bool, RecordMetadata]] = []
    holder: dict[str, Any] = {}

    def callback(md: RecordMetadata, e: Exception | None) -> None:
        seen.append((threading.get_ident(), holder["f"].done(), md))

    p = AsyncKafkaProducer(configs=UNREACHABLE)
    holder["f"] = f = await p.send(record=RECORD, callback=callback)
    with pytest.raises(KafkaTimeoutError):
        await asyncio.wait_for(f, 30)
    ((thread, future_done, md),) = seen
    assert thread == loop_thread and not future_done
    assert (md.topic(), md.partition(), md.offset()) == (TOPIC, -1, -1)
    await p.flush()
    await p.close()
    with pytest.raises(IllegalStateError) as err:
        await p.send(record=RECORD)
    assert str(err.value) == CLOSED


CALLBACK_ERROR = "Error executing user-provided callback on message for topic-partition 'topic--1'"
FLUSH_IN_CALLBACK = ("KafkaProducer.flush() invocation inside a callback is not permitted "
                     "because it may lead to deadlock.")


async def _until(condition: Any, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition not reached"
        await asyncio.sleep(0.01)


async def test_async_send_awaits_an_async_def_callback_before_the_future() -> None:
    # Java fires the callbacks before produceFuture.done()
    # (ProducerBatch.completeFutureAndFireCallbacks): an `async def` callback
    # is awaited on the loop, and the record's future completes, with the
    # record's own outcome, only once it returned and reported.
    release = asyncio.Event()
    started = asyncio.Event()
    seen: list[tuple[bool, Exception | None]] = []
    holder: dict[str, Any] = {}

    async def callback(md: RecordMetadata, e: Exception | None) -> None:
        started.set()
        await release.wait()
        seen.append((holder["f"].done(), e))

    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        holder["f"] = f = await p.send(record=RECORD, callback=callback)
        await asyncio.wait_for(started.wait(), 30)
        await asyncio.sleep(0.2)
        assert not f.done()
        release.set()
        with pytest.raises(KafkaTimeoutError):
            await asyncio.wait_for(f, 30)
        ((future_done, error),) = seen
        assert not future_done and isinstance(error, KafkaTimeoutError)
    finally:
        await p.close()


async def test_async_flush_waits_for_an_async_def_callback() -> None:
    # flush() returns once the records sent before it completed, their
    # callbacks run first: an awaited callback included.
    release = asyncio.Event()
    started = asyncio.Event()
    finished: list[bool] = []

    async def callback(md: RecordMetadata, e: Exception | None) -> None:
        started.set()
        await release.wait()
        finished.append(True)

    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        f = await p.send(record=RECORD, callback=callback)
        flush = asyncio.ensure_future(p.flush())
        await asyncio.wait_for(started.wait(), 30)
        await asyncio.sleep(0.3)
        assert not flush.done() and finished == []
        release.set()
        await asyncio.wait_for(flush, 30)
        assert finished == [True] and f.done()
    finally:
        await p.close()


async def test_async_def_callbacks_run_one_at_a_time_in_completion_order() -> None:
    # A producer's callbacks run one at a time in completion order (CLAUDE.md,
    # Python Binding Conventions, Threads and callbacks): a later completion
    # waits behind an awaited callback.
    release = asyncio.Event()
    order: list[str] = []

    async def first(md: RecordMetadata, e: Exception | None) -> None:
        order.append("first started")
        await release.wait()
        order.append("first done")

    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        f1 = await p.send(record=RECORD, callback=first)
        f2 = await p.send(record=RECORD, callback=lambda md, e: order.append("second"))
        await _until(lambda: order == ["first started"])
        await asyncio.sleep(0.3)
        assert order == ["first started"] and not f2.done()
        release.set()
        for f in (f1, f2):
            with pytest.raises(KafkaTimeoutError):
                await asyncio.wait_for(f, 30)
        assert order == ["first started", "first done", "second"]
    finally:
        await p.close()


@pytest.mark.parametrize(("kind", "text"), [
    ("plain", "callback blew up"),
    ("async def", "async callback blew up"),
    # A bare CancelledError has an empty str(): its type's name is the text,
    # as Java's toString() is the class name for a null message.
    ("cancelled", "CancelledError"),
])
async def test_a_raising_callback_on_the_async_producer_is_reported_and_fails_nothing(
        caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str], kind: str,
        text: str) -> None:
    # A throwing onCompletion is logged (ProducerBatch.completeFutureAndFireCallbacks)
    # and fails nothing: the future keeps the record's own outcome and the next
    # callback runs. Python reports the exception to the core, which logs it
    # once; Python logs nothing.
    seen: list[str] = []

    def plain(md: RecordMetadata, e: Exception | None) -> None:
        raise RuntimeError("callback blew up")

    async def async_def(md: RecordMetadata, e: Exception | None) -> None:
        await asyncio.sleep(0)
        if kind == "cancelled":
            raise asyncio.CancelledError
        raise RuntimeError("async callback blew up")

    capfd.readouterr()
    caplog.set_level(logging.DEBUG, logger="confluent_kafka")
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        f = await p.send(record=RECORD, callback=plain if kind == "plain" else async_def)
        g = await p.send(record=RECORD, callback=lambda md, e: seen.append("next"))
        for future in (f, g):
            with pytest.raises(KafkaTimeoutError):
                await asyncio.wait_for(future, 30)
        await p.flush()
        assert seen == ["next"]
    finally:
        await p.close()
    assert _core_callback_errors(capfd) == [f"{CALLBACK_ERROR}: LocalCallbackError: {text}"]
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []


async def test_flush_in_an_async_def_callback_raises_javas_error() -> None:
    out: dict[str, Any] = {}
    p = AsyncKafkaProducer(configs=UNREACHABLE)

    async def callback(md: RecordMetadata, e: Exception | None) -> None:
        try:
            await p.flush()
        except KafkaError as error:
            out["flush"] = error

    try:
        f = await p.send(record=RECORD, callback=callback)
        with pytest.raises(KafkaTimeoutError):
            await asyncio.wait_for(f, 30)
        assert str(out["flush"]) == FLUSH_IN_CALLBACK
    finally:
        await p.close()


def test_a_coroutine_callback_on_the_sync_producer_is_closed_and_reported(
        caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str]) -> None:
    # Only an asyncio producer awaits an `async def` callback: the sync one
    # closes the coroutine and reports a TypeError, as for a raising callback,
    # which the core logs.
    ran: list[bool] = []

    async def callback(md: RecordMetadata, e: Exception | None) -> None:
        ran.append(True)

    capfd.readouterr()
    with caplog.at_level(logging.DEBUG, logger="confluent_kafka"):
        with KafkaProducer(configs=UNREACHABLE) as p:
            f = p.send(record=RECORD, callback=callback)  # type: ignore[arg-type]
            assert isinstance(f.exception(timeout=30), KafkaTimeoutError)
    assert _core_callback_errors(capfd) == [
        f"{CALLBACK_ERROR}: LocalCallbackError: "
        "a coroutine send() callback requires an AsyncProducer"]
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
    assert ran == []


async def test_async_mock_awaits_an_async_def_callback_and_flush_waits_for_it() -> None:
    release = asyncio.Event()
    finished: list[bool] = []

    async def callback(md: RecordMetadata, e: Exception | None) -> None:
        await release.wait()
        finished.append(True)

    p = AsyncMockProducer()
    f = await p.send(record=RECORD, callback=callback)
    flush = asyncio.ensure_future(p.flush())
    await asyncio.sleep(0.2)
    assert not flush.done() and not f.done()
    release.set()
    await asyncio.wait_for(flush, 30)
    assert finished == [True] and (await f).offset() == 0
    await p.close()


def test_an_async_completion_after_its_loop_closed_is_dropped(
        caplog: pytest.LogCaptureFixture) -> None:
    # The callback runs on the event loop (CLAUDE.md, Python Binding
    # Conventions, Threads and callbacks), and a future only its own loop can
    # complete: a record that completes after its loop closed is dropped with a
    # warning, its callback not run on the completion thread, its future no
    # longer tracked, so a flush() and close() from a fresh loop return. The
    # send fails on its metadata wait after max.block.ms, long after the loop
    # closed.
    caplog.set_level(logging.WARNING, logger="confluent_kafka.producer")
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 1000})
    ran: list[Exception | None] = []
    loop = asyncio.new_event_loop()
    try:
        future = loop.run_until_complete(
            p.send(record=RECORD, callback=lambda md, e: ran.append(e)))
    finally:
        loop.close()
    assert future in p._futures  # noqa: SLF001
    deadline = time.monotonic() + 30
    while future in p._futures and time.monotonic() < deadline:  # noqa: SLF001
        time.sleep(0.05)
    assert future not in p._futures  # noqa: SLF001
    assert ran == []
    assert not future.done()
    assert [r.getMessage() for r in caplog.records if r.name == "confluent_kafka.producer"] == [
        f"The completion of a record sent to topic-partition '{TOPIC}--1' was dropped: its "
        "event loop had closed, so its callback did not run and its future does not complete"]

    async def flush_and_close() -> None:
        await asyncio.wait_for(p.flush(), 30)
        await asyncio.wait_for(p.close(), 30)

    asyncio.run(flush_and_close())
    assert p._closed  # noqa: SLF001


DROPPED = (f"The completion of a record sent to topic-partition '{TOPIC}--1' was dropped: its "
           "event loop had closed, so its callback did not run and its future does not complete")


def _dropped(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records
            if r.name == "confluent_kafka.producer" and r.getMessage().startswith("The completion")]


def test_a_flush_that_starts_before_a_closed_loops_completion_returns(
        caplog: pytest.LogCaptureFixture) -> None:
    # Loop A closes with a send in flight, and a flush() on loop B starts
    # before the record completes: the flush's wait list holds a future no
    # loop can complete any more, so the wait leaves it out, and the
    # completion is dropped with one warning when it arrives.
    caplog.set_level(logging.WARNING, logger="confluent_kafka.producer")
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 1000})
    ran: list[Exception | None] = []
    loop = asyncio.new_event_loop()
    try:
        future = loop.run_until_complete(
            p.send(record=RECORD, callback=lambda md, e: ran.append(e)))
    finally:
        loop.close()

    async def flush_then_close() -> None:
        assert future in p._futures  # noqa: SLF001 - the completion has not arrived
        await asyncio.wait_for(p.flush(), 10)
        await asyncio.wait_for(p.close(), 10)

    asyncio.run(flush_then_close())
    assert future not in p._futures  # noqa: SLF001
    assert ran == []
    assert not future.done()
    assert _dropped(caplog) == [DROPPED]


def test_a_completion_left_for_a_closed_loop_is_dropped_by_the_next_flush(
        caplog: pytest.LogCaptureFixture) -> None:
    # The record completes while its loop is open but not running, so its
    # completion waits for that loop, which then closes without running it: a
    # flush() on another loop drops it, with one warning, and returns.
    caplog.set_level(logging.WARNING, logger="confluent_kafka.producer")
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    ran: list[Exception | None] = []
    loop = asyncio.new_event_loop()
    try:
        future = loop.run_until_complete(
            p.send(record=RECORD, callback=lambda md, e: ran.append(e)))
        deadline = time.monotonic() + 30
        while loop not in p._pending and time.monotonic() < deadline:  # noqa: SLF001
            time.sleep(0.01)
        assert loop in p._pending  # noqa: SLF001
    finally:
        loop.close()

    async def flush_then_close() -> None:
        await asyncio.wait_for(p.flush(), 10)
        await asyncio.wait_for(p.close(), 10)

    asyncio.run(flush_then_close())
    assert loop not in p._pending  # noqa: SLF001
    assert future not in p._futures  # noqa: SLF001
    assert ran == []
    assert not future.done()
    assert _dropped(caplog) == [DROPPED]


def test_a_completion_its_closing_loop_refuses_is_dropped(
        hold_completion: Any, caplog: pytest.LogCaptureFixture,
        capfd: pytest.CaptureFixture[str]) -> None:
    # The loop is open when the completion checks it, then closes before
    # call_soon_threadsafe, which raises RuntimeError on the C completion
    # thread: the completion is dropped there, with one warning, instead of the
    # error reaching the C trampoline (which printed it) and the completion
    # waiting for a later flush().
    caplog.set_level(logging.WARNING, logger="confluent_kafka.producer")
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    # Updated for the branch C API: the queued send is Producer_send_cb, and
    # the delivery callback is its next-to-last argument.
    held = hold_completion("Producer_send_cb", -2)
    ran: list[Exception | None] = []
    loop = asyncio.new_event_loop()
    try:
        future = loop.run_until_complete(
            p.send(record=RECORD, callback=lambda md, e: ran.append(e)))
    finally:
        loop.close()
    loop.is_closed = lambda: False  # type: ignore[method-assign] # the check saw it open
    capfd.readouterr()
    held.release.set()
    assert held.delivered.wait(30)
    del loop.is_closed
    assert held.raised == []
    assert capfd.readouterr().err == ""
    assert loop not in p._pending  # noqa: SLF001
    assert future not in p._futures  # noqa: SLF001
    assert ran == []
    assert not future.done()
    assert _dropped(caplog) == [DROPPED]
    asyncio.run(p.close())


def test_a_completion_left_for_a_closed_loop_is_dropped_by_close(
        hold_completion: Any, caplog: pytest.LogCaptureFixture) -> None:
    # As by the next flush(), with no flush(): close() drops it, with one
    # warning.
    caplog.set_level(logging.WARNING, logger="confluent_kafka.producer")
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    # Updated for the branch C API: the queued send is Producer_send_cb, and
    # the delivery callback is its next-to-last argument.
    held = hold_completion("Producer_send_cb", -2)
    ran: list[Exception | None] = []
    loop = asyncio.new_event_loop()
    try:
        future = loop.run_until_complete(
            p.send(record=RECORD, callback=lambda md, e: ran.append(e)))
        held.release.set()
        assert held.delivered.wait(30)
        assert loop in p._pending  # noqa: SLF001
    finally:
        loop.close()
    asyncio.run(asyncio.wait_for(p.close(), 10))
    assert loop not in p._pending  # noqa: SLF001
    assert future not in p._futures  # noqa: SLF001
    assert ran == []
    assert not future.done()
    assert _dropped(caplog) == [DROPPED]


def test_an_operation_result_its_closing_loop_refuses_is_freed(
        hold_completion: Any, freed_errors: list[int], stopped_loop: Any,
        capfd: pytest.CaptureFixture[str]) -> None:
    # The same race for the result of an operation (begin_transaction()'s
    # error on a non-transactional producer), whose task waits on a loop that
    # stopped: the loop is open when the result checks it, then closes before
    # call_soon_threadsafe. The result is freed on the pumping thread
    # instead of the RuntimeError reaching the C trampoline, which printed it
    # and left the result's handles allocated.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    held = hold_completion("Producer_begin_transaction_cb")
    stopped_loop.start(p.begin_transaction())
    assert held.submitted.is_set()
    loop = stopped_loop.loop
    loop.close()
    loop.is_closed = lambda: False  # type: ignore[method-assign] # the check saw it open
    capfd.readouterr()
    held.release.set()
    assert held.delivered.wait(30)
    del loop.is_closed
    ((error,),) = held.payloads
    assert error
    assert held.raised == []
    assert capfd.readouterr().err == ""
    assert freed_errors == [error]
    asyncio.run(p.close())


@pytest.mark.parametrize("call", ["flush", "close"])
def test_an_operation_result_left_for_a_closed_loop_is_freed_by_the_next_call(
        call: str, hold_completion: Any, freed_errors: list[int], stopped_loop: Any) -> None:
    # The result reaches its loop while the loop is stopped, and the loop then
    # closes, which discards the queued delivery: the next operation of the
    # producer, from another loop, frees it.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    held = hold_completion("Producer_begin_transaction_cb")
    stopped_loop.start(p.begin_transaction())
    held.release.set()
    assert held.delivered.wait(30)
    stopped_loop.loop.close()
    ((error,),) = held.payloads
    assert error
    assert freed_errors == []

    async def next_call_then_close() -> None:
        await asyncio.wait_for(getattr(p, call)(), 10)
        await asyncio.wait_for(p.close(), 10)

    asyncio.run(next_call_then_close())
    assert freed_errors == [error]


async def test_async_begin_transaction_is_a_coroutine_and_does_not_block_the_loop() -> None:
    # begin_transaction() is a coroutine (Class family).
    # Updated for the branch C API (was: a record still waiting for its
    # topic's metadata does not hold it, Critic 75 N1): the branch's
    # beginTransaction drains the submission queue first, as every
    # transaction-control operation does (producer-transactions.md §13), so it
    # waits out the record's max.block.ms; it does so off the loop, which
    # keeps running meanwhile.
    async with AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 3000}) as p:
        f = await p.send(record=RECORD)
        ticks = 0

        async def tick() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.05)
                ticks += 1

        ticker = asyncio.ensure_future(tick())
        start = time.monotonic()
        begin = p.begin_transaction()
        assert inspect.iscoroutine(begin)
        try:
            with pytest.raises(IllegalStateError) as err:
                await begin
        finally:
            ticker.cancel()
        elapsed = time.monotonic() - start
        assert elapsed >= 2.5
        assert ticks >= 20, (ticks, elapsed)
        assert str(err.value) == NOT_TRANSACTIONAL
        with pytest.raises(IllegalStateError):
            await p.commit_transaction()
    with pytest.raises(KafkaTimeoutError):
        await f


# ===========================================================================
# MockProducer beyond MockProducerTest
# ===========================================================================

def test_mock_callback_runs_on_the_caller_before_the_future_completes() -> None:
    p = MockProducer(auto_complete=True, partitioner=None, key_serializer=None,
                     value_serializer=None)
    caller = threading.get_ident()
    seen: list[tuple[int, RecordMetadata, Exception | None]] = []
    f = p.send(record=RECORD, callback=lambda md, e: seen.append((threading.get_ident(), md, e)))
    ((thread, md, e),) = seen
    assert thread == caller and e is None
    assert (md.topic(), md.partition(), md.offset()) == (TOPIC, 0, 0)
    assert f.done() and not f.cancel()
    assert f.result().offset() == 0 and f.result().timestamp() == -1


def test_mock_raising_callback_propagates_from_the_completing_call() -> None:
    # Java's MockProducer.Completion.complete calls onCompletion with no
    # try/catch, before result.done() (MockProducer.java:640-650): the
    # exception propagates out of the call that completed the record, the
    # record's future never completes, and the completions behind it stay
    # queued.
    raised = RuntimeError("callback blew up")

    def boom(md: RecordMetadata, e: Exception | None) -> None:
        raise raised

    p = MockProducer()
    f = p.send(record=RECORD, callback=boom)
    g = p.send(record=RECORD)
    with pytest.raises(RuntimeError) as info:
        p.complete_next()
    assert info.value is raised
    assert not f.done() and not g.done()
    assert p.complete_next()
    assert g.result().offset() == 1

    h = p.send(record=RECORD, callback=boom)
    with pytest.raises(RuntimeError) as info:
        p.error_next(e=KafkaError(message="failed"))
    assert info.value is raised
    assert not h.done()

    i = p.send(record=RECORD, callback=boom)
    j = p.send(record=RECORD)
    with pytest.raises(RuntimeError) as info:
        p.flush()
    assert info.value is raised
    assert not i.done() and not j.done()
    p.flush()
    assert j.result().offset() == 4

    auto = MockProducer(auto_complete=True, partitioner=None, key_serializer=None,
                        value_serializer=None)
    with pytest.raises(RuntimeError) as info:
        auto.send(record=RECORD, callback=boom)
    assert info.value is raised
    assert auto.history() == [RECORD]

    txn = MockProducer()
    txn.init_transactions()
    txn.begin_transaction()
    txn.send(record=RECORD, callback=boom)
    with pytest.raises(RuntimeError) as info:
        txn.commit_transaction()
    assert info.value is raised
    assert not txn.transaction_committed() and txn.transaction_in_flight()


def test_mock_coroutine_callback_raises_a_type_error() -> None:
    # The sync mock cannot await an `async def` callback: its coroutine is
    # closed and a TypeError raised from the completing call.
    ran: list[bool] = []

    async def callback(md: RecordMetadata, e: Exception | None) -> None:
        ran.append(True)

    p = MockProducer()
    f = p.send(record=RECORD, callback=callback)  # type: ignore[arg-type]
    with pytest.raises(TypeError) as info:
        p.complete_next()
    assert str(info.value) == "a coroutine send() callback requires an AsyncProducer"
    assert not f.done() and ran == []


async def test_async_mock_raising_callback_propagates_from_the_completing_call() -> None:
    raised = RuntimeError("callback blew up")

    def boom(md: RecordMetadata, e: Exception | None) -> None:
        raise raised

    auto = AsyncMockProducer(auto_complete=True, partitioner=None, key_serializer=None,
                             value_serializer=None)
    with pytest.raises(RuntimeError) as info:
        await auto.send(record=RECORD, callback=boom)
    assert info.value is raised
    assert auto.history() == [RECORD]

    p = AsyncMockProducer()
    f = await p.send(record=RECORD, callback=boom)
    with pytest.raises(RuntimeError) as info:
        p.complete_next()
    assert info.value is raised
    assert not f.done()
    await p.flush()  # nothing deferred
    await p.close()


@pytest.mark.parametrize("cancelled", [False, True])
async def test_async_mock_raising_async_def_callback_is_a_deferred_error(
        cancelled: bool) -> None:
    # The completing call has returned when an awaited callback raises: its
    # exception, CancelledError included, is raised by the next awaited call of
    # the mock, once, and the record's future never completes.
    raised: BaseException = (asyncio.CancelledError() if cancelled
                             else RuntimeError("async callback blew up"))

    async def boom(md: RecordMetadata, e: Exception | None) -> None:
        await asyncio.sleep(0)
        raise raised

    p = AsyncMockProducer()
    f = await p.send(record=RECORD, callback=boom)
    assert p.complete_next()
    with pytest.raises(type(raised)) as info:
        await p.flush()
    assert info.value is raised
    assert not f.done()
    await p.flush()

    g = await p.send(record=RECORD, callback=boom)
    assert p.complete_next()
    await _until(lambda: p._mock_callback_errors)  # noqa: SLF001
    with pytest.raises(type(raised)) as info:
        await p.send(record=RECORD)
    assert info.value is raised
    assert not g.done()
    await p.close()


async def test_async_mock_close_raises_a_deferred_callback_error_once_closed() -> None:
    raised = RuntimeError("async callback blew up")

    async def boom(md: RecordMetadata, e: Exception | None) -> None:
        await asyncio.sleep(0)
        raise raised

    p = AsyncMockProducer()
    await p.send(record=RECORD, callback=boom)
    assert p.complete_next()
    with pytest.raises(RuntimeError) as info:
        await p.close()
    assert info.value is raised
    assert p.closed()


def test_mock_error_next_with_none_completes_successfully() -> None:
    # Java's completeNext() is errorNext(null).
    p = MockProducer()
    f = p.send(record=RECORD)
    assert p.error_next(e=None)  # type: ignore[arg-type]
    assert f.result().offset() == 0


def test_mock_injected_errors_are_raised_as_the_same_instance() -> None:
    p = MockProducer(auto_complete=True, partitioner=None, key_serializer=None,
                     value_serializer=None)
    for setter, call in [
        ("set_send_exception", lambda: p.send(record=RECORD)),
        ("set_flush_exception", p.flush),
        ("set_partitions_for_exception", lambda: p.partitions_for(topic=TOPIC)),
        ("set_init_transaction_exception", p.init_transactions),
    ]:
        error = KafkaError(message=setter)
        getattr(p, setter)(**{setter[len("set_"):]: error})
        with pytest.raises(KafkaError) as err:
            call()
        assert err.value is error
        getattr(p, setter)(**{setter[len("set_"):]: None})
    p.init_transactions()
    for setter, call in [
        ("set_begin_transaction_exception", p.begin_transaction),
    ]:
        error = KafkaError(message=setter)
        getattr(p, setter)(**{setter[len("set_"):]: error})
        with pytest.raises(KafkaError) as err:
            call()
        assert err.value is error
        getattr(p, setter)(**{setter[len("set_"):]: None})
    p.begin_transaction()
    for setter, call in [
        ("set_send_offsets_to_transaction_exception",
         lambda: p.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata())),
        ("set_commit_transaction_exception", p.commit_transaction),
        ("set_abort_transaction_exception", p.abort_transaction),
    ]:
        error = KafkaError(message=setter)
        getattr(p, setter)(**{setter[len("set_"):]: error})
        with pytest.raises(KafkaError) as err:
            call()
        assert err.value is error
        getattr(p, setter)(**{setter[len("set_"):]: None})
    error = KafkaError(message="close")
    p.set_close_exception(close_exception=error)
    with pytest.raises(KafkaError) as err:
        p.close()
    assert err.value is error and not p.closed()
    p.set_close_exception(close_exception=None)
    p.close()


def test_mock_close_ignores_its_timeout() -> None:
    # Java's MockProducer.close(Duration) never reads the timeout.
    p = MockProducer()
    p.close(timeout=-1)
    assert p.closed()
    p.close()  # twice is harmless


def test_mock_metrics_and_partitions_for_use_what_it_is_given() -> None:
    cluster = _Cluster([_partition_info(0), _partition_info(1)])
    p = MockProducer(cluster=cluster, auto_complete=False, partitioner=None)
    assert [i.partition() for i in p.partitions_for(topic=TOPIC)] == [0, 1]
    assert p.metrics() == {}
    name = MetricName(name="n", group="g", description="d", tags={})
    metric = object()
    p.set_mock_metrics(name=name, metric=metric)  # type: ignore[arg-type]
    assert p.metrics() == {name: metric}


def test_mock_partitions_with_the_cluster_and_the_partitioner() -> None:
    cluster = _Cluster([_partition_info(0), _partition_info(1), _partition_info(2)])
    calls: list[tuple[Any, ...]] = []

    class Chooser:
        def partition(self, *args: Any) -> int:
            calls.append(args)
            return 2

    p = MockProducer(cluster=cluster, auto_complete=True, partitioner=Chooser(),
                     key_serializer=string_serializer(), value_serializer=string_serializer())
    f = p.send(record=ProducerRecord(topic=TOPIC, key="k", value="v"))
    assert f.result().partition() == 2
    assert calls == [(TOPIC, "k", b"k", "v", b"v", cluster)]
    # Without a partitioner the first partition; a given partition must exist.
    p2 = MockProducer(cluster=cluster, auto_complete=True, partitioner=None)
    assert p2.send(record=RECORD).result().partition() == 0
    assert p2.send(record=ProducerRecord(topic=TOPIC, partition=1, key=None, value=b"v")).result(
    ).partition() == 1
    with pytest.raises(IllegalArgumentError) as err:
        p2.send(record=ProducerRecord(topic=TOPIC, partition=3, key=None, value=b"v"))
    assert str(err.value) == "Invalid partition given with record: 3 is not in the range [0...3]."


def test_mock_records_and_offsets_observers() -> None:
    p = MockProducer(auto_complete=True, partitioner=None, key_serializer=None,
                     value_serializer=None)
    p.init_transactions()
    p.begin_transaction()
    p.send(record=RECORD)
    assert p.uncommitted_records() == [RECORD] and p.history() == []
    tp = TopicPartition(topic=TOPIC, partition=0)
    from confluent_kafka.consumer import OffsetAndMetadata
    offsets = {tp: OffsetAndMetadata(offset=5)}
    p.send_offsets_to_transaction(offsets=offsets, group_metadata=_group_metadata())
    assert p.uncommitted_offsets() == {"g": offsets}
    p.commit_transaction()
    assert p.history() == [RECORD] and p.uncommitted_records() == []
    assert p.consumer_group_offsets_history() == [{"g": offsets}]
    assert p.uncommitted_offsets() == {}


async def test_async_mock_family() -> None:
    p = AsyncMockProducer(auto_complete=True, partitioner=None, key_serializer=None,
                          value_serializer=None)
    loop_thread = threading.get_ident()
    seen: list[int] = []
    f = await p.send(record=RECORD, callback=lambda md, e: seen.append(threading.get_ident()))
    assert f.done() and seen == [loop_thread]
    md = await (await p.send(record=RECORD))
    assert md.offset() == 1
    await p.init_transactions()
    await p.begin_transaction()
    assert p.transaction_in_flight()
    await p.commit_transaction()
    assert p.commit_count() == 1
    manual = AsyncMockProducer()
    pending = await manual.send(record=RECORD)
    assert not pending.done()
    await manual.flush()
    assert (await pending).offset() == 0
    async with AsyncMockProducer() as q:
        await q.send(record=RECORD)
    assert q.closed() and q.flushed()
    await p.close(timeout=-1)
    assert p.closed()


def test_a_bare_kafka_exception_from_the_core_is_the_base_error(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The core reports a bare KafkaException (the producer's "Failed to
    # construct kafka producer") with UnknownServerException's id; it is not an
    # ApiException, so it arrives as the base KafkaError.
    from confluent_kafka import _errors as errmod
    from confluent_kafka.common.errors import UnknownServerError

    class _Lib:
        api_error = False

        @staticmethod
        def KafkaError_code(handle: int) -> int:
            return -1

        @staticmethod
        def KafkaError_message(handle: int) -> str:
            return "Failed to construct kafka producer"

        @staticmethod
        def KafkaError_source(handle: int) -> int:
            return 0

        @staticmethod
        def KafkaError_payload(handle: int) -> object:
            return None

        @classmethod
        def KafkaError_is_api_error(cls, handle: int) -> bool:
            return cls.api_error

        @staticmethod
        def KafkaError_destroy(handle: int) -> None:
            pass

    monkeypatch.setattr(errmod, "_lib", _Lib)
    assert type(errmod.from_ffi_error(1)) is KafkaError
    _Lib.api_error = True
    assert type(errmod.from_ffi_error(1)) is UnknownServerError
