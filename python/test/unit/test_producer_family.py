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
path, the callback contract, close, and the flush and transaction-control calls,
which include the sends that returned before them and do not wait for one still
in send() (producer-transactions.md §13).

Java's own tests are in ``test_mock_producer.py`` (``MockProducerTest``) and
``test_kafka_producer.py`` (``KafkaProducerTest``). A real ``KafkaProducer``
here talks to an unreachable bootstrap address, so its sends fail on the
metadata wait (``max.block.ms``); the broker round trip is covered by the
integration tests.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import inspect
import itertools
import logging
import math
import os
import random
import signal
import subprocess
import sys
import threading
import time
from types import FrameType
from collections.abc import Callable
from typing import Any, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import ExecutionError, IllegalArgumentError, IllegalStateError
from confluent_kafka._args import MISSING
from confluent_kafka.common import (
    AsyncKafkaFuture,
    KafkaError,
    KafkaFuture,
    MetricName,
    PartitionInfo,
    TopicPartition,
)
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka import UnsupportedOperationError
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
from confluent_kafka.common.kafka_future import _KafkaFutureImpl
from confluent_kafka.producer._base import NO_GROUP_METADATA_HANDLE_MESSAGE, raise_if_error
from confluent_kafka.producer._record_future import AsyncRecordFuture, RecordFuture

from .producer_futures import (
    assert_send_throws,
    async_assert_send_throws,
    async_failure,
    failure,
)

TOPIC = "topic"
RECORD = ProducerRecord(topic=TOPIC, key=b"k", value=b"v")
# Sends fail on the metadata wait after max.block.ms.
UNREACHABLE = {"bootstrap.servers": "127.0.0.1:59999", "max.block.ms": 100}
CLOSED = "Cannot perform operation after producer has been closed"
BOUND_ELSEWHERE = "{cls} is bound to a different event loop"
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
    # Signatures, "'Given' means ..."): callback defaults to MISSING. Both sets
    # are Java overloads, so there is no check, and send fills an omitted one.
    for cls in (Producer, AsyncProducer, MockProducer, AsyncMockProducer):
        assert inspect.signature(cls.send).parameters["callback"].default is MISSING, cls
    p = MockProducer(auto_complete=True, partitioner=None)
    assert p.send(record=RECORD).get().offset() == 0
    assert p.send(record=RECORD, callback=None).get().offset() == 1


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
                              "Producer", "ProducerConfig", "ProducerRecord",
                              "RecordMetadata", "BufferExhaustedError"]
    assert not hasattr(module, "DeliveryCallback")
    assert Partitioner is object
    # A Protocol of Java's onCompletion parameters, keyword-only plus **kwargs.
    params = inspect.signature(Callback.__call__).parameters
    assert [(n, p.kind) for n, p in params.items()] == [
        ("self", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        ("metadata", inspect.Parameter.KEYWORD_ONLY),
        ("exception", inspect.Parameter.KEYWORD_ONLY),
        ("kwargs", inspect.Parameter.VAR_KEYWORD)]


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
    assert not f.is_done()
    assert p.partitions_for(topic=TOPIC) == []
    assert p.complete_next()
    assert f.get().offset() == 0
    # auto_complete given as False still counts as given: the four-argument form.
    p2 = MockProducer(auto_complete=False, partitioner=None, key_serializer=None,
                      value_serializer=None)
    assert not p2.send(record=RECORD).is_done()


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
            native = p._native_parts(ProducerRecord(topic=TOPIC, key="k", value="abc"))[0]  # noqa: SLF001
            assert (native.key, native.value) == (b"K", b"ABC")
            assert _UpperSerializer.configured == [True, False]
        finally:
            _close_now(p)
        _UpperSerializer.configured.clear()
        p = cls(configs={**UNREACHABLE, "key.serializer": route, "value.serializer": route},
                value_serializer=string_serializer())
        try:
            native = p._native_parts(ProducerRecord(topic=TOPIC, key="k", value="abc"))[0]  # noqa: SLF001
            assert (native.key, native.value) == (b"K", b"abc")
            assert _UpperSerializer.configured == [True]
        finally:
            _close_now(p)
    _UpperSerializer.configured.clear()


def test_serializer_argument_wins_over_its_config_key() -> None:
    # Critic 74 F1 (Actor 74): the argument wins and the config key is ignored,
    # not even parsed: Java's ProducerConfig.appendSerializerToConfig replaces
    # the key with the argument's class before ConfigDef parses it
    # (ProducerConfig.java:718).
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

    def __call__(self, *, topic: str, data: str | None, headers: Any = None, **kwargs: Any) -> bytes | None:
        return None if data is None else data.upper().encode()


# ===========================================================================
# The send path
# ===========================================================================

def _native(record: ProducerRecord[Any, Any], **kwargs: Any) -> Any:
    p = KafkaProducer(configs=UNREACHABLE, **kwargs)
    try:
        return p._native_parts(record)[0]  # noqa: SLF001 - the struct the FFI reads
    finally:
        p.close(timeout=0)


class _OwnTopic(ProducerRecord[Any, Any]):
    """A record whose class overrides a getter."""

    __slots__ = ()

    def topic(self) -> str:
        return "from-the-subclass"


def test_send_reads_the_record_through_its_getters() -> None:
    # Java's doSend reads the record through its getters, so a subclass that
    # overrides one, and a getter patched on the class, are what is sent.
    assert _native(_OwnTopic(topic=TOPIC, value=b"v")).topic == "from-the-subclass"
    from unittest import mock

    with mock.patch.object(ProducerRecord, "partition", lambda self: 4):
        assert _native(ProducerRecord(topic=TOPIC, value=b"v")).partition == 4
    assert _native(ProducerRecord(topic=TOPIC, value=b"v")).partition is None


def test_send_reads_each_getter_once() -> None:
    counts: dict[str, int] = {}

    class Counting(ProducerRecord[Any, Any]):
        __slots__ = ()

        def __getattribute__(self, name: str) -> Any:
            if name in ("topic", "partition", "timestamp", "key", "value", "headers"):
                counts[name] = counts.get(name, 0) + 1
            return object.__getattribute__(self, name)

    _native(Counting(topic=TOPIC, key=b"k", value=b"v"))
    assert counts == {"topic": 1, "partition": 1, "timestamp": 1, "key": 1, "value": 1,
                      "headers": 1}


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
    expected = [(_utf8_address(k), 0 if original is None else _bytes_address(original))
                for k, original in zip([h.key() for h in record.headers()],
                                       [*header_values, None])]
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

    def serializer(*, topic: str, data: str | None, headers: Any = None, **kwargs: Any) -> bytes | None:
        calls.append((topic, data, headers))
        return b"<null>" if data is None else data.encode()

    record = ProducerRecord(topic=TOPIC, partition=None, key=None, value=None,
                            headers=[("h", b"1")])
    native = _native(record, key_serializer=serializer, value_serializer=serializer)
    assert native.value == b"<null>" and native.key == b"<null>"
    assert calls == [(TOPIC, None, record.headers()), (TOPIC, None, record.headers())]


def test_serializer_must_return_bytes() -> None:
    with pytest.raises(TypeError) as err:
        _native(ProducerRecord(topic=TOPIC, value=b"v"),
                value_serializer=lambda *, topic, data, headers=None, **_: "text")
    assert str(err.value) == "a serializer must return bytes or None, not str"


def test_send_failure_metadata_callback_and_future() -> None:
    # Java's AppendCallbacks hands the callback RecordMetadata(tp, -1, -1,
    # NO_TIMESTAMP, -1, -1), never null; the callback runs off the caller's
    # thread and before the future completes; the future cannot be cancelled.
    # send() waits for the record's handover, and the core's send() fires the
    # callback of a metadata TimeoutError before it returns (Java's doSend
    # does, KafkaProducer.java:1137-1149), so the callback may run before
    # send() returned the future: that future is not done either.
    caller = threading.get_ident()
    seen: list[tuple[int, bool, RecordMetadata, Exception | None]] = []
    done = threading.Event()
    holder: dict[str, Any] = {}

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        future = holder.get("f")
        seen.append((threading.get_ident(), future is not None and future.is_done(), metadata, exception))
        done.set()

    with KafkaProducer(configs=UNREACHABLE) as p:
        holder["f"] = f = p.send(record=ProducerRecord(topic=TOPIC, partition=3, key=None,
                                                       value=b"v"),
                                 callback=callback)
        assert not f.cancel(may_interrupt_if_running=True)
        assert done.wait(30)
        cause = assert_send_throws(f, KafkaTimeoutError, 30)
        assert not f.is_cancelled()
    ((thread, future_done, md, e),) = seen
    assert thread != caller
    assert not future_done
    assert e is cause
    assert str(e) == "Topic topic not present in metadata after 100 ms."
    assert (md.topic(), md.partition(), md.offset(), md.timestamp(),
            md.serialized_key_size(), md.serialized_value_size()) == (TOPIC, 3, -1, -1, -1, -1)
    assert not md.has_offset() and not md.has_timestamp()


@pytest.mark.parametrize("cls", [KafkaProducer, AsyncKafkaProducer])
def test_a_send_waits_for_its_handover_and_keeps_api_errors_in_the_future(
        cls: type[KafkaProducer[Any, Any]] | type[AsyncKafkaProducer[Any, Any]]) -> None:
    # send() returns once the record is with the Rust producer, as Java's
    # returns once doSend appended it, so it can raise what Java's doSend
    # rethrows (Critic 75 F2; the broker tests in test/integration). The
    # metadata wait's TimeoutError is an ApiException: Java gives it to the
    # callback and returns a failed future (KafkaProducer.java:1130-1142),
    # after send() waited max.block.ms.
    configs = {**UNREACHABLE, "max.block.ms": 300}
    seen: list[Exception | None] = []

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        seen.append(exception)

    async def send_async() -> tuple[float, BaseException | None]:
        p = AsyncKafkaProducer(configs=configs)
        try:
            start = time.monotonic()
            f = await p.send(record=RECORD, callback=callback)
            elapsed = time.monotonic() - start
            return elapsed, await async_failure(f, 30)
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
            error = failure(f, 30)
        finally:
            p.close(timeout=0)
    assert elapsed >= 0.3, elapsed
    assert type(error) is KafkaTimeoutError
    assert str(error) == "Topic topic not present in metadata after 300 ms."
    assert seen == [error]


_NOT_IN_A_TRANSACTION = "Cannot add partition topic-0 to transaction while in state  READY"


def _fake_queued_send(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fakes the send of a record the producer cannot take at once, queued and
    not handed over yet: the delivery callback and the handover callback send()
    passed are kept."""
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
def test_ctrl_c_while_a_send_waits_for_its_handover_raises_after_it(
        monkeypatch: pytest.MonkeyPatch, rethrown: bool) -> None:
    # Updated for the branch C API: send() waits for its record's handover,
    # the core's producer.send() holding it up to max.block.ms for metadata
    # and buffer space as Java's does, and a sync wait lets the operation
    # finish on Ctrl+C, raising KeyboardInterrupt after it. With no
    # transaction started, the handover carries what Java's doSend rethrows.
    # A Ctrl+C meanwhile is raised once the record is
    # handed over: a rethrown error then reaches neither the callback nor the
    # future (as in Java; the callback runs only if the record completes later,
    # see test_a_rethrown_send_runs_its_callback_once_if_the_record_completes),
    # an accepted record's later failure reaches both, and either way flush()
    # has nothing left to wait for (Copilot review, PR #187).
    p = KafkaProducer(configs={**UNREACHABLE, "transactional.id": "txn"})
    captured = _fake_queued_send(monkeypatch)
    seen: list[Exception | None] = []
    called = threading.Event()

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        seen.append(exception)
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
async def test_cancelling_a_send_waiting_for_its_handover_raises_after_it(
        monkeypatch: pytest.MonkeyPatch, rethrown: bool) -> None:
    # The async peer of the test above: cancelling the task awaiting send()
    # lets the wait end, then raises CancelledError.
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "transactional.id": "txn"})
    captured = _fake_queued_send(monkeypatch)
    seen: list[Exception | None] = []

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        seen.append(exception)

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
# throws (KafkaProducer.java:1110-1120), so the record completes with its batch
# and its Callback runs then, with the batch's outcome: the metadata of a
# committed record, or the error that aborted it, with the -1 metadata of
# KafkaProducer.AppendCallbacks.onCompletion (:1676-1678).
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

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        seen.append((metadata.offset(), exception))

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

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        seen.append((metadata.offset(), exception))

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
        def cb(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
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
                assert_send_throws(f, KafkaTimeoutError, 30)
    assert order == [0, 1, 2, 3]
    assert _core_callback_errors(capfd) == [f"{CALLBACK_ERROR}: LocalCallbackError: callback blew up"]
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []


def test_send_returns_only_once_its_record_is_handed_over(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's send() returns once its record is in the accumulator (doSend), so
    # a full buffer.memory blocks it up to max.block.ms: the producer's
    # backpressure. send() waits for the handover's internal future, then
    # returns the record's future; the record's delivery completes that future
    # later.
    p = KafkaProducer(configs=UNREACHABLE)
    captured = _fake_queued_send(monkeypatch)
    out: dict[str, Any] = {}

    def send() -> None:
        out["future"] = p.send(record=RECORD)

    sender = threading.Thread(target=send, daemon=True)
    try:
        sender.start()
        while "resolve" not in captured:
            time.sleep(0.01)
        sender.join(0.3)
        assert sender.is_alive(), "send() returned before its record was handed over"
        captured["resolve"](0)
        sender.join(5)
        assert not sender.is_alive()
        future = out["future"]
        assert not future.is_done()
        assert p._futures == {future._impl}  # noqa: SLF001 (flush() waits for it)
        captured["on_completion"](("topic", 0, 7, 1234, 1, 1), 0, 0)
        assert future.get(timeout=5).offset() == 7
        assert not p._futures  # noqa: SLF001
    finally:
        monkeypatch.undo()
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        p.close(timeout=0)


async def test_async_send_returns_only_once_its_record_is_handed_over(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The async peer of the test above: the awaited send() resolves with the
    # record's future once the record is handed over, the loop staying free.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    captured = _fake_queued_send(monkeypatch)
    try:
        task = asyncio.ensure_future(p.send(record=RECORD))
        while "resolve" not in captured:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.2)
        assert not task.done(), "send() returned before its record was handed over"
        captured["resolve"](0)
        future = await asyncio.wait_for(task, 5)
        assert isinstance(future, AsyncKafkaFuture)
        assert not future.is_done()
        assert p._futures == {future._impl}  # noqa: SLF001 (flush() waits for it)
        captured["on_completion"](("topic", 0, 7, 1234, 1, 1), 0, 0)
        assert (await future.get(timeout=5)).offset() == 7
        assert not p._futures  # noqa: SLF001
    finally:
        monkeypatch.undo()
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        await p.close(timeout=0)


def test_send_future_dependants_run_where_the_record_completes_after_its_callback(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # send() returns a KafkaFuture (CLAUDE.md, Python Binding Conventions,
    # Futures): its then_apply functions and when_complete actions run where
    # the record completes (the producer's completion thread; here the thread
    # the faked completion runs on), after the record's callback, as Java fires
    # the callbacks before completing the future. Registered on a done future,
    # they run inside the registering call. cancel() answers False, as Java's
    # FutureRecordMetadata.
    p = KafkaProducer(configs=UNREACHABLE)
    captured = _fake_queued_send(monkeypatch)
    order: list[tuple[str, int]] = []
    out: dict[str, Any] = {}

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        order.append(("callback", threading.get_ident()))

    def offset_of(md: RecordMetadata) -> int:
        order.append(("then_apply", threading.get_ident()))
        return md.offset()

    seen: list[tuple[RecordMetadata | None, BaseException | None]] = []

    def action(md: RecordMetadata | None, e: BaseException | None) -> None:
        order.append(("when_complete", threading.get_ident()))
        seen.append((md, e))

    sender = threading.Thread(
        target=lambda: out.__setitem__("f", p.send(record=RECORD, callback=callback)))
    try:
        sender.start()
        while "resolve" not in captured:
            time.sleep(0.01)
        captured["resolve"](0)
        sender.join(5)
        f = out["f"]
        assert isinstance(f, KafkaFuture)
        assert not f.cancel(may_interrupt_if_running=True)
        offset = f.then_apply(function=offset_of)
        completed = f.when_complete(action=action)
        assert order == [] and not offset.is_done()
        completer = threading.Thread(target=captured["on_completion"], args=(_METADATA, 0, 0))
        completer.start()
        completer.join(5)
        assert [name for name, _ in order] == ["callback", "then_apply", "when_complete"]
        assert {thread for _, thread in order} == {completer.ident}
        assert offset.get(timeout=5) == 0
        assert completed.get(timeout=5).offset() == 0
        ((md, error),) = seen
        assert md is not None and md.offset() == 0 and error is None
        late: list[int] = []
        f.then_apply(function=lambda md: late.append(threading.get_ident()))
        assert late == [threading.get_ident()]
        assert not f.cancel(may_interrupt_if_running=True)
        assert not f.to_completion_stage().cancel()
        assert not f.is_cancelled() and not f.is_completed_exceptionally()
    finally:
        monkeypatch.undo()
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        p.close(timeout=0)


async def test_async_send_future_dependants_run_on_the_loop_after_its_callback(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The asyncio peer: the record completes on the producer's event loop, so
    # its future's then_apply functions and when_complete actions run there,
    # after the callback, even when the completion arrives on another thread.
    # A failed record skips then_apply's function (Java's thenApply) and fails
    # the dependants with its error.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    loop_thread = threading.get_ident()
    order: list[tuple[str, int]] = []
    errors: list[BaseException | None] = []

    def offset_of(md: RecordMetadata) -> int:
        order.append(("then_apply", threading.get_ident()))
        return md.offset()

    def action(md: RecordMetadata | None, e: BaseException | None) -> None:
        order.append(("when_complete", threading.get_ident()))
        errors.append(e)

    async def send() -> tuple[AsyncKafkaFuture[RecordMetadata], dict[str, Any]]:
        captured = _fake_queued_send(monkeypatch)
        task = asyncio.ensure_future(p.send(
            record=RECORD, callback=lambda *, metadata, exception, **_:
                                        order.append(('callback', threading.get_ident()))))
        while "resolve" not in captured:
            await asyncio.sleep(0.01)
        captured["resolve"](0)
        f = await asyncio.wait_for(task, 5)
        assert isinstance(f, AsyncKafkaFuture)
        assert not f.cancel(may_interrupt_if_running=True)
        return f, captured

    try:
        f, captured = await send()
        offset = f.then_apply(function=offset_of)
        completed = f.when_complete(action=action)
        completer = threading.Thread(target=captured["on_completion"], args=(_METADATA, 0, 0))
        completer.start()
        completer.join(5)
        await _until(lambda: len(order) == 3)
        assert [name for name, _ in order] == ["callback", "then_apply", "when_complete"]
        assert {thread for _, thread in order} == {loop_thread}
        assert await offset.get(timeout=5) == 0
        assert (await completed.get(timeout=5)).offset() == 0 and errors == [None]

        order.clear()
        errors.clear()
        f, captured = await send()
        offset = f.then_apply(function=offset_of)
        completed = f.when_complete(action=action)
        _complete_off_loop(captured)
        await _until(lambda: len(order) == 2)
        assert [name for name, _ in order] == ["callback", "when_complete"]
        assert {thread for _, thread in order} == {loop_thread}
        cause = await async_assert_send_throws(f, KafkaTimeoutError, 5)
        assert errors == [cause]
        assert await async_assert_send_throws(offset, KafkaTimeoutError, 5) is cause
        assert await async_assert_send_throws(completed, KafkaTimeoutError, 5) is cause
        assert not f.is_cancelled() and f.is_completed_exceptionally()
    finally:
        monkeypatch.undo()
        p._futures.clear()  # noqa: SLF001 (a failure above must not hang close())
        await p.close(timeout=0)


def test_a_producer_call_from_a_record_futures_dependant_is_a_call_from_a_callback() -> None:
    # Java tests Thread.currentThread() == ioThread, so whatever runs on the
    # I/O thread counts, not only the record's Callback: a when_complete
    # action runs on the completion thread after the callback returned, and
    # a call into the producer there behaves as from the callback. flush()
    # raises KafkaError at once (it would otherwise wait for the thread it
    # blocks), and send() calls the blocking C send, which hands the record
    # over on that thread instead of waiting for it: it waits there for the
    # topic's metadata up to max.block.ms, as Java's send() does on the I/O
    # thread (see the producer module docstring). The action is registered on
    # a record sent from a callback, whose send() returns before the record
    # completes.
    p = KafkaProducer(configs={**UNREACHABLE, "max.block.ms": 300})
    out: dict[str, Any] = {}
    done = threading.Event()

    def action(md: RecordMetadata | None, e: BaseException | None) -> None:
        out["action_thread"] = threading.get_ident()
        try:
            p.flush()
        except KafkaError as error:
            out["flush"] = error
        start = time.monotonic()
        out["sent"] = p.send(record=RECORD)
        out["send_s"] = time.monotonic() - start
        done.set()

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        out["callback_thread"] = threading.get_ident()
        p.send(record=RECORD).when_complete(action=action)

    try:
        p.send(record=RECORD, callback=callback)
        assert done.wait(30)
        assert type(out["flush"]) is KafkaError and str(out["flush"]) == FLUSH_IN_CALLBACK
        assert out["action_thread"] == out["callback_thread"] != threading.get_ident()
        assert 0.25 <= out["send_s"] < 10, out["send_s"]  # max.block.ms, no hang
        assert_send_throws(out["sent"], KafkaTimeoutError, 30)
    finally:
        p.close(timeout=0)


def test_a_record_future_failed_with_a_cancelled_error_is_an_ordinary_failure() -> None:
    # Java's FutureRecordMetadata is never cancelled and boxes every error in
    # ExecutionException (valueOrError), a CancellationException included,
    # which MockProducer.errorNext(RuntimeException) can inject: get() raises
    # ExecutionError from it, waiters already in get() included.
    p = MockProducer()
    f = p.send(record=RECORD)
    stage = f.to_completion_stage()
    outcome: list[BaseException] = []
    waiter = threading.Thread(target=lambda: outcome.append(
        pytest.raises(Exception, f.get, timeout=30).value))
    waiter.start()
    time.sleep(0.05)
    injected = concurrent.futures.CancelledError()
    assert p.error_next(e=injected)  # type: ignore[arg-type]
    waiter.join(10)
    assert not waiter.is_alive()
    (error,) = outcome
    assert type(error) is ExecutionError and error.__cause__ is injected
    assert assert_send_throws(f, concurrent.futures.CancelledError) is injected
    assert not f.is_cancelled() and f.is_completed_exceptionally() and f.is_done()
    assert stage.done() and not stage.cancelled() and stage.exception() is injected


async def test_an_async_record_future_failed_with_a_cancelled_error_is_an_ordinary_failure(
        ) -> None:
    p = AsyncMockProducer()
    f = await p.send(record=RECORD)
    waiting = asyncio.ensure_future(f.get(timeout=30))
    await asyncio.sleep(0.05)
    injected = asyncio.CancelledError()
    assert p.error_next(e=injected)  # type: ignore[arg-type]
    done, _ = await asyncio.wait((waiting,), timeout=5)
    assert done, "a get() waiting when the error arrived never returned"
    error = waiting.exception()
    assert type(error) is ExecutionError and error.__cause__ is injected
    assert await async_assert_send_throws(f, asyncio.CancelledError) is injected
    assert not f.is_cancelled() and f.is_completed_exceptionally()
    await p.close()


async def test_a_refused_inner_cancel_still_releases_the_waiters() -> None:
    # The inner future of a record's future refuses cancellation; were its
    # state ever cancelled, settling it must still release a get() already
    # waiting, which then answers from the state, for both kinds.
    impl: _KafkaFutureImpl[RecordMetadata] = _KafkaFutureImpl()
    f = RecordFuture._of_impl(impl)
    outcome: list[BaseException] = []
    waiter = threading.Thread(target=lambda: outcome.append(
        pytest.raises(BaseException, f.get, timeout=30).value))
    waiter.start()
    await asyncio.sleep(0.05)
    assert impl.cancel(concurrent.futures.CancelledError())
    waiter.join(10)
    assert not waiter.is_alive()
    assert type(outcome[0]) is concurrent.futures.CancelledError
    assert f.to_completion_stage().done()

    impl2: _KafkaFutureImpl[RecordMetadata] = _KafkaFutureImpl()
    g = AsyncRecordFuture._of_impl(impl2, asyncio.get_running_loop())
    waiting = asyncio.ensure_future(g.get(timeout=30))
    await asyncio.sleep(0.05)
    assert impl2.cancel(asyncio.CancelledError())
    done, _ = await asyncio.wait((waiting,), timeout=5)
    assert done and waiting.cancelled()
    assert g.to_completion_stage().done()


def test_mock_send_future_dependants_run_in_the_completing_call() -> None:
    # On MockProducer the record completes inside the call that completes it
    # (complete_next here): its future's dependants run there, after the
    # callback.
    order: list[str] = []
    p = MockProducer()
    f = p.send(record=RECORD, callback=lambda *, metadata, exception, **_: order.append('callback'))
    assert isinstance(f, KafkaFuture)
    offset = f.then_apply(function=lambda md: order.append("then_apply") or md.offset())
    assert order == [] and not f.cancel(may_interrupt_if_running=True)
    assert p.complete_next()
    assert order == ["callback", "then_apply"]
    assert offset.get_now(value_if_absent=-1) == 0


async def test_async_mock_send_returns_an_async_kafka_future() -> None:
    p = AsyncMockProducer()
    f = await p.send(record=RECORD)
    assert isinstance(f, AsyncKafkaFuture)
    assert not f.cancel(may_interrupt_if_running=True)
    p.error_next(e=KafkaError(message="failed"))
    assert str(await async_assert_send_throws(f, KafkaError)) == "failed"
    await p.close()


def test_flush_waits_for_the_records_sent_before_it() -> None:
    # Java's flush returns once the records sent before it have completed
    # (their callbacks run first).
    with KafkaProducer(configs=UNREACHABLE) as p:
        seen: list[int] = []
        futures = [p.send(record=RECORD, callback=lambda *, metadata, exception, i=i, **_:
                                                      seen.append(i))
                   for i in range(3)]
        p.flush()
        assert all(f.is_done() for f in futures)
        assert seen == [0, 1, 2]


def test_flush_in_a_callback_raises_and_close_in_a_callback_does_not_deadlock(
        caplog: pytest.LogCaptureFixture) -> None:
    p = KafkaProducer(configs=UNREACHABLE)
    out: dict[str, Any] = {}
    done = threading.Event()

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
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
    # in-callback check (KafkaProducer.java:1298-1300).
    p = KafkaProducer(configs=UNREACHABLE)
    out: dict[str, Any] = {}
    done = threading.Event()

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
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
        assert f.is_done() and not f.is_cancelled()
        assert_send_throws(f, KafkaTimeoutError)


SENDERS = 5


def _send_outcome(sent: KafkaFuture[RecordMetadata] | BaseException) -> BaseException | None:
    """The error of a send that a close ended: what its send() raised, or its
    future's error."""
    if isinstance(sent, BaseException):
        return sent
    assert sent.is_done() and not sent.is_cancelled()
    return failure(sent)


def test_timed_close_is_bounded_and_fails_what_it_could_not_send() -> None:
    # Updated for the branch C API: it has no test pause; the 60 s max.block.ms
    # alone keeps the first record waiting for metadata. send() waits for its
    # record's handover as Java's does, so each record is sent from a thread
    # of its own: the first waits for metadata, the others behind it.
    p = KafkaProducer(configs={"bootstrap.servers": "127.0.0.1:59999"})  # max.block.ms 60 s
    outcomes: list[KafkaFuture[RecordMetadata] | BaseException] = []

    def send() -> None:
        try:
            outcomes.append(p.send(record=RECORD))
        except BaseException as e:  # noqa: BLE001 - checked below
            outcomes.append(e)

    senders = [threading.Thread(target=send, daemon=True) for _ in range(SENDERS)]
    for sender in senders:
        sender.start()
    time.sleep(0.2)  # the records are with the submission task
    start = time.monotonic()
    p.close(timeout=1.0)
    assert time.monotonic() - start < 20
    for sender in senders:
        sender.join(10)
        assert not sender.is_alive(), "close() did not end a waiting send()"
    # Every record failed with the close: its send() raising, or its future
    # failing.
    errors = [_send_outcome(sent) for sent in outcomes]
    assert len(errors) == SENDERS
    assert all(isinstance(e, (KafkaError, IllegalStateError)) for e in errors), errors
def test_timed_close_is_bounded_and_wakes_a_send_waiting_for_metadata() -> None:
    # A send waiting for its topic's metadata (max.block.ms 60 s) does not hold
    # a timed close: the close wakes it, and it raises Java's
    # KafkaError("Producer closed while send in progress"), as doSend does when
    # the metadata closes during its wait (KafkaProducer.java:1068-1074).
    p = KafkaProducer(configs={"bootstrap.servers": "127.0.0.1:59999"})  # max.block.ms 60 s
    raised: list[BaseException] = []

    def send() -> None:
        try:
            p.send(record=RECORD)
        except BaseException as error:  # noqa: BLE001 - the test reads which one
            raised.append(error)

    sender = threading.Thread(target=send, daemon=True)
    sender.start()
    time.sleep(0.5)  # the send waits for its topic's metadata
    start = time.monotonic()
    p.close(timeout=1.0)
    assert time.monotonic() - start < 20
    sender.join(20)
    assert not sender.is_alive()
    ((error,),) = (raised,)
    assert type(error) is KafkaError
    assert str(error) == "Producer closed while send in progress"
    with pytest.raises(IllegalStateError) as err:
        p.send(record=RECORD)
    assert str(err.value) == CLOSED


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

    def slow(*, topic: str, data: bytes | None, headers: Any = None, **kwargs: Any) -> bytes | None:
        started.set()
        time.sleep(0.5)
        return data

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
        # The close runs to its end on another thread before the call goes on.
        # Updated for the one-loop binding: a close() from another loop is
        # refused, so the thread is the one the close of an async def callback
        # frees the handle on (_close_from_callback), the only one besides the
        # producer's loop.
        passed()
        p._check_not_closed = passed  # type: ignore[method-assign]  # noqa: SLF001
        assert p._begin_close()  # noqa: SLF001
        closer = threading.Thread(target=p._finish_close_from_callback)  # noqa: SLF001
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


def test_close_returns_after_every_delivery_callback_has_run() -> None:
    # The C poll thread runs the delivery callbacks; close() joins it (in
    # Producer_destroy), as Java's close() joins its I/O thread, so none runs
    # after close() returns.
    ran: list[Exception | None] = []

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        time.sleep(0.02)
        ran.append(exception)

    p = KafkaProducer(configs=UNREACHABLE)
    for _ in range(10):
        p.send(record=RECORD, callback=callback)
    p.close()
    assert len(ran) == 10


async def test_async_close_returns_after_every_delivery_callback_has_run() -> None:
    ran: list[Exception | None] = []

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        time.sleep(0.02)
        ran.append(exception)

    p = AsyncKafkaProducer(configs=UNREACHABLE)
    for _ in range(10):
        await p.send(record=RECORD, callback=callback)
    await p.close()
    assert len(ran) == 10


# What a successful completion's delivery callback receives from the C
# extension: (topic, partition, offset, timestamp, key size, value size).
_METADATA = ("t", 0, 0, -1, -1, 1)


def test_a_record_that_completes_before_send_tracks_it_is_not_kept(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The completion untracks a record's future right after completing it. It
    # can run on the C completion thread before send() tracks the future; then
    # send() finds the future done and does not keep it, so a later flush()
    # does not hold it for ever.
    # Adapted to the branch C API: the send is Producer_send_cb, whose
    # delivery callback takes (metadata, error, callback_id); 0 is success.
    # Updated for send() waiting for its handover: the record completes just
    # before it is handed over.
    def complete_at_once(c_producer: int, native: object, cb: Any, resolve: Any) -> int:
        cb(_METADATA, 0, 0)
        resolve(0)
        return 0

    monkeypatch.setattr(_lib, "Producer_send_cb", complete_at_once)
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        future = p.send(record=RECORD)
        assert future.is_done()
        assert not p._futures  # noqa: SLF001
    finally:
        monkeypatch.undo()
        p.close(timeout=0)


def test_the_completion_untracks_its_future_without_a_done_callback(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # No listener of the producer's own is added to a record's future, and no
    # inner future is made for it unless a wait happens (CLAUDE.md, Python
    # Binding Conventions, Futures: the lazy design): the completion removes
    # the future's Python state from the tracked set itself.
    captured: dict[str, Any] = {}

    def hold(c_producer: int, native: object, cb: Any, resolve: Any) -> int:
        captured["cb"] = cb
        resolve(0)  # handed over, its completion held
        return 0

    monkeypatch.setattr(_lib, "Producer_send_cb", hold)
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        future = p.send(record=RECORD)
        impl = future._impl  # noqa: SLF001
        assert impl in p._futures  # noqa: SLF001
        assert impl._listeners is None and future._inner is None  # noqa: SLF001
        captured["cb"](_METADATA, 0, 0)
        assert future.is_done()
        assert impl not in p._futures  # noqa: SLF001
        assert future._inner is None  # noqa: SLF001 - nobody waited
    finally:
        monkeypatch.undo()
        p.close(timeout=0)


async def test_the_async_send_future_is_its_own_state_in_one_object(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # One object per record: the AsyncKafkaFuture send() returns is the
    # record's own future state, its lock the producer's, and the completion
    # the C send gets; a dependant built from it is an ordinary future.
    captured: dict[str, Any] = {}

    def hold(c_producer: int, native: object, cb: Any, resolve: Any) -> int:
        captured["cb"] = cb
        resolve(0)  # handed over, its completion held
        return 0

    monkeypatch.setattr(_lib, "Producer_send_cb", hold)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        future = await p.send(record=RECORD)
        assert isinstance(future, AsyncRecordFuture) and isinstance(future, AsyncKafkaFuture)
        assert captured["cb"] is future
        assert future._impl is future and future._lock is p._fire_lock  # noqa: SLF001
        # No self-reference and no __dict__: freed by its reference count.
        assert future not in gc.get_referents(future) and not hasattr(future, "__dict__")
        assert str(future) == "KafkaFuture{value=None,exception=None,done=False}"
        assert not future.is_done() and future.get_now(value_if_absent=None) is None
        dependant = future.then_apply(function=RecordMetadata.offset)
        assert isinstance(dependant, AsyncKafkaFuture) and not isinstance(dependant, AsyncRecordFuture)
        seen: list[tuple[Any, Any]] = []
        future.when_complete(action=lambda md, exc: seen.append((md, exc)))
        captured["cb"](_METADATA, 0, 0)
        assert future.is_done() and future not in p._futures  # noqa: SLF001
        metadata = await future.get()
        assert metadata.offset() == _METADATA[2]
        assert await dependant.get() == _METADATA[2]
        assert seen == [(metadata, None)]
    finally:
        monkeypatch.undo()
        await p.close(timeout=0)


async def test_an_async_send_future_cannot_be_cancelled_and_is_untracked_by_its_completion(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Java's FutureRecordMetadata.cancel() returns false: the future stays
    # tracked until its completion arrives, which completes it normally.
    captured: dict[str, Any] = {}

    def hold(c_producer: int, native: object, cb: Any, resolve: Any) -> int:
        captured["cb"] = cb
        resolve(0)  # handed over, its completion held
        return 0

    monkeypatch.setattr(_lib, "Producer_send_cb", hold)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        future = await p.send(record=RECORD)
        assert not future.cancel(may_interrupt_if_running=True)
        assert not future.to_completion_stage().cancel()
        assert not future.is_cancelled() and not future.is_done()
        assert future._impl in p._futures  # noqa: SLF001
        completer = threading.Thread(target=captured["cb"], args=(_METADATA, 0, 0))
        completer.start()
        completer.join(5)
        for _ in range(500):
            if future._impl not in p._futures:  # noqa: SLF001
                break
            await asyncio.sleep(0.01)
        assert future._impl not in p._futures  # noqa: SLF001
        assert not future.is_cancelled()
        assert (await future.get(timeout=5)).offset() == _METADATA[2]
    finally:
        monkeypatch.undo()
        await p.close(timeout=0)


# ---------------------------------------------------------------------------
# Tracking and close, together: many senders, completions out of order and
# before send() tracks its future, cancellations, and close() with records in
# flight. Every completion untracks its future (no done callback does it).
# ---------------------------------------------------------------------------

def _completing_send(monkeypatch: pytest.MonkeyPatch, seed: int) -> list[threading.Thread]:
    """Fake the C send: each record completes successfully, a third of them at
    once inside the call (before send() tracks the future), the rest on another
    thread after a short random delay (out of order), as the C completion
    thread would. Returns the completer threads, for the caller to join.

    Adapted to this branch's C API: the core never runs a delivery callback
    inside ``Producer_send_cb`` (it queues it for the callback pump), and the
    pump on an event loop runs a completion inline when nothing is queued
    before it there. So on a running loop the "at once" completion runs as
    that pump would, in a call the loop makes after send() returns.
    Updated for send() waiting for its handover: every record is handed over
    (``resolve(0)``), an "at once" completion just before it, which on a
    running loop is a call the loop makes before the one handing it over."""
    rng = random.Random(seed)
    lock = threading.Lock()
    threads: list[threading.Thread] = []

    def producer_send(c_producer: int, native: Any, cb: Any, resolve: Any) -> int:
        with lock:
            at_once = rng.random() < 0.33
            delay = rng.random() * 0.003
        if at_once:
            try:
                loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is None:
                cb(_METADATA, 0, 0)
                resolve(0)
            else:
                loop.call_soon(cb, _METADATA, 0, 0)
                loop.call_soon(resolve, 0)
        else:
            resolve(0)
            t = threading.Thread(target=lambda: (time.sleep(delay), cb(_METADATA, 0, 0)),
                                 daemon=True)
            with lock:
                threads.append(t)
            t.start()
        return 0

    monkeypatch.setattr(_lib, "Producer_send_cb", producer_send)
    return threads


def test_many_senders_with_completions_out_of_order_leave_nothing_tracked(
        monkeypatch: pytest.MonkeyPatch) -> None:
    completers = _completing_send(monkeypatch, seed=1)
    p = KafkaProducer(configs=UNREACHABLE)
    callbacks: list[None] = []
    done_callbacks: list[None] = []
    futures: list[Any] = []
    lock = threading.Lock()

    def sender() -> None:
        for _ in range(250):
            f = p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                                   callbacks.append(None))
            f.when_complete(action=lambda md, e: done_callbacks.append(None))
            with lock:
                futures.append(f)

    try:
        threads = [threading.Thread(target=sender) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        p.flush()
        # flush() waited for every record sent before it to complete.
        assert all(f.is_done() for f in futures) and len(futures) == 1000
        assert len(callbacks) == 1000
        # A future's when_complete actions, and its completion's untracking,
        # run on the completing thread around waking flush()'s wait: join
        # those threads, then nothing is left tracked.
        for t in list(completers):
            t.join(5)
        assert len(done_callbacks) == 1000
        assert not p._futures  # noqa: SLF001
    finally:
        for t in list(completers):
            t.join(5)
        monkeypatch.undo()
        p.close(timeout=0)


async def test_async_senders_with_refused_cancellations_leave_nothing_tracked(
        monkeypatch: pytest.MonkeyPatch) -> None:
    completers = _completing_send(monkeypatch, seed=2)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    callbacks: list[None] = []
    done_callbacks: list[None] = []
    futures: list[Any] = []
    cancelled: list[None] = []

    async def sender(n: int) -> None:
        for i in range(200):
            f = await p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                                         callbacks.append(None))
            f.when_complete(action=lambda md, e: done_callbacks.append(None))
            # cancel() answers False and cancels nothing (Java's
            # FutureRecordMetadata): the completion still runs the callback,
            # completes the future and untracks it.
            if i % 10 == n and f.cancel(may_interrupt_if_running=False):
                cancelled.append(None)
            futures.append(f)

    try:
        await asyncio.gather(*(sender(n) for n in range(4)))
        await p.flush()
        for t in list(completers):
            t.join(5)
        for _ in range(500):  # the completions may still be on their way
            if len(callbacks) == 800 and not p._futures:  # noqa: SLF001
                break
            await asyncio.sleep(0.01)
        assert all(f.is_done() for f in futures) and len(futures) == 800
        assert sum(f.is_cancelled() for f in futures) == len(cancelled) == 0
        assert len(callbacks) == 800 and len(done_callbacks) == 800
        assert not p._futures  # noqa: SLF001
    finally:
        monkeypatch.undo()
        await p.close(timeout=0)


def test_close_with_records_in_flight_from_several_threads_completes_and_untracks_all() -> None:
    # Real native path: the records wait for metadata from an unreachable
    # broker and fail after max.block.ms. close() returns only after every
    # callback has run, every future is done, and none is left tracked.
    p = KafkaProducer(configs=UNREACHABLE)
    callbacks: list[Exception | None] = []
    futures: list[Any] = []
    lock = threading.Lock()
    started = threading.Barrier(4)

    def sender() -> None:
        started.wait()
        for _ in range(4):
            f = p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                                   callbacks.append(exception))
            with lock:
                futures.append(f)

    threads = [threading.Thread(target=sender) for _ in range(3)]
    for t in threads:
        t.start()
    started.wait()
    for t in threads:
        t.join(30)
    p.close()
    assert len(futures) == 12 and all(f.is_done() for f in futures)
    assert len(callbacks) == 12
    assert not p._futures  # noqa: SLF001


async def test_async_close_with_records_in_flight_from_several_tasks_completes_and_untracks_all(
        ) -> None:
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    callbacks: list[Exception | None] = []
    futures: list[Any] = []

    async def sender() -> None:
        for _ in range(4):
            futures.append(await p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                                                    callbacks.append(exception)))

    await asyncio.gather(*(sender() for _ in range(3)))
    await p.close()
    assert len(futures) == 12 and all(f.is_done() for f in futures)
    assert len(callbacks) == 12
    assert not p._futures  # noqa: SLF001


def test_close_from_a_callback_with_records_in_flight_completes_the_rest() -> None:
    # The first record's callback closes the producer (from the completion
    # thread, so with a zero timeout); the records still in flight complete
    # anyway, their callbacks run, and nothing stays tracked. send() waits for
    # its record's handover, so the records are sent from threads of their
    # own: each send() returns its future (its callback then runs, once) or,
    # for a record the close refused before the producer took it, raises.
    p = KafkaProducer(configs={**UNREACHABLE, "max.block.ms": 300})
    callbacks: list[Exception | None] = []
    closed = threading.Event()
    outcomes: list[KafkaFuture[RecordMetadata] | BaseException] = []

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        callbacks.append(exception)
        p.close()  # the first one closes; closing again is harmless
        closed.set()

    def send() -> None:
        try:
            outcomes.append(p.send(record=RECORD, callback=callback))
        except BaseException as e:  # noqa: BLE001 - checked below
            outcomes.append(e)

    senders = [threading.Thread(target=send, daemon=True) for _ in range(SENDERS + 1)]
    for sender in senders:
        sender.start()
    assert closed.wait(30)
    for sender in senders:
        sender.join(30)
        assert not sender.is_alive()
    futures = [o for o in outcomes if not isinstance(o, BaseException)]
    raised = [o for o in outcomes if isinstance(o, BaseException)]
    assert all(isinstance(e, (KafkaError, IllegalStateError)) for e in raised), raised
    deadline = time.monotonic() + 30
    while (len(callbacks) < len(futures) or p._futures) and time.monotonic() < deadline:  # noqa: SLF001
        time.sleep(0.01)
    assert len(futures) >= 1 and len(callbacks) == len(futures)
    assert all(f.is_done() for f in futures)
    assert not p._futures  # noqa: SLF001
    assert p._closed  # noqa: SLF001


def test_an_async_send_from_another_loop_is_refused_before_its_serializer() -> None:
    # Updated for the one-loop binding (was: a send() on another loop racing
    # close() is refused after its serializer, which only the sync producer
    # can still race): a send() from a loop other than the producer's raises
    # RuntimeError before its serializer runs, and the producer stays usable.
    serialized: list[bytes | None] = []

    def recording(*, topic: str, data: bytes | None, headers: Any = None, **kwargs: Any) -> bytes | None:
        serialized.append(data)
        return data

    async def main() -> None:
        p = AsyncKafkaProducer(configs=UNREACHABLE, value_serializer=recording)
        loop = asyncio.get_running_loop()
        with pytest.raises(RuntimeError) as err:
            await loop.run_in_executor(None, lambda: asyncio.run(p.send(record=RECORD)))
        assert str(err.value) == BOUND_ELSEWHERE.format(cls="AsyncKafkaProducer")
        assert serialized == []
        await p.send(record=RECORD)
        assert serialized == [RECORD.value()]
        await p.close(timeout=0)

    asyncio.run(main())


def _count_submissions(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Count the calls of the real ``Producer_send_cb`` (one element each)."""
    count: list[int] = []
    real = _lib.Producer_send_cb

    def send_cb(*args: Any) -> Any:
        result = real(*args)
        count.append(1)
        return result

    monkeypatch.setattr(_lib, "Producer_send_cb", send_cb)
    return count


def _wait_for(condition: Callable[[], bool], timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)


@pytest.mark.parametrize("cls", [KafkaProducer, AsyncKafkaProducer])
def test_one_drain_keeps_the_gil_from_the_first_callback_to_the_last(
        monkeypatch: pytest.MonkeyPatch, cls: type) -> None:
    # `<Client>__execute_callbacks` runs a drain's callbacks with the GIL held
    # throughout, instead of releasing it after each callback and taking it
    # again for the next. A thread made runnable by the first callback can
    # therefore run only once the drain is over, not between two callbacks.
    # The long switch interval keeps the interpreter from handing the GIL over
    # on its own, so only an explicit release would let the thread in; the
    # garbage collector is off, as a finalizer it ran inside the drain (an
    # earlier client's teardown) could release the GIL itself.
    released = threading.Event()
    other_ran: list[str] = []
    seen: list[list[str]] = []
    ran = 0

    def other() -> None:
        released.wait()
        other_ran.append("other")

    # Every gap between two callbacks is a chance for a release to let the
    # thread in. The records wait for metadata one after the other, max.block.ms
    # each, so five fail well within the second the test waits. send() waits
    # for its record's handover, so the five are sent at once (from threads, or
    # tasks), and the callbacks act by the order they run in: the first
    # releases the thread, the last records whether it ran.
    records = 5

    def callback(*, metadata: Any, exception: BaseException | None, **kwargs: Any) -> None:
        nonlocal ran
        ran += 1
        if ran == 1:
            released.set()
        elif ran == records:
            seen.append(list(other_ran))

    submitted = _count_submissions(monkeypatch)
    thread = threading.Thread(target=other)
    thread.start()
    interval = sys.getswitchinterval()
    gc.collect()
    gc.disable()
    try:
        if cls is KafkaProducer:
            p = KafkaProducer(configs=UNREACHABLE)
            futures: list[KafkaFuture[RecordMetadata]] = []

            def send() -> None:
                futures.append(p.send(record=RECORD, callback=callback))

            senders = [threading.Thread(target=send) for _ in range(records)]
            for sender in senders:
                sender.start()
            _wait_for(lambda: len(submitted) == records)
            # The records fail after max.block.ms; holding the GIL keeps the
            # completion thread from draining before all are queued.
            sys.setswitchinterval(30)
            hold_gil_until = time.monotonic() + 1
            while time.monotonic() < hold_gil_until:
                pass
            for sender in senders:
                sender.join(30)
            p.flush()
            assert len(futures) == records
            for future in futures:
                assert_send_throws(future, KafkaTimeoutError)
            p.close(timeout=0)
        else:
            async def main() -> None:
                p = AsyncKafkaProducer(configs=UNREACHABLE)
                # The futures are kept: one freed unretrieved inside the drain
                # would log, and the log's stream write releases the GIL.
                sends = [asyncio.ensure_future(p.send(record=RECORD, callback=callback))
                         for _ in range(records)]
                while len(submitted) < records:
                    await asyncio.sleep(0.01)
                # Block the loop until every record failed, so one pump drains
                # all the completions.
                sys.setswitchinterval(30)
                time.sleep(1)
                futures = await asyncio.gather(*sends)
                await p.flush()
                for future in futures:
                    await async_assert_send_throws(future, KafkaTimeoutError)
                await p.close(timeout=0)

            asyncio.run(main())
    finally:
        sys.setswitchinterval(interval)
        gc.enable()
        released.set()
        thread.join()
    assert seen == [[]]
    assert other_ran == ["other"]


def test_a_cancelled_async_close_is_forced_and_raises_promptly() -> None:
    # Critic 75 F1, the async analog of KafkaProducerTest's
    # shouldCloseProperlyAndThrowIfInterrupted (see test_kafka_producer.py):
    # close() waits for the callbacks of the records sent before it, here an
    # async def one that has not ended. Cancelling the task awaiting close()
    # force-closes the producer (KafkaProducer.java:1514-1525) and frees it,
    # which completes the callback's report, and re-raises CancelledError
    # instead of waiting for the callback; the callback still ends on the loop,
    # and its record's future completes then. The callback is released after
    # 5 s in any case, so a close that waited for it fails the test.
    async def main() -> None:
        loop = asyncio.get_running_loop()
        entered = asyncio.Event()
        release = asyncio.Event()
        loop.call_later(5, release.set)

        async def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
            entered.set()
            await release.wait()

        p = AsyncKafkaProducer(configs=UNREACHABLE)
        future = await p.send(record=RECORD, callback=callback)
        await asyncio.wait_for(entered.wait(), 10)
        task = asyncio.ensure_future(p.close())
        await asyncio.sleep(0.2)
        assert not task.done()  # close() waits for the callback
        start = loop.time()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert loop.time() - start < 4
        assert not future.is_done()  # its callback has not ended
        with pytest.raises(IllegalStateError) as err:
            await p.send(record=RECORD)
        assert str(err.value) == CLOSED
        await p.close()  # closing again is harmless
        release.set()
        await async_assert_send_throws(future, KafkaTimeoutError, 10)

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
    assert_send_throws(f, KafkaTimeoutError)
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
    await async_assert_send_throws(f, KafkaTimeoutError)
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
    assert f.is_done()
    with pytest.raises(IllegalStateError):
        p.flush()


# ===========================================================================
# flush and the transaction-control operations beside a send in progress
# ===========================================================================

# A send to the unreachable broker waits this long for its topic's metadata,
# inside send() (max.block.ms).
HELD_MS = 500


@pytest.mark.parametrize("operation", ["flush", "init_transactions", "begin_transaction",
                                       "commit_transaction", "abort_transaction"])
def test_control_operations_do_not_wait_for_a_send_in_progress(operation: str) -> None:
    # A send that has not returned has not reached the accumulator, as in Java,
    # whose doSend appends the record after its metadata wait
    # (KafkaProducer.java:1065-1111): Java's flush waits only for the batches
    # appended (RecordAccumulator.awaitFlushCompletion), and the
    # transaction-control operations do not wait for a send either. So each
    # returns, or raises Java's error, while another thread's send waits for
    # its topic's metadata, and that send then ends as usual, with the metadata
    # wait's TimeoutError in its future. A send that returned before the call
    # is in the accumulator, so in the call (producer-transactions.md §13).
    p = KafkaProducer(configs={**UNREACHABLE, "max.block.ms": HELD_MS})
    try:
        futures: list[KafkaFuture[RecordMetadata]] = []
        sender = threading.Thread(target=lambda: futures.append(p.send(record=RECORD)))
        sender.start()
        time.sleep(0.05)  # the send waits for its topic's metadata
        if operation == "flush":
            p.flush()
        else:
            with pytest.raises(IllegalStateError) as err:
                getattr(p, operation)()
            assert str(err.value) == NOT_TRANSACTIONAL
        assert sender.is_alive(), f"{operation} waited for the send in progress"
        sender.join(30)
        (future,) = futures
        assert_send_throws(future, KafkaTimeoutError, 0)
    finally:
        p.close(timeout=0)


# The sync KafkaProducer never calls a blocking C entry point (branch commit
# 2942d0921): every blocking-in-Java operation is submitted through its _cb
# twin and waited for in Python in 100 ms slices, so Ctrl+C is honoured. The
# one exception is a send() from a delivery callback, which calls the blocking
# Producer_send (see test_a_send_from_a_callback_calls_the_blocking_send).
_BLOCKING_ENTRY_POINTS = (
    "Producer_send", "Producer_flush", "Producer_partitions_for",
    "Producer_init_transactions", "Producer_begin_transaction",
    "Producer_send_offsets_to_transaction",
    "Producer_commit_transaction", "Producer_abort_transaction", "Producer_close",
)


def test_the_extension_hands_out_no_record_future() -> None:
    # No producer hands out a record future over a C handle, whose C get would
    # run the producer's queued callbacks on the calling thread: the record's
    # outcome reaches Python only through its delivery callback, and both
    # sends, Producer_send_cb and the blocking Producer_send a send() from a
    # delivery callback calls, return only their error (see
    # test_a_send_from_a_callback_calls_the_blocking_send).
    assert not hasattr(_lib, "KAFKA_FUTURE_CONV_RECORD_METADATA")


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
    future = p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                                got.append(exception))
    for call in (p.init_transactions, p.begin_transaction, p.commit_transaction,
                 p.abort_transaction):
        with pytest.raises(IllegalStateError) as err:
            call()
        assert str(err.value) == NOT_TRANSACTIONAL
    p.flush()
    assert_send_throws(future, KafkaTimeoutError, 0)
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
        # They make Java 4.4 await the offsets' topic metadata (awaitTopicMetadata,
        # KafkaProducer.java:800-805) before ensureTransactional. Both outcomes are
        # Java-faithful, as in the C twin (test_kafka_producer.c): the unreachable
        # cluster times the wait out, or the sender's lazy bootstrap lands after the
        # wait took the metadata version (Metadata.java:309 bumps it), ending the
        # wait early, and ensureTransactional rejects the call. Either way the
        # error comes through the completion the call waits on.
        offsets = {TopicPartition(topic=TOPIC, partition=0): OffsetAndMetadata(offset=1)}
        with pytest.raises((KafkaTimeoutError, IllegalStateError)) as err:
            p.send_offsets_to_transaction(offsets=offsets, group_metadata=group_metadata)
        if isinstance(err.value, KafkaTimeoutError):
            assert str(err.value) == "Failed to update metadata after 100 ms."
        else:
            assert str(err.value) == NOT_TRANSACTIONAL


def test_a_send_from_a_callback_calls_the_blocking_send(monkeypatch: pytest.MonkeyPatch) -> None:
    # A record's handover runs on the completion thread, which a delivery
    # callback blocks: a send() from a callback calls the blocking C send
    # instead, which hands the record over on that thread, as Java's send()
    # from a callback runs doSend on the I/O thread (see the producer module
    # docstring). The second record waits there for its topic's metadata and
    # fails it as the first did; nothing deadlocks.
    entry_points: list[str] = []
    returned: list[Any] = []

    def spy(name: str) -> Any:
        real = getattr(_lib, name)

        def entry(*args: Any) -> Any:
            entry_points.append(name)
            result = real(*args)
            returned.append(result)
            return result

        return entry

    for name in ("Producer_send", "Producer_send_cb"):
        monkeypatch.setattr(_lib, name, spy(name))
    p = KafkaProducer(configs=UNREACHABLE)
    errors: list[Exception | None] = []
    inner: list[KafkaFuture[RecordMetadata]] = []
    outer: list[KafkaFuture[RecordMetadata]] = []

    def second(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        errors.append(exception)

    def first(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        errors.append(exception)
        inner.append(p.send(record=RECORD, callback=second))

    sender = threading.Thread(
        target=lambda: outer.append(p.send(record=RECORD, callback=first)), daemon=True)
    sender.start()
    sender.join(30)
    assert not sender.is_alive(), "a send() from a callback deadlocked"
    try:
        (f,) = outer
        (g,) = inner
        for future in (f, g):
            assert_send_throws(future, KafkaTimeoutError, 30)
        assert entry_points == ["Producer_send_cb", "Producer_send"]
        # Each returns only its error: no record future crosses over a C
        # handle (test_the_extension_hands_out_no_record_future).
        assert returned == [0, 0]
        assert len(errors) == 2
        assert all(isinstance(e, KafkaTimeoutError) for e in errors)
    finally:
        p.close(timeout=0)


def test_sends_between_two_producers_callbacks_call_the_blocking_send(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Two producers whose callbacks send to each other, neither able to take
    # the record at once: each callback's send() calls the blocking C send,
    # which waits on that completion thread for the topic's metadata up to
    # max.block.ms, as Java's send() blocks the I/O thread in waitOnMetadata
    # and times out. Waiting for the other producer's handover instead would
    # hang both, each waiting for the completion thread the other's callback
    # blocks.
    entry_points: list[str] = []

    def spy(name: str) -> Any:
        real = getattr(_lib, name)

        def entry(*args: Any) -> Any:
            entry_points.append(name)
            return real(*args)

        return entry

    for name in ("Producer_send", "Producer_send_cb"):
        monkeypatch.setattr(_lib, name, spy(name))
    a = KafkaProducer(configs=UNREACHABLE)
    b = KafkaProducer(configs=UNREACHABLE)
    inner: list[KafkaFuture[RecordMetadata]] = []
    outer: list[KafkaFuture[RecordMetadata]] = []

    def sending_to(other: KafkaProducer[bytes, bytes]) -> Callback:
        def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
            inner.append(other.send(record=RECORD))

        return callback

    def send_first(p: KafkaProducer[bytes, bytes], other: KafkaProducer[bytes, bytes]) -> None:
        outer.append(p.send(record=RECORD, callback=sending_to(other)))

    # Sent together, so that both first records time out, and both callbacks
    # run, at about the same time.
    senders = [threading.Thread(target=send_first, args=pair, daemon=True)
               for pair in ((a, b), (b, a))]
    for sender in senders:
        sender.start()
    for sender in senders:
        sender.join(30)
    assert not any(sender.is_alive() for sender in senders), "the callbacks' sends deadlocked"
    try:
        assert (len(outer), len(inner)) == (2, 2)
        for future in outer + inner:
            assert_send_throws(future, KafkaTimeoutError, 30)
        assert sorted(entry_points) == ["Producer_send"] * 2 + ["Producer_send_cb"] * 2
    finally:
        a.close(timeout=0)
        b.close(timeout=0)


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
        assert_send_throws(p.send(record=RECORD), KafkaTimeoutError, 30)
    finally:
        p.close(timeout=0)


def test_send_offsets_to_transaction_checks_group_metadata_first() -> None:
    # Java: throwIfInvalidGroupMetadata, throwIfNoTransactionManager,
    # throwIfProducerClosed, then nothing to send for empty offsets. The core
    # makes the transaction-manager check (it takes a KafkaConsumer's metadata).
    with KafkaProducer(configs={**UNREACHABLE, "enable.idempotence": False}) as p:
        with pytest.raises(IllegalArgumentError) as err:
            p.send_offsets_to_transaction(offsets={}, group_metadata=None)  # type: ignore[arg-type]
        assert str(err.value) == "Consumer group metadata could not be None"
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
        with pytest.raises(UnsupportedOperationError) as err:
            p.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata())
        assert str(err.value) == NO_GROUP_METADATA_HANDLE_MESSAGE


async def test_async_send_offsets_to_transaction_needs_a_kafka_consumers_group_metadata() -> None:  # noqa: E501
    async with AsyncKafkaProducer(configs=UNREACHABLE) as p:
        with pytest.raises(UnsupportedOperationError) as err:
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
    # throwIfProducerClosed() (KafkaProducer.java:709-710, 735-736, 797-798,
    # 871-872, 905-906); here the core makes that check, which a closed producer
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
# Backpressure: send() waits for its record's handover, as Java's does
# ===========================================================================

# The bound the no-memory children are handed (their prelude reads it).
BACKPRESSURE_BOUND = 1000


async def test_an_async_send_waiting_for_its_topic_metadata_leaves_the_loop_running() -> None:
    # send() waits up to max.block.ms for its record's topic metadata and for
    # buffer space, as Java's doSend does (KafkaProducer.java:1065-1111): the
    # async send awaits that wait, so the loop runs other tasks meanwhile.
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 300})
    ticks = 0

    async def tick() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    ticker = asyncio.ensure_future(tick())
    try:
        start = time.monotonic()
        f = await p.send(record=RECORD)
        elapsed = time.monotonic() - start
    finally:
        ticker.cancel()
    assert elapsed >= 0.3, elapsed
    assert ticks >= 10, (ticks, elapsed)
    error = await async_assert_send_throws(f, KafkaTimeoutError, 30)
    assert str(error) == f"Topic {TOPIC} not present in metadata after 300 ms."
    await asyncio.wait_for(p.close(timeout=30.0), timeout=60)


# Updated for the branch C API (was: a send crossing the extension's bound of
# queued records waits for space until close releases it): the branch queues
# each record on the core's submission task, whose producer.send() waits
# max.block.ms for metadata and buffer space as Java's does, and send() returns
# only once its record is handed over, so each send to an unreachable broker
# takes max.block.ms, as in Java, and its future fails with the core's error.
QUEUED_SENDS = 3
BLOCK_MS = 200


def test_sync_send_waits_for_the_metadata_as_java_does() -> None:
    p = KafkaProducer(configs={**UNREACHABLE, "max.block.ms": BLOCK_MS})
    try:
        start = time.monotonic()
        futures = [p.send(record=RECORD) for _ in range(QUEUED_SENDS)]
        elapsed = time.monotonic() - start
        assert elapsed >= QUEUED_SENDS * (BLOCK_MS - 50) / 1000.0, elapsed
        for f in futures:
            error = assert_send_throws(f, KafkaTimeoutError, 30)
            assert str(error) == f"Topic {TOPIC} not present in metadata after {BLOCK_MS} ms."
    finally:
        p.close(timeout=30.0)


async def test_async_send_waits_for_the_metadata_without_blocking_the_loop() -> None:
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": BLOCK_MS})
    ticks = 0

    async def tick() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    ticker = asyncio.ensure_future(tick())
    try:
        start = time.monotonic()
        futures = [await p.send(record=RECORD) for _ in range(QUEUED_SENDS)]
        elapsed = time.monotonic() - start
        assert elapsed >= QUEUED_SENDS * (BLOCK_MS - 50) / 1000.0, elapsed
        assert ticks >= 10, "the loop must stay responsive while send() waits"
        for f in futures:
            error = await async_assert_send_throws(f, KafkaTimeoutError, 30)
            assert str(error) == f"Topic {TOPIC} not present in metadata after {BLOCK_MS} ms."
    finally:
        ticker.cancel()
        await asyncio.wait_for(p.close(timeout=30.0), timeout=60)


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
from confluent_kafka import ExecutionError
from confluent_kafka.producer import KafkaProducer, ProducerRecord
from confluent_kafka.producer._send import completion_to_python
UNREACHABLE = {"bootstrap.servers": "127.0.0.1:59999", "max.block.ms": 100}


def failure(future):
    # The error the record's KafkaFuture.get() raises an ExecutionError from.
    try:
        future.get(timeout=FUTURE_TIMEOUT)
    except ExecutionError as e:
        return e.__cause__
    return None


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
    # Updated for the branch C API: the send is Producer_send_cb, whose
    # first allocation is the context it hands the C API.
    _run_without_memory("""
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
p = KafkaProducer(configs=UNREACHABLE)
record = p._native_parts(ProducerRecord(topic="t", value=b"v"))[0]
resolve = lambda error: None
refs = sys.getrefcount(record), sys.getrefcount(on_complete), sys.getrefcount(resolve)
outcome = without_memory(_lib.Producer_send_cb, p._c_producer, record, on_complete, resolve)
assert isinstance(outcome, MemoryError), outcome
# Not queued: every reference taken for the queue was given back...
assert (sys.getrefcount(record), sys.getrefcount(on_complete), sys.getrefcount(resolve)) == refs
# ...and no half-built batch was left behind: the next record still goes
# through the batch list to the producer, whose metadata wait it fails.
error = failure(p.send(record=ProducerRecord(topic="t", value=b"v")))
assert isinstance(error, KafkaTimeoutError), error
p.close(timeout=0)
print("ok")
""")


@pytest.mark.skip(reason="Subject not on this branch: the extension has no list of senders "
                         "waiting for space (no Producer_on_space_available); send() waits for "
                         "its record's handover instead, the core's producer.send() "
                         "holding it up to max.block.ms, as Java's does.")
def test_waiting_for_space_without_memory_does_not_wait() -> None:
    # The list of senders waiting for space grows on demand. A failed realloc
    # was assigned over the list and then written through; now the sender is
    # told not to wait, because its record is already queued.
    _run_without_memory("""
p = KafkaProducer(configs=UNREACHABLE)
_lib.Producer_test_set_paused(p._c_producer, True)
futures = [p.send(record=ProducerRecord(topic="t", value=b"v")) for _ in range(BACKPRESSURE_BOUND - 1)]
record = p._native_parts(ProducerRecord(topic="t", value=b"v"))[0]
assert _lib.Producer_send(p._c_producer, record, on_complete) is True
space_cb = lambda: None
refs = sys.getrefcount(space_cb)
outcome = without_memory(_lib.Producer_on_space_available, p._c_producer, space_cb)
assert outcome is True, outcome
assert sys.getrefcount(space_cb) == refs  # not registered
_lib.Producer_test_set_paused(p._c_producer, False)
# The queued records are not lost: the close completes every one of them.
p.close(timeout=0)
assert all(future.is_done() for future in futures)
print("ok")
""")


# ===========================================================================
# The async real producer
# ===========================================================================

async def test_async_send_callback_runs_on_the_loop_before_the_future() -> None:
    # send() waits for the record's handover, and the core's send() fires the
    # callback of a metadata TimeoutError before it returns (Java's doSend
    # does, KafkaProducer.java:1137-1149), so the callback may run before
    # send() returned the future: that future is not done either.
    loop_thread = threading.get_ident()
    seen: list[tuple[int, bool, RecordMetadata]] = []
    holder: dict[str, Any] = {}

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        future = holder.get("f")
        seen.append((threading.get_ident(), future is not None and future.is_done(), metadata))

    p = AsyncKafkaProducer(configs=UNREACHABLE)
    holder["f"] = f = await p.send(record=RECORD, callback=callback)
    await async_assert_send_throws(f, KafkaTimeoutError, 30)
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

    async def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        started.set()
        await release.wait()
        seen.append((holder["f"].is_done(), exception))

    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        holder["f"] = f = await p.send(record=RECORD, callback=callback)
        await asyncio.wait_for(started.wait(), 30)
        await asyncio.sleep(0.2)
        assert not f.is_done()
        release.set()
        await async_assert_send_throws(f, KafkaTimeoutError, 30)
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

    async def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
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
        assert finished == [True] and f.is_done()
    finally:
        await p.close()


async def test_async_def_callbacks_run_one_at_a_time_in_completion_order() -> None:
    # A producer's callbacks run one at a time in completion order (CLAUDE.md,
    # Python Binding Conventions, Threads and callbacks): a later completion
    # waits behind an awaited callback.
    release = asyncio.Event()
    order: list[str] = []

    async def first(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        order.append("first started")
        await release.wait()
        order.append("first done")

    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        f1 = await p.send(record=RECORD, callback=first)
        f2 = await p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                                      order.append('second'))
        await _until(lambda: order == ["first started"])
        await asyncio.sleep(0.3)
        assert order == ["first started"] and not f2.is_done()
        release.set()
        for f in (f1, f2):
            await async_assert_send_throws(f, KafkaTimeoutError, 30)
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

    def plain(*, metadata: RecordMetadata, exception: Exception | None,
              **kwargs: Any) -> None:
        raise RuntimeError("callback blew up")

    async def async_def(*, metadata: RecordMetadata, exception: Exception | None,
                        **kwargs: Any) -> None:
        await asyncio.sleep(0)
        if kind == "cancelled":
            raise asyncio.CancelledError
        raise RuntimeError("async callback blew up")

    capfd.readouterr()
    caplog.set_level(logging.DEBUG, logger="confluent_kafka")
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        f = await p.send(record=RECORD, callback=plain if kind == "plain" else async_def)
        g = await p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                                     seen.append('next'))
        for future in (f, g):
            await async_assert_send_throws(future, KafkaTimeoutError, 30)
        await p.flush()
        assert seen == ["next"]
    finally:
        await p.close()
    assert _core_callback_errors(capfd) == [f"{CALLBACK_ERROR}: LocalCallbackError: {text}"]
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []


async def test_flush_in_an_async_def_callback_raises_javas_error() -> None:
    out: dict[str, Any] = {}
    p = AsyncKafkaProducer(configs=UNREACHABLE)

    async def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        try:
            await p.flush()
        except KafkaError as error:
            out["flush"] = error

    try:
        f = await p.send(record=RECORD, callback=callback)
        await async_assert_send_throws(f, KafkaTimeoutError, 30)
        assert str(out["flush"]) == FLUSH_IN_CALLBACK
    finally:
        await p.close()


COROUTINE_CALLBACK = "a coroutine send() callback requires an AsyncProducer"


@pytest.mark.parametrize("cls", [KafkaProducer, MockProducer])
def test_a_coroutine_callback_on_a_sync_producer_raises_a_type_error_at_send(
        cls: type[Any]) -> None:
    # Only an asyncio producer awaits an `async def` callback (CLAUDE.md,
    # Python Binding Conventions, Threads and callbacks): the sync producers
    # reject a coroutine function with TypeError at send(), before the record
    # is sent, and stay usable.
    ran: list[bool] = []

    async def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        ran.append(True)

    class AsyncCallable:
        async def __call__(self, *, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
            ran.append(True)

    p = (KafkaProducer(configs=UNREACHABLE) if cls is KafkaProducer
         else MockProducer(auto_complete=True, partitioner=None))
    try:
        for given in (callback, AsyncCallable()):
            with pytest.raises(TypeError) as err:
                p.send(record=RECORD, callback=given)  # type: ignore[arg-type]
            assert str(err.value) == COROUTINE_CALLBACK
        if cls is MockProducer:
            assert p.history() == []
        called: list[Exception | None] = []
        f = p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                               called.append(exception))
        failure(f, 30)
        assert len(called) == 1
    finally:
        p.close(timeout=0)
    assert ran == []


def test_a_callback_returning_a_coroutine_on_the_sync_producer_is_closed_and_reported(
        caplog: pytest.LogCaptureFixture, capfd: pytest.CaptureFixture[str]) -> None:
    # A plain callback that returns an awaitable passes the check at send():
    # the completion thread closes the coroutine and reports a TypeError, as
    # for a raising callback, which the core logs.
    ran: list[bool] = []

    async def coroutine(*, metadata: RecordMetadata, exception: Exception | None,
                        **kwargs: Any) -> None:
        ran.append(True)

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> Any:
        return coroutine(metadata=metadata, exception=exception)

    capfd.readouterr()
    with caplog.at_level(logging.DEBUG, logger="confluent_kafka"):
        with KafkaProducer(configs=UNREACHABLE) as p:
            f = p.send(record=RECORD, callback=callback)
            assert_send_throws(f, KafkaTimeoutError, 30)
    assert _core_callback_errors(capfd) == [
        f"{CALLBACK_ERROR}: LocalCallbackError: "
        "a coroutine send() callback requires an AsyncProducer"]
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
    assert ran == []


async def test_async_mock_awaits_an_async_def_callback_and_flush_waits_for_it() -> None:
    release = asyncio.Event()
    finished: list[bool] = []

    async def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        await release.wait()
        finished.append(True)

    p = AsyncMockProducer()
    f = await p.send(record=RECORD, callback=callback)
    flush = asyncio.ensure_future(p.flush())
    await asyncio.sleep(0.2)
    assert not flush.done() and not f.is_done()
    release.set()
    await asyncio.wait_for(flush, 30)
    assert finished == [True] and (await f.get()).offset() == 0
    await p.close()


def _send_handed_over(p: Any, loop: asyncio.AbstractEventLoop, captured: dict[str, Any],
                      ran: list[Exception | None]) -> AsyncKafkaFuture[RecordMetadata]:
    """Send one record on ``loop`` and hand it over (the faked send, see
    :func:`_fake_queued_send`), its completion left for later: with a real
    unreachable broker the record completes (its metadata wait failing)
    before its handover, so before send() returns."""
    async def send() -> AsyncKafkaFuture[RecordMetadata]:
        task = asyncio.ensure_future(
            p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                               ran.append(exception)))
        while "resolve" not in captured:
            await asyncio.sleep(0.01)
        captured["resolve"](0)
        return cast("AsyncKafkaFuture[RecordMetadata]", await task)

    return loop.run_until_complete(send())


def _send_on_a_loop_that_closes(p: Any, captured: dict[str, Any],
                                 ran: list[Exception | None]) -> AsyncKafkaFuture[RecordMetadata]:
    """:func:`_send_handed_over` on a fresh loop, closed after it: the record's
    completion then arrives after its loop closed."""
    loop = asyncio.new_event_loop()
    try:
        return _send_handed_over(p, loop, captured, ran)
    finally:
        loop.close()


def _complete_off_loop(captured: dict[str, Any]) -> None:
    """The record's completion, a metadata TimeoutError (a BORROWED error
    handle; callback id 0, as the faked send registered no report with the
    core), on a thread of its own, as the helper pump of a closed loop."""
    def complete() -> None:
        error = _lib.KafkaError_new(KafkaTimeoutError._ffi_id,
                                    f"Topic {TOPIC} not present in metadata after 1000 ms.")
        try:
            captured["on_completion"](None, error, 0)
        finally:
            _lib.KafkaError_destroy(error)

    helper = threading.Thread(target=complete)
    helper.start()
    helper.join(10)


def test_an_async_completion_after_its_loop_closed_is_dropped(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    # The callback runs on the event loop (CLAUDE.md, Python Binding
    # Conventions, Threads and callbacks), and a future only its own loop can
    # complete: a record that completes after its loop closed is dropped with a
    # warning, its callback not run, its future no longer tracked. The send
    # fails on its metadata wait after max.block.ms, long after the loop
    # closed, and the helper thread that pumps once the loop has closed drops
    # it. Updated for the one-loop binding (was: a flush() and close() from a
    # fresh loop then return): a call from another loop is refused. Updated
    # for send() waiting for its handover (the core fires the metadata
    # wait's TimeoutError before it, on the loop): a faked send hands the
    # record over at once and completes it once the loop closed.
    caplog.set_level(logging.WARNING, logger="confluent_kafka.producer")
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 1000})
    captured = _fake_queued_send(monkeypatch)
    ran: list[Exception | None] = []
    future = _send_on_a_loop_that_closes(p, captured, ran)
    assert future._impl in p._futures  # noqa: SLF001
    _complete_off_loop(captured)
    monkeypatch.undo()
    assert future._impl not in p._futures  # noqa: SLF001
    assert ran == []
    assert not future.is_done()
    assert _dropped(caplog) == [DROPPED]
    with pytest.raises(RuntimeError) as err:
        asyncio.run(p.flush())
    assert str(err.value) == BOUND_ELSEWHERE.format(cls="AsyncKafkaProducer")
    p._destroy()  # noqa: SLF001


DROPPED = (f"The completion of a record sent to topic-partition '{TOPIC}--1' was dropped: its "
           "event loop had closed, so its callback did not run and its future does not complete")


def _dropped(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records
            if r.name == "confluent_kafka.producer" and r.getMessage().startswith("The completion")]


def _refused_or_closed(p: Any, call: str, real_schedule: Callable[[], None],
                       freed_errors: list[int]) -> None:
    """After the producer's bound loop closed: a ``flush()`` from another loop
    is refused with no effect, and the producer is freed by hand; a
    ``close()`` from another loop is allowed (``LoopBinding.bind_for_close``)
    and frees it, the held pump schedules released first so its own
    completion is pumped. Updated for close() rebinding (was: both refused)."""
    if call == "close":
        p._schedule_pump = real_schedule  # noqa: SLF001
        asyncio.run(p.close(timeout=0))
        assert p._closed  # noqa: SLF001
        return
    with pytest.raises(RuntimeError) as err:
        asyncio.run(getattr(p, call)())
    assert str(err.value) == BOUND_ELSEWHERE.format(cls="AsyncKafkaProducer")
    assert freed_errors == []
    p._destroy()  # noqa: SLF001


def _hold_pump_schedules(p: Any) -> tuple[threading.Event, Callable[[], None]]:
    """Hold the producer's pump schedules: the event is set once the notify
    hook asks for one, and calling the function schedules that one as the hook
    would have. Holding it keeps the pump off the loop while the loop still
    runs, which on a fast machine would otherwise run the pump, and then the
    delivery, before the loop stops. The hook goes through the callable this
    replaces: the socket-pair wake it takes otherwise (``_install_wake``) has
    nothing to hold, so it is off for this producer (its loop is not bound
    yet)."""
    scheduled = threading.Event()
    p._WAKE_BY_FD = False  # noqa: SLF001
    real = p._schedule_pump  # noqa: SLF001

    def schedule() -> None:
        scheduled.set()

    p._schedule_pump = schedule  # noqa: SLF001
    return scheduled, real


def test_a_call_from_another_loop_is_refused_while_a_closed_loops_completion_is_in_flight(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    # Updated for the one-loop binding (was: a flush() on loop B that starts
    # before loop A's completion arrives leaves that record out of its wait):
    # loop A closes with a send in flight, and a flush() from loop B is
    # refused, the record still in flight; the helper thread drops its
    # completion, with one warning, when it arrives (faked as in the test
    # above).
    caplog.set_level(logging.WARNING, logger="confluent_kafka.producer")
    p = AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 1000})
    captured = _fake_queued_send(monkeypatch)
    ran: list[Exception | None] = []
    future = _send_on_a_loop_that_closes(p, captured, ran)

    async def flush() -> None:
        assert future._impl in p._futures  # noqa: SLF001 - the completion has not arrived
        await p.flush()

    with pytest.raises(RuntimeError) as err:
        asyncio.run(flush())
    assert str(err.value) == BOUND_ELSEWHERE.format(cls="AsyncKafkaProducer")
    _complete_off_loop(captured)
    monkeypatch.undo()
    assert future._impl not in p._futures  # noqa: SLF001
    assert ran == []
    assert not future.is_done()
    assert _dropped(caplog) == [DROPPED]
    p._destroy()  # noqa: SLF001


def test_a_completion_for_a_stopped_loop_runs_when_the_loop_runs_again(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Updated for the one-loop binding (was: the completion, pumped on a
    # helper thread while its loop is stopped, is dropped by a flush() from
    # another loop once the loop closes): a record that completes while its
    # loop is open but not running waits, its completion scheduled on the
    # loop, and runs there, callback first, once the loop runs again. Updated
    # for send() waiting for its handover: the faked send completes the record
    # after it, from another thread, as a pump off the loop would.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    captured = _fake_queued_send(monkeypatch)
    ran: list[Exception | None] = []
    loop = asyncio.new_event_loop()
    try:
        future = _send_handed_over(p, loop, captured, ran)
        _complete_off_loop(captured)
        monkeypatch.undo()
        assert loop in p._pending  # noqa: SLF001 - waiting for the loop
        assert ran == [] and not future.is_done()

        loop.run_until_complete(async_assert_send_throws(future, KafkaTimeoutError, 30))
        assert len(ran) == 1 and isinstance(ran[0], KafkaTimeoutError)
        loop.run_until_complete(p.close())
    finally:
        loop.close()


def test_a_completion_left_for_a_closed_loop_is_dropped_when_the_producer_is_destroyed(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    # The loop closes without running the pump scheduled on it, so the
    # completion stays queued; close() from another loop, which the binding
    # allows once the bound loop is closed (LoopBinding.bind_for_close),
    # frees the producer, and freeing it runs the completion, which is
    # dropped with one warning. Updated for close() rebinding (was: refused,
    # the test freeing the producer by hand). Updated for send() waiting for its handover: the
    # record is handed over by the faked send (see _send_handed_over), and
    # Producer_destroy runs its queued completion first, as the core's
    # destroy runs the callbacks still pending.
    caplog.set_level(logging.WARNING, logger="confluent_kafka.producer")
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    captured = _fake_queued_send(monkeypatch)
    real_destroy = _lib.Producer_destroy

    def destroy(c_producer: Any) -> None:
        _complete_off_loop(captured)
        real_destroy(c_producer)

    monkeypatch.setattr(_lib, "Producer_destroy", destroy)
    ran: list[Exception | None] = []
    future = _send_on_a_loop_that_closes(p, captured, ran)
    assert future._impl in p._futures and _dropped(caplog) == []  # noqa: SLF001
    asyncio.run(p.close(timeout=0))
    assert future._impl not in p._futures  # noqa: SLF001
    assert ran == []
    assert not future.is_done()
    assert _dropped(caplog) == [DROPPED]


def test_a_send_abandoned_with_its_loop_before_its_handover_stays_tracked(
        monkeypatch: pytest.MonkeyPatch, stopped_loop: Any) -> None:
    # A send() task left on a loop that closes before its record is handed
    # over has its coroutine closed (what the garbage collector does to it):
    # it ends without an error of its own, the record still sent and tracked
    # until its completion, dropped for the closed loop, untracks it.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    captured = _fake_queued_send(monkeypatch)
    stopped_loop.start(p.send(record=RECORD))
    assert "resolve" in captured
    stopped_loop.abandon()
    assert len(p._futures) == 1  # noqa: SLF001
    p._destroy()  # noqa: SLF001


def test_a_completion_its_closing_loop_refuses_is_dropped(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
        capfd: pytest.CaptureFixture[str]) -> None:
    # The loop is open when the completion checks it, then closes before
    # call_soon_threadsafe, which raises RuntimeError on the pumping thread:
    # the completion is dropped there, with one warning, instead of the error
    # reaching the C trampoline (which printed it) and the completion waiting
    # for a later flush(). Updated for the one-loop binding (was: then closed
    # from another loop): the producer is freed instead. Updated for send()
    # waiting for its handover (was: the delivery callback held on the pump,
    # which now runs on the loop the send awaits on): the record is handed
    # over by the faked send and completes on a thread of its own, any error
    # it raised there printed by the thread's excepthook.
    caplog.set_level(logging.WARNING, logger="confluent_kafka.producer")
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    captured = _fake_queued_send(monkeypatch)
    ran: list[Exception | None] = []
    loop = asyncio.new_event_loop()
    try:
        future = _send_handed_over(p, loop, captured, ran)
    finally:
        loop.close()
    loop.is_closed = lambda: False  # type: ignore[method-assign] # the check saw it open
    capfd.readouterr()
    _complete_off_loop(captured)
    del loop.is_closed
    assert capfd.readouterr().err == ""
    assert loop not in p._pending  # noqa: SLF001
    assert future._impl not in p._futures  # noqa: SLF001
    assert ran == []
    assert not future.is_done()
    assert _dropped(caplog) == [DROPPED]
    p._destroy()  # noqa: SLF001


def test_an_operation_result_its_closing_loop_refuses_is_freed(
        hold_completion: Any, freed_errors: list[int], stopped_loop: Any,
        capfd: pytest.CaptureFixture[str]) -> None:
    # The same race for the result of an operation (begin_transaction()'s
    # error on a non-transactional producer), whose task waits on a loop that
    # stopped: the loop is open when the pump and the result check it, then
    # closes before call_soon_threadsafe. The result is freed on the pumping
    # thread instead of the RuntimeError reaching the C trampoline, which
    # printed it and left the result's handles allocated. Updated for the
    # one-loop binding: submitted once the loop has closed, so the notify hook
    # that finds it open hands the pump to the helper thread when
    # call_soon_threadsafe refuses it; the producer is then freed.
    # The notify callable is the hook of a loop that cannot watch a socket;
    # the socket-pair wake (AsyncProducer._install_wake) cannot see a closed
    # loop: see test_a_completion_for_a_closed_loop_waits_for_the_producers_destruction.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    p._WAKE_BY_FD = False  # noqa: SLF001
    held = hold_completion("Producer_begin_transaction_cb", submit_later=True)
    stopped_loop.start(p.begin_transaction())
    loop = stopped_loop.loop
    loop.close()
    loop.is_closed = lambda: False  # type: ignore[method-assign] # the checks saw it open
    capfd.readouterr()
    held.submit()
    assert held.submitted.is_set()
    held.release.set()
    assert held.delivered.wait(30)
    del loop.is_closed
    ((error,),) = held.payloads
    assert error
    assert held.raised == []
    assert capfd.readouterr().err == ""
    assert freed_errors == [error]
    p._destroy()  # noqa: SLF001


@pytest.mark.parametrize("call", ["flush", "close"])
def test_an_operation_result_left_for_a_closed_loop_is_freed_when_the_producer_is_destroyed(
        call: str, freed_errors: list[int], stopped_loop: Any) -> None:
    # Updated for the one-loop binding (was: the next operation, from another
    # loop, frees it): the result arrives while its loop is stopped, which
    # then closes without running the pump scheduled on it; the next
    # operation, from another loop, is refused, and freeing the producer runs
    # the queued completion, which frees the result.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    scheduled, schedule = _hold_pump_schedules(p)
    stopped_loop.start(p.begin_transaction())
    assert scheduled.wait(30)
    schedule()  # on the stopped loop, whose close discards it
    stopped_loop.loop.close()
    assert freed_errors == []

    _refused_or_closed(p, call, schedule, freed_errors)
    assert len(freed_errors) == 1


@pytest.mark.parametrize("call", ["flush", "close"])
def test_an_operation_result_handed_to_a_loop_that_closed_is_freed_when_the_producer_is_destroyed(
        call: str, freed_errors: list[int], stopped_loop: Any) -> None:
    # As above, but the pump ran before the loop closed, as it does when the
    # result arrives during the loop's last iteration: the result was handed
    # to the stopped loop, whose close discards its delivery, so it waits in
    # the producer, not in the C API, and the destroy frees it there.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    scheduled, schedule = _hold_pump_schedules(p)
    stopped_loop.start(p.begin_transaction())
    assert scheduled.wait(30)
    p._pump()  # noqa: SLF001
    stopped_loop.loop.close()
    assert freed_errors == []

    _refused_or_closed(p, call, schedule, freed_errors)
    assert len(freed_errors) == 1


@pytest.mark.parametrize("call", ["flush", "close"])
def test_an_operation_result_delivered_to_a_task_left_on_a_closed_loop_is_freed_with_the_task(
        call: str, freed_errors: list[int], stopped_loop: Any) -> None:
    # As above, but the loop also ran the delivery before it closed, as it
    # does when the result arrives early in the loop's last iterations: the
    # result is in the awaiting task's future, and the task never resumes to
    # take it. The producer cannot reach it; the task frees it when it goes
    # away, which closes its coroutine.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    scheduled, schedule = _hold_pump_schedules(p)
    stopped_loop.start(p.begin_transaction())
    assert scheduled.wait(30)
    p._pump()  # noqa: SLF001
    # One iteration: the delivery runs, the task's wake-up it schedules does not.
    stopped_loop.loop.stop()
    stopped_loop.loop.run_forever()
    assert not stopped_loop._task.done()  # noqa: SLF001
    stopped_loop.loop.close()
    assert freed_errors == []

    _refused_or_closed(p, call, schedule, [])
    assert freed_errors == []
    stopped_loop.abandon()
    assert len(freed_errors) == 1


async def test_async_begin_transaction_is_a_coroutine_and_does_not_wait_for_a_send() -> None:
    # begin_transaction() is a coroutine (Class family), and Java's
    # beginTransaction does not wait for a send in progress, here one awaiting
    # its topic's metadata for max.block.ms.
    async with AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 1000}) as p:
        send = asyncio.ensure_future(p.send(record=RECORD))
        await asyncio.sleep(0.1)  # the send waits for its topic's metadata
        begin = p.begin_transaction()
        assert inspect.iscoroutine(begin)
        with pytest.raises(IllegalStateError) as err:
            await begin
        assert str(err.value) == NOT_TRANSACTIONAL
        assert not send.done()
        with pytest.raises(IllegalStateError):
            await p.commit_transaction()
        f = await send
    await async_assert_send_throws(f, KafkaTimeoutError)


# ===========================================================================
# MockProducer beyond MockProducerTest
# ===========================================================================

def test_mock_callback_runs_on_the_caller_before_the_future_completes() -> None:
    p = MockProducer(auto_complete=True, partitioner=None, key_serializer=None,
                     value_serializer=None)
    caller = threading.get_ident()
    seen: list[tuple[int, RecordMetadata, Exception | None]] = []
    f = p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                           seen.append((threading.get_ident(), metadata, exception)))
    ((thread, md, e),) = seen
    assert thread == caller and e is None
    assert (md.topic(), md.partition(), md.offset()) == (TOPIC, 0, 0)
    assert f.is_done() and not f.cancel(may_interrupt_if_running=True)
    assert f.get().offset() == 0 and f.get().timestamp() == -1


def test_mock_raising_callback_propagates_from_the_completing_call() -> None:
    # Java's MockProducer.Completion.complete calls onCompletion with no
    # try/catch, before result.done() (MockProducer.java:640-650): the
    # exception propagates out of the call that completed the record, the
    # record's future never completes, and the completions behind it stay
    # queued.
    raised = RuntimeError("callback blew up")

    def boom(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        raise raised

    p = MockProducer()
    f = p.send(record=RECORD, callback=boom)
    g = p.send(record=RECORD)
    with pytest.raises(RuntimeError) as info:
        p.complete_next()
    assert info.value is raised
    assert not f.is_done() and not g.is_done()
    assert p.complete_next()
    assert g.get().offset() == 1

    h = p.send(record=RECORD, callback=boom)
    with pytest.raises(RuntimeError) as info:
        p.error_next(e=KafkaError(message="failed"))
    assert info.value is raised
    assert not h.is_done()

    i = p.send(record=RECORD, callback=boom)
    j = p.send(record=RECORD)
    with pytest.raises(RuntimeError) as info:
        p.flush()
    assert info.value is raised
    assert not i.is_done() and not j.is_done()
    p.flush()
    assert j.get().offset() == 4

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


def test_mock_callback_returning_a_coroutine_raises_a_type_error() -> None:
    # The sync mock cannot await what a callback returns: a plain callback
    # returning a coroutine (a coroutine function itself is refused at send())
    # has it closed and a TypeError raised from the completing call.
    ran: list[bool] = []

    async def coroutine(*, metadata: RecordMetadata, exception: Exception | None,
                        **kwargs: Any) -> None:
        ran.append(True)

    def callback(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> Any:
        return coroutine(metadata=metadata, exception=exception)

    p = MockProducer()
    f = p.send(record=RECORD, callback=callback)
    with pytest.raises(TypeError) as info:
        p.complete_next()
    assert str(info.value) == COROUTINE_CALLBACK
    assert not f.is_done() and ran == []


async def test_async_mock_raising_callback_propagates_from_the_completing_call() -> None:
    raised = RuntimeError("callback blew up")

    def boom(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
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
    assert not f.is_done()
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

    async def boom(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
        await asyncio.sleep(0)
        raise raised

    p = AsyncMockProducer()
    f = await p.send(record=RECORD, callback=boom)
    assert p.complete_next()
    with pytest.raises(type(raised)) as info:
        await p.flush()
    assert info.value is raised
    assert not f.is_done()
    await p.flush()

    g = await p.send(record=RECORD, callback=boom)
    assert p.complete_next()
    await _until(lambda: p._mock_callback_errors)  # noqa: SLF001
    with pytest.raises(type(raised)) as info:
        await p.send(record=RECORD)
    assert info.value is raised
    assert not g.is_done()
    await p.close()


async def test_async_mock_close_raises_a_deferred_callback_error_once_closed() -> None:
    raised = RuntimeError("async callback blew up")

    async def boom(*, metadata: RecordMetadata, exception: Exception | None, **kwargs: Any) -> None:
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
    assert f.get().offset() == 0


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
    assert f.get().partition() == 2
    assert calls == [(TOPIC, "k", b"k", "v", b"v", cluster)]
    # Without a partitioner the first partition; a given partition must exist.
    p2 = MockProducer(cluster=cluster, auto_complete=True, partitioner=None)
    assert p2.send(record=RECORD).get().partition() == 0
    assert p2.send(record=ProducerRecord(topic=TOPIC, partition=1, key=None, value=b"v")).get(
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
    f = await p.send(record=RECORD, callback=lambda *, metadata, exception, **_:
                                                 seen.append(threading.get_ident()))
    assert f.is_done() and seen == [loop_thread]
    md = await (await p.send(record=RECORD)).get()
    assert md.offset() == 1
    await p.init_transactions()
    await p.begin_transaction()
    assert p.transaction_in_flight()
    await p.commit_transaction()
    assert p.commit_count() == 1
    manual = AsyncMockProducer()
    pending = await manual.send(record=RECORD)
    assert not pending.is_done()
    await manual.flush()
    assert (await pending.get()).offset() == 0
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


async def test_a_payload_delivered_just_before_a_cancellation_is_freed(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The completion reaches the loop and fills the awaited future, then the task
    # is cancelled before it resumes (an ``asyncio.wait_for`` timeout in the same
    # loop iteration): ``cancel()`` cannot cancel the done future, so the awaiting
    # coroutine frees the payload.
    from confluent_kafka.producer import async_producer

    freed: list[tuple[Any, ...]] = []
    monkeypatch.setattr(async_producer, "free_error_payload", freed.append)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        captured: dict[str, Any] = {}
        task = asyncio.ensure_future(
            p._await_payload(lambda handle, cb: captured.__setitem__("cb", cb)))
        await asyncio.sleep(0)  # the task now awaits its future
        captured["cb"](0)  # the completion: its delivery is scheduled on the loop
        await asyncio.sleep(0)  # the delivery fills the future; the task is not run yet
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert freed == [(0,)]
    finally:
        monkeypatch.undo()
        await p.close(timeout=0)


# ---------------------------------------------------------------------------
# The fast path yields to the loop: after 1000 sends in a row that the producer
# took at once, the next send() gives the loop one iteration before touching
# its record (rule 3, asyncio's cooperative scheduling:
# AsyncProducer._FAST_PATH_YIELD_EVERY). A queued send suspends on its own and
# restarts the count; a task cancelled at the yield has sent nothing.
# ---------------------------------------------------------------------------

def _taking_at_once(order: list[str]) -> Callable[..., int]:
    """Fake the C send: every record is taken, and completes, at once, so
    send() never suspends on its own (the fast path). Each call appends
    "send" to ``order``."""

    def producer_send_cb(c_producer: int, native: object, cb: Any, resolve: Any) -> int:
        order.append("send")
        cb(_METADATA, 0, 0)
        resolve(0)
        return 0

    return producer_send_cb


async def test_the_async_send_yields_to_the_loop_after_1000_sends_taken_at_once(
        monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    monkeypatch.setattr(_lib, "Producer_send_cb", _taking_at_once(order))
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        every = AsyncKafkaProducer._FAST_PATH_YIELD_EVERY  # noqa: SLF001
        assert every == 1000
        asyncio.get_running_loop().call_soon(order.append, "loop")
        for _ in range(every):
            await p.send(record=RECORD)
        # No send suspended: what was scheduled on the loop before has not run.
        assert order == ["send"] * every
        assert p._fast_path_sends == every  # noqa: SLF001
        await p.send(record=RECORD)
        # The 1001st gave the loop one iteration before touching its record.
        assert order == ["send"] * every + ["loop", "send"]
        assert p._fast_path_sends == 1  # noqa: SLF001
    finally:
        monkeypatch.undo()
        await p.close(timeout=0)


async def test_a_queued_async_send_restarts_the_count_of_sends_taken_at_once(
        monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    taken = _taking_at_once(order)
    loop = asyncio.get_running_loop()

    def queued(c_producer: int, native: object, cb: Any, resolve: Any) -> int:
        # The record waits for the submission task: its handover, then its
        # completion, come from the pump later, and send() suspends meanwhile.
        order.append("queued")
        loop.call_soon(resolve, 0)
        loop.call_soon(cb, _METADATA, 0, 0)
        return 0

    monkeypatch.setattr(_lib, "Producer_send_cb", taken)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        every = AsyncKafkaProducer._FAST_PATH_YIELD_EVERY  # noqa: SLF001
        for _ in range(every - 1):
            await p.send(record=RECORD)
        assert p._fast_path_sends == every - 1  # noqa: SLF001
        monkeypatch.setattr(_lib, "Producer_send_cb", queued)
        await p.send(record=RECORD)
        assert order[-1] == "queued"
        assert p._fast_path_sends == 0  # noqa: SLF001
        monkeypatch.setattr(_lib, "Producer_send_cb", taken)
        loop.call_soon(order.append, "loop")
        for _ in range(every):
            await p.send(record=RECORD)
        # The count restarted at the queued send: these 1000 did not yield.
        assert "loop" not in order
        assert p._fast_path_sends == every  # noqa: SLF001
    finally:
        monkeypatch.undo()
        await p.close(timeout=0)


async def test_a_task_cancelled_at_the_fast_path_yield_has_sent_nothing(
        monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    monkeypatch.setattr(_lib, "Producer_send_cb", _taking_at_once(order))
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        every = AsyncKafkaProducer._FAST_PATH_YIELD_EVERY  # noqa: SLF001
        for _ in range(every):
            await p.send(record=RECORD)
        task = asyncio.ensure_future(p.send(record=RECORD))
        await asyncio.sleep(0)  # the task is suspended at the yield, before its record
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert order == ["send"] * every  # the 1001st record was never sent
        assert p._fast_path_sends == 0  # noqa: SLF001
        await p.send(record=RECORD)  # and the next send works
        assert order == ["send"] * (every + 1)
    finally:
        monkeypatch.undo()
        await p.close(timeout=0)


# Between those yields, send() runs the callback pump on entry only on every
# 64th send in a row that the producer took at once (AsyncProducer._PUMP_EVERY)
# and on the first send after a suspension; the sends in between only count. A
# completion that arrives while the caller waits is the notify hook's, which
# wakes the loop (the tests below), so it never waits for a later send.
# ---------------------------------------------------------------------------

def _counting_pump(monkeypatch: pytest.MonkeyPatch, order: list[str]) -> None:
    """Record each run of the callback pump (the C drain) as "pump" in
    ``order``; the drain still runs."""
    real = _lib.Producer__execute_callbacks

    def execute_callbacks(c_producer: int) -> Any:
        order.append("pump")
        return real(c_producer)

    monkeypatch.setattr(_lib, "Producer__execute_callbacks", execute_callbacks)


async def test_the_async_send_runs_the_pump_on_every_64th_send_taken_at_once(
        monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    monkeypatch.setattr(_lib, "Producer_send_cb", _taking_at_once(order))
    _counting_pump(monkeypatch, order)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        every = AsyncKafkaProducer._PUMP_EVERY  # noqa: SLF001
        assert every == 64
        for _ in range(every):
            await p.send(record=RECORD)
        # The first send pumped on entry (the count at 0); the next 63 only
        # counted.
        assert order == ["pump", "send"] + ["send"] * (every - 1)
        await p.send(record=RECORD)
        # The 65th, the count at 64, pumped before touching its record.
        assert order == ["pump", "send"] + ["send"] * (every - 1) + ["pump", "send"]
        assert p._fast_path_sends == every + 1  # noqa: SLF001
    finally:
        monkeypatch.undo()
        await p.close(timeout=0)


async def test_the_first_async_send_after_a_queued_one_runs_the_pump(
        monkeypatch: pytest.MonkeyPatch) -> None:
    order: list[str] = []
    taken = _taking_at_once(order)
    loop = asyncio.get_running_loop()

    def queued(c_producer: int, native: object, cb: Any, resolve: Any) -> int:
        # The record waits for the submission task: send() suspends until the
        # handover, which comes from the loop here.
        order.append("queued")
        loop.call_soon(resolve, 0)
        loop.call_soon(cb, _METADATA, 0, 0)
        return 0

    monkeypatch.setattr(_lib, "Producer_send_cb", taken)
    _counting_pump(monkeypatch, order)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    try:
        for _ in range(10):
            await p.send(record=RECORD)
        assert order == ["pump", "send"] + ["send"] * 9
        monkeypatch.setattr(_lib, "Producer_send_cb", queued)
        await p.send(record=RECORD)
        # The count was 10, so this send did not pump; it suspended, and the
        # count restarted.
        assert order == ["pump", "send"] + ["send"] * 9 + ["queued"]
        assert p._fast_path_sends == 0  # noqa: SLF001
        monkeypatch.setattr(_lib, "Producer_send_cb", taken)
        await p.send(record=RECORD)
        # The first send after the suspension pumped on entry.
        assert order[-2:] == ["pump", "send"]
        assert p._fast_path_sends == 1  # noqa: SLF001
    finally:
        monkeypatch.undo()
        await p.close(timeout=0)


async def test_a_completion_wakes_the_loop_through_the_socket_pair_not_the_notify_callable(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # The native producer's notify hook, fired on the Rust task's thread when
    # its callback queue turns non-empty, writes one byte to the socket pair
    # the loop watches (AsyncProducer._install_wake) instead of taking the GIL
    # to call the notify callable: a completion that arrives while the loop
    # waits reaches its callback with the callable never called. close()
    # releases the pair: the loop stops watching it and both ends are closed.
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    scheduled: list[int] = []
    monkeypatch.setattr(p, "_schedule_pump", lambda: scheduled.append(1))
    with pytest.raises(IllegalStateError) as err:
        await p.begin_transaction()  # its queued completion is the error
    assert str(err.value) == NOT_TRANSACTIONAL
    reader, writer = p._wake_r, p._wake_w  # noqa: SLF001
    assert reader is not None and writer is not None
    assert scheduled == []
    await p.close()
    await asyncio.sleep(0)  # the loop stops watching the read end on its next iteration
    assert p._wake_r is None and p._wake_w is None  # noqa: SLF001
    assert reader.fileno() == -1 and writer.fileno() == -1


def test_a_completion_for_a_closed_loop_waits_for_the_producers_destruction(
        hold_completion: Any, freed_errors: list[int], stopped_loop: Any,
        capfd: pytest.CaptureFixture[str]) -> None:
    # The socket-pair wake cannot see that the loop has closed: a byte written
    # to a pair nobody reads wakes nothing, so a completion that arrives for a
    # closed loop stays queued in the C API until the producer is destroyed,
    # which runs the still pending callbacks (CLAUDE.md §4); the result is
    # freed then, on the destroying thread, with no error printed. (With the
    # notify callable the helper thread would have run it at once: see
    # test_an_operation_result_its_closing_loop_refuses_is_freed.)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    held = hold_completion("Producer_begin_transaction_cb", submit_later=True)
    stopped_loop.start(p.begin_transaction())
    assert p._wake_r is not None  # noqa: SLF001 - the loop watches the pair
    stopped_loop.loop.close()
    capfd.readouterr()
    held.submit()
    assert held.submitted.is_set()
    held.release.set()
    assert not held.delivered.wait(0.5)
    assert freed_errors == []
    p._destroy()  # noqa: SLF001
    assert held.delivered.wait(30)
    ((error,),) = held.payloads
    assert error
    assert held.raised == []
    assert capfd.readouterr().err == ""
    assert freed_errors == [error]
    assert p._wake_r is None and p._wake_w is None  # noqa: SLF001


def _counting_schedule_pump(p: Any) -> list[int]:
    """Count the calls of the producer's notify callable, which keeps working."""
    calls: list[int] = []
    real = p._schedule_pump  # noqa: SLF001

    def schedule() -> None:
        calls.append(1)
        real()

    p._schedule_pump = schedule  # noqa: SLF001
    return calls


async def test_a_loop_that_cannot_watch_a_socket_keeps_the_notify_callable(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # A loop without add_reader (Windows' Proactor raises NotImplementedError):
    # no socket pair is installed, and the hook keeps calling the notify
    # callable, which schedules the pump on the loop.
    loop = asyncio.get_running_loop()

    def no_reader(*args: Any) -> None:
        raise NotImplementedError

    monkeypatch.setattr(type(loop), "add_reader", no_reader)
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    calls = _counting_schedule_pump(p)
    with pytest.raises(IllegalStateError) as err:
        await p.begin_transaction()
    assert str(err.value) == NOT_TRANSACTIONAL
    assert p._wake_r is None and p._wake_w is None  # noqa: SLF001
    assert calls
    await p.close()


async def test_the_wake_switch_off_keeps_the_notify_callable() -> None:
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    p._WAKE_BY_FD = False  # noqa: SLF001
    calls = _counting_schedule_pump(p)
    with pytest.raises(IllegalStateError) as err:
        await p.begin_transaction()
    assert str(err.value) == NOT_TRANSACTIONAL
    assert p._wake_r is None and p._wake_w is None  # noqa: SLF001
    assert calls
    await p.close()


def test_the_sync_producer_sets_no_wake_fd(monkeypatch: pytest.MonkeyPatch) -> None:
    # The socket pair is the asyncio producer's: the sync producer's completion
    # thread waits on the condition the hook broadcasts, so it never hands the
    # native producer a wake fd.
    set_calls: list[tuple[Any, ...]] = []
    real = _lib.Producer__set_wake_fd

    def set_wake_fd(*args: Any) -> Any:
        set_calls.append(args)
        return real(*args)

    monkeypatch.setattr(_lib, "Producer__set_wake_fd", set_wake_fd)
    p = KafkaProducer(configs=UNREACHABLE)
    with pytest.raises(IllegalStateError) as err:
        p.begin_transaction()  # its completion reaches the completion thread
    assert str(err.value) == NOT_TRANSACTIONAL
    p.close()
    assert set_calls == []



def test_an_async_producer_whose_loop_ended_can_still_be_closed() -> None:
    # close() alone may run on another loop once the bound loop is closed
    # (LoopBinding.bind_for_close), as Java's close() always works; any
    # other call from there stays refused.
    p: AsyncKafkaProducer[bytes, bytes] = AsyncKafkaProducer(configs=UNREACHABLE)

    async def flush() -> None:
        await p.flush()

    asyncio.run(flush())
    with pytest.raises(RuntimeError) as err:
        asyncio.run(flush())
    assert str(err.value) == BOUND_ELSEWHERE.format(cls="AsyncKafkaProducer")
    asyncio.run(p.close(timeout=0))
    assert p._closed  # noqa: SLF001


def test_a_close_on_a_new_loop_does_not_drain_beside_the_off_loop_pump(
        monkeypatch: pytest.MonkeyPatch) -> None:
    # Critic 103 item 10: on the callable wake path (a loop without
    # add_reader, Windows' default loop) the completions for a closed loop are
    # pumped by the off-loop helper thread. A close() moved to a new loop
    # (LoopBinding.bind_for_close) pumps on entry: it must wait for the helper,
    # one drainer at a time, as the admin client's _drain_lock; two drainers
    # deadlock in the native call, which holds the GIL. The drain is faked to
    # count drainers inside it at once.
    p: AsyncKafkaProducer[bytes, bytes] = AsyncKafkaProducer(configs=UNREACHABLE)
    p._WAKE_BY_FD = False  # noqa: SLF001 - the callable wake path

    async def flush() -> None:
        await p.flush()

    asyncio.run(flush())  # binds the loop, which then closes
    real = _lib.Producer__execute_callbacks
    inside = [0]
    most = [0]
    helper_in = threading.Event()
    release = threading.Event()
    guard = threading.Lock()

    def execute(c_producer: Any) -> Any:
        with guard:
            inside[0] += 1
            most[0] = max(most[0], inside[0])
        try:
            if threading.current_thread().name == "confluent-kafka-producer-pump":
                helper_in.set()
                release.wait(10)
            return real(c_producer)
        finally:
            with guard:
                inside[0] -= 1

    monkeypatch.setattr(_lib, "Producer__execute_callbacks", execute)
    p._schedule_pump()  # noqa: SLF001 - the hook, its loop closed: the helper pumps
    assert helper_in.wait(10)
    closer = threading.Thread(target=lambda: asyncio.run(p.close(timeout=0)), daemon=True)
    closer.start()
    time.sleep(0.3)  # the close is now waiting for the helper's drain
    release.set()
    closer.join(10)
    assert not closer.is_alive(), "close() on the new loop hung"
    assert most[0] == 1, "two drainers at once"
    assert p._closed  # noqa: SLF001
