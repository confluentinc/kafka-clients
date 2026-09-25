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
import inspect
import itertools
import logging
import threading
import time
import warnings
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka.common import KafkaError, MetricName, PartitionInfo, TopicPartition
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.serialization import bytes_serializer, string_serializer
from confluent_kafka.consumer import ConsumerGroupMetadata
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
# Java waits in these (KafkaProducer's implementation), so they are async def.
WAITING = {"init_transactions", "send_offsets_to_transaction", "commit_transaction",
           "abort_transaction", "send", "flush", "partitions_for", "close"}

MOCK_FORMS = ("MockProducer() takes one of (cluster, auto_complete, partitioner, "
              "key_serializer, value_serializer), (auto_complete, partitioner, key_serializer, "
              "value_serializer), (); got ")


def _group_metadata() -> ConsumerGroupMetadata:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return ConsumerGroupMetadata(group_id="g")


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
              "key_serializer": None, "value_serializer": None}
    rest = ["auto_complete", "partitioner", "key_serializer", "value_serializer"]
    prefix = MOCK_FORMS.replace("MockProducer()", f"{cls.__name__}()")
    for with_cluster in (False, True):
        for n in range(len(rest) + 1):
            for subset in itertools.combinations(rest, n):
                given = (["cluster"] if with_cluster else []) + list(subset)
                kwargs = {k: values[k] for k in given}
                # (cluster, …) takes any subset: ``()`` passes (false, null,
                # null, null) to it; without a cluster only () and the
                # four-argument overload exist.
                if with_cluster or n in (0, len(rest)):
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
    native = _native(ProducerRecord(topic=TOPIC, key=b"k", value=b"v",
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
    record = ProducerRecord(topic=topic, key=key, value=value,
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

    record = ProducerRecord(topic=TOPIC, value=None, headers=[("h", b"1")])
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
        holder["f"] = f = p.send(record=ProducerRecord(topic=TOPIC, partition=3, value=b"v"),
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


def test_callbacks_run_in_completion_order_and_a_raising_one_is_logged(
        caplog: pytest.LogCaptureFixture) -> None:
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

    with caplog.at_level(logging.ERROR, logger="confluent_kafka.producer"):
        with KafkaProducer(configs=UNREACHABLE) as p:
            futures = [p.send(record=RECORD, callback=make(i)) for i in range(4)]
            assert all_done.wait(30)
            for f in futures:
                assert isinstance(f.exception(timeout=30), KafkaTimeoutError)
    assert order == [0, 1, 2, 3]
    assert [r.getMessage() for r in caplog.records if r.name == "confluent_kafka.producer"] == [
        "Error executing user-provided callback on message for topic-partition 'topic--1'"]


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
    p = KafkaProducer(configs={"bootstrap.servers": "127.0.0.1:59999"})  # max.block.ms 60 s
    _lib.Producer_test_set_paused(p._c_producer, True)  # noqa: SLF001
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


def _count_teardowns(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[int]]:
    """Count Producer_shutdown / Producer_destroy per handle; a second destroy
    of one handle is counted, not run (it would free the struct twice)."""
    calls: dict[str, list[int]] = {"shutdown": [], "destroy": []}
    real_shutdown, real_destroy = _lib.Producer_shutdown, _lib.Producer_destroy

    def shutdown(c_producer: int) -> None:
        calls["shutdown"].append(c_producer)
        real_shutdown(c_producer)

    def destroy(c_producer: int) -> None:
        calls["destroy"].append(c_producer)
        if calls["destroy"].count(c_producer) == 1:
            real_destroy(c_producer)

    monkeypatch.setattr(_lib, "Producer_shutdown", shutdown)
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
    assert calls == {"shutdown": [p._c_producer], "destroy": [p._c_producer]}  # noqa: SLF001


def test_concurrent_closes_tear_down_once_at_a_short_switch_interval(
        monkeypatch: pytest.MonkeyPatch) -> None:
    import sys

    calls = _count_teardowns(monkeypatch)
    previous = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for _ in range(30):
            calls["shutdown"].clear()
            calls["destroy"].clear()
            p = KafkaProducer(configs=UNREACHABLE)
            _close_from_two_threads(p)
            assert len(calls["shutdown"]) == 1 and len(calls["destroy"]) == 1
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
    assert calls == {"shutdown": [c_producer], "destroy": [c_producer]}


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


def test_context_manager_flushes_then_closes() -> None:
    with KafkaProducer(configs=UNREACHABLE) as p:
        f = p.send(record=RECORD)
    assert f.done()
    with pytest.raises(IllegalStateError):
        p.flush()


# ===========================================================================
# The drain before flush and the transaction-control operations
# ===========================================================================

def test_drain_waits_until_the_accumulation_is_handed_over() -> None:
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        _lib.Producer_test_set_paused(p._c_producer, True)  # noqa: SLF001
        p.send(record=RECORD)
        fired = threading.Event()
        assert _lib.Producer_drain(p._c_producer, fired.set) is False  # noqa: SLF001
        assert not fired.wait(0.2)
        _lib.Producer_test_set_paused(p._c_producer, False)  # noqa: SLF001
        assert fired.wait(30)
        assert _lib.Producer_drain(p._c_producer, lambda: None) is True  # noqa: SLF001
    finally:
        p.close(timeout=0)


@pytest.mark.parametrize("operation", ["commit_transaction", "abort_transaction",
                                       "init_transactions", "flush"])
def test_control_operations_run_after_the_sends_that_returned(operation: str) -> None:
    # producer-transactions.md §13: a send that returned belongs to the
    # control call after it, so the call waits until the record is with the
    # Rust producer.
    p = KafkaProducer(configs=UNREACHABLE)
    try:
        _lib.Producer_test_set_paused(p._c_producer, True)  # noqa: SLF001
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
        _lib.Producer_test_set_paused(p._c_producer, False)  # noqa: SLF001
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
def test_begin_transaction_does_not_wait_for_earlier_sends(transactional: bool) -> None:
    # Critic 75 N1: Java's beginTransaction does not wait. No send that
    # returned can still be on its way (a transactional producer's send with no
    # transaction started returns once its record is with the Rust producer),
    # so begin_transaction() does not drain: with the handover held, it returns
    # at once, while a transactional send is still waiting for its handover.
    configs = {**UNREACHABLE, "transactional.id": "txn"} if transactional else UNREACHABLE
    p = KafkaProducer(configs=configs)
    try:
        _lib.Producer_test_set_paused(p._c_producer, True)  # noqa: SLF001
        sent = threading.Event()

        def send() -> None:
            p.send(record=RECORD)
            sent.set()

        t = threading.Thread(target=send)
        t.start()
        assert sent.wait(0.3) is not transactional
        start = time.monotonic()
        with pytest.raises(IllegalStateError) as err:
            p.begin_transaction()
        assert time.monotonic() - start < 0.3
        assert str(err.value) == (
            "TransactionalId txn: Invalid transition attempted from state UNINITIALIZED to "
            "state IN_TRANSACTION" if transactional else NOT_TRANSACTIONAL)
        _lib.Producer_test_set_paused(p._c_producer, False)  # noqa: SLF001
        t.join(30)
        assert sent.is_set()
    finally:
        p.close(timeout=0)


def test_send_offsets_to_transaction_checks_group_metadata_first() -> None:
    # Java: throwIfInvalidGroupMetadata, throwIfNoTransactionManager,
    # throwIfProducerClosed, then nothing to send for empty offsets.
    with KafkaProducer(configs={**UNREACHABLE, "enable.idempotence": False}) as p:
        with pytest.raises(IllegalArgumentError) as err:
            p.send_offsets_to_transaction(offsets={}, group_metadata=None)  # type: ignore[arg-type]
        assert str(err.value) == "Consumer group metadata could not be null"
        with pytest.raises(IllegalStateError) as err2:
            p.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata())
        assert str(err2.value) == NO_TXN
        with pytest.raises(IllegalStateError) as err2:
            p.begin_transaction()
        assert str(err2.value) == NO_TXN
    with KafkaProducer(configs=UNREACHABLE) as idempotent:
        idempotent.send_offsets_to_transaction(offsets={}, group_metadata=_group_metadata())


# ===========================================================================
# Backpressure: send waits for buffer space (why the async send is awaited)
# ===========================================================================

# PRODUCER_MAX_ACCUMULATED_RECORDS in _confluentkafka.c.
BACKPRESSURE_BOUND = 1000


def test_sync_send_blocks_on_full_and_close_unblocks() -> None:
    p = KafkaProducer(configs=UNREACHABLE)
    _lib.Producer_test_set_paused(p._c_producer, True)  # noqa: SLF001
    for _ in range(BACKPRESSURE_BOUND - 1):
        p.send(record=RECORD)  # below the bound: none block
    done = threading.Event()

    def crossing_send() -> None:
        p.send(record=RECORD)  # crosses the bound -> waits for space
        done.set()

    t = threading.Thread(target=crossing_send)
    t.start()
    try:
        assert not done.wait(timeout=0.4), "send should block while the buffer is full"
        p.close(timeout=2.0)
        assert done.wait(timeout=30), "close must release the blocked sender"
    finally:
        t.join(timeout=30)


async def test_async_send_suspends_on_full_and_close_unblocks() -> None:
    p = AsyncKafkaProducer(configs=UNREACHABLE)
    _lib.Producer_test_set_paused(p._c_producer, True)  # noqa: SLF001
    for _ in range(BACKPRESSURE_BOUND - 1):
        await p.send(record=RECORD)
    task = asyncio.ensure_future(p.send(record=RECORD))
    await asyncio.sleep(0.3)
    assert not task.done(), "crossing send should suspend on backpressure"
    assert await asyncio.sleep(0, result=True)  # the loop stays responsive
    await asyncio.wait_for(p.close(timeout=2.0), timeout=60)
    await asyncio.wait_for(task, timeout=5)


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


async def test_async_begin_transaction_is_plain_and_does_not_block_the_loop() -> None:
    # Critic 75 N1: a record still waiting for its topic's metadata (up to
    # max.block.ms) does not hold begin_transaction() on the loop thread.
    async with AsyncKafkaProducer(configs={**UNREACHABLE, "max.block.ms": 3000}) as p:
        f = await p.send(record=RECORD)
        start = time.monotonic()
        with pytest.raises(IllegalStateError) as err:
            p.begin_transaction()
        assert time.monotonic() - start < 1
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


def test_mock_raising_callback_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    p = MockProducer()

    def boom(md: RecordMetadata, e: Exception | None) -> None:
        raise RuntimeError("callback blew up")

    f = p.send(record=RECORD, callback=boom)
    with caplog.at_level(logging.ERROR, logger="confluent_kafka.producer"):
        assert p.complete_next()
    assert f.result().offset() == 0
    assert [r.getMessage() for r in caplog.records] == [
        "Error executing user-provided callback on message for topic-partition 'topic-0'"]


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
    p = MockProducer(cluster=cluster)
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
    p2 = MockProducer(cluster=cluster, auto_complete=True)
    assert p2.send(record=RECORD).result().partition() == 0
    assert p2.send(record=ProducerRecord(topic=TOPIC, partition=1, value=b"v")).result(
    ).partition() == 1
    with pytest.raises(IllegalArgumentError) as err:
        p2.send(record=ProducerRecord(topic=TOPIC, partition=3, value=b"v"))
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
    p.begin_transaction()
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
