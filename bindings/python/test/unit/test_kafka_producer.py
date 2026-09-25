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

"""``KafkaProducerTest.java`` (Apache Kafka 4.3.1): the tests that run without a
broker, in Java order, asserting Java's messages.

Where Java drives a ``MockClient`` only to make an operation fail, the test uses
an unreachable bootstrap address and a short ``max.block.ms`` instead, and says
so. Not translated, with the reason:

- a ``MockClient`` / mocked ``ProducerMetadata`` / ``MockTime`` scripts the
  broker, which the binding cannot inject: the idempotence-config overrides
  (``testOverwriteAcksAndRetriesForIdempotentProducers`` … ``testInflight…``,
  covered by the core's ``ProducerConfig`` tests), every ``testMetadata*`` and
  ``testTopic*InMetadata``, ``testFlushCompleteSendOfInflightBatches``,
  ``testFlushMeasureLatency``, the transaction round trips
  (``testInitTransactionsResponseAfterTimeout``, ``testInitTransactionWhileThrottled``,
  ``testClusterAuthorizationFailure``, ``testAbortTransaction``,
  ``testMeasure*``, ``testCommitTransactionWith*``, ``testSendTxnOffsets*``,
  ``testPartitionAddedToTransaction``), ``testSendToInvalidTopic``,
  ``testCloseWhenWaitingForMetadataUpdate``, ``testCloseIsForcedOnPending*``,
  ``negativePartitionShouldThrow`` (also a custom partitioner);
- metrics reporters, JMX and client telemetry (``testMetricsReporter…``,
  ``testDisableJmx…``, ``testExplicitlyOnlyEnable…``,
  ``testConstructorWithInvalidMetricReporterClass``, ``testProducerJmxPrefix``,
  ``testMetricConfigRecordingLevel``, ``testClientInstanceId*``, the
  ``*CustomMetric*`` / ``*MetricReporter*`` subscription tests): the core loads
  no metric reporters, and ``clientInstanceId`` and the metric subscription
  methods are not generated (their FFI entry points are missing);
- interceptors and custom partitioners (``testInterceptor*``,
  ``testPartitionerClose``, ``configurableObjectsShouldSeeGeneratedClientId``):
  the client runs none, and ``interceptor.classes`` / ``partitioner.class``
  raise ``ConfigError``;
- ``testHeadersFailure``: Python headers are immutable, so there is no
  read-only state to check;
- ``testUnusedConfigs``: the binding logs only keys the ``ConfigDef`` does not
  define, as it cannot see which defined keys the core reads.
"""

from __future__ import annotations

import os
import signal
import threading
import time
import warnings
from types import FrameType
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError, IllegalStateError, NullPointerError
from confluent_kafka.common import KafkaError
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.serialization import bytes_serializer, string_serializer
from confluent_kafka.consumer import ConsumerGroupMetadata
from confluent_kafka.producer import KafkaProducer, ProducerRecord, RecordMetadata

INIT_TXN_TIMEOUT_MSG = ("InitTransactions timed out - did not complete coordinator discovery or "
                        "receive the InitProducerId response within max.block.ms.")


class MockSerializer:
    """Java's ``org.apache.kafka.test.MockSerializer``: a UTF-8 string
    serializer counting its instances and closes."""

    INIT_COUNT = 0
    CLOSE_COUNT = 0

    def __init__(self) -> None:
        MockSerializer.INIT_COUNT += 1

    def __call__(self, topic: str, value: str | None, headers: Any = None) -> bytes | None:
        return None if value is None else value.encode("utf-8")

    def close(self) -> None:
        MockSerializer.CLOSE_COUNT += 1


def test_constructor_with_serializers() -> None:
    producer_props = {"bootstrap.servers": "localhost:9000"}
    KafkaProducer(configs=producer_props, key_serializer=bytes_serializer(),
                  value_serializer=bytes_serializer()).close()


def test_no_serializer_provided() -> None:
    # Java requires both serializers and throws ConfigException without them;
    # *(deviation)* a serializer defaults to bytes_serializer().
    producer_props = {"bootstrap.servers": "localhost:9000"}
    with KafkaProducer(configs=producer_props) as producer:
        assert producer._key_serializer(  # noqa: SLF001 - the default in use
            "t", b"k") == b"k"


def test_constructor_failure_close_resource() -> None:
    # Java also counts MockMetricsReporter init/close; the core loads no
    # metrics reporter, so only the wrapped failure is checked.
    props = {"client.id": "testConstructorClose",
             "bootstrap.servers": "some.invalid.hostname.foo.bar.local:9999"}
    with pytest.raises(KafkaError) as err:
        KafkaProducer(configs=props, key_serializer=bytes_serializer(),
                      value_serializer=bytes_serializer())
    assert type(err.value) is KafkaError
    assert str(err.value) == "Failed to construct kafka producer"
    assert err.value.__cause__ is not None


def test_constructor_with_not_string_key() -> None:
    # Java's Properties path says "One or more keys is not a string."; a dict is
    # Java's Map path, Utils.castToStringObjectMap.
    props: dict[Any, Any] = {"bootstrap.servers": "localhost:9999", 1: "not string key"}
    with pytest.raises(ConfigError) as err:
        KafkaProducer(configs=props, key_serializer=string_serializer(),
                      value_serializer=string_serializer())
    assert str(err.value) == "Invalid value not string key for configuration 1: Key must be a string."


def test_serializer_close() -> None:
    configs = {"client.id": "testConstructorClose", "bootstrap.servers": "localhost:9999",
               "security.protocol": "PLAINTEXT"}
    old_init_count = MockSerializer.INIT_COUNT
    old_close_count = MockSerializer.CLOSE_COUNT

    with KafkaProducer(configs=configs, key_serializer=MockSerializer(),
                       value_serializer=MockSerializer()):
        assert MockSerializer.INIT_COUNT == old_init_count + 2
        assert MockSerializer.CLOSE_COUNT == old_close_count

    assert MockSerializer.INIT_COUNT == old_init_count + 2
    assert MockSerializer.CLOSE_COUNT == old_close_count + 2


def test_should_close_properly_and_throw_if_interrupted() -> None:
    # Java interrupts the thread blocked in close() and expects an
    # InterruptException after the close force-closed the sender
    # (KafkaProducer.close(Duration, boolean), :1419-1437). The Python analog of
    # the interrupt is Ctrl+C: SIGINT, which only the main thread receives, so
    # close() runs here and a timer sends the signal. Java's MockClient holds
    # the send in flight; here an unreachable broker keeps the record waiting
    # for metadata (max.block.ms 60 s), so close() cannot end on its own sooner.
    configs = {"bootstrap.servers": "localhost:9999", "batch.size": "1",
               "max.block.ms": 60000}
    producer = KafkaProducer(configs=configs, key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    future = producer.send(record=ProducerRecord(topic="topic", key="key", value="value"))

    armed = True

    def on_sigint(signum: int, frame: FrameType | None) -> None:
        if armed:
            raise KeyboardInterrupt

    previous = signal.signal(signal.SIGINT, on_sigint)
    timer = threading.Timer(0.1, os.kill, (os.getpid(), signal.SIGINT))
    start = time.monotonic()
    try:
        timer.start()
        with pytest.raises(KeyboardInterrupt):
            producer.close()
            pytest.fail("Close should block and throw.")
        elapsed = time.monotonic() - start
    finally:
        timer.cancel()
        timer.join()
        armed = False
        time.sleep(0.05)  # a signal still pending runs the disarmed handler
        signal.signal(signal.SIGINT, previous)

    # Close did not complete without waiting for the send, and the interrupt
    # surfaced once the close was forced, not after max.block.ms.
    assert 0.1 <= elapsed < 10, elapsed
    # Closed properly: the pending record failed, and the producer is closed.
    assert future.done()
    assert isinstance(future.exception(), KafkaError)
    with pytest.raises(IllegalStateError) as err:
        producer.send(record=ProducerRecord(topic="topic", key="key", value="value"))
    assert str(err.value) == "Cannot perform operation after producer has been closed"
    producer.close()  # closing again is harmless


def test_os_default_socket_buffer_sizes() -> None:
    # Selectable.USE_DEFAULT_BUFFER_SIZE is -1.
    config = {"bootstrap.servers": "localhost:9999", "send.buffer.bytes": -1,
              "receive.buffer.bytes": -1}
    KafkaProducer(configs=config, key_serializer=bytes_serializer(),
                  value_serializer=bytes_serializer()).close()


def test_invalid_socket_send_buffer_size() -> None:
    config = {"bootstrap.servers": "localhost:9999", "send.buffer.bytes": -2}
    with pytest.raises(KafkaError) as err:
        KafkaProducer(configs=config, key_serializer=bytes_serializer(),
                      value_serializer=bytes_serializer())
    assert isinstance(err.value, ConfigError)
    assert str(err.value) == ("Invalid value -2 for configuration send.buffer.bytes: "
                              "Value must be at least -1")


def test_invalid_socket_receive_buffer_size() -> None:
    config = {"bootstrap.servers": "localhost:9999", "receive.buffer.bytes": -2}
    with pytest.raises(KafkaError) as err:
        KafkaProducer(configs=config, key_serializer=bytes_serializer(),
                      value_serializer=bytes_serializer())
    assert isinstance(err.value, ConfigError)
    assert str(err.value) == ("Invalid value -2 for configuration receive.buffer.bytes: "
                              "Value must be at least -1")


def test_headers_success() -> None:
    # Java also checks, with an interceptor, that the headers turn read-only;
    # Python headers are immutable. The serializers get the record's headers.
    calls: list[tuple[str, object, object]] = []

    def recording(topic: str, value: str | None, headers: Any = None) -> bytes | None:
        calls.append((topic, value, headers))
        return None if value is None else value.encode()

    configs = {"bootstrap.servers": "localhost:9999", "max.block.ms": 5}
    producer = KafkaProducer(configs=configs, key_serializer=recording,
                             value_serializer=recording)
    topic = "topic"
    record = ProducerRecord(topic=topic, key="key", value="value",
                            headers=[("test", b"header2")])
    producer.send(record=record)
    assert calls == [(topic, "key", record.headers()), (topic, "value", record.headers())]
    assert bytes(record.headers()[-1][1]) == b"header2"  # type: ignore[arg-type]
    producer.close(timeout=0)


def test_close_should_be_idempotent() -> None:
    producer_props = {"bootstrap.servers": "localhost:9000"}
    producer = KafkaProducer(configs=producer_props, key_serializer=bytes_serializer(),
                             value_serializer=bytes_serializer())
    producer.close()
    producer.close()


def test_close_with_negative_timestamp_should_throw() -> None:
    producer_props = {"bootstrap.servers": "localhost:9000"}
    with KafkaProducer(configs=producer_props, key_serializer=bytes_serializer(),
                       value_serializer=bytes_serializer()) as producer:
        with pytest.raises(IllegalArgumentError) as err:
            producer.close(timeout=-0.1)
        assert str(err.value) == "The timeout cannot be negative."


def test_partitions_for_with_null_topic() -> None:
    props = {"bootstrap.servers": "localhost:9000"}
    with KafkaProducer(configs=props, key_serializer=bytes_serializer(),
                       value_serializer=bytes_serializer()) as producer:
        with pytest.raises(NullPointerError) as err:
            producer.partitions_for(topic=None)  # type: ignore[arg-type]
        assert str(err.value) == "topic cannot be null"


def test_init_transaction_timeout() -> None:
    # Java scripts the FindCoordinator response with a MockClient, then lets
    # the retry succeed; without a broker only the timeout half runs.
    configs = {"transactional.id": "bad-transaction", "max.block.ms": 500,
               "bootstrap.servers": "localhost:9000"}
    producer = KafkaProducer(configs=configs, key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    try:
        with pytest.raises(KafkaTimeoutError) as err:
            producer.init_transactions()
        assert INIT_TXN_TIMEOUT_MSG in str(err.value)
    finally:
        producer.close(timeout=0)


def test_null_group_metadata_in_send_offsets() -> None:
    _verify_invalid_group_metadata(None, "Consumer group metadata could not be null")


def test_invalid_generation_id_and_member_id_combined_in_send_offsets() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        group_metadata = ConsumerGroupMetadata(group_id="group", generation_id=2, member_id="",
                                               group_instance_id=None)
    _verify_invalid_group_metadata(
        group_metadata,
        "Passed in group metadata GroupMetadata(groupId = group, generationId = 2, memberId = , "
        "groupInstanceId = ) has generationId > 0 but the member.id is unknown")


def _verify_invalid_group_metadata(group_metadata: ConsumerGroupMetadata | None,
                                   message: str) -> None:
    # Java runs initTransactions / beginTransaction against a MockClient first;
    # KafkaProducer.sendOffsetsToTransaction checks the metadata before
    # anything else, so a producer without a transaction raises the same error.
    configs = {"bootstrap.servers": "localhost:9000", "max.block.ms": 10000}
    with KafkaProducer(configs=configs, key_serializer=string_serializer(),
                       value_serializer=string_serializer()) as producer:
        with pytest.raises(IllegalArgumentError) as err:
            producer.send_offsets_to_transaction(offsets={},
                                                 group_metadata=group_metadata)  # type: ignore[arg-type]
        assert str(err.value) == message


def test_only_can_execute_close_after_init_transactions_timeout() -> None:
    # Java's MockClient never answers; an unreachable broker does the same.
    configs = {"transactional.id": "bad-transaction", "max.block.ms": 5,
               "bootstrap.servers": "localhost:9000"}
    producer = KafkaProducer(configs=configs, key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    with pytest.raises(KafkaTimeoutError) as err:
        producer.init_transactions()
    assert INIT_TXN_TIMEOUT_MSG in str(err.value)
    # other transactional operations should not be allowed if we catch the
    # error after initTransactions failed
    try:
        with pytest.raises(IllegalStateError) as err2:
            producer.begin_transaction()
        assert str(err2.value) == ("Cannot attempt operation `beginTransaction` because the "
                                   "previous call to `initTransactions` timed out and must be "
                                   "retried")
    finally:
        producer.close(timeout=0)


def test_transactional_method_throws_when_sender_closed() -> None:
    configs = {"bootstrap.servers": "localhost:9000",
               "transactional.id": "this-is-a-transactional-id"}
    producer = KafkaProducer(configs=configs, key_serializer=string_serializer(),
                             value_serializer=string_serializer())
    producer.close()
    with pytest.raises(IllegalStateError) as err:
        producer.init_transactions()
    assert str(err.value) == "Cannot perform operation after producer has been closed"


def test_null_topic_name() -> None:
    # send a record with null topic should fail
    with pytest.raises(IllegalArgumentError) as err:
        ProducerRecord(topic=None, partition=1, key=b"key",  # type: ignore[arg-type]
                       value=b"value")
    assert str(err.value) == "Topic cannot be null."


def test_callback_and_interceptor_handle_error() -> None:
    # Java fails the send with an invalid topic name through a MockClient and
    # counts an interceptor's acknowledgements; here the send fails on the
    # metadata wait (unreachable broker, max.block.ms), and the client runs no
    # interceptors. The callback's metadata contract is the same.
    configs = {"bootstrap.servers": "localhost:9000", "max.block.ms": "100"}
    invalid_topic_name = "topic abc"  # Invalid topic name due to space
    seen: list[tuple[RecordMetadata, Exception | None]] = []
    done = threading.Event()

    def call_back(record_metadata: RecordMetadata, exception: Exception | None) -> None:
        seen.append((record_metadata, exception))
        done.set()

    with KafkaProducer(configs=configs, key_serializer=string_serializer(),
                       value_serializer=string_serializer()) as producer:
        record = ProducerRecord(topic=invalid_topic_name, value="HelloKafka")
        producer.send(record=record, callback=call_back)
        assert done.wait(30)
    ((record_metadata, exception),) = seen
    assert exception is not None
    assert record_metadata is not None
    assert record_metadata.topic() == invalid_topic_name, (
        "Topic name should be valid even on send failure")
    assert not record_metadata.has_offset()
    assert record_metadata.offset() == -1
    assert not record_metadata.has_timestamp()
    assert record_metadata.timestamp() == -1
    assert record_metadata.serialized_key_size() == -1
    assert record_metadata.serialized_value_size() == -1
    assert record_metadata.partition() == -1


def test_should_not_invoke_flush_in_callback() -> None:
    # Java completes the send through a MockClient; here it fails on the
    # metadata wait, which runs the callback all the same.
    configs = {"bootstrap.servers": "localhost:9000", "enable.idempotence": False,
               "max.block.ms": 100}
    kafka_exception: list[BaseException] = []
    done = threading.Event()

    with KafkaProducer(configs=configs, key_serializer=string_serializer(),
                       value_serializer=string_serializer()) as producer:
        def callback(record_metadata: RecordMetadata, exception: Exception | None) -> None:
            try:
                producer.flush()
            except KafkaError as e:
                kafka_exception.append(e)
            done.set()

        producer.send(record=ProducerRecord(topic="topic", value="value"), callback=callback)
        assert done.wait(30)

    (error,) = kafka_exception
    assert type(error) is KafkaError
    assert str(error) == ("KafkaProducer.flush() invocation inside a callback is not permitted "
                          "because it may lead to deadlock.")
