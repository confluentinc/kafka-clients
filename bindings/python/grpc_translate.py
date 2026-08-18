#!/usr/bin/env python3
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

"""Pure proto<->Python translation helpers shared by the sync and async gRPC
test servers (grpc_server.py / grpc_server_async.py).

These are client-agnostic: they convert between the generated protobuf messages
and the bindings/python producer.py / consumer.py value types, and do not depend
on whether the driving Kafka client is the sync or the asyncio-native one
(`ProducerRecord` and `KafkaError` are the same C-backed types in both). Kept in
one module so the two servers don't duplicate ~150 lines of conversion code.

Importing this module requires the generated proto stubs (`producer_service_pb2`,
`consumer_service_pb2`) to be on `sys.path` — the servers arrange that before
importing us.
"""

import producer as kp  # noqa: E402  (KafkaProducer / MockProducer / KafkaError)
import consumer as kc  # noqa: E402  (TopicPartition / OffsetAndMetadata / ...)
import producer_service_pb2 as pb  # noqa: E402  (generated)
import consumer_service_pb2 as cpb  # noqa: E402  (generated)

# Mapping from KafkaError variant integers (matching the proto enum) to a
# best-effort label. The Rust client decodes the variant explicitly, so
# the only thing that matters here is that we send a correct discriminator.
GENERIC = 0
TOPIC_AUTHORIZATION = 1
INVALID_TOPIC = 2
GROUP_AUTHORIZATION = 3
BUFFER_EXHAUSTED = 4
ILLEGAL_ARGUMENT = 5
ILLEGAL_STATE = 6
TIMEOUT = 7
RECORD_TOO_LARGE = 8
SERIALIZATION = 9


def _guess_variant(message):
    """Infer the proto KafkaError.Variant from a KafkaError message.

    The C FFI doesn't surface the Rust-side enum discriminator — only
    the integer code, the message string, and the retriable/fatal
    flags. Map the messages we know about to specific variants so the
    Rust client's `matches!(err, KafkaError::Foo(_))` assertions hold.
    """
    if not message:
        return GENERIC
    lowered = message.lower()
    if "max.request.size" in lowered or "is larger than" in lowered or "too large" in lowered:
        return RECORD_TOO_LARGE
    if "buffer is full" in lowered or "buffer.memory" in lowered:
        return BUFFER_EXHAUSTED
    if "timed out" in lowered or "expired" in lowered or "not present in metadata" in lowered:
        return TIMEOUT
    if "topic authorization" in lowered:
        return TOPIC_AUTHORIZATION
    if "invalid topic" in lowered:
        return INVALID_TOPIC
    if "group authorization" in lowered:
        return GROUP_AUTHORIZATION
    if "illegal state" in lowered or "already been closed" in lowered:
        return ILLEGAL_STATE
    if "serialization" in lowered or "failed to serialize" in lowered:
        return SERIALIZATION
    return GENERIC


def _kafka_error_to_proto(err):
    """Translate a producer.py KafkaError (or generic Exception) into a
    proto KafkaError. The C FFI doesn't expose the structured variant
    discriminator (it's all KafkaError on the C side), so we infer the
    variant heuristically from the message — it has to round-trip
    through the wire because the Rust client matches on variant."""
    if isinstance(err, kp.KafkaError):
        message = err.message or ""
        return pb.KafkaError(
            variant=_guess_variant(message),
            code=err.code,
            message=message,
            is_retriable=err.is_retriable,
            is_fatal=err.is_fatal,
        )
    # Unexpected non-Kafka exception: surface as IllegalState so the
    # Rust side sees a clear signal something went wrong server-side.
    return pb.KafkaError(
        variant=ILLEGAL_STATE,
        code=-1,
        message=f"python server: {type(err).__name__}: {err}",
        is_retriable=False,
        is_fatal=True,
    )


def _record_metadata_to_proto(meta):
    """Translate a producer.py RecordMetadata to its proto form."""
    return pb.RecordMetadata(
        offset=meta.offset(),
        timestamp=meta.timestamp(),
        # producer.py's RecordMetadata doesn't expose serialized sizes
        # today; the C FFI carries them but the Python wrapper drops
        # them. Surface -1 so RecordMetadata::new on the Rust side still
        # constructs validly.
        serialized_key_size=-1,
        serialized_value_size=-1,
        topic=meta.topic(),
        partition=meta.partition(),
    )


def _proto_to_producer_record(proto_record):
    """Translate a proto ProducerRecord to a producer.py ProducerRecord.

    The C extension's ProducerRecord init signature is
    (topic, value, key, partition=-1, timestamp=-1). Note that value
    is required (Python wrapper rejects None values today — Java's
    null-value tombstones aren't surfaced) and partition / timestamp
    use -1 as the sentinel for unset.

    Header forwarding is not yet implemented in producer.py, so we
    drop them. Existing integration tests don't use headers."""
    # value must be bytes — proto carries optional bytes; if absent
    # send a zero-length bytestring rather than None to satisfy the
    # Python wrapper's not-None validation.
    value = proto_record.value if proto_record.HasField("value") else b""
    key = proto_record.key if proto_record.HasField("key") else None
    partition = proto_record.partition if proto_record.HasField("partition") else -1
    timestamp = proto_record.timestamp if proto_record.HasField("timestamp") else -1
    return kp.ProducerRecord(
        proto_record.topic,
        value,
        key,
        partition,
        timestamp,
    )


def _node_to_proto(node):
    if node is None:
        return None
    return pb.Node(id=node.id, host=node.host, port=node.port,
                    rack=node.rack if node.rack is not None else None)


def _partition_info_to_proto(info):
    return pb.PartitionInfo(
        topic=info.topic,
        partition=info.partition,
        leader=_node_to_proto(info.leader),
        replicas=[_node_to_proto(n) for n in info.replicas],
        in_sync_replicas=[_node_to_proto(n) for n in info.in_sync_replicas],
        offline_replicas=[_node_to_proto(n) for n in info.offline_replicas],
    )


def _tp(proto_tp):
    return kc.TopicPartition(proto_tp.topic, proto_tp.partition)


def _tp_to_proto(tp):
    return cpb.TopicPartition(topic=tp.topic, partition=tp.partition)


def _oam_to_proto(oam):
    return cpb.OffsetAndMetadata(
        offset=oam.offset,
        metadata=oam.metadata or "",
        leader_epoch=oam.leader_epoch if oam.leader_epoch is not None else None,
    )


# Metric value kinds as reported by consumer.metrics()'s "kind" key; mirrors
# the Rust MetricValue variants (see KAFKA_CONSUMER_METRIC_VALUE_* in
# src/ffi/consumer.rs).
_METRIC_KIND_DOUBLE = 0
_METRIC_KIND_STRING = 1
_METRIC_KIND_LONG = 2
_METRIC_KIND_INT = 3


def _metric_to_proto(m):
    """One entry of consumer.metrics() -> cpb.Metric.

    `m` is a dict with keys name/group/description/tags/value/kind. `kind` picks
    the `value` oneof member; it is load-bearing for the integer cases because
    Python has a single `int` where Rust distinguishes Long from Int.
    """
    metric = cpb.Metric(
        name=m["name"],
        group=m["group"],
        description=m["description"],
    )
    metric.tags.update(m["tags"])
    kind = m["kind"]
    value = m["value"]
    if kind == _METRIC_KIND_STRING:
        metric.string_value = value
    elif kind == _METRIC_KIND_LONG:
        metric.long_value = int(value)
    elif kind == _METRIC_KIND_INT:
        metric.int_value = int(value)
    else:
        metric.double_value = float(value)
    return metric


def _record_to_proto(r):
    key = bytes(r.key) if r.key is not None else None
    value = bytes(r.value) if r.value is not None else None
    headers = [pb.Header(key=k, value=bytes(v) if v is not None else b"") for (k, v) in r.headers]
    return cpb.ConsumerRecord(
        topic=r.topic,
        partition=r.partition,
        offset=r.offset,
        timestamp=r.timestamp,
        timestamp_type=r.timestamp_type,
        key=key,
        value=value,
        headers=headers,
        leader_epoch=r.leader_epoch if r.leader_epoch is not None else None,
    )
