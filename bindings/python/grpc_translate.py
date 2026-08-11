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
and the bindings/python producer.py / consumer.py / admin.py value types, and do
not depend on whether the driving Kafka client is the sync or the asyncio-native
one (`ProducerRecord`, `KafkaError` and admin.py's value types are the same
C-backed types in both). Kept in one module so the two servers don't duplicate
their conversion code.

Importing this module requires the generated proto stubs (`producer_service_pb2`,
`consumer_service_pb2`, `admin_service_pb2`) to be on `sys.path` — the servers
arrange that before importing us.
"""

import producer as kp  # noqa: E402  (KafkaProducer / MockProducer / KafkaError)
import consumer as kc  # noqa: E402  (TopicPartition / OffsetAndMetadata / ...)
import admin as ka  # noqa: E402  (NewTopic / NewPartitions / RecordsToDelete / ...)
import producer_service_pb2 as pb  # noqa: E402  (generated)
import consumer_service_pb2 as cpb  # noqa: E402  (generated)
import admin_service_pb2 as apb  # noqa: E402  (generated)

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


# ---------------------------------------------------------------------------
# Admin service (admin_service.proto)
#
# Shared by both admin servicers. admin.py's methods hand back plain dicts of
# already-resolved values — `{key: value | KafkaError}` for the RPCs whose Java
# future has a value and `{key: None | KafkaError}` for the void ones — so these
# helpers only have to wrap those dicts in the per-key envelope.
# ---------------------------------------------------------------------------


def _admin_selects_mock(config):
    """The normative mock-selection rule of admin_service.proto's
    CreateAdminRequest: an empty config, or one whose every value is empty,
    selects the mock. All three AdminService servers apply exactly this."""
    return not config or all(not v for v in config.values())


def _admin_constructor_error(err):
    """Translate a *constructor* failure for CreateAdminResponse.error.

    A genuine KafkaError (a bad AdminClient config the FFI rejected) is
    forwarded verbatim and keeps its own variant. Anything else means the
    constructor rejected its arguments without a Kafka error of its own — the
    mock's `num_brokers < 1`, where the FFI returns NULL — and crosses as
    ILLEGAL_ARGUMENT, matching Java's IllegalArgumentException and the C++
    server. Without this the shared fallback in `_kafka_error_to_proto` would
    report ILLEGAL_STATE and the C and Python backends would disagree on a
    state neither one is wrong about."""
    if isinstance(err, kp.KafkaError):
        return _kafka_error_to_proto(err)
    return pb.KafkaError(
        variant=ILLEGAL_ARGUMENT,
        code=-1,
        message=f"python server: {type(err).__name__}: {err}",
        is_retriable=False,
        is_fatal=True,
    )


def _admin_timeout(request):
    """`optional int32 timeout_ms` -> the `timeout` seconds admin.py takes."""
    return request.timeout_ms / 1000.0 if request.HasField("timeout_ms") else None


def _admin_retry_on_quota(request):
    """`optional bool retry_on_quota_violation` -> Java's default of True when
    absent."""
    return (request.retry_on_quota_violation
            if request.HasField("retry_on_quota_violation") else True)


def _admin_name_key(name):
    return apb.ResultKey(name=name)


def _admin_topic_id_key(topic_id):
    return apb.ResultKey(topic_id=topic_id)


def _admin_partition_key(topic, partition):
    return apb.ResultKey(partition=cpb.TopicPartition(topic=topic, partition=partition))


def _admin_void_response(outcomes, key_fn):
    """`{key: None | KafkaError}` -> VoidKeyedResponse. An absent per-key error
    is the success signal, there being no value for a KafkaFuture<Void>."""
    entries = []
    for key, outcome in outcomes.items():
        entry = apb.VoidResultEntry(key=key_fn(key))
        if outcome is not None:
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        entries.append(entry)
    return apb.VoidKeyedResponse(entries=entries)


def _admin_new_topics(protos):
    """[proto NewTopic] -> [admin.py NewTopic].

    -1 is the wire's "absent" for num_partitions / replication_factor, which is
    admin.py's None. A non-empty replicas_assignments selects Java's
    NewTopic(name, Map<Integer, List<Integer>>) constructor."""
    topics = []
    for p in protos:
        assignments = {a.partition: list(a.broker_ids) for a in p.replicas_assignments}
        topics.append(ka.NewTopic(
            p.name,
            None if p.num_partitions < 0 else p.num_partitions,
            None if p.replication_factor < 0 else p.replication_factor,
            configs=dict(p.configs),
            replicas_assignments=assignments or None,
        ))
    return topics


def _admin_new_partitions(protos):
    """[proto NewPartitions] -> `{topic: admin.py NewPartitions}`.

    An absent new_assignments selects increaseTo(int); present selects
    increaseTo(int, List<List<Integer>>) — two different broker requests."""
    out = {}
    for p in protos:
        assignments = None
        if p.HasField("new_assignments"):
            assignments = [list(a.broker_ids) for a in p.new_assignments.assignments]
        out[p.topic] = ka.NewPartitions(p.total_count, assignments)
    return out


def _admin_records_to_delete(protos):
    """[proto RecordsToDelete] -> `{(topic, partition): RecordsToDelete}`, the
    key shape admin.py's delete_records takes and reports back."""
    return {(p.partition.topic, p.partition.partition): ka.RecordsToDelete(p.before_offset)
            for p in protos}


def _admin_config_entry_to_proto(entry):
    return apb.ConfigEntry(
        name=entry.name,
        value=entry.value if entry.value is not None else None,
        is_default=bool(entry.is_default),
        is_sensitive=bool(entry.is_sensitive),
        is_read_only=bool(entry.is_read_only),
    )


def _admin_metadata_to_proto(metadata):
    """admin.py TopicMetadataAndConfig -> proto TopicMetadataAndConfig.

    `metadata.error` is the value-level error of envelope exception 3: the topic
    was created, but the broker did not return its metadata, so every Java
    accessor on the object rethrows. It must cross as the error arm rather than
    as a metadata with -1 fields, or all four backends would silently agree on a
    state the harness had discarded."""
    if metadata.error is not None:
        return apb.TopicMetadataAndConfig(error=_kafka_error_to_proto(metadata.error))
    return apb.TopicMetadataAndConfig(metadata=apb.TopicMetadata(
        topic_id=metadata.topic_id,
        num_partitions=metadata.num_partitions,
        replication_factor=metadata.replication_factor,
        configs=[_admin_config_entry_to_proto(c) for c in metadata.configs],
    ))


def _admin_create_topics_response(outcomes):
    """`{name: TopicMetadataAndConfig | KafkaError}` -> CreateTopicsResponse."""
    entries = []
    for name, outcome in outcomes.items():
        entry = apb.CreateTopicsEntry(key=_admin_name_key(name))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(_admin_metadata_to_proto(outcome))
        entries.append(entry)
    return apb.CreateTopicsResponse(entries=entries)


def _admin_partition_info_to_proto(info):
    """admin.py TopicPartitionInfo -> proto TopicPartitionInfo. `elr` and
    `last_known_elr` are nullable in Java, hence the wrapper message."""
    out = apb.TopicPartitionInfo(
        partition=info.partition,
        replicas=[_node_to_proto(n) for n in info.replicas],
        isr=[_node_to_proto(n) for n in info.isr],
    )
    if info.leader is not None:
        out.leader.CopyFrom(_node_to_proto(info.leader))
    if info.elr is not None:
        out.elr.CopyFrom(apb.NodeList(nodes=[_node_to_proto(n) for n in info.elr]))
    if info.last_known_elr is not None:
        out.last_known_elr.CopyFrom(
            apb.NodeList(nodes=[_node_to_proto(n) for n in info.last_known_elr]))
    return out


def _admin_description_to_proto(description):
    """admin.py TopicDescription -> proto TopicDescription."""
    out = apb.TopicDescription(
        name=description.name,
        topic_id=description.topic_id,
        is_internal=bool(description.is_internal),
        partitions=[_admin_partition_info_to_proto(p) for p in description.partitions],
    )
    # Nullable: absent means the broker did not report the operations, which is
    # not the same as reporting that none are authorized.
    if description.authorized_operations is not None:
        out.authorized_operations.CopyFrom(
            apb.AclOperationList(operations=list(description.authorized_operations)))
    return out


def _admin_describe_topics_response(outcomes, key_fn):
    """`{key: TopicDescription | KafkaError}` -> DescribeTopicsResponse."""
    entries = []
    for key, outcome in outcomes.items():
        entry = apb.DescribeTopicsEntry(key=key_fn(key))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(_admin_description_to_proto(outcome))
        entries.append(entry)
    return apb.DescribeTopicsResponse(entries=entries)


def _admin_list_topics_response(listings):
    """`{name: TopicListing}` -> AdminListTopicsResponse. A whole-value
    response: Java's ListTopicsResult holds one future for the entire map, so no
    listing can fail on its own."""
    return apb.AdminListTopicsResponse(listings=[
        apb.AdminTopicListing(name=listing.name, topic_id=listing.topic_id,
                              is_internal=bool(listing.is_internal))
        for listing in listings.values()
    ])


def _admin_delete_records_response(outcomes):
    """`{(topic, partition): DeletedRecords | KafkaError}` ->
    DeleteRecordsResponse."""
    entries = []
    for (topic, partition), outcome in outcomes.items():
        entry = apb.DeleteRecordsEntry(key=_admin_partition_key(topic, partition))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(apb.DeletedRecords(low_watermark=outcome.low_watermark))
        entries.append(entry)
    return apb.DeleteRecordsResponse(entries=entries)
