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

"""Proto<->Python translation helpers, plus the callback-log machinery, shared
by the sync and async gRPC test servers (grpc_server.py / grpc_server_async.py).

These are client-agnostic: they convert between the generated protobuf messages
and the bindings/python producer.py / consumer.py / admin.py value types, and do
not depend on whether the driving Kafka client is the sync or the asyncio-native
one (`ProducerRecord`, `KafkaError` and admin.py's value types are the same
C-backed types in both). Kept in one module so the two servers don't duplicate
their conversion code.

The `CallbackLog` / `LoggingRebalanceListener` / `make_logging_*_callback`
section at the bottom is shared for the same reason, and is likewise
client-agnostic: the rebalance-listener and commit-callback signatures are
identical on `Consumer` and `AsyncConsumer`, and the listener methods are
deliberately plain (non-coroutine) so one implementation serves both.

Importing this module requires the generated proto stubs (`producer_service_pb2`,
`consumer_service_pb2`, `admin_service_pb2`) to be on `sys.path` — the servers
arrange that before importing us.
"""

import datetime as _dt
import threading  # noqa: E402  (CallbackLog's lock)

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


class AdminRequestError(ValueError):
    """A request this server cannot translate into a binding call at all.

    **This is a rule, not a one-off.** A request whose *arguments* are malformed
    (a dropped `OffsetSpec.kind`, a `FOR_TIMESTAMP` with no timestamp, ...) is a
    caller error, and Java answers caller errors with `IllegalArgumentException`.
    The C++ server stamps `VARIANT_ILLEGAL_ARGUMENT` for exactly these; raising a
    bare `ValueError` here instead landed in `_kafka_error_to_proto`'s
    generic-exception branch and crossed as `ILLEGAL_STATE`, so the two servers
    disagreed on the one field the Rust client matches on — `variant` — while
    both carried a comment claiming they agreed "at the same level" (true about
    the level, silent about the variant).

    So every request-validation failure in either Python server raises this, and
    `_kafka_error_to_proto` maps it to `ILLEGAL_ARGUMENT`. A genuine
    `KafkaError` from the binding is untouched and keeps its own variant. Same
    shape as `_admin_constructor_error`, which solved the constructor-rejection
    case; this generalises it to the request path.
    """


def _kafka_error_to_proto(err):
    """Translate a producer.py KafkaError (or generic Exception) into a
    proto KafkaError. The C FFI doesn't expose the structured variant
    discriminator (it's all KafkaError on the C side), so we infer the
    variant heuristically from the message — it has to round-trip
    through the wire because the Rust client matches on variant."""
    if isinstance(err, AdminRequestError):
        # Request-validation failure: Java's IllegalArgumentException, and the
        # variant the C++ server stamps for the same condition.
        return pb.KafkaError(
            variant=ILLEGAL_ARGUMENT,
            code=-1,
            message=f"python server: {err}",
            is_retriable=False,
            is_fatal=True,
        )
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


def _proto_offsets_to_dict(entries):
    """Translate repeated OffsetMapEntry into dict[TopicPartition, OffsetAndMetadata].

    Shared by the CommitSync and CommitAsync handlers; an empty `entries` yields
    an empty dict, which both handlers turn into "commit the current positions".
    """
    return {
        _tp(e.partition): kc.OffsetAndMetadata(
            e.offset.offset, e.offset.metadata,
            e.offset.leader_epoch if e.offset.HasField("leader_epoch") else None)
        for e in entries
    }


def _oam_to_proto(oam):
    return cpb.OffsetAndMetadata(
        offset=oam.offset,
        metadata=oam.metadata or "",
        leader_epoch=oam.leader_epoch if oam.leader_epoch is not None else None,
    )


# Metric value kinds as reported by metrics()'s "kind" key; mirrors the Rust
# MetricValue variants (see METRIC_VALUE_* in src/ffi/common.rs).
_METRIC_KIND_DOUBLE = 0
_METRIC_KIND_STRING = 1
_METRIC_KIND_LONG = 2
_METRIC_KIND_INT = 3


def _metric_to_proto(m):
    """One entry of a producer/consumer metrics() snapshot -> pb.Metric.

    `m` is a dict with keys name/group/description/tags/value/kind. `kind` picks
    the `value` oneof member; it is load-bearing for the integer cases because
    Python has a single `int` where Rust distinguishes Long from Int.

    `Metric`/`MetricList`/`MetricsResponse` live in producer_service.proto (the
    shared base that consumer_service.proto imports), so they are `pb.*` types
    reused by both the producer and consumer gRPC servers.
    """
    metric = pb.Metric(
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


# ---------------------------------------------------------------------------
# Callback log
#
# The Rust multilanguage harness cannot hand an in-process callback object to a
# server in another process, so instead it asks the server (via the
# SubscribeRequest.with_listener / CommitAsyncRequest.with_callback /
# SendRequest.with_callback flags) to register a *real* callback built by this
# binding, and reads back what those callbacks observed with GetCallbackLog.
#
# The kind strings and the field encoding are pinned by
# producer_service.proto's CallbackLogEntry — every server (this one, the async
# one, and the C++ one) must emit them identically or the shared Rust test body
# cannot compare backends.
# ---------------------------------------------------------------------------

KIND_ASSIGNED = "assigned"
KIND_REVOKED = "revoked"
KIND_LOST = "lost"
KIND_COMMIT = "commit"
KIND_DELIVERY = "delivery"


def _offset_key(topic, partition):
    """The CallbackLogEntry.offsets key for a partition: "<topic>-<partition>"."""
    return f"{topic}-{partition}"


class CallbackLog:
    """Thread-safe per-client log of user-callback invocations.

    One instance per service (producer or consumer), keyed by the server-local
    client id. The lock is mandatory rather than defensive: consumer callbacks
    are invoked from the Rust dispatcher thread and producer delivery callbacks
    from the completion thread, while GetCallbackLog is served on a gRPC worker
    thread — and in the async server the event loop is a *fourth* context. None
    of those are the same thread, in either server.
    """

    def __init__(self):
        self._entries = {}
        self._lock = threading.Lock()

    def append(self, client_id, kind, partitions=(), offsets=None, error=""):
        """Record one invocation.

        Args:
            client_id: server-local producer/consumer id.
            kind: one of the ``KIND_*`` constants.
            partitions: iterable of ``TopicPartition`` (or ``(topic, partition)``).
            offsets: optional ``{"<topic>-<partition>": offset}`` mapping.
            error: error message, or "" when the callback saw no error.
        """
        entry = pb.CallbackLogEntry(
            kind=kind,
            partitions=[_callback_log_partition(p) for p in partitions],
            offsets=dict(offsets or {}),
            error=error or "",
        )
        with self._lock:
            self._entries.setdefault(client_id, []).append(entry)

    def response(self, client_id):
        """The client's log as a CallbackLogResponse, oldest entry first.

        Entries are never dropped, not even when the client is closed: the
        callbacks a close() drives (a delivery report from its flush, a commit
        callback from its final drain) are exactly the ones a test wants to read
        afterwards. The server's lifetime is one test session, so the map cannot
        grow meaningfully.
        """
        with self._lock:
            entries = list(self._entries.get(client_id, ()))
        return pb.CallbackLogResponse(entries=entries)


def _callback_log_partition(p):
    """Accept either a TopicPartition or a plain ``(topic, partition)`` tuple."""
    if isinstance(p, tuple):
        topic, partition = p
    else:
        topic, partition = p.topic, p.partition
    return pb.CallbackLogPartition(topic=topic, partition=partition)


class LoggingRebalanceListener:
    """A real rebalance listener that records each invocation in a CallbackLog.

    The three methods are deliberately plain functions, not coroutines, in both
    servers: a coroutine listener method must not await AsyncConsumer methods
    (the dispatcher thread is parked in ``run_coroutine_threadsafe(...).result()``
    waiting for the listener, so the awaited FFI call could never complete — a
    deadlock). Nothing here needs to touch the consumer, so plain methods are
    both correct and the safe choice.

    ``on_partitions_lost`` is implemented explicitly rather than left to the
    binding's Java-faithful "delegate to on_partitions_revoked" default, so a
    lost callback is distinguishable from a revoke in the log.
    """

    __slots__ = ("_log", "_client_id")

    def __init__(self, log, client_id):
        self._log = log
        self._client_id = client_id

    def on_partitions_assigned(self, partitions):
        self._log.append(self._client_id, KIND_ASSIGNED, partitions)

    def on_partitions_revoked(self, partitions):
        self._log.append(self._client_id, KIND_REVOKED, partitions)

    def on_partitions_lost(self, partitions):
        self._log.append(self._client_id, KIND_LOST, partitions)


def make_logging_commit_callback(log, client_id):
    """A real ``OffsetCommitCallback`` recording the committed offsets."""

    def on_complete(offsets, exception):
        log.append(
            client_id,
            KIND_COMMIT,
            partitions=list(offsets.keys()),
            offsets={_offset_key(tp.topic, tp.partition): oam.offset
                     for tp, oam in offsets.items()},
            error="" if exception is None else str(exception),
        )

    return on_complete


def make_logging_delivery_callback(log, client_id):
    """A real ``on_delivery`` callback recording the delivered record's metadata.

    Both arguments can be set at once: a producer that rejects a record before
    it reaches the accumulator delivers placeholder metadata (offset/partition
    -1) *alongside* the error, mirroring Java's
    ``callback.onCompletion(nullMetadata, e)``. Record whichever is present.
    """

    def on_delivery(metadata, exception):
        partitions = ()
        offsets = None
        if metadata is not None:
            topic, partition = metadata.topic(), metadata.partition()
            partitions = ((topic, partition),)
            offsets = {_offset_key(topic, partition): metadata.offset()}
        log.append(
            client_id,
            KIND_DELIVERY,
            partitions=partitions,
            offsets=offsets,
            error="" if exception is None else str(exception),
        )

    return on_delivery


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
    """`optional int32 timeout_ms` -> the `timeout` admin.py takes.

    A ``timedelta`` of whole milliseconds rather than a float of seconds. The
    original went through ``request.timeout_ms / 1000.0``, and ``admin.py``'s
    ``_ms`` converts back with an integer truncation, so 1482 of the 200 001
    values in ``0..=200000`` lost 1 ms (1001 -> 1000, 2002 -> 2001, ...) on both
    Python backends while the C++ server passed the value verbatim — a latent
    2-vs-2 split on a field every admin request type already populates.
    ``_ms``/``_close_ms`` divide a ``timedelta`` by ``timedelta(milliseconds=1)``,
    which is exact integer arithmetic, so this round-trips."""
    return _dt.timedelta(milliseconds=request.timeout_ms) if request.HasField("timeout_ms") else None


def _admin_close_timeout(request):
    """`optional int64 timeout_ms` -> the `timeout` admin.py's close() takes.

    Absent is Java's no-argument `close()`. Shared by both servers' Close
    handlers, which previously each divided by 1000.0 inline and inherited the
    same 1 ms truncation as `_admin_timeout`."""
    return _dt.timedelta(milliseconds=request.timeout_ms) if request.HasField("timeout_ms") else None


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


# ---------------------------------------------------------------------------
# Cluster, configs & log dirs (slice G2)
# ---------------------------------------------------------------------------


def _admin_config_resource_to_proto(resource):
    """admin.py ConfigResource -> proto ConfigResource. `resource_type` is Java's
    `ConfigResource.Type.id()`, which is what admin.py already holds."""
    return apb.ConfigResource(resource_type=resource.resource_type, name=resource.name)


def _admin_config_resource_key(resource):
    return apb.ResultKey(config_resource=_admin_config_resource_to_proto(resource))


def _admin_replica_to_proto(replica):
    """admin.py TopicPartitionReplica -> proto TopicPartitionReplica."""
    return apb.TopicPartitionReplica(topic=replica.topic, partition=replica.partition,
                                     broker_id=replica.broker_id)


def _admin_replica_key(replica):
    return apb.ResultKey(replica=_admin_replica_to_proto(replica))


def _admin_config_resources(protos):
    """[proto ConfigResource] -> [admin.py ConfigResource]."""
    return [ka.ConfigResource(p.resource_type, p.name) for p in protos]


def _admin_alter_configs(protos):
    """[proto ConfigResourceOps] -> `{ConfigResource: [AlterConfigOp]}`.

    An absent op value is Java's null, which is what a DELETE carries."""
    out = {}
    for p in protos:
        resource = ka.ConfigResource(p.resource.resource_type, p.resource.name)
        out[resource] = [
            ka.AlterConfigOp(
                ka.ConfigEntry(op.name, op.value if op.HasField("value") else None,
                               False, False, False),
                op.op_type)
            for op in p.ops
        ]
    return out


def _admin_replicas(protos):
    """[proto TopicPartitionReplica] -> [admin.py TopicPartitionReplica]."""
    return [ka.TopicPartitionReplica(p.topic, p.partition, p.broker_id) for p in protos]


def _admin_replica_log_dir_assignments(protos):
    """[proto ReplicaLogDirAssignment] -> `{TopicPartitionReplica: log_dir}`."""
    return {ka.TopicPartitionReplica(p.replica.topic, p.replica.partition, p.replica.broker_id):
            p.log_dir
            for p in protos}


def _admin_cluster_description_response(description):
    """admin.py ClusterDescription -> DescribeClusterResponse. A whole-value
    response: Java's DescribeClusterResult holds four *independent* futures over
    attributes of one cluster, so there is nothing to key and any failure is a
    whole-call failure (which arrives here as a raise, not as this object)."""
    out = apb.ClusterDescription(
        cluster_id=description.cluster_id,
        nodes=[_node_to_proto(n) for n in description.nodes],
    )
    # Java's controller() is nullable: absent means no current controller.
    if description.controller is not None:
        out.controller.CopyFrom(_node_to_proto(description.controller))
    # Nullable: absent means the broker did not report the operations, which is
    # not the same as reporting that none are authorized.
    if description.authorized_operations is not None:
        out.authorized_operations.CopyFrom(
            apb.AclOperationList(operations=list(description.authorized_operations)))
    return apb.DescribeClusterResponse(description=out)


def _admin_full_config_entry_to_proto(entry):
    """admin.py ConfigEntry -> proto ConfigEntry, all nine fields.

    Unlike `_admin_config_entry_to_proto`, which serves createTopics and carries
    only the five fields that RPC reports, describe_configs populates every
    field, so all nine cross. `source` and `config_type` are Java's enum
    constant names; neither enum has a numeric id, so the name is the contract.
    Synonyms keep Java's precedence order."""
    out = apb.ConfigEntry(
        name=entry.name,
        value=entry.value if entry.value is not None else None,
        is_default=bool(entry.is_default),
        is_sensitive=bool(entry.is_sensitive),
        is_read_only=bool(entry.is_read_only),
        source=entry.source if entry.source is not None else None,
        config_type=entry.config_type if entry.config_type is not None else None,
        documentation=entry.documentation if entry.documentation is not None else None,
    )
    for synonym in entry.synonyms:
        out.synonyms.append(apb.ConfigSynonym(
            name=synonym.name,
            value=synonym.value if synonym.value is not None else None,
            source=synonym.source))
    return out


def _admin_describe_configs_response(outcomes):
    """`{ConfigResource: Config | KafkaError}` -> DescribeConfigsResponse."""
    entries = []
    for resource, outcome in outcomes.items():
        entry = apb.DescribeConfigsEntry(key=_admin_config_resource_key(resource))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(apb.AdminConfig(entries=[
                _admin_full_config_entry_to_proto(e) for e in outcome.entries]))
        entries.append(entry)
    return apb.DescribeConfigsResponse(entries=entries)


def _admin_list_config_resources_response(resources):
    """`[ConfigResource]` -> ListConfigResourcesResponse. A whole-value response:
    Java's ListConfigResourcesResult holds one future for the whole
    collection."""
    return apb.ListConfigResourcesResponse(
        resources=[_admin_config_resource_to_proto(r) for r in resources])


def _admin_list_client_metrics_resources_response(resources):
    """`[ClientMetricsResourceListing]` ->
    ListClientMetricsResourcesResponse. Whole-value, as above."""
    return apb.ListClientMetricsResourcesResponse(
        resources=[apb.ClientMetricsResourceListing(name=r.name) for r in resources])


def _admin_log_dir_description_to_proto(description):
    """admin.py LogDirDescription -> proto LogDirDescription.

    `error` is the log dir's own error (offline, unreadable): the broker
    answered, so it is *not* the per-broker error, which arrives as the entry's
    error arm instead of a value. `total_bytes` / `usable_bytes` are Java's
    OptionalLong, absent when the broker did not report them."""
    out = apb.LogDirDescription()
    if description.error is not None:
        out.error.CopyFrom(_kafka_error_to_proto(description.error))
    if description.total_bytes is not None:
        out.total_bytes = description.total_bytes
    if description.usable_bytes is not None:
        out.usable_bytes = description.usable_bytes
    for (topic, partition), info in description.replica_infos.items():
        out.replica_infos.append(apb.ReplicaInfoEntry(
            partition=cpb.TopicPartition(topic=topic, partition=partition),
            size=info.size,
            offset_lag=info.offset_lag,
            is_future=bool(info.is_future)))
    return out


def _admin_describe_log_dirs_response(outcomes):
    """`{broker: {log_dir: LogDirDescription} | KafkaError}` ->
    DescribeLogDirsResponse. The value is nested: one description per log-dir
    path, each with its own error."""
    entries = []
    for broker, outcome in outcomes.items():
        entry = apb.DescribeLogDirsEntry(key=apb.ResultKey(broker_id=broker))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            value = apb.LogDirDescriptionMap()
            for path, description in outcome.items():
                value.log_dirs[path].CopyFrom(_admin_log_dir_description_to_proto(description))
            entry.value.CopyFrom(value)
        entries.append(entry)
    return apb.DescribeLogDirsResponse(entries=entries)


def _admin_describe_replica_log_dirs_response(outcomes):
    """`{TopicPartitionReplica: ReplicaLogDirInfo | KafkaError}` ->
    DescribeReplicaLogDirsResponse."""
    entries = []
    for replica, outcome in outcomes.items():
        entry = apb.DescribeReplicaLogDirsEntry(key=_admin_replica_key(replica))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            value = apb.ReplicaLogDirInfo(
                current_replica_offset_lag=outcome.current_replica_offset_lag,
                future_replica_offset_lag=outcome.future_replica_offset_lag)
            # Both dirs are nullable: no replica hosted here, and no pending
            # move, respectively.
            if outcome.current_replica_log_dir is not None:
                value.current_replica_log_dir = outcome.current_replica_log_dir
            if outcome.future_replica_log_dir is not None:
                value.future_replica_log_dir = outcome.future_replica_log_dir
            entry.value.CopyFrom(value)
        entries.append(entry)
    return apb.DescribeReplicaLogDirsResponse(entries=entries)


# ---------------------------------------------------------------------------
# Elections, reassignments & offsets (slice G3)
# ---------------------------------------------------------------------------


def _admin_tp_tuple_key(key):
    """admin.py's `(topic, partition)` result key -> proto ResultKey.

    The G3 void RPCs (electLeaders, alterPartitionReassignments) key their
    results by TopicPartition, which admin.py hands back as a plain tuple, so
    `_admin_void_response`'s `key_fn(key)` needs the unpacking form rather than
    `_admin_partition_key`'s two arguments."""
    return _admin_partition_key(key[0], key[1])


def _admin_optional_partitions(request):
    """`optional TopicPartitionList partitions` -> the partition selection
    admin.py takes, or None.

    Absent is Java's **null** `Set` / `Optional.empty()`, i.e. every partition in
    the cluster; present-but-empty is an empty selection. `HasField` is what keeps
    them apart — testing the repeated field for emptiness would collapse the two,
    which is the bug class `admin_service.proto`'s ElectLeadersRequest documents.
    admin.py carries the same distinction as its own `partitions is None`
    column."""
    if not request.HasField("partitions"):
        return None
    return [(p.topic, p.partition) for p in request.partitions.partitions]


def _admin_reassignments(protos):
    """[proto PartitionReassignmentSpec] ->
    `{(topic, partition): NewPartitionReassignment | None}`.

    An absent `reassignment` is Java's empty `Optional`, which **cancels** that
    partition's ongoing reassignment. It must stay None rather than becoming a
    `NewPartitionReassignment([])`, which Java rejects outright — hence
    `HasField` rather than a check on `target_replicas`."""
    out = {}
    for p in protos:
        key = (p.partition.topic, p.partition.partition)
        out[key] = (ka.NewPartitionReassignment(list(p.reassignment.target_replicas))
                    if p.HasField("reassignment") else None)
    return out


# proto OffsetSpec.Kind -> the admin.py factory for that Java `OffsetSpec`
# variant. Calling the *named factory* is the point: the wire carries the variant
# name, so each server reaches the six `ListOffsets` sentinels through its own
# binding's table (admin.py's `OffsetSpec._EARLIEST` etc. here, the C entry
# point's `spec_timestamps` argument in the C++ server) and the two tables become
# differential instead of both forwarding one written in the Rust harness.
_ADMIN_OFFSET_SPEC_FACTORIES = {
    apb.OffsetSpec.EARLIEST: ka.OffsetSpec.earliest,
    apb.OffsetSpec.LATEST: ka.OffsetSpec.latest,
    apb.OffsetSpec.MAX_TIMESTAMP: ka.OffsetSpec.max_timestamp,
    apb.OffsetSpec.EARLIEST_LOCAL: ka.OffsetSpec.earliest_local,
    apb.OffsetSpec.LATEST_TIERED: ka.OffsetSpec.latest_tiered,
    apb.OffsetSpec.EARLIEST_PENDING_UPLOAD: ka.OffsetSpec.earliest_pending_upload,
}


def _admin_offset_specs(protos):
    """[proto OffsetSpecEntry] -> `{(topic, partition): OffsetSpec}`.

    KIND_UNSPECIFIED and FOR_TIMESTAMP-without-a-timestamp are protocol errors
    rather than a defaulted variant: a dropped `kind` field must fail the call,
    not silently become `earliest()` and pass. They raise `AdminRequestError`, so
    they cross with the **ILLEGAL_ARGUMENT variant** the C++ server stamps for the
    same condition -- not merely at the same level. See `AdminRequestError`."""
    out = {}
    for p in protos:
        key = (p.partition.topic, p.partition.partition)
        if p.spec.kind == apb.OffsetSpec.FOR_TIMESTAMP:
            if not p.spec.HasField("timestamp"):
                raise AdminRequestError(
                    f"OffsetSpec FOR_TIMESTAMP for {key} carries no timestamp")
            out[key] = ka.OffsetSpec.for_timestamp(p.spec.timestamp)
            continue
        factory = _ADMIN_OFFSET_SPEC_FACTORIES.get(p.spec.kind)
        if factory is None:
            raise AdminRequestError(
                f"OffsetSpec for {key} has kind {p.spec.kind}, not a Java OffsetSpec variant")
        out[key] = factory()
    return out


def _admin_list_partition_reassignments_response(reassignments):
    """`{(topic, partition): PartitionReassignment}` ->
    ListPartitionReassignmentsResponse.

    A whole-value response: Java's ListPartitionReassignmentsResult holds one
    future for the entire map, so a failure arrives here as a raise rather than
    per key. Only partitions with an ongoing reassignment appear."""
    out = apb.ListPartitionReassignmentsResponse()
    for (topic, partition), reassignment in reassignments.items():
        entry = out.reassignments.add()
        entry.partition.topic = topic
        entry.partition.partition = partition
        entry.reassignment.replicas.extend(reassignment.replicas)
        entry.reassignment.adding_replicas.extend(reassignment.adding_replicas)
        entry.reassignment.removing_replicas.extend(reassignment.removing_replicas)
    return out


def _admin_list_offsets_response(outcomes):
    """`{(topic, partition): ListOffsetsResultInfo | KafkaError}` ->
    ListOffsetsResponse."""
    entries = []
    for (topic, partition), outcome in outcomes.items():
        entry = apb.ListOffsetsEntry(key=_admin_partition_key(topic, partition))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            value = apb.ListOffsetsResultInfo(offset=outcome.offset,
                                              timestamp=outcome.timestamp)
            # Java's leaderEpoch() is an Optional<Integer>; absent is not epoch 0.
            if outcome.leader_epoch is not None:
                value.leader_epoch = outcome.leader_epoch
            entry.value.CopyFrom(value)
        entries.append(entry)
    return apb.ListOffsetsResponse(entries=entries)


# ---------------------------------------------------------------------------
# Groups & offsets (slice G4)
#
# admin.py's group methods hand back the same resolved shapes as the earlier
# slices, with two exceptions worth naming here:
#
#   - list_groups / list_consumer_groups return a *pair* of lists,
#     `([listing], [KafkaError])`, because Java's ListGroupsResult splits one
#     future into valid() and an unkeyed errors() collection. The two are
#     independent and generally of different length, so nothing may be zipped or
#     indexed across them.
#   - list_consumer_group_offsets' per-group value is itself a map whose values
#     are nullable: an inner None is Java's null map value, "this group has no
#     committed offset for that partition", which is not offset 0.
# ---------------------------------------------------------------------------


def _admin_group_listing_to_proto(listing):
    """admin.py GroupListing -> proto GroupListing.

    `group_type` / `group_state` are Java Optionals and stay absent when None —
    "the broker did not report a state" is not the empty string.
    `is_simple_consumer_group` is derived in Java rather than a field, but both
    bindings expose it, so it crosses and the Rust client checks it against the
    derived value.
    """
    out = apb.GroupListing(group_id=listing.group_id,
                           protocol=listing.protocol,
                           is_simple_consumer_group=bool(listing.is_simple_consumer_group))
    if listing.group_type is not None:
        out.group_type = listing.group_type
    if listing.group_state is not None:
        out.group_state = listing.group_state
    return out


def _admin_list_groups_response(outcome):
    """`([GroupListing], [KafkaError])` -> ListGroupsResponse.

    A whole-value response whose value is Java's valid()/errors() split; a
    failure of the single underlying future raises instead and becomes the
    top-level error.
    """
    valid, errors = outcome
    return apb.ListGroupsResponse(
        valid=[_admin_group_listing_to_proto(listing) for listing in valid],
        listing_errors=[_kafka_error_to_proto(e) for e in errors])


def _admin_consumer_group_listing_to_proto(listing):
    """admin.py ConsumerGroupListing -> proto ConsumerGroupListing.

    Both `group_state` and the deprecated `state` cross even though Java derives
    the second from the first: a backend that dropped one is a finding, and the
    Rust client checks the pair.
    """
    out = apb.ConsumerGroupListing(
        group_id=listing.group_id,
        is_simple_consumer_group=bool(listing.is_simple_consumer_group))
    if listing.group_state is not None:
        out.group_state = listing.group_state
    if listing.state is not None:
        out.state = listing.state
    if listing.group_type is not None:
        out.group_type = listing.group_type
    return out


def _admin_list_consumer_groups_response(outcome):
    """`([ConsumerGroupListing], [KafkaError])` -> ListConsumerGroupsResponse."""
    valid, errors = outcome
    return apb.ListConsumerGroupsResponse(
        valid=[_admin_consumer_group_listing_to_proto(listing) for listing in valid],
        listing_errors=[_kafka_error_to_proto(e) for e in errors])


def _admin_member_assignment_to_proto(assignment):
    """admin.py MemberAssignment -> proto MemberAssignment."""
    return apb.MemberAssignment(topic_partitions=[
        cpb.TopicPartition(topic=topic, partition=partition)
        for topic, partition in assignment.topic_partitions
    ])


def _admin_member_description_to_proto(member):
    """admin.py MemberDescription -> proto MemberDescription.

    `group_instance_id` / `rack_id` / `member_epoch` / `upgraded` are Java
    Optionals and stay absent when None: a static member with an empty instance
    id is not a dynamic member, and an absent member epoch is not epoch 0.
    `target_assignment` is nullable too, and an absent one must not become an
    empty assignment.
    """
    out = apb.MemberDescription(
        consumer_id=member.consumer_id,
        client_id=member.client_id,
        host=member.host,
        assignment=_admin_member_assignment_to_proto(member.assignment))
    if member.group_instance_id is not None:
        out.group_instance_id = member.group_instance_id
    if member.rack_id is not None:
        out.rack_id = member.rack_id
    if member.target_assignment is not None:
        out.target_assignment.CopyFrom(
            _admin_member_assignment_to_proto(member.target_assignment))
    if member.member_epoch is not None:
        out.member_epoch = member.member_epoch
    if member.upgraded is not None:
        out.upgraded = member.upgraded
    return out


def _admin_consumer_group_description_to_proto(description):
    """admin.py ConsumerGroupDescription -> proto ConsumerGroupDescription.

    The coordinator is the field this whole milestone was motivated by: it must
    carry the broker's real host and port, not a placeholder. `_node_to_proto`
    returns None for a null coordinator, which stays absent.
    """
    out = apb.ConsumerGroupDescription(
        group_id=description.group_id,
        is_simple_consumer_group=bool(description.is_simple_consumer_group),
        members=[_admin_member_description_to_proto(m) for m in description.members],
        partition_assignor=description.partition_assignor,
        group_type=description.group_type,
        state=description.state,
        group_state=description.group_state)
    node = _node_to_proto(description.coordinator)
    if node is not None:
        out.coordinator.CopyFrom(node)
    # Absent means the broker did not report the operations at all, which is not
    # the same as reporting that none are authorized.
    if description.authorized_operations is not None:
        out.authorized_operations.CopyFrom(
            apb.AclOperationList(operations=list(description.authorized_operations)))
    if description.group_epoch is not None:
        out.group_epoch = description.group_epoch
    if description.target_assignment_epoch is not None:
        out.target_assignment_epoch = description.target_assignment_epoch
    return out


def _admin_classic_group_description_to_proto(description):
    """admin.py ClassicGroupDescription -> proto ClassicGroupDescription.

    `protocol` (the protocol type) and `protocol_data` (the selected assignment
    strategy) are two different response fields; both cross so a transposition is
    detectable.
    """
    out = apb.ClassicGroupDescription(
        group_id=description.group_id,
        protocol=description.protocol,
        protocol_data=description.protocol_data,
        is_simple_consumer_group=bool(description.is_simple_consumer_group),
        members=[_admin_member_description_to_proto(m) for m in description.members],
        state=description.state)
    node = _node_to_proto(description.coordinator)
    if node is not None:
        out.coordinator.CopyFrom(node)
    if description.authorized_operations is not None:
        out.authorized_operations.CopyFrom(
            apb.AclOperationList(operations=list(description.authorized_operations)))
    return out


def _admin_describe_consumer_groups_response(outcomes):
    """`{group_id: ConsumerGroupDescription | KafkaError}` ->
    DescribeConsumerGroupsResponse."""
    entries = []
    for group_id, outcome in outcomes.items():
        entry = apb.DescribeConsumerGroupsEntry(key=_admin_name_key(group_id))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(_admin_consumer_group_description_to_proto(outcome))
        entries.append(entry)
    return apb.DescribeConsumerGroupsResponse(entries=entries)


def _admin_describe_classic_groups_response(outcomes):
    """`{group_id: ClassicGroupDescription | KafkaError}` ->
    DescribeClassicGroupsResponse."""
    entries = []
    for group_id, outcome in outcomes.items():
        entry = apb.DescribeClassicGroupsEntry(key=_admin_name_key(group_id))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(_admin_classic_group_description_to_proto(outcome))
        entries.append(entry)
    return apb.DescribeClassicGroupsResponse(entries=entries)


def _admin_group_offset_specs(protos):
    """[proto ListConsumerGroupOffsetsSpec] ->
    `{group_id: ListConsumerGroupOffsetsSpec}`.

    An absent `topic_partitions` is Java's **unset** collection — every partition
    the group has committed offsets for — and present-but-empty selects nothing.
    `HasField` is what keeps them apart; testing the repeated field for emptiness
    would collapse the two, the bug class ElectLeadersRequest documents.
    """
    out = {}
    for p in protos:
        partitions = (None if not p.HasField("topic_partitions")
                      else [(tp.topic, tp.partition) for tp in p.topic_partitions.partitions])
        out[p.group_id] = ka.ListConsumerGroupOffsetsSpec(partitions)
    return out


def _admin_list_consumer_group_offsets_response(outcomes):
    """`{group_id: {(topic, partition): OffsetAndMetadata | None} | KafkaError}`
    -> ListConsumerGroupOffsetsResponse.

    Two levels, like describeLogDirs: the per-group future carries a whole map.
    An inner None is Java's null map value and stays absent on the wire — it is
    what proves deleteConsumerGroupOffsets removed the commit rather than zeroing
    it.
    """
    entries = []
    for group_id, outcome in outcomes.items():
        entry = apb.ListConsumerGroupOffsetsEntry(key=_admin_name_key(group_id))
        if isinstance(outcome, kp.KafkaError):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            value = apb.GroupOffsets()
            for (topic, partition), offset in outcome.items():
                pair = value.offsets.add()
                pair.partition.topic = topic
                pair.partition.partition = partition
                if offset is not None:
                    pair.offset.CopyFrom(_oam_to_proto(offset))
            entry.value.CopyFrom(value)
        entries.append(entry)
    return apb.ListConsumerGroupOffsetsResponse(entries=entries)


def _admin_group_offset_commits(protos):
    """[proto GroupOffsetCommit] -> `{(topic, partition): OffsetAndMetadata}`.

    `leader_epoch` absent is Java's empty Optional, which admin.py carries as
    None and turns into its own present-flag column at the C boundary, so it must
    not become 0 here.
    """
    out = {}
    for p in protos:
        key = (p.partition.topic, p.partition.partition)
        out[key] = kc.OffsetAndMetadata(
            p.offset.offset,
            p.offset.metadata,
            p.offset.leader_epoch if p.offset.HasField("leader_epoch") else None)
    return out


def _admin_members_to_remove(request):
    """`optional MemberToRemoveList members` -> the selection admin.py takes.

    Absent is Java's no-argument RemoveMembersFromConsumerGroupOptions(), i.e.
    removeAll, which admin.py spells `members=None`. Present-but-empty is the
    Collection constructor with an empty collection, which Java rejects with
    IllegalArgumentException — so it must stay an empty *list* and not collapse to
    None. `HasField` is the discriminant, never emptiness.
    """
    if not request.HasField("members"):
        return None
    return [ka.MemberToRemove(m.group_instance_id) for m in request.members.members]
