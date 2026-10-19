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
and the value types of the ``confluent_kafka`` package (producer, consumer and
admin), and do not depend on whether the driving Kafka client is the sync or the
asyncio-native one (`ProducerRecord`, `KafkaError` and the admin value types are
the same types in both). Kept in one module so the two servers don't duplicate
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

# The gRPC integration servers speak the new ``confluent_kafka`` package
# (CLAUDE.md, Python Binding Conventions, Modules), not the retired top-level
# ``producer.py`` / ``consumer.py``.
from confluent_kafka import IllegalArgumentError, IllegalStateError  # noqa: E402
from confluent_kafka.common import TopicPartition  # noqa: E402
from confluent_kafka._errors import to_ffi_id
from confluent_kafka.common import KafkaError  # noqa: E402
# The broker wire ``TimeoutException`` (code 7), distinct from the JDK-analog
# ``confluent_kafka.TimeoutError`` (code -5). Aliased to avoid the name clash.
from confluent_kafka.common.errors import TimeoutError as _WireTimeout  # noqa: E402
from confluent_kafka.consumer import (  # noqa: E402
    CloseOptions,
    ConsumerRebalanceListener,
    OffsetAndMetadata,
    SubscriptionPattern,
)
from confluent_kafka.producer import ProducerRecord  # noqa: E402
# The admin client is the new ``confluent_kafka.admin`` package: keyword-only
# value types with method accessors, typed errors, results that hold futures.
from confluent_kafka import ExecutionError  # noqa: E402
from confluent_kafka.common import AsyncKafkaFuture, KafkaFuture  # noqa: E402
import types as _types  # noqa: E402
from confluent_kafka import admin as ka  # noqa: E402  (NewTopic / NewPartitions / ...)
from confluent_kafka.common import (  # noqa: E402
    AccessControlEntry,
    AccessControlEntryFilter,
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    ClientQuotaAlteration,
    ClientQuotaEntity,
    ClientQuotaFilter,
    ClientQuotaFilterComponent,
    ElectionType,
    GroupState,
    GroupType,
    IsolationLevel,
    KafkaPrincipal,
    Node,
    PatternType,
    ResourcePattern,
    ResourcePatternFilter,
    ResourceType,
    TopicCollection,
    TopicPartitionReplica,
    Uuid,
)
from confluent_kafka.common.config import ConfigResource  # noqa: E402
import producer_service_pb2 as pb  # noqa: E402  (generated)
import consumer_service_pb2 as cpb  # noqa: E402  (generated)
import admin_service_pb2 as apb  # noqa: E402  (generated)

# Error codes formerly imported from the generated ``_error_code.py`` module
# (retired in P6). The proto's ``KafkaError.code`` is the FFI error code
# (``kafka_common_Error_e``), so a synthetic server-side error stamps the
# code by reading the typed error class' private ``_ffi_id`` — the same value
# the generator wrote into ``_error_code.py``. These are the only codes the
# servers manufacture (LOCAL_ILLEGAL_ARGUMENT / LOCAL_ILLEGAL_STATE /
# REQUEST_TIMED_OUT); everything else crosses via ``to_ffi_id`` on a real error.
_LOCAL_ILLEGAL_ARGUMENT = IllegalArgumentError._ffi_id
_LOCAL_ILLEGAL_STATE = IllegalStateError._ffi_id
# ``REQUEST_TIMED_OUT`` is Java's wire code 7; the wire ``TimeoutException``
# class carries it as ``_ffi_id``.
_REQUEST_TIMED_OUT = _WireTimeout._ffi_id


def _error_code_of(err):
    """The proto ``KafkaError.code`` for a Python error.

    A typed error from the ``confluent_kafka`` package maps through
    ``to_ffi_id``. Any error class without an ``_ffi_id`` (a plain Python
    exception) falls back to the catch-all local-illegal-state code.
    """
    try:
        return to_ffi_id(err)
    except TypeError:
        return _LOCAL_ILLEGAL_STATE


class AdminRequestError(ValueError):
    """A request this server cannot translate into a binding call at all.

    **This is a rule, not a one-off.** A request whose *arguments* are malformed
    (a dropped `OffsetSpec.kind`, a `FOR_TIMESTAMP` with no timestamp, ...) is a
    caller error, and Java answers caller errors with `IllegalArgumentException`.
    The C++ server stamps `LOCAL_ILLEGAL_ARGUMENT` for exactly these; raising a
    bare `ValueError` here instead landed in `_kafka_error_to_proto`'s
    generic-exception branch and crossed as `LOCAL_ILLEGAL_STATE`, so the two
    servers disagreed on the one field the Rust client decodes — `code` — while
    both carried a comment claiming they agreed "at the same level" (true about
    the level, silent about the class).

    So every request-validation failure in either Python server raises this, and
    `_kafka_error_to_proto` maps it to `LOCAL_ILLEGAL_ARGUMENT`. A genuine
    `KafkaError` from the binding is untouched and keeps its own class. Same
    shape as `_admin_constructor_error`, which solved the constructor-rejection
    case; this generalises it to the request path.
    """


class _AdminEncodeError(RuntimeError):
    """A *response* this server cannot encode: the binding reported success but
    handed back a value with a required field missing.

    Distinct from [AdminRequestError], which is a malformed *request* and is
    Java's `IllegalArgumentException`. This one is a binding/server bug, so it
    crosses as LOCAL_ILLEGAL_STATE through `_kafka_error_to_proto`'s generic
    branch — which is exactly the class the C++ server's
    `make_synthetic_error(...)` stamps for the same state, so the servers agree
    on the *class* and not merely on the level (the mistake round 14 caught for
    the request direction).

    It exists so the failure is a typed envelope rather than an AttributeError
    escaping as a bare gRPC UNKNOWN with no KafkaError at all, which the Rust
    client cannot classify.
    """


def _kafka_error_to_proto(err):
    """Translate a ``confluent_kafka`` KafkaError (or generic Exception) into a
    proto KafkaError.

    `code` is the only discriminator the proto carries, and that is enough:
    it is the FFI error code (kafka_common_Error_e), which is injective
    over the client's error classes, so the Rust client derives the class from
    it. This is what retired `_guess_variant`, which substring-matched the
    message text to recover a class the code could not carry back when several
    classes shared code -1.

    `is_retriable` / `is_fatal` are gone from the proto too: no reader consumed
    them and both are derivable from the code. KafkaError.is_retriable stays in
    the Python public API; it simply has no internal caller.
    """
    if isinstance(err, AdminRequestError):
        # Request-validation failure: Java's IllegalArgumentException, and the
        # class the C++ server stamps for the same condition.
        return pb.KafkaError(
            code=_LOCAL_ILLEGAL_ARGUMENT,
            message=f"python server: {err}",
        )
    # A real client error: a Kafka error, or one of the JDK-analog classes
    # (``TimeoutError``, ``IllegalStateError``, ...) the core names with an id of
    # its own. Both resolve to their FFI code via ``_error_code_of``.
    if isinstance(err, KafkaError) or hasattr(type(err), "_ffi_id"):
        return pb.KafkaError(code=_error_code_of(err), message=str(err) or "")
    # Unexpected non-Kafka exception: this is the server's own bookkeeping
    # failure, not an error the client reported, so it is fabricated as
    # LocalIllegalState. That is inventing an error rather than guessing at
    # one, which is why this path survives while _guess_variant did not.
    return pb.KafkaError(
        code=_LOCAL_ILLEGAL_STATE,
        message=f"python server: {type(err).__name__}: {err}",
    )


def _admin_synthetic_error(message):
    """A server-manufactured `KafkaError` for a per-entry state the binding
    should not have produced.

    Mirrors the C++ server's `make_synthetic_error(...)` field for field (the
    `LOCAL_ILLEGAL_STATE` code, and the message) so that when both servers meet
    the same impossible input they answer with the same *class* and not merely
    at the same level — the distinction round 14 caught for the request
    direction. Only the `"c server: "` / `"python server: "` prefix differs,
    which is deliberate: it names which server manufactured it.
    """
    return pb.KafkaError(
        code=_LOCAL_ILLEGAL_STATE,
        message=f"python server: {message}",
    )


def _record_metadata_to_proto(meta):
    """Translate a :class:`confluent_kafka.producer.RecordMetadata` to its proto
    form. The new ``RecordMetadata`` exposes the serialized key/value sizes
    (Java's 8 methods), so they cross faithfully instead of the old ``-1``."""
    return pb.RecordMetadata(
        offset=meta.offset(),
        timestamp=meta.timestamp(),
        serialized_key_size=meta.serialized_key_size(),
        serialized_value_size=meta.serialized_value_size(),
        topic=meta.topic(),
        partition=meta.partition(),
    )


def _proto_to_producer_record(proto_record):
    """Translate a proto ``ProducerRecord`` to a
    :class:`confluent_kafka.producer.ProducerRecord` (keyword-only ctor).

    ``value`` is proto3 ``optional``: an absent value is Java's null-value
    tombstone, which the new ``ProducerRecord`` accepts as ``None`` (unlike the
    retired wrapper, which forced an empty bytestring). ``partition`` /
    ``timestamp`` are ``None`` when unset (Java's null), not the old ``-1``
    sentinel.

    The producer send path DOES carry ProducerRecord headers end-to-end now (the
    native record + FFI struct + Rust send preserve them — B1), but the gRPC
    *producer* proto ``ProducerRecord`` has no ``headers`` field, so this
    translation cannot forward them; they default to empty on this arm. Adding a
    headers field to the producer proto is a separate gRPC-contract change. (The
    consumer arm's proto DOES carry headers — see ``_consumer_record_to_proto``.)"""
    value = proto_record.value if proto_record.HasField("value") else None
    key = proto_record.key if proto_record.HasField("key") else None
    partition = proto_record.partition if proto_record.HasField("partition") else None
    timestamp = proto_record.timestamp if proto_record.HasField("timestamp") else None
    return ProducerRecord(
        topic=proto_record.topic,
        value=value,
        key=key,
        partition=partition,
        timestamp=timestamp,
    )


def _member(obj, name):
    """Read ``obj.name`` whether it is a method (the keyword-only value types,
    whose accessors are methods) or a bare attribute (the removed legacy admin
    binding's positional ``Node`` / ``PartitionInfo``; the ``confluent_kafka``
    value types all have method accessors). The node/partition-info proto
    helpers go through this."""
    attr = getattr(obj, name)
    return attr() if callable(attr) else attr


def _node_to_proto(node):
    if node is None:
        return None
    rack = _member(node, "rack")
    # Java's Node.isFenced() (KIP-1073); a node from partition metadata or a
    # coordinator lookup is never fenced.
    return pb.Node(id=_member(node, "id"), host=_member(node, "host"),
                   port=_member(node, "port"),
                   rack=rack if rack is not None else None,
                   is_fenced=bool(_member(node, "is_fenced")))


def _partition_info_to_proto(info):
    return pb.PartitionInfo(
        topic=info.topic(),
        partition=info.partition(),
        leader=_node_to_proto(info.leader()),
        replicas=[_node_to_proto(n) for n in info.replicas()],
        in_sync_replicas=[_node_to_proto(n) for n in info.in_sync_replicas()],
        offline_replicas=[_node_to_proto(n) for n in info.offline_replicas()],
    )


def _tp(proto_tp):
    return TopicPartition(topic=proto_tp.topic, partition=proto_tp.partition)


def _tp_to_proto(tp):
    return cpb.TopicPartition(topic=tp.topic(), partition=tp.partition())


def _proto_offsets_to_dict(entries):
    """Translate repeated OffsetMapEntry into dict[TopicPartition, OffsetAndMetadata].

    Shared by the CommitSync and CommitAsync handlers; an empty `entries` yields
    an empty dict, which both handlers turn into "commit the current positions".
    """
    return {
        _tp(e.partition): OffsetAndMetadata(
            offset=e.offset.offset, metadata=e.offset.metadata,
            leader_epoch=(e.offset.leader_epoch
                          if e.offset.HasField("leader_epoch") else None))
        for e in entries
    }


def _proto_offset_entries_to_dict(entries):
    """Translate the producer proto's repeated ``OffsetEntry`` into
    ``dict[TopicPartition, OffsetAndMetadata]`` for send_offsets_to_transaction.

    Unlike :func:`_proto_offsets_to_dict` (which reads the *consumer* proto's
    nested ``OffsetMapEntry.partition`` / ``.offset``), ``OffsetEntry`` is FLAT:
    ``topic`` / ``partition`` / ``offset`` are always present, while
    ``leader_epoch`` and ``metadata`` are proto3 ``optional``. An absent
    ``leader_epoch`` means "no epoch" — ``None``, which
    ``Producer._offsets_to_spec`` marshals to the FFI's ``-1`` — and an absent
    ``metadata`` means the empty string, matching ``OffsetAndMetadata``'s own
    defaults and the sync FFI's ``read_offset_map`` contract. An empty
    ``entries`` yields an empty dict (a legitimate "stage nothing").
    """
    return {
        _tp(e): OffsetAndMetadata(
            offset=e.offset,
            metadata=e.metadata if e.HasField("metadata") else "",
            leader_epoch=(e.leader_epoch
                          if e.HasField("leader_epoch") else None))
        for e in entries
    }


class GroupMetadataStore:
    """Server-side group-metadata objects, by id, shared by the producer and
    consumer services of one server.

    ``ConsumerService.GroupMetadata`` adds what ``Consumer.group_metadata()``
    returned, and ``ProducerService.SendOffsetsToTransaction`` passes it back to
    ``Producer.send_offsets_to_transaction``:
    :class:`confluent_kafka.consumer.ConsumerGroupMetadata` has no constructor
    (Java deprecated its constructors in 4.2), so an object a consumer handed out
    is the only one a producer can take.
    ``ConsumerService.ReleaseGroupMetadata`` drops it when the Rust client drops
    its last reference, and its Rust handle is freed on GC. The object holds its
    own reference to the metadata, so it stays valid after its consumer closes.

    Locked because the sync server serves each RPC on its own worker thread.
    """

    def __init__(self):
        self._entries = {}
        self._next_id = 1
        self._lock = threading.Lock()

    def add(self, group_metadata):
        with self._lock:
            group_metadata_id = self._next_id
            self._next_id += 1
            self._entries[group_metadata_id] = group_metadata
        return group_metadata_id

    def get(self, group_metadata_id):
        """The stored object, or ``None`` for an unknown (or released) id."""
        with self._lock:
            return self._entries.get(group_metadata_id)

    def release(self, group_metadata_id):
        """Drop the object; unknown ids are ignored."""
        with self._lock:
            self._entries.pop(group_metadata_id, None)


def _group_metadata_to_proto(group_metadata):
    """The four fields of a :class:`confluent_kafka.consumer.ConsumerGroupMetadata`
    as the producer proto's ``ConsumerGroupMetadata`` message. ``group_instance_id()``
    is ``None`` for a dynamic member, which leaves the proto3 ``optional`` unset."""
    return pb.ConsumerGroupMetadata(
        group_id=group_metadata.group_id(),
        generation_id=group_metadata.generation_id(),
        member_id=group_metadata.member_id(),
        group_instance_id=group_metadata.group_instance_id(),
    )


def _consumer_timeout(request):
    """The optional ``timeout_ms`` of a ConsumerService request as the
    binding's ``timeout`` (seconds): ``None`` (not given) selects the Java
    overload without a ``Duration``."""
    return request.timeout_ms / 1000.0 if request.HasField("timeout_ms") else None


def _consumer_subscribe_kwargs(request, listener):
    """SubscribeRequest -> the keyword arguments of ``Consumer.subscribe``:
    ``pattern`` (a ``SubscriptionPattern``, Java's ``subscribe(SubscriptionPattern
    [, listener])``) when the request carries one, else ``topics``."""
    if request.HasField("pattern"):
        return {"pattern": SubscriptionPattern(pattern=request.pattern), "callback": listener}
    return {"topics": list(request.topics), "callback": listener}


def _consumer_close_options(request):
    """ConsumerCloseRequest -> the ``CloseOptions`` of Java's
    ``close(CloseOptions)``, or ``None`` for the no-argument ``close()``. The
    operation crosses as its enum constant name; the timeout is optional."""
    if not request.HasField("group_membership_operation"):
        return None
    operation = CloseOptions.GroupMembershipOperation[request.group_membership_operation]
    timeout = (request.close_timeout_ms / 1000.0
               if request.HasField("close_timeout_ms") else None)
    return CloseOptions(operation=operation, timeout=timeout)


def _current_lag_response(lag):
    """``current_lag()``'s ``int | None`` (Java's ``OptionalLong``) ->
    CurrentLagResponse, absent ``lag`` for an empty one."""
    return cpb.CurrentLagResponse() if lag is None else cpb.CurrentLagResponse(lag=lag)


def _oam_to_proto(oam):
    # Shared by the consumer ``Committed`` handler and the admin group-offsets
    # response, both ``confluent_kafka.consumer.OffsetAndMetadata`` with method
    # accessors; read through ``_member`` like the node helpers.
    leader_epoch = _member(oam, "leader_epoch")
    return cpb.OffsetAndMetadata(
        offset=_member(oam, "offset"),
        metadata=_member(oam, "metadata") or "",
        leader_epoch=leader_epoch if leader_epoch is not None else None,
    )


def _metric_to_proto(metric):
    """One entry of a producer/consumer ``metrics()`` snapshot -> ``pb.Metric``.

    ``metric`` is a :class:`confluent_kafka.common.Metric` (the snapshot is now
    ``dict[MetricName, Metric]``, Java's ``Map``; the handlers iterate ``.values()``).
    Its ``MetricName`` carries name/group/description/tags; ``metric_value()`` is
    the value. The proto ``value`` oneof is picked from the Python type of the
    value — ``str`` -> string, ``bool``/``int`` -> long, ``float`` -> double.
    (The public ``Metric`` no longer distinguishes the Rust ``Long`` vs ``Int``
    gauge cases — both are a Python ``int`` — so an integer value crosses as
    ``long_value``; the integration metric assertions are structural on
    name/group/tags, not on the numeric oneof member.)

    ``Metric``/``MetricList``/``MetricsResponse`` live in producer_service.proto
    (the shared base that consumer_service.proto imports), so they are ``pb.*``
    types reused by both the producer and consumer gRPC servers.
    """
    name = metric.metric_name()
    out = pb.Metric(
        name=name.name(),
        group=name.group(),
        description=name.description(),
    )
    out.tags.update(name.tags())
    value = metric.metric_value()
    if isinstance(value, str):
        out.string_value = value
    elif isinstance(value, bool):
        # bool is an int subclass — check it first; the proto has no bool member,
        # so a boolean gauge crosses as its integer value.
        out.long_value = int(value)
    elif isinstance(value, int):
        out.long_value = value
    else:
        out.double_value = float(value)
    return out


def _record_to_proto(r):
    """A :class:`confluent_kafka.consumer.ConsumerRecord` (accessors are methods,
    rule 15) -> ``pb.ConsumerRecord``.

    ``key()`` / ``value()`` are ``bytes`` deserializer output (the gRPC servers
    build byte-typed consumers), or ``None`` for a tombstone; ``headers()``
    yields ``(str, memoryview)`` pairs; ``timestamp_type()`` is a
    ``TimestampType`` int-enum whose value is the proto's ``int32``."""
    key = bytes(r.key()) if r.key() is not None else None
    value = bytes(r.value()) if r.value() is not None else None
    headers = [pb.Header(key=k, value=bytes(v) if v is not None else b"")
               for (k, v) in r.headers()]
    leader_epoch = r.leader_epoch()
    return cpb.ConsumerRecord(
        topic=r.topic(),
        partition=r.partition(),
        offset=r.offset(),
        timestamp=r.timestamp(),
        timestamp_type=int(r.timestamp_type()),
        key=key,
        value=value,
        headers=headers,
        leader_epoch=leader_epoch if leader_epoch is not None else None,
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
    run on the thread (or event loop) of the consumer call that delivers them,
    and producer delivery callbacks on the completion thread, while
    GetCallbackLog is served on another gRPC worker thread.
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
    """Accept either a new ``TopicPartition`` (method accessors, rule 15) or a
    plain ``(topic, partition)`` tuple."""
    if isinstance(p, tuple):
        topic, partition = p
    else:
        topic, partition = p.topic(), p.partition()
    return pb.CallbackLogPartition(topic=topic, partition=partition)


class LoggingRebalanceListener(ConsumerRebalanceListener):
    """A real rebalance listener that records each invocation in a CallbackLog.

    Subclasses ``ConsumerRebalanceListener`` (a multi-method interface the user
    implements, a class). The listener runs on the caller's thread inside
    ``poll()`` / ``close()`` (consumer-threading §31), so these plain ``def``
    methods run on the gRPC worker thread (or the event loop) that called
    ``poll()``. They only touch the log, never the consumer, so no reentrancy
    is involved.

    ``on_partitions_lost`` is implemented explicitly rather than left to the
    binding's Java-faithful "delegate to on_partitions_revoked" default, so a
    lost callback is distinguishable from a revoke in the log.
    """

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
    """A real ``OffsetCommitCallback`` (``callback=``) recording the committed
    offsets.

    The callback receives ``offsets: dict[TopicPartition, OffsetAndMetadata] |
    None`` (``None`` when the commit failed before its offsets were known, as
    Java passes ``null``) and an optional exception.
    """

    def on_complete(offsets, exception):
        offsets = offsets or {}
        log.append(
            client_id,
            KIND_COMMIT,
            partitions=list(offsets.keys()),
            offsets={_offset_key(tp.topic(), tp.partition()): oam.offset()
                     for tp, oam in offsets.items()},
            error="" if exception is None else str(exception),
        )

    return on_complete


def ignore_commit_completion(offsets, exception):
    """The callback of a ``commit_nowait(offsets=…)`` the harness sends without
    one: Java has no ``commitAsync(Map)`` overload, only ``commitAsync(Map,
    OffsetCommitCallback)`` (``commit_nowait(offsets=…)`` alone is rejected), and
    a ``null`` callback reports nothing."""


def make_logging_delivery_callback(log, client_id):
    """A real delivery ``Callback`` recording the delivered record's metadata.

    The metadata is never ``None``: on failure it is Java's placeholder
    ``RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)`` beside the error
    (``KafkaProducer.AppendCallbacks.onCompletion``), recorded as it is.
    """

    def delivery_callback(metadata, exception):
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

    return delivery_callback


# ---------------------------------------------------------------------------
# Admin service (admin_service.proto)
#
# Shared by both admin servicers. the admin facade's methods hand back plain dicts of
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
    forwarded verbatim and keeps its own class. Anything else means the
    constructor rejected its arguments without a Kafka error of its own — the
    mock's `num_brokers < 1`, where the FFI returns NULL — and crosses as
    LOCAL_ILLEGAL_ARGUMENT, matching Java's IllegalArgumentException and the
    C++ server. Without this the shared fallback in `_kafka_error_to_proto`
    would report LOCAL_ILLEGAL_STATE and the C and Python backends would
    disagree on a state neither one is wrong about."""
    if isinstance(err, KafkaError):
        return _kafka_error_to_proto(err)
    return pb.KafkaError(
        code=_LOCAL_ILLEGAL_ARGUMENT,
        message=f"python server: {type(err).__name__}: {err}",
    )


def _admin_timeout(request):
    """`optional int32 timeout_ms` -> the `timeout` the admin facade takes.

    A ``timedelta`` of whole milliseconds rather than a float of seconds. The
    original went through ``request.timeout_ms / 1000.0``, and ``the admin facade``'s
    ``_ms`` converts back with an integer truncation, so 1482 of the 200 001
    values in ``0..=200000`` lost 1 ms (1001 -> 1000, 2002 -> 2001, ...) on both
    Python backends while the C++ server passed the value verbatim — a latent
    2-vs-2 split on a field every admin request type already populates.
    ``_ms``/``_close_ms`` divide a ``timedelta`` by ``timedelta(milliseconds=1)``,
    which is exact integer arithmetic, so this round-trips."""
    return _dt.timedelta(milliseconds=request.timeout_ms) if request.HasField("timeout_ms") else None


def _admin_close_timeout(request):
    """`optional int64 timeout_ms` -> the `timeout` the admin facade's close() takes.

    Absent is Java's no-argument `close()`. Shared by both servers' Close
    handlers, which previously each divided by 1000.0 inline and inherited the
    same 1 ms truncation as `_admin_timeout`."""
    return _dt.timedelta(milliseconds=request.timeout_ms) if request.HasField("timeout_ms") else None


def _admin_retry_on_quota(request):
    """`optional bool retry_on_quota_violation` -> Java's default of True when
    absent."""
    return (request.retry_on_quota_violation
            if request.HasField("retry_on_quota_violation") else True)


def _enum(cls, value):
    """The enum member of `cls` for a wire code/name. An unknown code is the
    enum's own UNKNOWN member where it has one (Java's `fromCode`), else a
    malformed request."""
    try:
        return cls(value)
    except ValueError:
        unknown = getattr(cls, "UNKNOWN", None)
        if unknown is None:
            raise AdminRequestError(f"{value!r} is not a {cls.__name__}") from None
        return unknown


def _sorted_tps(tps):
    return sorted(tps, key=lambda tp: (tp.topic(), tp.partition()))


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
    """[proto NewTopic] -> [admin NewTopic].

    -1 is the wire's "absent" for num_partitions / replication_factor, which is
    Java's `Optional.empty()` (None). A non-empty replicas_assignments selects
    Java's NewTopic(name, Map<Integer, List<Integer>>) constructor."""
    topics = []
    for p in protos:
        assignments = {a.partition: list(a.broker_ids) for a in p.replicas_assignments}
        if assignments:
            topic = ka.NewTopic(name=p.name, replicas_assignments=assignments)
        else:
            topic = ka.NewTopic(
                name=p.name,
                num_partitions=None if p.num_partitions < 0 else p.num_partitions,
                replication_factor=None if p.replication_factor < 0 else p.replication_factor)
        if p.configs:
            topic.configs(configs=dict(p.configs))
        topics.append(topic)
    return topics

def _admin_new_partitions(protos):
    """[proto NewPartitions] -> `{topic: admin NewPartitions}`.

    An absent new_assignments selects increaseTo(int); present -- *including
    present-but-empty* -- selects increaseTo(int, List<List<Integer>>), two
    different broker requests, so `HasField` is the test rather than the length
    of the rebuilt list."""
    out = {}
    for p in protos:
        assignments = None
        if p.HasField("new_assignments"):
            assignments = [list(a.broker_ids) for a in p.new_assignments.assignments]
        out[p.topic] = ka.NewPartitions.increase_to(
            total_count=p.total_count, new_assignments=assignments)
    return out

def _admin_records_to_delete(protos):
    """[proto RecordsToDelete] -> `{TopicPartition: RecordsToDelete}`, the key
    shape the admin's delete_records takes and reports back."""
    return {_tp(p.partition): ka.RecordsToDelete.before_offset(offset=p.before_offset)
            for p in protos}

def _admin_config_entry_to_proto(entry):
    return apb.ConfigEntry(
        name=entry.name(),
        value=entry.value(),
        is_default=bool(entry.is_default()),
        is_sensitive=bool(entry.is_sensitive()),
        is_read_only=bool(entry.is_read_only()),
    )

def _admin_metadata_to_proto(metadata):
    """the admin facade TopicMetadataAndConfig -> proto TopicMetadataAndConfig.

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
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(_admin_metadata_to_proto(outcome))
        entries.append(entry)
    return apb.CreateTopicsResponse(entries=entries)


def _admin_partition_info_to_proto(info):
    """admin TopicPartitionInfo -> proto TopicPartitionInfo. `elr` and
    `last_known_elr` are nullable in Java, hence the wrapper message."""
    out = apb.TopicPartitionInfo(
        partition=info.partition(),
        replicas=[_node_to_proto(n) for n in info.replicas()],
        isr=[_node_to_proto(n) for n in info.isr()],
    )
    if info.leader() is not None:
        out.leader.CopyFrom(_node_to_proto(info.leader()))
    if info.elr() is not None:
        out.elr.CopyFrom(apb.NodeList(nodes=[_node_to_proto(n) for n in info.elr()]))
    if info.last_known_elr() is not None:
        out.last_known_elr.CopyFrom(
            apb.NodeList(nodes=[_node_to_proto(n) for n in info.last_known_elr()]))
    return out

def _op_codes(operations):
    """A set of `AclOperation` -> its wire codes, in a stable order."""
    return sorted(int(op) for op in operations)


def _admin_description_to_proto(description):
    """admin TopicDescription -> proto TopicDescription."""
    out = apb.TopicDescription(
        name=description.name(),
        topic_id=str(description.topic_id()),
        is_internal=bool(description.is_internal()),
        partitions=[_admin_partition_info_to_proto(p) for p in description.partitions()],
    )
    # Nullable: absent means the broker did not report the operations, which is
    # not the same as reporting that none are authorized.
    if description.authorized_operations() is not None:
        out.authorized_operations.CopyFrom(
            apb.AclOperationList(operations=_op_codes(description.authorized_operations())))
    return out

def _admin_describe_topics_response(outcomes, key_fn):
    """`{key: TopicDescription | KafkaError}` -> DescribeTopicsResponse."""
    entries = []
    for key, outcome in outcomes.items():
        entry = apb.DescribeTopicsEntry(key=key_fn(key))
        if isinstance(outcome, BaseException):
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
        apb.AdminTopicListing(name=listing.name(), topic_id=str(listing.topic_id()),
                              is_internal=bool(listing.is_internal()))
        for listing in listings.values()
    ])

def _admin_delete_records_response(outcomes):
    """`{TopicPartition: DeletedRecords | error}` -> DeleteRecordsResponse."""
    entries = []
    for tp, outcome in outcomes.items():
        entry = apb.DeleteRecordsEntry(key=_admin_partition_key(tp.topic(), tp.partition()))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(apb.DeletedRecords(low_watermark=outcome.low_watermark()))
        entries.append(entry)
    return apb.DeleteRecordsResponse(entries=entries)

# ---------------------------------------------------------------------------
# Cluster, configs & log dirs (slice G2)
# ---------------------------------------------------------------------------


def _admin_config_resource_to_proto(resource):
    """admin ConfigResource -> proto ConfigResource. `resource_type` is Java's
    `ConfigResource.Type.id()`, the value of the new `ConfigResource.Type`."""
    return apb.ConfigResource(resource_type=resource.type().value, name=resource.name())

def _admin_config_resource_key(resource):
    return apb.ResultKey(config_resource=_admin_config_resource_to_proto(resource))


def _admin_replica_to_proto(replica):
    """admin TopicPartitionReplica -> proto TopicPartitionReplica."""
    return apb.TopicPartitionReplica(topic=replica.topic(), partition=replica.partition(),
                                     broker_id=replica.broker_id())

def _admin_replica_key(replica):
    return apb.ResultKey(replica=_admin_replica_to_proto(replica))


def _admin_config_resources(protos):
    """[proto ConfigResource] -> [admin ConfigResource]."""
    return [_admin_config_resource(p) for p in protos]


def _admin_config_resource(p):
    return ConfigResource(type=_enum(ConfigResource.Type, p.resource_type), name=p.name)


def _admin_config_resource_types(codes):
    """ListConfigResourcesRequest's `repeated int32 resource_types` (each Java's
    `ConfigResource.Type.id()`) -> the `[ConfigResource.Type]` the admin client
    takes; an unknown code is `UNKNOWN`, as `ConfigResource.Type.forId` makes it.
    The raw ints are not enough: `list_config_resources` reads each type's `id()`."""
    return [_enum(ConfigResource.Type, code) for code in codes]

def _admin_alter_configs(protos):
    """[proto ConfigResourceOps] -> `{ConfigResource: [AlterConfigOp]}`.

    An absent op value is Java's null, which is what a DELETE carries."""
    out = {}
    for p in protos:
        resource = _admin_config_resource(p.resource)
        out[resource] = [
            ka.AlterConfigOp(
                config_entry=ka.ConfigEntry(
                    name=op.name, value=op.value if op.HasField("value") else None),
                operation_type=_enum(ka.AlterConfigOp.OpType, op.op_type))
            for op in p.ops
        ]
    return out

def _admin_replicas(protos):
    """[proto TopicPartitionReplica] -> [TopicPartitionReplica]."""
    return [TopicPartitionReplica(topic=p.topic, partition=p.partition, broker_id=p.broker_id)
            for p in protos]

def _admin_replica_log_dir_assignments(protos):
    """[proto ReplicaLogDirAssignment] -> `{TopicPartitionReplica: log_dir}`."""
    return {TopicPartitionReplica(topic=p.replica.topic, partition=p.replica.partition,
                                  broker_id=p.replica.broker_id): p.log_dir
            for p in protos}

def _admin_cluster_description_response(description):
    """the admin facade ClusterDescription -> DescribeClusterResponse. A whole-value
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
            apb.AclOperationList(operations=_op_codes(description.authorized_operations)))
    return apb.DescribeClusterResponse(description=out)


def _admin_full_config_entry_to_proto(entry):
    """admin ConfigEntry -> proto ConfigEntry, all nine fields.

    Unlike `_admin_config_entry_to_proto`, which serves createTopics and carries
    only the five fields that RPC reports, describe_configs populates every
    field, so all nine cross. `source` and `config_type` are Java's enum
    constant names; neither enum has a numeric id, so the name is the contract.
    Synonyms keep Java's precedence order."""
    out = apb.ConfigEntry(
        name=entry.name(),
        value=entry.value(),
        is_default=bool(entry.is_default()),
        is_sensitive=bool(entry.is_sensitive()),
        is_read_only=bool(entry.is_read_only()),
        source=entry.source().name if entry.source() is not None else None,
        config_type=entry.type().name if entry.type() is not None else None,
        documentation=entry.documentation(),
    )
    for synonym in entry.synonyms():
        out.synonyms.append(apb.ConfigSynonym(
            name=synonym.name(),
            value=synonym.value(),
            source=synonym.source().name))
    return out

def _admin_describe_configs_response(outcomes):
    """`{ConfigResource: Config | KafkaError}` -> DescribeConfigsResponse."""
    entries = []
    for resource, outcome in outcomes.items():
        entry = apb.DescribeConfigsEntry(key=_admin_config_resource_key(resource))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(apb.AdminConfig(entries=[
                _admin_full_config_entry_to_proto(e) for e in outcome.entries()]))
        entries.append(entry)
    return apb.DescribeConfigsResponse(entries=entries)


def _admin_list_config_resources_response(resources):
    """`[ConfigResource]` -> ListConfigResourcesResponse. A whole-value response:
    Java's ListConfigResourcesResult holds one future for the whole
    collection."""
    return apb.ListConfigResourcesResponse(
        resources=[_admin_config_resource_to_proto(r) for r in resources])


def _admin_log_dir_description_to_proto(description):
    """admin LogDirDescription -> proto LogDirDescription.

    `error` is the log dir's own error (offline, unreadable): the broker
    answered, so it is *not* the per-broker error, which arrives as the entry's
    error arm instead of a value. `total_bytes` / `usable_bytes` are Java's
    OptionalLong, absent when the broker did not report them; `is_cordoned` is
    the KIP-1066 flag."""
    out = apb.LogDirDescription()
    if description.error() is not None:
        out.error.CopyFrom(_kafka_error_to_proto(description.error()))
    if description.total_bytes() is not None:
        out.total_bytes = description.total_bytes()
    if description.usable_bytes() is not None:
        out.usable_bytes = description.usable_bytes()
    # Java's isCordoned() (KIP-1066).
    out.is_cordoned = bool(description.is_cordoned())
    for tp, info in description.replica_infos().items():
        out.replica_infos.append(apb.ReplicaInfoEntry(
            partition=cpb.TopicPartition(topic=tp.topic(), partition=tp.partition()),
            size=info.size(),
            offset_lag=info.offset_lag(),
            is_future=bool(info.is_future())))
    return out

def _admin_describe_log_dirs_response(outcomes):
    """`{broker: {log_dir: LogDirDescription} | KafkaError}` ->
    DescribeLogDirsResponse. The value is nested: one description per log-dir
    path, each with its own error."""
    entries = []
    for broker, outcome in outcomes.items():
        entry = apb.DescribeLogDirsEntry(key=apb.ResultKey(broker_id=broker))
        if isinstance(outcome, BaseException):
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
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            value = apb.ReplicaLogDirInfo(
                current_replica_offset_lag=outcome.current_replica_offset_lag(),
                future_replica_offset_lag=outcome.future_replica_offset_lag())
            # Both dirs are nullable: no replica hosted here, and no pending
            # move, respectively.
            if outcome.current_replica_log_dir() is not None:
                value.current_replica_log_dir = outcome.current_replica_log_dir()
            if outcome.future_replica_log_dir() is not None:
                value.future_replica_log_dir = outcome.future_replica_log_dir()
            entry.value.CopyFrom(value)
        entries.append(entry)
    return apb.DescribeReplicaLogDirsResponse(entries=entries)


# ---------------------------------------------------------------------------
# Elections, reassignments & offsets (slice G3)
# ---------------------------------------------------------------------------


def _admin_tp_tuple_key(key):
    """A `TopicPartition` result key -> proto ResultKey.

    The G3 void RPCs (electLeaders, alterPartitionReassignments) key their
    results by TopicPartition."""
    return _admin_partition_key(key.topic(), key.partition())

def _admin_optional_partitions(request):
    """`optional TopicPartitionList partitions` -> the partition selection
    the admin facade takes, or None.

    Absent is Java's **null** `Set` / `Optional.empty()`, i.e. every partition in
    the cluster; present-but-empty is an empty selection. `HasField` is what keeps
    them apart — testing the repeated field for emptiness would collapse the two,
    which is the bug class `admin_service.proto`'s ElectLeadersRequest documents.
    the admin facade carries the same distinction as its own `partitions is None`
    column."""
    if not request.HasField("partitions"):
        return None
    return [_tp(p) for p in request.partitions.partitions]


def _admin_reassignments(protos):
    """[proto PartitionReassignmentSpec] ->
    `{TopicPartition: NewPartitionReassignment | None}`.

    An absent `reassignment` is Java's empty `Optional`, which **cancels** that
    partition's ongoing reassignment. It must stay None rather than becoming a
    `NewPartitionReassignment([])`, which Java rejects outright — hence
    `HasField` rather than a check on `target_replicas`."""
    out = {}
    for p in protos:
        out[_tp(p.partition)] = (
            ka.NewPartitionReassignment(target_replicas=list(p.reassignment.target_replicas))
            if p.HasField("reassignment") else None)
    return out


# proto OffsetSpec.Kind -> the the admin facade factory for that Java `OffsetSpec`
# variant. Calling the *named factory* is the point: the wire carries the variant
# name, so each server reaches the six `ListOffsets` sentinels through its own
# binding's table (the admin facade's `OffsetSpec._EARLIEST` etc. here, the C entry
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
    """[proto OffsetSpecEntry] -> `{TopicPartition: OffsetSpec}`.

    KIND_UNSPECIFIED and FOR_TIMESTAMP-without-a-timestamp are protocol errors
    rather than a defaulted variant: a dropped `kind` field must fail the call,
    not silently become `earliest()` and pass. They raise `AdminRequestError`, so
    they cross with the **LOCAL_ILLEGAL_ARGUMENT code** the C++ server stamps for the
    same condition -- not merely at the same level. See `AdminRequestError`."""
    out = {}
    for p in protos:
        key = _tp(p.partition)
        label = (p.partition.topic, p.partition.partition)
        if p.spec.kind == apb.OffsetSpec.FOR_TIMESTAMP:
            if not p.spec.HasField("timestamp"):
                raise AdminRequestError(
                    f"OffsetSpec FOR_TIMESTAMP for {label} carries no timestamp")
            out[key] = ka.OffsetSpec.for_timestamp(timestamp=p.spec.timestamp)
            continue
        factory = _ADMIN_OFFSET_SPEC_FACTORIES.get(p.spec.kind)
        if factory is None:
            raise AdminRequestError(
                f"OffsetSpec for {label} has kind {p.spec.kind}, not a Java OffsetSpec variant")
        out[key] = factory()
    return out


def _admin_list_partition_reassignments_response(reassignments):
    """`{TopicPartition: PartitionReassignment}` ->
    ListPartitionReassignmentsResponse.

    A whole-value response: Java's ListPartitionReassignmentsResult holds one
    future for the entire map, so a failure arrives here as a raise rather than
    per key. Only partitions with an ongoing reassignment appear."""
    out = apb.ListPartitionReassignmentsResponse()
    for tp, reassignment in reassignments.items():
        entry = out.reassignments.add()
        entry.partition.topic = tp.topic()
        entry.partition.partition = tp.partition()
        entry.reassignment.replicas.extend(reassignment.replicas())
        entry.reassignment.adding_replicas.extend(reassignment.adding_replicas())
        entry.reassignment.removing_replicas.extend(reassignment.removing_replicas())
    return out

def _admin_list_offsets_response(outcomes):
    """`{TopicPartition: ListOffsetsResultInfo | error}` -> ListOffsetsResponse."""
    entries = []
    for tp, outcome in outcomes.items():
        entry = apb.ListOffsetsEntry(key=_admin_partition_key(tp.topic(), tp.partition()))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            value = apb.ListOffsetsResultInfo(offset=outcome.offset(),
                                              timestamp=outcome.timestamp())
            # Java's leaderEpoch() is an Optional<Integer>; absent is not epoch 0.
            if outcome.leader_epoch() is not None:
                value.leader_epoch = outcome.leader_epoch()
            entry.value.CopyFrom(value)
        entries.append(entry)
    return apb.ListOffsetsResponse(entries=entries)

# ---------------------------------------------------------------------------
# Groups & offsets (slice G4)
#
# the admin facade's group methods hand back the same resolved shapes as the earlier
# slices, with two exceptions worth naming here:
#
#   - list_groups returns a *pair* of lists,
#     `([listing], [KafkaError])`, because Java's ListGroupsResult splits one
#     future into valid() and an unkeyed errors() collection. The two are
#     independent and generally of different length, so nothing may be zipped or
#     indexed across them.
#   - list_consumer_group_offsets' per-group value is itself a map whose values
#     are nullable: an inner None is Java's null map value, "this group has no
#     committed offset for that partition", which is not offset 0.
# ---------------------------------------------------------------------------


def _admin_group_listing_to_proto(listing):
    """admin GroupListing -> proto GroupListing.

    `group_type` / `group_state` are Java Optionals and stay absent when None —
    "the broker did not report a state" is not the empty string; they cross as
    Java's `toString()` names. `is_simple_consumer_group` is derived in Java
    rather than a field, but the Rust client checks it against the derived
    value, so it crosses.
    """
    out = apb.GroupListing(group_id=listing.group_id(),
                           protocol=listing.protocol(),
                           is_simple_consumer_group=bool(listing.is_simple_consumer_group()))
    if listing.type() is not None:
        out.group_type = str(listing.type())
    if listing.group_state() is not None:
        out.group_state = str(listing.group_state())
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
        # A null entry in errors() is skipped rather than mapped to a synthetic
        # error, which is what the C++ server does (`if (listing_err != nullptr)`
        # shortens `listing_errors`). Mapping it would preserve the length and
        # invent content, so the two servers would disagree on both.
        listing_errors=[_kafka_error_to_proto(e) for e in errors if e is not None])


def _admin_member_assignment_to_proto(assignment):
    """admin MemberAssignment -> proto MemberAssignment."""
    return apb.MemberAssignment(topic_partitions=[
        cpb.TopicPartition(topic=tp.topic(), partition=tp.partition())
        for tp in _sorted_tps(assignment.topic_partitions())
    ])

def _admin_member_description_to_proto(member):
    """admin MemberDescription -> proto MemberDescription.

    `group_instance_id` / `rack_id` / `member_epoch` / `upgraded` are Java
    Optionals and stay absent when None: a static member with an empty instance
    id is not a dynamic member, and an absent member epoch is not epoch 0.
    `target_assignment` is nullable too, and an absent one must not become an
    empty assignment.
    """
    out = apb.MemberDescription(
        consumer_id=member.consumer_id(),
        client_id=member.client_id(),
        host=member.host())
    # Java's `assignment` is never null, so a null one is left *absent* rather
    # than dereferenced or turned into an empty assignment: the wire contract
    # (admin_service.proto, MemberDescription.assignment) makes a missing
    # assignment a protocol error the client reports, and the C++ server leaves it
    # absent for the same input.
    if member.assignment() is not None:
        out.assignment.CopyFrom(_admin_member_assignment_to_proto(member.assignment()))
    if member.group_instance_id() is not None:
        out.group_instance_id = member.group_instance_id()
    if member.rack_id() is not None:
        out.rack_id = member.rack_id()
    if member.target_assignment() is not None:
        out.target_assignment.CopyFrom(
            _admin_member_assignment_to_proto(member.target_assignment()))
    if member.member_epoch() is not None:
        out.member_epoch = member.member_epoch()
    if member.upgraded() is not None:
        out.upgraded = member.upgraded()
    return out

def _admin_consumer_group_description_to_proto(description):
    """admin ConsumerGroupDescription -> proto ConsumerGroupDescription.

    The coordinator is the field this whole milestone was motivated by: it must
    carry the broker's real host and port, not a placeholder. `_node_to_proto`
    returns None for a null coordinator, which stays absent.
    """
    out = apb.ConsumerGroupDescription(
        group_id=description.group_id(),
        is_simple_consumer_group=bool(description.is_simple_consumer_group()),
        members=[_admin_member_description_to_proto(m) for m in description.members()],
        partition_assignor=description.partition_assignor(),
        group_type=str(description.type()),
        group_state=str(description.group_state()))
    node = _node_to_proto(description.coordinator())
    if node is not None:
        out.coordinator.CopyFrom(node)
    # Absent means the broker did not report the operations at all, which is not
    # the same as reporting that none are authorized.
    if description.authorized_operations() is not None:
        out.authorized_operations.CopyFrom(
            apb.AclOperationList(operations=_op_codes(description.authorized_operations())))
    if description.group_epoch() is not None:
        out.group_epoch = description.group_epoch()
    if description.target_assignment_epoch() is not None:
        out.target_assignment_epoch = description.target_assignment_epoch()
    return out

def _admin_classic_group_description_to_proto(description):
    """admin ClassicGroupDescription -> proto ClassicGroupDescription.

    `protocol` (the protocol type) and `protocol_data` (the selected assignment
    strategy) are two different response fields; both cross so a transposition is
    detectable.
    """
    out = apb.ClassicGroupDescription(
        group_id=description.group_id(),
        protocol=description.protocol(),
        protocol_data=description.protocol_data(),
        is_simple_consumer_group=bool(description.is_simple_consumer_group()),
        members=[_admin_member_description_to_proto(m) for m in description.members()],
        state=str(description.state()))
    node = _node_to_proto(description.coordinator())
    if node is not None:
        out.coordinator.CopyFrom(node)
    if description.authorized_operations() is not None:
        out.authorized_operations.CopyFrom(
            apb.AclOperationList(operations=_op_codes(description.authorized_operations())))
    return out

def _admin_describe_consumer_groups_response(outcomes):
    """`{group_id: ConsumerGroupDescription | KafkaError}` ->
    DescribeConsumerGroupsResponse."""
    entries = []
    for group_id, outcome in outcomes.items():
        entry = apb.DescribeConsumerGroupsEntry(key=_admin_name_key(group_id))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        elif outcome is None:
            # Neither a description nor an error: a synthetic per-entry error,
            # keeping the other entries, which is what the C++ server does.
            entry.error.CopyFrom(_admin_synthetic_error(
                f"describeConsumerGroups reported neither a description nor an error for {group_id!r}"))
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
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        elif outcome is None:
            # Neither a description nor an error: a synthetic per-entry error,
            # keeping the other entries, which is what the C++ server does.
            entry.error.CopyFrom(_admin_synthetic_error(
                f"describeClassicGroups reported neither a description nor an error for {group_id!r}"))
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

    A repeated `group_id` is **rejected**, not silently de-duplicated. Building a
    dict keyed by the wire's `group_id` would let a second entry replace the
    first, which is exactly what the C entry point refuses
    (`src/ffi/admin.rs`: "group id `{id}` appears more than once at index {i}",
    documented there because Java takes a `Map` "where the second entry would
    silently have replaced the first"). This is the only multi-key admin request
    in the harness where a Python dict is keyed by a wire field rather than
    forwarding a `repeated` list, so it is the only place the divergence can
    arise — and the field is unreachable through `AdminBackend`'s own
    `&HashMap<String, _>` signature, so this closes it before it becomes
    reachable rather than after.
    """
    out = {}
    for p in protos:
        if p.group_id in out:
            raise AdminRequestError(
                f"group id `{p.group_id}` appears more than once in "
                f"listConsumerGroupOffsets")
        if p.HasField("topic_partitions"):
            out[p.group_id] = ka.ListConsumerGroupOffsetsSpec(
                topic_partitions=[_tp(tp) for tp in p.topic_partitions.partitions])
        else:
            out[p.group_id] = ka.ListConsumerGroupOffsetsSpec()
    return out


def _admin_list_consumer_group_offsets_response(outcomes):
    """`{group_id: {TopicPartition: OffsetAndMetadata | None} | error}`
    -> ListConsumerGroupOffsetsResponse.

    Two levels, like describeLogDirs: the per-group future carries a whole map.
    An inner None is Java's null map value and stays absent on the wire — it is
    what proves deleteConsumerGroupOffsets removed the commit rather than zeroing
    it.
    """
    entries = []
    for group_id, outcome in outcomes.items():
        entry = apb.ListConsumerGroupOffsetsEntry(key=_admin_name_key(group_id))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        elif outcome is None:
            # Neither offsets nor an error: a synthetic per-entry error rather
            # than failing the whole call, matching the C++ server. Note an empty
            # *map* is a legitimate value (the group committed nothing) and is not
            # this case.
            entry.error.CopyFrom(_admin_synthetic_error(
                f"listConsumerGroupOffsets reported neither offsets nor an error for {group_id!r}"))
        else:
            value = apb.GroupOffsets()
            for tp, offset in outcome.items():
                pair = value.offsets.add()
                pair.partition.topic = tp.topic()
                pair.partition.partition = tp.partition()
                if offset is not None:
                    pair.offset.CopyFrom(_oam_to_proto(offset))
            entry.value.CopyFrom(value)
        entries.append(entry)
    return apb.ListConsumerGroupOffsetsResponse(entries=entries)


def _admin_group_offset_commits(protos):
    """[proto GroupOffsetCommit] -> `{TopicPartition: OffsetAndMetadata}`.

    `leader_epoch` absent is Java's empty Optional, which is None here and must
    not become 0.
    """
    return {
        _tp(p.partition): OffsetAndMetadata(
            offset=p.offset.offset,
            leader_epoch=p.offset.leader_epoch if p.offset.HasField("leader_epoch") else None,
            metadata=p.offset.metadata)
        for p in protos
    }

def _admin_members_to_remove(request):
    """`optional MemberToRemoveList members` -> the selection the admin facade takes.

    Absent is Java's no-argument RemoveMembersFromConsumerGroupOptions(), i.e.
    removeAll, which the admin facade spells `members=None`. Present-but-empty is the
    Collection constructor with an empty collection, which Java rejects with
    IllegalArgumentException — so it must stay an empty *list* and not collapse to
    None. `HasField` is the discriminant, never emptiness.
    """
    if not request.HasField("members"):
        return None
    return [ka.MemberToRemove(group_instance_id=m.group_instance_id)
            for m in request.members.members]


# ---------------------------------------------------------------------------
# ACLs, quotas, SCRAM, delegation tokens & features (slice G5)
#
# the admin facade's methods hand back the resolved shapes the earlier slices
# established, with four that are worth naming here:
#
#   - describe_acls / describe_client_quotas / create_delegation_token /
#     describe_delegation_token / describe_features hold ONE future for the whole
#     call, so a failure *raises* and becomes the response's top-level error.
#     There is no per-key slot to put it in.
#   - delete_acls' per-filter value is a *list* of DeletedAcl, each of which
#     carries its own exception. The filter's own future can also have failed, so
#     the value is `KafkaError | [DeletedAcl]` and both levels cross.
#   - describe_user_scram_credentials is per-user, and a user the broker reports
#     as having *no* credential is a success with an empty credential_infos —
#     never an error. That is Java's `all()` treating RESOURCE_NOT_FOUND as a
#     success; the facade composes the views as the C and native backends do
#     (`all()`, then `users()` + `description(user)` when it fails), so once
#     `all()` has failed a requested RESOURCE_NOT_FOUND user is left out.
#   - update_features raises for an empty map against a real client (Java's
#     IllegalArgumentException) while a mock yields an empty result. Both are
#     faithful; the raise becomes the top-level error.
#
# Nulls that must survive as None rather than collapsing to "" / 0 / an empty
# list — the recurring defect class of this milestone:
#
#   - AclBindingFilter's resource_name / principal / host: None is Java's
#     match-any, "" matches the literally-empty name.
#   - ClientQuotaEntity's entity_name: None is the built-in default entity.
#   - ClientQuotaOp's value: None **removes** the quota; 0.0 sets it to zero.
#   - UserScramCredentialUpsertion's salt: None selects Java's salt-generating
#     three-argument constructor; b"" selects the four-argument one with a
#     zero-length salt, which the C layer distinguishes with `has_salts`.
#   - describe_delegation_token's owners and describe_features' node_id: None is
#     Java's unset filter / empty OptionalInt.
# ---------------------------------------------------------------------------


def _admin_acl_binding_key(binding):
    """the admin facade AclBinding -> ResultKey.acl_binding."""
    return apb.ResultKey(acl_binding=_admin_acl_binding_to_proto(binding))


def _admin_acl_filter_key(acl_filter):
    """the admin facade AclBindingFilter -> ResultKey.acl_binding_filter."""
    return apb.ResultKey(acl_binding_filter=_admin_acl_filter_to_proto(acl_filter))


def _admin_quota_entity_key(entity):
    """the admin facade ClientQuotaEntity -> ResultKey.client_quota_entity."""
    return apb.ResultKey(client_quota_entity=_admin_quota_entity_to_proto(entity))


def _admin_acl_bindings(protos):
    """[proto AclBinding] -> [admin AclBinding]."""
    return [AclBinding(
        pattern=ResourcePattern(resource_type=_enum(ResourceType, p.resource_type),
                                name=p.resource_name,
                                pattern_type=_enum(PatternType, p.pattern_type)),
        entry=AccessControlEntry(principal=p.principal, host=p.host,
                                 operation=_enum(AclOperation, p.operation),
                                 permission_type=_enum(AclPermissionType, p.permission_type)))
        for p in protos]

def _admin_acl_filter(p):
    """proto AclBindingFilter -> admin AclBindingFilter.

    The three nullable strings are read through `HasField`, never through an
    emptiness test: absent is Java's match-any and "" is a literal name, and
    the constructors preserve whichever they are given.
    """
    return AclBindingFilter(
        pattern_filter=ResourcePatternFilter(
            resource_type=_enum(ResourceType, p.resource_type),
            name=p.resource_name if p.HasField("resource_name") else None,
            pattern_type=_enum(PatternType, p.pattern_type)),
        entry_filter=AccessControlEntryFilter(
            principal=p.principal if p.HasField("principal") else None,
            host=p.host if p.HasField("host") else None,
            operation=_enum(AclOperation, p.operation),
            permission_type=_enum(AclPermissionType, p.permission_type)))

def _admin_acl_filters(protos):
    """[proto AclBindingFilter] -> [the admin facade AclBindingFilter]."""
    return [_admin_acl_filter(p) for p in protos]


def _admin_acl_binding_to_proto(binding):
    """admin AclBinding -> proto AclBinding. Every field is required."""
    pattern, entry = binding.pattern(), binding.entry()
    return apb.AclBinding(resource_type=int(pattern.resource_type()),
                          resource_name=pattern.name(),
                          pattern_type=int(pattern.pattern_type()),
                          principal=entry.principal(),
                          host=entry.host(),
                          operation=int(entry.operation()),
                          permission_type=int(entry.permission_type()))

def _admin_acl_filter_to_proto(acl_filter):
    """admin AclBindingFilter -> proto AclBindingFilter, keeping each of the
    three nullable strings absent when it is None."""
    pattern, entry = acl_filter.pattern_filter(), acl_filter.entry_filter()
    out = apb.AclBindingFilter(resource_type=int(pattern.resource_type()),
                               pattern_type=int(pattern.pattern_type()),
                               operation=int(entry.operation()),
                               permission_type=int(entry.permission_type()))
    if pattern.name() is not None:
        out.resource_name = pattern.name()
    if entry.principal() is not None:
        out.principal = entry.principal()
    if entry.host() is not None:
        out.host = entry.host()
    return out

def _admin_describe_acls_response(bindings):
    """`[AclBinding]` -> DescribeAclsResponse. A whole-value response: one future
    for the whole call, so an empty list is a successful "nothing matched"."""
    return apb.DescribeAclsResponse(
        acls=[_admin_acl_binding_to_proto(b) for b in bindings])


def _admin_create_acls_response(outcomes):
    """`{AclBinding: None | KafkaError}` -> VoidKeyedResponse keyed by binding."""
    return _admin_void_response(outcomes, _admin_acl_binding_key)


def _admin_delete_acls_response(outcomes):
    """`{AclBindingFilter: KafkaError | [DeletedAcl]}` -> DeleteAclsResponse.

    Two levels, and the inner one is the *value-carries-its-own-error* case: the
    filter's future can succeed (it matched) while an individual matched ACL
    failed to delete. Both halves of a DeletedAcl are written independently
    rather than as a oneof, so a backend that set neither or both stays visible.
    """
    entries = []
    for acl_filter, outcome in outcomes.items():
        entry = apb.DeleteAclsEntry(key=_admin_acl_filter_key(acl_filter))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            value = apb.FilterResults()
            for deleted in outcome:
                pair = value.values.add()
                if deleted.binding() is not None:
                    pair.binding.CopyFrom(_admin_acl_binding_to_proto(deleted.binding()))
                if deleted.exception() is not None:
                    pair.exception.CopyFrom(_kafka_error_to_proto(deleted.exception()))
            entry.value.CopyFrom(value)
        entries.append(entry)
    return apb.DeleteAclsResponse(entries=entries)


def _admin_quota_entity_to_proto(entity):
    """admin ClientQuotaEntity -> proto ClientQuotaEntity, keeping a None
    entity name absent — that is the built-in default entity of its type, not the
    entity named ""."""
    out = apb.ClientQuotaEntity()
    for entity_type, entity_name in entity.entries().items():
        pair = out.entries.add()
        pair.entity_type = entity_type
        if entity_name is not None:
            pair.entity_name = entity_name
    return out

def _admin_quota_entity(p):
    """proto ClientQuotaEntity -> admin ClientQuotaEntity."""
    return ClientQuotaEntity(entries={
        e.entity_type: (e.entity_name if e.HasField("entity_name") else None)
        for e in p.entries
    })

def _admin_quota_filter(request):
    """DescribeClientQuotasRequest -> the admin facade ClientQuotaFilter.

    The named ClientQuotaMatchKind is resolved through the admin facade's *public
    factories* rather than by writing the MATCH_TYPE_* table here, which is the
    whole point of naming the kind on the wire: this server and the C++ server
    then own independent copies of the table, so a disagreement is a finding
    instead of a shared assumption. MATCH_KIND_UNSPECIFIED is never sent and is
    rejected rather than defaulted — a defaulted EXACT would turn a dropped field
    into a silently valid filter.
    """
    components = []
    for c in request.components:
        if c.match_kind == apb.MATCH_KIND_EXACT:
            if not c.HasField("match_name"):
                raise AdminRequestError(
                    "ClientQuotaFilterComponent with MATCH_KIND_EXACT carries no match_name")
            components.append(ClientQuotaFilterComponent.of_entity(
                entity_type=c.entity_type, entity_name=c.match_name))
        elif c.match_kind == apb.MATCH_KIND_DEFAULT:
            components.append(ClientQuotaFilterComponent.of_default_entity(
                entity_type=c.entity_type))
        elif c.match_kind == apb.MATCH_KIND_ANY:
            components.append(ClientQuotaFilterComponent.of_entity_type(
                entity_type=c.entity_type))
        else:
            raise AdminRequestError(
                f"ClientQuotaFilterComponent has no match_kind (got {c.match_kind})")
    return (ClientQuotaFilter.contains_only(components=components) if request.strict
            else ClientQuotaFilter.contains(components=components))


def _admin_quota_alterations(protos):
    """[proto ClientQuotaAlteration] -> [the admin facade ClientQuotaAlteration].

    An op with no `value` is Java's null Double, which **removes** the quota.
    `HasField` is the discriminant; reading `p.value` unconditionally would turn
    every removal into a set-to-zero, which is the mutation that once passed the
    whole suite.
    """
    out = []
    for p in protos:
        ops = [ClientQuotaAlteration.Op(key=op.key,
                                        value=op.value if op.HasField("value") else None)
               for op in p.ops]
        out.append(ClientQuotaAlteration(entity=_admin_quota_entity(p.entity), ops=ops))
    return out


def _admin_describe_client_quotas_response(entities):
    """`{ClientQuotaEntity: {key: float}}` -> DescribeClientQuotasResponse.

    A whole-value response. A *removed* quota is absent from the inner map rather
    than reported as zero, which is the only observable separating a removal from
    a zero-valued set.
    """
    out = apb.DescribeClientQuotasResponse()
    for entity, values in entities.items():
        reported = out.entities.add()
        reported.entity.CopyFrom(_admin_quota_entity_to_proto(entity))
        for key, value in values.items():
            pair = reported.values.add()
            pair.key = key
            pair.value = value
    return out


def _admin_alter_client_quotas_response(outcomes):
    """`{ClientQuotaEntity: None | KafkaError}` -> VoidKeyedResponse."""
    return _admin_void_response(outcomes, _admin_quota_entity_key)


def _admin_scram_alterations(protos):
    """[proto UserScramCredentialAlteration] -> the the admin facade upsertion/deletion
    objects.

    `is_deletion` is the discriminant and cannot be inferred: both forms carry a
    user and a mechanism, so "password is absent" would conflate a deletion with
    a malformed upsertion. A None salt selects the admin facade's (and Java's)
    salt-generating constructor, while a present-but-empty one selects the
    salt-supplying constructor with a zero-length salt -- so `HasField("salt")`
    is the test, not truthiness.
    """
    out = []
    for p in protos:
        if p.is_deletion:
            out.append(ka.UserScramCredentialDeletion(
                user=p.user, mechanism=ka.ScramMechanism.from_type(type=p.mechanism)))
        else:
            info = ka.ScramCredentialInfo(
                mechanism=ka.ScramMechanism.from_type(type=p.mechanism), iterations=p.iterations)
            out.append(ka.UserScramCredentialUpsertion(
                user=p.user, credential_info=info,
                password=p.password if p.HasField("password") else b"",
                salt=p.salt if p.HasField("salt") else None))
    return out


def _admin_describe_user_scram_credentials_response(outcomes):
    """`{user: UserScramCredentialsDescription | KafkaError}` ->
    DescribeUserScramCredentialsResponse.

    A user with an *empty* credential_infos is a successful description ("the
    broker reports no credential"), not an error — Java's `all()` treats
    RESOURCE_NOT_FOUND that way, and the facade answers from `all()` when it
    succeeds (`g_describe_user_scram_credentials`). The broker never
    returns the salted password or the salt, so neither has a field to carry.
    """
    entries = []
    for user, outcome in outcomes.items():
        entry = apb.DescribeUserScramCredentialsEntry(key=_admin_name_key(user))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        else:
            entry.value.CopyFrom(apb.UserScramCredentialsDescription(
                name=outcome.name(),
                credential_infos=[
                    apb.ScramCredentialInfo(mechanism=info.mechanism().type(),
                                            iterations=info.iterations())
                    for info in outcome.credential_infos()
                ]))
        entries.append(entry)
    return apb.DescribeUserScramCredentialsResponse(entries=entries)


def _admin_principals(protos):
    """[proto KafkaPrincipal] -> [admin KafkaPrincipal]."""
    return [KafkaPrincipal(principal_type=p.principal_type, name=p.name,
                           token_authenticated=p.token_authenticated) for p in protos]

def _admin_principal_to_proto(principal):
    """admin KafkaPrincipal -> proto KafkaPrincipal.

    `token_authenticated` crosses even though the *request* messages do not carry
    it (Java's CreateDelegationTokenRequest has only the type and the name): the
    broker and the mock report it back, and the Rust client asserts it explicitly
    because Java's KafkaPrincipal.equals ignores it.
    """
    return apb.KafkaPrincipal(principal_type=principal.principal_type(),
                              name=principal.name(),
                              token_authenticated=bool(principal.token_authenticated()))

def _admin_delegation_token_to_proto(token):
    """admin DelegationToken -> proto DelegationToken.

    `hmac_as_base64` is derived from `hmac`, and the Rust client re-derives it and
    fails the call on a mismatch, so a transposition or a dropped field here is
    caught rather than absorbed.
    """
    info = token.token_info()
    if info is None:
        raise _AdminEncodeError("DelegationToken carries no token_info")
    if info.owner() is None or info.token_requester() is None:
        raise _AdminEncodeError("TokenInformation carries no owner or no token_requester")
    return apb.DelegationToken(
        token_information=apb.TokenInformation(
            token_id=info.token_id(),
            owner=_admin_principal_to_proto(info.owner()),
            token_requester=_admin_principal_to_proto(info.token_requester()),
            renewers=[_admin_principal_to_proto(r) for r in info.renewers()],
            issue_timestamp=info.issue_timestamp(),
            max_timestamp=info.max_timestamp(),
            expiry_timestamp=info.expiry_timestamp()),
        hmac=token.hmac(),
        hmac_as_base64=token.hmac_as_base64_string())

def _admin_create_delegation_token_response(token):
    """the admin facade DelegationToken -> CreateDelegationTokenResponse.

    A None token from a successful call is a binding bug rather than a Kafka
    outcome, and it has to become a *typed* error rather than an AttributeError:
    the C++ server stamps a synthetic LOCAL_ILLEGAL_STATE for the same state, and
    without this the two would disagree on whether the response even carries an
    envelope.
    """
    if token is None:
        raise _AdminEncodeError("create_delegation_token returned no token and no error")
    return apb.CreateDelegationTokenResponse(token=_admin_delegation_token_to_proto(token))


def _admin_describe_delegation_token_response(tokens):
    """`[DelegationToken]` -> DescribeDelegationTokenResponse."""
    return apb.DescribeDelegationTokenResponse(
        tokens=[_admin_delegation_token_to_proto(t) for t in tokens])


def _admin_token_owners(request):
    """`optional KafkaPrincipalList owners` -> the filter the admin facade takes.

    Absent is Java's *unset* filter (describe every token the caller may see) and
    present-but-empty is an explicit empty filter; `HasField` is what keeps them
    apart, so an empty list must stay a list and never collapse to None.
    """
    if not request.HasField("owners"):
        return None
    return _admin_principals(request.owners.principals)


def _admin_feature_metadata_to_proto(metadata):
    """admin FeatureMetadata -> proto FeatureMetadata.

    The two maps are independent and need not agree in size or keys. An absent
    epoch is Java's empty Optional<Long> and must not become 0.
    """
    out = apb.FeatureMetadata()
    for feature, rng in metadata.finalized_features().items():
        out.finalized_features[feature].min_version_level = rng.min_version_level()
        out.finalized_features[feature].max_version_level = rng.max_version_level()
    for feature, rng in metadata.supported_features().items():
        out.supported_features[feature].min_version = rng.min_version()
        out.supported_features[feature].max_version = rng.max_version()
    if metadata.finalized_features_epoch() is not None:
        out.finalized_features_epoch = metadata.finalized_features_epoch()
    return out

def _admin_describe_features_response(metadata):
    """the admin facade FeatureMetadata -> DescribeFeaturesResponse. See
    [_admin_create_delegation_token_response] for why the None branch is explicit.
    """
    if metadata is None:
        raise _AdminEncodeError("describe_features returned no metadata and no error")
    return apb.DescribeFeaturesResponse(metadata=_admin_feature_metadata_to_proto(metadata))


def _admin_feature_updates(request):
    """`map<string, FeatureUpdate>` -> the `{feature: FeatureUpdate}` the admin facade
    takes. An empty map is forwarded as such: a real client raises
    (IllegalArgumentException) and a mock yields an empty result, and both are
    faithful.
    """
    return {feature: ka.FeatureUpdate(
                max_version_level=update.max_version_level,
                upgrade_type=_enum(ka.FeatureUpdate.UpgradeType, update.upgrade_type))
            for feature, update in request.feature_updates.items()}


# ---------------------------------------------------------------------------
# G6 — producers & transactions
#
# `TransactionState` crosses as Java's `TransactionState.toString()` name (the
# same string the admin facade already stores on `TransactionListing.state` and
# `TransactionDescription.state`), never as a code: the enum has no numeric id in
# Java. The Rust client rejects a name that parses to `Unknown` without spelling
# "Unknown", so a garbled name cannot be absorbed.
# ---------------------------------------------------------------------------


def _admin_broker_id_key(broker_id):
    return apb.ResultKey(broker_id=int(broker_id))


def _admin_producer_state_to_proto(state):
    """admin ProducerState -> proto ProducerState.

    The two Optionals stay absent when None: every int, 0 and -1 included, is a
    legal `coordinator_epoch` / `current_transaction_start_offset`, so absence
    cannot be encoded as a value.
    """
    out = apb.ProducerState(producer_id=state.producer_id(),
                            producer_epoch=state.producer_epoch(),
                            last_sequence=state.last_sequence(),
                            last_timestamp=state.last_timestamp())
    if state.coordinator_epoch() is not None:
        out.coordinator_epoch = state.coordinator_epoch()
    if state.current_transaction_start_offset() is not None:
        out.current_transaction_start_offset = state.current_transaction_start_offset()
    return out

def _admin_describe_producers_response(outcomes):
    """`{(topic, partition): PartitionProducerState | KafkaError}` ->
    DescribeProducersResponse.

    An empty `active_producers` is a successful description of a partition with
    no producer state, not an error and not an absent value.
    """
    entries = []
    for tp, outcome in outcomes.items():
        topic, partition = tp.topic(), tp.partition()
        entry = apb.DescribeProducersEntry(key=_admin_partition_key(topic, partition))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        elif outcome is None:
            entry.error.CopyFrom(_admin_synthetic_error(
                f"describeProducers reported neither state nor error for {topic}-{partition}"))
        else:
            entry.value.CopyFrom(apb.PartitionProducerState(
                active_producers=[_admin_producer_state_to_proto(p) for p in outcome.active_producers()]))
        entries.append(entry)
    return apb.DescribeProducersResponse(entries=entries)


def _admin_describe_transactions_response(outcomes):
    """`{transactional_id: TransactionDescription | KafkaError}` ->
    DescribeTransactionsResponse.

    `transaction_start_time_ms` stays absent when None (Java's empty
    `OptionalLong` for a transaction that is not in progress), and
    `topic_partitions` is a plain repeated field because Java has no null form
    for it.
    """
    entries = []
    for tid, outcome in outcomes.items():
        entry = apb.DescribeTransactionsEntry(key=_admin_name_key(tid))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        elif outcome is None:
            entry.error.CopyFrom(_admin_synthetic_error(
                f"describeTransactions reported neither description nor error for {tid!r}"))
        else:
            value = apb.TransactionDescription(
                coordinator_id=outcome.coordinator_id(),
                state=str(outcome.state()),
                producer_id=outcome.producer_id(),
                producer_epoch=outcome.producer_epoch(),
                transaction_timeout_ms=outcome.transaction_timeout_ms(),
                topic_partitions=[cpb.TopicPartition(topic=tp.topic(), partition=tp.partition())
                                  for tp in _sorted_tps(outcome.topic_partitions())])
            if outcome.transaction_start_time_ms() is not None:
                value.transaction_start_time_ms = outcome.transaction_start_time_ms()
            entry.value.CopyFrom(value)
        entries.append(entry)
    return apb.DescribeTransactionsResponse(entries=entries)


def _admin_transaction_states(request):
    """The `states` filter as a list of names, or None when empty.

    Java's default is an empty set meaning "every state", and the admin facade's
    `_list_transactions_filters` treats None and [] identically for exactly that
    reason, so collapsing them here loses nothing.
    """
    return list(request.states) or None


def _admin_transaction_id_pattern(request):
    """The pattern filter, preserving absent-vs-empty.

    `HasField` is what separates Java's null pattern from an explicitly empty
    one. They are not observably different — Java's own
    `ListTransactionsHandler.buildBatchedRequest` drops an empty pattern before
    it reaches the wire — but the distinction is carried rather than re-derived
    from emptiness so no server has to guess.
    """
    return request.transactional_id_pattern if request.HasField("transactional_id_pattern") else None


def _admin_list_transactions_response(outcomes):
    """`{broker_id: [TransactionListing] | KafkaError}` -> ListTransactionsResponse.

    Keyed by broker because `byBrokerId()` is the only one of Java's three views
    that keeps a per-broker error, so a listing that succeeded on one broker and
    failed on another reports both.
    """
    entries = []
    for broker_id, outcome in outcomes.items():
        entry = apb.ListTransactionsEntry(key=_admin_broker_id_key(broker_id))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        elif outcome is None:
            entry.error.CopyFrom(_admin_synthetic_error(
                f"listTransactions reported neither listings nor error for broker {broker_id}"))
        else:
            entry.value.CopyFrom(apb.TransactionListingList(listings=[
                apb.TransactionListing(transactional_id=listing.transactional_id(),
                                       producer_id=listing.producer_id(),
                                       state=str(listing.state()))
                for listing in outcome]))
        entries.append(entry)
    return apb.ListTransactionsResponse(entries=entries)


def _admin_fence_producers_response(outcomes):
    """`{transactional_id: ProducerIdAndEpoch | KafkaError}` ->
    FenceProducersResponse.

    `(-1, -1)` is Java's `ProducerIdAndEpoch.NONE` sentinel, a legal value rather
    than an absence, so neither field is optional.
    """
    entries = []
    for tid, outcome in outcomes.items():
        entry = apb.FenceProducersEntry(key=_admin_name_key(tid))
        if isinstance(outcome, BaseException):
            entry.error.CopyFrom(_kafka_error_to_proto(outcome))
        elif outcome is None:
            entry.error.CopyFrom(_admin_synthetic_error(
                f"fenceProducers reported neither producer id nor error for {tid!r}"))
        else:
            entry.value.CopyFrom(apb.ProducerIdAndEpoch(producer_id=outcome.producer_id,
                                                        epoch=outcome.epoch))
        entries.append(entry)
    return apb.FenceProducersResponse(entries=entries)


def _admin_abort_transaction_spec(request):
    """AbortTransactionRequest -> the the admin facade AbortTransactionSpec.

    The partition is a required submessage; Java's `AbortTransactionSpec` holds a
    `TopicPartition`, which has no null-topic form, so an absent one is a
    malformed request rather than a defaultable field.
    """
    if not request.HasField("topic_partition"):
        raise AdminRequestError("abort_transaction requires a topic_partition")
    tp = request.topic_partition
    return ka.AbortTransactionSpec(
        topic_partition=_tp(tp), producer_id=request.producer_id,
        producer_epoch=request.producer_epoch, coordinator_epoch=request.coordinator_epoch)


# ---------------------------------------------------------------------------
# The admin client as the gRPC servers drive it
#
# The ``confluent_kafka.admin`` package is Java-faithful: each RPC returns a
# ``*Result`` that holds futures (``concurrent.futures.Future`` on ``Admin``,
# ``asyncio.Future`` on ``AsyncAdmin``). The servers answer with the per-key
# envelope of admin_service.proto, which wants every future *resolved*:
# ``{key: value | error}``. This facade does that resolving once, for both
# servers.
#
# Each RPC is written once, as a generator that ``yield``s each future and gets
# the value back (or has the error thrown in). ``_drive_sync`` resolves a
# yielded future with ``get()``, ``_drive_async`` with ``await get()``; the
# ``ExecutionError`` a failed ``get()`` raises, as Java's ``Future.get()`` does,
# is unwrapped to its ``__cause__`` before it is thrown into the RPC, so the RPC
# and the proto error shapes see the error the broker or the client raised. So the sync and the async server share every
# line of the RPC logic.
#
# Outcome shapes (what the ``_admin_*_response`` builders above read):
#   - per-key RPCs      -> ``{key: value | exception}``; void ones ``{key: None | exception}``
#   - whole-value RPCs  -> the value; a failed future raises (whole-call error)
# Keys are the new package's value types (``TopicPartition``,
# ``ConfigResource``, ...), except topic ids, which are their string form.
# ---------------------------------------------------------------------------

_NO_TIMEOUT = object()


def _timeout_ms(timeout):
    """`timedelta | float seconds | None` -> whole milliseconds, exactly
    (see `_admin_timeout`)."""
    if timeout is None:
        return None
    if isinstance(timeout, _dt.timedelta):
        return timeout // _dt.timedelta(milliseconds=1)
    return int(timeout * 1000)


def _opts(cls, timeout, **kwargs):
    """`cls(**kwargs)` with the request timeout; a ``None`` kwarg is left out
    (Java's option unset)."""
    options = cls(**{k: v for k, v in kwargs.items() if v is not None})
    ms = _timeout_ms(timeout)
    if ms is not None:
        options.timeout_ms(timeout_ms=ms)
    return options


def _collect(futures):
    """Resolve `{key: future}` into `{key: value | exception}`."""
    out = {}
    for key, future in futures.items():
        try:
            out[key] = yield future
        except Exception as e:  # noqa: BLE001  (a per-key failure is data)
            out[key] = e
    return out


def _per_key(res, keys, future_of, key_of=lambda k: k):
    """Resolve one future per key (`partitionResult` / `memberResult`), each
    failing with its own error. With no key there is only `all()`, whose failure
    is the whole-call error and whose success is an empty result."""
    if not keys:
        yield res.all()
        return {}
    out = {}
    for key in keys:
        try:
            yield future_of(key)
            out[key_of(key)] = None
        except Exception as e:  # noqa: BLE001
            out[key_of(key)] = e
    return out


def _id_str(key):
    return str(key)


class _AdminOps:
    """The RPCs, as generators over a raw ``Admin`` / ``AsyncAdmin``. ``mock`` is
    true when ``admin`` is a ``MockAdminClient`` / ``AsyncMockAdminClient``: the
    one RPC whose request the server must reshape for the mock reads it."""

    def __init__(self, admin, mock=False):
        self._a = admin
        self._mock = mock

    # -- Topics & partitions ----------------------------------------------

    def g_create_topics(self, new_topics, timeout=None, validate_only=False,
                        retry_on_quota_violation=True):
        res = yield self._a.create_topics(
            new_topics=new_topics,
            options=_opts(ka.CreateTopicsOptions, timeout, validate_only=validate_only,
                          retry_on_quota_violation=retry_on_quota_violation))
        out = {}
        for name, future in res.values().items():
            try:
                yield future
            except Exception as e:  # noqa: BLE001
                out[name] = e
                continue
            # The topic was created; the accessors rethrow when the broker did
            # not return its metadata (envelope exception 3): a value-level error.
            try:
                topic_id = yield res.topic_id(topic=name)
                num_partitions = yield res.num_partitions(topic=name)
                replication_factor = yield res.replication_factor(topic=name)
                config = yield res.config(topic=name)
                out[name] = _types.SimpleNamespace(
                    error=None, topic_id=str(topic_id), num_partitions=num_partitions,
                    replication_factor=replication_factor, configs=config.entries())
            except Exception as e:  # noqa: BLE001
                out[name] = _types.SimpleNamespace(error=e)
        return out

    def g_delete_topics(self, names, timeout=None, retry_on_quota_violation=True):
        res = yield self._a.delete_topics(
            topics=names,
            options=_opts(ka.DeleteTopicsOptions, timeout,
                          retry_on_quota_violation=retry_on_quota_violation))
        return (yield from _collect(res.topic_name_values()))

    def g_delete_topics_by_ids(self, topic_ids, timeout=None, retry_on_quota_violation=True):
        res = yield self._a.delete_topics(
            topics=TopicCollection.of_topic_ids(
                topics=[Uuid.from_string(str=s) for s in topic_ids]),
            options=_opts(ka.DeleteTopicsOptions, timeout,
                          retry_on_quota_violation=retry_on_quota_violation))
        out = yield from _collect(res.topic_id_values())
        return {_id_str(k): v for k, v in out.items()}

    def g_list_topics(self, timeout=None, list_internal=False):
        res = yield self._a.list_topics(options=_opts(ka.ListTopicsOptions, timeout,
                                                list_internal=list_internal))
        return (yield res.names_to_listings())

    def g_describe_topics(self, topics, timeout=None, include_authorized_operations=False,
                          partition_size_limit=None):
        res = yield self._a.describe_topics(
            topic_names=topics,
            options=_opts(ka.DescribeTopicsOptions, timeout,
                          include_authorized_operations=include_authorized_operations,
                          partition_size_limit_per_response=partition_size_limit))
        return (yield from _collect(res.topic_name_values()))

    def g_describe_topics_by_ids(self, topic_ids, timeout=None,
                                 include_authorized_operations=False, partition_size_limit=None):
        res = yield self._a.describe_topics(
            topics=TopicCollection.of_topic_ids(
                topics=[Uuid.from_string(str=s) for s in topic_ids]),
            options=_opts(ka.DescribeTopicsOptions, timeout,
                          include_authorized_operations=include_authorized_operations,
                          partition_size_limit_per_response=partition_size_limit))
        out = yield from _collect(res.topic_id_values())
        return {_id_str(k): v for k, v in out.items()}

    def g_create_partitions(self, new_partitions, timeout=None, validate_only=False,
                            retry_on_quota_violation=True):
        res = yield self._a.create_partitions(
            new_partitions=new_partitions,
            options=_opts(ka.CreatePartitionsOptions, timeout, validate_only=validate_only,
                          retry_on_quota_violation=retry_on_quota_violation))
        return (yield from _collect(res.values()))

    def g_delete_records(self, records_to_delete, timeout=None):
        res = yield self._a.delete_records(
            records_to_delete=records_to_delete,
            options=_opts(ka.DeleteRecordsOptions, timeout))
        return (yield from _collect(res.low_watermarks()))

    # -- Cluster, configs & log dirs ----------------------------------------

    def g_describe_cluster(self, timeout=None, include_authorized_operations=False,
                           include_fenced_brokers=False):
        res = yield self._a.describe_cluster(
            options=_opts(ka.DescribeClusterOptions, timeout,
                          include_authorized_operations=include_authorized_operations,
                          include_fenced_brokers=include_fenced_brokers))
        cluster_id = yield res.cluster_id()
        nodes = yield res.nodes()
        controller = yield res.controller()
        operations = yield res.authorized_operations()
        # Java's controller() is null when there is no controller; the
        # no-controller node is the sentinel with a negative id.
        if controller is not None and controller.id() < 0:
            controller = None
        return _types.SimpleNamespace(cluster_id=cluster_id, nodes=nodes,
                                      controller=controller, authorized_operations=operations)

    def g_describe_configs(self, resources, timeout=None, include_synonyms=False,
                           include_documentation=False):
        res = yield self._a.describe_configs(
            resources=resources,
            options=_opts(ka.DescribeConfigsOptions, timeout, include_synonyms=include_synonyms,
                          include_documentation=include_documentation))
        return (yield from _collect(res.values()))

    def g_incremental_alter_configs(self, configs, timeout=None, validate_only=False):
        res = yield self._a.incremental_alter_configs(
            configs=configs,
            options=_opts(ka.AlterConfigsOptions, timeout, validate_only=validate_only))
        return (yield from _collect(res.values()))

    def g_list_config_resources(self, resource_types, timeout=None):
        res = yield self._a.list_config_resources(
            config_resource_types=resource_types,
            options=_opts(ka.ListConfigResourcesOptions, timeout))
        return (yield res.all())

    def g_describe_log_dirs(self, brokers, timeout=None):
        res = yield self._a.describe_log_dirs(
            brokers=brokers, options=_opts(ka.DescribeLogDirsOptions, timeout))
        return (yield from _collect(res.descriptions()))

    def g_alter_replica_log_dirs(self, assignment, timeout=None):
        res = yield self._a.alter_replica_log_dirs(
            replica_assignment=assignment, options=_opts(ka.AlterReplicaLogDirsOptions, timeout))
        return (yield from _collect(res.values()))

    def g_describe_replica_log_dirs(self, replicas, timeout=None):
        res = yield self._a.describe_replica_log_dirs(
            replicas=replicas, options=_opts(ka.DescribeReplicaLogDirsOptions, timeout))
        return (yield from _collect(res.values()))

    # -- Elections, reassignments & offsets ---------------------------------

    def g_elect_leaders(self, election_type, partitions, timeout=None):
        res = yield self._a.elect_leaders(
            election_type=_enum(ElectionType, election_type), partitions=partitions,
            options=_opts(ka.ElectLeadersOptions, timeout))
        # One future for the whole map; each value is Java's Optional<Throwable>.
        return (yield res.partitions())

    def g_alter_partition_reassignments(self, reassignments, timeout=None,
                                        allow_replication_factor_change=True):
        res = yield self._a.alter_partition_reassignments(
            reassignments=reassignments,
            options=_opts(ka.AlterPartitionReassignmentsOptions, timeout,
                          allow_replication_factor_change=allow_replication_factor_change))
        return (yield from _collect(res.values()))

    def g_list_partition_reassignments(self, partitions, timeout=None):
        res = yield self._a.list_partition_reassignments(
            partitions=partitions, options=_opts(ka.ListPartitionReassignmentsOptions, timeout))
        return (yield res.reassignments())

    def g_list_offsets(self, specs, timeout=None, isolation_level=0):
        res = yield self._a.list_offsets(
            topic_partition_offsets=specs,
            options=_opts(ka.ListOffsetsOptions, timeout,
                          isolation_level=_enum(IsolationLevel, isolation_level)))
        out = {}
        for tp in specs:
            try:
                out[tp] = yield res.partition_result(partition=tp)
            except Exception as e:  # noqa: BLE001
                out[tp] = e
        return out

    # -- Groups & offsets ----------------------------------------------------

    def g_list_groups(self, group_states=(), protocol_types=(), types=(), timeout=None):
        options = _opts(ka.ListGroupsOptions, timeout)
        if group_states:
            options.in_group_states(group_states=[GroupState.parse(name=s) for s in group_states])
        if protocol_types:
            options.with_protocol_types(protocol_types=list(protocol_types))
        if types:
            options.with_types(types=[GroupType.parse(name=s) for s in types])
        res = yield self._a.list_groups(options=options)
        valid = yield res.valid()
        errors = yield res.errors()
        return list(valid), list(errors)

    def g_describe_consumer_groups(self, group_ids, timeout=None,
                                   include_authorized_operations=False):
        res = yield self._a.describe_consumer_groups(
            group_ids=group_ids,
            options=_opts(ka.DescribeConsumerGroupsOptions, timeout,
                          include_authorized_operations=include_authorized_operations))
        return (yield from _collect(res.described_groups()))

    def g_describe_classic_groups(self, group_ids, timeout=None,
                                  include_authorized_operations=False):
        res = yield self._a.describe_classic_groups(
            group_ids=group_ids,
            options=_opts(ka.DescribeClassicGroupsOptions, timeout,
                          include_authorized_operations=include_authorized_operations))
        return (yield from _collect(res.described_groups()))

    def g_list_consumer_group_offsets(self, specs, timeout=None, require_stable=False):
        res = yield self._a.list_consumer_group_offsets(
            group_specs=specs,
            options=_opts(ka.ListConsumerGroupOffsetsOptions, timeout,
                          require_stable=require_stable))
        out = {}
        for group_id in specs:
            try:
                out[group_id] = yield res.partitions_to_offset_and_metadata(group_id=group_id)
            except Exception as e:  # noqa: BLE001
                out[group_id] = e
        return out

    def g_alter_consumer_group_offsets(self, group_id, offsets, timeout=None):
        res = yield self._a.alter_consumer_group_offsets(
            group_id=group_id, offsets=offsets,
            options=_opts(ka.AlterConsumerGroupOffsetsOptions, timeout))
        return (yield from _per_key(res, offsets, lambda tp: res.partition_result(partition=tp)))

    def g_delete_consumer_group_offsets(self, group_id, partitions, timeout=None):
        res = yield self._a.delete_consumer_group_offsets(
            group_id=group_id, partitions=partitions,
            options=_opts(ka.DeleteConsumerGroupOffsetsOptions, timeout))
        return (yield from _per_key(res, list(partitions),
                                    lambda tp: res.partition_result(partition=tp)))

    def g_delete_consumer_groups(self, group_ids, timeout=None):
        res = yield self._a.delete_consumer_groups(
            group_ids=group_ids, options=_opts(ka.DeleteConsumerGroupsOptions, timeout))
        return (yield from _collect(res.deleted_groups()))

    def g_remove_members_from_consumer_group(self, group_id, members, reason=None, timeout=None):
        options = _opts(ka.RemoveMembersFromConsumerGroupOptions, timeout, members=members)
        if reason is not None:
            options.reason(reason=reason)
        res = yield self._a.remove_members_from_consumer_group(group_id=group_id, options=options)
        # removeAll has no per-member future: an empty result, whole-call error only.
        keys = [] if members is None else list(members)
        return (yield from _per_key(res, keys, lambda m: res.member_result(member=m),
                                    key_of=lambda m: m.group_instance_id()))

    # -- ACLs, quotas, SCRAM, delegation tokens & features --------------------

    def g_create_acls(self, acls, timeout=None):
        res = yield self._a.create_acls(acls=acls, options=_opts(ka.CreateAclsOptions, timeout))
        return (yield from _collect(res.values()))

    def g_describe_acls(self, acl_filter, timeout=None):
        res = yield self._a.describe_acls(filter=acl_filter,
                                    options=_opts(ka.DescribeAclsOptions, timeout))
        return (yield res.values())

    def g_delete_acls(self, filters, timeout=None):
        res = yield self._a.delete_acls(filters=filters, options=_opts(ka.DeleteAclsOptions, timeout))
        out = yield from _collect(res.values())
        # A filter's value is the list of matched ACLs, each with its own error.
        return {f: (v if isinstance(v, BaseException) else v.values()) for f, v in out.items()}

    def g_describe_client_quotas(self, quota_filter, timeout=None):
        res = yield self._a.describe_client_quotas(
            filter=quota_filter, options=_opts(ka.DescribeClientQuotasOptions, timeout))
        return (yield res.entities())

    def g_alter_client_quotas(self, entries, timeout=None, validate_only=False):
        res = yield self._a.alter_client_quotas(
            entries=entries,
            options=_opts(ka.AlterClientQuotasOptions, timeout, validate_only=validate_only))
        return (yield from _collect(res.values()))

    def g_describe_user_scram_credentials(self, users=(), timeout=None):
        options = _opts(ka.DescribeUserScramCredentialsOptions, timeout)
        if users:
            res = yield self._a.describe_user_scram_credentials(users=list(users), options=options)
        else:
            # Java's no-argument overload: every user.
            res = yield self._a.describe_user_scram_credentials(options=options)
        # Java's three views, composed as #251's contract does for every backend
        # (admin_service.proto, describeUserScramCredentials): `all()` first;
        # when it fails, `users()` plus `description(user)`. `all()` treats a
        # user the broker reports as having no credential (RESOURCE_NOT_FOUND)
        # as a success, so it is never an error here; once `all()` has failed,
        # `users()` leaves such a requested user out, and so does the answer.
        try:
            return dict((yield res.all()))
        except Exception as all_error:  # noqa: BLE001
            names = yield res.users()  # raises when the response itself failed
            out = {}
            for name in names:
                try:
                    out[name] = yield res.description(user_name=name)
                except Exception as e:  # noqa: BLE001
                    out[name] = e
            if not out:
                raise all_error
            return out

    def g_alter_user_scram_credentials(self, alterations, timeout=None):
        res = yield self._a.alter_user_scram_credentials(
            alterations=alterations, options=_opts(ka.AlterUserScramCredentialsOptions, timeout))
        return (yield from _collect(res.values()))

    def g_create_delegation_token(self, renewers=(), owner=None, max_lifetime_ms=None,
                                  timeout=None):
        res = yield self._a.create_delegation_token(
            options=_opts(ka.CreateDelegationTokenOptions, timeout, renewers=list(renewers),
                          owner=owner, max_lifetime_ms=max_lifetime_ms))
        return (yield res.delegation_token())

    def g_renew_delegation_token(self, hmac, renew_time_period_ms=None, timeout=None):
        res = yield self._a.renew_delegation_token(
            hmac=hmac, options=_opts(ka.RenewDelegationTokenOptions, timeout,
                                     renew_time_period_ms=renew_time_period_ms))
        return (yield res.expiry_timestamp())

    def g_expire_delegation_token(self, hmac, expiry_time_period_ms=None, timeout=None):
        res = yield self._a.expire_delegation_token(
            hmac=hmac, options=_opts(ka.ExpireDelegationTokenOptions, timeout,
                                     expiry_time_period_ms=expiry_time_period_ms))
        return (yield res.expiry_timestamp())

    def g_describe_delegation_token(self, owners=None, timeout=None):
        if owners is None and self._mock:
            # admin_service.proto: an absent owners filter is Java's unset filter,
            # "describe every token". The Python mock keeps Java's MockAdminClient
            # NullPointerException for a null owners() (decision log 10), so on
            # the mock the server sends the empty list, which that mock reads as
            # "every token" (`owners().isEmpty()`); the native mock returns every
            # token for both. The real client keeps None: its null is "all".
            owners = []
        res = yield self._a.describe_delegation_token(
            options=_opts(ka.DescribeDelegationTokenOptions, timeout, owners=owners))
        return (yield res.delegation_tokens())

    def g_describe_features(self, node_id=None, timeout=None):
        res = yield self._a.describe_features(
            options=_opts(ka.DescribeFeaturesOptions, timeout, node_id=node_id))
        return (yield res.feature_metadata())

    def g_update_features(self, feature_updates, timeout=None, validate_only=False):
        res = yield self._a.update_features(
            feature_updates=feature_updates,
            options=_opts(ka.UpdateFeaturesOptions, timeout, validate_only=validate_only))
        return (yield from _collect(res.values()))

    # -- Producers & transactions ----------------------------------------------

    def g_describe_producers(self, partitions, broker_id=None, timeout=None):
        res = yield self._a.describe_producers(
            partitions=partitions,
            options=_opts(ka.DescribeProducersOptions, timeout, broker_id=broker_id))
        out = {}
        for tp in partitions:
            try:
                out[tp] = yield res.partition_result(partition=tp)
            except Exception as e:  # noqa: BLE001
                out[tp] = e
        return out

    def g_describe_transactions(self, transactional_ids, timeout=None):
        res = yield self._a.describe_transactions(
            transactional_ids=transactional_ids,
            options=_opts(ka.DescribeTransactionsOptions, timeout))
        out = {}
        for tid in transactional_ids:
            try:
                out[tid] = yield res.description(transactional_id=tid)
            except Exception as e:  # noqa: BLE001
                out[tid] = e
        return out

    def g_abort_transaction(self, spec, timeout=None):
        res = yield self._a.abort_transaction(
            spec=spec, options=_opts(ka.AbortTransactionOptions, timeout))
        yield res.all()

    def g_force_terminate_transaction(self, transactional_id, timeout=None):
        res = yield self._a.force_terminate_transaction(
            transactional_id=transactional_id,
            options=_opts(ka.TerminateTransactionOptions, timeout))
        yield res.result()

    def g_unregister_controller(self, controller_id, timeout=None):
        res = yield self._a.unregister_controller(
            controller_id=controller_id,
            options=_opts(ka.UnregisterControllerOptions, timeout))
        yield res.all()

    def g_list_transactions(self, states=None, producer_ids=None, duration_ms=-1,
                            transactional_id_pattern=None, timeout=None):
        options = _opts(ka.ListTransactionsOptions, timeout,
                        filtered_states=[ka.TransactionState.parse(name=s) for s in states or []],
                        filtered_producer_ids=[int(p) for p in producer_ids or []],
                        filtered_duration=int(duration_ms))
        if transactional_id_pattern is not None:
            options.filter_on_transactional_id_pattern(pattern=str(transactional_id_pattern))
        res = yield self._a.list_transactions(options=options)
        by_broker = yield res.by_broker_id()
        return (yield from _collect(by_broker))

    def g_fence_producers(self, transactional_ids, timeout=None):
        res = yield self._a.fence_producers(
            transactional_ids=transactional_ids,
            options=_opts(ka.FenceProducersOptions, timeout))
        fenced = res.fenced_producers()
        out = {}
        for tid in transactional_ids:
            try:
                yield fenced[tid]
                producer_id = yield res.producer_id(transactional_id=tid)
                epoch = yield res.epoch_id(transactional_id=tid)
                out[tid] = _types.SimpleNamespace(producer_id=producer_id, epoch=epoch)
            except Exception as e:  # noqa: BLE001
                out[tid] = e
        return out


def _unwrapped(error):
    """The error behind a ``get()`` failure: Java's ``ExecutionException`` wraps
    the cause the future failed with."""
    if isinstance(error, ExecutionError) and error.__cause__ is not None:
        return error.__cause__
    return error


def _drive_sync(gen):
    """Run a `_AdminOps` generator. A yielded ``KafkaFuture`` (the sync
    ``Admin``'s futures) is resolved with `get()`, an ``ExecutionError`` becoming
    its cause; anything else (the ``*Result`` a call returned) is passed back as is."""
    try:
        item = next(gen)
    except StopIteration as done:
        return done.value
    while True:
        try:
            value = item.get() if isinstance(item, KafkaFuture) else item
        except BaseException as e:  # noqa: BLE001  (thrown into the RPC, which decides)
            try:
                item = gen.throw(_unwrapped(e))
            except StopIteration as done:
                return done.value
        else:
            try:
                item = gen.send(value)
            except StopIteration as done:
                return done.value


async def _drive_async(gen):
    """As `_drive_sync` for ``AsyncAdmin``: an ``AsyncKafkaFuture`` is resolved
    with ``await get()``, anything else passed back."""
    try:
        item = next(gen)
    except StopIteration as done:
        return done.value
    while True:
        try:
            value = await item.get() if isinstance(item, AsyncKafkaFuture) else item
        except Exception as e:  # noqa: BLE001
            try:
                item = gen.throw(_unwrapped(e))
            except StopIteration as done:
                return done.value
        else:
            try:
                item = gen.send(value)
            except StopIteration as done:
                return done.value


class SyncAdminFacade:
    """``Admin`` / ``MockAdminClient`` behind the resolved-outcome shape the
    sync gRPC server's handlers use: ``client.create_topics(...)`` blocks the
    worker thread and returns ``{name: value | error}``."""

    def __init__(self, admin, mock=False):
        self._admin = admin
        self._ops = _AdminOps(admin, mock)

    def __getattr__(self, name):
        op = getattr(self._ops, "g_" + name, None)
        if op is None:
            raise AttributeError(name)

        def call(*args, **kwargs):
            return _drive_sync(op(*args, **kwargs))
        return call

    def close(self, timeout=None):
        self._admin.close(timeout=timeout)


class AsyncAdminFacade:
    """``AsyncAdmin`` behind the same shape; every RPC is a coroutine."""

    def __init__(self, admin, mock=False):
        self._admin = admin
        self._ops = _AdminOps(admin, mock)

    def __getattr__(self, name):
        op = getattr(self._ops, "g_" + name, None)
        if op is None:
            raise AttributeError(name)

        async def call(*args, **kwargs):
            return await _drive_async(op(*args, **kwargs))
        return call

    async def close(self, timeout=None):
        await self._admin.close(timeout=timeout)


def _admin_mock_brokers(num_brokers):
    """`CreateAdminRequest.num_brokers` -> the broker list of a mock admin: ids
    0..n-1, the first one also the controller. Fewer than one broker is Java's
    IllegalArgumentException (and crosses as LOCAL_ILLEGAL_ARGUMENT)."""
    if num_brokers < 1:
        raise IllegalArgumentError(message="num_brokers must be at least 1")
    return [Node(id=i, host="localhost", port=9092 + i) for i in range(num_brokers)]


def admin_create_sync(config, num_brokers):
    """The sync server's CreateAdmin: a mock for an empty config, else a real
    admin client; wrapped in the resolved-outcome facade."""
    if _admin_selects_mock(config):
        brokers = _admin_mock_brokers(num_brokers)
        return SyncAdminFacade(ka.MockAdminClient(brokers=brokers, controller=brokers[0]),
                               mock=True)
    return SyncAdminFacade(ka.Admin.create(configs=config))


def admin_create_async(config, num_brokers):
    """The async server's CreateAdmin (see `admin_create_sync`)."""
    if _admin_selects_mock(config):
        brokers = _admin_mock_brokers(num_brokers)
        return AsyncAdminFacade(
            ka.AsyncMockAdminClient(brokers=brokers, controller=brokers[0]), mock=True)
    return AsyncAdminFacade(ka.AsyncAdmin.create(configs=config))
