"""Pythonic Kafka admin client over the Rust admin C FFI.

Mirrors the Java ``Admin`` interface with Python naming, in both a synchronous
(:class:`AdminClient`, :class:`MockAdminClient`) and an asyncio-native
(:class:`AsyncAdminClient`, :class:`AsyncMockAdminClient`) form.

Design notes
------------
* The C extension (``_confluentkafka``) is a marshaling layer only: it converts
  Python objects to/from the C FFI and bridges the FFI's async callbacks back
  into Python. All orchestration lives here in pure Python.
* **Both APIs drive the async C bindings.** Even the synchronous methods submit
  an async FFI op and then wait on an interruptible Python primitive, so a slow
  admin RPC never parks the calling thread inside a native ``block_on`` where
  Python signal handlers cannot run.
* Unlike the consumer, the admin client has no ``wakeup()``: an in-flight RPC
  cannot be aborted. On ``KeyboardInterrupt`` the sync waiter therefore keeps
  waiting for the callback (so the result handles are freed rather than leaked)
  and then re-raises. This matches Java, where interrupting
  ``KafkaFuture.get()`` does not cancel the underlying admin request.
* The Rust admin client is thread-safe (Java's ``KafkaAdminClient`` is too), so
  concurrent calls from several threads or tasks are allowed — there is no
  single-owner guard as on the consumer.

Per-key results
---------------
Java returns one ``KafkaFuture<T>`` per key (per topic for ``createTopics``).
C has no ``KafkaFuture``, so each RPC delivers one flattened result handle
carrying a value *and* an error per key, and this module drains it into a plain
dict whose values are either the result object or a :class:`KafkaError`:

* ``create_topics`` -> ``{topic_name: TopicMetadataAndConfig | KafkaError}``
* ``delete_topics`` -> ``{topic_name: None | KafkaError}`` (Java's per-key
  future is ``KafkaFuture<Void>``, so ``None`` means success)
* ``describe_topics`` -> ``{topic_name: TopicDescription | KafkaError}``
* ``list_topics`` -> ``{topic_name: TopicListing}`` (Java has a single future
  here, so a failure raises instead of appearing per key)
* ``create_partitions`` -> ``{topic_name: None | KafkaError}`` (per-topic future
  is ``KafkaFuture<Void>``, so ``None`` means success)
* ``delete_records`` -> ``{(topic, partition): DeletedRecords | KafkaError}``
* ``describe_configs`` -> ``{ConfigResource: Config | KafkaError}``
* ``incremental_alter_configs`` -> ``{ConfigResource: None | KafkaError}``
  (per-resource future is ``KafkaFuture<Void>``, so ``None`` means success)
* ``describe_log_dirs`` ->
  ``{broker_id: {log_dir: LogDirDescription} | KafkaError}``
* ``alter_replica_log_dirs`` ->
  ``{TopicPartitionReplica: None | KafkaError}``
* ``describe_replica_log_dirs`` ->
  ``{TopicPartitionReplica: ReplicaLogDirInfo | KafkaError}``
* ``elect_leaders`` -> ``{(topic, partition): None | KafkaError}`` (Java's
  per-partition value is an ``Optional<Throwable>``, so ``None`` means that
  partition's election succeeded)
* ``alter_partition_reassignments`` ->
  ``{(topic, partition): None | KafkaError}`` (per-partition future is
  ``KafkaFuture<Void>``, so ``None`` means success)
* ``list_partition_reassignments`` ->
  ``{(topic, partition): PartitionReassignment}`` (Java has a single future
  here, so a failure raises instead of appearing per key)
* ``list_offsets`` ->
  ``{(topic, partition): ListOffsetsResultInfo | KafkaError}``
* ``describe_cluster`` -> a single :class:`ClusterDescription` (Java's result is
  four independent futures, not a map, so a failure raises)
* ``list_config_resources`` -> ``[ConfigResource]``, and
  ``list_client_metrics_resources`` -> ``[ClientMetricsResourceListing]``
  (single futures in Java, so a failure raises)

A per-key failure therefore does **not** raise; iterate the dict and check for
``KafkaError`` values. Only a whole-call failure raises.
"""

import asyncio
import datetime as _dt
import threading

import _confluentkafka as _lib
from consumer import Node  # shared broker-node type
from producer import KafkaError  # shared error type


# --------------------------------------------------------------------------
# Supporting value types (mirror the Java admin types).
# --------------------------------------------------------------------------
class NewTopic:
    """A request to create a new topic (Java ``NewTopic``).

    Pass ``num_partitions`` / ``replication_factor`` as ``None`` to use the
    broker's ``num.partitions`` / ``default.replication.factor``.

    Setting ``replicas_assignments`` selects Java's
    ``NewTopic(name, Map<Integer, List<Integer>>)`` constructor, in which
    ``num_partitions`` and ``replication_factor`` are not sent.
    """

    __slots__ = ("name", "num_partitions", "replication_factor", "configs",
                 "replicas_assignments")

    def __init__(self, name, num_partitions=None, replication_factor=None,
                 configs=None, replicas_assignments=None):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.configs = dict(configs) if configs else {}
        self.replicas_assignments = (dict(replicas_assignments)
                                     if replicas_assignments else {})

    def _to_spec(self):
        """The tuple shape the C layer parses (see ``build_new_topics``)."""
        return (
            self.name,
            -1 if self.num_partitions is None else int(self.num_partitions),
            -1 if self.replication_factor is None else int(self.replication_factor),
            [(str(k), str(v)) for k, v in self.configs.items()],
            [(int(p), [int(b) for b in brokers])
             for p, brokers in self.replicas_assignments.items()],
        )

    def __repr__(self):
        return (f"NewTopic(name={self.name!r}, num_partitions={self.num_partitions}, "
                f"replication_factor={self.replication_factor})")


class NewPartitions:
    """A request to increase a topic's partition count (Java ``NewPartitions``).

    ``total_count`` is the total number of partitions *after* the operation, not
    the number added. Leave ``new_assignments`` as ``None`` to let the broker
    decide the replica assignment (Java's ``NewPartitions.increaseTo(int)``);
    supplying it selects ``increaseTo(int, List<List<Integer>>)``, in which case
    it should hold one list of broker ids per *new* partition (existing
    partitions are not reassigned) and the first id in a list is the preferred
    leader.
    """

    __slots__ = ("total_count", "new_assignments")

    def __init__(self, total_count, new_assignments=None):
        self.total_count = total_count
        self.new_assignments = (None if new_assignments is None
                                else [list(a) for a in new_assignments])

    def _to_spec(self, topic):
        """The tuple shape the C layer parses (see ``build_new_partitions``)."""
        assignments = (None if self.new_assignments is None
                       else [[int(b) for b in a] for a in self.new_assignments])
        return (str(topic), int(self.total_count), assignments)

    def __repr__(self):
        return (f"NewPartitions(total_count={self.total_count}, "
                f"new_assignments={self.new_assignments})")


class RecordsToDelete:
    """Records to delete from one partition (Java ``RecordsToDelete``).

    Java exposes only the static factory ``beforeOffset(long)``; the constructor
    here is the same thing. Pass ``-1`` to truncate the partition to its high
    watermark.
    """

    __slots__ = ("before_offset",)

    def __init__(self, before_offset):
        self.before_offset = before_offset

    def __repr__(self):
        return f"RecordsToDelete(before_offset={self.before_offset})"


class DeletedRecords:
    """The outcome of deleting records from one partition (Java
    ``DeletedRecords``): the partition's low watermark afterwards."""

    __slots__ = ("low_watermark",)

    def __init__(self, low_watermark):
        self.low_watermark = low_watermark

    def __repr__(self):
        return f"DeletedRecords(low_watermark={self.low_watermark})"


class ConfigSynonym:
    """A configuration synonym of a :class:`ConfigEntry` (Java
    ``ConfigEntry.ConfigSynonym``).

    ``source`` is Java's ``ConfigEntry.ConfigSource`` enum constant name, e.g.
    ``"STATIC_BROKER_CONFIG"``; that enum has no numeric id in Java, so the name
    is the contract. ``value`` may be ``None``.
    """

    __slots__ = ("name", "value", "source")

    def __init__(self, name, value, source):
        self.name = name
        self.value = value
        self.source = source

    def __repr__(self):
        return f"ConfigSynonym(name={self.name!r}, source={self.source!r})"


class ConfigEntry:
    """A configuration entry (Java ``ConfigEntry``).

    ``source`` and ``config_type`` are Java's ``ConfigSource`` / ``ConfigType``
    enum constant names (neither enum has a numeric id in Java). Synonyms keep
    Java's precedence order.

    ``create_topics`` populates only the first five fields — the broker's
    ``CreateTopicsResponse`` carries no source, type, documentation or synonyms —
    so on entries obtained from it the rest are ``None`` / empty.
    ``describe_configs`` populates all of them.
    """

    __slots__ = ("name", "value", "is_default", "is_sensitive", "is_read_only",
                 "source", "config_type", "documentation", "synonyms")

    def __init__(self, name, value, is_default, is_sensitive, is_read_only,
                 source=None, config_type=None, documentation=None, synonyms=()):
        self.name = name
        self.value = value
        self.is_default = is_default
        self.is_sensitive = is_sensitive
        self.is_read_only = is_read_only
        self.source = source
        self.config_type = config_type
        self.documentation = documentation
        self.synonyms = list(synonyms)

    def __repr__(self):
        return f"ConfigEntry(name={self.name!r}, value={self.value!r})"


class Config:
    """The configuration entries of one resource (Java ``Config``)."""

    __slots__ = ("entries",)

    def __init__(self, entries):
        self.entries = list(entries)

    def get(self, name):
        """The entry named ``name``, or ``None`` (Java's ``Config.get``)."""
        for entry in self.entries:
            if entry.name == name:
                return entry
        return None

    def __repr__(self):
        return f"Config(entries={len(self.entries)})"


class ConfigResourceType:
    """Config-resource types (Java ``ConfigResource.Type``).

    The values are Java's ``ConfigResource.Type.id()`` wire codes, which is what
    crosses the C boundary.
    """

    UNKNOWN = 0
    TOPIC = 2
    BROKER = 4
    BROKER_LOGGER = 8
    CLIENT_METRICS = 16
    GROUP = 32


class ConfigResource:
    """A resource that has configs (Java ``ConfigResource``).

    Hashable, so it can key the ``describe_configs`` /
    ``incremental_alter_configs`` result dicts exactly as in Java.
    """

    __slots__ = ("resource_type", "name")

    def __init__(self, resource_type, name):
        self.resource_type = int(resource_type)
        self.name = str(name)

    def is_default(self):
        """Whether this is the cluster-wide default resource (empty name)."""
        return self.name == ""

    def __eq__(self, other):
        return (isinstance(other, ConfigResource)
                and self.resource_type == other.resource_type
                and self.name == other.name)

    def __hash__(self):
        return hash((self.resource_type, self.name))

    def __repr__(self):
        return f"ConfigResource(resource_type={self.resource_type}, name={self.name!r})"


class OpType:
    """Incremental config-alteration operations (Java ``AlterConfigOp.OpType``).

    The values are Java's ``OpType.id()`` wire codes.
    """

    SET = 0
    DELETE = 1
    APPEND = 2
    SUBTRACT = 3


class AlterConfigOp:
    """One incremental configuration change (Java ``AlterConfigOp``).

    ``config_entry`` supplies the name and the value; a ``DELETE`` carries a
    ``None`` value.
    """

    __slots__ = ("config_entry", "op_type")

    def __init__(self, config_entry, op_type):
        self.config_entry = config_entry
        self.op_type = op_type

    def __repr__(self):
        return (f"AlterConfigOp(name={self.config_entry.name!r}, "
                f"op_type={self.op_type})")


class TopicPartitionReplica:
    """One replica of one partition on one broker (Java
    ``TopicPartitionReplica``). Hashable, so it can key the log-dir result
    dicts as in Java."""

    __slots__ = ("topic", "partition", "broker_id")

    def __init__(self, topic, partition, broker_id):
        self.topic = str(topic)
        self.partition = int(partition)
        self.broker_id = int(broker_id)

    def __eq__(self, other):
        return (isinstance(other, TopicPartitionReplica)
                and self.topic == other.topic
                and self.partition == other.partition
                and self.broker_id == other.broker_id)

    def __hash__(self):
        return hash((self.topic, self.partition, self.broker_id))

    def __repr__(self):
        return (f"TopicPartitionReplica(topic={self.topic!r}, "
                f"partition={self.partition}, broker_id={self.broker_id})")


class ReplicaInfo:
    """One replica hosted in a log directory (Java ``ReplicaInfo``)."""

    __slots__ = ("size", "offset_lag", "is_future")

    def __init__(self, size, offset_lag, is_future):
        self.size = size
        self.offset_lag = offset_lag
        self.is_future = is_future

    def __repr__(self):
        return f"ReplicaInfo(size={self.size}, offset_lag={self.offset_lag})"


class LogDirDescription:
    """One log directory of one broker (Java ``LogDirDescription``).

    ``error`` is this directory's own error (offline, unreadable, ...) and is
    distinct from a per-broker failure, which appears as a :class:`KafkaError`
    *instead of* this object. ``total_bytes`` / ``usable_bytes`` are ``None``
    when the broker did not report them (Java's empty ``OptionalLong``).
    ``replica_infos`` is keyed by ``(topic, partition)``.
    """

    __slots__ = ("error", "total_bytes", "usable_bytes", "replica_infos")

    def __init__(self, error, total_bytes, usable_bytes, replica_infos):
        self.error = error
        self.total_bytes = total_bytes
        self.usable_bytes = usable_bytes
        self.replica_infos = replica_infos

    def __repr__(self):
        return f"LogDirDescription(replicas={len(self.replica_infos)})"


class ReplicaLogDirInfo:
    """Where one replica lives, and where it is moving to (Java
    ``DescribeReplicaLogDirsResult.ReplicaLogDirInfo``).

    ``current_replica_log_dir`` is ``None`` when the broker hosts no replica of
    that partition; ``future_replica_log_dir`` is ``None`` when no move is
    pending.
    """

    __slots__ = ("current_replica_log_dir", "current_replica_offset_lag",
                 "future_replica_log_dir", "future_replica_offset_lag")

    def __init__(self, current_replica_log_dir, current_replica_offset_lag,
                 future_replica_log_dir, future_replica_offset_lag):
        self.current_replica_log_dir = current_replica_log_dir
        self.current_replica_offset_lag = current_replica_offset_lag
        self.future_replica_log_dir = future_replica_log_dir
        self.future_replica_offset_lag = future_replica_offset_lag

    def __repr__(self):
        return (f"ReplicaLogDirInfo(current={self.current_replica_log_dir!r}, "
                f"future={self.future_replica_log_dir!r})")


class ElectionType:
    """Leader-election types (Java ``ElectionType``). The values are Java's
    public ``byte value`` field."""

    PREFERRED = 0
    UNCLEAN = 1


class IsolationLevel:
    """Read isolation levels (Java ``IsolationLevel``). The values are Java's
    ``id()`` wire codes."""

    READ_UNCOMMITTED = 0
    READ_COMMITTED = 1


class NewPartitionReassignment:
    """A target replica set for one partition (Java
    ``NewPartitionReassignment``).

    To *cancel* an ongoing reassignment pass ``None`` in place of this object,
    which is Java's empty ``Optional`` (``Admin.java:1142-1143``). An empty
    ``target_replicas`` is an error, not a cancellation — Java's constructor
    throws ``IllegalArgumentException``.
    """

    __slots__ = ("target_replicas",)

    def __init__(self, target_replicas):
        self.target_replicas = [int(r) for r in target_replicas]

    def __repr__(self):
        return f"NewPartitionReassignment(target_replicas={self.target_replicas})"


class PartitionReassignment:
    """An ongoing reassignment of one partition (Java
    ``PartitionReassignment``): the current replicas, plus those being added and
    removed. All three are broker ids."""

    __slots__ = ("replicas", "adding_replicas", "removing_replicas")

    def __init__(self, replicas, adding_replicas, removing_replicas):
        self.replicas = list(replicas)
        self.adding_replicas = list(adding_replicas)
        self.removing_replicas = list(removing_replicas)

    def __repr__(self):
        return (f"PartitionReassignment(replicas={self.replicas}, "
                f"adding_replicas={self.adding_replicas}, "
                f"removing_replicas={self.removing_replicas})")


class OffsetSpec:
    """Which offset ``list_offsets`` should return for a partition (Java
    ``OffsetSpec``).

    Java models the variants as subclasses with static factories; the same
    factories are class methods here. Internally each one is the
    ``ListOffsets`` wire sentinel Java's ``KafkaAdminClient.getOffsetFromSpec``
    emits for it, plus a flag separating ``for_timestamp`` from the rest — the
    projection is not injective (``for_timestamp(-2)`` and ``earliest()`` both
    give ``-2``), so the flag is what keeps them apart.
    """

    __slots__ = ("is_timestamp", "value")

    # `ListOffsetsRequest` sentinels (src/common/requests/list_offsets_request.rs).
    _LATEST = -1
    _EARLIEST = -2
    _MAX_TIMESTAMP = -3
    _EARLIEST_LOCAL = -4
    _LATEST_TIERED = -5
    _EARLIEST_PENDING_UPLOAD = -6

    def __init__(self, is_timestamp, value):
        self.is_timestamp = bool(is_timestamp)
        self.value = int(value)

    @classmethod
    def latest(cls):
        return cls(False, cls._LATEST)

    @classmethod
    def earliest(cls):
        return cls(False, cls._EARLIEST)

    @classmethod
    def max_timestamp(cls):
        return cls(False, cls._MAX_TIMESTAMP)

    @classmethod
    def earliest_local(cls):
        return cls(False, cls._EARLIEST_LOCAL)

    @classmethod
    def latest_tiered(cls):
        return cls(False, cls._LATEST_TIERED)

    @classmethod
    def earliest_pending_upload(cls):
        return cls(False, cls._EARLIEST_PENDING_UPLOAD)

    @classmethod
    def for_timestamp(cls, timestamp):
        """The earliest offset whose timestamp is at least ``timestamp``
        (epoch milliseconds). Java ``OffsetSpec.forTimestamp(long)``."""
        return cls(True, timestamp)

    def __eq__(self, other):
        return (isinstance(other, OffsetSpec)
                and self.is_timestamp == other.is_timestamp
                and self.value == other.value)

    def __hash__(self):
        return hash((self.is_timestamp, self.value))

    def __repr__(self):
        if self.is_timestamp:
            return f"OffsetSpec.for_timestamp({self.value})"
        return f"OffsetSpec(sentinel={self.value})"


class ListOffsetsResultInfo:
    """One partition's offset (Java
    ``ListOffsetsResult.ListOffsetsResultInfo``).

    ``timestamp`` is ``-1`` when the broker reported none, and ``leader_epoch``
    is ``None`` for Java's empty ``Optional``.
    """

    __slots__ = ("offset", "timestamp", "leader_epoch")

    def __init__(self, offset, timestamp, leader_epoch):
        self.offset = offset
        self.timestamp = timestamp
        self.leader_epoch = leader_epoch

    def __repr__(self):
        return (f"ListOffsetsResultInfo(offset={self.offset}, "
                f"timestamp={self.timestamp}, leader_epoch={self.leader_epoch})")


class ClientMetricsResourceListing:
    """A client-metrics resource (Java ``ClientMetricsResourceListing``)."""

    __slots__ = ("name",)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"ClientMetricsResourceListing(name={self.name!r})"


class ClusterDescription:
    """The cluster, as ``describe_cluster`` reports it.

    Java has no such class: ``DescribeClusterResult`` exposes four independent
    ``KafkaFuture``s (nodes, controller, cluster id, authorized operations).
    Neither C nor this module has a ``KafkaFuture``, so one call yields one
    object holding all four values; a failure of any of them fails the call.

    ``controller`` is ``None`` when there is no current controller.
    ``authorized_operations`` holds ``AclOperation`` wire codes (Java's
    ``AclOperation.code()``) and is ``None`` — not empty — when the broker did
    not report them at all.
    """

    __slots__ = ("cluster_id", "nodes", "controller", "authorized_operations")

    def __init__(self, cluster_id, nodes, controller, authorized_operations):
        self.cluster_id = cluster_id
        self.nodes = nodes
        self.controller = controller
        self.authorized_operations = authorized_operations

    def __repr__(self):
        return (f"ClusterDescription(cluster_id={self.cluster_id!r}, "
                f"nodes={len(self.nodes)})")


class TopicMetadataAndConfig:
    """Metadata for one created topic (Java
    ``CreateTopicsResult.TopicMetadataAndConfig``).

    If the broker created the topic but returned no metadata, ``error`` holds
    the exception Java's ``ensureSuccess()`` would rethrow from every accessor,
    ``topic_id`` is empty and the numeric fields are ``-1``. That is distinct
    from a per-key creation failure, which appears as a :class:`KafkaError`
    *instead of* this object.
    """

    __slots__ = ("topic_id", "num_partitions", "replication_factor", "configs", "error")

    def __init__(self, topic_id, num_partitions, replication_factor, configs, error):
        self.topic_id = topic_id
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.configs = configs
        self.error = error

    def __repr__(self):
        return (f"TopicMetadataAndConfig(topic_id={self.topic_id!r}, "
                f"num_partitions={self.num_partitions}, "
                f"replication_factor={self.replication_factor})")


class TopicListing:
    """A topic returned by ``list_topics`` (Java ``TopicListing``)."""

    __slots__ = ("name", "topic_id", "is_internal")

    def __init__(self, name, topic_id, is_internal):
        self.name = name
        self.topic_id = topic_id
        self.is_internal = is_internal

    def __repr__(self):
        return (f"TopicListing(name={self.name!r}, topic_id={self.topic_id!r}, "
                f"is_internal={self.is_internal})")


class TopicPartitionInfo:
    """One partition of a described topic (Java ``TopicPartitionInfo``).

    ``elr`` / ``last_known_elr`` are ``None`` when the broker did not report the
    eligible-leader-replica sets (Java returns null), and an empty list when it
    reported them empty.
    """

    __slots__ = ("partition", "leader", "replicas", "isr", "elr", "last_known_elr")

    def __init__(self, partition, leader, replicas, isr, elr, last_known_elr):
        self.partition = partition
        self.leader = leader
        self.replicas = replicas
        self.isr = isr
        self.elr = elr
        self.last_known_elr = last_known_elr

    def __repr__(self):
        return (f"TopicPartitionInfo(partition={self.partition}, "
                f"leader={self.leader!r})")


class TopicDescription:
    """A described topic (Java ``TopicDescription``).

    ``authorized_operations`` holds ``AclOperation`` wire codes (Java's
    ``AclOperation.code()``); it is empty unless the request asked for them.
    """

    __slots__ = ("name", "topic_id", "is_internal", "partitions", "authorized_operations")

    def __init__(self, name, topic_id, is_internal, partitions, authorized_operations):
        self.name = name
        self.topic_id = topic_id
        self.is_internal = is_internal
        self.partitions = partitions
        self.authorized_operations = authorized_operations

    def __repr__(self):
        return (f"TopicDescription(name={self.name!r}, topic_id={self.topic_id!r}, "
                f"partitions={len(self.partitions)})")


# --------------------------------------------------------------------------
# Conversions from the C drain dicts to the Python value types.
# --------------------------------------------------------------------------
def _to_error(raw):
    """``(code, message, is_retriable, is_fatal)`` -> KafkaError, or None."""
    return None if raw is None else KafkaError._from_parts(*raw)


def _to_node(n):
    return None if n is None else Node(n[0], n[1], n[2], n[3])


def _to_config_entry(raw):
    name, value, is_default, is_sensitive, is_read_only = raw
    return ConfigEntry(name, value, bool(is_default), bool(is_sensitive),
                       bool(is_read_only))


def _to_metadata(raw):
    topic_id, num_partitions, replication_factor, configs, error = raw
    return TopicMetadataAndConfig(topic_id, num_partitions, replication_factor,
                                  [_to_config_entry(c) for c in configs],
                                  _to_error(error))


def _to_create_topics(raw):
    """{name: (error, metadata)} -> {name: TopicMetadataAndConfig | KafkaError}"""
    out = {}
    for name, (error, metadata) in raw.items():
        out[name] = _to_error(error) if error is not None else _to_metadata(metadata)
    return out


def _to_delete_topics(raw):
    """{key: error} -> {key: None | KafkaError}"""
    return {key: _to_error(error) for key, error in raw.items()}


def _to_list_topics(raw):
    """{name: (name, topic_id, is_internal)} -> {name: TopicListing}"""
    return {name: TopicListing(n, topic_id, bool(is_internal))
            for name, (n, topic_id, is_internal) in raw.items()}


def _to_partition_info(raw):
    partition, leader, replicas, isr, elr, last_known_elr = raw
    return TopicPartitionInfo(
        partition,
        _to_node(leader),
        [_to_node(n) for n in replicas],
        [_to_node(n) for n in isr],
        None if elr is None else [_to_node(n) for n in elr],
        None if last_known_elr is None else [_to_node(n) for n in last_known_elr],
    )


def _to_description(raw):
    name, topic_id, is_internal, partitions, operations = raw
    return TopicDescription(name, topic_id, bool(is_internal),
                            [_to_partition_info(p) for p in partitions],
                            list(operations))


def _to_describe_topics(raw):
    """{key: (error, description)} -> {key: TopicDescription | KafkaError}"""
    out = {}
    for key, (error, description) in raw.items():
        out[key] = _to_error(error) if error is not None else _to_description(description)
    return out


def _to_create_partitions(raw):
    """{topic: error} -> {topic: None | KafkaError}

    Same shape as ``delete_topics``: Java's per-topic future is
    ``KafkaFuture<Void>``, so ``None`` means success.
    """
    return {topic: _to_error(error) for topic, error in raw.items()}


def _to_delete_records(raw):
    """{(topic, partition): (error, low_watermark)}
    -> {(topic, partition): DeletedRecords | KafkaError}"""
    out = {}
    for key, (error, low_watermark) in raw.items():
        out[key] = _to_error(error) if error is not None else DeletedRecords(low_watermark)
    return out


def _to_cluster_description(raw):
    """(cluster_id, [node], controller, operations) -> ClusterDescription"""
    cluster_id, nodes, controller, operations = raw
    return ClusterDescription(cluster_id, [_to_node(n) for n in nodes],
                              _to_node(controller),
                              None if operations is None else list(operations))


def _to_full_config_entry(raw):
    """The 9-tuple describe_configs reports -> ConfigEntry."""
    (name, value, is_default, is_sensitive, is_read_only, source, config_type,
     documentation, synonyms) = raw
    return ConfigEntry(name, value, bool(is_default), bool(is_sensitive),
                       bool(is_read_only), source, config_type, documentation,
                       [ConfigSynonym(*s) for s in synonyms])


def _to_describe_configs(raw):
    """{(type, name): (error, [entry])} -> {ConfigResource: Config | KafkaError}"""
    out = {}
    for (resource_type, name), (error, entries) in raw.items():
        resource = ConfigResource(resource_type, name)
        out[resource] = (_to_error(error) if error is not None
                         else Config([_to_full_config_entry(e) for e in entries]))
    return out


def _to_alter_configs(raw):
    """{(type, name): error} -> {ConfigResource: None | KafkaError}

    Java's per-resource future is ``KafkaFuture<Void>``, so ``None`` means
    success.
    """
    return {ConfigResource(resource_type, name): _to_error(error)
            for (resource_type, name), error in raw.items()}


def _to_config_resources(raw):
    """[(type, name)] -> [ConfigResource]"""
    return [ConfigResource(resource_type, name) for resource_type, name in raw]


def _to_client_metrics_resources(raw):
    """[name] -> [ClientMetricsResourceListing]"""
    return [ClientMetricsResourceListing(name) for name in raw]


def _to_log_dir_description(raw):
    """(error, total_bytes, usable_bytes, [replica]) -> LogDirDescription"""
    error, total_bytes, usable_bytes, replicas = raw
    # -1 is the wire's UNKNOWN_VOLUME_BYTES, i.e. Java's empty OptionalLong.
    return LogDirDescription(
        _to_error(error),
        None if total_bytes < 0 else total_bytes,
        None if usable_bytes < 0 else usable_bytes,
        {(topic, partition): ReplicaInfo(size, offset_lag, bool(is_future))
         for topic, partition, size, offset_lag, is_future in replicas},
    )


def _to_describe_log_dirs(raw):
    """{broker: (error, {log_dir: description})}
    -> {broker: {log_dir: LogDirDescription} | KafkaError}"""
    out = {}
    for broker, (error, log_dirs) in raw.items():
        out[broker] = (_to_error(error) if error is not None
                       else {name: _to_log_dir_description(d)
                             for name, d in log_dirs.items()})
    return out


def _to_alter_replica_log_dirs(raw):
    """{(topic, partition, broker): error}
    -> {TopicPartitionReplica: None | KafkaError}"""
    return {TopicPartitionReplica(*key): _to_error(error) for key, error in raw.items()}


def _to_describe_replica_log_dirs(raw):
    """{(topic, partition, broker): (error, info)}
    -> {TopicPartitionReplica: ReplicaLogDirInfo | KafkaError}"""
    out = {}
    for key, (error, info) in raw.items():
        out[TopicPartitionReplica(*key)] = (_to_error(error) if error is not None
                                            else ReplicaLogDirInfo(*info))
    return out


def _to_elect_leaders(raw):
    """{(topic, partition): error} -> {(topic, partition): None | KafkaError}

    Java's ``ElectLeadersResult.partitions()`` is
    ``Map<TopicPartition, Optional<Throwable>>``: there is no per-partition
    value, so ``None`` means the election succeeded for that partition.
    """
    return {key: _to_error(error) for key, error in raw.items()}


def _to_alter_partition_reassignments(raw):
    """{(topic, partition): error} -> {(topic, partition): None | KafkaError}"""
    return {key: _to_error(error) for key, error in raw.items()}


def _to_list_partition_reassignments(raw):
    """{(topic, partition): (replicas, adding, removing)}
    -> {(topic, partition): PartitionReassignment}"""
    return {key: PartitionReassignment(*value) for key, value in raw.items()}


def _to_list_offsets(raw):
    """{(topic, partition): (error, info)}
    -> {(topic, partition): ListOffsetsResultInfo | KafkaError}"""
    out = {}
    for key, (error, info) in raw.items():
        out[key] = _to_error(error) if error is not None else ListOffsetsResultInfo(*info)
    return out


def _ms(timeout):
    """Convert a timeout (seconds float, ``timedelta``, or None) to int32 ms.

    ``None`` becomes -1, which leaves the RPC's ``timeoutMs`` option unset so the
    client's ``default.api.timeout.ms`` applies (Java passes a null timeout).
    """
    if timeout is None:
        return -1
    if isinstance(timeout, _dt.timedelta):
        return int(timeout.total_seconds() * 1000)
    return int(float(timeout) * 1000)


def _close_ms(timeout):
    """Convert a close timeout to int64 ms; ``None`` becomes -1, which the C
    layer maps to Java's no-argument ``close()`` (wait indefinitely)."""
    if timeout is None:
        return -1
    if isinstance(timeout, _dt.timedelta):
        return int(timeout.total_seconds() * 1000)
    return int(float(timeout) * 1000)


# --------------------------------------------------------------------------
# Shared base: handle ownership and the per-method (submit, resolve, free)
# specs driven by _run_sync / _run_async.
# --------------------------------------------------------------------------
class _AdminBase:
    def __init__(self):
        self._h = None
        self.closed = False

    def _init_mock(self, num_brokers=1):
        self._h = _lib.Admin_MockAdminClient_new(num_brokers)

    def _init_kafka(self, config):
        self._h = _lib.Admin_AdminClient_new(config)

    def _check_closed(self):
        if self.closed:
            raise RuntimeError("AdminClient is already closed")

    def _destroy(self):
        if self._h is not None:
            _lib.Admin_destroy(self._h)
            self._h = None

    # ---- resolve / free pairs (by callback payload shape) ------------------
    @staticmethod
    def _resolve_void(payload):
        (error,) = payload
        if error:
            raise KafkaError._from_c(error)
        return None

    @staticmethod
    def _free_void(payload):
        if payload[0]:
            _lib.KafkaError_destroy(payload[0])

    @staticmethod
    def _resolve_value(drain, convert):
        def resolve(payload):
            handle, error = payload
            if error:
                raise KafkaError._from_c(error)
            return convert(drain(handle))
        return resolve

    @staticmethod
    def _free_value(drain):
        def free(payload):
            handle, error = payload
            if error:
                _lib.KafkaError_destroy(error)
            if handle:
                drain(handle)  # drain destroys the handle
        return free

    # ---- per-method specs: (submit, resolve, free) -------------------------
    def _close_spec(self, timeout):
        ms = _close_ms(timeout)
        return (lambda cb: _lib.Admin_close_async(self._h, ms, cb),
                self._resolve_void, self._free_void)

    def _create_topics_spec(self, new_topics, timeout, validate_only,
                            retry_on_quota_violation):
        spec = [t._to_spec() for t in new_topics]
        ms = _ms(timeout)
        drain = _lib.CreateTopicsResult_drain
        return (lambda cb: _lib.Admin_create_topics_async(
                    self._h, spec, ms, bool(validate_only),
                    bool(retry_on_quota_violation), cb),
                self._resolve_value(drain, _to_create_topics),
                self._free_value(drain))

    def _delete_topics_spec(self, topics, timeout, retry_on_quota_violation, by_ids):
        names = [str(t) for t in topics]
        ms = _ms(timeout)
        fn = (_lib.Admin_delete_topics_by_ids_async if by_ids
              else _lib.Admin_delete_topics_async)
        drain = _lib.DeleteTopicsResult_drain
        return (lambda cb: fn(self._h, names, ms, bool(retry_on_quota_violation), cb),
                self._resolve_value(drain, _to_delete_topics),
                self._free_value(drain))

    def _list_topics_spec(self, timeout, list_internal):
        ms = _ms(timeout)
        drain = _lib.ListTopicsResult_drain
        return (lambda cb: _lib.Admin_list_topics_async(
                    self._h, ms, bool(list_internal), cb),
                self._resolve_value(drain, _to_list_topics),
                self._free_value(drain))

    def _create_partitions_spec(self, new_partitions, timeout, validate_only,
                                retry_on_quota_violation):
        spec = [np._to_spec(topic) for topic, np in new_partitions.items()]
        ms = _ms(timeout)
        drain = _lib.CreatePartitionsResult_drain
        return (lambda cb: _lib.Admin_create_partitions_async(
                    self._h, spec, ms, bool(validate_only),
                    bool(retry_on_quota_violation), cb),
                self._resolve_value(drain, _to_create_partitions),
                self._free_value(drain))

    def _delete_records_spec(self, records_to_delete, timeout):
        spec = [(str(topic), int(partition), int(rtd.before_offset))
                for (topic, partition), rtd in records_to_delete.items()]
        ms = _ms(timeout)
        drain = _lib.DeleteRecordsResult_drain
        return (lambda cb: _lib.Admin_delete_records_async(self._h, spec, ms, cb),
                self._resolve_value(drain, _to_delete_records),
                self._free_value(drain))

    def _describe_cluster_spec(self, timeout, include_authorized_operations,
                               include_fenced_brokers):
        ms = _ms(timeout)
        drain = _lib.DescribeClusterResult_drain
        return (lambda cb: _lib.Admin_describe_cluster_async(
                    self._h, ms, bool(include_authorized_operations),
                    bool(include_fenced_brokers), cb),
                self._resolve_value(drain, _to_cluster_description),
                self._free_value(drain))

    def _describe_configs_spec(self, resources, timeout, include_synonyms,
                               include_documentation):
        spec = [(int(r.resource_type), str(r.name)) for r in resources]
        ms = _ms(timeout)
        drain = _lib.DescribeConfigsResult_drain
        return (lambda cb: _lib.Admin_describe_configs_async(
                    self._h, spec, ms, bool(include_synonyms),
                    bool(include_documentation), cb),
                self._resolve_value(drain, _to_describe_configs),
                self._free_value(drain))

    def _incremental_alter_configs_spec(self, configs, timeout, validate_only):
        # Java's Map<ConfigResource, Collection<AlterConfigOp>> flattens to one
        # row per operation; the Rust side regroups them by resource.
        spec = [(int(resource.resource_type), str(resource.name),
                 str(op.config_entry.name),
                 None if op.config_entry.value is None else str(op.config_entry.value),
                 int(op.op_type))
                for resource, ops in configs.items() for op in ops]
        ms = _ms(timeout)
        drain = _lib.AlterConfigsResult_drain
        return (lambda cb: _lib.Admin_incremental_alter_configs_async(
                    self._h, spec, ms, bool(validate_only), cb),
                self._resolve_value(drain, _to_alter_configs),
                self._free_value(drain))

    def _list_config_resources_spec(self, resource_types, timeout):
        types = [] if resource_types is None else [int(t) for t in resource_types]
        ms = _ms(timeout)
        drain = _lib.ListConfigResourcesResult_drain
        return (lambda cb: _lib.Admin_list_config_resources_async(self._h, types, ms, cb),
                self._resolve_value(drain, _to_config_resources),
                self._free_value(drain))

    def _list_client_metrics_resources_spec(self, timeout):
        ms = _ms(timeout)
        drain = _lib.ListClientMetricsResourcesResult_drain
        return (lambda cb: _lib.Admin_list_client_metrics_resources_async(self._h, ms, cb),
                self._resolve_value(drain, _to_client_metrics_resources),
                self._free_value(drain))

    def _describe_log_dirs_spec(self, brokers, timeout):
        ids = [int(b) for b in brokers]
        ms = _ms(timeout)
        drain = _lib.DescribeLogDirsResult_drain
        return (lambda cb: _lib.Admin_describe_log_dirs_async(self._h, ids, ms, cb),
                self._resolve_value(drain, _to_describe_log_dirs),
                self._free_value(drain))

    def _alter_replica_log_dirs_spec(self, replica_assignment, timeout):
        spec = [(str(r.topic), int(r.partition), int(r.broker_id), str(log_dir))
                for r, log_dir in replica_assignment.items()]
        ms = _ms(timeout)
        drain = _lib.AlterReplicaLogDirsResult_drain
        return (lambda cb: _lib.Admin_alter_replica_log_dirs_async(self._h, spec, ms, cb),
                self._resolve_value(drain, _to_alter_replica_log_dirs),
                self._free_value(drain))

    def _describe_replica_log_dirs_spec(self, replicas, timeout):
        spec = [(str(r.topic), int(r.partition), int(r.broker_id)) for r in replicas]
        ms = _ms(timeout)
        drain = _lib.DescribeReplicaLogDirsResult_drain
        return (lambda cb: _lib.Admin_describe_replica_log_dirs_async(self._h, spec, ms, cb),
                self._resolve_value(drain, _to_describe_replica_log_dirs),
                self._free_value(drain))

    def _elect_leaders_spec(self, election_type, partitions, timeout):
        # `partitions is None` is Java's null Set: elect for every partition.
        # It crosses as an explicit flag so it cannot be confused with an empty
        # selection (`Admin.java:1096-1097`).
        all_partitions = partitions is None
        spec = [] if all_partitions else [(str(t), int(p)) for t, p in partitions]
        ms = _ms(timeout)
        drain = _lib.ElectLeadersResult_drain
        return (lambda cb: _lib.Admin_elect_leaders_async(
                    self._h, int(election_type), all_partitions, spec, ms, cb),
                self._resolve_value(drain, _to_elect_leaders),
                self._free_value(drain))

    def _alter_partition_reassignments_spec(self, reassignments, timeout,
                                            allow_replication_factor_change):
        # A None value is Java's empty Optional, which *reverts* the
        # reassignment; it crosses as a separate flag so it stays distinct from
        # an empty replica list, which Java rejects.
        spec = [(str(topic), int(partition), r is None,
                 [] if r is None else [int(x) for x in r.target_replicas])
                for (topic, partition), r in reassignments.items()]
        ms = _ms(timeout)
        drain = _lib.AlterPartitionReassignmentsResult_drain
        return (lambda cb: _lib.Admin_alter_partition_reassignments_async(
                    self._h, spec, ms, bool(allow_replication_factor_change), cb),
                self._resolve_value(drain, _to_alter_partition_reassignments),
                self._free_value(drain))

    def _list_partition_reassignments_spec(self, partitions, timeout):
        # `partitions is None` is Java's Optional.empty(): list everything.
        all_partitions = partitions is None
        spec = [] if all_partitions else [(str(t), int(p)) for t, p in partitions]
        ms = _ms(timeout)
        drain = _lib.ListPartitionReassignmentsResult_drain
        return (lambda cb: _lib.Admin_list_partition_reassignments_async(
                    self._h, all_partitions, spec, ms, cb),
                self._resolve_value(drain, _to_list_partition_reassignments),
                self._free_value(drain))

    def _list_offsets_spec(self, topic_partition_offsets, timeout, isolation_level):
        spec = [(str(topic), int(partition), spec_.is_timestamp, int(spec_.value))
                for (topic, partition), spec_ in topic_partition_offsets.items()]
        ms = _ms(timeout)
        drain = _lib.ListOffsetsResult_drain
        return (lambda cb: _lib.Admin_list_offsets_async(
                    self._h, spec, ms, int(isolation_level), cb),
                self._resolve_value(drain, _to_list_offsets),
                self._free_value(drain))

    def _describe_topics_spec(self, topics, timeout, include_authorized_operations,
                              partition_size_limit, by_ids):
        names = [str(t) for t in topics]
        ms = _ms(timeout)
        limit = -1 if partition_size_limit is None else int(partition_size_limit)
        fn = (_lib.Admin_describe_topics_by_ids_async if by_ids
              else _lib.Admin_describe_topics_async)
        drain = _lib.DescribeTopicsResult_drain
        return (lambda cb: fn(self._h, names, ms,
                              bool(include_authorized_operations), limit, cb),
                self._resolve_value(drain, _to_describe_topics),
                self._free_value(drain))


class _MockAdminClientMixin:
    """Mock-only operations (test helper)."""

    def timeout_next_request(self, number_of_requests):
        """Make the next ``number_of_requests`` RPCs fail with a timeout
        (Java ``MockAdminClient.timeoutNextRequest``)."""
        e = _lib.MockAdminClient_timeout_next_request(self._h, number_of_requests)
        if e:
            raise KafkaError._from_c(e)

    def update_beginning_offsets(self, offsets):
        """Seed the offsets ``list_offsets`` reports for
        ``OffsetSpec.earliest()``, from ``{(topic, partition): offset}``
        (Java ``MockAdminClient.updateBeginningOffsets``). Merges into, rather
        than replaces, what was seeded before."""
        spec = [(str(t), int(p), int(o)) for (t, p), o in offsets.items()]
        e = _lib.MockAdminClient_update_beginning_offsets(self._h, spec)
        if e:
            raise KafkaError._from_c(e)

    def update_end_offsets(self, offsets):
        """Seed the offsets ``list_offsets`` reports for every ``OffsetSpec``
        other than ``earliest()`` and ``for_timestamp()``, from
        ``{(topic, partition): offset}`` (Java
        ``MockAdminClient.updateEndOffsets``). Merges into, rather than
        replaces, what was seeded before."""
        spec = [(str(t), int(p), int(o)) for (t, p), o in offsets.items()]
        e = _lib.MockAdminClient_update_end_offsets(self._h, spec)
        if e:
            raise KafkaError._from_c(e)


# --------------------------------------------------------------------------
# Synchronous API.
# --------------------------------------------------------------------------
class Admin(_AdminBase):
    """A synchronous Kafka admin client. Every RPC submits an async FFI op and
    waits on an interruptible event, so ``KeyboardInterrupt`` is honored
    promptly (though the request itself is not cancelled — see module
    docstring)."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _run_sync(self, submit, resolve, free):
        box = {}
        done = threading.Event()

        def cb(*payload):
            box["payload"] = payload
            done.set()

        submit(cb)
        interrupted = None
        while True:
            try:
                # Short slices keep the main-thread eval loop reachable so a
                # pending signal raises KeyboardInterrupt here rather than after
                # the whole op completes.
                while not done.wait(0.1):
                    pass
                break
            except KeyboardInterrupt as exc:
                interrupted = exc
                # There is no admin wakeup(): keep waiting for the callback so
                # the result handles are freed rather than leaked.
        payload = box["payload"]
        if interrupted is not None:
            free(payload)
            raise interrupted
        return resolve(payload)

    def create_topics(self, new_topics, timeout=None, validate_only=False,
                      retry_on_quota_violation=True):
        """Create topics. Returns
        ``{topic_name: TopicMetadataAndConfig | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._create_topics_spec(
            new_topics, timeout, validate_only, retry_on_quota_violation))

    def delete_topics(self, topics, timeout=None, retry_on_quota_violation=True):
        """Delete topics by name. Returns ``{topic_name: None | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._delete_topics_spec(
            topics, timeout, retry_on_quota_violation, by_ids=False))

    def delete_topics_by_ids(self, topic_ids, timeout=None,
                             retry_on_quota_violation=True):
        """Delete topics by base64 topic id. Returns
        ``{topic_id: None | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._delete_topics_spec(
            topic_ids, timeout, retry_on_quota_violation, by_ids=True))

    def list_topics(self, timeout=None, list_internal=False):
        """List topics. Returns ``{topic_name: TopicListing}``."""
        self._check_closed()
        return self._run_sync(*self._list_topics_spec(timeout, list_internal))

    def describe_topics(self, topics, timeout=None,
                        include_authorized_operations=False,
                        partition_size_limit=None):
        """Describe topics by name. Returns
        ``{topic_name: TopicDescription | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._describe_topics_spec(
            topics, timeout, include_authorized_operations, partition_size_limit,
            by_ids=False))

    def describe_topics_by_ids(self, topic_ids, timeout=None,
                               include_authorized_operations=False,
                               partition_size_limit=None):
        """Describe topics by base64 topic id. Returns
        ``{topic_id: TopicDescription | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._describe_topics_spec(
            topic_ids, timeout, include_authorized_operations, partition_size_limit,
            by_ids=True))

    def create_partitions(self, new_partitions, timeout=None, validate_only=False,
                          retry_on_quota_violation=True):
        """Increase the partition counts of ``{topic_name: NewPartitions}``.
        Returns ``{topic_name: None | KafkaError}`` (``None`` means success)."""
        self._check_closed()
        return self._run_sync(*self._create_partitions_spec(
            new_partitions, timeout, validate_only, retry_on_quota_violation))

    def delete_records(self, records_to_delete, timeout=None):
        """Delete records before the given offsets of
        ``{(topic, partition): RecordsToDelete}``. Returns
        ``{(topic, partition): DeletedRecords | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._delete_records_spec(records_to_delete, timeout))

    def describe_cluster(self, timeout=None, include_authorized_operations=False,
                         include_fenced_brokers=False):
        """Describe the cluster. Returns a :class:`ClusterDescription`.

        Java's result holds four independent futures rather than a per-key map,
        so any failure raises instead of appearing per key.
        """
        self._check_closed()
        return self._run_sync(*self._describe_cluster_spec(
            timeout, include_authorized_operations, include_fenced_brokers))

    def describe_configs(self, resources, timeout=None, include_synonyms=False,
                         include_documentation=False):
        """Describe the configuration of ``resources`` (:class:`ConfigResource`).
        Returns ``{ConfigResource: Config | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._describe_configs_spec(
            resources, timeout, include_synonyms, include_documentation))

    def incremental_alter_configs(self, configs, timeout=None, validate_only=False):
        """Incrementally alter ``{ConfigResource: [AlterConfigOp]}``. Returns
        ``{ConfigResource: None | KafkaError}`` (``None`` means success)."""
        self._check_closed()
        return self._run_sync(*self._incremental_alter_configs_spec(
            configs, timeout, validate_only))

    def list_config_resources(self, resource_types=None, timeout=None):
        """List the cluster's config resources whose type is in
        ``resource_types`` (:class:`ConfigResourceType` values); ``None`` or an
        empty sequence means every supported type. Returns
        ``[ConfigResource]``."""
        self._check_closed()
        return self._run_sync(*self._list_config_resources_spec(resource_types, timeout))

    def list_client_metrics_resources(self, timeout=None):
        """List the cluster's client-metrics resources. Returns
        ``[ClientMetricsResourceListing]``.

        Deprecated in Java since 4.1 in favour of
        ``list_config_resources([ConfigResourceType.CLIENT_METRICS])``; exposed
        for parity.
        """
        self._check_closed()
        return self._run_sync(*self._list_client_metrics_resources_spec(timeout))

    def describe_log_dirs(self, brokers, timeout=None):
        """Query the log directories of ``brokers``. Returns
        ``{broker_id: {log_dir: LogDirDescription} | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._describe_log_dirs_spec(brokers, timeout))

    def alter_replica_log_dirs(self, replica_assignment, timeout=None):
        """Move ``{TopicPartitionReplica: log_dir}`` to new log directories.
        Returns ``{TopicPartitionReplica: None | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._alter_replica_log_dirs_spec(
            replica_assignment, timeout))

    def describe_replica_log_dirs(self, replicas, timeout=None):
        """Query the log directories of ``replicas``
        (:class:`TopicPartitionReplica`). Returns
        ``{TopicPartitionReplica: ReplicaLogDirInfo | KafkaError}``.

        Replicas of a topic the cluster does not know are omitted, so the result
        can be smaller than the request.
        """
        self._check_closed()
        return self._run_sync(*self._describe_replica_log_dirs_spec(replicas, timeout))

    def elect_leaders(self, election_type, partitions=None, timeout=None):
        """Elect leaders for ``partitions`` (an iterable of ``(topic,
        partition)``), or for **every** partition when ``partitions`` is
        ``None`` — Java's null ``Set``. Returns
        ``{(topic, partition): None | KafkaError}``, where ``None`` means the
        election succeeded for that partition.
        """
        self._check_closed()
        return self._run_sync(*self._elect_leaders_spec(election_type, partitions, timeout))

    def alter_partition_reassignments(self, reassignments, timeout=None,
                                      allow_replication_factor_change=True):
        """Apply ``{(topic, partition): NewPartitionReassignment | None}``. A
        ``None`` value **reverts** that partition's reassignment (Java's empty
        ``Optional``). Returns ``{(topic, partition): None | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._alter_partition_reassignments_spec(
            reassignments, timeout, allow_replication_factor_change))

    def list_partition_reassignments(self, partitions=None, timeout=None):
        """List ongoing reassignments, restricted to ``partitions`` (an
        iterable of ``(topic, partition)``) or over the whole cluster when it is
        ``None`` — Java's ``Optional.empty()``. Returns
        ``{(topic, partition): PartitionReassignment}``; partitions with no
        ongoing reassignment are omitted, so the result can be smaller than the
        request."""
        self._check_closed()
        return self._run_sync(*self._list_partition_reassignments_spec(partitions, timeout))

    def list_offsets(self, topic_partition_offsets, timeout=None,
                     isolation_level=IsolationLevel.READ_UNCOMMITTED):
        """Look up ``{(topic, partition): OffsetSpec}``. Returns
        ``{(topic, partition): ListOffsetsResultInfo | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._list_offsets_spec(
            topic_partition_offsets, timeout, isolation_level))

    def close(self, timeout=None):
        if self.closed:
            return
        self.closed = True
        try:
            self._run_sync(*self._close_spec(timeout))
        finally:
            self._destroy()


# --------------------------------------------------------------------------
# Asyncio-native API.
# --------------------------------------------------------------------------
class AsyncAdmin(_AdminBase):
    """An asyncio-native Kafka admin client. Every RPC is a coroutine that
    submits an async FFI op and ``await``s its completion on the event loop."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    @staticmethod
    def _deliver(fut, payload, free):
        # Runs on the event loop thread.
        if fut.cancelled() or fut.done():
            free(payload)
            return
        fut.set_result(payload)

    async def _run_async(self, submit, resolve, free):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def cb(*payload):
            # Runs on the Rust dispatcher thread with the GIL held. asyncio
            # futures must be touched only on the loop thread.
            if loop.is_closed():
                free(payload)
                return
            loop.call_soon_threadsafe(self._deliver, fut, payload, free)

        submit(cb)
        # On cancellation the late callback frees the handles via _deliver (the
        # future is cancelled by then). There is no admin wakeup(), so the
        # request itself keeps running — as in Java.
        payload = await fut
        return resolve(payload)

    async def create_topics(self, new_topics, timeout=None, validate_only=False,
                            retry_on_quota_violation=True):
        self._check_closed()
        return await self._run_async(*self._create_topics_spec(
            new_topics, timeout, validate_only, retry_on_quota_violation))

    async def delete_topics(self, topics, timeout=None, retry_on_quota_violation=True):
        self._check_closed()
        return await self._run_async(*self._delete_topics_spec(
            topics, timeout, retry_on_quota_violation, by_ids=False))

    async def delete_topics_by_ids(self, topic_ids, timeout=None,
                                   retry_on_quota_violation=True):
        self._check_closed()
        return await self._run_async(*self._delete_topics_spec(
            topic_ids, timeout, retry_on_quota_violation, by_ids=True))

    async def list_topics(self, timeout=None, list_internal=False):
        self._check_closed()
        return await self._run_async(*self._list_topics_spec(timeout, list_internal))

    async def describe_topics(self, topics, timeout=None,
                              include_authorized_operations=False,
                              partition_size_limit=None):
        self._check_closed()
        return await self._run_async(*self._describe_topics_spec(
            topics, timeout, include_authorized_operations, partition_size_limit,
            by_ids=False))

    async def describe_topics_by_ids(self, topic_ids, timeout=None,
                                     include_authorized_operations=False,
                                     partition_size_limit=None):
        self._check_closed()
        return await self._run_async(*self._describe_topics_spec(
            topic_ids, timeout, include_authorized_operations, partition_size_limit,
            by_ids=True))

    async def create_partitions(self, new_partitions, timeout=None, validate_only=False,
                                retry_on_quota_violation=True):
        self._check_closed()
        return await self._run_async(*self._create_partitions_spec(
            new_partitions, timeout, validate_only, retry_on_quota_violation))

    async def delete_records(self, records_to_delete, timeout=None):
        self._check_closed()
        return await self._run_async(*self._delete_records_spec(records_to_delete, timeout))

    async def describe_cluster(self, timeout=None, include_authorized_operations=False,
                               include_fenced_brokers=False):
        self._check_closed()
        return await self._run_async(*self._describe_cluster_spec(
            timeout, include_authorized_operations, include_fenced_brokers))

    async def describe_configs(self, resources, timeout=None, include_synonyms=False,
                               include_documentation=False):
        self._check_closed()
        return await self._run_async(*self._describe_configs_spec(
            resources, timeout, include_synonyms, include_documentation))

    async def incremental_alter_configs(self, configs, timeout=None, validate_only=False):
        self._check_closed()
        return await self._run_async(*self._incremental_alter_configs_spec(
            configs, timeout, validate_only))

    async def list_config_resources(self, resource_types=None, timeout=None):
        self._check_closed()
        return await self._run_async(*self._list_config_resources_spec(
            resource_types, timeout))

    async def list_client_metrics_resources(self, timeout=None):
        self._check_closed()
        return await self._run_async(*self._list_client_metrics_resources_spec(timeout))

    async def describe_log_dirs(self, brokers, timeout=None):
        self._check_closed()
        return await self._run_async(*self._describe_log_dirs_spec(brokers, timeout))

    async def alter_replica_log_dirs(self, replica_assignment, timeout=None):
        self._check_closed()
        return await self._run_async(*self._alter_replica_log_dirs_spec(
            replica_assignment, timeout))

    async def describe_replica_log_dirs(self, replicas, timeout=None):
        self._check_closed()
        return await self._run_async(*self._describe_replica_log_dirs_spec(
            replicas, timeout))

    async def elect_leaders(self, election_type, partitions=None, timeout=None):
        self._check_closed()
        return await self._run_async(*self._elect_leaders_spec(
            election_type, partitions, timeout))

    async def alter_partition_reassignments(self, reassignments, timeout=None,
                                            allow_replication_factor_change=True):
        self._check_closed()
        return await self._run_async(*self._alter_partition_reassignments_spec(
            reassignments, timeout, allow_replication_factor_change))

    async def list_partition_reassignments(self, partitions=None, timeout=None):
        self._check_closed()
        return await self._run_async(*self._list_partition_reassignments_spec(
            partitions, timeout))

    async def list_offsets(self, topic_partition_offsets, timeout=None,
                           isolation_level=IsolationLevel.READ_UNCOMMITTED):
        self._check_closed()
        return await self._run_async(*self._list_offsets_spec(
            topic_partition_offsets, timeout, isolation_level))

    async def close(self, timeout=None):
        if self.closed:
            return
        self.closed = True
        try:
            await self._run_async(*self._close_spec(timeout))
        finally:
            self._destroy()


# --------------------------------------------------------------------------
# Concrete variants.
# --------------------------------------------------------------------------
class AdminClient(Admin):
    """A synchronous admin client connected to a real cluster.

    Args:
        config: dict of configuration properties (e.g. ``bootstrap.servers``).
    """

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class MockAdminClient(_MockAdminClientMixin, Admin):
    """A synchronous, broker-less admin client for tests (Java
    ``MockAdminClient``)."""

    def __init__(self, num_brokers=1):
        super().__init__()
        self._init_mock(num_brokers)


class AsyncAdminClient(AsyncAdmin):
    """An asyncio-native admin client connected to a real cluster."""

    def __init__(self, config):
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config must be a dict")
        self._init_kafka(config)


class AsyncMockAdminClient(_MockAdminClientMixin, AsyncAdmin):
    """An asyncio-native, broker-less admin client for tests."""

    def __init__(self, num_brokers=1):
        super().__init__()
        self._init_mock(num_brokers)
