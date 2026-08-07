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
* ``describe_consumer_groups`` / ``describe_classic_groups`` ->
  ``{group_id: ConsumerGroupDescription | ClassicGroupDescription | KafkaError}``
* ``list_consumer_group_offsets`` ->
  ``{group_id: {(topic, partition): OffsetAndMetadata | None} | KafkaError}``
  (an inner ``None`` is Java's null map value: no committed offset for that
  partition, which is not a committed offset of 0)
* ``alter_consumer_group_offsets`` / ``delete_consumer_group_offsets`` ->
  ``{(topic, partition): None | KafkaError}``, ``delete_consumer_groups`` ->
  ``{group_id: None | KafkaError}`` and ``remove_members_from_consumer_group``
  -> ``{group_instance_id: None | KafkaError}`` (all per-key
  ``KafkaFuture<Void>``, so ``None`` means success)
* ``list_groups`` -> ``([GroupListing], [KafkaError])`` and
  ``list_consumer_groups`` -> ``([ConsumerGroupListing], [KafkaError])`` —
  Java splits one future into ``valid()`` and an *unkeyed* ``errors()``
  collection, so these are two independent lists, not a dict and not parallel
  arrays
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
from consumer import Node, OffsetAndMetadata  # shared broker-node / committed-offset types
# `OffsetAndMetadata` is `org.apache.kafka.clients.consumer` in Java, so the
# consumer module owns it and the admin group-offset RPCs reuse it, exactly as
# they already reuse `Node`.
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
class GroupListing:
    """A group as ``list_groups`` reports it (Java ``GroupListing``).

    ``group_type`` and ``group_state`` are the Java enums' ``toString()``
    names (``"Consumer"``, ``"Classic"``, ``"Stable"``, ...) and are ``None``
    for Java's empty ``Optional`` — neither enum has a numeric id, so the name
    is the contract. ``protocol`` is the lower-case wire protocol-type string
    and is unrelated to ``group_type``.
    """

    __slots__ = ("group_id", "group_type", "protocol", "group_state",
                 "is_simple_consumer_group")

    def __init__(self, group_id, group_type, protocol, group_state,
                 is_simple_consumer_group):
        self.group_id = group_id
        self.group_type = group_type
        self.protocol = protocol
        self.group_state = group_state
        self.is_simple_consumer_group = is_simple_consumer_group

    def __eq__(self, other):
        return (isinstance(other, GroupListing)
                and self.group_id == other.group_id
                and self.group_type == other.group_type
                and self.protocol == other.protocol
                and self.group_state == other.group_state
                and self.is_simple_consumer_group == other.is_simple_consumer_group)

    def __repr__(self):
        return (f"GroupListing(group_id={self.group_id!r}, "
                f"group_type={self.group_type!r}, protocol={self.protocol!r}, "
                f"group_state={self.group_state!r})")


class ConsumerGroupListing:
    """A consumer group as ``list_consumer_groups`` reports it (Java
    ``ConsumerGroupListing``, deprecated since 4.1 in favour of
    :class:`GroupListing`).

    ``state`` is Java's deprecated ``state()``, i.e. ``group_state`` mapped
    through ``ConsumerGroupState.parse``; both are ``None`` for an empty
    ``Optional``.
    """

    __slots__ = ("group_id", "is_simple_consumer_group", "group_state", "state",
                 "group_type")

    def __init__(self, group_id, is_simple_consumer_group, group_state, state, group_type):
        self.group_id = group_id
        self.is_simple_consumer_group = is_simple_consumer_group
        self.group_state = group_state
        self.state = state
        self.group_type = group_type

    def __eq__(self, other):
        return (isinstance(other, ConsumerGroupListing)
                and self.group_id == other.group_id
                and self.is_simple_consumer_group == other.is_simple_consumer_group
                and self.group_state == other.group_state
                and self.state == other.state
                and self.group_type == other.group_type)

    def __repr__(self):
        return (f"ConsumerGroupListing(group_id={self.group_id!r}, "
                f"group_state={self.group_state!r}, group_type={self.group_type!r})")


class MemberAssignment:
    """The partitions assigned to one group member (Java
    ``MemberAssignment``). ``topic_partitions`` is a list of
    ``(topic, partition)``, sorted."""

    __slots__ = ("topic_partitions",)

    def __init__(self, topic_partitions):
        self.topic_partitions = topic_partitions

    def __eq__(self, other):
        return (isinstance(other, MemberAssignment)
                and self.topic_partitions == other.topic_partitions)

    def __repr__(self):
        return f"MemberAssignment(topic_partitions={self.topic_partitions!r})"


class MemberDescription:
    """One member of a described group (Java ``MemberDescription``).

    ``group_instance_id``, ``rack_id``, ``member_epoch`` and ``upgraded`` are
    ``None`` for Java's empty ``Optional``. ``target_assignment`` is ``None``
    when Java reported none at all, which is distinct from a
    :class:`MemberAssignment` holding no partitions; ``assignment`` is never
    ``None``.
    """

    __slots__ = ("consumer_id", "group_instance_id", "rack_id", "client_id", "host",
                 "assignment", "target_assignment", "member_epoch", "upgraded")

    def __init__(self, consumer_id, group_instance_id, rack_id, client_id, host,
                 assignment, target_assignment, member_epoch, upgraded):
        self.consumer_id = consumer_id
        self.group_instance_id = group_instance_id
        self.rack_id = rack_id
        self.client_id = client_id
        self.host = host
        self.assignment = assignment
        self.target_assignment = target_assignment
        self.member_epoch = member_epoch
        self.upgraded = upgraded

    def __repr__(self):
        return (f"MemberDescription(consumer_id={self.consumer_id!r}, "
                f"client_id={self.client_id!r}, host={self.host!r}, "
                f"assignment={self.assignment!r})")


class ConsumerGroupDescription:
    """A described consumer group (Java ``ConsumerGroupDescription``).

    ``group_type``, ``state`` and ``group_state`` are the Java enums'
    ``toString()`` names; ``state`` is Java's deprecated ``state()``.
    ``coordinator`` is a :class:`Node` or ``None``, ``authorized_operations``
    holds ``AclOperation`` wire codes (empty when the request did not ask for
    them), and ``group_epoch`` / ``target_assignment_epoch`` are ``None`` for a
    classic group.
    """

    __slots__ = ("group_id", "is_simple_consumer_group", "members", "partition_assignor",
                 "group_type", "state", "group_state", "coordinator",
                 "authorized_operations", "group_epoch", "target_assignment_epoch")

    def __init__(self, group_id, is_simple_consumer_group, members, partition_assignor,
                 group_type, state, group_state, coordinator, authorized_operations,
                 group_epoch, target_assignment_epoch):
        self.group_id = group_id
        self.is_simple_consumer_group = is_simple_consumer_group
        self.members = members
        self.partition_assignor = partition_assignor
        self.group_type = group_type
        self.state = state
        self.group_state = group_state
        self.coordinator = coordinator
        self.authorized_operations = authorized_operations
        self.group_epoch = group_epoch
        self.target_assignment_epoch = target_assignment_epoch

    def __repr__(self):
        return (f"ConsumerGroupDescription(group_id={self.group_id!r}, "
                f"group_state={self.group_state!r}, members={len(self.members)})")


class ClassicGroupDescription:
    """A described classic group (Java ``ClassicGroupDescription``).

    ``protocol`` is the group's protocol type and ``protocol_data`` the
    assignment strategy it selected; ``state`` is the ``ClassicGroupState``
    name.
    """

    __slots__ = ("group_id", "protocol", "protocol_data", "is_simple_consumer_group",
                 "members", "state", "coordinator", "authorized_operations")

    def __init__(self, group_id, protocol, protocol_data, is_simple_consumer_group,
                 members, state, coordinator, authorized_operations):
        self.group_id = group_id
        self.protocol = protocol
        self.protocol_data = protocol_data
        self.is_simple_consumer_group = is_simple_consumer_group
        self.members = members
        self.state = state
        self.coordinator = coordinator
        self.authorized_operations = authorized_operations

    def __repr__(self):
        return (f"ClassicGroupDescription(group_id={self.group_id!r}, "
                f"protocol={self.protocol!r}, state={self.state!r}, "
                f"members={len(self.members)})")


class ListConsumerGroupOffsetsSpec:
    """Which partitions to list offsets for, per group (Java
    ``ListConsumerGroupOffsetsSpec``).

    ``topic_partitions`` is an iterable of ``(topic, partition)``, or ``None``
    for Java's unset collection: every partition the group has committed
    offsets for.
    """

    __slots__ = ("topic_partitions",)

    def __init__(self, topic_partitions=None):
        self.topic_partitions = topic_partitions

    def __repr__(self):
        return f"ListConsumerGroupOffsetsSpec(topic_partitions={self.topic_partitions!r})"


class MemberToRemove:
    """A static group member to remove, by ``group.instance.id`` (Java
    ``MemberToRemove``)."""

    __slots__ = ("group_instance_id",)

    def __init__(self, group_instance_id):
        self.group_instance_id = group_instance_id

    def __eq__(self, other):
        return (isinstance(other, MemberToRemove)
                and self.group_instance_id == other.group_instance_id)

    def __hash__(self):
        return hash(self.group_instance_id)

    def __repr__(self):
        return f"MemberToRemove(group_instance_id={self.group_instance_id!r})"


class ScramMechanism:
    """SASL/SCRAM mechanisms (Java ``ScramMechanism``).

    The values are Java's ``ScramMechanism.type()`` wire indicators. ``UNKNOWN``
    is what an unrecognised indicator decodes to, mirroring Java's ``fromType``;
    the broker rejects it.
    """

    UNKNOWN = 0
    SCRAM_SHA_256 = 1
    SCRAM_SHA_512 = 2

    _NAMES = {0: "UNKNOWN", 1: "SCRAM-SHA-256", 2: "SCRAM-SHA-512"}

    @staticmethod
    def mechanism_name(mechanism):
        """The SASL mechanism name for a type indicator (Java
        ``mechanismName()``)."""
        return ScramMechanism._NAMES.get(int(mechanism), "UNKNOWN")


class ScramCredentialInfo:
    """A SCRAM mechanism and its iteration count (Java
    ``ScramCredentialInfo``)."""

    __slots__ = ("mechanism", "iterations")

    def __init__(self, mechanism, iterations):
        self.mechanism = int(mechanism)
        self.iterations = int(iterations)

    def __eq__(self, other):
        return (isinstance(other, ScramCredentialInfo)
                and self.mechanism == other.mechanism and self.iterations == other.iterations)

    def __hash__(self):
        return hash((self.mechanism, self.iterations))

    def __repr__(self):
        return (f"ScramCredentialInfo(mechanism={ScramMechanism.mechanism_name(self.mechanism)}, "
                f"iterations={self.iterations})")


class UserScramCredentialUpsertion:
    """A request to insert or update a user's SCRAM credential (Java
    ``UserScramCredentialUpsertion``).

    ``password`` is raw ``bytes``; a ``str`` is encoded as UTF-8, matching
    Java's ``(String user, ScramCredentialInfo, String password)`` constructor.
    Leave ``salt`` as ``None`` to have the client generate one, which selects
    Java's three-argument constructor.
    """

    __slots__ = ("user", "credential_info", "password", "salt")

    def __init__(self, user, credential_info, password, salt=None):
        self.user = str(user)
        self.credential_info = credential_info
        self.password = password.encode("utf-8") if isinstance(password, str) else bytes(password)
        self.salt = None if salt is None else bytes(salt)

    def __repr__(self):
        return (f"UserScramCredentialUpsertion(user={self.user!r}, "
                f"credential_info={self.credential_info!r})")


class UserScramCredentialDeletion:
    """A request to delete a user's SCRAM credential for one mechanism (Java
    ``UserScramCredentialDeletion``)."""

    __slots__ = ("user", "mechanism")

    def __init__(self, user, mechanism):
        self.user = str(user)
        self.mechanism = int(mechanism)

    def __repr__(self):
        return (f"UserScramCredentialDeletion(user={self.user!r}, "
                f"mechanism={ScramMechanism.mechanism_name(self.mechanism)})")


class UserScramCredentialsDescription:
    """A user's SCRAM credentials (Java ``UserScramCredentialsDescription``).

    A user the broker reports as having no credential is described with an empty
    ``credential_infos``, not as an error -- Java's ``all()`` treats
    ``RESOURCE_NOT_FOUND`` the same way.
    """

    __slots__ = ("name", "credential_infos")

    def __init__(self, name, credential_infos):
        self.name = str(name)
        self.credential_infos = list(credential_infos)

    def __eq__(self, other):
        return (isinstance(other, UserScramCredentialsDescription)
                and self.name == other.name and self.credential_infos == other.credential_infos)

    def __repr__(self):
        return (f"UserScramCredentialsDescription(name={self.name!r}, "
                f"credential_infos={self.credential_infos!r})")


class KafkaPrincipal:
    """A Kafka principal (Java ``KafkaPrincipal``), e.g. ``User:alice``."""

    USER_TYPE = "User"

    __slots__ = ("principal_type", "name", "token_authenticated")

    def __init__(self, principal_type, name, token_authenticated=False):
        self.principal_type = str(principal_type)
        self.name = str(name)
        self.token_authenticated = bool(token_authenticated)

    def __eq__(self, other):
        # Java's `equals` compares only the type and the name;
        # `tokenAuthenticated` is deliberately excluded.
        return (isinstance(other, KafkaPrincipal)
                and self.principal_type == other.principal_type and self.name == other.name)

    def __hash__(self):
        return hash((self.principal_type, self.name))

    def __str__(self):
        return f"{self.principal_type}:{self.name}"

    def __repr__(self):
        return (f"KafkaPrincipal(principal_type={self.principal_type!r}, name={self.name!r}, "
                f"token_authenticated={self.token_authenticated})")


class TokenInformation:
    """A delegation token's metadata (Java ``TokenInformation``).

    ``token_requester`` is the principal that asked for the token, which differs
    from ``owner`` when a superuser creates one on another principal's behalf
    (KIP-373). All three timestamps are milliseconds since the epoch.
    """

    __slots__ = ("token_id", "owner", "token_requester", "renewers", "issue_timestamp",
                 "expiry_timestamp", "max_timestamp")

    def __init__(self, token_id, owner, token_requester, renewers, issue_timestamp,
                 expiry_timestamp, max_timestamp):
        self.token_id = str(token_id)
        self.owner = owner
        self.token_requester = token_requester
        self.renewers = list(renewers)
        self.issue_timestamp = int(issue_timestamp)
        self.expiry_timestamp = int(expiry_timestamp)
        self.max_timestamp = int(max_timestamp)

    def __repr__(self):
        return (f"TokenInformation(token_id={self.token_id!r}, owner={self.owner!r}, "
                f"renewers={self.renewers!r}, expiry_timestamp={self.expiry_timestamp})")


class DelegationToken:
    """A delegation token (Java ``DelegationToken``).

    ``hmac`` is raw ``bytes`` -- it is a MAC and can contain NUL, so it is not a
    ``str``. Pass it back verbatim to :meth:`Admin.renew_delegation_token` /
    :meth:`Admin.expire_delegation_token`.
    """

    __slots__ = ("token_info", "hmac", "hmac_as_base64_string")

    def __init__(self, token_info, hmac, hmac_as_base64_string):
        self.token_info = token_info
        self.hmac = bytes(hmac)
        self.hmac_as_base64_string = str(hmac_as_base64_string)

    def __repr__(self):
        return (f"DelegationToken(token_info={self.token_info!r}, "
                f"hmac_as_base64_string={self.hmac_as_base64_string!r})")


class UpgradeType:
    """How a feature update should be applied (Java
    ``FeatureUpdate.UpgradeType``).

    The values are Java's ``code()``. ``UNKNOWN`` marshals fine and is rejected
    by the broker, mirroring Java's ``fromCode``.
    """

    UNKNOWN = 0
    UPGRADE = 1
    SAFE_DOWNGRADE = 2
    UNSAFE_DOWNGRADE = 3


class FeatureUpdate:
    """An update to one finalized feature (Java ``FeatureUpdate``).

    ``max_version_level`` of 0 deletes the finalized feature and must be paired
    with a downgrade ``upgrade_type``; the C layer rejects the combination Java's
    constructor rejects.
    """

    __slots__ = ("max_version_level", "upgrade_type")

    def __init__(self, max_version_level, upgrade_type):
        self.max_version_level = int(max_version_level)
        self.upgrade_type = int(upgrade_type)

    def __eq__(self, other):
        return (isinstance(other, FeatureUpdate)
                and self.max_version_level == other.max_version_level
                and self.upgrade_type == other.upgrade_type)

    def __repr__(self):
        return (f"FeatureUpdate(max_version_level={self.max_version_level}, "
                f"upgrade_type={self.upgrade_type})")


class FinalizedVersionRange:
    """The finalized version range of a feature (Java
    ``FinalizedVersionRange``)."""

    __slots__ = ("min_version_level", "max_version_level")

    def __init__(self, min_version_level, max_version_level):
        self.min_version_level = int(min_version_level)
        self.max_version_level = int(max_version_level)

    def __eq__(self, other):
        return (isinstance(other, FinalizedVersionRange)
                and self.min_version_level == other.min_version_level
                and self.max_version_level == other.max_version_level)

    def __repr__(self):
        return (f"FinalizedVersionRange(min_version_level={self.min_version_level}, "
                f"max_version_level={self.max_version_level})")


class SupportedVersionRange:
    """The version range a feature supports (Java ``SupportedVersionRange``)."""

    __slots__ = ("min_version", "max_version")

    def __init__(self, min_version, max_version):
        self.min_version = int(min_version)
        self.max_version = int(max_version)

    def __eq__(self, other):
        return (isinstance(other, SupportedVersionRange)
                and self.min_version == other.min_version
                and self.max_version == other.max_version)

    def __repr__(self):
        return (f"SupportedVersionRange(min_version={self.min_version}, "
                f"max_version={self.max_version})")


class FeatureMetadata:
    """The cluster's finalized and supported features (Java
    ``FeatureMetadata``).

    ``finalized_features`` and ``supported_features`` are independent maps: they
    need not have the same keys. ``finalized_features_epoch`` is ``None`` when
    the broker reported none -- every integer, 0 included, is a legal epoch, so
    absence cannot be a sentinel.
    """

    __slots__ = ("finalized_features", "finalized_features_epoch", "supported_features")

    def __init__(self, finalized_features, finalized_features_epoch, supported_features):
        self.finalized_features = dict(finalized_features)
        self.finalized_features_epoch = finalized_features_epoch
        self.supported_features = dict(supported_features)

    def __repr__(self):
        return (f"FeatureMetadata(finalized_features={self.finalized_features!r}, "
                f"finalized_features_epoch={self.finalized_features_epoch}, "
                f"supported_features={self.supported_features!r})")


class AclOperation:
    """ACL operations (Java ``AclOperation``).

    The values are Java's ``AclOperation.code()`` wire codes. ``ANY`` is a
    filter-only value: :meth:`Admin.create_acls` rejects it, exactly as Java's
    ``AccessControlEntry`` constructor does.
    """

    UNKNOWN = 0
    ANY = 1
    ALL = 2
    READ = 3
    WRITE = 4
    CREATE = 5
    DELETE = 6
    ALTER = 7
    DESCRIBE = 8
    CLUSTER_ACTION = 9
    DESCRIBE_CONFIGS = 10
    ALTER_CONFIGS = 11
    IDEMPOTENT_WRITE = 12
    CREATE_TOKENS = 13
    DESCRIBE_TOKENS = 14
    TWO_PHASE_COMMIT = 15


class AclPermissionType:
    """ACL permission types (Java ``AclPermissionType``).

    The values are Java's ``AclPermissionType.code()`` wire codes. ``ANY`` is
    filter-only, as for :class:`AclOperation`.
    """

    UNKNOWN = 0
    ANY = 1
    DENY = 2
    ALLOW = 3


class ResourceType:
    """Kinds of resource an ACL can apply to (Java ``ResourceType``).

    The values are Java's ``ResourceType.code()`` wire codes. ``ANY`` is
    filter-only: Java's ``ResourcePattern`` constructor rejects it.
    """

    UNKNOWN = 0
    ANY = 1
    TOPIC = 2
    GROUP = 3
    CLUSTER = 4
    TRANSACTIONAL_ID = 5
    DELEGATION_TOKEN = 6
    USER = 7


class PatternType:
    """How an ACL's resource name is matched (Java ``PatternType``).

    The values are Java's ``PatternType.code()`` wire codes. ``ANY`` and
    ``MATCH`` are filter-only — Java's ``ResourcePattern`` constructor rejects
    both — where ``ANY`` means "any pattern type" and ``MATCH`` selects the
    literal, prefixed and wildcard patterns that would match the name.
    """

    UNKNOWN = 0
    ANY = 1
    MATCH = 2
    LITERAL = 3
    PREFIXED = 4


class AclBinding:
    """An ACL binding: a resource pattern plus an access-control entry (Java
    ``AclBinding``).

    Java nests these as ``pattern()`` and ``entry()``; the seven fields are
    flattened here, keeping Java's field names. Hashable, so it can key the
    :meth:`Admin.create_acls` result dict as in Java.

    All three strings are required. The nullable, "match any" form is
    :class:`AclBindingFilter`.
    """

    __slots__ = ("resource_type", "resource_name", "pattern_type", "principal", "host",
                 "operation", "permission_type")

    def __init__(self, resource_type, resource_name, pattern_type, principal, host, operation,
                 permission_type):
        self.resource_type = int(resource_type)
        self.resource_name = str(resource_name)
        self.pattern_type = int(pattern_type)
        self.principal = str(principal)
        self.host = str(host)
        self.operation = int(operation)
        self.permission_type = int(permission_type)

    def _as_tuple(self):
        return (self.resource_type, self.resource_name, self.pattern_type, self.principal,
                self.host, self.operation, self.permission_type)

    def to_filter(self):
        """The filter that matches exactly this binding (Java ``toFilter()``)."""
        return AclBindingFilter(*self._as_tuple())

    def __eq__(self, other):
        return isinstance(other, AclBinding) and self._as_tuple() == other._as_tuple()

    def __hash__(self):
        return hash(self._as_tuple())

    def __repr__(self):
        return (f"AclBinding(resource_type={self.resource_type}, "
                f"resource_name={self.resource_name!r}, pattern_type={self.pattern_type}, "
                f"principal={self.principal!r}, host={self.host!r}, "
                f"operation={self.operation}, permission_type={self.permission_type})")


class AclBindingFilter:
    """A filter over ACL bindings (Java ``AclBindingFilter``).

    Differs from :class:`AclBinding` in exactly the two ways Java's filter types
    do: ``resource_name``, ``principal`` and ``host`` may be ``None``, meaning
    "match any", and the four enums may take their ``ANY`` value (and
    ``pattern_type`` may be ``MATCH``).

    ``None`` is not the same as ``""``: the empty string filters on the empty
    name. Hashable, so it can key the :meth:`Admin.delete_acls` result dict.
    """

    __slots__ = ("resource_type", "resource_name", "pattern_type", "principal", "host",
                 "operation", "permission_type")

    def __init__(self, resource_type=ResourceType.ANY, resource_name=None,
                 pattern_type=PatternType.ANY, principal=None, host=None,
                 operation=AclOperation.ANY, permission_type=AclPermissionType.ANY):
        self.resource_type = int(resource_type)
        self.resource_name = None if resource_name is None else str(resource_name)
        self.pattern_type = int(pattern_type)
        self.principal = None if principal is None else str(principal)
        self.host = None if host is None else str(host)
        self.operation = int(operation)
        self.permission_type = int(permission_type)

    def _as_tuple(self):
        return (self.resource_type, self.resource_name, self.pattern_type, self.principal,
                self.host, self.operation, self.permission_type)

    def __eq__(self, other):
        return isinstance(other, AclBindingFilter) and self._as_tuple() == other._as_tuple()

    def __hash__(self):
        return hash(self._as_tuple())

    def __repr__(self):
        return (f"AclBindingFilter(resource_type={self.resource_type}, "
                f"resource_name={self.resource_name!r}, pattern_type={self.pattern_type}, "
                f"principal={self.principal!r}, host={self.host!r}, "
                f"operation={self.operation}, permission_type={self.permission_type})")


class DeletedAcl:
    """One ACL a :meth:`Admin.delete_acls` filter matched (Java
    ``DeleteAclsResult.FilterResult``).

    Exactly one of the two is set: ``binding`` when the ACL was deleted,
    ``error`` when the filter matched it but deleting it failed.
    """

    __slots__ = ("binding", "error")

    def __init__(self, binding, error):
        self.binding = binding
        self.error = error

    def __eq__(self, other):
        return (isinstance(other, DeletedAcl)
                and self.binding == other.binding and self.error == other.error)

    def __repr__(self):
        return f"DeletedAcl(binding={self.binding!r}, error={self.error!r})"


class ClientQuotaEntity:
    """A quota entity: entity type -> entity name (Java ``ClientQuotaEntity``).

    ``entries`` maps ``"user"`` / ``"client-id"`` / ``"ip"`` to a name, where a
    ``None`` name is Java's null map value: the **built-in default entity** for
    that type (the ``--entity-default`` of the command-line tools). That is not
    the same as the type being absent from the map, and not the same as the name
    ``""``.

    Hashable, so it can key the client-quota result dicts as in Java.
    """

    USER = "user"
    CLIENT_ID = "client-id"
    IP = "ip"

    __slots__ = ("entries",)

    def __init__(self, entries):
        self.entries = {str(k): (None if v is None else str(v)) for k, v in dict(entries).items()}

    def _as_key(self):
        return tuple(sorted(self.entries.items(), key=lambda kv: kv[0]))

    def __eq__(self, other):
        return isinstance(other, ClientQuotaEntity) and self.entries == other.entries

    def __hash__(self):
        return hash(self._as_key())

    def __repr__(self):
        return f"ClientQuotaEntity(entries={self.entries!r})"


class ClientQuotaFilterComponent:
    """One component of a client-quota filter (Java
    ``ClientQuotaFilterComponent``).

    The match is a genuine tri-state and is built through the three factories
    below rather than by hand, because two of the three carry no name and so
    could not be told apart by the name alone:

    * :meth:`of_entity` — match this entity type with exactly this name.
    * :meth:`of_default_entity` — match the built-in default entity of the type.
    * :meth:`of_entity_type` — match any *named* entity of the type.

    ``match_type`` holds Kafka's own wire constant: 0 EXACT, 1 DEFAULT,
    2 SPECIFIED.
    """

    EXACT = 0
    DEFAULT = 1
    SPECIFIED = 2

    __slots__ = ("entity_type", "match_type", "match_name")

    def __init__(self, entity_type, match_type, match_name):
        self.entity_type = str(entity_type)
        self.match_type = int(match_type)
        self.match_name = None if match_name is None else str(match_name)

    @staticmethod
    def of_entity(entity_type, entity_name):
        """Match ``entity_type`` with exactly ``entity_name`` (Java
        ``ofEntity``)."""
        return ClientQuotaFilterComponent(entity_type, ClientQuotaFilterComponent.EXACT,
                                          entity_name)

    @staticmethod
    def of_default_entity(entity_type):
        """Match the built-in default entity of ``entity_type`` (Java
        ``ofDefaultEntity``)."""
        return ClientQuotaFilterComponent(entity_type, ClientQuotaFilterComponent.DEFAULT, None)

    @staticmethod
    def of_entity_type(entity_type):
        """Match any *named* entity of ``entity_type`` (Java ``ofEntityType``).

        Distinct from :meth:`of_default_entity`, which matches only the
        default; the two differ in both equality and the wire encoding.
        """
        return ClientQuotaFilterComponent(entity_type, ClientQuotaFilterComponent.SPECIFIED, None)

    def _as_tuple(self):
        return (self.entity_type, self.match_type, self.match_name)

    def __eq__(self, other):
        return (isinstance(other, ClientQuotaFilterComponent)
                and self._as_tuple() == other._as_tuple())

    def __hash__(self):
        return hash(self._as_tuple())

    def __repr__(self):
        return (f"ClientQuotaFilterComponent(entity_type={self.entity_type!r}, "
                f"match_type={self.match_type}, match_name={self.match_name!r})")


class ClientQuotaFilter:
    """A filter over client quotas (Java ``ClientQuotaFilter``).

    Built through the three factories, mirroring Java's private constructor.
    """

    __slots__ = ("components", "strict")

    def __init__(self, components, strict):
        self.components = list(components)
        self.strict = bool(strict)

    @staticmethod
    def contains(components):
        """Match entities that have at least these components (Java
        ``contains``)."""
        return ClientQuotaFilter(components, False)

    @staticmethod
    def contains_only(components):
        """Match entities that have exactly these components and no others
        (Java ``containsOnly``)."""
        return ClientQuotaFilter(components, True)

    @staticmethod
    def all():
        """Match every entity (Java ``all()``).

        Note this is *not* ``contains_only([])``, which matches only the entity
        with no components at all.
        """
        return ClientQuotaFilter([], False)

    def __eq__(self, other):
        return (isinstance(other, ClientQuotaFilter)
                and self.components == other.components and self.strict == other.strict)

    def __repr__(self):
        return f"ClientQuotaFilter(components={self.components!r}, strict={self.strict})"


class ClientQuotaOp:
    """One quota change (Java ``ClientQuotaAlteration.Op``).

    ``value`` is ``None`` to **remove** the quota, mirroring Java's nullable
    ``Double``. Every number, 0 included, is a legal quota value, so ``None`` is
    the only way to say "remove".
    """

    __slots__ = ("key", "value")

    def __init__(self, key, value):
        self.key = str(key)
        self.value = None if value is None else float(value)

    def __eq__(self, other):
        return (isinstance(other, ClientQuotaOp)
                and self.key == other.key and self.value == other.value)

    def __repr__(self):
        return f"ClientQuotaOp(key={self.key!r}, value={self.value!r})"


class ClientQuotaAlteration:
    """The quota changes to apply to one entity (Java
    ``ClientQuotaAlteration``)."""

    __slots__ = ("entity", "ops")

    def __init__(self, entity, ops):
        self.entity = entity
        self.ops = list(ops)

    def __eq__(self, other):
        return (isinstance(other, ClientQuotaAlteration)
                and self.entity == other.entity and self.ops == other.ops)

    def __repr__(self):
        return f"ClientQuotaAlteration(entity={self.entity!r}, ops={self.ops!r})"


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


def _to_member_assignment(raw):
    """[(topic, partition)] -> MemberAssignment, or None for Java's absent
    ``Optional`` (which is not the same as an assignment with no partitions)."""
    return None if raw is None else MemberAssignment(list(raw))


def _to_member_description(raw):
    (consumer_id, group_instance_id, rack_id, client_id, host, assignment,
     target_assignment, member_epoch, upgraded) = raw
    return MemberDescription(consumer_id, group_instance_id, rack_id, client_id, host,
                             _to_member_assignment(assignment),
                             _to_member_assignment(target_assignment),
                             member_epoch, upgraded)


def _to_consumer_group_description(raw):
    if raw is None:
        return None
    (group_id, is_simple, members, partition_assignor, group_type, state, group_state,
     coordinator, authorized_operations, group_epoch, target_assignment_epoch) = raw
    return ConsumerGroupDescription(
        group_id, bool(is_simple), [_to_member_description(m) for m in members],
        partition_assignor, group_type, state, group_state, _to_node(coordinator),
        list(authorized_operations), group_epoch, target_assignment_epoch)


def _to_classic_group_description(raw):
    if raw is None:
        return None
    (group_id, protocol, protocol_data, is_simple, members, state, coordinator,
     authorized_operations) = raw
    return ClassicGroupDescription(
        group_id, protocol, protocol_data, bool(is_simple),
        [_to_member_description(m) for m in members], state, _to_node(coordinator),
        list(authorized_operations))


def _to_list_groups(raw):
    """([listing_tuple], [error_tuple]) -> ([GroupListing], [KafkaError])

    Java's ``ListGroupsResult`` splits one future into ``valid()`` and
    ``errors()``; the two are independent collections of generally different
    length, so this stays a pair of lists rather than becoming a dict.
    """
    valid, errors = raw
    return ([GroupListing(*row) for row in valid],
            [_to_error(e) for e in errors])


def _to_list_consumer_groups(raw):
    """([listing_tuple], [error_tuple])
    -> ([ConsumerGroupListing], [KafkaError])"""
    valid, errors = raw
    return ([ConsumerGroupListing(*row) for row in valid],
            [_to_error(e) for e in errors])


def _to_describe_consumer_groups(raw):
    """{group_id: (error, description)}
    -> {group_id: ConsumerGroupDescription | KafkaError}"""
    out = {}
    for key, (error, description) in raw.items():
        out[key] = (_to_error(error) if error is not None
                    else _to_consumer_group_description(description))
    return out


def _to_describe_classic_groups(raw):
    """{group_id: (error, description)}
    -> {group_id: ClassicGroupDescription | KafkaError}"""
    out = {}
    for key, (error, description) in raw.items():
        out[key] = (_to_error(error) if error is not None
                    else _to_classic_group_description(description))
    return out


def _to_group_offsets(raw):
    """{(topic, partition): (offset, metadata, leader_epoch) | None}
    -> {(topic, partition): OffsetAndMetadata | None}

    A ``None`` value is Java's null map value: the group has no committed
    offset for that partition, which is distinct from a committed offset of 0.
    """
    return {key: (None if value is None else OffsetAndMetadata(*value))
            for key, value in raw.items()}


def _to_list_consumer_group_offsets(raw):
    """{group_id: (error, offsets)}
    -> {group_id: {(topic, partition): OffsetAndMetadata | None} | KafkaError}"""
    out = {}
    for key, (error, offsets) in raw.items():
        out[key] = _to_error(error) if error is not None else _to_group_offsets(offsets)
    return out


def _to_acl_binding(raw):
    """``(resource_type, resource_name, pattern_type, principal, host,
    operation, permission_type)`` -> AclBinding, or None."""
    return None if raw is None else AclBinding(*raw)


def _to_acl_binding_filter(raw):
    """The same seven fields, with nullable strings -> AclBindingFilter."""
    return None if raw is None else AclBindingFilter(*raw)


def _to_create_acls(raw):
    """{binding_tuple: error} -> {AclBinding: None | KafkaError}"""
    return {_to_acl_binding(key): _to_error(error) for key, error in raw.items()}


def _to_describe_acls(raw):
    """[binding_tuple] -> [AclBinding]

    A list, not a dict: ``DescribeAclsResult`` has one future for the whole
    call, so there is no key an error could hang on and a failure raises
    instead.
    """
    return [_to_acl_binding(row) for row in raw]


def _to_delete_acls(raw):
    """{filter_tuple: (error, [(error, binding_tuple)])}
    -> {AclBindingFilter: KafkaError | [DeletedAcl]}

    A filter whose own future failed maps to that error; otherwise it maps to
    the ACLs it matched, each of which either was deleted or carries its own
    exception. The two levels are Java's, not an invention: ``FilterResults``
    holds one ``FilterResult`` per matched ACL.
    """
    out = {}
    for key, (error, results) in raw.items():
        filter_key = _to_acl_binding_filter(key)
        if error is not None:
            out[filter_key] = _to_error(error)
        else:
            out[filter_key] = [DeletedAcl(_to_acl_binding(b), _to_error(e)) for e, b in results]
    return out


def _to_client_quota_entity(raw):
    """``((entity_type, entity_name_or_None), ...)`` -> ClientQuotaEntity."""
    return ClientQuotaEntity({entity_type: name for entity_type, name in raw})


def _to_describe_client_quotas(raw):
    """{entity_pairs: [(quota_key, value)]}
    -> {ClientQuotaEntity: {quota_key: float}}"""
    return {_to_client_quota_entity(key): dict(quotas) for key, quotas in raw.items()}


def _to_alter_client_quotas(raw):
    """{entity_pairs: error} -> {ClientQuotaEntity: None | KafkaError}"""
    return {_to_client_quota_entity(key): _to_error(error) for key, error in raw.items()}


def _to_kafka_principal(raw):
    """``(principal_type, name, token_authenticated)`` -> KafkaPrincipal."""
    return None if raw is None else KafkaPrincipal(*raw)


def _to_delegation_token(raw):
    """``(token_id, owner, requester, [renewers], issue_ts, expiry_ts, max_ts,
    hmac, hmac_base64)`` -> DelegationToken."""
    if raw is None:
        return None
    (token_id, owner, requester, renewers, issue_ts, expiry_ts, max_ts, hmac, hmac_base64) = raw
    info = TokenInformation(token_id, _to_kafka_principal(owner), _to_kafka_principal(requester),
                            [_to_kafka_principal(r) for r in renewers],
                            issue_ts, expiry_ts, max_ts)
    return DelegationToken(info, hmac, hmac_base64)


def _to_describe_delegation_token(raw):
    """``[token_tuple]`` -> ``[DelegationToken]``.

    A list, not a dict: ``describeDelegationToken`` has one future for the whole
    call and no key to map from.
    """
    return [_to_delegation_token(t) for t in raw]


def _to_describe_user_scram_credentials(raw):
    """``{user: (error, [(mechanism, iterations)])}``
    -> ``{user: UserScramCredentialsDescription | KafkaError}``.

    A user whose own description failed maps to its error; every other user maps
    to its credentials, which may be an empty list when the broker reports it as
    having none.
    """
    out = {}
    for user, (error, infos) in raw.items():
        if error is not None:
            out[user] = _to_error(error)
        else:
            out[user] = UserScramCredentialsDescription(
                user, [ScramCredentialInfo(mechanism, iterations) for mechanism, iterations in infos])
    return out


def _to_feature_metadata(raw):
    """``([(feature, min, max)], epoch_or_None, [(feature, min, max)])``
    -> FeatureMetadata.

    The first list is the *finalized* features and the second the *supported*
    ones; they are independent and need not agree in size or in keys.
    """
    finalized, epoch, supported = raw
    return FeatureMetadata(
        {name: FinalizedVersionRange(low, high) for name, low, high in finalized},
        epoch,
        {name: SupportedVersionRange(low, high) for name, low, high in supported},
    )


def _to_keyed_errors(raw):
    """{key: error} -> {key: None | KafkaError}

    The shape every RPC whose per-key future is ``KafkaFuture<Void>`` drains
    to: ``None`` means that key succeeded.
    """
    return {key: _to_error(error) for key, error in raw.items()}


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

    @staticmethod
    def _create_partitions_rows(new_partitions):
        """``{topic: NewPartitions}`` -> the ``(topic, total_count,
        assignments)`` rows the C extension unpacks.

        ``assignments is None`` selects Java's ``increaseTo(int)`` and a list
        selects ``increaseTo(int, List<List<Integer>>)`` — two different broker
        requests. Java's ``MockAdminClient.createPartitions`` throws
        (`MockAdminClient.java:626-628`) before either can be echoed back, so
        this builder is the only place the distinction is observable.
        """
        return [np._to_spec(topic) for topic, np in new_partitions.items()]

    def _create_partitions_spec(self, new_partitions, timeout, validate_only,
                                retry_on_quota_violation):
        spec = self._create_partitions_rows(new_partitions)
        ms = _ms(timeout)
        drain = _lib.CreatePartitionsResult_drain
        return (lambda cb: _lib.Admin_create_partitions_async(
                    self._h, spec, ms, bool(validate_only),
                    bool(retry_on_quota_violation), cb),
                self._resolve_value(drain, _to_create_partitions),
                self._free_value(drain))

    @staticmethod
    def _delete_records_rows(records_to_delete):
        """``{(topic, partition): RecordsToDelete}`` -> the
        ``(topic, partition, before_offset)`` rows the C extension unpacks.

        Java's ``MockAdminClient.deleteRecords`` throws for any non-empty
        request (`MockAdminClient.java:631-638`), so the column order is not
        observable end to end.
        """
        return [(str(topic), int(partition), int(rtd.before_offset))
                for (topic, partition), rtd in records_to_delete.items()]

    def _delete_records_spec(self, records_to_delete, timeout):
        spec = self._delete_records_rows(records_to_delete)
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

    @staticmethod
    def _elect_leaders_rows(partitions):
        """Partition selection -> ``(all_partitions, rows)``.

        ``partitions is None`` is Java's null Set: elect for every partition.
        It crosses as an explicit flag so it cannot be confused with an empty
        selection (`Admin.java:1096-1097`) — a cluster-wide election versus a
        no-op. Java's ``MockAdminClient.electLeaders`` throws
        (`MockAdminClient.java:797`), so neither the flag nor the column order
        has an end-to-end observable.
        """
        all_partitions = partitions is None
        rows = [] if all_partitions else [(str(t), int(p)) for t, p in partitions]
        return (all_partitions, rows)

    def _elect_leaders_spec(self, election_type, partitions, timeout):
        all_partitions, spec = self._elect_leaders_rows(partitions)
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

    def _list_groups_spec(self, group_states, protocol_types, types, timeout):
        # Group states and types cross as the Java enums' toString() names:
        # neither has a numeric id, so the name is the contract. An empty list
        # leaves the filter unset, i.e. "everything".
        states = [] if group_states is None else [str(s) for s in group_states]
        protocols = [] if protocol_types is None else [str(p) for p in protocol_types]
        kinds = [] if types is None else [str(t) for t in types]
        ms = _ms(timeout)
        drain = _lib.ListGroupsResult_drain
        return (lambda cb: _lib.Admin_list_groups_async(
                    self._h, states, protocols, kinds, ms, cb),
                self._resolve_value(drain, _to_list_groups),
                self._free_value(drain))

    def _list_consumer_groups_spec(self, group_states, types, timeout):
        states = [] if group_states is None else [str(s) for s in group_states]
        kinds = [] if types is None else [str(t) for t in types]
        ms = _ms(timeout)
        drain = _lib.ListConsumerGroupsResult_drain
        return (lambda cb: _lib.Admin_list_consumer_groups_async(
                    self._h, states, kinds, ms, cb),
                self._resolve_value(drain, _to_list_consumer_groups),
                self._free_value(drain))

    def _describe_consumer_groups_spec(self, group_ids, timeout,
                                       include_authorized_operations):
        ids = [str(g) for g in group_ids]
        ms = _ms(timeout)
        drain = _lib.DescribeConsumerGroupsResult_drain
        return (lambda cb: _lib.Admin_describe_consumer_groups_async(
                    self._h, ids, ms, bool(include_authorized_operations), cb),
                self._resolve_value(drain, _to_describe_consumer_groups),
                self._free_value(drain))

    def _describe_classic_groups_spec(self, group_ids, timeout,
                                      include_authorized_operations):
        ids = [str(g) for g in group_ids]
        ms = _ms(timeout)
        drain = _lib.DescribeClassicGroupsResult_drain
        return (lambda cb: _lib.Admin_describe_classic_groups_async(
                    self._h, ids, ms, bool(include_authorized_operations), cb),
                self._resolve_value(drain, _to_describe_classic_groups),
                self._free_value(drain))

    def _list_consumer_group_offsets_spec(self, group_specs, timeout, require_stable):
        # One ragged partition list per group. A spec whose topic_partitions is
        # None is Java's unset collection — "every partition the group has
        # committed offsets for" — and crosses as an explicit flag so it stays
        # distinct from an empty selection.
        spec = []
        for group_id, group_spec in group_specs.items():
            partitions = None if group_spec is None else group_spec.topic_partitions
            all_partitions = partitions is None
            rows = [] if all_partitions else [(str(t), int(p)) for t, p in partitions]
            spec.append((str(group_id), all_partitions, rows))
        ms = _ms(timeout)
        drain = _lib.ListConsumerGroupOffsetsResult_drain
        return (lambda cb: _lib.Admin_list_consumer_group_offsets_async(
                    self._h, spec, ms, bool(require_stable), cb),
                self._resolve_value(drain, _to_list_consumer_group_offsets),
                self._free_value(drain))

    @staticmethod
    def _alter_consumer_group_offsets_rows(offsets):
        """``{(topic, partition): OffsetAndMetadata}`` -> the 6-tuples the C
        extension unpacks.

        Two nulls are load-bearing and neither survives a round trip — Java's
        ``MockAdminClient.alterConsumerGroupOffsets`` throws
        (`MockAdminClient.java:1213`):

          - ``leader_epoch is None`` is Java's empty Optional and crosses as a
            separate present-flag, so epoch 0 stays distinguishable from an
            absent epoch;
          - ``metadata is None`` stays ``None`` (a NULL pointer), distinct from
            the empty string Java's ``OffsetAndMetadata`` defaults to.
        """
        return [(str(topic), int(partition), int(o.offset),
                 None if o.metadata is None else str(o.metadata),
                 o.leader_epoch is not None,
                 0 if o.leader_epoch is None else int(o.leader_epoch))
                for (topic, partition), o in offsets.items()]

    def _alter_consumer_group_offsets_spec(self, group_id, offsets, timeout):
        spec = self._alter_consumer_group_offsets_rows(offsets)
        ms = _ms(timeout)
        drain = _lib.AlterConsumerGroupOffsetsResult_drain
        return (lambda cb: _lib.Admin_alter_consumer_group_offsets_async(
                    self._h, str(group_id), spec, ms, cb),
                self._resolve_value(drain, _to_keyed_errors),
                self._free_value(drain))

    @staticmethod
    def _delete_consumer_group_offsets_rows(partitions):
        """``{(topic, partition)}`` -> the ``(topic, partition)`` rows the C
        extension unpacks.

        Java's ``MockAdminClient.deleteConsumerGroupOffsets`` throws
        (`MockAdminClient.java:783`), so the column order is not observable end
        to end.
        """
        return [(str(t), int(p)) for t, p in partitions]

    def _delete_consumer_group_offsets_spec(self, group_id, partitions, timeout):
        spec = self._delete_consumer_group_offsets_rows(partitions)
        ms = _ms(timeout)
        drain = _lib.DeleteConsumerGroupOffsetsResult_drain
        return (lambda cb: _lib.Admin_delete_consumer_group_offsets_async(
                    self._h, str(group_id), spec, ms, cb),
                self._resolve_value(drain, _to_keyed_errors),
                self._free_value(drain))

    def _delete_consumer_groups_spec(self, group_ids, timeout):
        ids = [str(g) for g in group_ids]
        ms = _ms(timeout)
        drain = _lib.DeleteConsumerGroupsResult_drain
        return (lambda cb: _lib.Admin_delete_consumer_groups_async(self._h, ids, ms, cb),
                self._resolve_value(drain, _to_keyed_errors),
                self._free_value(drain))

    @staticmethod
    def _remove_members_rows(members):
        """Member selection -> ``(remove_all, group_instance_ids)``.

        ``members is None`` selects Java's no-argument options constructor
        ("remove every member"). An empty *list* is not the same thing: Java's
        Collection constructor rejects it, so it must not silently become the
        destructive form. Java's
        ``MockAdminClient.removeMembersFromConsumerGroup`` throws
        (`MockAdminClient.java:801-803`), so the flag has no end-to-end
        observable.
        """
        remove_all = members is None
        ids = ([] if remove_all
               else [str(getattr(m, "group_instance_id", m)) for m in members])
        return (remove_all, ids)

    def _remove_members_from_consumer_group_spec(self, group_id, members, reason, timeout):
        remove_all, ids = self._remove_members_rows(members)
        ms = _ms(timeout)
        drain = _lib.RemoveMembersFromConsumerGroupResult_drain
        return (lambda cb: _lib.Admin_remove_members_from_consumer_group_async(
                    self._h, str(group_id), remove_all, ids,
                    None if reason is None else str(reason), ms, cb),
                self._resolve_value(drain, _to_keyed_errors),
                self._free_value(drain))


    # ---- B5a: ACLs and client quotas ---------------------------------------
    #
    # The four ACL enums cross as Java's `code()` values, and quota filter
    # components as Kafka's wire match-type constants; both are real protocol
    # numbers, not names or invented codes.

    @staticmethod
    def _acl_binding_rows(acls):
        """AclBinding objects -> the 7-tuples the C extension unpacks."""
        return [(int(a.resource_type), str(a.resource_name), int(a.pattern_type),
                 str(a.principal), str(a.host), int(a.operation), int(a.permission_type))
                for a in acls]

    @staticmethod
    def _acl_filter_rows(filters):
        """AclBindingFilter objects -> the 7-tuples the C extension unpacks.

        The three strings stay ``None`` where absent: ``None`` is Java's
        match-any, distinct from ``""``.
        """
        return [(int(f.resource_type),
                 None if f.resource_name is None else str(f.resource_name),
                 int(f.pattern_type),
                 None if f.principal is None else str(f.principal),
                 None if f.host is None else str(f.host),
                 int(f.operation), int(f.permission_type))
                for f in filters]

    def _create_acls_spec(self, acls, timeout):
        rows = self._acl_binding_rows(acls)
        ms = _ms(timeout)
        drain = _lib.CreateAclsResult_drain
        return (lambda cb: _lib.Admin_create_acls_async(self._h, rows, ms, cb),
                self._resolve_value(drain, _to_create_acls),
                self._free_value(drain))

    def _describe_acls_spec(self, acl_filter, timeout):
        row = self._acl_filter_rows([acl_filter])[0]
        ms = _ms(timeout)
        drain = _lib.DescribeAclsResult_drain
        return (lambda cb: _lib.Admin_describe_acls_async(
                    self._h, row[0], row[1], row[2], row[3], row[4], row[5], row[6], ms, cb),
                self._resolve_value(drain, _to_describe_acls),
                self._free_value(drain))

    def _delete_acls_spec(self, filters, timeout):
        rows = self._acl_filter_rows(filters)
        ms = _ms(timeout)
        drain = _lib.DeleteAclsResult_drain
        return (lambda cb: _lib.Admin_delete_acls_async(self._h, rows, ms, cb),
                self._resolve_value(drain, _to_delete_acls),
                self._free_value(drain))

    @staticmethod
    def _quota_filter_rows(quota_filter):
        """``ClientQuotaFilter`` -> the ``(entity_type, match_type, match_name)``
        rows the C extension unpacks.

        The match type is a real wire constant, not an invented code
        (`DescribeClientQuotasRequest.MATCH_TYPE_*`), and it has to be explicit:
        ``ofDefaultEntity`` and ``ofEntityType`` both carry no name, so a null
        name alone cannot separate them. The name stays ``None`` where absent —
        the C reader ignores it for the two nameless match types, which is
        exactly why a transposition here would otherwise be silent. Java's
        ``MockAdminClient.describeClientQuotas`` throws
        (`MockAdminClient.java:1243`), so nothing else pins these three columns.
        """
        return [(str(c.entity_type), int(c.match_type),
                 None if c.match_name is None else str(c.match_name))
                for c in quota_filter.components]

    def _describe_client_quotas_spec(self, quota_filter, timeout):
        components = self._quota_filter_rows(quota_filter)
        strict = bool(quota_filter.strict)
        ms = _ms(timeout)
        drain = _lib.DescribeClientQuotasResult_drain
        return (lambda cb: _lib.Admin_describe_client_quotas_async(
                    self._h, components, strict, ms, cb),
                self._resolve_value(drain, _to_describe_client_quotas),
                self._free_value(drain))

    @staticmethod
    def _quota_alteration_rows(entries):
        """ClientQuotaAlteration objects -> the ``(entity_pairs, ops)`` rows the
        C extension unpacks.

        Both nulls are load-bearing and must survive as ``None``: a ``None``
        entity name is the built-in default entity, and a ``None`` op value is
        a removal. Neither is observable end to end — Java's mock
        throws before echoing anything back — so this is extracted as a pure
        function precisely so a test can pin it.
        """
        return [([(str(t), None if n is None else str(n))
                  for t, n in a.entity.entries.items()],
                 [(str(o.key), None if o.value is None else float(o.value)) for o in a.ops])
                for a in entries]

    def _alter_client_quotas_spec(self, entries, timeout, validate_only):
        rows = self._quota_alteration_rows(entries)
        ms = _ms(timeout)
        drain = _lib.AlterClientQuotasResult_drain
        return (lambda cb: _lib.Admin_alter_client_quotas_async(
                    self._h, rows, ms, validate_only, cb),
                self._resolve_value(drain, _to_alter_client_quotas),
                self._free_value(drain))


    # ---- B5b: SCRAM, delegation tokens and features ------------------------
    #
    # SCRAM mechanisms cross as Java's `ScramMechanism.type()` indicators and
    # feature upgrades as `FeatureUpdate.UpgradeType.code()`; both are real
    # protocol numbers, not names or invented codes.
    #
    # Java's `MockAdminClient` throws for both SCRAM RPCs
    # (`MockAdminClient.java:1251-1259`), so nothing the suite can run observes
    # what their requests carry -- which is why the two row builders below are
    # pure static methods with direct tests, as B5a established for the request
    # direction.

    @staticmethod
    def _scram_alteration_rows(alterations):
        """Upsertions and deletions -> the 6-tuples the C extension unpacks.

        The ``is_deletion`` flag is load-bearing and cannot be inferred: both
        forms carry a user and a mechanism, and a deletion simply has no
        password, so "password is None" would conflate a deletion with a
        malformed upsertion. A ``None`` salt is Java's three-argument
        constructor, which generates one.
        """
        rows = []
        for alteration in alterations:
            if isinstance(alteration, UserScramCredentialDeletion):
                rows.append((str(alteration.user), True, int(alteration.mechanism), 0, None, None))
            else:
                info = alteration.credential_info
                rows.append((str(alteration.user), False, int(info.mechanism), int(info.iterations),
                             bytes(alteration.password),
                             None if alteration.salt is None else bytes(alteration.salt)))
        return rows

    @staticmethod
    def _principal_rows(principals):
        """KafkaPrincipals -> the ``(principal_type, name)`` pairs the C
        extension unpacks.

        ``token_authenticated`` is not sent: Java's request carries only the
        type and the name, and the flag is an authentication-side property of a
        principal the broker reports back.
        """
        return [(str(p.principal_type), str(p.name)) for p in (principals or [])]

    @staticmethod
    def _feature_update_rows(feature_updates):
        """``{feature: FeatureUpdate}`` -> the
        ``(feature, max_version_level, upgrade_type)`` rows the C extension
        unpacks."""
        return [(str(feature), int(update.max_version_level), int(update.upgrade_type))
                for feature, update in feature_updates.items()]

    def _describe_user_scram_credentials_spec(self, users, timeout):
        names = [] if users is None else [str(u) for u in users]
        ms = _ms(timeout)
        drain = _lib.DescribeUserScramCredentialsResult_drain
        return (lambda cb: _lib.Admin_describe_user_scram_credentials_async(self._h, names, ms, cb),
                self._resolve_value(drain, _to_describe_user_scram_credentials),
                self._free_value(drain))

    def _alter_user_scram_credentials_spec(self, alterations, timeout):
        rows = self._scram_alteration_rows(alterations)
        ms = _ms(timeout)
        drain = _lib.AlterUserScramCredentialsResult_drain
        return (lambda cb: _lib.Admin_alter_user_scram_credentials_async(self._h, rows, ms, cb),
                self._resolve_value(drain, _to_keyed_errors),
                self._free_value(drain))

    def _create_delegation_token_spec(self, renewers, owner, max_lifetime_ms, timeout):
        rows = self._principal_rows(renewers)
        # A None owner leaves Java's field empty, which makes the requesting
        # principal the owner; both halves must be present or neither.
        owner_type = None if owner is None else str(owner.principal_type)
        owner_name = None if owner is None else str(owner.name)
        ms = _ms(timeout)
        drain = _lib.CreateDelegationTokenResult_drain
        return (lambda cb: _lib.Admin_create_delegation_token_async(
                    self._h, rows, owner_type, owner_name, int(max_lifetime_ms), ms, cb),
                self._resolve_value(drain, _to_delegation_token),
                self._free_value(drain))

    def _renew_delegation_token_spec(self, hmac, renew_time_period_ms, timeout):
        ms = _ms(timeout)
        drain = _lib.RenewDelegationTokenResult_drain
        return (lambda cb: _lib.Admin_renew_delegation_token_async(
                    self._h, bytes(hmac), int(renew_time_period_ms), ms, cb),
                self._resolve_value(drain, int),
                self._free_value(drain))

    def _expire_delegation_token_spec(self, hmac, expiry_time_period_ms, timeout):
        ms = _ms(timeout)
        drain = _lib.ExpireDelegationTokenResult_drain
        return (lambda cb: _lib.Admin_expire_delegation_token_async(
                    self._h, bytes(hmac), int(expiry_time_period_ms), ms, cb),
                self._resolve_value(drain, int),
                self._free_value(drain))

    def _describe_delegation_token_spec(self, owners, timeout):
        # `owners is None` is Java's unset filter: describe every token. It
        # crosses as an explicit flag so it stays distinct from an empty filter.
        has_owners = owners is not None
        rows = self._principal_rows(owners)
        ms = _ms(timeout)
        drain = _lib.DescribeDelegationTokenResult_drain
        return (lambda cb: _lib.Admin_describe_delegation_token_async(
                    self._h, has_owners, rows, ms, cb),
                self._resolve_value(drain, _to_describe_delegation_token),
                self._free_value(drain))

    def _describe_features_spec(self, node_id, timeout):
        # `node_id is None` is Java's empty OptionalInt: send to an arbitrary
        # controller/broker. Node id 0 is a legal broker, so the flag is what
        # carries absence.
        has_node_id = node_id is not None
        ms = _ms(timeout)
        drain = _lib.DescribeFeaturesResult_drain
        return (lambda cb: _lib.Admin_describe_features_async(
                    self._h, has_node_id, 0 if node_id is None else int(node_id), ms, cb),
                self._resolve_value(drain, _to_feature_metadata),
                self._free_value(drain))

    def _update_features_spec(self, feature_updates, timeout, validate_only):
        rows = self._feature_update_rows(feature_updates)
        ms = _ms(timeout)
        drain = _lib.UpdateFeaturesResult_drain
        return (lambda cb: _lib.Admin_update_features_async(
                    self._h, rows, ms, bool(validate_only), cb),
                self._resolve_value(drain, _to_keyed_errors),
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

    def set_feature_levels(self, feature_levels):
        """Seed the feature levels ``describe_features`` reports and
        ``update_features`` validates against, from
        ``{feature: (level, min_supported, max_supported)}``.

        Mirrors the three ``MockAdminClient.Builder`` setters ``featureLevels``,
        ``minSupportedFeatureLevels`` and ``maxSupportedFeatureLevels``, which
        Java takes at construction time. Unlike the offset setters this
        **replaces** what was seeded before.
        """
        spec = [(str(f), int(level), int(low), int(high))
                for f, (level, low, high) in feature_levels.items()]
        e = _lib.MockAdminClient_set_feature_levels(self._h, spec)
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

    def update_consumer_group_offsets(self, offsets):
        """Seed the committed offsets ``list_consumer_group_offsets`` reports,
        from ``{(topic, partition): offset}`` (Java
        ``MockAdminClient.updateConsumerGroupOffsets``). Merges into, rather
        than replaces, what was seeded before.

        The mock keys these by partition only and ignores the group id — its
        ``listConsumerGroupOffsets`` answers every request from one shared map
        and rejects more than one requested group — so there is no group
        argument. The real client has no such restriction.
        """
        spec = [(str(t), int(p), int(o)) for (t, p), o in offsets.items()]
        e = _lib.MockAdminClient_update_consumer_group_offsets(self._h, spec)
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

    def elect_leaders(self, election_type, partitions, timeout=None):
        """Elect leaders for ``partitions`` (an iterable of ``(topic,
        partition)``), or for **every** partition when ``partitions`` is
        ``None`` — Java's null ``Set``. Returns
        ``{(topic, partition): None | KafkaError}``, where ``None`` means the
        election succeeded for that partition.

        ``partitions`` is required, unlike the neighbouring
        :meth:`list_partition_reassignments`. Both of Java's ``electLeaders``
        overloads (``Admin.java:1092`` and the three-argument form) take the
        ``Set`` explicitly; there is no no-argument form. Pass ``None``
        explicitly for a cluster-wide election, as a Java caller must — an
        omitted argument must not silently mean an unclean election over every
        partition in the cluster.
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

    def list_groups(self, group_states=None, protocol_types=None, types=None, timeout=None):
        """List every group in the cluster. Returns
        ``([GroupListing], [KafkaError])``.

        Java's ``ListGroupsResult`` has no per-key future: one source future is
        split into ``valid()`` listings and an **unkeyed** ``errors()``
        collection, and the two are independent — a partial success has both
        non-empty. The two lists are therefore returned as a pair rather than
        merged, and the error list must not be indexed by listing position.

        The three filters take the Java enums' ``toString()`` names
        (``"Stable"``, ``"Consumer"``, ...); matching is case-insensitive and
        an unrecognised name becomes ``UNKNOWN``, as in Java's ``parse``.
        ``None`` leaves a filter unset.
        """
        self._check_closed()
        return self._run_sync(*self._list_groups_spec(
            group_states, protocol_types, types, timeout))

    def list_consumer_groups(self, group_states=None, types=None, timeout=None):
        """List the consumer groups in the cluster. Returns
        ``([ConsumerGroupListing], [KafkaError])``.

        **Deprecated in Java since 4.1** in favour of :meth:`list_groups`,
        which covers every group type; mirrored here because it is still part
        of the Java ``Admin`` surface.

        Java's deprecated ``inStates(Set<ConsumerGroupState>)`` is defined as
        ``inGroupStates`` over ``GroupState.parse`` of the same names, so
        ``group_states`` accepts either spelling.
        """
        self._check_closed()
        return self._run_sync(*self._list_consumer_groups_spec(group_states, types, timeout))

    def describe_consumer_groups(self, group_ids, timeout=None,
                                 include_authorized_operations=False):
        """Describe ``group_ids``. Returns
        ``{group_id: ConsumerGroupDescription | KafkaError}``.

        Covers both classic and consumer (KIP-848) protocol groups; the
        classic-only sibling is :meth:`describe_classic_groups`.
        """
        self._check_closed()
        return self._run_sync(*self._describe_consumer_groups_spec(
            group_ids, timeout, include_authorized_operations))

    def describe_classic_groups(self, group_ids, timeout=None,
                                include_authorized_operations=False):
        """Describe classic ``group_ids``. Returns
        ``{group_id: ClassicGroupDescription | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._describe_classic_groups_spec(
            group_ids, timeout, include_authorized_operations))

    def list_consumer_group_offsets(self, group_specs, timeout=None, require_stable=False):
        """List committed offsets for ``{group_id: ListConsumerGroupOffsetsSpec
        | None}``. Returns
        ``{group_id: {(topic, partition): OffsetAndMetadata | None} | KafkaError}``.

        A spec of ``None`` (or one whose ``topic_partitions`` is ``None``) is
        Java's unset collection: every partition the group has committed
        offsets for. An inner ``None`` value is Java's null map value — the
        group has no committed offset for that partition, which is not the same
        as a committed offset of 0.
        """
        self._check_closed()
        return self._run_sync(*self._list_consumer_group_offsets_spec(
            group_specs, timeout, require_stable))

    def alter_consumer_group_offsets(self, group_id, offsets, timeout=None):
        """Commit ``{(topic, partition): OffsetAndMetadata}`` on behalf of
        ``group_id``. Returns ``{(topic, partition): None | KafkaError}``.

        With an empty ``offsets`` there is no per-partition slot for the
        outcome, so a failure raises instead — which is also the only thing
        Java's ``all()`` could report.
        """
        self._check_closed()
        return self._run_sync(*self._alter_consumer_group_offsets_spec(
            group_id, offsets, timeout))

    def delete_consumer_group_offsets(self, group_id, partitions, timeout=None):
        """Delete ``group_id``'s committed offsets for ``partitions`` (an
        iterable of ``(topic, partition)``). Returns
        ``{(topic, partition): None | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._delete_consumer_group_offsets_spec(
            group_id, partitions, timeout))

    def delete_consumer_groups(self, group_ids, timeout=None):
        """Delete ``group_ids``. Returns ``{group_id: None | KafkaError}``."""
        self._check_closed()
        return self._run_sync(*self._delete_consumer_groups_spec(group_ids, timeout))

    def remove_members_from_consumer_group(self, group_id, members, reason=None,
                                           timeout=None):
        """Remove ``members`` (an iterable of :class:`MemberToRemove`, or of
        bare ``group.instance.id`` strings) from ``group_id``. Returns
        ``{group_instance_id: None | KafkaError}``.

        Pass ``members=None`` for Java's no-argument
        ``RemoveMembersFromConsumerGroupOptions()``: remove **every** member of
        the group. That mode has no per-member outcome in Java at all —
        ``memberResult`` refuses and ``all()`` is the only observable — so the
        returned dict is empty and a failure raises. An empty iterable is not
        the same thing: Java's collection constructor rejects it, and so does
        this, rather than silently selecting the destructive form.
        """
        self._check_closed()
        return self._run_sync(*self._remove_members_from_consumer_group_spec(
            group_id, members, reason, timeout))

    def create_acls(self, acls, timeout=None):
        """Create ``acls`` (an iterable of :class:`AclBinding`). Returns
        ``{AclBinding: None | KafkaError}``.

        A binding with an ``ANY`` resource type, operation or permission type,
        or an ``ANY`` or ``MATCH`` pattern type, raises before the request is
        sent — Java's ``ResourcePattern`` and ``AccessControlEntry``
        constructors reject those, and they are what
        :class:`AclBindingFilter` is for.
        """
        self._check_closed()
        return self._run_sync(*self._create_acls_spec(acls, timeout))

    def describe_acls(self, acl_filter, timeout=None):
        """Describe the ACLs matching ``acl_filter`` (an
        :class:`AclBindingFilter`). Returns ``[AclBinding]``.

        Java takes a single filter here, not a collection, and has no
        no-argument overload, so ``acl_filter`` is required. Unlike the other
        four RPCs in this group the result is one future for the whole call, so
        a failure raises rather than landing in a per-key slot.
        """
        self._check_closed()
        return self._run_sync(*self._describe_acls_spec(acl_filter, timeout))

    def delete_acls(self, filters, timeout=None):
        """Delete the ACLs matching each of ``filters`` (an iterable of
        :class:`AclBindingFilter`). Returns
        ``{AclBindingFilter: KafkaError | [DeletedAcl]}``.

        A filter maps to a ``KafkaError`` when its own request failed and
        nothing was deleted for it, or to the ACLs it matched — each of which
        either was deleted or carries its own exception, as Java's
        ``FilterResult`` does.
        """
        self._check_closed()
        return self._run_sync(*self._delete_acls_spec(filters, timeout))

    def describe_client_quotas(self, quota_filter, timeout=None):
        """Describe the client quotas matching ``quota_filter`` (a
        :class:`ClientQuotaFilter`). Returns
        ``{ClientQuotaEntity: {quota_key: float}}``.

        One future for the whole call, so a failure raises.
        """
        self._check_closed()
        return self._run_sync(*self._describe_client_quotas_spec(quota_filter, timeout))

    def alter_client_quotas(self, entries, timeout=None, validate_only=False):
        """Apply ``entries`` (an iterable of :class:`ClientQuotaAlteration`).
        Returns ``{ClientQuotaEntity: None | KafkaError}``.

        An op whose ``value`` is ``None`` removes that quota. Two alterations
        of the same entity raise. Java accepts them -- it sends both and only
        the future map collapses -- but the result crosses as a flat array, so
        the caller could not tell which alteration the surviving outcome
        describes; see ``read_client_quota_alterations``.
        """
        self._check_closed()
        return self._run_sync(*self._alter_client_quotas_spec(entries, timeout, validate_only))

    def describe_user_scram_credentials(self, users=None, timeout=None):
        """Describe SASL/SCRAM credentials. Returns
        ``{user: UserScramCredentialsDescription | KafkaError}``.

        ``users`` of ``None`` (or an empty list) describes every user, mirroring
        Java's null/empty list. A user the broker reports as having no
        credential is described with an empty ``credential_infos``, not as an
        error -- Java's ``all()`` treats ``RESOURCE_NOT_FOUND`` the same way; a
        user whose description genuinely failed maps to its error.
        """
        self._check_closed()
        return self._run_sync(*self._describe_user_scram_credentials_spec(users, timeout))

    def alter_user_scram_credentials(self, alterations, timeout=None):
        """Apply ``alterations`` (:class:`UserScramCredentialUpsertion` and
        :class:`UserScramCredentialDeletion` objects). Returns
        ``{user: None | KafkaError}``.
        """
        self._check_closed()
        return self._run_sync(*self._alter_user_scram_credentials_spec(alterations, timeout))

    def create_delegation_token(self, renewers=None, owner=None, max_lifetime_ms=-1, timeout=None):
        """Create a delegation token. Returns a :class:`DelegationToken`.

        ``renewers`` are the principals allowed to renew it; an empty list means
        only the owner may. ``owner`` of ``None`` leaves Java's field empty,
        making the requesting principal the owner. ``max_lifetime_ms`` of -1 is
        Java's "use the broker's ``delegation.token.max.lifetime.ms``".

        One future for the whole call, so a failure raises.
        """
        self._check_closed()
        return self._run_sync(
            *self._create_delegation_token_spec(renewers, owner, max_lifetime_ms, timeout))

    def renew_delegation_token(self, hmac, renew_time_period_ms=-1, timeout=None):
        """Renew the token whose raw HMAC is ``hmac``. Returns the new expiry
        timestamp in milliseconds.

        ``renew_time_period_ms`` of -1 is Java's "use the broker's
        ``delegation.token.expiry.time.ms``". One future for the whole call, so
        a failure raises.
        """
        self._check_closed()
        return self._run_sync(*self._renew_delegation_token_spec(hmac, renew_time_period_ms, timeout))

    def expire_delegation_token(self, hmac, expiry_time_period_ms=-1, timeout=None):
        """Expire the token whose raw HMAC is ``hmac``. Returns the expiry
        timestamp in milliseconds.

        ``expiry_time_period_ms >= 0`` moves the expiry to
        ``min(now + period, max_timestamp)``; **negative expires the token
        immediately**, which is what Java's -1 default means here (unlike
        :meth:`renew_delegation_token`, where -1 is a broker default). One
        future for the whole call, so a failure raises.
        """
        self._check_closed()
        return self._run_sync(
            *self._expire_delegation_token_spec(hmac, expiry_time_period_ms, timeout))

    def describe_delegation_token(self, owners=None, timeout=None):
        """Describe delegation tokens. Returns a list of
        :class:`DelegationToken`.

        ``owners`` of ``None`` leaves Java's filter unset, describing every
        token the caller may see; an empty list is a different request. One
        future for the whole call, so a failure raises.
        """
        self._check_closed()
        return self._run_sync(*self._describe_delegation_token_spec(owners, timeout))

    def describe_features(self, node_id=None, timeout=None):
        """Describe the cluster's features. Returns a :class:`FeatureMetadata`.

        ``node_id`` of ``None`` sends the request to an arbitrary
        controller/broker, mirroring Java's empty ``OptionalInt``. One future
        for the whole call, so a failure raises.
        """
        self._check_closed()
        return self._run_sync(*self._describe_features_spec(node_id, timeout))

    def update_features(self, feature_updates, timeout=None, validate_only=False):
        """Apply ``feature_updates`` (``{feature: FeatureUpdate}``). Returns
        ``{feature: None | KafkaError}``.

        Against a real client an empty map raises, as Java's
        ``KafkaAdminClient.updateFeatures`` throws ``IllegalArgumentException``
        for it; Java's ``MockAdminClient`` does not check, so against a mock it
        yields an empty result.
        """
        self._check_closed()
        return self._run_sync(*self._update_features_spec(feature_updates, timeout, validate_only))

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

    async def elect_leaders(self, election_type, partitions, timeout=None):
        """See :meth:`Admin.elect_leaders`. ``partitions`` is required; pass
        ``None`` explicitly for a cluster-wide election, as in Java."""
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

    async def list_groups(self, group_states=None, protocol_types=None, types=None,
                          timeout=None):
        """See :meth:`Admin.list_groups`."""
        self._check_closed()
        return await self._run_async(*self._list_groups_spec(
            group_states, protocol_types, types, timeout))

    async def list_consumer_groups(self, group_states=None, types=None, timeout=None):
        """See :meth:`Admin.list_consumer_groups` (deprecated in Java since 4.1)."""
        self._check_closed()
        return await self._run_async(*self._list_consumer_groups_spec(
            group_states, types, timeout))

    async def describe_consumer_groups(self, group_ids, timeout=None,
                                       include_authorized_operations=False):
        """See :meth:`Admin.describe_consumer_groups`."""
        self._check_closed()
        return await self._run_async(*self._describe_consumer_groups_spec(
            group_ids, timeout, include_authorized_operations))

    async def describe_classic_groups(self, group_ids, timeout=None,
                                      include_authorized_operations=False):
        """See :meth:`Admin.describe_classic_groups`."""
        self._check_closed()
        return await self._run_async(*self._describe_classic_groups_spec(
            group_ids, timeout, include_authorized_operations))

    async def list_consumer_group_offsets(self, group_specs, timeout=None,
                                          require_stable=False):
        """See :meth:`Admin.list_consumer_group_offsets`."""
        self._check_closed()
        return await self._run_async(*self._list_consumer_group_offsets_spec(
            group_specs, timeout, require_stable))

    async def alter_consumer_group_offsets(self, group_id, offsets, timeout=None):
        """See :meth:`Admin.alter_consumer_group_offsets`."""
        self._check_closed()
        return await self._run_async(*self._alter_consumer_group_offsets_spec(
            group_id, offsets, timeout))

    async def delete_consumer_group_offsets(self, group_id, partitions, timeout=None):
        """See :meth:`Admin.delete_consumer_group_offsets`."""
        self._check_closed()
        return await self._run_async(*self._delete_consumer_group_offsets_spec(
            group_id, partitions, timeout))

    async def delete_consumer_groups(self, group_ids, timeout=None):
        """See :meth:`Admin.delete_consumer_groups`."""
        self._check_closed()
        return await self._run_async(*self._delete_consumer_groups_spec(group_ids, timeout))

    async def remove_members_from_consumer_group(self, group_id, members, reason=None,
                                                 timeout=None):
        """See :meth:`Admin.remove_members_from_consumer_group`. ``members`` is
        required; pass ``None`` explicitly to remove every member."""
        self._check_closed()
        return await self._run_async(*self._remove_members_from_consumer_group_spec(
            group_id, members, reason, timeout))

    async def create_acls(self, acls, timeout=None):
        self._check_closed()
        return await self._run_async(*self._create_acls_spec(acls, timeout))

    async def describe_acls(self, acl_filter, timeout=None):
        self._check_closed()
        return await self._run_async(*self._describe_acls_spec(acl_filter, timeout))

    async def delete_acls(self, filters, timeout=None):
        self._check_closed()
        return await self._run_async(*self._delete_acls_spec(filters, timeout))

    async def describe_client_quotas(self, quota_filter, timeout=None):
        self._check_closed()
        return await self._run_async(*self._describe_client_quotas_spec(quota_filter, timeout))

    async def alter_client_quotas(self, entries, timeout=None, validate_only=False):
        self._check_closed()
        return await self._run_async(
            *self._alter_client_quotas_spec(entries, timeout, validate_only))

    async def describe_user_scram_credentials(self, users=None, timeout=None):
        self._check_closed()
        return await self._run_async(*self._describe_user_scram_credentials_spec(users, timeout))

    async def alter_user_scram_credentials(self, alterations, timeout=None):
        self._check_closed()
        return await self._run_async(*self._alter_user_scram_credentials_spec(alterations, timeout))

    async def create_delegation_token(self, renewers=None, owner=None, max_lifetime_ms=-1,
                                      timeout=None):
        self._check_closed()
        return await self._run_async(
            *self._create_delegation_token_spec(renewers, owner, max_lifetime_ms, timeout))

    async def renew_delegation_token(self, hmac, renew_time_period_ms=-1, timeout=None):
        self._check_closed()
        return await self._run_async(
            *self._renew_delegation_token_spec(hmac, renew_time_period_ms, timeout))

    async def expire_delegation_token(self, hmac, expiry_time_period_ms=-1, timeout=None):
        self._check_closed()
        return await self._run_async(
            *self._expire_delegation_token_spec(hmac, expiry_time_period_ms, timeout))

    async def describe_delegation_token(self, owners=None, timeout=None):
        self._check_closed()
        return await self._run_async(*self._describe_delegation_token_spec(owners, timeout))

    async def describe_features(self, node_id=None, timeout=None):
        self._check_closed()
        return await self._run_async(*self._describe_features_spec(node_id, timeout))

    async def update_features(self, feature_updates, timeout=None, validate_only=False):
        self._check_closed()
        return await self._run_async(
            *self._update_features_spec(feature_updates, timeout, validate_only))

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
