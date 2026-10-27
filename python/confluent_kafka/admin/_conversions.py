# Copyright 2026 Confluent Inc.
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

"""Private: the admin client's values to and from the C extension.

Python passes the extension plain values and builds the value classes from the
plain values it returns, by the conversion table of CLAUDE.md (Python Binding
Conventions, Implementation over the FFI). Each RPC has, in Java declaration
order:

- ``<rpc>_args(...)``: the plain arguments of its ``Admin_<rpc>`` call (the
  options' ``(setter, value)`` pairs follow them);
- ``<rpc>_result(native, kind, loop)``: its ``*Result`` over the extension's
  ``AdminResult`` ``native``, returned at once: each future of the C result is a
  :class:`KafkaFuture` (``kind`` ``Sync``) or an :class:`AsyncKafkaFuture` on
  ``loop`` (``Async``) over the extension's future object (:func:`wrap`), whose
  delivery converts the C value to plain values in C and builds the value class
  from them in Python (the ``post`` of ``AdminResult.futures`` /
  ``AdminResult.future``).

The shared helpers come first.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from confluent_kafka._args import MISSING, reject_str
from confluent_kafka._errors import from_borrowed_ffi_error
from confluent_kafka.admin._futures import Async, Sync
from confluent_kafka.common.errors.api_error import ApiError
from confluent_kafka.common.kafka_future import (
    _CANCELLED,
    _PENDING,
    _VALUE,
    AsyncKafkaFuture,
    KafkaFuture,
    _FutureState,
    _KafkaFutureImpl,
)

if TYPE_CHECKING:
    from confluent_kafka.admin._internals import (
        _CoordinatorKey,
        _DescribeUserScramCredentialsResponseData,
    )
    from confluent_kafka.admin.abort_transaction_result import AbortTransactionResult
    from confluent_kafka.admin.abort_transaction_spec import AbortTransactionSpec
    from confluent_kafka.admin.alter_client_quotas_result import AlterClientQuotasResult
    from confluent_kafka.admin.alter_config_op import AlterConfigOp
    from confluent_kafka.admin.alter_configs_result import AlterConfigsResult
    from confluent_kafka.admin.alter_consumer_group_offsets_result import (
        AlterConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.alter_partition_reassignments_result import (
        AlterPartitionReassignmentsResult,
    )
    from confluent_kafka.admin.alter_replica_log_dirs_result import AlterReplicaLogDirsResult
    from confluent_kafka.admin.alter_user_scram_credentials_result import (
        AlterUserScramCredentialsResult,
    )
    from confluent_kafka.admin.classic_group_description import ClassicGroupDescription
    from confluent_kafka.admin.config import Config
    from confluent_kafka.admin.config_entry import ConfigEntry
    from confluent_kafka.admin.consumer_group_description import ConsumerGroupDescription
    from confluent_kafka.admin.create_acls_result import CreateAclsResult
    from confluent_kafka.admin.create_delegation_token_options import (
        CreateDelegationTokenOptions,
    )
    from confluent_kafka.admin.create_delegation_token_result import CreateDelegationTokenResult
    from confluent_kafka.admin.create_partitions_result import CreatePartitionsResult
    from confluent_kafka.admin.create_topics_result import (
        CreateTopicsResult,
        _TopicMetadataAndConfig,
    )
    from confluent_kafka.admin.delete_acls_result import DeleteAclsResult
    from confluent_kafka.admin.delete_consumer_group_offsets_result import (
        DeleteConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.delete_consumer_groups_result import DeleteConsumerGroupsResult
    from confluent_kafka.admin.delete_records_result import DeleteRecordsResult
    from confluent_kafka.admin.deleted_records import DeletedRecords
    from confluent_kafka.admin.delete_topics_result import DeleteTopicsResult
    from confluent_kafka.admin.describe_acls_result import DescribeAclsResult
    from confluent_kafka.admin.describe_classic_groups_result import DescribeClassicGroupsResult
    from confluent_kafka.admin.describe_client_quotas_result import DescribeClientQuotasResult
    from confluent_kafka.admin.describe_cluster_result import DescribeClusterResult
    from confluent_kafka.admin.describe_configs_result import DescribeConfigsResult
    from confluent_kafka.admin.describe_consumer_groups_result import DescribeConsumerGroupsResult
    from confluent_kafka.admin.describe_delegation_token_options import (
        DescribeDelegationTokenOptions,
    )
    from confluent_kafka.admin.describe_delegation_token_result import DescribeDelegationTokenResult
    from confluent_kafka.admin.describe_features_result import DescribeFeaturesResult
    from confluent_kafka.admin.describe_log_dirs_result import DescribeLogDirsResult
    from confluent_kafka.admin.describe_producers_result import DescribeProducersResult
    from confluent_kafka.admin.describe_replica_log_dirs_result import DescribeReplicaLogDirsResult
    from confluent_kafka.admin.describe_topics_result import DescribeTopicsResult
    from confluent_kafka.admin.describe_transactions_result import DescribeTransactionsResult
    from confluent_kafka.admin.describe_user_scram_credentials_result import (
        DescribeUserScramCredentialsResult,
    )
    from confluent_kafka.admin.elect_leaders_result import ElectLeadersResult
    from confluent_kafka.admin.expire_delegation_token_result import ExpireDelegationTokenResult
    from confluent_kafka.admin.feature_metadata import FeatureMetadata
    from confluent_kafka.admin.feature_update import FeatureUpdate
    from confluent_kafka.admin.fence_producers_result import FenceProducersResult
    from confluent_kafka.admin.group_listing import GroupListing
    from confluent_kafka.admin.list_config_resources_result import ListConfigResourcesResult
    from confluent_kafka.admin.list_consumer_group_offsets_result import (
        ListConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.list_consumer_group_offsets_spec import ListConsumerGroupOffsetsSpec
    from confluent_kafka.admin.list_groups_result import ListGroupsResult
    from confluent_kafka.admin.list_offsets_result import ListOffsetsResult
    from confluent_kafka.admin.list_partition_reassignments_result import (
        ListPartitionReassignmentsResult,
    )
    from confluent_kafka.admin.list_topics_result import ListTopicsResult
    from confluent_kafka.admin.list_transactions_options import ListTransactionsOptions
    from confluent_kafka.admin.list_transactions_result import ListTransactionsResult
    from confluent_kafka.admin.log_dir_description import LogDirDescription
    from confluent_kafka.admin.member_assignment import MemberAssignment
    from confluent_kafka.admin.member_description import MemberDescription
    from confluent_kafka.admin.member_to_remove import MemberToRemove
    from confluent_kafka.admin.new_partition_reassignment import NewPartitionReassignment
    from confluent_kafka.admin.new_partitions import NewPartitions
    from confluent_kafka.admin.new_topic import NewTopic
    from confluent_kafka.admin.offset_spec import OffsetSpec
    from confluent_kafka.admin.partition_reassignment import PartitionReassignment
    from confluent_kafka.admin.records_to_delete import RecordsToDelete
    from confluent_kafka.admin.remove_members_from_consumer_group_options import (
        RemoveMembersFromConsumerGroupOptions,
    )
    from confluent_kafka.admin.remove_members_from_consumer_group_result import (
        RemoveMembersFromConsumerGroupResult,
    )
    from confluent_kafka.admin.renew_delegation_token_result import RenewDelegationTokenResult
    from confluent_kafka.admin.terminate_transaction_result import TerminateTransactionResult
    from confluent_kafka.admin.topic_description import TopicDescription
    from confluent_kafka.admin.topic_listing import TopicListing
    from confluent_kafka.admin.transaction_description import TransactionDescription
    from confluent_kafka.admin.transaction_listing import TransactionListing
    from confluent_kafka.admin.unregister_controller_result import UnregisterControllerResult
    from confluent_kafka.admin.update_features_result import UpdateFeaturesResult
    from confluent_kafka.admin.user_scram_credential_alteration import UserScramCredentialAlteration
    from confluent_kafka.admin.user_scram_credentials_description import (
        UserScramCredentialsDescription,
    )
    from confluent_kafka.common import (
        AclBinding,
        AclBindingFilter,
        ClientQuotaAlteration,
        ClientQuotaEntity,
        ClientQuotaFilter,
        DelegationToken,
        KafkaPrincipal,
        Uuid,
    )
    from confluent_kafka.common.acl_operation import AclOperation
    from confluent_kafka.common.config.config_resource import ConfigResource
    from confluent_kafka.common.election_type import ElectionType
    from confluent_kafka.common.node import Node
    from confluent_kafka.common.topic_partition import TopicPartition
    from confluent_kafka.common.topic_partition_info import TopicPartitionInfo
    from confluent_kafka.common.topic_partition_replica import TopicPartitionReplica
    from confluent_kafka.consumer import OffsetAndMetadata

__all__ = [
    "DELETE_TOPIC_ID_COLLECTION",
    "DELETE_TOPIC_NAMES",
    "DELETE_TOPIC_NAME_COLLECTION",
    "DESCRIBE_TOPIC_ID_COLLECTION",
    "describe_topic_names",
    "group_ids_list",
    "transactional_ids_list",
    "DESCRIBE_TOPIC_NAMES",
    "DESCRIBE_TOPIC_NAME_COLLECTION",
    "Kind",
    "abort_transaction_args",
    "abort_transaction_result",
    "acl_operations_from_codes",
    "alter_client_quotas_args",
    "alter_client_quotas_result",
    "alter_consumer_group_offsets_args",
    "alter_consumer_group_offsets_result",
    "alter_partition_reassignments_args",
    "alter_partition_reassignments_result",
    "alter_replica_log_dirs_args",
    "alter_replica_log_dirs_result",
    "alter_user_scram_credentials_args",
    "alter_user_scram_credentials_result",
    "borrowed_error",
    "config_from_rows",
    "config_resource_from_tuple",
    "create_acls_args",
    "create_acls_result",
    "create_delegation_token_options",
    "create_delegation_token_result",
    "create_partitions_args",
    "create_partitions_result",
    "create_topics_args",
    "create_topics_result",
    "delete_acls_args",
    "delete_acls_result",
    "delete_consumer_group_offsets_args",
    "delete_consumer_group_offsets_result",
    "delete_consumer_groups_result",
    "delete_records_args",
    "delete_records_result",
    "delete_topics_args",
    "delete_topics_result",
    "describe_acls_args",
    "describe_acls_result",
    "describe_classic_groups_result",
    "describe_client_quotas_args",
    "describe_client_quotas_result",
    "describe_cluster_result",
    "describe_configs_args",
    "describe_configs_result",
    "describe_consumer_groups_result",
    "describe_delegation_token_options",
    "describe_delegation_token_result",
    "describe_features_result",
    "describe_log_dirs_args",
    "describe_log_dirs_result",
    "describe_producers_args",
    "describe_producers_result",
    "describe_replica_log_dirs_args",
    "describe_replica_log_dirs_result",
    "describe_topics_args",
    "describe_topics_result",
    "describe_transactions_args",
    "describe_transactions_result",
    "describe_user_scram_credentials_args",
    "describe_user_scram_credentials_result",
    "elect_leaders_args",
    "elect_leaders_result",
    "expire_delegation_token_result",
    "fence_producers_args",
    "fence_producers_result",
    "force_terminate_transaction_result",
    "incremental_alter_configs_args",
    "incremental_alter_configs_result",
    "linked",
    "list_config_resources_args",
    "list_config_resources_result",
    "list_consumer_group_offsets_args",
    "list_consumer_group_offsets_result",
    "list_groups_options",
    "list_groups_result",
    "list_offsets_args",
    "list_offsets_result",
    "list_partition_reassignments_args",
    "list_partition_reassignments_result",
    "list_topics_result",
    "list_transactions_options",
    "list_transactions_result",
    "node_from_tuple",
    "offset_spec_value",
    "optional_node_from_tuple",
    "outcomes",
    "partitions_spec",
    "remove_members_from_consumer_group_args",
    "remove_members_from_consumer_group_result",
    "renew_delegation_token_result",
    "replica_from_tuple",
    "topic_collection_target",
    "topic_partition_from_tuple",
    "unregister_controller_result",
    "update_features_args",
    "update_features_result",
    "wrap",
    "wrap_map",
    "all_partitions_valid",
    "check_keys",
    "present",
    "ambiguous_none",
    "any_topic_id",
    "any_topic_name",
    "element",
    "non_null",
    "options_or_new",
]

_T = TypeVar("_T")

#: The client kind a result is built for (``_futures.Sync`` / ``_futures.Async``).
Kind = type[Sync] | type[Async]


# ---- shared helpers ---------------------------------------------------------


def non_null(value: object, name: str) -> None:
    """Java's ``NullPointerException`` where a method dereferences a null argument
    (``newTopics.size()``, ``for (x : coll)``, ``Optional.of(null)``) or an FFI
    entry point takes a non-NULL pointer (CLAUDE.md, Python Binding Conventions,
    Types and Implementation over the FFI, exception 4): Python's own
    ``TypeError("<name> must not be None")``."""
    if value is None:
        raise TypeError(f"{name} must not be None")


def element(value: _T | None, name: str) -> _T:
    """One element of a collection Java dereferences (``newTopic.name()``,
    ``acl.toFilter()``, ``replica.brokerId()``): ``TypeError("<name> must not
    contain None")`` for a ``None``, the value otherwise."""
    if value is None:
        raise TypeError(f"{name} must not contain None")
    return value


def check_keys(mapping: Mapping[Any, Any], name: str) -> None:
    """Java's dereference of each key of ``mapping`` in the method's own loop,
    before it reads ``options`` (``nodeFor(resource)``, ``replica.brokerId()``,
    ``Collectors.toUnmodifiableMap``'s null-key check): ``element`` on each key.
    The values Java reads only on the admin thread are checked after ``options``,
    in the ``*_args`` conversion."""
    for key in mapping:
        element(key, name)


def present(values: Iterable[Any], name: str) -> list[Any]:
    """``values`` as a list, a ``None`` element raising
    ``TypeError("<name> must not contain None")``. For a ``None`` the FFI cannot
    carry while Java reads it only on a later path (``CoordinatorStrategy``'s group
    or transactional id, ``createRequest``), so Java fails that key's future instead
    (CLAUDE.md, Implementation over the FFI, exception 4, the later-path clause)."""
    out = list(values)
    for value in out:
        element(value, name)
    return out


_O = TypeVar("_O")


def options_or_new(options: _O | None, factory: Callable[[], _O], *, read: bool = True) -> _O:
    """An RPC's ``options``: left out (``MISSING``), Java's short form, which passes
    ``new XxxOptions()``; an explicit ``None`` is Java's ``null``, which
    ``KafkaAdminClient`` dereferences (``options.timeoutMs()``):
    ``TypeError("options must not be None")``. The caller runs it where Java reads
    ``options``, after Java's own loop over the arguments; ``read`` is ``False`` when
    Java's body returns before reading it (an empty input, every input failed
    early): then a ``None`` is accepted and a new options object, which the core does
    not read either, goes to the FFI."""
    if options is MISSING:
        return factory()
    if options is None:
        if not read:
            return factory()
        raise TypeError("options must not be None")
    return options


def any_topic_name(names: Iterable[Any]) -> bool:
    """Whether Java reads ``options`` for these topic names: some name is not
    unrepresentable (``KafkaAdminClient.topicNameIsUnrepresentable``: ``null`` or
    empty), so ``if (!topics.isEmpty())`` holds (KafkaAdminClient:1905, 2042, 2453)."""
    return any(name for name in names)


def any_topic_id(ids: Iterable[Any]) -> bool:
    """Whether Java's ``handleDeleteTopicsUsingIds`` reads ``options``: some id is not
    ``Uuid.ZERO_UUID`` (KafkaAdminClient:2058, 2067)."""
    from confluent_kafka.common.uuid import Uuid

    return any(topic_id != Uuid.ZERO_UUID for topic_id in ids)


def all_partitions_valid(rows: Iterable[tuple[Any, int]] | None) -> bool:
    """Whether Java's ``listPartitionReassignments`` reads ``options``: it returns
    early on the first unrepresentable topic or negative partition
    (KafkaAdminClient:4194-4205)."""
    return rows is None or all(topic and partition >= 0 for topic, partition in rows)


def ambiguous_none(method: str, name: str, forms: tuple[str, ...]) -> Exception:
    """A ``None`` for a parameter whose Java overloads differ only in its type
    (CLAUDE.md, Signatures): no overload matches, as Java's compiler rejects
    ``f(null)`` there. ``forms`` are the overloads' types of ``name``."""
    from confluent_kafka.illegal_argument_error import IllegalArgumentError

    return IllegalArgumentError(
        message=f"{method}(): {name}=None matches more than one overload: "
        + ", ".join(f"({name}: {form})" for form in forms))


def wrap(kind: Kind, loop: asyncio.AbstractEventLoop | None, native: Any
         ) -> KafkaFuture[Any] | AsyncKafkaFuture[Any]:
    """The package's future over the extension's future object ``native``: a
    :class:`KafkaFuture` for the sync kind, an :class:`AsyncKafkaFuture` whose
    inner future is on ``loop`` (the client's, else the loop of its first wait)
    for the async one (Futures)."""
    if kind is Async:
        return AsyncKafkaFuture._of_native(native, loop)
    return KafkaFuture._of_native(native)


def wrap_map(kind: Kind, loop: asyncio.AbstractEventLoop | None, natives: Mapping[Any, Any],
             key: Callable[[Any], Any] | None = None
             ) -> dict[Any, KafkaFuture[Any] | AsyncKafkaFuture[Any]]:
    """:func:`wrap` of each future of an ``AdminResult.futures`` map, its key
    turned into the Python key by ``key`` (as it is when ``None``)."""
    return {(raw if key is None else key(raw)): wrap(kind, loop, native)
            for raw, native in natives.items()}


def linked(kind: Kind, loop: asyncio.AbstractEventLoop | None,
           key_future: KafkaFuture[Any] | AsyncKafkaFuture[Any],
           source: KafkaFuture[_T] | AsyncKafkaFuture[_T]
           ) -> KafkaFuture[_T] | AsyncKafkaFuture[_T]:
    """A refinement whose value comes from a C future (``source``), linked as a
    dependant of its key's future (Futures): it completes as ``source`` does,
    and is cancelled when ``key_future`` is, as Java's
    ``futures.get(key).thenApply(...)`` is."""
    impl: _KafkaFutureImpl[_T] = _KafkaFutureImpl()

    def on_key(key_state: _FutureState[Any]) -> None:
        if key_state._state == _CANCELLED:
            impl.cancel(key_state._outcome)

    def on_source(source_state: _FutureState[_T]) -> None:
        impl._settle(source_state._state, source_state._outcome)

    key_future._python_state()._add_listener(on_key)
    source._python_state()._add_listener(on_source)
    if kind is Async:
        return AsyncKafkaFuture._of_impl(impl, loop)
    return KafkaFuture._of_impl(impl)


def borrowed_error(raw: tuple[Any, ...] | None) -> BaseException | None:
    """A borrowed error tuple read from a delivered value (the borrowed form of
    ``from_ffi_error``), or ``None`` for no error."""
    return None if raw is None else from_borrowed_ffi_error(raw)


def node_from_tuple(raw: tuple[int, str, int, str | None]) -> Node:
    """A drained ``(id, host, port, rack)`` as Java's ``new Node(id, host, port, rack)``
    (``is_fenced()`` ``False``: the FFI has no fenced flag)."""
    from confluent_kafka.common.node import Node

    node_id, host, port, rack = raw
    return Node(id=node_id, host=host, port=port, rack=rack)


def optional_node_from_tuple(raw: tuple[int, str, int, str | None] | None) -> Node | None:
    """``node_from_tuple(raw)``, or ``None`` for Java's ``null``."""
    return None if raw is None else node_from_tuple(raw)


def acl_operations_from_codes(codes: list[int] | None) -> set[AclOperation] | None:
    """Java's ``AdminUtils.validAclOperations`` result: ``None`` when the broker did not
    report the operations (Java's ``null``), else the operations of the codes."""
    from confluent_kafka.common.acl_operation import AclOperation

    return None if codes is None else {AclOperation.from_code(code=code) for code in codes}


def topic_partition_from_tuple(raw: tuple[str, int]) -> TopicPartition:
    """A drained ``(topic, partition)`` as Java's ``new TopicPartition(topic,
    partition)``."""
    from confluent_kafka.common.topic_partition import TopicPartition

    topic, partition = raw
    return TopicPartition(topic=topic, partition=partition)


# ---- the RPCs, in Java declaration order ------------------------------------


def create_topics_args(new_topics: Iterable[NewTopic]) -> tuple[Any, ...]:
    """``Admin_create_topics(h, spec, options)``'s arguments: each ``NewTopic`` as
    ``(name, num_partitions, replication_factor, configs, assignments)``, the
    counts ``-1`` for Java's empty ``Optional``, the configs and assignments
    ``None`` when not set (``NewTopic(name, replicasAssignments)`` when the
    assignments are set, else ``NewTopic(name, numPartitions,
    replicationFactor)``)."""
    spec = []
    for topic in new_topics:
        element(topic, "new_topics")  # newTopic.name()
        if topic.name() is None:
            # Java's unrepresentable name (KafkaAdminClient:1895): that topic's future
            # fails. The FFI cannot carry it: TypeError up front.
            raise TypeError("new_topics must not contain a NewTopic with a None name")
        configs = topic.configs()
        assignments = topic.replicas_assignments()
        spec.append((
            topic.name(), topic.num_partitions(), topic.replication_factor(),
            None if configs is None else list(configs.items()),
            None if assignments is None else list(assignments.items()),
        ))
    return (spec,)


def config_from_rows(rows: list[tuple[Any, ...]]) -> Config:
    """A ``Config`` from the extension's config entry rows (``config_to_py``):
    ``(name, value, is_default, is_sensitive, is_read_only, source, type,
    documentation, synonyms)``."""
    from confluent_kafka.admin.config import Config

    return Config(entries=[_config_entry(row) for row in rows])


def _uuid_from_string(raw: str) -> Uuid:
    from confluent_kafka.common import Uuid

    return Uuid.from_string(str=raw)


#: ``CreateTopicsResult``'s per-topic C accessors and the value class each
#: future's delivery builds from the plain value.
_CREATE_TOPICS_REFINEMENTS: dict[str, Callable[[Any], Any] | None] = {
    "config": config_from_rows,
    "topic_id": _uuid_from_string,
    "num_partitions": None,
    "replication_factor": None,
}


def create_topics_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                         ) -> CreateTopicsResult[Any]:
    """The ``CreateTopicsResult`` over the C result: the per-topic ``values()``
    futures, and each per-topic getter's future read from the C accessor of the
    same name (``config(topic)``, ...), which fails with ``IllegalArgumentError``
    for a topic not in the request, linked as a dependant of the topic's
    future (:func:`linked`)."""
    from confluent_kafka.admin.create_topics_result import CreateTopicsResult

    futures: dict[str, KafkaFuture[Any] | AsyncKafkaFuture[Any]] = wrap_map(
        kind, loop, native.futures("values"))

    def refine(topic: str, accessor: str) -> KafkaFuture[Any] | AsyncKafkaFuture[Any]:
        source = wrap(kind, loop, native.future(accessor, topic,
                                                _CREATE_TOPICS_REFINEMENTS[accessor]))
        key_future = futures.get(topic)
        return source if key_future is None else linked(kind, loop, key_future, source)

    return CreateTopicsResult._of_client(futures=futures, refine=refine, kind=kind)


def topic_collection_target(topics: Any, rpc: str) -> tuple[bool, list[Any]]:
    """Java's ``KafkaAdminClient.deleteTopics`` / ``describeTopics`` dispatch on the
    ``TopicCollection`` subclass: ``(True, ids)`` for a ``TopicIdCollection``,
    ``(False, names)`` for a ``TopicNameCollection`` or a ``Collection<String>``
    (``Admin``'s ``TopicCollection.ofTopicNames(topics)``); any other
    ``TopicCollection`` raises Java's ``IllegalArgumentException``
    (``KafkaAdminClient.java:1916``, ``:2154``)."""
    from confluent_kafka._java import java_str
    from confluent_kafka.common.topic_collection import TopicCollection
    from confluent_kafka.illegal_argument_error import IllegalArgumentError

    if topics is None:  # Java's instanceof chain: neither subclass
        raise IllegalArgumentError(
            message="The TopicCollection: None provided did not match any supported "
            "classes for " + rpc + ".")
    # A None name is Java's unrepresentable name (KafkaAdminClient:1847), whose
    # future fails; a None id, Java's NPE (deleteTopics, 2057) or unrepresentable id
    # (describeTopics, 1851). The FFI carries neither: TypeError up front, before the
    # options, as Java's loop precedes its options read.
    if not isinstance(topics, TopicCollection):
        reject_str("topics", topics)
        return False, present(topics, "topics")
    if isinstance(topics, TopicCollection.TopicIdCollection):
        return True, present(topics.topic_ids(), "topics")
    if isinstance(topics, TopicCollection.TopicNameCollection):
        return False, present(topics.topic_names(), "topics")
    raise IllegalArgumentError(
        message="The TopicCollection: " + java_str(topics)
        + " provided did not match any supported classes for " + rpc + ".")


def _uuid_key(raw: str) -> Any:
    from confluent_kafka.common.uuid import Uuid

    return Uuid.from_string(str=raw)


#: ``Admin_delete_topics``'s ``form``: which Java overload's entry point the
#: extension calls, as for ``Admin_describe_topics``: ``deleteTopics(Collection<String>)``
#: (``_with_string_slice_options``), or a ``TopicNameCollection`` / ``TopicIdCollection``
#: (``_with_topic_collection_options``).
DELETE_TOPIC_NAMES = 0
DELETE_TOPIC_NAME_COLLECTION = 1
DELETE_TOPIC_ID_COLLECTION = 2


def delete_topics_args(topics: Any, by_ids: bool, keys: list[Any]) -> tuple[Any, ...]:
    """``Admin_delete_topics(h, form, names_or_ids, options)``'s arguments: the form of
    the Java overload ``topics`` selects, and the names, or the ids as Java's
    ``Uuid.toString()``."""
    from confluent_kafka.common.topic_collection import TopicCollection

    if by_ids:
        return (DELETE_TOPIC_ID_COLLECTION, [str(key) for key in keys])
    if isinstance(topics, TopicCollection):
        return (DELETE_TOPIC_NAME_COLLECTION, keys)
    return (DELETE_TOPIC_NAMES, keys)


def delete_topics_result(native: Any, by_ids: bool, kind: Kind,
                         loop: asyncio.AbstractEventLoop | None) -> DeleteTopicsResult[Any]:
    """The ``DeleteTopicsResult`` over the C result: keyed by topic id for a
    ``TopicIdCollection`` (``topicIdValues()``), else by name
    (``topicNameValues()``), as Java's ``DeleteTopicsResult.ofTopicIds`` /
    ``ofTopicNames``."""
    from confluent_kafka.admin.delete_topics_result import DeleteTopicsResult

    if by_ids:
        return DeleteTopicsResult._new(
            topic_id_futures=wrap_map(kind, loop, native.futures("topic_id_values"), _uuid_key),
            name_futures=None, kind=kind)
    return DeleteTopicsResult._new(
        topic_id_futures=None, name_futures=wrap_map(kind, loop, native.futures("topic_name_values")),
        kind=kind)


def _listings(rows: Mapping[str, tuple[str, str, int]]) -> dict[str, TopicListing]:
    """``{name: TopicListing}`` from the extension's ``{name: (name, topic_id,
    is_internal)}``."""
    from confluent_kafka.admin.topic_listing import TopicListing
    from confluent_kafka.common import Uuid

    return {key: TopicListing(name=name, topic_id=Uuid.from_string(str=topic_id),
                              internal=bool(internal))
            for key, (name, topic_id, internal) in rows.items()}


def list_topics_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                       ) -> ListTopicsResult[Any]:
    """The ``ListTopicsResult`` over the C result's ``namesToListings()`` future."""
    from confluent_kafka.admin.list_topics_result import ListTopicsResult

    future = wrap(kind, loop, native.future("names_to_listings", None, _listings))
    return ListTopicsResult._new(future=future, kind=kind)


def _topic_partition_info(raw: tuple[Any, ...]) -> TopicPartitionInfo:
    """The extension's ``(partition, leader, replicas, isr, elr, last_known_elr)`` as
    Java's ``new TopicPartitionInfo(partition, leader, replicas, isr, elr,
    lastKnownElr)``, or, where the broker reported no ELR sets (``None``), as Java's
    four-argument ``new TopicPartitionInfo(partition, leader, replicas, isr)``, whose
    ``elr()`` and ``lastKnownElr()`` are ``null``."""
    from confluent_kafka.common.topic_partition_info import TopicPartitionInfo

    partition, leader, replicas, isr, elr, last_known_elr = raw
    leader_node = optional_node_from_tuple(leader)
    replica_nodes = [node_from_tuple(n) for n in replicas]
    isr_nodes = [node_from_tuple(n) for n in isr]
    if elr is None or last_known_elr is None:
        return TopicPartitionInfo(partition=partition, leader=leader_node,
                                  replicas=replica_nodes, isr=isr_nodes)
    return TopicPartitionInfo(
        partition=partition, leader=leader_node, replicas=replica_nodes, isr=isr_nodes,
        elr=[node_from_tuple(n) for n in elr],
        last_known_elr=[node_from_tuple(n) for n in last_known_elr])


def _topic_description(raw: tuple[Any, ...]) -> TopicDescription:
    """The extension's ``(name, topic_id, is_internal, partitions, operations)`` as
    Java's ``new TopicDescription(name, isInternal, partitions, authorizedOperations,
    topicId)``."""
    from confluent_kafka.admin.topic_description import TopicDescription
    from confluent_kafka.common.uuid import Uuid

    name, topic_id, internal, partitions, operations = raw
    return TopicDescription(
        name=name, internal=bool(internal),
        partitions=[_topic_partition_info(p) for p in partitions],
        authorized_operations=acl_operations_from_codes(operations),
        topic_id=Uuid.from_string(str=topic_id))


#: ``Admin_describe_topics``' forms: Java's ``describeTopics(Collection<String>)``,
#: and a ``TopicCollection`` of names or of ids.
DESCRIBE_TOPIC_NAMES = 0
DESCRIBE_TOPIC_NAME_COLLECTION = 1
DESCRIBE_TOPIC_ID_COLLECTION = 2


def describe_topic_names(topic_names: Iterable[str]) -> list[str]:
    """``describeTopics(Collection<String> topicNames)``'s names as a list; a ``str``
    is refused (``reject_str``)."""
    reject_str("topic_names", topic_names)
    return present(topic_names, "topic_names")  # see topic_collection_target


def group_ids_list(group_ids: Iterable[str]) -> list[str]:
    """A ``Collection<String> groupIds`` as a list; a ``str`` is refused
    (``reject_str``)."""
    reject_str("group_ids", group_ids)
    return present(group_ids, "group_ids")


def transactional_ids_list(transactional_ids: Iterable[str]) -> list[str]:
    """A ``Collection<String> transactionalIds`` as a list; a ``str`` is refused
    (``reject_str``)."""
    reject_str("transactional_ids", transactional_ids)
    return present(transactional_ids, "transactional_ids")


def describe_topics_args(form: int, keys: list[Any]) -> tuple[Any, ...]:
    """``Admin_describe_topics(h, form, names_or_ids, options)``'s arguments, the ids
    as Java's ``Uuid.toString()``."""
    targets = [str(key) for key in keys] if form == DESCRIBE_TOPIC_ID_COLLECTION else keys
    return (form, targets)


def describe_topics_result(native: Any, by_ids: bool, kind: Kind,
                           loop: asyncio.AbstractEventLoop | None) -> DescribeTopicsResult[Any]:
    """The ``DescribeTopicsResult`` over the C result: keyed by topic id for a
    ``TopicIdCollection``, else by name (Java's ``DescribeTopicsResult.ofTopicIds`` /
    ``ofTopicNames``)."""
    from confluent_kafka.admin.describe_topics_result import DescribeTopicsResult

    if by_ids:
        futures = wrap_map(kind, loop, native.futures("topic_id_values", _topic_description),
                           _uuid_key)
        return DescribeTopicsResult._new(topic_id_futures=futures, name_futures=None, kind=kind)
    futures = wrap_map(kind, loop, native.futures("topic_name_values", _topic_description))
    return DescribeTopicsResult._new(topic_id_futures=None, name_futures=futures, kind=kind)


def _cluster_node(raw: tuple[int, str, int, str | None, bool]) -> Node:
    """describeCluster's ``(id, host, port, rack, is_fenced)`` as Java's ``new Node(id,
    host, port, rack, isFenced)`` (``KafkaAdminClient.describeCluster``)."""
    from confluent_kafka.common.node import Node

    node_id, host, port, rack, is_fenced = raw
    return Node(id=node_id, host=host, port=port, rack=rack, is_fenced=bool(is_fenced))


def _cluster_nodes(rows: list[tuple[int, str, int, str | None, bool]]) -> list[Node]:
    return [_cluster_node(row) for row in rows]


def _cluster_controller(raw: tuple[int, str, int, str | None, bool] | None) -> Node | None:
    return None if raw is None else _cluster_node(raw)


def describe_cluster_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                            ) -> DescribeClusterResult[Any]:
    """The ``DescribeClusterResult`` over the C result's four futures."""
    from confluent_kafka.admin.describe_cluster_result import DescribeClusterResult

    return DescribeClusterResult._new(
        nodes=wrap(kind, loop, native.future("nodes", None, _cluster_nodes)),
        controller=wrap(kind, loop, native.future("controller", None, _cluster_controller)),
        cluster_id=wrap(kind, loop, native.future("cluster_id")),
        authorized_operations=wrap(kind, loop, native.future(
            "authorized_operations", None, acl_operations_from_codes)),
        kind=kind)


def _acl_binding_args(binding: AclBinding) -> tuple[Any, ...]:
    """An ``AclBinding`` as ``build_acl_arrays``' seven-tuple ``(resource_type, name,
    pattern_type, principal, host, operation, permission_type)``, the enums as codes."""
    pattern = binding.pattern()
    entry = binding.entry()
    return (pattern.resource_type().code(), pattern.name(), pattern.pattern_type().code(),
            entry.principal(), entry.host(), entry.operation().code(),
            entry.permission_type().code())


def _acl_binding_filter_args(acl_filter: AclBindingFilter) -> tuple[Any, ...]:
    """An ``AclBindingFilter`` as the same seven-tuple, Java's ``null`` strings
    (match any) ``None``."""
    pattern = acl_filter.pattern_filter()
    entry = acl_filter.entry_filter()
    return (pattern.resource_type().code(), pattern.name(), pattern.pattern_type().code(),
            entry.principal(), entry.host(), entry.operation().code(),
            entry.permission_type().code())


def _to_acl_binding(raw: tuple[Any, ...]) -> AclBinding:
    """``acl_binding_to_py``'s seven-tuple as an ``AclBinding``."""
    from confluent_kafka.common import (
        AccessControlEntry,
        AclBinding,
        AclOperation,
        AclPermissionType,
        PatternType,
        ResourcePattern,
        ResourceType,
    )

    resource_type, name, pattern_type, principal, host, operation, permission_type = raw
    return AclBinding(
        pattern=ResourcePattern(resource_type=ResourceType.from_code(code=resource_type),
                                name=name, pattern_type=PatternType.from_code(code=pattern_type)),
        entry=AccessControlEntry(principal=principal, host=host,
                                 operation=AclOperation.from_code(code=operation),
                                 permission_type=AclPermissionType.from_code(
                                     code=permission_type)))


def _to_acl_binding_filter(raw: tuple[Any, ...]) -> AclBindingFilter:
    """``acl_binding_filter_to_py``'s seven-tuple as an ``AclBindingFilter``."""
    from confluent_kafka.common import (
        AccessControlEntryFilter,
        AclBindingFilter,
        AclOperation,
        AclPermissionType,
        PatternType,
        ResourcePatternFilter,
        ResourceType,
    )

    resource_type, name, pattern_type, principal, host, operation, permission_type = raw
    return AclBindingFilter(
        pattern_filter=ResourcePatternFilter(
            resource_type=ResourceType.from_code(code=resource_type), name=name,
            pattern_type=PatternType.from_code(code=pattern_type)),
        entry_filter=AccessControlEntryFilter(
            principal=principal, host=host, operation=AclOperation.from_code(code=operation),
            permission_type=AclPermissionType.from_code(code=permission_type)))


def describe_acls_args(acl_filter: AclBindingFilter) -> tuple[Any, ...]:
    """``Admin_describe_acls(h, filter7, options)``'s plain arguments: the filter as
    ``_acl_binding_filter_args``' seven-tuple."""
    return (_acl_binding_filter_args(acl_filter),)


def _acl_bindings(rows: Iterable[tuple[Any, ...]]) -> list[AclBinding]:
    """The bindings ``values()`` delivers, as a ``list`` (Java's ``KafkaAdminClient``
    collects an ``ArrayList``)."""
    return [_to_acl_binding(raw) for raw in rows]


def describe_acls_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                         ) -> DescribeAclsResult[Any]:
    """The ``DescribeAclsResult`` over the C result's ``values()`` future."""
    from confluent_kafka.admin.describe_acls_result import DescribeAclsResult

    future = wrap(kind, loop, native.future("values", None, _acl_bindings))
    return DescribeAclsResult._new(future=future, kind=kind)


def create_acls_args(acls: Iterable[AclBinding]) -> tuple[Any, ...]:
    """``Admin_create_acls(h, acls, options)``'s plain arguments: each binding as
    ``_acl_binding_args``' seven-tuple."""
    return ([_acl_binding_args(element(acl, "acls")) for acl in acls],)


def create_acls_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                       ) -> CreateAclsResult[Any]:
    """The ``CreateAclsResult`` over the C result's per-binding ``values()``
    futures."""
    from confluent_kafka.admin.create_acls_result import CreateAclsResult

    futures = wrap_map(kind, loop, native.futures("values"), _to_acl_binding)
    return CreateAclsResult._new(futures=futures, kind=kind)


def delete_acls_args(filters: Iterable[AclBindingFilter]) -> tuple[Any, ...]:
    """``Admin_delete_acls(h, filters, options)``'s plain arguments: each filter as
    ``_acl_binding_filter_args``' seven-tuple."""
    return ([_acl_binding_filter_args(element(f, "filters")) for f in filters],)


def _filter_results(rows: Iterable[tuple[Any, Any]]) -> Any:
    """The ``FilterResults`` a filter's future delivers, from the C result's
    ``[(error, binding)]``: one ``FilterResult(binding, exception)`` per matched ACL,
    its error the borrowed error tuple of the extension. A matched ACL's error that
    is not an ``ApiError`` (Java's ``Errors.exception()`` always is) fails the
    filter's future (decision log 53)."""
    from confluent_kafka.admin.delete_acls_result import DeleteAclsResult

    values = []
    for raw_error, raw_binding in rows:
        error = borrowed_error(raw_error)
        if error is not None and not isinstance(error, ApiError):
            raise error
        values.append(DeleteAclsResult.FilterResult._new(
            binding=None if raw_binding is None else _to_acl_binding(raw_binding),
            exception=error))
    return DeleteAclsResult.FilterResults._new(values=values)


def delete_acls_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                       ) -> DeleteAclsResult[Any]:
    """The ``DeleteAclsResult`` over the C result's per-filter ``values()``
    futures."""
    from confluent_kafka.admin.delete_acls_result import DeleteAclsResult

    futures = wrap_map(kind, loop, native.futures("values", _filter_results),
                       _to_acl_binding_filter)
    return DeleteAclsResult._new(futures=futures, kind=kind)


def config_resource_from_tuple(raw: tuple[int, str]) -> ConfigResource:
    """The extension's ``(Type.id(), name)`` as Java's ``new ConfigResource(type,
    name)``."""
    from confluent_kafka.common.config.config_resource import ConfigResource

    type_id, name = raw
    return ConfigResource(type=ConfigResource.Type.for_id(id=type_id), name=name)


def describe_configs_args(resources: list[ConfigResource]) -> tuple[Any, ...]:
    """``Admin_describe_configs(h, resources, options)``'s arguments: each
    ``ConfigResource`` as ``(Type.id(), name)``."""
    return ([(element(resource, "resources").type().id(), resource.name())
             for resource in resources],)


def _config_entry(raw: tuple[Any, ...]) -> ConfigEntry:
    """The extension's ``(name, value, is_default, is_sensitive, is_read_only, source,
    type, documentation, synonyms)`` as Java's ``new ConfigEntry(name, value, source,
    isSensitive, readOnly, synonyms, type, documentation)``
    (``KafkaAdminClient.describeConfigs``); ``is_default`` is not a field, as Java
    derives ``isDefault()`` from the source. ``type`` is ``None`` for Java's null
    type, which the entries of a ``createTopics`` result carry."""
    from confluent_kafka.admin.config_entry import ConfigEntry

    (name, value, _is_default, is_sensitive, is_read_only, source, config_type,
     documentation, synonyms) = raw
    return ConfigEntry(
        name=name, value=value, source=ConfigEntry.ConfigSource(source),
        is_sensitive=bool(is_sensitive), is_read_only=bool(is_read_only),
        synonyms=[ConfigEntry.ConfigSynonym._new(
            name=synonym_name, value=synonym_value,
            source=ConfigEntry.ConfigSource(synonym_source))
            for synonym_name, synonym_value, synonym_source in synonyms],
        type=None if config_type is None else ConfigEntry.ConfigType(config_type),
        documentation=documentation)


def describe_configs_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                            ) -> DescribeConfigsResult[Any]:
    """The ``DescribeConfigsResult`` over the C result's ``values()``."""
    from confluent_kafka.admin.describe_configs_result import DescribeConfigsResult

    futures = wrap_map(kind, loop, native.futures("values", config_from_rows),
                       config_resource_from_tuple)
    return DescribeConfigsResult._new(futures=futures, kind=kind)


def incremental_alter_configs_args(configs: Mapping[ConfigResource, Iterable[AlterConfigOp]]
                                   ) -> tuple[Any, ...]:
    """``Admin_incremental_alter_configs(h, rows, options)``'s arguments: one row per
    resource, ``(Type.id(), name, [(configEntry().name(), value() or None,
    opType().id())])``, a resource with no op included (Java sends it with an empty
    config list)."""
    spec = []
    for resource, ops in configs.items():
        element(resource, "configs")  # nodeFor(resource): resource.type()
        element(ops, "configs")  # configs.get(resource): for (AlterConfigOp op : ops)
        spec.append((resource.type().id(), resource.name(),
                     [(element(op, "configs").config_entry().name(), op.config_entry().value(),
                       op.op_type().id())
                      for op in ops]))
    return (spec,)


def incremental_alter_configs_result(native: Any, kind: Kind,
                                     loop: asyncio.AbstractEventLoop | None
                                     ) -> AlterConfigsResult[Any]:
    """The ``AlterConfigsResult`` over the C result's ``values()``."""
    from confluent_kafka.admin.alter_configs_result import AlterConfigsResult

    futures = wrap_map(kind, loop, native.futures("values"), config_resource_from_tuple)
    return AlterConfigsResult._new(futures=futures, kind=kind)


def replica_from_tuple(raw: tuple[str, int, int]) -> TopicPartitionReplica:
    """The extension's ``(topic, partition, broker_id)`` as Java's
    ``new TopicPartitionReplica(topic, partition, brokerId)``."""
    from confluent_kafka.common.topic_partition_replica import TopicPartitionReplica

    topic, partition, broker_id = raw
    return TopicPartitionReplica(topic=topic, partition=partition, broker_id=broker_id)


def alter_replica_log_dirs_args(replica_assignment: Mapping[TopicPartitionReplica, str]
                                ) -> tuple[Any, ...]:
    """``Admin_alter_replica_log_dirs(h, rows, options)``'s arguments: each entry as
    ``(topic, partition, broker_id, log_dir)``."""
    return ([(element(replica, "replica_assignment").topic(), replica.partition(),
              replica.broker_id(), element(log_dir, "replica_assignment"))
             for replica, log_dir in replica_assignment.items()],)


def alter_replica_log_dirs_result(native: Any, kind: Kind,
                                  loop: asyncio.AbstractEventLoop | None
                                  ) -> AlterReplicaLogDirsResult[Any]:
    """The ``AlterReplicaLogDirsResult`` over the C result's ``values()``."""
    from confluent_kafka.admin.alter_replica_log_dirs_result import AlterReplicaLogDirsResult

    futures = wrap_map(kind, loop, native.futures("values"), replica_from_tuple)
    return AlterReplicaLogDirsResult._new(futures=futures, kind=kind)


def describe_log_dirs_args(brokers: list[int]) -> tuple[Any, ...]:
    """``Admin_describe_log_dirs(h, brokers, options)``'s arguments; a ``None``
    broker is Java's unboxing ``new ConstantNodeIdProvider(brokerId)``:
    ``NullPointerException``."""
    return ([element(broker, "brokers") for broker in brokers],)


def _log_dir_descriptions(dirs: Mapping[str, tuple[Any, ...]]) -> dict[str, LogDirDescription]:
    """The extension's ``{log_dir: (error, total_bytes, usable_bytes, replicas,
    is_cordoned)}`` as the broker's ``dict[str, LogDirDescription]``: Java's
    ``new LogDirDescription(error, replicaInfos, totalBytes, usableBytes,
    isCordoned)`` (``KafkaAdminClient.java:3178``), ``-1`` the core's unknown volume
    size. A log dir error that is not an ``ApiError`` fails the broker's future."""
    from confluent_kafka.admin.log_dir_description import LogDirDescription
    from confluent_kafka.admin.replica_info import ReplicaInfo
    from confluent_kafka.common.topic_partition import TopicPartition

    descriptions = {}
    for log_dir, (dir_error, total_bytes, usable_bytes, replicas, is_cordoned) in dirs.items():
        embedded = borrowed_error(dir_error)
        if embedded is not None and not isinstance(embedded, ApiError):
            raise embedded
        descriptions[log_dir] = LogDirDescription(
            error=embedded,
            replica_infos={TopicPartition(topic=topic, partition=partition):
                           ReplicaInfo(size=size, offset_lag=offset_lag,
                                       is_future=bool(is_future))
                           for topic, partition, size, offset_lag, is_future in replicas},
            total_bytes=total_bytes, usable_bytes=usable_bytes, is_cordoned=bool(is_cordoned))
    return descriptions


def describe_log_dirs_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                             ) -> DescribeLogDirsResult[Any]:
    """The ``DescribeLogDirsResult`` over the C result's ``descriptions()``, keyed by
    broker id."""
    from confluent_kafka.admin.describe_log_dirs_result import DescribeLogDirsResult

    futures = wrap_map(kind, loop, native.futures("descriptions", _log_dir_descriptions))
    return DescribeLogDirsResult._new(futures=futures, kind=kind)


def describe_replica_log_dirs_args(replicas: list[TopicPartitionReplica]) -> tuple[Any, ...]:
    """``Admin_describe_replica_log_dirs(h, replicas, options)``'s arguments: each
    replica as ``(topic, partition, broker_id)``."""
    return ([(element(replica, "replicas").topic(), replica.partition(), replica.broker_id())
             for replica in replicas],)


def _replica_log_dir_info(info: tuple[Any, ...]) -> Any:
    """The extension's ``(current_dir, current_lag, future_dir, future_lag)`` as
    Java's ``new ReplicaLogDirInfo(currentReplicaLogDir, currentReplicaOffsetLag,
    futureReplicaLogDir, futureReplicaOffsetLag)``."""
    from confluent_kafka.admin.describe_replica_log_dirs_result import (
        DescribeReplicaLogDirsResult,
    )

    current_dir, current_lag, future_dir, future_lag = info
    return DescribeReplicaLogDirsResult.ReplicaLogDirInfo._new(
        current_replica_log_dir=current_dir, current_replica_offset_lag=current_lag,
        future_replica_log_dir=future_dir, future_replica_offset_lag=future_lag)


def describe_replica_log_dirs_result(native: Any, kind: Kind,
                                     loop: asyncio.AbstractEventLoop | None
                                     ) -> DescribeReplicaLogDirsResult[Any]:
    """The ``DescribeReplicaLogDirsResult`` over the C result's ``values()``."""
    from confluent_kafka.admin.describe_replica_log_dirs_result import (
        DescribeReplicaLogDirsResult,
    )

    futures = wrap_map(kind, loop, native.futures("values", _replica_log_dir_info),
                       replica_from_tuple)
    return DescribeReplicaLogDirsResult._new(futures=futures, kind=kind)


def create_partitions_args(new_partitions: Mapping[str, NewPartitions]) -> tuple[Any, ...]:
    """``Admin_create_partitions(h, rows, options)``'s arguments: each entry as
    ``(topic, totalCount(), assignments() or None)`` (``None`` is Java's
    ``increaseTo(int)``, an empty list ``increaseTo(int, List.of())``)."""
    return ([(topic, element(partitions, "new_partitions").total_count(),
              partitions.assignments())
             for topic, partitions in new_partitions.items()],)


def create_partitions_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                             ) -> CreatePartitionsResult[Any]:
    """The ``CreatePartitionsResult`` over the C result's ``values()``."""
    from confluent_kafka.admin.create_partitions_result import CreatePartitionsResult

    return CreatePartitionsResult._new(values=wrap_map(kind, loop, native.futures("values")),
                                       kind=kind)


def delete_records_args(records_to_delete: Mapping[TopicPartition, RecordsToDelete]
                        ) -> tuple[Any, ...]:
    """``Admin_delete_records(h, rows, options)``'s arguments: each entry as
    ``(topic, partition, beforeOffset())``."""
    return ([(element(partition, "records_to_delete").topic(), partition.partition(),
              element(records, "records_to_delete").before_offset())
             for partition, records in records_to_delete.items()],)


def _deleted_records(low_watermark: int) -> DeletedRecords:
    from confluent_kafka.admin.deleted_records import DeletedRecords

    return DeletedRecords(low_watermark=low_watermark)


def delete_records_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                          ) -> DeleteRecordsResult[Any]:
    """The ``DeleteRecordsResult`` over the C result's ``lowWatermarks()``."""
    from confluent_kafka.admin.delete_records_result import DeleteRecordsResult

    futures = wrap_map(kind, loop, native.futures("low_watermarks", _deleted_records),
                       topic_partition_from_tuple)
    return DeleteRecordsResult._new(futures=futures, kind=kind)


def _principal_args(principal: KafkaPrincipal) -> tuple[str, str]:
    """A ``KafkaPrincipal`` as ``build_principal_arrays``' ``(principal_type, name)``."""
    return (principal.principal_type(), principal.name())


def _to_kafka_principal(raw: tuple[str, str, bool]) -> KafkaPrincipal:
    """``kafka_principal_to_py``'s ``(principal_type, name, token_authenticated)``."""
    from confluent_kafka.common import KafkaPrincipal

    principal_type, name, token_authenticated = raw
    return KafkaPrincipal(principal_type=principal_type, name=name,
                          token_authenticated=bool(token_authenticated))


def _to_delegation_token(raw: tuple[Any, ...]) -> DelegationToken:
    """``delegation_token_to_py``'s ``(token_id, owner, requester, renewers, issue,
    expiry, max, hmac, hmac_base64)`` as a ``DelegationToken``, as Java's
    ``KafkaAdminClient`` builds it (``new TokenInformation(tokenId, owner,
    tokenRequester, renewers, issue, max, expiry)``); the class computes the base64
    form itself."""
    from confluent_kafka.common import DelegationToken, TokenInformation

    (token_id, owner, requester, renewers, issue_timestamp, expiry_timestamp,
     max_timestamp, hmac, _hmac_base64) = raw
    common = {"token_id": token_id, "owner": _to_kafka_principal(owner),
              "renewers": [_to_kafka_principal(r) for r in renewers],
              "issue_timestamp": issue_timestamp, "max_timestamp": max_timestamp,
              "expiry_timestamp": expiry_timestamp}
    # No requester from the core: Java's shorter constructor (the requester is the
    # owner), as before.
    info = (TokenInformation(**common) if requester is None
            else TokenInformation(token_requester=_to_kafka_principal(requester), **common))
    return DelegationToken(token_information=info, hmac=hmac)


def create_delegation_token_options(options: CreateDelegationTokenOptions
                                    ) -> list[tuple[str, Any]]:
    """The ``(setter, value)`` pairs of a ``CreateDelegationTokenOptions``: the
    renewers as ``(principal_type, name)`` pairs and the owner as one (``None`` for
    none), read back from the options' own getters. A ``None`` renewers, which the
    setter stores as Java's does, raises where Java iterates it
    (``KafkaAdminClient.createDelegationToken``: ``for (KafkaPrincipal principal :
    options.renewers())``)."""
    renewers = options.renewers()
    if renewers is None:
        raise TypeError("CreateDelegationTokenOptions.renewers must not be None")
    stored: dict[str, Callable[[], Any]] = {
        "renewers": lambda: [_principal_args(p) for p in renewers],
        "owner": lambda: _optional_principal_args(options.owner())}
    return [(setter, stored[setter]() if setter in stored else value)
            for setter, value in options._setters_ran()]


def _optional_principal_args(principal: KafkaPrincipal | None) -> tuple[str, str] | None:
    return None if principal is None else _principal_args(principal)


def create_delegation_token_result(native: Any, kind: Kind,
                                   loop: asyncio.AbstractEventLoop | None
                                   ) -> CreateDelegationTokenResult[Any]:
    """The ``CreateDelegationTokenResult`` over the C result's ``delegationToken()``
    future."""
    from confluent_kafka.admin.create_delegation_token_result import (
        CreateDelegationTokenResult,
    )

    future = wrap(kind, loop, native.future("delegation_token", None, _to_delegation_token))
    return CreateDelegationTokenResult._new(delegation_token=future, kind=kind)


def renew_delegation_token_result(native: Any, kind: Kind,
                                  loop: asyncio.AbstractEventLoop | None
                                  ) -> RenewDelegationTokenResult[Any]:
    """The ``RenewDelegationTokenResult`` over the C result's ``expiryTimestamp()``
    future."""
    from confluent_kafka.admin.renew_delegation_token_result import RenewDelegationTokenResult

    future = wrap(kind, loop, native.future("expiry_timestamp"))
    return RenewDelegationTokenResult._new(expiry_timestamp=future, kind=kind)


def expire_delegation_token_result(native: Any, kind: Kind,
                                   loop: asyncio.AbstractEventLoop | None
                                   ) -> ExpireDelegationTokenResult[Any]:
    """The ``ExpireDelegationTokenResult`` over the C result's ``expiryTimestamp()``
    future."""
    from confluent_kafka.admin.expire_delegation_token_result import (
        ExpireDelegationTokenResult,
    )

    future = wrap(kind, loop, native.future("expiry_timestamp"))
    return ExpireDelegationTokenResult._new(expiry_timestamp=future, kind=kind)


def describe_delegation_token_options(options: DescribeDelegationTokenOptions
                                      ) -> list[tuple[str, Any]]:
    """The ``(setter, value)`` pairs of a ``DescribeDelegationTokenOptions``: the
    owners as ``(principal_type, name)`` pairs, ``None`` for Java's ``null`` (every
    owner), read back from the options' own getter."""
    def owners() -> list[tuple[str, str]] | None:
        stored = options.owners()
        return None if stored is None else [_principal_args(p) for p in stored]

    return [(setter, owners() if setter == "owners" else value)
            for setter, value in options._setters_ran()]


def _delegation_tokens(rows: Iterable[tuple[Any, ...]]) -> list[DelegationToken]:
    """The tokens ``delegationTokens()`` delivers, as a ``list`` (Java's
    ``ArrayList``)."""
    return [_to_delegation_token(raw) for raw in rows]


def describe_delegation_token_result(native: Any, kind: Kind,
                                     loop: asyncio.AbstractEventLoop | None
                                     ) -> DescribeDelegationTokenResult[Any]:
    """The ``DescribeDelegationTokenResult`` over the C result's
    ``delegationTokens()`` future."""
    from confluent_kafka.admin.describe_delegation_token_result import (
        DescribeDelegationTokenResult,
    )

    future = wrap(kind, loop, native.future("delegation_tokens", None, _delegation_tokens))
    return DescribeDelegationTokenResult._new(delegation_tokens=future, kind=kind)


def _group_member_assignment(raw: list[tuple[str, int]] | None) -> MemberAssignment | None:
    """A member's ``MemberAssignment`` from its ``[(topic, partition), ...]``,
    ``None`` for an absent one (``Optional.empty()``)."""
    from confluent_kafka.admin.member_assignment import MemberAssignment
    from confluent_kafka.common import TopicPartition

    if raw is None:
        return None
    return MemberAssignment(topic_partitions=[
        TopicPartition(topic=topic, partition=partition) for topic, partition in raw])


def _group_member(raw: tuple[Any, ...]) -> MemberDescription:
    """A ``MemberDescription`` from the extension's ``(consumer_id,
    group_instance_id, rack_id, client_id, host, assignment, target_assignment,
    member_epoch, upgraded)`` (``member_description_to_py``)."""
    from confluent_kafka.admin.member_description import MemberDescription

    (consumer_id, group_instance_id, rack_id, client_id, host, assignment, target_assignment,
     member_epoch, upgraded) = raw
    return MemberDescription(
        member_id=consumer_id, group_instance_id=group_instance_id, rack_id=rack_id,
        client_id=client_id, host=host, assignment=_group_member_assignment(assignment),
        target_assignment=_group_member_assignment(target_assignment),
        member_epoch=member_epoch, upgraded=upgraded)


def _consumer_group_description(raw: tuple[Any, ...]) -> ConsumerGroupDescription:
    """A ``ConsumerGroupDescription`` from the extension's ``(group_id, is_simple,
    members, partition_assignor, type, state, coordinator, authorized_operations,
    group_epoch, target_assignment_epoch)``
    (``consumer_group_description_to_py``); the type and state names are Java's
    ``toString()`` values, read back with ``parse``."""
    from confluent_kafka.admin.consumer_group_description import ConsumerGroupDescription
    from confluent_kafka.common import GroupState, GroupType

    (group_id, is_simple, members, partition_assignor, group_type, group_state, coordinator,
     operations, group_epoch, target_assignment_epoch) = raw
    return ConsumerGroupDescription(
        group_id=group_id, is_simple_consumer_group=bool(is_simple),
        members=[_group_member(member) for member in members],
        partition_assignor=partition_assignor, type=GroupType.parse(name=group_type),
        group_state=GroupState.parse(name=group_state),
        coordinator=optional_node_from_tuple(coordinator),
        authorized_operations=acl_operations_from_codes(operations), group_epoch=group_epoch,
        target_assignment_epoch=target_assignment_epoch)


def describe_consumer_groups_result(native: Any, kind: Kind,
                                    loop: asyncio.AbstractEventLoop | None
                                    ) -> DescribeConsumerGroupsResult[Any]:
    """The ``DescribeConsumerGroupsResult`` over the C result's
    ``describedGroups()`` futures."""
    from confluent_kafka.admin.describe_consumer_groups_result import (
        DescribeConsumerGroupsResult,
    )

    futures: dict[str, Any] = wrap_map(
        kind, loop, native.futures("described_groups", _consumer_group_description))
    return DescribeConsumerGroupsResult._new(futures=futures, kind=kind)


def list_consumer_group_offsets_args(
        group_id: str | None,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec] | None) -> tuple[Any, ...]:
    """``Admin_list_consumer_group_offsets(h, group_id, group_specs, options)``'s
    inputs: the group id of Java's ``(groupId, options)`` form, or each entry of its
    ``(groupSpecs, options)`` form as ``(group_id, [(topic, partition), ...])``,
    ``None`` partitions for Java's ``topicPartitions() == null`` (every partition of
    the group)."""
    if group_specs is MISSING:  # the (groupId, options) form
        return (group_id, None)
    assert group_specs is not None  # the caller's non_null
    rows = []
    for spec_group_id, spec in group_specs.items():
        if spec_group_id is None:  # a later-path null: CoordinatorStrategy's group id
            raise TypeError("group_specs must not contain None")
        if spec is None:  # the handler's groupSpecs.get(groupId).topicPartitions()
            raise TypeError("group_specs must not contain None")
        partitions = spec.topic_partitions()
        rows.append((spec_group_id, None if partitions is None
                     else [(tp.topic(), tp.partition()) for tp in partitions]))
    return (None, rows)


def _group_offsets(raw: Mapping[tuple[str, int], tuple[int, str, int | None] | None]
                   ) -> dict[TopicPartition, OffsetAndMetadata | None]:
    """``{TopicPartition: OffsetAndMetadata | None}`` from the extension's
    ``{(topic, partition): (offset, metadata, leader_epoch) | None}``, ``None`` being
    Java's ``null`` value: no committed offset."""
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.consumer import OffsetAndMetadata

    return {
        TopicPartition(topic=topic, partition=partition):
            None if value is None
            else OffsetAndMetadata(offset=value[0], leader_epoch=value[2], metadata=value[1])
        for (topic, partition), value in raw.items()}


def list_consumer_group_offsets_result(group_ids: Iterable[str], native: Any, kind: Kind,
                                       loop: asyncio.AbstractEventLoop | None
                                       ) -> ListConsumerGroupOffsetsResult[Any]:
    """The ``ListConsumerGroupOffsetsResult`` over the C result: each requested
    group's future is the C ``partitionsToOffsetAndMetadata(groupId)``, keyed by
    Java's ``CoordinatorKey.byGroupId``."""
    from confluent_kafka.admin._internals import _CoordinatorKey
    from confluent_kafka.admin.list_consumer_group_offsets_result import (
        ListConsumerGroupOffsetsResult,
    )

    futures: dict[_CoordinatorKey, Any] = {
        _CoordinatorKey.by_group_id(group_id=group_id): wrap(kind, loop, native.future(
            "partitions_to_offset_and_metadata_with_group_id", group_id, _group_offsets))
        for group_id in dict.fromkeys(group_ids)}
    return ListConsumerGroupOffsetsResult._new(futures=futures, kind=kind)


def delete_consumer_groups_result(native: Any, kind: Kind,
                                  loop: asyncio.AbstractEventLoop | None
                                  ) -> DeleteConsumerGroupsResult[Any]:
    """The ``DeleteConsumerGroupsResult`` over the C result's ``deletedGroups()``
    futures."""
    from confluent_kafka.admin.delete_consumer_groups_result import DeleteConsumerGroupsResult

    futures: dict[str, Any] = wrap_map(kind, loop, native.futures("deleted_groups"))
    return DeleteConsumerGroupsResult._new(futures=futures, kind=kind)


def outcomes(kind: Kind, loop: asyncio.AbstractEventLoop | None,
             futures: Mapping[_T, KafkaFuture[Any] | AsyncKafkaFuture[Any]]
             ) -> KafkaFuture[dict[_T, BaseException | None]] | AsyncKafkaFuture[
                 dict[_T, BaseException | None]]:
    """One future of every key's outcome as it stands: ``{key: None}`` for a key
    whose future succeeded, ``{key: error}`` for one that failed, complete once every
    key's future is. It is Java's ``Map<K, Errors>`` future of a result that holds
    one, read from the core's per-key futures (``partitionResult(partition)``), each
    of which already completes as Java's result completes that key: with the key's
    ``Errors.exception()``, with the failure of the call, which every key then
    carries, or with the result's own ``IllegalArgumentException`` for a key the
    response left out. A key's error kept as it stands answers each of these the
    way Java's result answers it, with no need to tell them apart."""
    impl: _KafkaFutureImpl[dict[_T, BaseException | None]] = _KafkaFutureImpl()
    errors: dict[_T, BaseException | None] = {}
    lock = threading.Lock()
    remaining = len(futures)

    def settle(key: _T, state: _FutureState[Any]) -> None:
        nonlocal remaining
        with lock:
            errors[key] = None if state._state == _VALUE else state._outcome
            remaining -= 1
            done = remaining == 0
        if done:
            impl.complete({k: errors[k] for k in futures})

    def listener(key: _T) -> Callable[[_FutureState[Any]], None]:
        return lambda state: settle(key, state)

    for key, future in futures.items():
        future._python_state()._add_listener(listener(key))
    if kind is Async:
        return AsyncKafkaFuture._of_impl(impl, loop)
    return KafkaFuture._of_impl(impl)


def delete_consumer_group_offsets_args(group_id: str, partitions: Iterable[TopicPartition]
                                       ) -> tuple[Any, ...]:
    """``Admin_delete_consumer_group_offsets(h, group_id, partitions, options)``'s
    inputs: each partition as ``(topic, partition)``."""
    return (group_id, [(element(tp, "partitions").topic(), tp.partition())
                       for tp in partitions])


def delete_consumer_group_offsets_result(partitions: Iterable[TopicPartition], native: Any,
                                         kind: Kind, loop: asyncio.AbstractEventLoop | None
                                         ) -> DeleteConsumerGroupOffsetsResult[Any]:
    """The ``DeleteConsumerGroupOffsetsResult`` over the C result: Java's ``(future,
    partitions)``, its future the :func:`outcomes` of the C
    ``partitionResult(partition)`` of each requested partition. With no partition
    there is no per-key future: Java's future then fails as the call fails, which is
    the C ``all()``, and is otherwise empty."""
    from confluent_kafka.admin.delete_consumer_group_offsets_result import (
        DeleteConsumerGroupOffsetsResult,
    )

    requested = list(dict.fromkeys(partitions))
    if requested:
        future: Any = outcomes(kind, loop, {
            tp: wrap(kind, loop, native.future("partition_result", (tp.topic(), tp.partition())))
            for tp in requested})
    else:
        future = wrap(kind, loop, native.future("all")).then_apply(function=lambda _: {})
    return DeleteConsumerGroupOffsetsResult._new(future=future, partitions=requested, kind=kind)


def list_groups_options(options: Any) -> list[tuple[str, Any]]:
    """The ``(setter, value)`` pairs of a ``ListGroupsOptions``: the set filters as
    their stored sets (the copies Java's setters store), each ``GroupState`` /
    ``GroupType`` as its ``toString()`` name, which the extension parses back
    (``GroupState.parse``, ``GroupType.parse``)."""
    stored: dict[str, Callable[[], list[str]]] = {
        "in_group_states": lambda: [str(state) for state in options.group_states()],
        "with_types": lambda: [str(group_type) for group_type in options.types()],
        "with_protocol_types": lambda: list(options.protocol_types())}
    return [(setter, stored[setter]() if setter in stored else value)
            for setter, value in options._setters_ran()]


def _group_listing(raw: tuple[Any, ...]) -> GroupListing:
    """A ``GroupListing`` from the extension's ``(group_id, group_type, protocol,
    group_state, is_simple)``, the names Java's ``toString()`` values (``None`` for
    ``Optional.empty()``); ``is_simple`` is ``GroupListing``'s own computation and
    is not read."""
    from confluent_kafka.admin.group_listing import GroupListing
    from confluent_kafka.common import GroupState, GroupType

    group_id, group_type, protocol, group_state, _is_simple = raw
    return GroupListing(group_id=group_id,
                        type=None if group_type is None else GroupType.parse(name=group_type),
                        protocol=protocol,
                        group_state=None if group_state is None else GroupState.parse(
                            name=group_state))


def _group_listings(raw: list[tuple[Any, ...]]) -> list[GroupListing]:
    return [_group_listing(row) for row in raw]


def _errors(raw: list[tuple[Any, ...]]) -> list[BaseException]:
    return [from_borrowed_ffi_error(row) for row in raw]


def list_groups_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                       ) -> ListGroupsResult[Any]:
    """The ``ListGroupsResult`` over the C result's ``all()``, ``valid()`` and
    ``errors()`` futures."""
    from confluent_kafka.admin.list_groups_result import ListGroupsResult

    return ListGroupsResult._of_client(
        all=wrap(kind, loop, native.future("all", None, _group_listings)),
        valid=wrap(kind, loop, native.future("valid", None, _group_listings)),
        errors=wrap(kind, loop, native.future("errors", None, _errors)), kind=kind)


def partitions_spec(partitions: Iterable[TopicPartition]) -> list[tuple[str, int]]:
    """A ``Set<TopicPartition>`` as the extension's ``(topic, partition)`` rows."""
    return [(element(partition, "partitions").topic(), partition.partition())
            for partition in partitions]


def elect_leaders_args(election_type: ElectionType, partitions: Iterable[TopicPartition] | None
                       ) -> tuple[Any, ...]:
    """``Admin_elect_leaders(h, election_type, partitions, options)``'s arguments:
    ``ElectionType.value``; Java's ``null`` partitions (every partition) as
    ``None``."""
    return (election_type.value, None if partitions is None else partitions_spec(partitions))


def _elections(rows: Mapping[tuple[str, int], Any]
               ) -> dict[TopicPartition, BaseException | None]:
    """The extension's ``{(topic, partition): error or None}`` as Java's
    ``Map<TopicPartition, Optional<Throwable>>``."""
    return {topic_partition_from_tuple(key): borrowed_error(raw_error)
            for key, raw_error in rows.items()}


def elect_leaders_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                         ) -> ElectLeadersResult[Any]:
    """The ``ElectLeadersResult`` over the C result's ``partitions()`` future."""
    from confluent_kafka.admin.elect_leaders_result import ElectLeadersResult

    future = wrap(kind, loop, native.future("partitions", None, _elections))
    return ElectLeadersResult._new(election_future=future, kind=kind)


def _reassignment_key(partition: TopicPartition | None) -> TopicPartition:
    """A key of ``alterPartitionReassignments``' map: Java's
    ``entry.getKey().topic()`` dereferences it; its value may be ``None`` (an
    empty ``Optional``: cancel)."""
    if partition is None:
        raise TypeError("reassignments must not contain a None key")
    return partition


def alter_partition_reassignments_args(
        reassignments: Mapping[TopicPartition, NewPartitionReassignment | None]
) -> tuple[Any, ...]:
    """``Admin_alter_partition_reassignments(h, rows, options)``'s arguments: each
    entry as ``(topic, partition, targetReplicas())``, ``None`` for Java's
    ``Optional.empty()`` (cancel the reassignment)."""
    return ([(_reassignment_key(partition).topic(), partition.partition(),
              None if reassignment is None else list(reassignment.target_replicas()))
             for partition, reassignment in reassignments.items()],)


def alter_partition_reassignments_result(native: Any, kind: Kind,
                                         loop: asyncio.AbstractEventLoop | None
                                         ) -> AlterPartitionReassignmentsResult[Any]:
    """The ``AlterPartitionReassignmentsResult`` over the C result's ``values()``."""
    from confluent_kafka.admin.alter_partition_reassignments_result import (
        AlterPartitionReassignmentsResult,
    )

    futures = wrap_map(kind, loop, native.futures("values"), topic_partition_from_tuple)
    return AlterPartitionReassignmentsResult._new(futures=futures, kind=kind)


def list_partition_reassignments_args(partitions: Iterable[TopicPartition] | None
                                      ) -> tuple[Any, ...]:
    """``Admin_list_partition_reassignments(h, partitions, options)``'s arguments:
    ``None`` for Java's ``listPartitionReassignments(options)`` (every
    reassignment)."""
    return (None if partitions is None else partitions_spec(partitions),)


def _reassignments(rows: Mapping[tuple[str, int], tuple[list[int], list[int], list[int]]]
                   ) -> dict[TopicPartition, PartitionReassignment]:
    """The extension's ``{(topic, partition): (replicas, adding, removing)}`` as
    Java's ``Map<TopicPartition, PartitionReassignment>``."""
    from confluent_kafka.admin.partition_reassignment import PartitionReassignment

    return {topic_partition_from_tuple(key): PartitionReassignment(
                replicas=replicas, adding_replicas=adding, removing_replicas=removing)
            for key, (replicas, adding, removing) in rows.items()}


def list_partition_reassignments_result(native: Any, kind: Kind,
                                        loop: asyncio.AbstractEventLoop | None
                                        ) -> ListPartitionReassignmentsResult[Any]:
    """The ``ListPartitionReassignmentsResult`` over the C result's
    ``reassignments()`` future."""
    from confluent_kafka.admin.list_partition_reassignments_result import (
        ListPartitionReassignmentsResult,
    )

    future = wrap(kind, loop, native.future("reassignments", None, _reassignments))
    return ListPartitionReassignmentsResult._new(reassignments=future, kind=kind)


def remove_members_from_consumer_group_args(group_id: str,
                                            options: RemoveMembersFromConsumerGroupOptions
                                            ) -> tuple[Any, ...]:
    """``Admin_remove_members_from_consumer_group(h, group_id, members, options)``'s
    inputs: the members' group instance ids (Java's ``MemberToRemove`` holds nothing
    else), ``None`` in ``removeAll`` mode (Java's ``new
    RemoveMembersFromConsumerGroupOptions()``)."""
    if options.remove_all():
        return (group_id, None)
    return (group_id, [member.group_instance_id() for member in options.members()])


def remove_members_from_consumer_group_result(
        options: RemoveMembersFromConsumerGroupOptions, native: Any, kind: Kind,
        loop: asyncio.AbstractEventLoop | None) -> RemoveMembersFromConsumerGroupResult[Any]:
    """The ``RemoveMembersFromConsumerGroupResult`` over the C result: each call of
    ``member_result`` and ``all`` reads the C ``memberResult(member)`` / ``all()``,
    which carry Java's outcome and raise Java's ``IllegalArgumentException`` for a
    member the result has no future of."""
    from confluent_kafka.admin.remove_members_from_consumer_group_result import (
        RemoveMembersFromConsumerGroupResult,
    )

    def member_result(member: MemberToRemove) -> Any:
        return wrap(kind, loop, native.future("member_result", member.group_instance_id()))

    return RemoveMembersFromConsumerGroupResult._of_client(
        member_result=member_result, all=lambda: wrap(kind, loop, native.future("all")),
        member_infos=options.members(), kind=kind)


def alter_consumer_group_offsets_args(group_id: str,
                                      offsets: Mapping[TopicPartition, OffsetAndMetadata]
                                      ) -> tuple[Any, ...]:
    """``Admin_alter_consumer_group_offsets(h, group_id, offsets, options)``'s inputs:
    each entry as ``(topic, partition, offset, metadata, leader_epoch)``, ``-1`` for
    an absent leader epoch."""
    rows = []
    for tp, offset_and_metadata in offsets.items():
        element(tp, "offsets")
        leader_epoch = element(offset_and_metadata, "offsets").leader_epoch()
        rows.append((tp.topic(), tp.partition(), offset_and_metadata.offset(),
                     offset_and_metadata.metadata(),
                     -1 if leader_epoch is None else leader_epoch))
    return (group_id, rows)


def alter_consumer_group_offsets_result(native: Any, kind: Kind,
                                        loop: asyncio.AbstractEventLoop | None
                                        ) -> AlterConsumerGroupOffsetsResult[Any]:
    """The ``AlterConsumerGroupOffsetsResult`` over the C result: each call of
    ``partition_result`` and ``all`` reads the C ``partitionResult(partition)`` /
    ``all()``."""
    from confluent_kafka.admin.alter_consumer_group_offsets_result import (
        AlterConsumerGroupOffsetsResult,
    )

    def partition_result(partition: TopicPartition) -> Any:
        return wrap(kind, loop, native.future(
            "partition_result", (partition.topic(), partition.partition())))

    return AlterConsumerGroupOffsetsResult._of_client(
        partition_result=partition_result,
        all=lambda: wrap(kind, loop, native.future("all")), kind=kind)


def offset_spec_value(spec: OffsetSpec) -> tuple[bool, int]:
    """An ``OffsetSpec`` as the extension's ``(is_timestamp, value)``: the timestamp of
    a ``TimestampSpec``, else the wire sentinel of Java's
    ``KafkaAdminClient.getOffsetFromSpec`` (``KafkaAdminClient.java:5176``), whose
    ``instanceof`` chain this follows, any other spec reading as ``latest()``."""
    from confluent_kafka.admin.offset_spec import OffsetSpec

    if isinstance(spec, OffsetSpec.TimestampSpec):
        return True, spec._timestamp
    if isinstance(spec, OffsetSpec.EarliestSpec):
        return False, -2  # ListOffsetsRequest.EARLIEST_TIMESTAMP
    if isinstance(spec, OffsetSpec.MaxTimestampSpec):
        return False, -3  # ListOffsetsRequest.MAX_TIMESTAMP
    if isinstance(spec, OffsetSpec.EarliestLocalSpec):
        return False, -4  # ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP
    if isinstance(spec, OffsetSpec.LatestTieredSpec):
        return False, -5  # ListOffsetsRequest.LATEST_TIERED_TIMESTAMP
    if isinstance(spec, OffsetSpec.EarliestPendingUploadSpec):
        return False, -6  # ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP
    return False, -1  # ListOffsetsRequest.LATEST_TIMESTAMP


def list_offsets_args(topic_partition_offsets: Mapping[TopicPartition, OffsetSpec]
                      ) -> tuple[Any, ...]:
    """``Admin_list_offsets(h, rows, isolation_level, options)``'s first argument:
    each entry as ``(topic, partition, is_timestamp, value)``. The caller adds the
    options' ``isolationLevel().id()`` once it has read ``options``, after these
    rows, as Java builds its offset queries before ``new ListOffsetsHandler(…,
    options, …)``."""
    spec = [(element(partition, "topic_partition_offsets").topic(), partition.partition(),
             *offset_spec_value(offset_spec))
            for partition, offset_spec in topic_partition_offsets.items()]
    return (spec,)


def _listed_offset(info: tuple[int, int, int | None]) -> Any:
    """The extension's ``(offset, timestamp, leader_epoch or None)`` as Java's
    ``ListOffsetsResultInfo``."""
    from confluent_kafka.admin.list_offsets_result import ListOffsetsResult

    offset, timestamp, leader_epoch = info
    return ListOffsetsResult.ListOffsetsResultInfo(
        offset=offset, timestamp=timestamp, leader_epoch=leader_epoch)


def list_offsets_result(native: Any, partitions: Iterable[TopicPartition], kind: Kind,
                        loop: asyncio.AbstractEventLoop | None) -> ListOffsetsResult[Any]:
    """The ``ListOffsetsResult`` over the C result: one future per requested
    partition, the core's ``partitionResult(partition)``."""
    from confluent_kafka.admin.list_offsets_result import ListOffsetsResult

    futures = {partition: wrap(kind, loop, native.future(
                   "partition_result", (partition.topic(), partition.partition()),
                   _listed_offset))
               for partition in partitions}
    return ListOffsetsResult._new(futures=futures, kind=kind)


# ``DescribeClientQuotasRequest.MATCH_TYPE_EXACT`` / ``_DEFAULT`` / ``_SPECIFIED``:
# the match-type column of a filter component the extension reads.
_MATCH_TYPE_EXACT = 0
_MATCH_TYPE_DEFAULT = 1
_MATCH_TYPE_SPECIFIED = 2


def describe_client_quotas_args(quota_filter: ClientQuotaFilter) -> tuple[Any, ...]:
    """``Admin_describe_client_quotas(h, components, strict, options)``'s plain
    arguments: each component as ``(entity_type, match_type, name or None)``, the
    match type as ``DescribeClientQuotasRequest`` picks it: ``null`` ``match()`` (the
    private ``_match_is_null``, decision log 25) specified, an empty one default,
    else exact."""
    components: list[tuple[str, int, str | None]] = []
    for component in quota_filter.components():
        match = component.match()
        if component._match_is_null:
            components.append((component.entity_type(), _MATCH_TYPE_SPECIFIED, None))
        elif match is None:
            components.append((component.entity_type(), _MATCH_TYPE_DEFAULT, None))
        else:
            components.append((component.entity_type(), _MATCH_TYPE_EXACT, match))
    return (components, quota_filter.strict())


def _to_client_quota_entity(pairs: Iterable[tuple[str, str | None]]) -> ClientQuotaEntity:
    """``client_quota_entity_to_py``'s ``(entity_type, name or None)`` pairs."""
    from confluent_kafka.common import ClientQuotaEntity

    return ClientQuotaEntity(entries=dict(pairs))


def _quota_entities(rows: Mapping[Any, list[tuple[str, float]]]
                    ) -> dict[ClientQuotaEntity, dict[str, float]]:
    """``entities()``' value: each entity's quota values by key."""
    return {_to_client_quota_entity(pairs): dict(quotas) for pairs, quotas in rows.items()}


def describe_client_quotas_result(native: Any, kind: Kind,
                                  loop: asyncio.AbstractEventLoop | None
                                  ) -> DescribeClientQuotasResult[Any]:
    """The ``DescribeClientQuotasResult`` over the C result's ``entities()``
    future."""
    from confluent_kafka.admin.describe_client_quotas_result import DescribeClientQuotasResult

    future = wrap(kind, loop, native.future("entities", None, _quota_entities))
    return DescribeClientQuotasResult._new(entities=future, kind=kind)


def alter_client_quotas_args(entries: Iterable[ClientQuotaAlteration]) -> tuple[Any, ...]:
    """``Admin_alter_client_quotas(h, entries, options)``'s plain arguments: each
    alteration as ``(entity, ops)``, the entity's ``(type, name or None)`` pairs and
    the ops' ``(key, value or None)``, ``None`` removing the quota."""
    rows = [(list(element(entry, "entries").entity().entries().items()),
             [(op.key(), op.value()) for op in entry.ops()]) for entry in entries]
    return (rows,)


def alter_client_quotas_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                               ) -> AlterClientQuotasResult[Any]:
    """The ``AlterClientQuotasResult`` over the C result's per-entity ``values()``
    futures."""
    from confluent_kafka.admin.alter_client_quotas_result import AlterClientQuotasResult

    futures = wrap_map(kind, loop, native.futures("values"), _to_client_quota_entity)
    return AlterClientQuotasResult._new(futures=futures, kind=kind)


def describe_user_scram_credentials_args(users: Iterable[str] | None) -> tuple[Any, ...]:
    """``Admin_describe_user_scram_credentials(h, users, options)``'s plain arguments:
    ``None`` is Java's ``null`` users (every user); a ``str`` is refused
    (``reject_str``)."""
    if users is not None:
        reject_str("users", users)
    # Java's createRequest skips a null user (``if (user != null)``,
    # KafkaAdminClient:4473): it is not described.
    return (None if users is None else [user for user in users if user is not None],)


def _scram_description(user: str, infos: Iterable[tuple[int, int]]
                       ) -> UserScramCredentialsDescription:
    """A user's ``UserScramCredentialsDescription`` from its ``(mechanism type,
    iterations)`` rows."""
    from confluent_kafka.admin.scram_credential_info import ScramCredentialInfo
    from confluent_kafka.admin.scram_mechanism import ScramMechanism
    from confluent_kafka.admin.user_scram_credentials_description import (
        UserScramCredentialsDescription,
    )

    return UserScramCredentialsDescription(name=user, credential_infos=[
        ScramCredentialInfo(mechanism=ScramMechanism.from_type(type=mechanism),
                            iterations=iterations)
        for mechanism, iterations in infos])


def _scram_descriptions(rows: Mapping[str, Iterable[tuple[int, int]]]
                        ) -> dict[str, UserScramCredentialsDescription]:
    """``all()``'s value: every described user's description."""
    return {user: _scram_description(user, infos) for user, infos in rows.items()}


def describe_user_scram_credentials_result(
        native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
) -> DescribeUserScramCredentialsResult[Any]:
    """The ``DescribeUserScramCredentialsResult`` over the C result: ``all()``,
    ``users()`` and ``description(user)`` are the core's own futures, each Java's
    dependant of the response data (see the result's ``_of_client``)."""
    from confluent_kafka.admin.describe_user_scram_credentials_result import (
        DescribeUserScramCredentialsResult,
    )

    def future(accessor: str, user: str | None) -> Any:
        if accessor == "all":
            return wrap(kind, loop, native.future("all", None, _scram_descriptions))
        if accessor == "users":
            return wrap(kind, loop, native.future("users", None, list))
        assert user is not None
        return wrap(kind, loop, native.future(
            "description", user, lambda infos: _scram_description(user, infos)))

    return DescribeUserScramCredentialsResult._of_client(future=future, kind=kind)


def _scram_alteration_row(alteration: UserScramCredentialAlteration) -> tuple[Any, ...]:
    """An alteration as ``(user, is_deletion, mechanism, iterations, password, salt)``:
    a ``UserScramCredentialDeletion`` sets ``is_deletion`` and has no iterations,
    password or salt; an upsertion always has a salt (its constructor makes one)."""
    from typing import cast

    from confluent_kafka.admin.user_scram_credential_deletion import (
        UserScramCredentialDeletion,
    )
    from confluent_kafka.admin.user_scram_credential_upsertion import (
        UserScramCredentialUpsertion,
    )

    if isinstance(alteration, UserScramCredentialDeletion):
        return (alteration.user(), True, alteration.mechanism().type(), 0, None, None)
    upsertion = cast(UserScramCredentialUpsertion, alteration)
    info = upsertion.credential_info()
    return (upsertion.user(), False, info.mechanism().type(), info.iterations(),
            upsertion.password(), upsertion.salt())


def alter_user_scram_credentials_args(alterations: Iterable[UserScramCredentialAlteration]
                                      ) -> tuple[Any, ...]:
    """``Admin_alter_user_scram_credentials(h, rows, options)``'s plain arguments."""
    return ([_scram_alteration_row(element(a, "alterations")) for a in alterations],)


def alter_user_scram_credentials_result(native: Any, kind: Kind,
                                        loop: asyncio.AbstractEventLoop | None
                                        ) -> AlterUserScramCredentialsResult[Any]:
    """The ``AlterUserScramCredentialsResult`` over the C result's per-user
    ``values()`` futures."""
    from confluent_kafka.admin.alter_user_scram_credentials_result import (
        AlterUserScramCredentialsResult,
    )

    futures = wrap_map(kind, loop, native.futures("values"))
    return AlterUserScramCredentialsResult._new(futures=futures, kind=kind)


def _to_feature_metadata(drained: tuple[list[tuple[str, int, int]], int | None,
                                        list[tuple[str, int, int]]]) -> FeatureMetadata:
    """``(finalized, epoch or None, supported)`` as a ``FeatureMetadata``, as Java's
    ``KafkaAdminClient`` builds it from the response."""
    from confluent_kafka.admin.feature_metadata import FeatureMetadata
    from confluent_kafka.admin.finalized_version_range import FinalizedVersionRange
    from confluent_kafka.admin.supported_version_range import SupportedVersionRange

    finalized, epoch, supported = drained
    return FeatureMetadata._new(
        finalized_features={
            name: FinalizedVersionRange(min_version_level=min_level, max_version_level=max_level)
            for name, min_level, max_level in finalized},
        finalized_features_epoch=epoch,
        supported_features={
            name: SupportedVersionRange(min_version=min_version, max_version=max_version)
            for name, min_version, max_version in supported})


def describe_features_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                             ) -> DescribeFeaturesResult[Any]:
    """The ``DescribeFeaturesResult`` over the C result's ``featureMetadata()``
    future."""
    from confluent_kafka.admin.describe_features_result import DescribeFeaturesResult

    future = wrap(kind, loop, native.future("feature_metadata", None, _to_feature_metadata))
    return DescribeFeaturesResult._new(future=future, kind=kind)


def update_features_args(feature_updates: Mapping[str, FeatureUpdate]) -> tuple[Any, ...]:
    """``Admin_update_features(h, rows, options)``'s plain arguments: each entry as
    ``(feature, max_version_level, upgrade_type code)``."""
    rows = [(feature, element(update, "feature_updates").max_version_level(),
             update.upgrade_type().code())
            for feature, update in feature_updates.items()]
    return (rows,)


def update_features_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                           ) -> UpdateFeaturesResult[Any]:
    """The ``UpdateFeaturesResult`` over the C result's per-feature ``values()``
    futures. The core's check of the arguments (``Feature updates can not be null or
    empty.``, ``Provided feature can not be empty.``), which Java's ``updateFeatures``
    throws before it makes a call (``KafkaAdminClient.java:4589-4598``), is raised by
    the extension's call itself."""
    from confluent_kafka.admin.update_features_result import UpdateFeaturesResult

    futures = wrap_map(kind, loop, native.futures("values"))
    return UpdateFeaturesResult._new(futures=futures, kind=kind)


def describe_producers_args(partitions: Iterable[TopicPartition]) -> tuple[Any, ...]:
    """``Admin_describe_producers(h, partitions, options)``'s plain arguments: each
    distinct partition as ``(topic, partition)`` (Java's ``Collection`` becomes the
    handler's key set)."""
    return ([(element(tp, "partitions").topic(), tp.partition())
             for tp in dict.fromkeys(partitions)],)


def _partition_producer_state(states: Iterable[tuple[Any, ...]]) -> Any:
    """A partition's ``PartitionProducerState`` from the ``[(producer_id,
    producer_epoch, last_sequence, last_timestamp, coordinator_epoch or None,
    current_transaction_start_offset or None)]`` its future delivers."""
    from confluent_kafka.admin.describe_producers_result import DescribeProducersResult
    from confluent_kafka.admin.producer_state import ProducerState

    return DescribeProducersResult.PartitionProducerState(active_producers=[
        ProducerState(producer_id=producer_id, producer_epoch=producer_epoch,
                      last_sequence=last_sequence, last_timestamp=last_timestamp,
                      coordinator_epoch=coordinator_epoch,
                      current_transaction_start_offset=start_offset)
        for (producer_id, producer_epoch, last_sequence, last_timestamp, coordinator_epoch,
             start_offset) in states])


def describe_producers_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None,
                              partitions: Iterable[TopicPartition]
                              ) -> DescribeProducersResult[Any]:
    """The ``DescribeProducersResult`` over the C result: one future per requested
    partition, the core's ``partitionResult(partition)``."""
    from confluent_kafka.admin.describe_producers_result import DescribeProducersResult

    futures = {tp: wrap(kind, loop, native.future("partition_result", (tp.topic(), tp.partition()),
                                                  _partition_producer_state))
               for tp in dict.fromkeys(partitions)}
    return DescribeProducersResult._new(futures=futures, kind=kind)


def _transactional_id_key(transactional_id: str) -> _CoordinatorKey:
    """Java's ``CoordinatorKey.byTransactionalId(transactionalId)``, the key of the
    transaction results."""
    from confluent_kafka.admin._internals import _CoordinatorKey

    return _CoordinatorKey.by_transactional_id(transactional_id=transactional_id)


def describe_transactions_args(transactional_ids: Iterable[str]) -> tuple[Any, ...]:
    """``Admin_describe_transactions(h, transactional_ids, options)``'s plain
    arguments; a ``str`` is refused (``reject_str``)."""
    reject_str("transactional_ids", transactional_ids)
    return (list(dict.fromkeys(transactional_ids)),)


def _transaction_description(description: tuple[Any, ...]) -> TransactionDescription:
    """A ``TransactionDescription`` from the ``(coordinator_id, state name,
    producer_id, producer_epoch, transaction_timeout_ms, start_time_ms or None,
    [(topic, partition)])`` its future delivers."""
    from confluent_kafka.admin.transaction_description import TransactionDescription
    from confluent_kafka.admin.transaction_state import TransactionState

    (coordinator_id, state, producer_id, producer_epoch, transaction_timeout_ms,
     transaction_start_time_ms, topic_partitions) = description
    return TransactionDescription(
        coordinator_id=coordinator_id, state=TransactionState.parse(name=state),
        producer_id=producer_id, producer_epoch=producer_epoch,
        transaction_timeout_ms=transaction_timeout_ms,
        transaction_start_time_ms=transaction_start_time_ms,
        topic_partitions=[topic_partition_from_tuple(tp) for tp in topic_partitions])


def describe_transactions_result(native: Any, kind: Kind,
                                 loop: asyncio.AbstractEventLoop | None,
                                 transactional_ids: Iterable[str]
                                 ) -> DescribeTransactionsResult[Any]:
    """The ``DescribeTransactionsResult`` over the C result: one future per
    requested transactional id, the core's ``description(transactionalId)``, keyed as
    Java keys it (``CoordinatorKey``)."""
    from confluent_kafka.admin.describe_transactions_result import DescribeTransactionsResult

    futures = {_transactional_id_key(transactional_id): wrap(
        kind, loop, native.future("description", transactional_id, _transaction_description))
        for transactional_id in dict.fromkeys(transactional_ids)}
    return DescribeTransactionsResult._new(futures=futures, kind=kind)


def abort_transaction_args(spec: AbortTransactionSpec) -> tuple[Any, ...]:
    """``Admin_abort_transaction(h, spec, options)``'s plain arguments: the spec as
    ``(topic, partition, producer_id, producer_epoch, coordinator_epoch)``."""
    partition = spec.topic_partition()
    return ((partition.topic(), partition.partition(), spec.producer_id(),
             spec.producer_epoch(), spec.coordinator_epoch()),)


def abort_transaction_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None,
                             spec: AbortTransactionSpec) -> AbortTransactionResult[Any]:
    """The ``AbortTransactionResult`` over the C result: Java's one entry, for the
    spec's partition, whose future is the core's ``all()`` (the C result has no
    per-partition accessor; ``all()`` completes as that one entry does)."""
    from confluent_kafka.admin.abort_transaction_result import AbortTransactionResult

    return AbortTransactionResult._new(
        futures={spec.topic_partition(): wrap(kind, loop, native.future("all"))}, kind=kind)


def list_transactions_options(options: ListTransactionsOptions) -> list[tuple[str, Any]]:
    """The ``(setter, value)`` pairs of a ``ListTransactionsOptions``: the filters as
    their stored sets (the copies Java's setters store), each ``TransactionState`` as
    its ``toString()`` name, which the extension parses back
    (``TransactionState.parse``)."""
    stored: dict[str, Callable[[], Any]] = {
        "filter_states": lambda: [str(state) for state in options.filtered_states()],
        "filter_producer_ids": lambda: list(options.filtered_producer_ids())}
    return [(setter, stored[setter]() if setter in stored else value)
            for setter, value in options._setters_ran()]


def _transaction_listings(rows: Iterable[tuple[str, int, str]]) -> list[TransactionListing]:
    """A broker's listings, as a ``list`` (Java's handler builds a ``List``)."""
    from confluent_kafka.admin.transaction_listing import TransactionListing
    from confluent_kafka.admin.transaction_state import TransactionState

    return [TransactionListing(transactional_id=transactional_id, producer_id=producer_id,
                               transaction_state=TransactionState.parse(name=state))
            for transactional_id, producer_id, state in rows]


def list_transactions_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None
                             ) -> ListTransactionsResult[Any]:
    """The ``ListTransactionsResult`` over the C result's ``byBrokerId()`` future:
    its value maps each broker to the core's own future of that broker's listings,
    each wrapped as the client's future."""
    from confluent_kafka.admin.list_transactions_result import ListTransactionsResult

    def by_broker(natives: Mapping[int, Any]) -> dict[int, Any]:
        futures = {}
        for broker_id, broker_native in natives.items():
            broker_native.set_post(_transaction_listings)
            futures[broker_id] = wrap(kind, loop, broker_native)
        return futures

    future = wrap(kind, loop, native.future("by_broker_id", None, by_broker))
    return ListTransactionsResult._new(future=future, kind=kind)


def fence_producers_args(transactional_ids: Iterable[str]) -> tuple[Any, ...]:
    """``Admin_fence_producers(h, transactional_ids, options)``'s plain arguments;
    a ``str`` is refused (``reject_str``)."""
    reject_str("transactional_ids", transactional_ids)
    return (list(dict.fromkeys(transactional_ids)),)


def fence_producers_result(native: Any, kind: Kind, loop: asyncio.AbstractEventLoop | None,
                           transactional_ids: Iterable[str]) -> FenceProducersResult[Any]:
    """The ``FenceProducersResult`` over the C result: Java's per-id future of the
    ``ProducerIdAndEpoch``, keyed as Java keys it (``CoordinatorKey``), completed from
    the core's ``producerId(id)`` and ``epochId(id)`` futures, which complete together
    with that id's fencing (the C result hands out the pair only as those two
    refinements)."""
    from confluent_kafka.admin._internals import _ProducerIdAndEpoch
    from confluent_kafka.admin.fence_producers_result import FenceProducersResult

    futures: dict[_CoordinatorKey, Any] = {}
    for transactional_id in dict.fromkeys(transactional_ids):
        producer_id = wrap(kind, loop, native.future("producer_id", transactional_id))
        epoch = wrap(kind, loop, native.future("epoch_id", transactional_id))
        impl: _KafkaFutureImpl[_ProducerIdAndEpoch] = _KafkaFutureImpl()

        def settle(_state: _FutureState[Any], impl: _KafkaFutureImpl[Any] = impl,
                   producer_id: Any = producer_id, epoch: Any = epoch) -> None:
            first, second = producer_id._python_state(), epoch._python_state()
            if first._state == _PENDING or second._state == _PENDING:
                return
            for source in (first, second):
                if source._state != _VALUE:
                    impl._settle(source._state, source._outcome)
                    return
            impl.complete(_ProducerIdAndEpoch(producer_id=first._outcome,
                                              epoch=second._outcome))

        producer_id._python_state()._add_listener(settle)
        epoch._python_state()._add_listener(settle)
        futures[_transactional_id_key(transactional_id)] = (
            AsyncKafkaFuture._of_impl(impl, loop) if kind is Async
            else KafkaFuture._of_impl(impl))
    return FenceProducersResult._new(futures=futures, kind=kind)


def list_config_resources_args(config_resource_types: Iterable[ConfigResource.Type]
                               ) -> tuple[Any, ...]:
    """``Admin_list_config_resources(h, type_ids, options)``'s arguments: each type's
    ``id()``; an empty set asks for every type."""
    return ([element(resource_type, "config_resource_types").id()
             for resource_type in config_resource_types],)


def _config_resources(rows: list[tuple[int, str]]) -> list[ConfigResource]:
    return [config_resource_from_tuple(raw) for raw in rows]


def list_config_resources_result(native: Any, kind: Kind,
                                 loop: asyncio.AbstractEventLoop | None
                                 ) -> ListConfigResourcesResult[Any]:
    """The ``ListConfigResourcesResult`` over the C result's ``all()`` future."""
    from confluent_kafka.admin.list_config_resources_result import ListConfigResourcesResult

    future = wrap(kind, loop, native.future("all", None, _config_resources))
    return ListConfigResourcesResult._new(future=future, kind=kind)


def unregister_controller_result(native: Any, kind: Kind,
                                 loop: asyncio.AbstractEventLoop | None
                                 ) -> UnregisterControllerResult[Any]:
    """The ``UnregisterControllerResult`` over the C result's ``all()`` future."""
    from confluent_kafka.admin.unregister_controller_result import UnregisterControllerResult

    return UnregisterControllerResult._new(future=wrap(kind, loop, native.future("all")),
                                           kind=kind)


def _classic_group_description(raw: tuple[Any, ...]) -> ClassicGroupDescription:
    """A ``ClassicGroupDescription`` from the extension's ``(group_id, protocol,
    protocol_data, is_simple, members, state, coordinator, authorized_operations)``
    (``classic_group_description_to_py``), built through ``_new`` so that an
    authorized operations set the core reports as ``null`` stays ``None`` (decision
    log 41). The members' assignments arrive decoded (the core's
    ``ConsumerProtocol``)."""
    from confluent_kafka.admin.classic_group_description import ClassicGroupDescription
    from confluent_kafka.common import ClassicGroupState

    (group_id, protocol, protocol_data, _is_simple, members, state, coordinator,
     operations) = raw
    return ClassicGroupDescription._new(
        group_id=group_id, protocol=protocol, protocol_data=protocol_data,
        members=[_group_member(member) for member in members],
        state=ClassicGroupState.parse(name=state),
        coordinator=optional_node_from_tuple(coordinator),
        authorized_operations=acl_operations_from_codes(operations))


def describe_classic_groups_result(native: Any, kind: Kind,
                                   loop: asyncio.AbstractEventLoop | None
                                   ) -> DescribeClassicGroupsResult[Any]:
    """The ``DescribeClassicGroupsResult`` over the C result's ``describedGroups()``
    futures."""
    from confluent_kafka.admin.describe_classic_groups_result import (
        DescribeClassicGroupsResult,
    )

    futures: dict[str, Any] = wrap_map(
        kind, loop, native.futures("described_groups", _classic_group_description))
    return DescribeClassicGroupsResult._new(futures=futures, kind=kind)


def force_terminate_transaction_result(native: Any, kind: Kind,
                                       loop: asyncio.AbstractEventLoop | None
                                       ) -> TerminateTransactionResult[Any]:
    """The ``TerminateTransactionResult`` over the C result's ``result()``
    future."""
    from confluent_kafka.admin.terminate_transaction_result import TerminateTransactionResult

    return TerminateTransactionResult._new(future=wrap(kind, loop, native.future("result")),
                                           kind=kind)
