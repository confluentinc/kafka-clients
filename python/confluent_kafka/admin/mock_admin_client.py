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

"""``MockAdminClient``: Java's ``org.apache.kafka.clients.admin.MockAdminClient``.

A direct Python translation of its Java source (CLAUDE.md, Python Binding
Conventions, Implementation over the FFI), not FFI-backed: the state
and the method bodies are in the private ``_mock_core``, shared with the asyncio
peer. Java's mock ships in the clients test jar and extends ``AdminClient``; here
it extends ``Admin`` and overrides every method of it.

Java's public constructors ``()`` and ``(List<Node> brokers, Node controller)``
are one ``__init__`` checked by ``java_forms``: both arguments or neither. The
static ``create()`` returns a ``MockAdminClient.Builder``, which Java builds with
setters, so it also takes one keyword per field. ``create()``
hides ``Admin.create(configs=…)`` as Java's static does.

As in Java, ``close()`` does nothing: the mock keeps answering after it. The
results' futures are ``KafkaFuture``\\ s over the private ``_KafkaFutureImpl``
(Java's ``KafkaFutureImpl``), most of them complete on return. Where
Java's mock throws (``UnsupportedOperationException("Not implemented yet")``, a
``NullPointerException`` or ``IndexOutOfBoundsException`` of its own), this mock
raises the matching error.

A mock does not generate what its base class does not: the overloads ``Admin``
leaves out for want of an entry point (see its module docstring) are left out here
too.

Members are in ``Admin``'s declaration order; the Java mock's own
methods (``controller``, ``add_topic``, ``mark_topic_for_deletion``,
``timeout_next_request``, ``handle_describe_topics_using_ids`` (wrapped here for
its future type),
``update_beginning_offsets``, ``update_end_offsets``,
``update_consumer_group_offsets``, ``set_fetches_remaining_until_visible``,
``brokers``, ``broker``) come from ``_mock_core``. Dropped with the base methods
they serve (Class family): ``setMockMetrics``, ``disableTelemetry``,
``injectTimeoutException``, ``advanceTimeOnClientInstanceId``,
``setClientInstanceId``, ``addedMetrics``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, overload

from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka.admin._futures import Sync
from confluent_kafka.admin._mock_core import MockAdminClientBuilderCore, MockAdminClientCore
from confluent_kafka.admin.admin import Admin
from confluent_kafka.admin.create_delegation_token_options import CreateDelegationTokenOptions
from confluent_kafka.admin.describe_delegation_token_options import DescribeDelegationTokenOptions
from confluent_kafka.admin.expire_delegation_token_options import ExpireDelegationTokenOptions
from confluent_kafka.admin.list_consumer_group_offsets_spec import ListConsumerGroupOffsetsSpec
from confluent_kafka.admin.renew_delegation_token_options import RenewDelegationTokenOptions
from confluent_kafka.admin.update_features_options import UpdateFeaturesOptions

if TYPE_CHECKING:
    from confluent_kafka.common.kafka_future import KafkaFuture
    from confluent_kafka.admin.unregister_controller_options import UnregisterControllerOptions
    from confluent_kafka.admin.unregister_controller_result import UnregisterControllerResult
    from confluent_kafka import Duration
    from confluent_kafka.admin.abort_transaction_options import AbortTransactionOptions
    from confluent_kafka.admin.abort_transaction_result import AbortTransactionResult
    from confluent_kafka.admin.abort_transaction_spec import AbortTransactionSpec
    from confluent_kafka.admin.alter_client_quotas_options import AlterClientQuotasOptions
    from confluent_kafka.admin.alter_client_quotas_result import AlterClientQuotasResult
    from confluent_kafka.admin.alter_config_op import AlterConfigOp
    from confluent_kafka.admin.alter_configs_options import AlterConfigsOptions
    from confluent_kafka.admin.alter_configs_result import AlterConfigsResult
    from confluent_kafka.admin.alter_consumer_group_offsets_options import (
        AlterConsumerGroupOffsetsOptions,
    )
    from confluent_kafka.admin.alter_consumer_group_offsets_result import (
        AlterConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.alter_partition_reassignments_options import (
        AlterPartitionReassignmentsOptions,
    )
    from confluent_kafka.admin.alter_partition_reassignments_result import (
        AlterPartitionReassignmentsResult,
    )
    from confluent_kafka.admin.alter_replica_log_dirs_options import AlterReplicaLogDirsOptions
    from confluent_kafka.admin.alter_replica_log_dirs_result import AlterReplicaLogDirsResult
    from confluent_kafka.admin.alter_user_scram_credentials_options import (
        AlterUserScramCredentialsOptions,
    )
    from confluent_kafka.admin.alter_user_scram_credentials_result import (
        AlterUserScramCredentialsResult,
    )
    from confluent_kafka.admin.create_acls_options import CreateAclsOptions
    from confluent_kafka.admin.create_acls_result import CreateAclsResult
    from confluent_kafka.admin.create_delegation_token_result import CreateDelegationTokenResult
    from confluent_kafka.admin.create_partitions_options import CreatePartitionsOptions
    from confluent_kafka.admin.create_partitions_result import CreatePartitionsResult
    from confluent_kafka.admin.create_topics_options import CreateTopicsOptions
    from confluent_kafka.admin.create_topics_result import CreateTopicsResult
    from confluent_kafka.admin.delete_acls_options import DeleteAclsOptions
    from confluent_kafka.admin.delete_acls_result import DeleteAclsResult
    from confluent_kafka.admin.delete_consumer_group_offsets_options import (
        DeleteConsumerGroupOffsetsOptions,
    )
    from confluent_kafka.admin.delete_consumer_group_offsets_result import (
        DeleteConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.delete_consumer_groups_options import DeleteConsumerGroupsOptions
    from confluent_kafka.admin.delete_consumer_groups_result import DeleteConsumerGroupsResult
    from confluent_kafka.admin.delete_records_options import DeleteRecordsOptions
    from confluent_kafka.admin.delete_records_result import DeleteRecordsResult
    from confluent_kafka.admin.delete_topics_options import DeleteTopicsOptions
    from confluent_kafka.admin.delete_topics_result import DeleteTopicsResult
    from confluent_kafka.admin.describe_acls_options import DescribeAclsOptions
    from confluent_kafka.admin.describe_acls_result import DescribeAclsResult
    from confluent_kafka.admin.describe_classic_groups_options import DescribeClassicGroupsOptions
    from confluent_kafka.admin.describe_classic_groups_result import DescribeClassicGroupsResult
    from confluent_kafka.admin.describe_client_quotas_options import DescribeClientQuotasOptions
    from confluent_kafka.admin.describe_client_quotas_result import DescribeClientQuotasResult
    from confluent_kafka.admin.describe_cluster_options import DescribeClusterOptions
    from confluent_kafka.admin.describe_cluster_result import DescribeClusterResult
    from confluent_kafka.admin.describe_configs_options import DescribeConfigsOptions
    from confluent_kafka.admin.describe_configs_result import DescribeConfigsResult
    from confluent_kafka.admin.describe_consumer_groups_options import (
        DescribeConsumerGroupsOptions,
    )
    from confluent_kafka.admin.describe_consumer_groups_result import DescribeConsumerGroupsResult
    from confluent_kafka.admin.describe_delegation_token_result import (
        DescribeDelegationTokenResult,
    )
    from confluent_kafka.admin.describe_features_options import DescribeFeaturesOptions
    from confluent_kafka.admin.describe_features_result import DescribeFeaturesResult
    from confluent_kafka.admin.describe_log_dirs_options import DescribeLogDirsOptions
    from confluent_kafka.admin.describe_log_dirs_result import DescribeLogDirsResult
    from confluent_kafka.admin.describe_producers_options import DescribeProducersOptions
    from confluent_kafka.admin.describe_producers_result import DescribeProducersResult
    from confluent_kafka.admin.describe_replica_log_dirs_options import (
        DescribeReplicaLogDirsOptions,
    )
    from confluent_kafka.admin.describe_replica_log_dirs_result import DescribeReplicaLogDirsResult
    from confluent_kafka.admin.describe_topics_options import DescribeTopicsOptions
    from confluent_kafka.admin.describe_topics_result import DescribeTopicsResult
    from confluent_kafka.admin.describe_transactions_options import DescribeTransactionsOptions
    from confluent_kafka.admin.describe_transactions_result import DescribeTransactionsResult
    from confluent_kafka.admin.describe_user_scram_credentials_options import (
        DescribeUserScramCredentialsOptions,
    )
    from confluent_kafka.admin.describe_user_scram_credentials_result import (
        DescribeUserScramCredentialsResult,
    )
    from confluent_kafka.admin.elect_leaders_options import ElectLeadersOptions
    from confluent_kafka.admin.elect_leaders_result import ElectLeadersResult
    from confluent_kafka.admin.expire_delegation_token_result import ExpireDelegationTokenResult
    from confluent_kafka.admin.feature_update import FeatureUpdate
    from confluent_kafka.admin.fence_producers_options import FenceProducersOptions
    from confluent_kafka.admin.fence_producers_result import FenceProducersResult
    from confluent_kafka.admin.list_config_resources_options import ListConfigResourcesOptions
    from confluent_kafka.admin.list_config_resources_result import ListConfigResourcesResult
    from confluent_kafka.admin.list_consumer_group_offsets_options import (
        ListConsumerGroupOffsetsOptions,
    )
    from confluent_kafka.admin.list_consumer_group_offsets_result import (
        ListConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.list_groups_options import ListGroupsOptions
    from confluent_kafka.admin.list_groups_result import ListGroupsResult
    from confluent_kafka.admin.list_offsets_options import ListOffsetsOptions
    from confluent_kafka.admin.list_offsets_result import ListOffsetsResult
    from confluent_kafka.admin.list_partition_reassignments_options import (
        ListPartitionReassignmentsOptions,
    )
    from confluent_kafka.admin.list_partition_reassignments_result import (
        ListPartitionReassignmentsResult,
    )
    from confluent_kafka.admin.list_topics_options import ListTopicsOptions
    from confluent_kafka.admin.list_topics_result import ListTopicsResult
    from confluent_kafka.admin.list_transactions_options import ListTransactionsOptions
    from confluent_kafka.admin.list_transactions_result import ListTransactionsResult
    from confluent_kafka.admin.new_partition_reassignment import NewPartitionReassignment
    from confluent_kafka.admin.new_partitions import NewPartitions
    from confluent_kafka.admin.new_topic import NewTopic
    from confluent_kafka.admin.offset_spec import OffsetSpec
    from confluent_kafka.admin.records_to_delete import RecordsToDelete
    from confluent_kafka.admin.remove_members_from_consumer_group_options import (
        RemoveMembersFromConsumerGroupOptions,
    )
    from confluent_kafka.admin.remove_members_from_consumer_group_result import (
        RemoveMembersFromConsumerGroupResult,
    )
    from confluent_kafka.admin.renew_delegation_token_result import RenewDelegationTokenResult
    from confluent_kafka.admin.terminate_transaction_options import TerminateTransactionOptions
    from confluent_kafka.admin.terminate_transaction_result import TerminateTransactionResult
    from confluent_kafka.admin.topic_description import TopicDescription
    from confluent_kafka.admin.update_features_result import UpdateFeaturesResult
    from confluent_kafka.admin.user_scram_credential_alteration import (
        UserScramCredentialAlteration,
    )
    from confluent_kafka.common import (
        AclBinding,
        AclBindingFilter,
        ClientQuotaAlteration,
        ClientQuotaFilter,
        ElectionType,
        Node,
        TopicCollection,
        TopicPartition,
        TopicPartitionReplica,
        Uuid,
    )
    from confluent_kafka.common.config import ConfigResource
    from confluent_kafka.consumer import OffsetAndMetadata

__all__ = ["MockAdminClient"]

_INIT_FORMS = (Form(), Form("brokers", "controller"))
_DESCRIBE_TOPICS_FORMS = (Form("topic_names"), Form("topic_names", "options"),
                          Form("topics"), Form("topics", "options"))
_LIST_CONSUMER_GROUP_OFFSETS_FORMS = (Form("group_id", "options"), Form("group_id"),
                                      Form("group_specs", "options"), Form("group_specs"))
# describeUserScramCredentials() passes (null, new DescribeUserScramCredentialsOptions()).
_DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS = (Form(), Form("users"), Form("users", "options"))
# listConfigResources() passes (Set.of(), new ListConfigResourcesOptions()).
_LIST_CONFIG_RESOURCES_FORMS = (
    Form("config_resource_types", "options",
         defaults={"config_resource_types": (), "options": None}),
    Form())


class MockAdminClient(MockAdminClientCore, Admin):
    """A mock of the ``Admin`` interface you can use for testing code that uses
    the admin client: an in-memory cluster of brokers, topics, configs, offsets
    and reassignments.

    Java: ``org.apache.kafka.clients.admin.MockAdminClient``.
    """

    _kind = Sync

    class Builder(MockAdminClientBuilderCore):
        """Builds a ``MockAdminClient``: one broker ``Node(0, "localhost", 1000)``
        with ``DEFAULT_LOG_DIRS`` by default, the first broker as controller, one
        partition and ``min(brokers, 3)`` replicas per new topic.

        Java: ``org.apache.kafka.clients.admin.MockAdminClient.Builder``.
        """

        def build(self) -> MockAdminClient:
            """The mock built from this builder's fields (``IndexError`` with no
            broker, as Java's ``brokers.get(0)``)."""
            return MockAdminClient._new(**self._build_args())

    @staticmethod
    def create() -> MockAdminClient.Builder:  # type: ignore[override]
        """A new ``MockAdminClient.Builder``."""
        return MockAdminClient.Builder()

    @overload
    def __init__(self) -> None: ...
    @overload
    def __init__(self, *, brokers: Sequence[Node], controller: Node) -> None: ...

    @java_forms(*_INIT_FORMS)
    def __init__(self, *, brokers: Sequence[Node] | None = None,
                 controller: Node | None = None) -> None:
        """A mock of ``brokers`` with ``controller`` (one of them), the default
        cluster id, one partition and ``len(brokers)`` replicas per new topic, and
        ``DEFAULT_LOG_DIRS`` on every broker; with no arguments, one broker
        ``Node.no_node()``, also the controller."""
        Admin.__init__(self)
        self._init_two(brokers, controller)

    @classmethod
    def _new(cls, *, brokers: Sequence[Node], controller: Node, cluster_id: str,
             default_partitions: int, default_replication_factor: int,
             broker_log_dirs: Sequence[Sequence[str]], using_raft_controller: bool,
             feature_levels: Mapping[str, int], min_supported_feature_levels: Mapping[str, int],
             max_supported_feature_levels: Mapping[str, int],
             default_group_configs: Mapping[str, str]) -> MockAdminClient:
        """Java's private eleven-argument constructor (``Builder.build()``)."""
        client = cls.__new__(cls)
        Admin.__init__(client)
        client._init_mock(
            brokers=brokers, controller=controller, cluster_id=cluster_id,
            default_partitions=default_partitions,
            default_replication_factor=default_replication_factor,
            broker_log_dirs=broker_log_dirs, using_raft_controller=using_raft_controller,
            feature_levels=feature_levels,
            min_supported_feature_levels=min_supported_feature_levels,
            max_supported_feature_levels=max_supported_feature_levels,
            default_group_configs=default_group_configs)
        return client

    def close(self, *, timeout: Duration | None = None) -> None:
        """Does nothing, as Java's mock (no check of ``timeout`` either)."""
        self._close()

    def __enter__(self) -> MockAdminClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions | None = None) -> CreateTopicsResult[Sync]:
        """Create the topics: ``default_replication_factor`` / ``default_partitions``
        stand for ``-1``, the first brokers are the replicas and the first broker
        the leader; ``options`` is ignored."""
        return self._create_topics(new_topics)

    @overload
    def delete_topics(self, *, topics: Iterable[str],
                      options: DeleteTopicsOptions | None = None) -> DeleteTopicsResult[Sync]: ...
    @overload
    def delete_topics(self, *, topics: TopicCollection,
                      options: DeleteTopicsOptions | None = None) -> DeleteTopicsResult[Sync]: ...

    def delete_topics(self, *, topics: Iterable[str] | TopicCollection,
                      options: DeleteTopicsOptions | None = None) -> DeleteTopicsResult[Sync]:
        """Delete the topics, by name or by id; ``options`` is ignored."""
        return self._delete_topics(topics)

    def list_topics(self, *, options: ListTopicsOptions | None = None) -> ListTopicsResult[Sync]:
        """List every topic, internal ones included (``options`` is ignored)."""
        return self._list_topics()

    @overload
    def describe_topics(self, *, topic_names: Iterable[str],
                        options: DescribeTopicsOptions | None = None
                        ) -> DescribeTopicsResult[Sync]: ...
    @overload
    def describe_topics(self, *, topics: TopicCollection,
                        options: DescribeTopicsOptions | None = None
                        ) -> DescribeTopicsResult[Sync]: ...

    @java_forms(*_DESCRIBE_TOPICS_FORMS)
    def describe_topics(self, *, topic_names: Iterable[str] | None = None,
                        topics: TopicCollection | None = None,
                        options: DescribeTopicsOptions | None = None
                        ) -> DescribeTopicsResult[Sync]:
        """Describe the topics, by name or by id; ``options`` is ignored."""
        return self._describe_topics(topic_names, topics)

    def describe_cluster(self, *, options: DescribeClusterOptions | None = None
                         ) -> DescribeClusterResult[Sync]:
        """The brokers, the controller, the cluster id and no authorized operations
        (``options`` is ignored)."""
        return self._describe_cluster()

    def describe_acls(self, *, filter: AclBindingFilter,  # noqa: A002 - Java's name
                      options: DescribeAclsOptions | None = None) -> DescribeAclsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._describe_acls(filter)

    def create_acls(self, *, acls: Iterable[AclBinding],
                    options: CreateAclsOptions | None = None) -> CreateAclsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._create_acls(acls)

    def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                    options: DeleteAclsOptions | None = None) -> DeleteAclsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._delete_acls(filters)

    def describe_configs(self, *, resources: Iterable[ConfigResource],
                         options: DescribeConfigsOptions | None = None
                         ) -> DescribeConfigsResult[Sync]:
        """The configs of broker, topic, client-metrics and group resources; another
        type fails its future with ``UnsupportedVersionError("Not implemented
        yet")`` (``options`` is ignored)."""
        return self._describe_configs(resources)

    def incremental_alter_configs(
            self, *, configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
            options: AlterConfigsOptions | None = None) -> AlterConfigsResult[Sync]:
        """Apply ``SET`` and ``DELETE`` operations to broker, topic, client-metrics
        and group configs (``options`` is ignored)."""
        return self._incremental_alter_configs(configs)

    def alter_replica_log_dirs(self, *, replica_assignment: Mapping[TopicPartitionReplica, str],
                               options: AlterReplicaLogDirsOptions | None = None
                               ) -> AlterReplicaLogDirsResult[Sync]:
        """Record a move of each replica to the given log directory, which must be
        one of its broker's (``options`` is ignored)."""
        return self._alter_replica_log_dirs(replica_assignment)

    def describe_log_dirs(self, *, brokers: Iterable[int],
                          options: DescribeLogDirsOptions | None = None
                          ) -> DescribeLogDirsResult[Sync]:
        """The log directories of the brokers, with every replica of every topic in
        its partition's first log directory (``options`` is ignored)."""
        return self._describe_log_dirs(brokers)

    def describe_replica_log_dirs(self, *, replicas: Iterable[TopicPartitionReplica],
                                  options: DescribeReplicaLogDirsOptions | None = None
                                  ) -> DescribeReplicaLogDirsResult[Sync]:
        """The current and future log directory of each replica of a known topic
        (``options`` is ignored)."""
        return self._describe_replica_log_dirs(replicas)

    def create_partitions(self, *, new_partitions: Mapping[str, NewPartitions],
                          options: CreatePartitionsOptions | None = None
                          ) -> CreatePartitionsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._create_partitions(new_partitions)

    def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                       options: DeleteRecordsOptions | None = None) -> DeleteRecordsResult[Sync]:
        """An empty result for no records; otherwise raises
        ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._delete_records(records_to_delete)

    def create_delegation_token(self, *, options: CreateDelegationTokenOptions | None = None
                                ) -> CreateDelegationTokenResult[Sync]:
        """Create a token owned by the first of ``options.renewers()``, issued now, its
        maximum timestamp ``options.max_lifetime_ms()``, its HMAC its id's bytes; a
        renewer not of the ``User`` type fails the future with
        ``InvalidPrincipalTypeError``, and no renewer raises ``IndexError``, as
        Java's mock (``options.owner()`` is ignored)."""
        return self._create_delegation_token(
            options if options is not None else CreateDelegationTokenOptions())

    def renew_delegation_token(self, *, hmac: bytes,
                               options: RenewDelegationTokenOptions | None = None
                               ) -> RenewDelegationTokenResult[Sync]:
        """Set the expiry timestamp of the tokens with this HMAC to
        ``options.renew_time_period_ms()``, the future's value; none fails it with
        ``DelegationTokenNotFoundError``."""
        return self._renew_delegation_token(
            hmac, options if options is not None else RenewDelegationTokenOptions())

    def expire_delegation_token(self, *, hmac: bytes,
                                options: ExpireDelegationTokenOptions | None = None
                                ) -> ExpireDelegationTokenResult[Sync]:
        """Remove the tokens with this HMAC when ``options.expiry_time_period_ms()`` is
        -1 or in the past, the future's value being that period; none fails it with
        ``DelegationTokenNotFoundError``."""
        return self._expire_delegation_token(
            hmac, options if options is not None else ExpireDelegationTokenOptions())

    def describe_delegation_token(self, *,
                                  options: DescribeDelegationTokenOptions | None = None
                                  ) -> DescribeDelegationTokenResult[Sync]:
        """The tokens of ``options.owners()``, all for an empty list; the default
        owners (``None``) raise ``NullPointerError()``, as Java's mock (decision
        10)."""
        return self._describe_delegation_token(
            options if options is not None else DescribeDelegationTokenOptions())

    def describe_consumer_groups(self, *, group_ids: Iterable[str],
                                 options: DescribeConsumerGroupsOptions | None = None
                                 ) -> DescribeConsumerGroupsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._describe_consumer_groups(group_ids)

    @overload
    def list_consumer_group_offsets(self, *, group_id: str,
                                    options: ListConsumerGroupOffsetsOptions | None = None
                                    ) -> ListConsumerGroupOffsetsResult[Sync]: ...
    @overload
    def list_consumer_group_offsets(
        self, *, group_specs: Mapping[str, ListConsumerGroupOffsetsSpec],
        options: ListConsumerGroupOffsetsOptions | None = None
    ) -> ListConsumerGroupOffsetsResult[Sync]: ...

    @java_forms(*_LIST_CONSUMER_GROUP_OFFSETS_FORMS)
    def list_consumer_group_offsets(
        self, *, group_id: str | None = None,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec] | None = None,
        options: ListConsumerGroupOffsetsOptions | None = None
    ) -> ListConsumerGroupOffsetsResult[Sync]:
        """The offsets set by ``update_consumer_group_offsets`` for one group: all
        for a spec with no partitions, else those of its partitions. More or fewer
        than one group raises ``UnsupportedVersionError("Not implemented yet")``;
        ``group_id`` (Java's ``new ListConsumerGroupOffsetsSpec()``, whose
        partitions are ``None``) raises ``NullPointerError()`` once an offset is
        stored, as Java's mock; ``options`` is ignored."""
        if group_specs is None:
            assert group_id is not None  # java_forms: one or the other
            group_specs = {group_id: ListConsumerGroupOffsetsSpec()}
        return self._list_consumer_group_offsets(group_specs)

    def delete_consumer_groups(self, *, group_ids: Iterable[str],
                               options: DeleteConsumerGroupsOptions | None = None
                               ) -> DeleteConsumerGroupsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._delete_consumer_groups(group_ids)

    def delete_consumer_group_offsets(self, *, group_id: str,
                                      partitions: Iterable[TopicPartition],
                                      options: DeleteConsumerGroupOffsetsOptions | None = None
                                      ) -> DeleteConsumerGroupOffsetsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._delete_consumer_group_offsets(group_id, partitions)

    def list_groups(self, *, options: ListGroupsOptions | None = None) -> ListGroupsResult[Sync]:
        """One stable consumer group of the ``consumer`` protocol per group with
        configs (``options`` and its filters are ignored)."""
        return self._list_groups()

    def elect_leaders(self, *, election_type: ElectionType,
                      partitions: Iterable[TopicPartition] | None,
                      options: ElectLeadersOptions | None = None) -> ElectLeadersResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._elect_leaders(election_type, partitions)

    def alter_partition_reassignments(
            self, *, reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
            options: AlterPartitionReassignmentsOptions | None = None
            ) -> AlterPartitionReassignmentsResult[Sync]:
        """Store each reassignment, or cancel it for ``None`` (Java's empty
        ``Optional``); an unknown partition fails its future with
        ``UnknownTopicOrPartitionError`` (``options`` is ignored)."""
        return self._alter_partition_reassignments(reassignments)

    def list_partition_reassignments(
            self, *, partitions: Iterable[TopicPartition] | None = None,
            options: ListPartitionReassignmentsOptions | None = None
            ) -> ListPartitionReassignmentsResult[Sync]:
        """The stored reassignments of ``partitions`` (all when ``None``, Java's
        ``listPartitionReassignments()`` / ``Optional.empty()``), with the replicas
        being added and removed (``options`` is ignored)."""
        return self._list_partition_reassignments(partitions)

    def remove_members_from_consumer_group(
        self, *, group_id: str, options: RemoveMembersFromConsumerGroupOptions
    ) -> RemoveMembersFromConsumerGroupResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._remove_members_from_consumer_group(group_id, options)

    def alter_consumer_group_offsets(self, *, group_id: str,
                                     offsets: Mapping[TopicPartition, OffsetAndMetadata],
                                     options: AlterConsumerGroupOffsetsOptions | None = None
                                     ) -> AlterConsumerGroupOffsetsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implement yet")``, as Java's mock."""
        return self._alter_consumer_group_offsets(group_id, offsets)

    def list_offsets(self, *, topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
                     options: ListOffsetsOptions | None = None) -> ListOffsetsResult[Sync]:
        """The offsets set by ``update_beginning_offsets`` for
        ``OffsetSpec.earliest()`` and by ``update_end_offsets`` for every other spec;
        ``OffsetSpec.for_timestamp`` raises ``UnsupportedVersionError("Not implement
        yet")``, as Java's mock (``options`` is ignored)."""
        return self._list_offsets(topic_partition_offsets)

    def describe_client_quotas(self, *, filter: ClientQuotaFilter,  # noqa: A002 - Java's name
                               options: DescribeClientQuotasOptions | None = None
                               ) -> DescribeClientQuotasResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implement yet")``, as Java's mock."""
        return self._describe_client_quotas(filter)

    def alter_client_quotas(self, *, entries: Iterable[ClientQuotaAlteration],
                            options: AlterClientQuotasOptions | None = None
                            ) -> AlterClientQuotasResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implement yet")``, as Java's mock."""
        return self._alter_client_quotas(entries)

    @overload
    def describe_user_scram_credentials(self, *, users: Sequence[str] | None = None
                                        ) -> DescribeUserScramCredentialsResult[Sync]: ...
    @overload
    def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None,
            options: DescribeUserScramCredentialsOptions
    ) -> DescribeUserScramCredentialsResult[Sync]: ...

    @java_forms(*_DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS)
    def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None = UNSET,
            options: DescribeUserScramCredentialsOptions | None = None
    ) -> DescribeUserScramCredentialsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._describe_user_scram_credentials(None if users is UNSET else users)

    def alter_user_scram_credentials(
            self, *, alterations: Sequence[UserScramCredentialAlteration],
            options: AlterUserScramCredentialsOptions | None = None
    ) -> AlterUserScramCredentialsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._alter_user_scram_credentials(alterations)

    def describe_features(self, *, options: DescribeFeaturesOptions | None = None
                          ) -> DescribeFeaturesResult[Sync]:
        """Each feature level of the builder as a finalized ``[level, level]`` range,
        with the builder's supported range, finalized epoch 123 (``options`` is
        ignored)."""
        return self._describe_features()

    def update_features(self, *, feature_updates: Mapping[str, FeatureUpdate],
                        options: UpdateFeaturesOptions | None = None
                        ) -> UpdateFeaturesResult[Sync]:
        """Check each update against the current level and the supported range; the
        first that fails fails every future with ``InvalidRequestError``, else each
        level is set (not with ``options.validate_only()``)."""
        if options is None:
            options = UpdateFeaturesOptions()
        return self._update_features(feature_updates, options.validate_only())

    def unregister_controller(self, *, controller_id: int,
                              options: UnregisterControllerOptions | None = None
                              ) -> UnregisterControllerResult[Sync]:
        """A completed future when the mock uses a raft controller, else one failed
        with ``UnsupportedVersionError("")``, as Java's mock."""
        return self._unregister_controller(controller_id)

    def describe_producers(self, *, partitions: Iterable[TopicPartition],
                           options: DescribeProducersOptions | None = None
                           ) -> DescribeProducersResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._describe_producers(partitions)

    def describe_transactions(self, *, transactional_ids: Iterable[str],
                              options: DescribeTransactionsOptions | None = None
                              ) -> DescribeTransactionsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._describe_transactions(transactional_ids)

    def abort_transaction(self, *, spec: AbortTransactionSpec,
                          options: AbortTransactionOptions | None = None
                          ) -> AbortTransactionResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._abort_transaction(spec)

    def list_transactions(self, *, options: ListTransactionsOptions | None = None
                          ) -> ListTransactionsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._list_transactions()

    def fence_producers(self, *, transactional_ids: Iterable[str],
                        options: FenceProducersOptions | None = None
                        ) -> FenceProducersResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._fence_producers(transactional_ids)

    @overload
    def list_config_resources(self, *, config_resource_types: Iterable[ConfigResource.Type],
                              options: ListConfigResourcesOptions
                              ) -> ListConfigResourcesResult[Sync]: ...
    @overload
    def list_config_resources(self) -> ListConfigResourcesResult[Sync]: ...

    @java_forms(*_LIST_CONFIG_RESOURCES_FORMS)
    def list_config_resources(self, *,
                              config_resource_types: Iterable[ConfigResource.Type] = UNSET,
                              options: ListConfigResourcesOptions | None = None
                              ) -> ListConfigResourcesResult[Sync]:
        """The topic, broker, broker-logger, client-metrics and group resources of the
        given types (all for none); ``options`` is ignored."""
        return self._list_config_resources(config_resource_types)

    def describe_classic_groups(self, *, group_ids: Iterable[str],
                                options: DescribeClassicGroupsOptions | None = None
                                ) -> DescribeClassicGroupsResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._describe_classic_groups(group_ids)

    def force_terminate_transaction(self, *, transactional_id: str,
                                    options: TerminateTransactionOptions | None = None
                                    ) -> TerminateTransactionResult[Sync]:
        """Raises ``UnsupportedVersionError("Not implemented yet")``, as Java's mock."""
        return self._force_terminate_transaction(transactional_id)

    # ---- The Java mock's own methods with a future in their type -----------------
    # (the others are inherited from _mock_core, typed the same on both classes)

    def handle_describe_topics_using_ids(
            self, *, topic_ids: Iterable[Uuid]) -> dict[Uuid, KafkaFuture[TopicDescription]]:
        """The futures ``describe_topics`` gives for a ``TopicIdCollection``."""
        return self._handle_describe_topics_using_ids(topic_ids)  # type: ignore[return-value]
