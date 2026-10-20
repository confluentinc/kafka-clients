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

from confluent_kafka._args import MISSING, Form, java_forms
from confluent_kafka.admin import _conversions
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
         defaults={"config_resource_types": (), "options": MISSING}),
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
    def __init__(self, *, brokers: Sequence[Node] = MISSING,
                 controller: Node = MISSING) -> None:
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

    @overload
    def close(self) -> None: ...
    @overload
    def close(self, *, timeout: Duration) -> None: ...

    def close(self, *, timeout: Duration = MISSING) -> None:
        """Does nothing, as Java's mock (no check of ``timeout`` either)."""
        self._close()

    def __enter__(self) -> MockAdminClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @overload
    def create_topics(self, *, new_topics: Iterable[NewTopic]) -> CreateTopicsResult[Sync]: ...
    @overload
    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions) -> CreateTopicsResult[Sync]: ...

    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions = MISSING) -> CreateTopicsResult[Sync]:
        """Create the topics: ``default_replication_factor`` / ``default_partitions``
        stand for ``-1``, the first brokers are the replicas and the first broker
        the leader; ``options`` is ignored."""
        return self._create_topics(new_topics)

    @overload
    def delete_topics(self, *, topics: Iterable[str]) -> DeleteTopicsResult[Sync]: ...
    @overload
    def delete_topics(self, *, topics: Iterable[str],
                      options: DeleteTopicsOptions) -> DeleteTopicsResult[Sync]: ...
    @overload
    def delete_topics(self, *, topics: TopicCollection) -> DeleteTopicsResult[Sync]: ...
    @overload
    def delete_topics(self, *, topics: TopicCollection,
                      options: DeleteTopicsOptions) -> DeleteTopicsResult[Sync]: ...

    def delete_topics(self, *, topics: Iterable[str] | TopicCollection,
                      options: DeleteTopicsOptions = MISSING) -> DeleteTopicsResult[Sync]:
        """Delete the topics, by name or by id; ``options`` is ignored."""
        if topics is None:  # the shape check: Java's compiler rejects deleteTopics(null)
            raise _conversions.ambiguous_none("delete_topics", "topics",
                                              ("Iterable[str]", "TopicCollection"))
        return self._delete_topics(topics)

    @overload
    def list_topics(self) -> ListTopicsResult[Sync]: ...
    @overload
    def list_topics(self, *, options: ListTopicsOptions) -> ListTopicsResult[Sync]: ...

    def list_topics(self, *, options: ListTopicsOptions = MISSING) -> ListTopicsResult[Sync]:
        """List every topic, internal ones included (``options`` is ignored)."""
        return self._list_topics()

    @overload
    def describe_topics(self, *, topic_names: Iterable[str]) -> DescribeTopicsResult[Sync]: ...
    @overload
    def describe_topics(self, *, topic_names: Iterable[str],
                        options: DescribeTopicsOptions) -> DescribeTopicsResult[Sync]: ...
    @overload
    def describe_topics(self, *, topics: TopicCollection) -> DescribeTopicsResult[Sync]: ...
    @overload
    def describe_topics(self, *, topics: TopicCollection,
                        options: DescribeTopicsOptions) -> DescribeTopicsResult[Sync]: ...

    @java_forms(*_DESCRIBE_TOPICS_FORMS)
    def describe_topics(self, *, topic_names: Iterable[str] = MISSING,
                        topics: TopicCollection = MISSING,
                        options: DescribeTopicsOptions = MISSING
                        ) -> DescribeTopicsResult[Sync]:
        """Describe the topics, by name or by id; ``options`` is ignored."""
        return self._describe_topics(topic_names, topics)

    @overload
    def describe_cluster(self) -> DescribeClusterResult[Sync]: ...
    @overload
    def describe_cluster(self, *,
                         options: DescribeClusterOptions) -> DescribeClusterResult[Sync]: ...

    def describe_cluster(self, *, options: DescribeClusterOptions = MISSING
                         ) -> DescribeClusterResult[Sync]:
        """The brokers, the controller, the cluster id and no authorized operations
        (``options`` is ignored)."""
        return self._describe_cluster()

    @overload
    def describe_acls(self, *, filter: AclBindingFilter) -> DescribeAclsResult[Sync]: ...
    @overload
    def describe_acls(self, *, filter: AclBindingFilter,
                      options: DescribeAclsOptions) -> DescribeAclsResult[Sync]: ...

    def describe_acls(self, *, filter: AclBindingFilter,  # noqa: A002 - Java's name
                      options: DescribeAclsOptions = MISSING) -> DescribeAclsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._describe_acls(filter)

    @overload
    def create_acls(self, *, acls: Iterable[AclBinding]) -> CreateAclsResult[Sync]: ...
    @overload
    def create_acls(self, *, acls: Iterable[AclBinding],
                    options: CreateAclsOptions) -> CreateAclsResult[Sync]: ...

    def create_acls(self, *, acls: Iterable[AclBinding],
                    options: CreateAclsOptions = MISSING) -> CreateAclsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._create_acls(acls)

    @overload
    def delete_acls(self, *, filters: Iterable[AclBindingFilter]) -> DeleteAclsResult[Sync]: ...
    @overload
    def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                    options: DeleteAclsOptions) -> DeleteAclsResult[Sync]: ...

    def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                    options: DeleteAclsOptions = MISSING) -> DeleteAclsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._delete_acls(filters)

    @overload
    def describe_configs(self, *,
                         resources: Iterable[ConfigResource]) -> DescribeConfigsResult[Sync]: ...
    @overload
    def describe_configs(self, *, resources: Iterable[ConfigResource],
                         options: DescribeConfigsOptions) -> DescribeConfigsResult[Sync]: ...

    def describe_configs(self, *, resources: Iterable[ConfigResource],
                         options: DescribeConfigsOptions = MISSING
                         ) -> DescribeConfigsResult[Sync]:
        """The configs of broker, topic, client-metrics and group resources; another
        type fails its future with ``UnsupportedOperationError("Not implemented
        yet")`` (``options`` is ignored)."""
        return self._describe_configs(resources)

    @overload
    def incremental_alter_configs(
        self, *,
        configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
    ) -> AlterConfigsResult[Sync]: ...
    @overload
    def incremental_alter_configs(self, *,
                                  configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
                                  options: AlterConfigsOptions) -> AlterConfigsResult[Sync]: ...

    def incremental_alter_configs(
            self, *, configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
            options: AlterConfigsOptions = MISSING) -> AlterConfigsResult[Sync]:
        """Apply ``SET`` and ``DELETE`` operations to broker, topic, client-metrics
        and group configs (``options`` is ignored)."""
        return self._incremental_alter_configs(configs)

    @overload
    def alter_replica_log_dirs(
        self, *,
        replica_assignment: Mapping[TopicPartitionReplica, str],
    ) -> AlterReplicaLogDirsResult[Sync]: ...
    @overload
    def alter_replica_log_dirs(
        self, *,
        replica_assignment: Mapping[TopicPartitionReplica, str],
        options: AlterReplicaLogDirsOptions,
    ) -> AlterReplicaLogDirsResult[Sync]: ...

    def alter_replica_log_dirs(self, *, replica_assignment: Mapping[TopicPartitionReplica, str],
                               options: AlterReplicaLogDirsOptions = MISSING
                               ) -> AlterReplicaLogDirsResult[Sync]:
        """Record a move of each replica to the given log directory, which must be
        one of its broker's (``options`` is ignored)."""
        return self._alter_replica_log_dirs(replica_assignment)

    @overload
    def describe_log_dirs(self, *, brokers: Iterable[int]) -> DescribeLogDirsResult[Sync]: ...
    @overload
    def describe_log_dirs(self, *, brokers: Iterable[int],
                          options: DescribeLogDirsOptions) -> DescribeLogDirsResult[Sync]: ...

    def describe_log_dirs(self, *, brokers: Iterable[int],
                          options: DescribeLogDirsOptions = MISSING
                          ) -> DescribeLogDirsResult[Sync]:
        """The log directories of the brokers, with every replica of every topic in
        its partition's first log directory (``options`` is ignored)."""
        return self._describe_log_dirs(brokers)

    @overload
    def describe_replica_log_dirs(
        self, *,
        replicas: Iterable[TopicPartitionReplica],
    ) -> DescribeReplicaLogDirsResult[Sync]: ...
    @overload
    def describe_replica_log_dirs(
        self, *,
        replicas: Iterable[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> DescribeReplicaLogDirsResult[Sync]: ...

    def describe_replica_log_dirs(self, *, replicas: Iterable[TopicPartitionReplica],
                                  options: DescribeReplicaLogDirsOptions = MISSING
                                  ) -> DescribeReplicaLogDirsResult[Sync]:
        """The current and future log directory of each replica of a known topic
        (``options`` is ignored)."""
        return self._describe_replica_log_dirs(replicas)

    @overload
    def create_partitions(
        self, *,
        new_partitions: Mapping[str, NewPartitions],
    ) -> CreatePartitionsResult[Sync]: ...
    @overload
    def create_partitions(self, *, new_partitions: Mapping[str, NewPartitions],
                          options: CreatePartitionsOptions) -> CreatePartitionsResult[Sync]: ...

    def create_partitions(self, *, new_partitions: Mapping[str, NewPartitions],
                          options: CreatePartitionsOptions = MISSING
                          ) -> CreatePartitionsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._create_partitions(new_partitions)

    @overload
    def delete_records(
        self, *,
        records_to_delete: Mapping[TopicPartition, RecordsToDelete],
    ) -> DeleteRecordsResult[Sync]: ...
    @overload
    def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                       options: DeleteRecordsOptions) -> DeleteRecordsResult[Sync]: ...

    def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                       options: DeleteRecordsOptions = MISSING) -> DeleteRecordsResult[Sync]:
        """An empty result for no records; otherwise raises
        ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._delete_records(records_to_delete)

    @overload
    def create_delegation_token(self) -> CreateDelegationTokenResult[Sync]: ...
    @overload
    def create_delegation_token(
        self, *,
        options: CreateDelegationTokenOptions,
    ) -> CreateDelegationTokenResult[Sync]: ...

    def create_delegation_token(self, *, options: CreateDelegationTokenOptions = MISSING
                                ) -> CreateDelegationTokenResult[Sync]:
        """Create a token owned by the first of ``options.renewers()``, issued now, its
        maximum timestamp ``options.max_lifetime_ms()``, its HMAC its id's bytes; a
        renewer not of the ``User`` type fails the future with
        ``InvalidPrincipalTypeError``, and no renewer raises ``IndexError``, as
        Java's mock (``options.owner()`` is ignored)."""
        return self._create_delegation_token(
            _conversions.options_or_new(options, CreateDelegationTokenOptions))

    @overload
    def renew_delegation_token(self, *, hmac: bytes) -> RenewDelegationTokenResult[Sync]: ...
    @overload
    def renew_delegation_token(
        self, *,
        hmac: bytes,
        options: RenewDelegationTokenOptions,
    ) -> RenewDelegationTokenResult[Sync]: ...

    def renew_delegation_token(self, *, hmac: bytes,
                               options: RenewDelegationTokenOptions = MISSING
                               ) -> RenewDelegationTokenResult[Sync]:
        """Set the expiry timestamp of the tokens with this HMAC to
        ``options.renew_time_period_ms()``, the future's value; none fails it with
        ``DelegationTokenNotFoundError``."""
        return self._renew_delegation_token(
            hmac, _conversions.options_or_new(options, RenewDelegationTokenOptions))

    @overload
    def expire_delegation_token(self, *, hmac: bytes) -> ExpireDelegationTokenResult[Sync]: ...
    @overload
    def expire_delegation_token(
        self, *,
        hmac: bytes,
        options: ExpireDelegationTokenOptions,
    ) -> ExpireDelegationTokenResult[Sync]: ...

    def expire_delegation_token(self, *, hmac: bytes,
                                options: ExpireDelegationTokenOptions = MISSING
                                ) -> ExpireDelegationTokenResult[Sync]:
        """Remove the tokens with this HMAC when ``options.expiry_time_period_ms()`` is
        -1 or in the past, the future's value being that period; none fails it with
        ``DelegationTokenNotFoundError``."""
        return self._expire_delegation_token(
            hmac, _conversions.options_or_new(options, ExpireDelegationTokenOptions))

    @overload
    def describe_delegation_token(self) -> DescribeDelegationTokenResult[Sync]: ...
    @overload
    def describe_delegation_token(
        self, *,
        options: DescribeDelegationTokenOptions,
    ) -> DescribeDelegationTokenResult[Sync]: ...

    def describe_delegation_token(self, *,
                                  options: DescribeDelegationTokenOptions = MISSING
                                  ) -> DescribeDelegationTokenResult[Sync]:
        """The tokens of ``options.owners()``, all for an empty list; the default
        owners (``None``) raise ``TypeError``, where Java's mock throws
        ``NullPointerException`` (decision 10)."""
        return self._describe_delegation_token(
            _conversions.options_or_new(options, DescribeDelegationTokenOptions))

    @overload
    def describe_consumer_groups(
        self, *,
        group_ids: Iterable[str],
        options: DescribeConsumerGroupsOptions,
    ) -> DescribeConsumerGroupsResult[Sync]: ...
    @overload
    def describe_consumer_groups(
        self, *,
        group_ids: Iterable[str],
    ) -> DescribeConsumerGroupsResult[Sync]: ...

    def describe_consumer_groups(self, *, group_ids: Iterable[str],
                                 options: DescribeConsumerGroupsOptions = MISSING
                                 ) -> DescribeConsumerGroupsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._describe_consumer_groups(group_ids)

    @overload
    def list_consumer_group_offsets(
        self, *,
        group_id: str,
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult[Sync]: ...
    @overload
    def list_consumer_group_offsets(self, *,
                                    group_id: str) -> ListConsumerGroupOffsetsResult[Sync]: ...
    @overload
    def list_consumer_group_offsets(
        self, *,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec],
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult[Sync]: ...
    @overload
    def list_consumer_group_offsets(
        self, *,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec],
    ) -> ListConsumerGroupOffsetsResult[Sync]: ...

    @java_forms(*_LIST_CONSUMER_GROUP_OFFSETS_FORMS)
    def list_consumer_group_offsets(
        self, *, group_id: str = MISSING,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec] = MISSING,
        options: ListConsumerGroupOffsetsOptions = MISSING
    ) -> ListConsumerGroupOffsetsResult[Sync]:
        """The offsets set by ``update_consumer_group_offsets`` for one group: all
        for a spec with no partitions, else those of its partitions. More or fewer
        than one group raises ``UnsupportedOperationError("Not implemented yet")``;
        ``group_id`` (Java's ``new ListConsumerGroupOffsetsSpec()``, whose
        partitions are ``None``) raises ``TypeError`` once an offset is stored,
        where Java's mock throws ``NullPointerException``; ``options`` is ignored."""
        if group_specs is MISSING:  # java_forms: one or the other
            # Admin.listConsumerGroupOffsets(groupId, options): singletonMap, a None
            # group id included.
            group_specs = {group_id: ListConsumerGroupOffsetsSpec()}
        return self._list_consumer_group_offsets(group_specs)

    @overload
    def delete_consumer_groups(
        self, *,
        group_ids: Iterable[str],
        options: DeleteConsumerGroupsOptions,
    ) -> DeleteConsumerGroupsResult[Sync]: ...
    @overload
    def delete_consumer_groups(self, *,
                               group_ids: Iterable[str]) -> DeleteConsumerGroupsResult[Sync]: ...

    def delete_consumer_groups(self, *, group_ids: Iterable[str],
                               options: DeleteConsumerGroupsOptions = MISSING
                               ) -> DeleteConsumerGroupsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._delete_consumer_groups(group_ids)

    @overload
    def delete_consumer_group_offsets(
        self, *,
        group_id: str,
        partitions: Iterable[TopicPartition],
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> DeleteConsumerGroupOffsetsResult[Sync]: ...
    @overload
    def delete_consumer_group_offsets(
        self, *,
        group_id: str,
        partitions: Iterable[TopicPartition],
    ) -> DeleteConsumerGroupOffsetsResult[Sync]: ...

    def delete_consumer_group_offsets(self, *, group_id: str,
                                      partitions: Iterable[TopicPartition],
                                      options: DeleteConsumerGroupOffsetsOptions = MISSING
                                      ) -> DeleteConsumerGroupOffsetsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._delete_consumer_group_offsets(group_id, partitions)

    @overload
    def list_groups(self) -> ListGroupsResult[Sync]: ...
    @overload
    def list_groups(self, *, options: ListGroupsOptions) -> ListGroupsResult[Sync]: ...

    def list_groups(self, *, options: ListGroupsOptions = MISSING) -> ListGroupsResult[Sync]:
        """One stable consumer group of the ``consumer`` protocol per group with
        configs (``options`` and its filters are ignored)."""
        return self._list_groups()

    @overload
    def elect_leaders(self, *, election_type: ElectionType,
                      partitions: Iterable[TopicPartition] | None) -> ElectLeadersResult[Sync]: ...
    @overload
    def elect_leaders(self, *, election_type: ElectionType,
                      partitions: Iterable[TopicPartition] | None,
                      options: ElectLeadersOptions) -> ElectLeadersResult[Sync]: ...

    def elect_leaders(self, *, election_type: ElectionType,
                      partitions: Iterable[TopicPartition] | None,
                      options: ElectLeadersOptions = MISSING) -> ElectLeadersResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._elect_leaders(election_type, partitions)

    @overload
    def alter_partition_reassignments(
        self, *,
        reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
    ) -> AlterPartitionReassignmentsResult[Sync]: ...
    @overload
    def alter_partition_reassignments(
        self, *,
        reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
        options: AlterPartitionReassignmentsOptions,
    ) -> AlterPartitionReassignmentsResult[Sync]: ...

    def alter_partition_reassignments(
            self, *, reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
            options: AlterPartitionReassignmentsOptions = MISSING
            ) -> AlterPartitionReassignmentsResult[Sync]:
        """Store each reassignment, or cancel it for ``None`` (Java's empty
        ``Optional``); an unknown partition fails its future with
        ``UnknownTopicOrPartitionError`` (``options`` is ignored)."""
        return self._alter_partition_reassignments(reassignments)

    @overload
    def list_partition_reassignments(self) -> ListPartitionReassignmentsResult[Sync]: ...
    @overload
    def list_partition_reassignments(
        self, *,
        partitions: Iterable[TopicPartition],
    ) -> ListPartitionReassignmentsResult[Sync]: ...
    @overload
    def list_partition_reassignments(
        self, *,
        partitions: Iterable[TopicPartition],
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult[Sync]: ...
    @overload
    def list_partition_reassignments(
        self, *,
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult[Sync]: ...

    def list_partition_reassignments(
            self, *, partitions: Iterable[TopicPartition] = MISSING,
            options: ListPartitionReassignmentsOptions = MISSING
            ) -> ListPartitionReassignmentsResult[Sync]:
        """The stored reassignments of ``partitions`` (all when ``None``, Java's
        ``listPartitionReassignments()`` / ``Optional.empty()``), with the replicas
        being added and removed (``options`` is ignored)."""
        if partitions is MISSING:  # Java's listPartitionReassignments(options): Optional.empty()
            return self._list_partition_reassignments(None)
        _conversions.non_null(partitions, "partitions")  # Optional.of(partitions)
        return self._list_partition_reassignments(partitions)

    def remove_members_from_consumer_group(
        self, *, group_id: str, options: RemoveMembersFromConsumerGroupOptions
    ) -> RemoveMembersFromConsumerGroupResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._remove_members_from_consumer_group(group_id, options)

    @overload
    def alter_consumer_group_offsets(
        self, *,
        group_id: str,
        offsets: Mapping[TopicPartition, OffsetAndMetadata],
    ) -> AlterConsumerGroupOffsetsResult[Sync]: ...
    @overload
    def alter_consumer_group_offsets(
        self, *,
        group_id: str,
        offsets: Mapping[TopicPartition, OffsetAndMetadata],
        options: AlterConsumerGroupOffsetsOptions,
    ) -> AlterConsumerGroupOffsetsResult[Sync]: ...

    def alter_consumer_group_offsets(self, *, group_id: str,
                                     offsets: Mapping[TopicPartition, OffsetAndMetadata],
                                     options: AlterConsumerGroupOffsetsOptions = MISSING
                                     ) -> AlterConsumerGroupOffsetsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implement yet")``, as Java's mock."""
        return self._alter_consumer_group_offsets(group_id, offsets)

    @overload
    def list_offsets(
        self, *,
        topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
    ) -> ListOffsetsResult[Sync]: ...
    @overload
    def list_offsets(self, *, topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
                     options: ListOffsetsOptions) -> ListOffsetsResult[Sync]: ...

    def list_offsets(self, *, topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
                     options: ListOffsetsOptions = MISSING) -> ListOffsetsResult[Sync]:
        """The offsets set by ``update_beginning_offsets`` for
        ``OffsetSpec.earliest()`` and by ``update_end_offsets`` for every other spec;
        ``OffsetSpec.for_timestamp`` raises ``UnsupportedOperationError("Not implement
        yet")``, as Java's mock (``options`` is ignored)."""
        return self._list_offsets(topic_partition_offsets)

    @overload
    def describe_client_quotas(self, *,
                               filter: ClientQuotaFilter) -> DescribeClientQuotasResult[Sync]: ...
    @overload
    def describe_client_quotas(
        self, *,
        filter: ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> DescribeClientQuotasResult[Sync]: ...

    def describe_client_quotas(self, *, filter: ClientQuotaFilter,  # noqa: A002 - Java's name
                               options: DescribeClientQuotasOptions = MISSING
                               ) -> DescribeClientQuotasResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implement yet")``, as Java's mock."""
        return self._describe_client_quotas(filter)

    @overload
    def alter_client_quotas(
        self, *,
        entries: Iterable[ClientQuotaAlteration],
    ) -> AlterClientQuotasResult[Sync]: ...
    @overload
    def alter_client_quotas(
        self, *,
        entries: Iterable[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> AlterClientQuotasResult[Sync]: ...

    def alter_client_quotas(self, *, entries: Iterable[ClientQuotaAlteration],
                            options: AlterClientQuotasOptions = MISSING
                            ) -> AlterClientQuotasResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implement yet")``, as Java's mock."""
        return self._alter_client_quotas(entries)

    @overload
    def describe_user_scram_credentials(self) -> DescribeUserScramCredentialsResult[Sync]: ...
    @overload
    def describe_user_scram_credentials(
        self, *,
        users: Sequence[str] | None,
    ) -> DescribeUserScramCredentialsResult[Sync]: ...
    @overload
    def describe_user_scram_credentials(
        self, *,
        users: Sequence[str] | None,
        options: DescribeUserScramCredentialsOptions,
    ) -> DescribeUserScramCredentialsResult[Sync]: ...

    @java_forms(*_DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS)
    def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None = MISSING,
            options: DescribeUserScramCredentialsOptions = MISSING
    ) -> DescribeUserScramCredentialsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._describe_user_scram_credentials(None if users is MISSING else users)

    @overload
    def alter_user_scram_credentials(
        self, *,
        alterations: Sequence[UserScramCredentialAlteration],
    ) -> AlterUserScramCredentialsResult[Sync]: ...
    @overload
    def alter_user_scram_credentials(
        self, *,
        alterations: Sequence[UserScramCredentialAlteration],
        options: AlterUserScramCredentialsOptions,
    ) -> AlterUserScramCredentialsResult[Sync]: ...

    def alter_user_scram_credentials(
            self, *, alterations: Sequence[UserScramCredentialAlteration],
            options: AlterUserScramCredentialsOptions = MISSING
    ) -> AlterUserScramCredentialsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._alter_user_scram_credentials(alterations)

    @overload
    def describe_features(self) -> DescribeFeaturesResult[Sync]: ...
    @overload
    def describe_features(self, *,
                          options: DescribeFeaturesOptions) -> DescribeFeaturesResult[Sync]: ...

    def describe_features(self, *, options: DescribeFeaturesOptions = MISSING
                          ) -> DescribeFeaturesResult[Sync]:
        """Each feature level of the builder as a finalized ``[level, level]`` range,
        with the builder's supported range, finalized epoch 123 (``options`` is
        ignored)."""
        return self._describe_features()

    @overload
    def update_features(
        self, *,
        feature_updates: Mapping[str, FeatureUpdate],
    ) -> UpdateFeaturesResult[Sync]: ...
    @overload
    def update_features(self, *, feature_updates: Mapping[str, FeatureUpdate],
                        options: UpdateFeaturesOptions) -> UpdateFeaturesResult[Sync]: ...

    def update_features(self, *, feature_updates: Mapping[str, FeatureUpdate],
                        options: UpdateFeaturesOptions = MISSING
                        ) -> UpdateFeaturesResult[Sync]:
        """Check each update against the current level and the supported range; the
        first that fails fails every future with ``InvalidRequestError``, else each
        level is set (not with ``options.validate_only()``)."""
        if options is MISSING:
            options = UpdateFeaturesOptions()
        return self._update_features(feature_updates, options)

    @overload
    def unregister_controller(self, *, controller_id: int) -> UnregisterControllerResult[Sync]: ...
    @overload
    def unregister_controller(
        self, *,
        controller_id: int,
        options: UnregisterControllerOptions,
    ) -> UnregisterControllerResult[Sync]: ...

    def unregister_controller(self, *, controller_id: int,
                              options: UnregisterControllerOptions = MISSING
                              ) -> UnregisterControllerResult[Sync]:
        """A completed future when the mock uses a raft controller, else one failed
        with ``UnsupportedVersionError("")``, as Java's mock."""
        _conversions.non_null(controller_id, "controller_id")  # Java's int parameter
        return self._unregister_controller(controller_id)

    @overload
    def describe_producers(
        self, *,
        partitions: Iterable[TopicPartition],
    ) -> DescribeProducersResult[Sync]: ...
    @overload
    def describe_producers(self, *, partitions: Iterable[TopicPartition],
                           options: DescribeProducersOptions) -> DescribeProducersResult[Sync]: ...

    def describe_producers(self, *, partitions: Iterable[TopicPartition],
                           options: DescribeProducersOptions = MISSING
                           ) -> DescribeProducersResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._describe_producers(partitions)

    @overload
    def describe_transactions(
        self, *,
        transactional_ids: Iterable[str],
    ) -> DescribeTransactionsResult[Sync]: ...
    @overload
    def describe_transactions(
        self, *,
        transactional_ids: Iterable[str],
        options: DescribeTransactionsOptions,
    ) -> DescribeTransactionsResult[Sync]: ...

    def describe_transactions(self, *, transactional_ids: Iterable[str],
                              options: DescribeTransactionsOptions = MISSING
                              ) -> DescribeTransactionsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._describe_transactions(transactional_ids)

    @overload
    def abort_transaction(self, *, spec: AbortTransactionSpec) -> AbortTransactionResult[Sync]: ...
    @overload
    def abort_transaction(self, *, spec: AbortTransactionSpec,
                          options: AbortTransactionOptions) -> AbortTransactionResult[Sync]: ...

    def abort_transaction(self, *, spec: AbortTransactionSpec,
                          options: AbortTransactionOptions = MISSING
                          ) -> AbortTransactionResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._abort_transaction(spec)

    @overload
    def list_transactions(self) -> ListTransactionsResult[Sync]: ...
    @overload
    def list_transactions(self, *,
                          options: ListTransactionsOptions) -> ListTransactionsResult[Sync]: ...

    def list_transactions(self, *, options: ListTransactionsOptions = MISSING
                          ) -> ListTransactionsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._list_transactions()

    @overload
    def fence_producers(self, *,
                        transactional_ids: Iterable[str]) -> FenceProducersResult[Sync]: ...
    @overload
    def fence_producers(self, *, transactional_ids: Iterable[str],
                        options: FenceProducersOptions) -> FenceProducersResult[Sync]: ...

    def fence_producers(self, *, transactional_ids: Iterable[str],
                        options: FenceProducersOptions = MISSING
                        ) -> FenceProducersResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._fence_producers(transactional_ids)

    @overload
    def list_config_resources(
        self, *,
        config_resource_types: Iterable[ConfigResource.Type],
        options: ListConfigResourcesOptions,
    ) -> ListConfigResourcesResult[Sync]: ...
    @overload
    def list_config_resources(self) -> ListConfigResourcesResult[Sync]: ...

    @java_forms(*_LIST_CONFIG_RESOURCES_FORMS)
    def list_config_resources(self, *,
                              config_resource_types: Iterable[ConfigResource.Type] = MISSING,
                              options: ListConfigResourcesOptions = MISSING
                              ) -> ListConfigResourcesResult[Sync]:
        """The topic, broker, broker-logger, client-metrics and group resources of the
        given types (all for none); ``options`` is ignored."""
        return self._list_config_resources(config_resource_types)

    @overload
    def describe_classic_groups(
        self, *,
        group_ids: Iterable[str],
        options: DescribeClassicGroupsOptions,
    ) -> DescribeClassicGroupsResult[Sync]: ...
    @overload
    def describe_classic_groups(self, *,
                                group_ids: Iterable[str]) -> DescribeClassicGroupsResult[Sync]: ...

    def describe_classic_groups(self, *, group_ids: Iterable[str],
                                options: DescribeClassicGroupsOptions = MISSING
                                ) -> DescribeClassicGroupsResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._describe_classic_groups(group_ids)

    @overload
    def force_terminate_transaction(self, *,
                                    transactional_id: str) -> TerminateTransactionResult[Sync]: ...
    @overload
    def force_terminate_transaction(
        self, *,
        transactional_id: str,
        options: TerminateTransactionOptions,
    ) -> TerminateTransactionResult[Sync]: ...

    def force_terminate_transaction(self, *, transactional_id: str,
                                    options: TerminateTransactionOptions = MISSING
                                    ) -> TerminateTransactionResult[Sync]:
        """Raises ``UnsupportedOperationError("Not implemented yet")``, as Java's mock."""
        return self._force_terminate_transaction(transactional_id)

    # ---- The Java mock's own methods with a future in their type -----------------
    # (the others are inherited from _mock_core, typed the same on both classes)

    def handle_describe_topics_using_ids(
            self, *, topic_ids: Iterable[Uuid]) -> dict[Uuid, KafkaFuture[TopicDescription]]:
        """The futures ``describe_topics`` gives for a ``TopicIdCollection``."""
        return self._handle_describe_topics_using_ids(topic_ids)  # type: ignore[return-value]
