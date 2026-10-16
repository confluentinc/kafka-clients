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

"""``AsyncMockAdminClient``: the asyncio peer of
:class:`~confluent_kafka.admin.MockAdminClient`.

By rule 3, ``MockAdminClient``'s constructors and methods (CLAUDE.md, Python
Binding Conventions, Class family), over the same private ``_mock_core``: the
``Admin`` methods are ``async def`` where :class:`AsyncAdmin`'s are (``close``),
the RPCs, the Java mock's own methods and ``create()`` plain ``def``, as none of
them waits. The nested ``Builder`` is repeated with the swap (ruling
24): ``AsyncMockAdminClient.Builder.build()`` returns an ``AsyncMockAdminClient``.

Like its sync peer, it leaves out the overloads ``Admin`` does not generate (see
``Admin``'s module docstring).

A result's futures are ``AsyncKafkaFuture``\\ s over the private
``_KafkaFutureImpl`` (Java's ``KafkaFutureImpl``), most of them complete on return.
Java's ``synchronized`` is the core's re-entrant lock, held for a body that never
awaits: each method takes it, runs, and releases it before it returns
(Implementation over the FFI). As in Java, ``close()`` does nothing.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, overload

from confluent_kafka._args import UNSET, java_forms
from confluent_kafka.admin._futures import Async
from confluent_kafka.admin._mock_core import MockAdminClientBuilderCore, MockAdminClientCore
from confluent_kafka.admin.async_admin import AsyncAdmin
from confluent_kafka.admin.create_delegation_token_options import CreateDelegationTokenOptions
from confluent_kafka.admin.describe_delegation_token_options import DescribeDelegationTokenOptions
from confluent_kafka.admin.expire_delegation_token_options import ExpireDelegationTokenOptions
from confluent_kafka.admin.list_consumer_group_offsets_spec import ListConsumerGroupOffsetsSpec
from confluent_kafka.admin.mock_admin_client import (
    _DESCRIBE_TOPICS_FORMS,
    _DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS,
    _INIT_FORMS,
    _LIST_CONFIG_RESOURCES_FORMS,
    _LIST_CONSUMER_GROUP_OFFSETS_FORMS,
)
from confluent_kafka.admin.renew_delegation_token_options import RenewDelegationTokenOptions
from confluent_kafka.admin.update_features_options import UpdateFeaturesOptions

if TYPE_CHECKING:
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture
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

__all__ = ["AsyncMockAdminClient"]


class AsyncMockAdminClient(MockAdminClientCore, AsyncAdmin):
    """The asyncio peer of ``MockAdminClient``, a mock of the ``Admin`` interface
    you can use for testing code that uses the admin client: see
    ``MockAdminClient``.

    Java: ``org.apache.kafka.clients.admin.MockAdminClient``.
    """

    _kind = Async

    class Builder(MockAdminClientBuilderCore):
        """Builds an ``AsyncMockAdminClient``: see ``MockAdminClient.Builder``.

        Java: ``org.apache.kafka.clients.admin.MockAdminClient.Builder``.
        """

        def build(self) -> AsyncMockAdminClient:
            """The mock built from this builder's fields (``IndexError`` with no
            broker, as Java's ``brokers.get(0)``)."""
            return AsyncMockAdminClient._new(**self._build_args())

    @staticmethod
    def create() -> AsyncMockAdminClient.Builder:  # type: ignore[override]
        """A new ``AsyncMockAdminClient.Builder``."""
        return AsyncMockAdminClient.Builder()

    @overload
    def __init__(self) -> None: ...
    @overload
    def __init__(self, *, brokers: Sequence[Node], controller: Node) -> None: ...

    @java_forms(*_INIT_FORMS)
    def __init__(self, *, brokers: Sequence[Node] | None = None,
                 controller: Node | None = None) -> None:
        """See :meth:`MockAdminClient.__init__`."""
        AsyncAdmin.__init__(self)
        self._init_two(brokers, controller)

    @classmethod
    def _new(cls, *, brokers: Sequence[Node], controller: Node, cluster_id: str,
             default_partitions: int, default_replication_factor: int,
             broker_log_dirs: Sequence[Sequence[str]], using_raft_controller: bool,
             feature_levels: Mapping[str, int], min_supported_feature_levels: Mapping[str, int],
             max_supported_feature_levels: Mapping[str, int],
             default_group_configs: Mapping[str, str]) -> AsyncMockAdminClient:
        """Java's private eleven-argument constructor (``Builder.build()``)."""
        client = cls.__new__(cls)
        AsyncAdmin.__init__(client)
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

    async def close(self, *, timeout: Duration | None = None) -> None:
        """Does nothing, as Java's mock (no check of ``timeout`` either)."""
        self._close()

    async def __aenter__(self) -> AsyncMockAdminClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions | None = None) -> CreateTopicsResult[Async]:
        """See :meth:`MockAdminClient.create_topics`."""
        return self._create_topics(new_topics)

    @overload
    def delete_topics(self, *, topics: Iterable[str],
                            options: DeleteTopicsOptions | None = None
                            ) -> DeleteTopicsResult[Async]: ...
    @overload
    def delete_topics(self, *, topics: TopicCollection,
                            options: DeleteTopicsOptions | None = None
                            ) -> DeleteTopicsResult[Async]: ...

    def delete_topics(self, *, topics: Iterable[str] | TopicCollection,
                            options: DeleteTopicsOptions | None = None
                            ) -> DeleteTopicsResult[Async]:
        """See :meth:`MockAdminClient.delete_topics`."""
        return self._delete_topics(topics)

    def list_topics(self, *, options: ListTopicsOptions | None = None
                    ) -> ListTopicsResult[Async]:
        """See :meth:`MockAdminClient.list_topics`."""
        return self._list_topics()

    @overload
    def describe_topics(self, *, topic_names: Iterable[str],
                              options: DescribeTopicsOptions | None = None
                              ) -> DescribeTopicsResult[Async]: ...
    @overload
    def describe_topics(self, *, topics: TopicCollection,
                              options: DescribeTopicsOptions | None = None
                              ) -> DescribeTopicsResult[Async]: ...

    @java_forms(*_DESCRIBE_TOPICS_FORMS)
    def describe_topics(self, *, topic_names: Iterable[str] | None = None,
                              topics: TopicCollection | None = None,
                              options: DescribeTopicsOptions | None = None
                              ) -> DescribeTopicsResult[Async]:
        """See :meth:`MockAdminClient.describe_topics`."""
        return self._describe_topics(topic_names, topics)

    def describe_cluster(self, *, options: DescribeClusterOptions | None = None
                               ) -> DescribeClusterResult[Async]:
        """See :meth:`MockAdminClient.describe_cluster`."""
        return self._describe_cluster()

    async def describe_acls(self, *, filter: AclBindingFilter,  # noqa: A002 - Java's name
                            options: DescribeAclsOptions | None = None
                            ) -> DescribeAclsResult[Async]:
        """See :meth:`MockAdminClient.describe_acls`."""
        return self._describe_acls(filter)

    async def create_acls(self, *, acls: Iterable[AclBinding],
                          options: CreateAclsOptions | None = None) -> CreateAclsResult[Async]:
        """See :meth:`MockAdminClient.create_acls`."""
        return self._create_acls(acls)

    async def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                          options: DeleteAclsOptions | None = None) -> DeleteAclsResult[Async]:
        """See :meth:`MockAdminClient.delete_acls`."""
        return self._delete_acls(filters)

    def describe_configs(self, *, resources: Iterable[ConfigResource],
                               options: DescribeConfigsOptions | None = None
                               ) -> DescribeConfigsResult[Async]:
        """See :meth:`MockAdminClient.describe_configs`."""
        return self._describe_configs(resources)

    def incremental_alter_configs(
            self, *, configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
            options: AlterConfigsOptions | None = None) -> AlterConfigsResult[Async]:
        """See :meth:`MockAdminClient.incremental_alter_configs`."""
        return self._incremental_alter_configs(configs)

    def alter_replica_log_dirs(
        self, *, replica_assignment: Mapping[TopicPartitionReplica, str],
        options: AlterReplicaLogDirsOptions | None = None
    ) -> AlterReplicaLogDirsResult[Async]:
        """See :meth:`MockAdminClient.alter_replica_log_dirs`."""
        return self._alter_replica_log_dirs(replica_assignment)

    def describe_log_dirs(self, *, brokers: Iterable[int],
                                options: DescribeLogDirsOptions | None = None
                                ) -> DescribeLogDirsResult[Async]:
        """See :meth:`MockAdminClient.describe_log_dirs`."""
        return self._describe_log_dirs(brokers)

    def describe_replica_log_dirs(self, *, replicas: Iterable[TopicPartitionReplica],
                                        options: DescribeReplicaLogDirsOptions | None = None
                                        ) -> DescribeReplicaLogDirsResult[Async]:
        """See :meth:`MockAdminClient.describe_replica_log_dirs`."""
        return self._describe_replica_log_dirs(replicas)

    def create_partitions(self, *, new_partitions: Mapping[str, NewPartitions],
                                options: CreatePartitionsOptions | None = None
                                ) -> CreatePartitionsResult[Async]:
        """See :meth:`MockAdminClient.create_partitions`."""
        return self._create_partitions(new_partitions)

    def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                             options: DeleteRecordsOptions | None = None
                             ) -> DeleteRecordsResult[Async]:
        """See :meth:`MockAdminClient.delete_records`."""
        return self._delete_records(records_to_delete)

    async def create_delegation_token(self, *, options: CreateDelegationTokenOptions | None = None
                                      ) -> CreateDelegationTokenResult[Async]:
        """See :meth:`MockAdminClient.create_delegation_token`."""
        return self._create_delegation_token(
            options if options is not None else CreateDelegationTokenOptions())

    async def renew_delegation_token(self, *, hmac: bytes,
                                     options: RenewDelegationTokenOptions | None = None
                                     ) -> RenewDelegationTokenResult[Async]:
        """See :meth:`MockAdminClient.renew_delegation_token`."""
        return self._renew_delegation_token(
            hmac, options if options is not None else RenewDelegationTokenOptions())

    async def expire_delegation_token(self, *, hmac: bytes,
                                      options: ExpireDelegationTokenOptions | None = None
                                      ) -> ExpireDelegationTokenResult[Async]:
        """See :meth:`MockAdminClient.expire_delegation_token`."""
        return self._expire_delegation_token(
            hmac, options if options is not None else ExpireDelegationTokenOptions())

    async def describe_delegation_token(self, *,
                                        options: DescribeDelegationTokenOptions | None = None
                                        ) -> DescribeDelegationTokenResult[Async]:
        """See :meth:`MockAdminClient.describe_delegation_token`."""
        return self._describe_delegation_token(
            options if options is not None else DescribeDelegationTokenOptions())

    async def describe_consumer_groups(self, *, group_ids: Iterable[str],
                                       options: DescribeConsumerGroupsOptions | None = None
                                       ) -> DescribeConsumerGroupsResult[Async]:
        """See :meth:`MockAdminClient.describe_consumer_groups`."""
        return self._describe_consumer_groups(group_ids)

    @overload
    async def list_consumer_group_offsets(self, *, group_id: str,
                                          options: ListConsumerGroupOffsetsOptions | None = None
                                          ) -> ListConsumerGroupOffsetsResult[Async]: ...
    @overload
    async def list_consumer_group_offsets(
        self, *, group_specs: Mapping[str, ListConsumerGroupOffsetsSpec],
        options: ListConsumerGroupOffsetsOptions | None = None
    ) -> ListConsumerGroupOffsetsResult[Async]: ...

    @java_forms(*_LIST_CONSUMER_GROUP_OFFSETS_FORMS)
    async def list_consumer_group_offsets(
        self, *, group_id: str | None = None,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec] | None = None,
        options: ListConsumerGroupOffsetsOptions | None = None
    ) -> ListConsumerGroupOffsetsResult[Async]:
        """See :meth:`MockAdminClient.list_consumer_group_offsets`."""
        if group_specs is None:
            assert group_id is not None  # java_forms: one or the other
            group_specs = {group_id: ListConsumerGroupOffsetsSpec()}
        return self._list_consumer_group_offsets(group_specs)

    async def delete_consumer_groups(self, *, group_ids: Iterable[str],
                                     options: DeleteConsumerGroupsOptions | None = None
                                     ) -> DeleteConsumerGroupsResult[Async]:
        """See :meth:`MockAdminClient.delete_consumer_groups`."""
        return self._delete_consumer_groups(group_ids)

    async def delete_consumer_group_offsets(self, *, group_id: str,
                                            partitions: Iterable[TopicPartition],
                                            options: DeleteConsumerGroupOffsetsOptions | None = None
                                            ) -> DeleteConsumerGroupOffsetsResult[Async]:
        """See :meth:`MockAdminClient.delete_consumer_group_offsets`."""
        return self._delete_consumer_group_offsets(group_id, partitions)

    async def list_groups(
        self, *, options: ListGroupsOptions | None = None
    ) -> ListGroupsResult[Async]:
        """See :meth:`MockAdminClient.list_groups`."""
        return self._list_groups()

    def elect_leaders(self, *, election_type: ElectionType,
                            partitions: Iterable[TopicPartition] | None,
                            options: ElectLeadersOptions | None = None
                            ) -> ElectLeadersResult[Async]:
        """See :meth:`MockAdminClient.elect_leaders`."""
        return self._elect_leaders(election_type, partitions)

    def alter_partition_reassignments(
            self, *, reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
            options: AlterPartitionReassignmentsOptions | None = None
            ) -> AlterPartitionReassignmentsResult[Async]:
        """See :meth:`MockAdminClient.alter_partition_reassignments`."""
        return self._alter_partition_reassignments(reassignments)

    def list_partition_reassignments(
            self, *, partitions: Iterable[TopicPartition] | None = None,
            options: ListPartitionReassignmentsOptions | None = None
            ) -> ListPartitionReassignmentsResult[Async]:
        """See :meth:`MockAdminClient.list_partition_reassignments`."""
        return self._list_partition_reassignments(partitions)

    async def remove_members_from_consumer_group(
        self, *, group_id: str, options: RemoveMembersFromConsumerGroupOptions
    ) -> RemoveMembersFromConsumerGroupResult[Async]:
        """See :meth:`MockAdminClient.remove_members_from_consumer_group`."""
        return self._remove_members_from_consumer_group(group_id, options)

    async def alter_consumer_group_offsets(self, *, group_id: str,
                                           offsets: Mapping[TopicPartition, OffsetAndMetadata],
                                           options: AlterConsumerGroupOffsetsOptions | None = None
                                           ) -> AlterConsumerGroupOffsetsResult[Async]:
        """See :meth:`MockAdminClient.alter_consumer_group_offsets`."""
        return self._alter_consumer_group_offsets(group_id, offsets)

    def list_offsets(self, *, topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
                           options: ListOffsetsOptions | None = None) -> ListOffsetsResult[Async]:
        """See :meth:`MockAdminClient.list_offsets`."""
        return self._list_offsets(topic_partition_offsets)

    async def describe_client_quotas(self, *, filter: ClientQuotaFilter,  # noqa: A002 - Java's name
                                     options: DescribeClientQuotasOptions | None = None
                                     ) -> DescribeClientQuotasResult[Async]:
        """See :meth:`MockAdminClient.describe_client_quotas`."""
        return self._describe_client_quotas(filter)

    async def alter_client_quotas(self, *, entries: Iterable[ClientQuotaAlteration],
                                  options: AlterClientQuotasOptions | None = None
                                  ) -> AlterClientQuotasResult[Async]:
        """See :meth:`MockAdminClient.alter_client_quotas`."""
        return self._alter_client_quotas(entries)

    @overload
    async def describe_user_scram_credentials(self, *, users: Sequence[str] | None = None
                                              ) -> DescribeUserScramCredentialsResult[Async]: ...
    @overload
    async def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None,
            options: DescribeUserScramCredentialsOptions
    ) -> DescribeUserScramCredentialsResult[Async]: ...

    @java_forms(*_DESCRIBE_USER_SCRAM_CREDENTIALS_FORMS)
    async def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None = UNSET,
            options: DescribeUserScramCredentialsOptions | None = None
    ) -> DescribeUserScramCredentialsResult[Async]:
        """See :meth:`MockAdminClient.describe_user_scram_credentials`."""
        return self._describe_user_scram_credentials(None if users is UNSET else users)

    async def alter_user_scram_credentials(
            self, *, alterations: Sequence[UserScramCredentialAlteration],
            options: AlterUserScramCredentialsOptions | None = None
    ) -> AlterUserScramCredentialsResult[Async]:
        """See :meth:`MockAdminClient.alter_user_scram_credentials`."""
        return self._alter_user_scram_credentials(alterations)

    async def describe_features(self, *, options: DescribeFeaturesOptions | None = None
                                ) -> DescribeFeaturesResult[Async]:
        """See :meth:`MockAdminClient.describe_features`."""
        return self._describe_features()

    async def update_features(self, *, feature_updates: Mapping[str, FeatureUpdate],
                              options: UpdateFeaturesOptions | None = None
                              ) -> UpdateFeaturesResult[Async]:
        """See :meth:`MockAdminClient.update_features`."""
        if options is None:
            options = UpdateFeaturesOptions()
        return self._update_features(feature_updates, options.validate_only())

    def unregister_controller(self, *, controller_id: int,
                              options: UnregisterControllerOptions | None = None
                              ) -> UnregisterControllerResult[Async]:
        """A completed future when the mock uses a raft controller, else one failed
        with ``UnsupportedVersionError("")``, as Java's mock."""
        return self._unregister_controller(controller_id)

    async def describe_producers(self, *, partitions: Iterable[TopicPartition],
                                 options: DescribeProducersOptions | None = None
                                 ) -> DescribeProducersResult[Async]:
        """See :meth:`MockAdminClient.describe_producers`."""
        return self._describe_producers(partitions)

    async def describe_transactions(self, *, transactional_ids: Iterable[str],
                                    options: DescribeTransactionsOptions | None = None
                                    ) -> DescribeTransactionsResult[Async]:
        """See :meth:`MockAdminClient.describe_transactions`."""
        return self._describe_transactions(transactional_ids)

    async def abort_transaction(self, *, spec: AbortTransactionSpec,
                                options: AbortTransactionOptions | None = None
                                ) -> AbortTransactionResult[Async]:
        """See :meth:`MockAdminClient.abort_transaction`."""
        return self._abort_transaction(spec)

    async def list_transactions(self, *, options: ListTransactionsOptions | None = None
                                ) -> ListTransactionsResult[Async]:
        """See :meth:`MockAdminClient.list_transactions`."""
        return self._list_transactions()

    async def fence_producers(self, *, transactional_ids: Iterable[str],
                              options: FenceProducersOptions | None = None
                              ) -> FenceProducersResult[Async]:
        """See :meth:`MockAdminClient.fence_producers`."""
        return self._fence_producers(transactional_ids)

    @overload
    def list_config_resources(self, *, config_resource_types: Iterable[ConfigResource.Type],
                                    options: ListConfigResourcesOptions
                                    ) -> ListConfigResourcesResult[Async]: ...
    @overload
    def list_config_resources(self) -> ListConfigResourcesResult[Async]: ...

    @java_forms(*_LIST_CONFIG_RESOURCES_FORMS)
    def list_config_resources(self, *,
                                    config_resource_types: Iterable[ConfigResource.Type] = UNSET,
                                    options: ListConfigResourcesOptions | None = None
                                    ) -> ListConfigResourcesResult[Async]:
        """See :meth:`MockAdminClient.list_config_resources`."""
        return self._list_config_resources(config_resource_types)

    async def describe_classic_groups(self, *, group_ids: Iterable[str],
                                      options: DescribeClassicGroupsOptions | None = None
                                      ) -> DescribeClassicGroupsResult[Async]:
        """See :meth:`MockAdminClient.describe_classic_groups`."""
        return self._describe_classic_groups(group_ids)

    async def force_terminate_transaction(self, *, transactional_id: str,
                                          options: TerminateTransactionOptions | None = None
                                          ) -> TerminateTransactionResult[Async]:
        """See :meth:`MockAdminClient.force_terminate_transaction`."""
        return self._force_terminate_transaction(transactional_id)

    # ---- The Java mock's own methods with a future in their type -----------------
    # (the others are inherited from _mock_core, typed the same on both classes)

    def handle_describe_topics_using_ids(
            self, *, topic_ids: Iterable[Uuid]) -> dict[Uuid, AsyncKafkaFuture[TopicDescription]]:
        """The futures ``describe_topics`` gives for a ``TopicIdCollection``."""
        return self._handle_describe_topics_using_ids(topic_ids)  # type: ignore[return-value]
