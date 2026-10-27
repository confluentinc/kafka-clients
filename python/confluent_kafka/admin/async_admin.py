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

"""``AsyncAdmin``: the asyncio peer of :class:`~confluent_kafka.admin.Admin`.

By rule 3, ``Admin``'s methods in the same order (CLAUDE.md, Python Binding
Conventions, Class family); a client class in a signature is its async peer, so
``AsyncAdmin.create`` returns an ``AsyncAdmin``, built as an
``AsyncKafkaAdminClient``. The RPCs are plain ``def``\\ s that return their
``*Result`` at once, as ``Admin``'s; the result's futures are
:class:`~confluent_kafka.common.AsyncKafkaFuture`\\ s, completed on the client's
event loop, where the notify hook schedules the client's queued callbacks
(Implementation over the FFI). ``close`` is the one ``async def``: its entry
point has a ``_cb`` twin. ``create`` is a plain ``def``:
``kafka_admin_AdminClient_create`` has none. The overloads left out are
``Admin``'s (see its module), and so are the null checks: an ``options`` left
out is Java's ``new XxxOptions()``, an explicit ``None`` raises ``TypeError``.
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import MISSING, Form, java_forms, reject_str
from confluent_kafka._async import (
    LoopBinding,
    OffLoopPump,
    await_to_end,
    call_soon_on,
)
from confluent_kafka.admin import _conversions
from confluent_kafka.admin._futures import Async
from confluent_kafka.admin.abort_transaction_options import AbortTransactionOptions
from confluent_kafka.admin.admin import _AdminState, close_timeout_ms
from confluent_kafka.admin.alter_client_quotas_options import AlterClientQuotasOptions
from confluent_kafka.admin.alter_configs_options import AlterConfigsOptions
from confluent_kafka.admin.alter_consumer_group_offsets_options import (
    AlterConsumerGroupOffsetsOptions,
)
from confluent_kafka.admin.alter_partition_reassignments_options import (
    AlterPartitionReassignmentsOptions,
)
from confluent_kafka.admin.alter_replica_log_dirs_options import AlterReplicaLogDirsOptions
from confluent_kafka.admin.alter_user_scram_credentials_options import (
    AlterUserScramCredentialsOptions,
)
from confluent_kafka.admin.create_acls_options import CreateAclsOptions
from confluent_kafka.admin.create_delegation_token_options import CreateDelegationTokenOptions
from confluent_kafka.admin.create_partitions_options import CreatePartitionsOptions
from confluent_kafka.admin.create_topics_options import CreateTopicsOptions
from confluent_kafka.admin.delete_acls_options import DeleteAclsOptions
from confluent_kafka.admin.delete_consumer_group_offsets_options import (
    DeleteConsumerGroupOffsetsOptions,
)
from confluent_kafka.admin.delete_consumer_groups_options import DeleteConsumerGroupsOptions
from confluent_kafka.admin.delete_records_options import DeleteRecordsOptions
from confluent_kafka.admin.delete_topics_options import DeleteTopicsOptions
from confluent_kafka.admin.describe_acls_options import DescribeAclsOptions
from confluent_kafka.admin.describe_classic_groups_options import DescribeClassicGroupsOptions
from confluent_kafka.admin.describe_client_quotas_options import DescribeClientQuotasOptions
from confluent_kafka.admin.describe_cluster_options import DescribeClusterOptions
from confluent_kafka.admin.describe_configs_options import DescribeConfigsOptions
from confluent_kafka.admin.describe_consumer_groups_options import DescribeConsumerGroupsOptions
from confluent_kafka.admin.describe_delegation_token_options import DescribeDelegationTokenOptions
from confluent_kafka.admin.describe_features_options import DescribeFeaturesOptions
from confluent_kafka.admin.describe_log_dirs_options import DescribeLogDirsOptions
from confluent_kafka.admin.describe_producers_options import DescribeProducersOptions
from confluent_kafka.admin.describe_replica_log_dirs_options import DescribeReplicaLogDirsOptions
from confluent_kafka.admin.describe_topics_options import DescribeTopicsOptions
from confluent_kafka.admin.describe_transactions_options import DescribeTransactionsOptions
from confluent_kafka.admin.describe_user_scram_credentials_options import (
    DescribeUserScramCredentialsOptions,
)
from confluent_kafka.admin.elect_leaders_options import ElectLeadersOptions
from confluent_kafka.admin.expire_delegation_token_options import ExpireDelegationTokenOptions
from confluent_kafka.admin.fence_producers_options import FenceProducersOptions
from confluent_kafka.admin.list_config_resources_options import ListConfigResourcesOptions
from confluent_kafka.admin.list_consumer_group_offsets_options import (
    ListConsumerGroupOffsetsOptions,
)
from confluent_kafka.admin.list_consumer_group_offsets_spec import ListConsumerGroupOffsetsSpec
from confluent_kafka.admin.list_groups_options import ListGroupsOptions
from confluent_kafka.admin.list_offsets_options import ListOffsetsOptions
from confluent_kafka.admin.list_partition_reassignments_options import (
    ListPartitionReassignmentsOptions,
)
from confluent_kafka.admin.list_topics_options import ListTopicsOptions
from confluent_kafka.admin.list_transactions_options import ListTransactionsOptions
from confluent_kafka.admin.renew_delegation_token_options import RenewDelegationTokenOptions
from confluent_kafka.admin.terminate_transaction_options import TerminateTransactionOptions
from confluent_kafka.admin.unregister_controller_options import UnregisterControllerOptions
from confluent_kafka.admin.update_features_options import UpdateFeaturesOptions
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka import Duration
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
    from confluent_kafka.admin.create_acls_result import CreateAclsResult
    from confluent_kafka.admin.create_delegation_token_result import CreateDelegationTokenResult
    from confluent_kafka.admin.create_partitions_result import CreatePartitionsResult
    from confluent_kafka.admin.create_topics_result import CreateTopicsResult
    from confluent_kafka.admin.delete_acls_result import DeleteAclsResult
    from confluent_kafka.admin.delete_consumer_group_offsets_result import (
        DeleteConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.delete_consumer_groups_result import DeleteConsumerGroupsResult
    from confluent_kafka.admin.delete_records_result import DeleteRecordsResult
    from confluent_kafka.admin.delete_topics_result import DeleteTopicsResult
    from confluent_kafka.admin.describe_acls_result import DescribeAclsResult
    from confluent_kafka.admin.describe_classic_groups_result import DescribeClassicGroupsResult
    from confluent_kafka.admin.describe_client_quotas_result import DescribeClientQuotasResult
    from confluent_kafka.admin.describe_cluster_result import DescribeClusterResult
    from confluent_kafka.admin.describe_configs_result import DescribeConfigsResult
    from confluent_kafka.admin.describe_consumer_groups_result import DescribeConsumerGroupsResult
    from confluent_kafka.admin.describe_delegation_token_result import (
        DescribeDelegationTokenResult,
    )
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
    from confluent_kafka.admin.feature_update import FeatureUpdate
    from confluent_kafka.admin.fence_producers_result import FenceProducersResult
    from confluent_kafka.admin.list_config_resources_result import ListConfigResourcesResult
    from confluent_kafka.admin.list_consumer_group_offsets_result import (
        ListConsumerGroupOffsetsResult,
    )
    from confluent_kafka.admin.list_groups_result import ListGroupsResult
    from confluent_kafka.admin.list_offsets_result import ListOffsetsResult
    from confluent_kafka.admin.list_partition_reassignments_result import (
        ListPartitionReassignmentsResult,
    )
    from confluent_kafka.admin.list_topics_result import ListTopicsResult
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
    from confluent_kafka.admin.terminate_transaction_result import TerminateTransactionResult
    from confluent_kafka.admin.unregister_controller_result import UnregisterControllerResult
    from confluent_kafka.admin.update_features_result import UpdateFeaturesResult
    from confluent_kafka.admin.user_scram_credential_alteration import (
        UserScramCredentialAlteration,
    )
    from confluent_kafka.common.acl import AclBinding, AclBindingFilter
    from confluent_kafka.common.quota import ClientQuotaAlteration, ClientQuotaFilter
    from confluent_kafka.common import TopicPartition
    from confluent_kafka.common.config.config_resource import ConfigResource
    from confluent_kafka.common.election_type import ElectionType
    from confluent_kafka.common.topic_collection import TopicCollection
    from confluent_kafka.common.topic_partition import TopicPartition
    from confluent_kafka.common.topic_partition_replica import TopicPartitionReplica
    from confluent_kafka.consumer import OffsetAndMetadata

__all__ = ["AsyncAdmin"]


def _set_if_pending(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


class AsyncAdmin(_AdminState):
    """The asyncio peer of ``Admin``, the administrative client for Kafka: see
    ``Admin``. Instances returned from ``create`` are safe to share between the
    tasks of one event loop.

    Its RPCs are plain ``def``\\ s that return their ``*Result`` at once, as
    ``Admin``'s, its futures :class:`~confluent_kafka.common.AsyncKafkaFuture`\\ s
    (Futures). The client is bound to one event loop (``_async.LoopBinding``): the
    one running when it is constructed, or else the loop of its first call made on
    a running loop (``close()`` alone may run on another loop once that loop is
    closed, ``LoopBinding.bind_for_close``); the notify hook schedules the client's queued callbacks
    (``kafka_admin_Admin__execute_callbacks``) there with ``call_soon_threadsafe``,
    so the futures complete, and their ``then_apply`` functions and
    ``when_complete`` actions run, on that loop. Until a loop is bound, or once it
    has closed, a helper thread runs them instead (``_async.OffLoopPump``).

    Java: ``org.apache.kafka.clients.admin.Admin`` (``AutoCloseable``: an async
    context manager whose exit calls ``close()``).
    """

    def __init__(self) -> None:
        if type(self) is AsyncAdmin:
            raise TypeError(
                "AsyncAdmin is a non-instantiable base; use AsyncAdmin.create or "
                "AsyncMockAdminClient")
        self._init_async_state()

    def _init_async_state(self) -> None:
        """The shared state, the loop binding and the off-loop pump."""
        _AdminState.__init__(self)
        self._binding = LoopBinding(type(self).__name__)
        self._off_loop_pump = OffLoopPump(self._run_pump, "confluent-kafka-admin-pump")
        # One drainer at a time: the loop and the off-loop pump never drain at once.
        self._drain_lock = threading.Lock()

    # ---- callback pump -----------------------------------------------------------

    def _notify_hook(self) -> Callable[[], None]:
        # A weak reference: the native client keeps the hook until it is
        # destroyed, and must not keep an unclosed client alive.
        ref = weakref.ref(self)

        def notify() -> None:
            client = ref()
            if client is not None:
                client._schedule_pump()

        return notify

    def _schedule_pump(self) -> None:
        """The notify hook (any thread, the GIL held): schedule :meth:`_run_pump`
        on the client's loop, or run it on the helper thread when no loop is bound
        or it has closed. It only schedules, never runs a callback (CLAUDE.md §4)."""
        loop = self._binding.loop
        if loop is not None and not loop.is_closed():
            try:
                call_soon_on(loop, self._run_pump)
                return
            except RuntimeError:  # the loop closed since the check
                pass
        self._off_loop_pump.start()

    def _run_pump(self) -> None:
        """Run the client's queued callbacks on this thread, until the teardown
        begins (it runs what is left itself)."""
        if not self._begin_pump():
            return
        try:
            with self._drain_lock:
                _lib.Admin__execute_callbacks(self._c_admin)
        finally:
            self._end_use()

    def _rpc_loop(self) -> asyncio.AbstractEventLoop | None:
        """The loop an RPC's futures complete on: the client's; an RPC called on a
        running loop binds it (``RuntimeError`` when the client is bound to
        another loop)."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return self._binding.loop
        return self._binding.bind()

    def _call(self, entry: Callable[..., Any], *args: Any) -> Any:
        self._rpc_loop()
        return super()._call(entry, *args)

    # ---- the Admin methods -----------------------------------------------------------

    @staticmethod
    def create(*, configs: dict[str, Any]) -> AsyncAdmin:
        """Create a new AsyncAdmin with the given configuration: see
        :meth:`Admin.create`. Returns the new ``AsyncKafkaAdminClient``; it does not
        wait, so it may run outside an event loop."""
        from confluent_kafka.admin.async_kafka_admin_client import AsyncKafkaAdminClient

        return AsyncKafkaAdminClient._create_internal(configs=configs)

    @overload
    async def close(self) -> None: ...
    @overload
    async def close(self, *, timeout: Duration) -> None: ...

    async def close(self, *, timeout: Duration = MISSING) -> None:
        """See :meth:`Admin.close`. Cancelling the awaiting task raises
        ``CancelledError`` at once; the close goes on in the core and the handle is
        freed when it ends."""
        if self._closed:
            return
        timeout_ms = close_timeout_ms(timeout)
        loop = self._binding.bind_for_close()
        if not self._begin_close():
            return
        closed = threading.Event()
        done: asyncio.Future[None] = loop.create_future()

        def on_closed() -> None:
            # Runs where the client's callbacks run: the loop, or the helper thread.
            closed.set()
            try:
                call_soon_on(loop, _set_if_pending, done)
            except RuntimeError:  # the loop closed
                pass

        _lib.Admin_close_cb(self._c_admin, timeout_ms, on_closed)
        try:
            await done
        except BaseException:
            threading.Thread(target=self._finish_close, args=(closed,),
                             name="kafka-admin-client-close", daemon=True).start()
            raise
        # Off the loop: waits for the uses still in flight, then frees the handle;
        # a cancellation meanwhile lets it run to its end.
        await await_to_end(loop.run_in_executor(None, self._teardown))

    def _finish_close(self, closed: threading.Event) -> None:
        closed.wait()
        self._teardown()

    async def __aenter__(self) -> AsyncAdmin:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @overload
    def create_topics(self, *, new_topics: Iterable[NewTopic]) -> CreateTopicsResult[Async]: ...
    @overload
    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions) -> CreateTopicsResult[Async]: ...

    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions = MISSING) -> CreateTopicsResult[Async]:
        """See :meth:`Admin.create_topics`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(new_topics, "new_topics")
        args = _conversions.create_topics_args(new_topics)
        reads = _conversions.any_topic_name(row[0] for row in args[0])
        options = _conversions.options_or_new(options, CreateTopicsOptions, read=reads)
        native = self._call(_lib.Admin_create_topics, *args, options._setters_ran())
        return _conversions.create_topics_result(native, Async, self._binding.loop)

    @overload
    def delete_topics(self, *, topics: Iterable[str]) -> DeleteTopicsResult[Async]: ...
    @overload
    def delete_topics(self, *, topics: Iterable[str],
                      options: DeleteTopicsOptions) -> DeleteTopicsResult[Async]: ...
    @overload
    def delete_topics(self, *, topics: TopicCollection) -> DeleteTopicsResult[Async]: ...
    @overload
    def delete_topics(self, *, topics: TopicCollection,
                      options: DeleteTopicsOptions) -> DeleteTopicsResult[Async]: ...

    def delete_topics(self, *, topics: Iterable[str] | TopicCollection,
                            options: DeleteTopicsOptions = MISSING
                            ) -> DeleteTopicsResult[Async]:
        """See :meth:`Admin.delete_topics`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        if topics is None:  # the shape checks precede the closed check
            raise _conversions.ambiguous_none("delete_topics", "topics",
                                              ("Iterable[str]", "TopicCollection"))
        reject_str("topics", topics)
        self._check_not_closed()
        by_ids, keys = _conversions.topic_collection_target(topics, "deleteTopics")
        args = _conversions.delete_topics_args(topics, by_ids, keys)
        reads = (_conversions.any_topic_id(keys) if by_ids
                 else _conversions.any_topic_name(keys))
        options = _conversions.options_or_new(options, DeleteTopicsOptions, read=reads)
        native = self._call(_lib.Admin_delete_topics, *args, options._setters_ran())
        return _conversions.delete_topics_result(native, by_ids, Async, self._binding.loop)

    @overload
    def list_topics(self) -> ListTopicsResult[Async]: ...
    @overload
    def list_topics(self, *, options: ListTopicsOptions) -> ListTopicsResult[Async]: ...

    def list_topics(self, *, options: ListTopicsOptions = MISSING
                    ) -> ListTopicsResult[Async]:
        """See :meth:`Admin.list_topics`."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, ListTopicsOptions)
        native = self._call(_lib.Admin_list_topics, options._setters_ran())
        return _conversions.list_topics_result(native, Async, self._binding.loop)

    @overload
    def describe_topics(self, *, topic_names: Iterable[str]) -> DescribeTopicsResult[Async]: ...
    @overload
    def describe_topics(self, *, topic_names: Iterable[str],
                        options: DescribeTopicsOptions) -> DescribeTopicsResult[Async]: ...
    @overload
    def describe_topics(self, *, topics: TopicCollection) -> DescribeTopicsResult[Async]: ...
    @overload
    def describe_topics(self, *, topics: TopicCollection,
                        options: DescribeTopicsOptions) -> DescribeTopicsResult[Async]: ...

    @java_forms(Form("topic_names"), Form("topic_names", "options"),
                Form("topics"), Form("topics", "options"))
    def describe_topics(self, *, topic_names: Iterable[str] = MISSING,
                              topics: TopicCollection = MISSING,
                              options: DescribeTopicsOptions = MISSING
                              ) -> DescribeTopicsResult[Async]:
        """See :meth:`Admin.describe_topics`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        if topics is MISSING:
            reject_str("topic_names", topic_names)  # the shape check precedes the closed check
        self._check_not_closed()
        if topics is MISSING:
            _conversions.non_null(topic_names, "topic_names")  # TopicCollection.ofTopicNames(null)
        if topics is MISSING:
            assert topic_names is not None  # the non_null above
            form, keys = (_conversions.DESCRIBE_TOPIC_NAMES,
                          _conversions.describe_topic_names(topic_names))
        else:
            by_ids, keys = _conversions.topic_collection_target(topics, "describeTopics")
            form = (_conversions.DESCRIBE_TOPIC_ID_COLLECTION if by_ids
                    else _conversions.DESCRIBE_TOPIC_NAME_COLLECTION)
        args = _conversions.describe_topics_args(form, keys)
        reads = (form == _conversions.DESCRIBE_TOPIC_ID_COLLECTION
                 or _conversions.any_topic_name(keys))
        options = _conversions.options_or_new(options, DescribeTopicsOptions, read=reads)
        native = self._call(_lib.Admin_describe_topics, *args, options._setters_ran())
        return _conversions.describe_topics_result(
            native, form == _conversions.DESCRIBE_TOPIC_ID_COLLECTION, Async, self._binding.loop)

    @overload
    def describe_cluster(self) -> DescribeClusterResult[Async]: ...
    @overload
    def describe_cluster(self, *,
                         options: DescribeClusterOptions) -> DescribeClusterResult[Async]: ...

    def describe_cluster(self, *, options: DescribeClusterOptions = MISSING
                               ) -> DescribeClusterResult[Async]:
        """See :meth:`Admin.describe_cluster`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeClusterOptions)
        native = self._call(_lib.Admin_describe_cluster, options._setters_ran())
        return _conversions.describe_cluster_result(native, Async, self._binding.loop)

    @overload
    def describe_acls(self, *, filter: AclBindingFilter) -> DescribeAclsResult[Async]: ...
    @overload
    def describe_acls(self, *, filter: AclBindingFilter,
                      options: DescribeAclsOptions) -> DescribeAclsResult[Async]: ...

    def describe_acls(self, *, filter: AclBindingFilter,  # noqa: A002 - Java's name
                      options: DescribeAclsOptions = MISSING) -> DescribeAclsResult[Async]:
        """See :meth:`Admin.describe_acls`; the result's future is an
        ``AsyncKafkaFuture``."""
        self._check_not_closed()
        _conversions.non_null(filter, "filter")
        args = _conversions.describe_acls_args(filter)
        reads = not filter.is_unknown()
        options = _conversions.options_or_new(options, DescribeAclsOptions, read=reads)
        native = self._call(_lib.Admin_describe_acls, *args, options._setters_ran())
        return _conversions.describe_acls_result(native, Async, self._binding.loop)

    @overload
    def create_acls(self, *, acls: Iterable[AclBinding]) -> CreateAclsResult[Async]: ...
    @overload
    def create_acls(self, *, acls: Iterable[AclBinding],
                    options: CreateAclsOptions) -> CreateAclsResult[Async]: ...

    def create_acls(self, *, acls: Iterable[AclBinding],
                    options: CreateAclsOptions = MISSING) -> CreateAclsResult[Async]:
        """See :meth:`Admin.create_acls`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(acls, "acls")
        args = _conversions.create_acls_args(acls)
        options = _conversions.options_or_new(options, CreateAclsOptions)
        native = self._call(_lib.Admin_create_acls, *args, options._setters_ran())
        return _conversions.create_acls_result(native, Async, self._binding.loop)

    @overload
    def delete_acls(self, *, filters: Iterable[AclBindingFilter]) -> DeleteAclsResult[Async]: ...
    @overload
    def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                    options: DeleteAclsOptions) -> DeleteAclsResult[Async]: ...

    def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                    options: DeleteAclsOptions = MISSING) -> DeleteAclsResult[Async]:
        """See :meth:`Admin.delete_acls`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(filters, "filters")
        args = _conversions.delete_acls_args(filters)
        options = _conversions.options_or_new(options, DeleteAclsOptions)
        native = self._call(_lib.Admin_delete_acls, *args, options._setters_ran())
        return _conversions.delete_acls_result(native, Async, self._binding.loop)

    @overload
    def describe_configs(self, *,
                         resources: Iterable[ConfigResource]) -> DescribeConfigsResult[Async]: ...
    @overload
    def describe_configs(self, *, resources: Iterable[ConfigResource],
                         options: DescribeConfigsOptions) -> DescribeConfigsResult[Async]: ...

    def describe_configs(self, *, resources: Iterable[ConfigResource],
                               options: DescribeConfigsOptions = MISSING
                               ) -> DescribeConfigsResult[Async]:
        """See :meth:`Admin.describe_configs`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(resources, "resources")
        args = _conversions.describe_configs_args(list(resources))
        options = _conversions.options_or_new(options, DescribeConfigsOptions, read=bool(args[0]))
        native = self._call(_lib.Admin_describe_configs, *args, options._setters_ran())
        return _conversions.describe_configs_result(native, Async, self._binding.loop)

    @overload
    def incremental_alter_configs(
        self, *,
        configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
    ) -> AlterConfigsResult[Async]: ...
    @overload
    def incremental_alter_configs(self, *,
                                  configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
                                  options: AlterConfigsOptions) -> AlterConfigsResult[Async]: ...

    def incremental_alter_configs(
            self, *, configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
            options: AlterConfigsOptions = MISSING) -> AlterConfigsResult[Async]:
        """See :meth:`Admin.incremental_alter_configs`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(configs, "configs")
        _conversions.check_keys(configs, "configs")  # nodeFor(resource), before options
        # Java reads options per node (3006), the ops only in createRequest.
        options = _conversions.options_or_new(options, AlterConfigsOptions, read=bool(configs))
        args = _conversions.incremental_alter_configs_args(configs)
        native = self._call(_lib.Admin_incremental_alter_configs, *args, options._setters_ran())
        return _conversions.incremental_alter_configs_result(native, Async, self._binding.loop)

    @overload
    def alter_replica_log_dirs(
        self, *,
        replica_assignment: Mapping[TopicPartitionReplica, str],
    ) -> AlterReplicaLogDirsResult[Async]: ...
    @overload
    def alter_replica_log_dirs(
        self, *,
        replica_assignment: Mapping[TopicPartitionReplica, str],
        options: AlterReplicaLogDirsOptions,
    ) -> AlterReplicaLogDirsResult[Async]: ...

    def alter_replica_log_dirs(
            self, *, replica_assignment: Mapping[TopicPartitionReplica, str],
            options: AlterReplicaLogDirsOptions = MISSING
            ) -> AlterReplicaLogDirsResult[Async]:
        """See :meth:`Admin.alter_replica_log_dirs`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(replica_assignment, "replica_assignment")
        # replica.brokerId() (3048) before options (3070); the log dir is not read.
        _conversions.check_keys(replica_assignment, "replica_assignment")
        reads = bool(replica_assignment)
        options = _conversions.options_or_new(options, AlterReplicaLogDirsOptions, read=reads)
        args = _conversions.alter_replica_log_dirs_args(replica_assignment)
        native = self._call(_lib.Admin_alter_replica_log_dirs, *args, options._setters_ran())
        return _conversions.alter_replica_log_dirs_result(native, Async, self._binding.loop)

    @overload
    def describe_log_dirs(self, *, brokers: Iterable[int]) -> DescribeLogDirsResult[Async]: ...
    @overload
    def describe_log_dirs(self, *, brokers: Iterable[int],
                          options: DescribeLogDirsOptions) -> DescribeLogDirsResult[Async]: ...

    def describe_log_dirs(self, *, brokers: Iterable[int],
                                options: DescribeLogDirsOptions = MISSING
                                ) -> DescribeLogDirsResult[Async]:
        """See :meth:`Admin.describe_log_dirs`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(brokers, "brokers")
        broker_ids = list(brokers)
        # new Call(…, calcDeadlineMs(now, options.timeoutMs()), new ConstantNodeIdProvider(
        # brokerId)) reads options before it unboxes the broker id (3128-3129).
        options = _conversions.options_or_new(options, DescribeLogDirsOptions,
                                              read=bool(broker_ids))
        args = _conversions.describe_log_dirs_args(broker_ids)
        native = self._call(_lib.Admin_describe_log_dirs, *args, options._setters_ran())
        return _conversions.describe_log_dirs_result(native, Async, self._binding.loop)

    @overload
    def describe_replica_log_dirs(
        self, *,
        replicas: Iterable[TopicPartitionReplica],
    ) -> DescribeReplicaLogDirsResult[Async]: ...
    @overload
    def describe_replica_log_dirs(
        self, *,
        replicas: Iterable[TopicPartitionReplica],
        options: DescribeReplicaLogDirsOptions,
    ) -> DescribeReplicaLogDirsResult[Async]: ...

    def describe_replica_log_dirs(
            self, *, replicas: Iterable[TopicPartitionReplica],
            options: DescribeReplicaLogDirsOptions = MISSING
            ) -> DescribeReplicaLogDirsResult[Async]:
        """See :meth:`Admin.describe_replica_log_dirs`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(replicas, "replicas")
        args = _conversions.describe_replica_log_dirs_args(list(replicas))
        reads = bool(args[0])
        options = _conversions.options_or_new(options, DescribeReplicaLogDirsOptions, read=reads)
        native = self._call(_lib.Admin_describe_replica_log_dirs, *args, options._setters_ran())
        return _conversions.describe_replica_log_dirs_result(native, Async, self._binding.loop)

    @overload
    def create_partitions(
        self, *,
        new_partitions: Mapping[str, NewPartitions],
    ) -> CreatePartitionsResult[Async]: ...
    @overload
    def create_partitions(self, *, new_partitions: Mapping[str, NewPartitions],
                          options: CreatePartitionsOptions) -> CreatePartitionsResult[Async]: ...

    def create_partitions(self, *, new_partitions: Mapping[str, NewPartitions],
                                options: CreatePartitionsOptions = MISSING
                                ) -> CreatePartitionsResult[Async]:
        """See :meth:`Admin.create_partitions`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(new_partitions, "new_partitions")
        args = _conversions.create_partitions_args(new_partitions)
        options = _conversions.options_or_new(options, CreatePartitionsOptions, read=bool(args[0]))
        native = self._call(_lib.Admin_create_partitions, *args, options._setters_ran())
        return _conversions.create_partitions_result(native, Async, self._binding.loop)

    @overload
    def delete_records(
        self, *,
        records_to_delete: Mapping[TopicPartition, RecordsToDelete],
    ) -> DeleteRecordsResult[Async]: ...
    @overload
    def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                       options: DeleteRecordsOptions) -> DeleteRecordsResult[Async]: ...

    def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                             options: DeleteRecordsOptions = MISSING
                             ) -> DeleteRecordsResult[Async]:
        """See :meth:`Admin.delete_records`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(records_to_delete, "records_to_delete")
        # PartitionLeaderFuture's toUnmodifiableMap rejects a null key (3385) before
        # options (3388); the values are read by the handler.
        _conversions.check_keys(records_to_delete, "records_to_delete")
        options = _conversions.options_or_new(options, DeleteRecordsOptions)
        native = self._call(_lib.Admin_delete_records,
                            *_conversions.delete_records_args(records_to_delete),
                            options._setters_ran())
        return _conversions.delete_records_result(native, Async, self._binding.loop)

    @overload
    def create_delegation_token(self) -> CreateDelegationTokenResult[Async]: ...
    @overload
    def create_delegation_token(
        self, *,
        options: CreateDelegationTokenOptions,
    ) -> CreateDelegationTokenResult[Async]: ...

    def create_delegation_token(self, *, options: CreateDelegationTokenOptions = MISSING
                                ) -> CreateDelegationTokenResult[Async]:
        """See :meth:`Admin.create_delegation_token`; the result's future is an
        ``AsyncKafkaFuture``."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, CreateDelegationTokenOptions)
        native = self._call(_lib.Admin_create_delegation_token,
                            _conversions.create_delegation_token_options(options))
        return _conversions.create_delegation_token_result(native, Async, self._binding.loop)

    @overload
    def renew_delegation_token(self, *, hmac: bytes) -> RenewDelegationTokenResult[Async]: ...
    @overload
    def renew_delegation_token(
        self, *,
        hmac: bytes,
        options: RenewDelegationTokenOptions,
    ) -> RenewDelegationTokenResult[Async]: ...

    def renew_delegation_token(self, *, hmac: bytes,
                               options: RenewDelegationTokenOptions = MISSING
                               ) -> RenewDelegationTokenResult[Async]:
        """See :meth:`Admin.renew_delegation_token`; the result's future is an
        ``AsyncKafkaFuture``."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, RenewDelegationTokenOptions)
        _conversions.non_null(hmac, "hmac")
        native = self._call(_lib.Admin_renew_delegation_token, hmac, options._setters_ran())
        return _conversions.renew_delegation_token_result(native, Async, self._binding.loop)

    @overload
    def expire_delegation_token(self, *, hmac: bytes) -> ExpireDelegationTokenResult[Async]: ...
    @overload
    def expire_delegation_token(
        self, *,
        hmac: bytes,
        options: ExpireDelegationTokenOptions,
    ) -> ExpireDelegationTokenResult[Async]: ...

    def expire_delegation_token(self, *, hmac: bytes,
                                options: ExpireDelegationTokenOptions = MISSING
                                ) -> ExpireDelegationTokenResult[Async]:
        """See :meth:`Admin.expire_delegation_token`; the result's future is an
        ``AsyncKafkaFuture``."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, ExpireDelegationTokenOptions)
        _conversions.non_null(hmac, "hmac")
        native = self._call(_lib.Admin_expire_delegation_token, hmac, options._setters_ran())
        return _conversions.expire_delegation_token_result(native, Async, self._binding.loop)

    @overload
    def describe_delegation_token(self) -> DescribeDelegationTokenResult[Async]: ...
    @overload
    def describe_delegation_token(
        self, *,
        options: DescribeDelegationTokenOptions,
    ) -> DescribeDelegationTokenResult[Async]: ...

    def describe_delegation_token(self, *,
                                  options: DescribeDelegationTokenOptions = MISSING
                                  ) -> DescribeDelegationTokenResult[Async]:
        """See :meth:`Admin.describe_delegation_token`; the result's future is an
        ``AsyncKafkaFuture``."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeDelegationTokenOptions)
        native = self._call(_lib.Admin_describe_delegation_token,
                            _conversions.describe_delegation_token_options(options))
        return _conversions.describe_delegation_token_result(native, Async, self._binding.loop)

    @overload
    def describe_consumer_groups(
        self, *,
        group_ids: Iterable[str],
        options: DescribeConsumerGroupsOptions,
    ) -> DescribeConsumerGroupsResult[Async]: ...
    @overload
    def describe_consumer_groups(
        self, *,
        group_ids: Iterable[str],
    ) -> DescribeConsumerGroupsResult[Async]: ...

    def describe_consumer_groups(self, *, group_ids: Iterable[str],
                                 options: DescribeConsumerGroupsOptions = MISSING
                                 ) -> DescribeConsumerGroupsResult[Async]:
        """See :meth:`Admin.describe_consumer_groups`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        reject_str("group_ids", group_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(group_ids, "group_ids")
        options = _conversions.options_or_new(options, DescribeConsumerGroupsOptions)
        native = self._call(_lib.Admin_describe_consumer_groups, _conversions.group_ids_list(group_ids),
                            options._setters_ran())
        return _conversions.describe_consumer_groups_result(native, Async, self._binding.loop)

    @overload
    def list_consumer_group_offsets(
        self, *,
        group_id: str,
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult[Async]: ...
    @overload
    def list_consumer_group_offsets(self, *,
                                    group_id: str) -> ListConsumerGroupOffsetsResult[Async]: ...
    @overload
    def list_consumer_group_offsets(
        self, *,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec],
        options: ListConsumerGroupOffsetsOptions,
    ) -> ListConsumerGroupOffsetsResult[Async]: ...
    @overload
    def list_consumer_group_offsets(
        self, *,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec],
    ) -> ListConsumerGroupOffsetsResult[Async]: ...

    @java_forms(Form("group_id", "options"), Form("group_id"),
                Form("group_specs", "options"), Form("group_specs"))
    def list_consumer_group_offsets(
        self, *, group_id: str = MISSING,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec] = MISSING,
        options: ListConsumerGroupOffsetsOptions = MISSING
    ) -> ListConsumerGroupOffsetsResult[Async]:
        """See :meth:`Admin.list_consumer_group_offsets`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        if group_specs is not MISSING:
            _conversions.non_null(group_specs, "group_specs")  # groupSpecs.keySet()
        options = _conversions.options_or_new(options, ListConsumerGroupOffsetsOptions)
        if group_id is not MISSING:
            _conversions.non_null(group_id, "group_id")
        native = self._call(_lib.Admin_list_consumer_group_offsets,
                            *_conversions.list_consumer_group_offsets_args(group_id, group_specs),
                            options._setters_ran())
        if group_specs is MISSING:  # java_forms: one or the other
            assert group_id is not None  # the non_null above
            return _conversions.list_consumer_group_offsets_result([group_id], native, Async,
                                                                   self._binding.loop)
        assert group_specs is not None  # the non_null above
        return _conversions.list_consumer_group_offsets_result(group_specs, native, Async,
                                                               self._binding.loop)

    @overload
    def delete_consumer_groups(
        self, *,
        group_ids: Iterable[str],
        options: DeleteConsumerGroupsOptions,
    ) -> DeleteConsumerGroupsResult[Async]: ...
    @overload
    def delete_consumer_groups(self, *,
                               group_ids: Iterable[str]) -> DeleteConsumerGroupsResult[Async]: ...

    def delete_consumer_groups(self, *, group_ids: Iterable[str],
                               options: DeleteConsumerGroupsOptions = MISSING
                               ) -> DeleteConsumerGroupsResult[Async]:
        """See :meth:`Admin.delete_consumer_groups`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        reject_str("group_ids", group_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(group_ids, "group_ids")
        options = _conversions.options_or_new(options, DeleteConsumerGroupsOptions)
        native = self._call(_lib.Admin_delete_consumer_groups, _conversions.group_ids_list(group_ids),
                            options._setters_ran())
        return _conversions.delete_consumer_groups_result(native, Async, self._binding.loop)

    @overload
    def delete_consumer_group_offsets(
        self, *,
        group_id: str,
        partitions: Iterable[TopicPartition],
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> DeleteConsumerGroupOffsetsResult[Async]: ...
    @overload
    def delete_consumer_group_offsets(
        self, *,
        group_id: str,
        partitions: Iterable[TopicPartition],
    ) -> DeleteConsumerGroupOffsetsResult[Async]: ...

    def delete_consumer_group_offsets(
        self, *, group_id: str, partitions: Iterable[TopicPartition],
        options: DeleteConsumerGroupOffsetsOptions = MISSING
    ) -> DeleteConsumerGroupOffsetsResult[Async]:
        """See :meth:`Admin.delete_consumer_group_offsets`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, DeleteConsumerGroupOffsetsOptions)
        _conversions.non_null(group_id, "group_id")
        _conversions.non_null(partitions, "partitions")
        tps = list(partitions)
        native = self._call(_lib.Admin_delete_consumer_group_offsets,
                            *_conversions.delete_consumer_group_offsets_args(group_id, tps),
                            options._setters_ran())
        return _conversions.delete_consumer_group_offsets_result(tps, native, Async,
                                                                 self._binding.loop)

    @overload
    def list_groups(self) -> ListGroupsResult[Async]: ...
    @overload
    def list_groups(self, *, options: ListGroupsOptions) -> ListGroupsResult[Async]: ...

    def list_groups(self, *, options: ListGroupsOptions = MISSING
                    ) -> ListGroupsResult[Async]:
        """See :meth:`Admin.list_groups`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, ListGroupsOptions)
        native = self._call(_lib.Admin_list_groups, _conversions.list_groups_options(options))
        return _conversions.list_groups_result(native, Async, self._binding.loop)

    @overload
    def elect_leaders(
        self, *,
        election_type: ElectionType,
        partitions: Iterable[TopicPartition] | None,
    ) -> ElectLeadersResult[Async]: ...
    @overload
    def elect_leaders(self, *, election_type: ElectionType,
                      partitions: Iterable[TopicPartition] | None,
                      options: ElectLeadersOptions) -> ElectLeadersResult[Async]: ...

    def elect_leaders(self, *, election_type: ElectionType,
                            partitions: Iterable[TopicPartition] | None,
                            options: ElectLeadersOptions = MISSING
                            ) -> ElectLeadersResult[Async]:
        """See :meth:`Admin.elect_leaders`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, ElectLeadersOptions)
        _conversions.non_null(election_type, "election_type")
        native = self._call(_lib.Admin_elect_leaders,
                            *_conversions.elect_leaders_args(election_type, partitions),
                            options._setters_ran())
        return _conversions.elect_leaders_result(native, Async, self._binding.loop)

    @overload
    def alter_partition_reassignments(
        self, *,
        reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
    ) -> AlterPartitionReassignmentsResult[Async]: ...
    @overload
    def alter_partition_reassignments(
        self, *,
        reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
        options: AlterPartitionReassignmentsOptions,
    ) -> AlterPartitionReassignmentsResult[Async]: ...

    def alter_partition_reassignments(
            self, *, reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
            options: AlterPartitionReassignmentsOptions = MISSING
            ) -> AlterPartitionReassignmentsResult[Async]:
        """See :meth:`Admin.alter_partition_reassignments`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(reassignments, "reassignments")
        args = _conversions.alter_partition_reassignments_args(reassignments)
        options = _conversions.options_or_new(options, AlterPartitionReassignmentsOptions)
        native = self._call(_lib.Admin_alter_partition_reassignments, *args, options._setters_ran())
        return _conversions.alter_partition_reassignments_result(native, Async, self._binding.loop)

    @overload
    def list_partition_reassignments(self) -> ListPartitionReassignmentsResult[Async]: ...
    @overload
    def list_partition_reassignments(
        self, *,
        partitions: Iterable[TopicPartition],
    ) -> ListPartitionReassignmentsResult[Async]: ...
    @overload
    def list_partition_reassignments(
        self, *,
        partitions: Iterable[TopicPartition],
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult[Async]: ...
    @overload
    def list_partition_reassignments(
        self, *,
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult[Async]: ...

    def list_partition_reassignments(
            self, *, partitions: Iterable[TopicPartition] = MISSING,
            options: ListPartitionReassignmentsOptions = MISSING
            ) -> ListPartitionReassignmentsResult[Async]:
        """See :meth:`Admin.list_partition_reassignments`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        targets: Iterable[TopicPartition] | None = None
        if partitions is not MISSING:  # else Java's (options) form: Optional.empty()
            _conversions.non_null(partitions, "partitions")  # Optional.of(partitions)
            targets = partitions
        args = _conversions.list_partition_reassignments_args(targets)
        reads = _conversions.all_partitions_valid(args[0])
        options = _conversions.options_or_new(options, ListPartitionReassignmentsOptions, read=reads)
        native = self._call(_lib.Admin_list_partition_reassignments, *args, options._setters_ran())
        return _conversions.list_partition_reassignments_result(native, Async, self._binding.loop)

    def remove_members_from_consumer_group(
        self, *, group_id: str, options: RemoveMembersFromConsumerGroupOptions
    ) -> RemoveMembersFromConsumerGroupResult[Async]:
        """See :meth:`Admin.remove_members_from_consumer_group`; the result's
        futures are ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        # Java reads options.reason() first; the group id only later, in the call.
        _conversions.non_null(options, "options")
        _conversions.non_null(group_id, "group_id")
        args = _conversions.remove_members_from_consumer_group_args(group_id, options)
        native = self._call(_lib.Admin_remove_members_from_consumer_group, *args,
                            options._setters_ran())
        return _conversions.remove_members_from_consumer_group_result(options, native, Async,
                                                                      self._binding.loop)

    @overload
    def alter_consumer_group_offsets(
        self, *,
        group_id: str,
        offsets: Mapping[TopicPartition, OffsetAndMetadata],
    ) -> AlterConsumerGroupOffsetsResult[Async]: ...
    @overload
    def alter_consumer_group_offsets(
        self, *,
        group_id: str,
        offsets: Mapping[TopicPartition, OffsetAndMetadata],
        options: AlterConsumerGroupOffsetsOptions,
    ) -> AlterConsumerGroupOffsetsResult[Async]: ...

    def alter_consumer_group_offsets(
        self, *, group_id: str, offsets: Mapping[TopicPartition, OffsetAndMetadata],
        options: AlterConsumerGroupOffsetsOptions = MISSING
    ) -> AlterConsumerGroupOffsetsResult[Async]:
        """See :meth:`Admin.alter_consumer_group_offsets`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, AlterConsumerGroupOffsetsOptions)
        _conversions.non_null(group_id, "group_id")
        _conversions.non_null(offsets, "offsets")
        native = self._call(_lib.Admin_alter_consumer_group_offsets,
                            *_conversions.alter_consumer_group_offsets_args(group_id, offsets),
                            options._setters_ran())
        return _conversions.alter_consumer_group_offsets_result(native, Async,
                                                                self._binding.loop)

    @overload
    def list_offsets(
        self, *,
        topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
    ) -> ListOffsetsResult[Async]: ...
    @overload
    def list_offsets(self, *, topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
                     options: ListOffsetsOptions) -> ListOffsetsResult[Async]: ...

    def list_offsets(self, *, topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
                           options: ListOffsetsOptions = MISSING) -> ListOffsetsResult[Async]:
        """See :meth:`Admin.list_offsets`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(topic_partition_offsets, "topic_partition_offsets")
        args = _conversions.list_offsets_args(topic_partition_offsets)
        options = _conversions.options_or_new(options, ListOffsetsOptions)
        partitions = list(topic_partition_offsets)
        native = self._call(_lib.Admin_list_offsets, *args, options.isolation_level().id(), options._setters_ran())
        return _conversions.list_offsets_result(native, partitions, Async, self._binding.loop)

    @overload
    def describe_client_quotas(self, *,
                               filter: ClientQuotaFilter) -> DescribeClientQuotasResult[Async]: ...
    @overload
    def describe_client_quotas(
        self, *,
        filter: ClientQuotaFilter,
        options: DescribeClientQuotasOptions,
    ) -> DescribeClientQuotasResult[Async]: ...

    def describe_client_quotas(self, *, filter: ClientQuotaFilter,  # noqa: A002 - Java's name
                               options: DescribeClientQuotasOptions = MISSING
                               ) -> DescribeClientQuotasResult[Async]:
        """See :meth:`Admin.describe_client_quotas`; the result's future is an
        ``AsyncKafkaFuture``."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeClientQuotasOptions)
        _conversions.non_null(filter, "filter")
        native = self._call(_lib.Admin_describe_client_quotas,
                            *_conversions.describe_client_quotas_args(filter),
                            options._setters_ran())
        return _conversions.describe_client_quotas_result(native, Async, self._binding.loop)

    @overload
    def alter_client_quotas(
        self, *,
        entries: Iterable[ClientQuotaAlteration],
    ) -> AlterClientQuotasResult[Async]: ...
    @overload
    def alter_client_quotas(
        self, *,
        entries: Iterable[ClientQuotaAlteration],
        options: AlterClientQuotasOptions,
    ) -> AlterClientQuotasResult[Async]: ...

    def alter_client_quotas(self, *, entries: Iterable[ClientQuotaAlteration],
                            options: AlterClientQuotasOptions = MISSING
                            ) -> AlterClientQuotasResult[Async]:
        """See :meth:`Admin.alter_client_quotas`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(entries, "entries")
        args = _conversions.alter_client_quotas_args(entries)
        options = _conversions.options_or_new(options, AlterClientQuotasOptions)
        native = self._call(_lib.Admin_alter_client_quotas, *args, options._setters_ran())
        return _conversions.alter_client_quotas_result(native, Async, self._binding.loop)

    @overload
    def describe_user_scram_credentials(self) -> DescribeUserScramCredentialsResult[Async]: ...
    @overload
    def describe_user_scram_credentials(
        self, *,
        users: Sequence[str] | None,
    ) -> DescribeUserScramCredentialsResult[Async]: ...
    @overload
    def describe_user_scram_credentials(
        self, *,
        users: Sequence[str] | None,
        options: DescribeUserScramCredentialsOptions,
    ) -> DescribeUserScramCredentialsResult[Async]: ...

    @java_forms(Form(), Form("users"), Form("users", "options"))
    def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None = MISSING,
            options: DescribeUserScramCredentialsOptions = MISSING
    ) -> DescribeUserScramCredentialsResult[Async]:
        """See :meth:`Admin.describe_user_scram_credentials`; the result's futures
        are ``AsyncKafkaFuture``\\ s."""
        if users is not MISSING and users is not None:
            reject_str("users", users)  # the shape check precedes the closed check
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeUserScramCredentialsOptions)
        if users is MISSING:
            users = None  # Java's describeUserScramCredentials() passes null: all users
        native = self._call(_lib.Admin_describe_user_scram_credentials,
                            *_conversions.describe_user_scram_credentials_args(users),
                            options._setters_ran())
        return _conversions.describe_user_scram_credentials_result(
            native, Async, self._binding.loop)

    @overload
    def alter_user_scram_credentials(
        self, *,
        alterations: Sequence[UserScramCredentialAlteration],
    ) -> AlterUserScramCredentialsResult[Async]: ...
    @overload
    def alter_user_scram_credentials(
        self, *,
        alterations: Sequence[UserScramCredentialAlteration],
        options: AlterUserScramCredentialsOptions,
    ) -> AlterUserScramCredentialsResult[Async]: ...

    def alter_user_scram_credentials(
            self, *, alterations: Sequence[UserScramCredentialAlteration],
            options: AlterUserScramCredentialsOptions = MISSING
    ) -> AlterUserScramCredentialsResult[Async]:
        """See :meth:`Admin.alter_user_scram_credentials`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(alterations, "alterations")
        args = _conversions.alter_user_scram_credentials_args(alterations)
        options = _conversions.options_or_new(options, AlterUserScramCredentialsOptions)
        native = self._call(_lib.Admin_alter_user_scram_credentials, *args, options._setters_ran())
        return _conversions.alter_user_scram_credentials_result(native, Async, self._binding.loop)

    @overload
    def describe_features(self) -> DescribeFeaturesResult[Async]: ...
    @overload
    def describe_features(self, *,
                          options: DescribeFeaturesOptions) -> DescribeFeaturesResult[Async]: ...

    def describe_features(self, *, options: DescribeFeaturesOptions = MISSING
                          ) -> DescribeFeaturesResult[Async]:
        """See :meth:`Admin.describe_features`; the result's future is an
        ``AsyncKafkaFuture``."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeFeaturesOptions)
        native = self._call(_lib.Admin_describe_features, options._setters_ran())
        return _conversions.describe_features_result(native, Async, self._binding.loop)

    @overload
    def update_features(
        self, *,
        feature_updates: Mapping[str, FeatureUpdate],
    ) -> UpdateFeaturesResult[Async]: ...
    @overload
    def update_features(self, *, feature_updates: Mapping[str, FeatureUpdate],
                        options: UpdateFeaturesOptions) -> UpdateFeaturesResult[Async]: ...

    def update_features(self, *, feature_updates: Mapping[str, FeatureUpdate],
                        options: UpdateFeaturesOptions = MISSING
                        ) -> UpdateFeaturesResult[Async]:
        """See :meth:`Admin.update_features`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(feature_updates, "feature_updates")
        if None in feature_updates:
            # Utils.isBlank(null) (4726-4727), before options (4733); the FFI cannot
            # carry the null, so Python raises Java's error (a blank key gets the
            # same error from the core).
            raise IllegalArgumentError(message="Provided feature can not be empty.")
        options = _conversions.options_or_new(options, UpdateFeaturesOptions)
        native = self._call(_lib.Admin_update_features,
                            *_conversions.update_features_args(feature_updates),
                            options._setters_ran())
        return _conversions.update_features_result(native, Async, self._binding.loop)

    @overload
    def unregister_controller(self, *,
                              controller_id: int) -> UnregisterControllerResult[Async]: ...
    @overload
    def unregister_controller(
        self, *,
        controller_id: int,
        options: UnregisterControllerOptions,
    ) -> UnregisterControllerResult[Async]: ...

    def unregister_controller(self, *, controller_id: int,
                              options: UnregisterControllerOptions = MISSING
                              ) -> UnregisterControllerResult[Async]:
        """See :meth:`Admin.unregister_controller`."""
        self._check_not_closed()
        _conversions.non_null(controller_id, "controller_id")
        options = _conversions.options_or_new(options, UnregisterControllerOptions)
        native = self._call(_lib.Admin_unregister_controller, controller_id,
                            options._setters_ran())
        return _conversions.unregister_controller_result(native, Async, self._binding.loop)

    @overload
    def describe_producers(
        self, *,
        partitions: Iterable[TopicPartition],
    ) -> DescribeProducersResult[Async]: ...
    @overload
    def describe_producers(
        self, *,
        partitions: Iterable[TopicPartition],
        options: DescribeProducersOptions,
    ) -> DescribeProducersResult[Async]: ...

    def describe_producers(self, *, partitions: Iterable[TopicPartition],
                           options: DescribeProducersOptions = MISSING
                           ) -> DescribeProducersResult[Async]:
        """See :meth:`Admin.describe_producers`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        _conversions.non_null(partitions, "partitions")
        targets = list(partitions)
        # PartitionLeaderFuture's toUnmodifiableMap rejects a null key (4997) before
        # options (4998).
        args = _conversions.describe_producers_args(targets)
        options = _conversions.options_or_new(options, DescribeProducersOptions)
        native = self._call(_lib.Admin_describe_producers, *args, options._setters_ran())
        return _conversions.describe_producers_result(native, Async, self._binding.loop, targets)

    @overload
    def describe_transactions(
        self, *,
        transactional_ids: Iterable[str],
    ) -> DescribeTransactionsResult[Async]: ...
    @overload
    def describe_transactions(
        self, *,
        transactional_ids: Iterable[str],
        options: DescribeTransactionsOptions,
    ) -> DescribeTransactionsResult[Async]: ...

    def describe_transactions(self, *, transactional_ids: Iterable[str],
                              options: DescribeTransactionsOptions = MISSING
                              ) -> DescribeTransactionsResult[Async]:
        """See :meth:`Admin.describe_transactions`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        reject_str("transactional_ids", transactional_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(transactional_ids, "transactional_ids")
        options = _conversions.options_or_new(options, DescribeTransactionsOptions)
        ids = _conversions.transactional_ids_list(transactional_ids)
        native = self._call(_lib.Admin_describe_transactions,
                            *_conversions.describe_transactions_args(ids), options._setters_ran())
        return _conversions.describe_transactions_result(native, Async, self._binding.loop, ids)

    @overload
    def abort_transaction(self, *,
                          spec: AbortTransactionSpec) -> AbortTransactionResult[Async]: ...
    @overload
    def abort_transaction(self, *, spec: AbortTransactionSpec,
                          options: AbortTransactionOptions) -> AbortTransactionResult[Async]: ...

    def abort_transaction(self, *, spec: AbortTransactionSpec,
                          options: AbortTransactionOptions = MISSING
                          ) -> AbortTransactionResult[Async]:
        """See :meth:`Admin.abort_transaction`; the result's future is an
        ``AsyncKafkaFuture``."""
        self._check_not_closed()
        _conversions.non_null(spec, "spec")
        options = _conversions.options_or_new(options, AbortTransactionOptions)
        native = self._call(_lib.Admin_abort_transaction, *_conversions.abort_transaction_args(spec),
                            options._setters_ran())
        return _conversions.abort_transaction_result(native, Async, self._binding.loop, spec)

    @overload
    def list_transactions(self) -> ListTransactionsResult[Async]: ...
    @overload
    def list_transactions(self, *,
                          options: ListTransactionsOptions) -> ListTransactionsResult[Async]: ...

    def list_transactions(self, *, options: ListTransactionsOptions = MISSING
                          ) -> ListTransactionsResult[Async]:
        """See :meth:`Admin.list_transactions`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, ListTransactionsOptions)
        native = self._call(_lib.Admin_list_transactions,
                            _conversions.list_transactions_options(options))
        return _conversions.list_transactions_result(native, Async, self._binding.loop)

    @overload
    def fence_producers(self, *,
                        transactional_ids: Iterable[str]) -> FenceProducersResult[Async]: ...
    @overload
    def fence_producers(self, *, transactional_ids: Iterable[str],
                        options: FenceProducersOptions) -> FenceProducersResult[Async]: ...

    def fence_producers(self, *, transactional_ids: Iterable[str],
                        options: FenceProducersOptions = MISSING
                        ) -> FenceProducersResult[Async]:
        """See :meth:`Admin.fence_producers`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        reject_str("transactional_ids", transactional_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(transactional_ids, "transactional_ids")
        options = _conversions.options_or_new(options, FenceProducersOptions)
        ids = _conversions.transactional_ids_list(transactional_ids)
        native = self._call(_lib.Admin_fence_producers, *_conversions.fence_producers_args(ids),
                            options._setters_ran())
        return _conversions.fence_producers_result(native, Async, self._binding.loop, ids)

    @overload
    def list_config_resources(
        self, *,
        config_resource_types: Iterable[ConfigResource.Type],
        options: ListConfigResourcesOptions,
    ) -> ListConfigResourcesResult[Async]: ...
    @overload
    def list_config_resources(self) -> ListConfigResourcesResult[Async]: ...

    @java_forms(Form("config_resource_types", "options",
                     defaults={"config_resource_types": (), "options": MISSING}),
                Form())
    def list_config_resources(
            self, *, config_resource_types: Iterable[ConfigResource.Type] = MISSING,
            options: ListConfigResourcesOptions = MISSING
            ) -> ListConfigResourcesResult[Async]:
        """See :meth:`Admin.list_config_resources`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, ListConfigResourcesOptions)
        _conversions.non_null(config_resource_types, "config_resource_types")
        native = self._call(_lib.Admin_list_config_resources,
                            *_conversions.list_config_resources_args(config_resource_types),
                            options._setters_ran())
        return _conversions.list_config_resources_result(native, Async, self._binding.loop)

    @overload
    def describe_classic_groups(
        self, *,
        group_ids: Iterable[str],
        options: DescribeClassicGroupsOptions,
    ) -> DescribeClassicGroupsResult[Async]: ...
    @overload
    def describe_classic_groups(
        self, *,
        group_ids: Iterable[str],
    ) -> DescribeClassicGroupsResult[Async]: ...

    def describe_classic_groups(self, *, group_ids: Iterable[str],
                                options: DescribeClassicGroupsOptions = MISSING
                                ) -> DescribeClassicGroupsResult[Async]:
        """See :meth:`Admin.describe_classic_groups`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        reject_str("group_ids", group_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(group_ids, "group_ids")
        options = _conversions.options_or_new(options, DescribeClassicGroupsOptions)
        native = self._call(_lib.Admin_describe_classic_groups, _conversions.group_ids_list(group_ids),
                            options._setters_ran())
        return _conversions.describe_classic_groups_result(native, Async, self._binding.loop)

    @overload
    def force_terminate_transaction(
        self, *,
        transactional_id: str,
    ) -> TerminateTransactionResult[Async]: ...
    @overload
    def force_terminate_transaction(
        self, *,
        transactional_id: str,
        options: TerminateTransactionOptions,
    ) -> TerminateTransactionResult[Async]: ...

    def force_terminate_transaction(self, *, transactional_id: str,
                                    options: TerminateTransactionOptions = MISSING
                                    ) -> TerminateTransactionResult[Async]:
        """See :meth:`Admin.force_terminate_transaction`; the result's future is an
        ``AsyncKafkaFuture``."""
        self._check_not_closed()
        options = _conversions.options_or_new(options, TerminateTransactionOptions)
        _conversions.non_null(transactional_id, "transactional_id")
        native = self._call(_lib.Admin_force_terminate_transaction, transactional_id,
                            options._setters_ran())
        return _conversions.force_terminate_transaction_result(native, Async, self._binding.loop)
