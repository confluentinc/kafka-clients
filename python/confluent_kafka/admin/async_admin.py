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
``Admin``'s (see its module).
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka._async import (
    LoopBinding,
    OffLoopPump,
    Undelivered,
    await_to_end,
    call_soon_on,
)
from confluent_kafka.admin import _conversions
from confluent_kafka.admin._futures import Async
from confluent_kafka.admin.abort_transaction_options import AbortTransactionOptions
from confluent_kafka.admin.admin import _AdminState, _Free, _Payload, _Submit, close_timeout_ms
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
from confluent_kafka.admin.update_features_options import UpdateFeaturesOptions

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
    from confluent_kafka.admin.update_features_result import UpdateFeaturesResult
    from confluent_kafka.admin.user_scram_credential_alteration import (
        UserScramCredentialAlteration,
    )
    from confluent_kafka.common import (
        AclBinding,
        AclBindingFilter,
        ClientQuotaAlteration,
        ClientQuotaFilter,
        TopicPartition,
    )
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
    a running loop; the notify hook schedules the client's queued callbacks
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
        self._undelivered = Undelivered()
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

    async def close(self, *, timeout: Duration | None = None) -> None:
        """See :meth:`Admin.close`. Cancelling the awaiting task raises
        ``CancelledError`` at once; the close goes on in the core and the handle is
        freed when it ends."""
        if self._closed:
            return
        timeout_ms = close_timeout_ms(timeout)
        loop = self._binding.bind()
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
            self._undelivered.free_closed()
            raise
        try:
            # Off the loop: waits for the uses still in flight, then frees the
            # handle; a cancellation meanwhile lets it run to its end.
            await await_to_end(loop.run_in_executor(None, self._teardown))
        finally:
            self._undelivered.free_closed()

    def _finish_close(self, closed: threading.Event) -> None:
        closed.wait()
        self._teardown()

    async def __aenter__(self) -> AsyncAdmin:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # ---- the previous FFI's whole-result callback --------------------------------
    # The RPCs not yet on the extension's ``AdminResult`` still await an ``_async``
    # entry point through this; it goes once every RPC uses ``_call``.

    async def _await_for(self, submit: _Submit, free: _Free) -> _Payload:
        self._undelivered.free_closed()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[_Payload] = loop.create_future()

        def deliver(payload: _Payload) -> None:
            if future.done():
                free(payload)
                return
            future.set_result(payload)

        def cb(*payload: Any) -> None:
            try:
                self._undelivered.hand_over(loop, deliver, payload, free)
            finally:
                self._end_use()

        self._submit(submit, cb)
        try:
            return await future
        except asyncio.CancelledError:
            if future.done() and not future.cancelled():
                free(future.result())
            raise

    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions | None = None) -> CreateTopicsResult[Async]:
        """See :meth:`Admin.create_topics`; the result's futures are
        ``AsyncKafkaFuture``\\ s."""
        if options is None:
            options = CreateTopicsOptions()
        native = self._call(_lib.Admin_create_topics,
                            *_conversions.create_topics_args(new_topics),
                            options._setters_ran())
        return _conversions.create_topics_result(native, Async, self._binding.loop)

    @overload
    async def delete_topics(self, *, topics: Iterable[str],
                            options: DeleteTopicsOptions | None = None
                            ) -> DeleteTopicsResult[Async]: ...
    @overload
    async def delete_topics(self, *, topics: TopicCollection,
                            options: DeleteTopicsOptions | None = None
                            ) -> DeleteTopicsResult[Async]: ...

    async def delete_topics(self, *, topics: Iterable[str] | TopicCollection,
                            options: DeleteTopicsOptions | None = None
                            ) -> DeleteTopicsResult[Async]:
        """See :meth:`Admin.delete_topics`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DeleteTopicsOptions()
        self._check_not_closed()
        by_ids, keys = _conversions.topic_collection_target(topics, "deleteTopics")
        args = _conversions.delete_topics_args(by_ids, keys, options)
        submit = (_lib.Admin_delete_topics_by_ids_async if by_ids
                  else _lib.Admin_delete_topics_async)
        payload = await self._await_for(lambda c_admin, cb: submit(c_admin, *args, cb),
                                        _conversions.free_delete_topics)
        return _conversions.delete_topics_result(by_ids, keys, payload, Async)

    def list_topics(self, *, options: ListTopicsOptions | None = None
                    ) -> ListTopicsResult[Async]:
        """See :meth:`Admin.list_topics`."""
        if options is None:
            options = ListTopicsOptions()
        native = self._call(_lib.Admin_list_topics, options._setters_ran())
        return _conversions.list_topics_result(native, Async, self._binding.loop)

    @overload
    async def describe_topics(self, *, topic_names: Iterable[str],
                              options: DescribeTopicsOptions | None = None
                              ) -> DescribeTopicsResult[Async]: ...
    @overload
    async def describe_topics(self, *, topics: TopicCollection,
                              options: DescribeTopicsOptions | None = None
                              ) -> DescribeTopicsResult[Async]: ...

    @java_forms(Form("topic_names"), Form("topic_names", "options"),
                Form("topics"), Form("topics", "options"))
    async def describe_topics(self, *, topic_names: Iterable[str] | None = None,
                              topics: TopicCollection | None = None,
                              options: DescribeTopicsOptions | None = None
                              ) -> DescribeTopicsResult[Async]:
        """See :meth:`Admin.describe_topics`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DescribeTopicsOptions()
        self._check_not_closed()
        by_ids, keys = _conversions.topic_collection_target(
            topic_names if topics is None else topics, "describeTopics")
        args = _conversions.describe_topics_args(by_ids, keys, options)
        submit = (_lib.Admin_describe_topics_by_ids_async if by_ids
                  else _lib.Admin_describe_topics_async)
        payload = await self._await_for(lambda c_admin, cb: submit(c_admin, *args, cb),
                                        _conversions.free_describe_topics)
        return _conversions.describe_topics_result(by_ids, keys, payload, Async)

    async def describe_cluster(self, *, options: DescribeClusterOptions | None = None
                               ) -> DescribeClusterResult[Async]:
        """See :meth:`Admin.describe_cluster`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DescribeClusterOptions()
        args = _conversions.describe_cluster_args(options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_cluster_async(c_admin, *args, cb),
            _conversions.free_describe_cluster)
        return _conversions.describe_cluster_result(payload, Async)

    async def describe_acls(self, *, filter: AclBindingFilter,  # noqa: A002 - Java's name
                            options: DescribeAclsOptions | None = None) -> DescribeAclsResult[Async]:
        """See :meth:`Admin.describe_acls`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = DescribeAclsOptions()
        args = _conversions.describe_acls_args(filter, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_acls_async(c_admin, *args, cb),
            _conversions.free_describe_acls)
        return _conversions.describe_acls_result(payload, Async)

    async def create_acls(self, *, acls: Iterable[AclBinding],
                          options: CreateAclsOptions | None = None) -> CreateAclsResult[Async]:
        """See :meth:`Admin.create_acls`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = CreateAclsOptions()
        bindings = list(acls)
        args = _conversions.create_acls_args(bindings, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_create_acls_async(c_admin, *args, cb),
            _conversions.free_create_acls)
        return _conversions.create_acls_result(bindings, payload, Async)

    async def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                          options: DeleteAclsOptions | None = None) -> DeleteAclsResult[Async]:
        """See :meth:`Admin.delete_acls`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DeleteAclsOptions()
        acl_filters = list(filters)
        args = _conversions.delete_acls_args(acl_filters, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_delete_acls_async(c_admin, *args, cb),
            _conversions.free_delete_acls)
        return _conversions.delete_acls_result(acl_filters, payload, Async)

    async def describe_configs(self, *, resources: Iterable[ConfigResource],
                               options: DescribeConfigsOptions | None = None
                               ) -> DescribeConfigsResult[Async]:
        """See :meth:`Admin.describe_configs`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DescribeConfigsOptions()
        targets = list(resources)
        args = _conversions.describe_configs_args(targets, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_configs_async(c_admin, *args, cb),
            _conversions.free_describe_configs)
        return _conversions.describe_configs_result(targets, payload, Async)

    async def incremental_alter_configs(
            self, *, configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
            options: AlterConfigsOptions | None = None) -> AlterConfigsResult[Async]:
        """See :meth:`Admin.incremental_alter_configs`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = AlterConfigsOptions()
        resources = list(configs)
        args = _conversions.incremental_alter_configs_args(configs, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_incremental_alter_configs_async(c_admin, *args, cb),
            _conversions.free_incremental_alter_configs)
        return _conversions.incremental_alter_configs_result(resources, payload, Async)

    async def alter_replica_log_dirs(
            self, *, replica_assignment: Mapping[TopicPartitionReplica, str],
            options: AlterReplicaLogDirsOptions | None = None
            ) -> AlterReplicaLogDirsResult[Async]:
        """See :meth:`Admin.alter_replica_log_dirs`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = AlterReplicaLogDirsOptions()
        replicas = list(replica_assignment)
        args = _conversions.alter_replica_log_dirs_args(replica_assignment, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_alter_replica_log_dirs_async(c_admin, *args, cb),
            _conversions.free_alter_replica_log_dirs)
        return _conversions.alter_replica_log_dirs_result(replicas, payload, Async)

    async def describe_log_dirs(self, *, brokers: Iterable[int],
                                options: DescribeLogDirsOptions | None = None
                                ) -> DescribeLogDirsResult[Async]:
        """See :meth:`Admin.describe_log_dirs`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DescribeLogDirsOptions()
        broker_ids = list(brokers)
        args = _conversions.describe_log_dirs_args(broker_ids, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_log_dirs_async(c_admin, *args, cb),
            _conversions.free_describe_log_dirs)
        return _conversions.describe_log_dirs_result(broker_ids, payload, Async)

    async def describe_replica_log_dirs(
            self, *, replicas: Iterable[TopicPartitionReplica],
            options: DescribeReplicaLogDirsOptions | None = None
            ) -> DescribeReplicaLogDirsResult[Async]:
        """See :meth:`Admin.describe_replica_log_dirs`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DescribeReplicaLogDirsOptions()
        targets = list(replicas)
        args = _conversions.describe_replica_log_dirs_args(targets, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_replica_log_dirs_async(c_admin, *args, cb),
            _conversions.free_describe_replica_log_dirs)
        return _conversions.describe_replica_log_dirs_result(targets, payload, Async)

    async def create_partitions(self, *, new_partitions: Mapping[str, NewPartitions],
                                options: CreatePartitionsOptions | None = None
                                ) -> CreatePartitionsResult[Async]:
        """See :meth:`Admin.create_partitions`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = CreatePartitionsOptions()
        topics = list(new_partitions)
        args = _conversions.create_partitions_args(new_partitions, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_create_partitions_async(c_admin, *args, cb),
            _conversions.free_create_partitions)
        return _conversions.create_partitions_result(topics, payload, Async)

    async def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                             options: DeleteRecordsOptions | None = None
                             ) -> DeleteRecordsResult[Async]:
        """See :meth:`Admin.delete_records`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DeleteRecordsOptions()
        partitions = list(records_to_delete)
        args = _conversions.delete_records_args(records_to_delete, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_delete_records_async(c_admin, *args, cb),
            _conversions.free_delete_records)
        return _conversions.delete_records_result(partitions, payload, Async)

    async def create_delegation_token(self, *, options: CreateDelegationTokenOptions | None = None
                                      ) -> CreateDelegationTokenResult[Async]:
        """See :meth:`Admin.create_delegation_token`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = CreateDelegationTokenOptions()
        args = _conversions.create_delegation_token_args(options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_create_delegation_token_async(c_admin, *args, cb),
            _conversions.free_create_delegation_token)
        return _conversions.create_delegation_token_result(payload, Async)

    async def renew_delegation_token(self, *, hmac: bytes,
                                     options: RenewDelegationTokenOptions | None = None
                                     ) -> RenewDelegationTokenResult[Async]:
        """See :meth:`Admin.renew_delegation_token`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = RenewDelegationTokenOptions()
        args = _conversions.renew_delegation_token_args(hmac, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_renew_delegation_token_async(c_admin, *args, cb),
            _conversions.free_renew_delegation_token)
        return _conversions.renew_delegation_token_result(payload, Async)

    async def expire_delegation_token(self, *, hmac: bytes,
                                      options: ExpireDelegationTokenOptions | None = None
                                      ) -> ExpireDelegationTokenResult[Async]:
        """See :meth:`Admin.expire_delegation_token`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = ExpireDelegationTokenOptions()
        args = _conversions.expire_delegation_token_args(hmac, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_expire_delegation_token_async(c_admin, *args, cb),
            _conversions.free_expire_delegation_token)
        return _conversions.expire_delegation_token_result(payload, Async)

    async def describe_delegation_token(self, *,
                                        options: DescribeDelegationTokenOptions | None = None
                                        ) -> DescribeDelegationTokenResult[Async]:
        """See :meth:`Admin.describe_delegation_token`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = DescribeDelegationTokenOptions()
        args = _conversions.describe_delegation_token_args(options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_delegation_token_async(c_admin, *args, cb),
            _conversions.free_describe_delegation_token)
        return _conversions.describe_delegation_token_result(payload, Async)

    async def describe_consumer_groups(self, *, group_ids: Iterable[str],
                                       options: DescribeConsumerGroupsOptions | None = None
                                       ) -> DescribeConsumerGroupsResult[Async]:
        """See :meth:`Admin.describe_consumer_groups`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DescribeConsumerGroupsOptions()
        ids = list(group_ids)
        args = _conversions.describe_consumer_groups_args(ids, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_consumer_groups_async(c_admin, *args, cb),
            _conversions.free_describe_consumer_groups)
        return _conversions.describe_consumer_groups_result(ids, payload, Async)

    @overload
    async def list_consumer_group_offsets(
        self, *, group_id: str, options: ListConsumerGroupOffsetsOptions | None = None
    ) -> ListConsumerGroupOffsetsResult[Async]: ...
    @overload
    async def list_consumer_group_offsets(
        self, *, group_specs: Mapping[str, ListConsumerGroupOffsetsSpec],
        options: ListConsumerGroupOffsetsOptions | None = None
    ) -> ListConsumerGroupOffsetsResult[Async]: ...

    @java_forms(Form("group_id", "options"), Form("group_id"),
                Form("group_specs", "options"), Form("group_specs"))
    async def list_consumer_group_offsets(
        self, *, group_id: str | None = None,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec] | None = None,
        options: ListConsumerGroupOffsetsOptions | None = None
    ) -> ListConsumerGroupOffsetsResult[Async]:
        """See :meth:`Admin.list_consumer_group_offsets`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = ListConsumerGroupOffsetsOptions()
        if group_specs is None:
            assert group_id is not None
            group_specs = {group_id: ListConsumerGroupOffsetsSpec()}
        specs = dict(group_specs)
        args = _conversions.list_consumer_group_offsets_args(specs, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_list_consumer_group_offsets_async(c_admin, *args, cb),
            _conversions.free_list_consumer_group_offsets)
        return _conversions.list_consumer_group_offsets_result(specs, payload, Async)

    async def delete_consumer_groups(self, *, group_ids: Iterable[str],
                                     options: DeleteConsumerGroupsOptions | None = None
                                     ) -> DeleteConsumerGroupsResult[Async]:
        """See :meth:`Admin.delete_consumer_groups`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DeleteConsumerGroupsOptions()
        ids = list(group_ids)
        args = _conversions.delete_consumer_groups_args(ids, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_delete_consumer_groups_async(c_admin, *args, cb),
            _conversions.free_delete_consumer_groups)
        return _conversions.delete_consumer_groups_result(ids, payload, Async)

    async def delete_consumer_group_offsets(
        self, *, group_id: str, partitions: Iterable[TopicPartition],
        options: DeleteConsumerGroupOffsetsOptions | None = None
    ) -> DeleteConsumerGroupOffsetsResult[Async]:
        """See :meth:`Admin.delete_consumer_group_offsets`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = DeleteConsumerGroupOffsetsOptions()
        tps = list(partitions)
        args = _conversions.delete_consumer_group_offsets_args(group_id, tps, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_delete_consumer_group_offsets_async(
                c_admin, *args, cb),
            _conversions.free_delete_consumer_group_offsets)
        return _conversions.delete_consumer_group_offsets_result(tps, payload, Async)

    async def list_groups(self, *, options: ListGroupsOptions | None = None
                          ) -> ListGroupsResult[Async]:
        """See :meth:`Admin.list_groups`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = ListGroupsOptions()
        args = _conversions.list_groups_args(options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_list_groups_async(c_admin, *args, cb),
            _conversions.free_list_groups)
        return _conversions.list_groups_result(payload, Async)

    async def elect_leaders(self, *, election_type: ElectionType,
                            partitions: Iterable[TopicPartition] | None,
                            options: ElectLeadersOptions | None = None
                            ) -> ElectLeadersResult[Async]:
        """See :meth:`Admin.elect_leaders`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = ElectLeadersOptions()
        args = _conversions.elect_leaders_args(election_type, partitions, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_elect_leaders_async(c_admin, *args, cb),
            _conversions.free_elect_leaders)
        return _conversions.elect_leaders_result(payload, Async)

    async def alter_partition_reassignments(
            self, *, reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
            options: AlterPartitionReassignmentsOptions | None = None
            ) -> AlterPartitionReassignmentsResult[Async]:
        """See :meth:`Admin.alter_partition_reassignments`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = AlterPartitionReassignmentsOptions()
        partitions = list(reassignments)
        args = _conversions.alter_partition_reassignments_args(reassignments, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_alter_partition_reassignments_async(
                c_admin, *args, cb),
            _conversions.free_alter_partition_reassignments)
        return _conversions.alter_partition_reassignments_result(partitions, payload, Async)

    async def list_partition_reassignments(
            self, *, partitions: Iterable[TopicPartition] | None = None,
            options: ListPartitionReassignmentsOptions | None = None
            ) -> ListPartitionReassignmentsResult[Async]:
        """See :meth:`Admin.list_partition_reassignments`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = ListPartitionReassignmentsOptions()
        args = _conversions.list_partition_reassignments_args(partitions, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_list_partition_reassignments_async(
                c_admin, *args, cb),
            _conversions.free_list_partition_reassignments)
        return _conversions.list_partition_reassignments_result(payload, Async)

    async def remove_members_from_consumer_group(
        self, *, group_id: str, options: RemoveMembersFromConsumerGroupOptions
    ) -> RemoveMembersFromConsumerGroupResult[Async]:
        """See :meth:`Admin.remove_members_from_consumer_group`; the result's
        futures are ``asyncio.Future``\\ s."""
        args = _conversions.remove_members_from_consumer_group_args(group_id, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_remove_members_from_consumer_group_async(
                c_admin, *args, cb),
            _conversions.free_remove_members_from_consumer_group)
        return _conversions.remove_members_from_consumer_group_result(options, payload, Async)

    async def alter_consumer_group_offsets(
        self, *, group_id: str, offsets: Mapping[TopicPartition, OffsetAndMetadata],
        options: AlterConsumerGroupOffsetsOptions | None = None
    ) -> AlterConsumerGroupOffsetsResult[Async]:
        """See :meth:`Admin.alter_consumer_group_offsets`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = AlterConsumerGroupOffsetsOptions()
        args = _conversions.alter_consumer_group_offsets_args(group_id, offsets, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_alter_consumer_group_offsets_async(
                c_admin, *args, cb),
            _conversions.free_alter_consumer_group_offsets)
        return _conversions.alter_consumer_group_offsets_result(payload, Async)

    async def list_offsets(self, *, topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
                           options: ListOffsetsOptions | None = None) -> ListOffsetsResult[Async]:
        """See :meth:`Admin.list_offsets`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = ListOffsetsOptions()
        partitions = list(topic_partition_offsets)
        args = _conversions.list_offsets_args(topic_partition_offsets, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_list_offsets_async(c_admin, *args, cb),
            _conversions.free_list_offsets)
        return _conversions.list_offsets_result(partitions, payload, Async)

    async def describe_client_quotas(self, *, filter: ClientQuotaFilter,  # noqa: A002 - Java's name
                                     options: DescribeClientQuotasOptions | None = None
                                     ) -> DescribeClientQuotasResult[Async]:
        """See :meth:`Admin.describe_client_quotas`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = DescribeClientQuotasOptions()
        args = _conversions.describe_client_quotas_args(filter, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_client_quotas_async(c_admin, *args, cb),
            _conversions.free_describe_client_quotas)
        return _conversions.describe_client_quotas_result(payload, Async)

    async def alter_client_quotas(self, *, entries: Iterable[ClientQuotaAlteration],
                                  options: AlterClientQuotasOptions | None = None
                                  ) -> AlterClientQuotasResult[Async]:
        """See :meth:`Admin.alter_client_quotas`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = AlterClientQuotasOptions()
        alterations = list(entries)
        args = _conversions.alter_client_quotas_args(alterations, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_alter_client_quotas_async(c_admin, *args, cb),
            _conversions.free_alter_client_quotas)
        return _conversions.alter_client_quotas_result(alterations, payload, Async)

    @overload
    async def describe_user_scram_credentials(self, *, users: Sequence[str] | None = None
                                              ) -> DescribeUserScramCredentialsResult[Async]: ...
    @overload
    async def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None,
            options: DescribeUserScramCredentialsOptions
    ) -> DescribeUserScramCredentialsResult[Async]: ...

    @java_forms(Form(), Form("users"), Form("users", "options"))
    async def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None = UNSET,
            options: DescribeUserScramCredentialsOptions | None = None
    ) -> DescribeUserScramCredentialsResult[Async]:
        """See :meth:`Admin.describe_user_scram_credentials`; the result's futures
        are ``asyncio.Future``\\ s."""
        if users is UNSET:
            users = None
        if options is None:
            options = DescribeUserScramCredentialsOptions()
        args = _conversions.describe_user_scram_credentials_args(users, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_user_scram_credentials_async(
                c_admin, *args, cb),
            _conversions.free_describe_user_scram_credentials)
        return _conversions.describe_user_scram_credentials_result(payload, Async)

    async def alter_user_scram_credentials(
            self, *, alterations: Sequence[UserScramCredentialAlteration],
            options: AlterUserScramCredentialsOptions | None = None
    ) -> AlterUserScramCredentialsResult[Async]:
        """See :meth:`Admin.alter_user_scram_credentials`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = AlterUserScramCredentialsOptions()
        rows = list(alterations)
        args = _conversions.alter_user_scram_credentials_args(rows, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_alter_user_scram_credentials_async(
                c_admin, *args, cb),
            _conversions.free_alter_user_scram_credentials)
        return _conversions.alter_user_scram_credentials_result(rows, payload, Async)

    async def describe_features(self, *, options: DescribeFeaturesOptions | None = None
                                ) -> DescribeFeaturesResult[Async]:
        """See :meth:`Admin.describe_features`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = DescribeFeaturesOptions()
        args = _conversions.describe_features_args(options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_features_async(c_admin, *args, cb),
            _conversions.free_describe_features)
        return _conversions.describe_features_result(payload, Async)

    async def update_features(self, *, feature_updates: Mapping[str, FeatureUpdate],
                              options: UpdateFeaturesOptions | None = None
                              ) -> UpdateFeaturesResult[Async]:
        """See :meth:`Admin.update_features`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = UpdateFeaturesOptions()
        updates = dict(feature_updates)
        args = _conversions.update_features_args(updates, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_update_features_async(c_admin, *args, cb),
            _conversions.free_update_features)
        return _conversions.update_features_result(updates, payload, Async)

    async def describe_producers(self, *, partitions: Iterable[TopicPartition],
                                 options: DescribeProducersOptions | None = None
                                 ) -> DescribeProducersResult[Async]:
        """See :meth:`Admin.describe_producers`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DescribeProducersOptions()
        topic_partitions = list(partitions)
        args = _conversions.describe_producers_args(topic_partitions, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_producers_async(c_admin, *args, cb),
            _conversions.free_describe_producers)
        return _conversions.describe_producers_result(topic_partitions, payload, Async)

    async def describe_transactions(self, *, transactional_ids: Iterable[str],
                                    options: DescribeTransactionsOptions | None = None
                                    ) -> DescribeTransactionsResult[Async]:
        """See :meth:`Admin.describe_transactions`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DescribeTransactionsOptions()
        ids = list(transactional_ids)
        args = _conversions.describe_transactions_args(ids, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_transactions_async(c_admin, *args, cb),
            _conversions.free_describe_transactions)
        return _conversions.describe_transactions_result(ids, payload, Async)

    async def abort_transaction(self, *, spec: AbortTransactionSpec,
                                options: AbortTransactionOptions | None = None
                                ) -> AbortTransactionResult[Async]:
        """See :meth:`Admin.abort_transaction`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = AbortTransactionOptions()
        args = _conversions.abort_transaction_args(spec, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_abort_transaction_async(c_admin, *args, cb),
            _conversions.free_abort_transaction)
        return _conversions.abort_transaction_result(spec, payload, Async)

    async def list_transactions(self, *, options: ListTransactionsOptions | None = None
                                ) -> ListTransactionsResult[Async]:
        """See :meth:`Admin.list_transactions`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = ListTransactionsOptions()
        args = _conversions.list_transactions_args(options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_list_transactions_async(c_admin, *args, cb),
            _conversions.free_list_transactions)
        return _conversions.list_transactions_result(payload, Async)

    async def fence_producers(self, *, transactional_ids: Iterable[str],
                              options: FenceProducersOptions | None = None
                              ) -> FenceProducersResult[Async]:
        """See :meth:`Admin.fence_producers`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = FenceProducersOptions()
        ids = list(transactional_ids)
        args = _conversions.fence_producers_args(ids, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_fence_producers_async(c_admin, *args, cb),
            _conversions.free_fence_producers)
        return _conversions.fence_producers_result(ids, payload, Async)

    @overload
    async def list_config_resources(self, *, config_resource_types: Iterable[ConfigResource.Type],
                                    options: ListConfigResourcesOptions
                                    ) -> ListConfigResourcesResult[Async]: ...
    @overload
    async def list_config_resources(self) -> ListConfigResourcesResult[Async]: ...

    @java_forms(Form("config_resource_types", "options",
                     defaults={"config_resource_types": (), "options": None}),
                Form())
    async def list_config_resources(
            self, *, config_resource_types: Iterable[ConfigResource.Type] = UNSET,
            options: ListConfigResourcesOptions | None = None
            ) -> ListConfigResourcesResult[Async]:
        """See :meth:`Admin.list_config_resources`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = ListConfigResourcesOptions()
        args = _conversions.list_config_resources_args(config_resource_types, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_list_config_resources_async(c_admin, *args, cb),
            _conversions.free_list_config_resources)
        return _conversions.list_config_resources_result(payload, Async)

    async def describe_classic_groups(self, *, group_ids: Iterable[str],
                                      options: DescribeClassicGroupsOptions | None = None
                                      ) -> DescribeClassicGroupsResult[Async]:
        """See :meth:`Admin.describe_classic_groups`; the result's futures are
        ``asyncio.Future``\\ s."""
        if options is None:
            options = DescribeClassicGroupsOptions()
        ids = list(group_ids)
        args = _conversions.describe_classic_groups_args(ids, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_describe_classic_groups_async(c_admin, *args, cb),
            _conversions.free_describe_classic_groups)
        return _conversions.describe_classic_groups_result(ids, payload, Async)

    async def force_terminate_transaction(self, *, transactional_id: str,
                                          options: TerminateTransactionOptions | None = None
                                          ) -> TerminateTransactionResult[Async]:
        """See :meth:`Admin.force_terminate_transaction`; the result's future is an
        ``asyncio.Future``."""
        if options is None:
            options = TerminateTransactionOptions()
        args = _conversions.force_terminate_transaction_args(transactional_id, options)
        payload = await self._await_for(
            lambda c_admin, cb: _lib.Admin_force_terminate_transaction_async(
                c_admin, *args, cb),
            _conversions.free_force_terminate_transaction)
        return _conversions.force_terminate_transaction_result(payload, Async)
