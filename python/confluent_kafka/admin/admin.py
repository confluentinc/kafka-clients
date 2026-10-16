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

"""``Admin``: Java's ``org.apache.kafka.clients.admin.Admin``.

A Java client interface, so a non-instantiable base class with all its methods,
which call the admin FFI (CLAUDE.md, Python Binding Conventions, Class family);
``KafkaAdminClient`` adds nothing but its private construction and
``MockAdminClient`` overrides every method in pure Python. Java's
``AutoCloseable``: a context manager whose exit calls ``close()``.

Every RPC is a plain ``def`` that returns its ``*Result`` at once, as Java's do:
the extension's ``Admin_<rpc>`` builds the C options from the setters the user ran
(``AbstractOptions._setters_ran``), calls ``kafka_admin_Admin_<rpc>_with_options``
and hands back the C result, whose per-key futures become
:class:`~confluent_kafka.common.KafkaFuture`\\ s (Futures). A future completes
later, on the client's one daemon pump thread, which runs the client's queued
callbacks (``kafka_admin_Admin__execute_callbacks``) whenever the C API's notify
hook signals it (Threads and callbacks): the ``then_apply`` functions and
``when_complete`` actions of the futures run there, so one that blocks blocks the
client's callbacks, as in Java; reading a sibling future that is already done
(``get()``, ``get_now()``) is safe there, but waiting there on a pending one never
ends. The client is thread-safe; ``close()`` frees the native handle only after
every call using it has returned. After ``close()`` every call raises
``IllegalStateError("Cannot accept new calls when AdminClient is closing.")``,
Java's own message for a call on a closing client.

Members are in Java's declaration order.

Not generated, for want of an entry point (the name looked for is
``kafka_admin_Admin_<name>``, with ``_with_options`` for the form with options):
``listStreamsGroupOffsets`` (both overloads, ``list_streams_group_offsets``),
``deleteStreamsGroups`` (``delete_streams_groups``), ``deleteStreamsGroupOffsets``
(``delete_streams_group_offsets``), ``alterStreamsGroupOffsets``
(``alter_streams_group_offsets``), ``describeMetadataQuorum``
(``describe_metadata_quorum``), ``unregisterBroker`` (``unregister_broker``),
``clientInstanceId(Duration)`` (``client_instance_id``), ``addRaftVoter``
(``add_raft_voter``), ``removeRaftVoter`` (``remove_raft_voter``),
``describeShareGroups`` (``describe_share_groups``), ``alterShareGroupOffsets``
(``alter_share_group_offsets``), ``listShareGroupOffsets``
(``list_share_group_offsets``), ``deleteShareGroupOffsets``
(``delete_share_group_offsets``), ``deleteShareGroups``
(``delete_share_groups``), ``describeStreamsGroups``
(``describe_streams_groups``), ``registerMetricForSubscription(KafkaMetric)``
(``register_metric_for_subscription``),
``unregisterMetricFromSubscription(KafkaMetric)``
(``unregister_metric_from_subscription``) and ``metrics()`` (``metrics``).
Deprecated, so not generated: ``listConsumerGroups`` and
``listClientMetricsResources``.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka._config import duration_to_ms
from confluent_kafka._errors import from_ffi_error
from confluent_kafka._wait import WAIT_SLICE_S
from confluent_kafka.admin import _conversions
from confluent_kafka.admin._futures import Sync
from confluent_kafka.admin.abort_transaction_options import AbortTransactionOptions
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
from confluent_kafka.illegal_state_error import IllegalStateError

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

__all__ = ["Admin"]

_LOG = logging.getLogger("confluent_kafka.admin")

#: Java's message for a call on a closing ``KafkaAdminClient``
#: (``KafkaAdminClient.java:1601``).
CLOSED_MESSAGE = "Cannot accept new calls when AdminClient is closing."

#: ``close()``'s timeout: Java's ``Duration.ofMillis(Long.MAX_VALUE)`` crosses as -1.
_CLOSE_FOREVER_MS = -1

#: Java's admin thread name prefix (``KafkaAdminClient.NETWORK_THREAD_PREFIX``).
_PUMP_THREAD_NAME = "kafka-admin-client-thread"

_Payload = tuple[Any, ...]
_Submit = Callable[[int, Callable[..., None]], None]
_Free = Callable[[_Payload], None]


class _AdminState:
    """The native admin handle, the closed flag and the uses in flight, shared by
    ``Admin`` and ``AsyncAdmin``.

    The handle lives until the teardown frees it. Every RPC is one *use* of it,
    counted under ``_lifecycle`` around its extension call, which returns at once
    with the call's C result; a use cannot start once ``_closed`` is set. A run of
    the client's queued callbacks is a use too (:meth:`_begin_pump`), which can
    start until the teardown begins, so the close's own completion still runs.
    ``close()`` sets ``_closed`` under the same lock, so exactly one ``close()``
    tears the client down; the teardown waits until no use is left, then frees the
    handle (``Admin_destroy``), which fires the ``get_cb`` deliveries still pending
    with ``LOCAL_ILLEGAL_STATE``.
    """

    def __init__(self) -> None:
        self._c_admin: int = 0
        self._closed = False
        self._tearing_down = False
        self._lifecycle = threading.Condition()
        self._uses = 0

    def _start(self, configs: dict[str, Any]) -> None:
        """Java's ``KafkaAdminClient.createInternal(new AdminClientConfig(configs,
        true), null)``: parse ``configs`` (Configuration) and build the native
        client; a construction error raises the core's error. The client's notify
        hook (:meth:`_notify_hook`) is set before any call."""
        from confluent_kafka._config import prepare

        _user_configs, native = prepare(configs, client="admin")
        c_admin, error = _lib.Admin_AdminClient_new(native)
        if error:
            raise from_ffi_error(error)
        self._c_admin = c_admin
        _lib.Admin__set_callbacks_notify(c_admin, self._notify_hook())

    def _notify_hook(self) -> Callable[[], None]:
        """The callable the C API's notify hook fires (from a Rust task, the GIL
        held) when the client's callback queue turns non-empty: it only
        schedules the pump."""
        raise NotImplementedError

    def _begin_use(self) -> int:
        """Start a use of the handle and return it; ``IllegalStateError`` with
        Java's message once ``close()`` has begun."""
        with self._lifecycle:
            if self._closed:
                raise IllegalStateError(message=CLOSED_MESSAGE)
            self._uses += 1
            return self._c_admin

    def _begin_pump(self) -> bool:
        """Start a run of the client's queued callbacks: ``False`` once the
        teardown has begun (it runs what is left itself)."""
        with self._lifecycle:
            if self._tearing_down or not self._c_admin:
                return False
            self._uses += 1
            return True

    def _end_use(self) -> None:
        with self._lifecycle:
            self._uses -= 1
            if self._uses == 0:
                self._lifecycle.notify_all()

    def _check_not_closed(self) -> None:
        if self._closed:
            raise IllegalStateError(message=CLOSED_MESSAGE)

    def _begin_close(self) -> bool:
        """Set ``_closed``; whether this call did (and so must tear down)."""
        with self._lifecycle:
            if self._closed:
                return False
            self._closed = True
            return True

    def _teardown(self) -> None:
        """Wait until no use is left (none can start any more), then free the
        handle, which runs the callbacks still pending on this thread."""
        with self._lifecycle:
            self._tearing_down = True
            while self._uses:
                self._lifecycle.wait()
            c_admin = self._c_admin
        if c_admin:
            _lib.Admin_destroy(c_admin)

    def _call(self, entry: Callable[..., Any], *args: Any) -> Any:
        """One RPC: ``entry(handle, *args)``, an ``Admin_<rpc>`` of the extension,
        which returns the C result at once (Java's RPCs return at once)."""
        c_admin = self._begin_use()
        try:
            return entry(c_admin, *args)
        finally:
            self._end_use()

    # ---- the previous FFI's whole-result callback --------------------------------
    # The RPCs not yet on the extension's ``AdminResult`` still submit an
    # ``_async`` entry point through these; they go once every RPC uses ``_call``.

    def _submit(self, submit: _Submit, cb: Callable[..., None]) -> None:
        c_admin = self._begin_use()
        try:
            submit(c_admin, cb)
        except BaseException:
            self._end_use()
            raise

    def _wait_for(self, submit: _Submit, free: _Free) -> _Payload:
        box: dict[str, _Payload] = {}
        done = threading.Event()
        lock = threading.Lock()
        abandoned = False

        def cb(*payload: Any) -> None:
            try:
                with lock:
                    if abandoned:
                        free(payload)
                        return
                    box["payload"] = payload
                done.set()
            finally:
                self._end_use()

        self._submit(submit, cb)
        try:
            done.wait()
        except BaseException:
            with lock:
                abandoned = True
                unclaimed = box.pop("payload", None)
            if unclaimed is not None:
                free(unclaimed)
            raise
        return box["payload"]



class _Pump:
    """The sync client's one daemon pump thread, Java's admin thread for the
    callbacks: it waits for the notify hook and runs the client's queued
    callbacks (``Admin__execute_callbacks``) each time it fires (Threads and
    callbacks). The ``then_apply`` functions and ``when_complete`` actions of the
    client's futures run here; one that waits for a pending future of this client
    never ends, as its delivery needs this thread. At ``close()`` it stops after
    its current run."""

    __slots__ = ("_state", "_event", "_stopping", "_after", "_thread")

    def __init__(self, state: _AdminState) -> None:
        self._state = state
        self._event = threading.Event()
        self._stopping = False
        self._after: Callable[[], None] | None = None
        self._thread = threading.Thread(target=self._run, name=_PUMP_THREAD_NAME, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def notify(self) -> None:
        """The notify hook: only signals the thread."""
        self._event.set()

    def is_current(self) -> bool:
        """Whether this is the pump thread (a callback calling into the client)."""
        return threading.current_thread() is self._thread

    def stop(self, after: Callable[[], None] | None = None) -> None:
        """Stop after the current run, then run ``after`` on the pump thread."""
        self._after = after
        self._stopping = True
        self._event.set()

    def join(self) -> None:
        if self._thread.is_alive() and not self.is_current():
            self._thread.join()

    def _run(self) -> None:
        state = self._state
        while True:
            self._event.wait()
            self._event.clear()
            if self._stopping:
                break
            if not state._begin_pump():
                break
            try:
                _lib.Admin__execute_callbacks(state._c_admin)
            except Exception:  # noqa: BLE001 - the pump thread has no caller
                _LOG.exception("Error running the admin client's callbacks")
            finally:
                state._end_use()
        after = self._after
        if after is not None:
            after()


def close_timeout_ms(timeout: Duration | None) -> int:
    """``close(Duration)``'s timeout in milliseconds (``-1`` for ``close()``); a
    negative one raises ``IllegalArgumentError("The timeout cannot be
    negative.")``, as ``KafkaAdminClient.close(Duration)`` does."""
    return _CLOSE_FOREVER_MS if timeout is None else duration_to_ms(timeout, default_ms=0)


class Admin(_AdminState):
    """The administrative client for Kafka, which supports managing and
    inspecting topics, brokers, configurations and ACLs.

    Instances returned from the ``create`` methods of this interface are
    guaranteed to be thread safe.

    The operations exposed by ``Admin`` follow a consistent pattern: an
    ``Admin`` instance is created with ``Admin.create(configs=…)``; each operation
    takes the arguments it needs and an optional ``*Options`` object, and returns
    a ``*Result`` at once, whose ``KafkaFuture``\\ s give the outcome per key when
    the call completes.

    The minimum broker version required is 0.10.0.0. Methods with stricter
    requirements will specify the minimum broker version required.

    Java: ``org.apache.kafka.clients.admin.Admin`` (``AutoCloseable``: a context
    manager whose exit calls ``close()``).
    """

    def __init__(self) -> None:
        if type(self) is Admin:
            raise TypeError(
                "Admin is a non-instantiable base; use Admin.create or MockAdminClient")
        _AdminState.__init__(self)
        self._pump: _Pump | None = None

    def _notify_hook(self) -> Callable[[], None]:
        pump = _Pump(self)
        self._pump = pump
        pump.start()
        return pump.notify

    @staticmethod
    def create(*, configs: dict[str, Any]) -> Admin:
        """Create a new Admin with the given configuration.

        ``configs``: the configuration *(deviation: Java names it ``props`` /
        ``conf``)*. Returns the new ``KafkaAdminClient``.
        """
        from confluent_kafka.admin.kafka_admin_client import KafkaAdminClient

        return KafkaAdminClient._create_internal(configs=configs)

    def close(self, *, timeout: Duration | None = None) -> None:
        """Close the Admin client and release all associated resources.

        The close operation has a grace period during which current operations
        will be allowed to complete, specified by the given duration (``close()``
        waits without limit, Java's ``Duration.ofMillis(Long.MAX_VALUE)``). New
        operations will not be accepted during the grace period. Once the grace
        period is over, all operations that have not yet been completed will be
        aborted with a ``TimeoutError``.

        Closing twice is harmless. A negative ``timeout`` raises
        ``IllegalArgumentError``, except that a second ``close()`` returns before
        checking it *(deviation)*. The wait is in short slices: Ctrl+C re-raises at
        once, the close going on in the core, and the handle is freed when it ends.
        Called from a callback on the client's pump thread, it does not wait, as
        Java's ``close`` does not join its own thread: the pump thread frees the
        handle once the close completed.
        """
        if self._closed:
            return
        timeout_ms = close_timeout_ms(timeout)
        # Check and set at once: exactly one close() tears the client down.
        if not self._begin_close():
            return
        closed = threading.Event()
        _lib.Admin_close_cb(self._c_admin, timeout_ms, closed.set)
        pump = self._pump
        if pump is not None and pump.is_current():
            pump.stop(after=self._teardown)
            return
        try:
            while not closed.wait(WAIT_SLICE_S):
                pass
        except BaseException:
            threading.Thread(target=self._finish_close, args=(closed,),
                             name="kafka-admin-client-close", daemon=True).start()
            raise
        self._finish_close(closed)

    def _finish_close(self, closed: threading.Event) -> None:
        """Once the core's close completed: stop the pump thread after its current
        run, then free the handle (:meth:`_AdminState._teardown`)."""
        closed.wait()
        pump = self._pump
        if pump is not None:
            pump.stop()
            pump.join()
        self._teardown()

    def __enter__(self) -> Admin:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions | None = None) -> CreateTopicsResult[Sync]:
        """Create a batch of new topics.

        This operation is not transactional so it may succeed for some topics while
        fail for others.

        It may take several seconds after ``CreateTopicsResult`` returns success for
        all the brokers to become aware that the topics have been created. During
        this time, ``list_topics()`` and ``describe_topics()`` may not return
        information about the new topics.

        This operation is supported by brokers with version 0.10.1.0 or higher. The
        ``validate_only`` option is supported from version 0.10.2.0.

        ``new_topics``: the new topics to create; ``options``: the options to use
        when creating the new topics (Java's ``createTopics(newTopics)`` passes
        ``new CreateTopicsOptions()``). Returns the ``CreateTopicsResult``.
        """
        if options is None:
            options = CreateTopicsOptions()
        native = self._call(_lib.Admin_create_topics,
                            *_conversions.create_topics_args(new_topics),
                            options._setters_ran())
        return _conversions.create_topics_result(native, Sync, None)

    @overload
    def delete_topics(self, *, topics: Iterable[str],
                      options: DeleteTopicsOptions | None = None) -> DeleteTopicsResult[Sync]: ...
    @overload
    def delete_topics(self, *, topics: TopicCollection,
                      options: DeleteTopicsOptions | None = None) -> DeleteTopicsResult[Sync]: ...

    def delete_topics(self, *, topics: Iterable[str] | TopicCollection,
                      options: DeleteTopicsOptions | None = None) -> DeleteTopicsResult[Sync]:
        """Delete a batch of topics.

        This operation is not transactional so it may succeed for some topics while
        fail for others.

        It may take several seconds after the ``DeleteTopicsResult`` returns success
        for all the brokers to become aware that the topics are gone. During this
        time, ``list_topics()`` and ``describe_topics()`` may continue to return
        information about the deleted topics.

        If delete.topic.enable is set to false on the brokers, an exception will be
        returned to the client indicating that topic deletion is disabled.

        When using topic IDs, this operation is supported by brokers with
        inter-broker protocol 2.8 or higher. When using topic names, this operation
        is supported by brokers with version 0.10.1.0 or higher.

        ``topics``: the topics to delete, a collection of names (Java's
        ``deleteTopics(Collection<String>)``, which passes
        ``TopicCollection.ofTopicNames(topics)``) or a ``TopicCollection``;
        ``options``: the options to use when deleting the topics (Java's short forms
        pass ``new DeleteTopicsOptions()``). Returns the ``DeleteTopicsResult``, every
        future of which is complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DeleteTopicsOptions()
        self._check_not_closed()
        by_ids, keys = _conversions.topic_collection_target(topics, "deleteTopics")
        args = _conversions.delete_topics_args(by_ids, keys, options)
        submit = (_lib.Admin_delete_topics_by_ids_async if by_ids
                  else _lib.Admin_delete_topics_async)
        payload = self._wait_for(lambda c_admin, cb: submit(c_admin, *args, cb),
                                 _conversions.free_delete_topics)
        return _conversions.delete_topics_result(by_ids, keys, payload, Sync)

    def list_topics(self, *, options: ListTopicsOptions | None = None) -> ListTopicsResult[Sync]:
        """List the topics available in the cluster.

        ``options``: the options to use when listing the topics (Java's
        ``listTopics()`` passes ``new ListTopicsOptions()``). Returns the
        ``ListTopicsResult``.
        """
        if options is None:
            options = ListTopicsOptions()
        native = self._call(_lib.Admin_list_topics, options._setters_ran())
        return _conversions.list_topics_result(native, Sync, None)

    @overload
    def describe_topics(self, *, topic_names: Iterable[str],
                        options: DescribeTopicsOptions | None = None
                        ) -> DescribeTopicsResult[Sync]: ...
    @overload
    def describe_topics(self, *, topics: TopicCollection,
                        options: DescribeTopicsOptions | None = None
                        ) -> DescribeTopicsResult[Sync]: ...

    @java_forms(Form("topic_names"), Form("topic_names", "options"),
                Form("topics"), Form("topics", "options"))
    def describe_topics(self, *, topic_names: Iterable[str] | None = None,
                        topics: TopicCollection | None = None,
                        options: DescribeTopicsOptions | None = None
                        ) -> DescribeTopicsResult[Sync]:
        """Describe some topics in the cluster.

        When using topic IDs, this operation is supported by brokers with version
        3.1.0 or higher.

        ``topic_names``: the names of the topics to describe (Java's
        ``describeTopics(Collection<String>)``, which passes
        ``TopicCollection.ofTopicNames(topicNames)``), or ``topics``: the topics to
        describe; ``options``: the options to use when describing the topics (Java's
        short forms pass ``new DescribeTopicsOptions()``). Returns the
        ``DescribeTopicsResult``, every future of which is complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = DescribeTopicsOptions()
        self._check_not_closed()
        by_ids, keys = _conversions.topic_collection_target(
            topic_names if topics is None else topics, "describeTopics")
        args = _conversions.describe_topics_args(by_ids, keys, options)
        submit = (_lib.Admin_describe_topics_by_ids_async if by_ids
                  else _lib.Admin_describe_topics_async)
        payload = self._wait_for(lambda c_admin, cb: submit(c_admin, *args, cb),
                                 _conversions.free_describe_topics)
        return _conversions.describe_topics_result(by_ids, keys, payload, Sync)

    def describe_cluster(self, *, options: DescribeClusterOptions | None = None
                         ) -> DescribeClusterResult[Sync]:
        """Get information about the nodes in the cluster.

        To obtain broker cluster information, you must configure
        ``bootstrap.servers``. To obtain controller cluster information, you must
        configure ``bootstrap.controllers``.

        ``options``: the options to use when getting information about the cluster
        (Java's ``describeCluster()`` passes ``new DescribeClusterOptions()``).
        Returns the ``DescribeClusterResult``, every future of which is complete
        *(deviation: see ``create_topics``)*. Its nodes report ``is_fenced()`` as
        ``False`` *(deviation: the FFI's nodes carry no fenced flag)*.
        """
        if options is None:
            options = DescribeClusterOptions()
        args = _conversions.describe_cluster_args(options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_cluster_async(c_admin, *args, cb),
            _conversions.free_describe_cluster)
        return _conversions.describe_cluster_result(payload, Sync)

    def describe_acls(self, *, filter: AclBindingFilter,  # noqa: A002 - Java's name
                      options: DescribeAclsOptions | None = None) -> DescribeAclsResult[Sync]:
        """Lists access control lists (ACLs) according to the supplied filter.

        Note: it may take some time for changes made by ``create_acls`` or
        ``delete_acls`` to be reflected in the output of ``describe_acls``.

        This operation is supported by brokers with version 0.11.0.0 or higher.

        ``filter``: the filter to use; ``options``: the options to use when listing the
        ACLs (Java's ``describeAcls(filter)`` passes ``new DescribeAclsOptions()``).
        Returns the ``DescribeAclsResult``, its future complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = DescribeAclsOptions()
        args = _conversions.describe_acls_args(filter, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_acls_async(c_admin, *args, cb),
            _conversions.free_describe_acls)
        return _conversions.describe_acls_result(payload, Sync)

    def create_acls(self, *, acls: Iterable[AclBinding],
                    options: CreateAclsOptions | None = None) -> CreateAclsResult[Sync]:
        """Creates access control lists (ACLs) which are bound to specific resources.

        This operation is not transactional so it may succeed for some ACLs while fail
        for others.

        If you attempt to add an ACL that duplicates an existing ACL, no error will be
        raised, but no changes will be made.

        This operation is supported by brokers with version 0.11.0.0 or higher.

        ``acls``: the ACLs to create; ``options``: the options to use when creating the
        ACLs (Java's ``createAcls(acls)`` passes ``new CreateAclsOptions()``). Returns
        the ``CreateAclsResult``, every future of which is complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = CreateAclsOptions()
        bindings = list(acls)
        args = _conversions.create_acls_args(bindings, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_create_acls_async(c_admin, *args, cb),
            _conversions.free_create_acls)
        return _conversions.create_acls_result(bindings, payload, Sync)

    def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                    options: DeleteAclsOptions | None = None) -> DeleteAclsResult[Sync]:
        """Deletes access control lists (ACLs) according to the supplied filters.

        This operation is not transactional so it may succeed for some ACLs while fail
        for others.

        This operation is supported by brokers with version 0.11.0.0 or higher.

        ``filters``: the filters to use; ``options``: the options to use when deleting
        the ACLs (Java's ``deleteAcls(filters)`` passes ``new DeleteAclsOptions()``).
        Returns the ``DeleteAclsResult``, every future of which is complete *(deviation:
        see ``create_topics``)*.
        """
        if options is None:
            options = DeleteAclsOptions()
        acl_filters = list(filters)
        args = _conversions.delete_acls_args(acl_filters, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_delete_acls_async(c_admin, *args, cb),
            _conversions.free_delete_acls)
        return _conversions.delete_acls_result(acl_filters, payload, Sync)

    def describe_configs(self, *, resources: Iterable[ConfigResource],
                         options: DescribeConfigsOptions | None = None
                         ) -> DescribeConfigsResult[Sync]:
        """Get the configuration for the specified resources.

        The returned configuration includes default values and the ``is_default()``
        method can be used to distinguish them from user supplied values.

        The value of config entries where ``is_sensitive()`` is true is always
        ``None`` so that sensitive information is not disclosed.

        Config entries where ``is_read_only()`` is true cannot be updated.

        The different behavior of nonexistent resource:

        - ``ConfigResource.Type.BROKER``: will raise a ``TimeoutError``
        - ``ConfigResource.Type.TOPIC``: will raise an ``UnknownTopicOrPartitionError``
        - ``ConfigResource.Type.GROUP``: just return default configs even if the
          target group is nonexistent
        - ``ConfigResource.Type.BROKER_LOGGER``: will raise a ``TimeoutError``
        - ``ConfigResource.Type.CLIENT_METRICS``: will return empty configs

        Note that you cannot describe broker configs or broker logger using
        ``bootstrap.controllers``, and you cannot describe controller configs or
        controller logger using ``bootstrap.servers``.

        This operation is supported by brokers with version 0.11.0.0 or higher.

        ``resources``: see the relevant ``ConfigResource.Type``; ``options``: the
        options to use when describing configs (Java's ``describeConfigs(resources)``
        passes ``new DescribeConfigsOptions()``). Returns the
        ``DescribeConfigsResult``, every future of which is complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = DescribeConfigsOptions()
        targets = list(resources)
        args = _conversions.describe_configs_args(targets, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_configs_async(c_admin, *args, cb),
            _conversions.free_describe_configs)
        return _conversions.describe_configs_result(targets, payload, Sync)

    def incremental_alter_configs(
            self, *, configs: Mapping[ConfigResource, Iterable[AlterConfigOp]],
            options: AlterConfigsOptions | None = None) -> AlterConfigsResult[Sync]:
        """Incrementally update the configuration for the specified resources.

        Updates are not transactional so they may succeed for some resources while
        fail for others. The configs for a particular resource are updated
        atomically.

        The following errors can be anticipated when calling ``result()`` on the
        futures obtained from the returned ``AlterConfigsResult``:

        - ``ClusterAuthorizationError`` if the authenticated user didn't have alter
          access to the cluster.
        - ``TopicAuthorizationError`` if the authenticated user didn't have alter
          access to the Topic.
        - ``UnknownTopicOrPartitionError`` if the Topic doesn't exist.
        - ``InvalidRequestError`` if the request details are invalid. e.g., a
          configuration key was specified more than once for a resource

        This operation is supported by brokers with version 2.3.0 or higher.

        ``configs``: the resources with their configs; ``options``: the options to
        use when altering configs (Java's ``incrementalAlterConfigs(configs)`` passes
        ``new AlterConfigsOptions()``). Returns the ``AlterConfigsResult``, every
        future of which is complete *(deviation: see ``create_topics``)*. On success,
        a resource given with no op gets no future *(deviation: the FFI takes one row
        per op, so the resource does not reach the core; Java sends it with no
        configs)*; on a whole-call error its future fails, as in Java.
        """
        if options is None:
            options = AlterConfigsOptions()
        resources = list(configs)
        args = _conversions.incremental_alter_configs_args(configs, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_incremental_alter_configs_async(c_admin, *args, cb),
            _conversions.free_incremental_alter_configs)
        return _conversions.incremental_alter_configs_result(resources, payload, Sync)

    def alter_replica_log_dirs(
            self, *, replica_assignment: Mapping[TopicPartitionReplica, str],
            options: AlterReplicaLogDirsOptions | None = None
            ) -> AlterReplicaLogDirsResult[Sync]:
        """Change the log directory for the specified replicas. If the replica does
        not exist on the broker, the result shows REPLICA_NOT_AVAILABLE for the given
        replica and the replica will be created in the given log directory on the
        broker when it is created later. If the replica already exists on the broker,
        the replica will be moved to the given log directory if it is not already
        there. For detailed result, inspect the returned ``AlterReplicaLogDirsResult``
        instance.

        This operation is not transactional so it may succeed for some replicas while
        fail for others.

        This operation is supported by brokers with version 1.1.0 or higher.

        ``replica_assignment``: the replicas with their log directory absolute path;
        ``options``: the options to use when changing replica dir (Java's
        ``alterReplicaLogDirs(replicaAssignment)`` passes
        ``new AlterReplicaLogDirsOptions()``). Returns the
        ``AlterReplicaLogDirsResult``, every future of which is complete *(deviation:
        see ``create_topics``)*.
        """
        if options is None:
            options = AlterReplicaLogDirsOptions()
        replicas = list(replica_assignment)
        args = _conversions.alter_replica_log_dirs_args(replica_assignment, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_alter_replica_log_dirs_async(c_admin, *args, cb),
            _conversions.free_alter_replica_log_dirs)
        return _conversions.alter_replica_log_dirs_result(replicas, payload, Sync)

    def describe_log_dirs(self, *, brokers: Iterable[int],
                          options: DescribeLogDirsOptions | None = None
                          ) -> DescribeLogDirsResult[Sync]:
        """Query the information of all log directories on the given set of brokers.

        This operation is supported by brokers with version 1.0.0 or higher.

        ``brokers``: a list of brokers; ``options``: the options to use when querying
        log dir info (Java's ``describeLogDirs(brokers)`` passes
        ``new DescribeLogDirsOptions()``). Returns the ``DescribeLogDirsResult``,
        every future of which is complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DescribeLogDirsOptions()
        broker_ids = list(brokers)
        args = _conversions.describe_log_dirs_args(broker_ids, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_log_dirs_async(c_admin, *args, cb),
            _conversions.free_describe_log_dirs)
        return _conversions.describe_log_dirs_result(broker_ids, payload, Sync)

    def describe_replica_log_dirs(
            self, *, replicas: Iterable[TopicPartitionReplica],
            options: DescribeReplicaLogDirsOptions | None = None
            ) -> DescribeReplicaLogDirsResult[Sync]:
        """Query the replica log directory information for the specified replicas.

        This operation is supported by brokers with version 1.0.0 or higher.

        ``replicas``: the replicas to query; ``options``: the options to use when
        querying replica log dir info (Java's ``describeReplicaLogDirs(replicas)``
        passes ``new DescribeReplicaLogDirsOptions()``). Returns the
        ``DescribeReplicaLogDirsResult``, every future of which is complete
        *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DescribeReplicaLogDirsOptions()
        targets = list(replicas)
        args = _conversions.describe_replica_log_dirs_args(targets, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_replica_log_dirs_async(c_admin, *args, cb),
            _conversions.free_describe_replica_log_dirs)
        return _conversions.describe_replica_log_dirs_result(targets, payload, Sync)

    def create_partitions(self, *, new_partitions: Mapping[str, NewPartitions],
                          options: CreatePartitionsOptions | None = None
                          ) -> CreatePartitionsResult[Sync]:
        """Increase the number of partitions of the topics given as the keys of
        ``new_partitions`` according to the corresponding values. **If partitions are
        increased for a topic that has a key, the partition logic or ordering of the
        messages will be affected.**

        This operation is not transactional so it may succeed for some topics while
        fail for others.

        It may take several seconds after this method returns success for all the
        brokers to become aware that the partitions have been created. During this
        time, ``describe_topics()`` may not return information about the new
        partitions.

        This operation is supported by brokers with version 1.0.0 or higher.

        The following errors can be anticipated when calling ``result()`` on the
        futures obtained from the ``values()`` method of the returned
        ``CreatePartitionsResult``:

        - ``AuthorizationError`` if the authenticated user is not authorized to alter
          the topic
        - ``TimeoutError`` if the request was not completed in within the given
          ``CreatePartitionsOptions.timeout_ms()``.
        - ``ReassignmentInProgressError`` if a partition reassignment is currently in
          progress
        - ``BrokerNotAvailableError`` if the requested ``NewPartitions.assignments()``
          contain a broker that is currently unavailable.
        - ``InvalidReplicationFactorError`` if no ``NewPartitions.assignments()`` are
          given and it is impossible for the broker to assign replicas with the topics
          replication factor.
        - Subclasses of ``KafkaError`` if the request is invalid in some way.
        - ``InvalidPartitionsError`` if the requested partition count is less than or
          equal to the current partition count.

        ``new_partitions``: the topics which should have new partitions created, and
        corresponding parameters for the created partitions; ``options``: the options
        to use when creating the new partitions (Java's
        ``createPartitions(newPartitions)`` passes ``new CreatePartitionsOptions()``).
        Returns the ``CreatePartitionsResult``, every future of which is complete
        *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = CreatePartitionsOptions()
        topics = list(new_partitions)
        args = _conversions.create_partitions_args(new_partitions, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_create_partitions_async(c_admin, *args, cb),
            _conversions.free_create_partitions)
        return _conversions.create_partitions_result(topics, payload, Sync)

    def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                       options: DeleteRecordsOptions | None = None
                       ) -> DeleteRecordsResult[Sync]:
        """Delete records whose offset is smaller than the given offset of the
        corresponding partition.

        This operation is supported by brokers with version 0.11.0.0 or higher.

        ``records_to_delete``: the topic partitions and related offsets from which
        records deletion starts; ``options``: the options to use when deleting records
        (Java's ``deleteRecords(recordsToDelete)`` passes
        ``new DeleteRecordsOptions()``). Returns the ``DeleteRecordsResult``, every
        future of which is complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DeleteRecordsOptions()
        partitions = list(records_to_delete)
        args = _conversions.delete_records_args(records_to_delete, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_delete_records_async(c_admin, *args, cb),
            _conversions.free_delete_records)
        return _conversions.delete_records_result(partitions, payload, Sync)

    def create_delegation_token(self, *, options: CreateDelegationTokenOptions | None = None
                                ) -> CreateDelegationTokenResult[Sync]:
        """Create a Delegation Token.

        This operation is supported by brokers with version 1.1.0 or higher.

        The following errors can be anticipated when calling ``result()`` on the future
        obtained from the ``delegation_token()`` method of the returned
        ``CreateDelegationTokenResult``:

        - ``UnsupportedByAuthenticationError``: if the request sent on PLAINTEXT/1-way
          SSL channels or delegation token authenticated channels.
        - ``InvalidPrincipalTypeError``: if the renewers principal type is not
          supported.
        - ``DelegationTokenDisabledError``: if the delegation token feature is disabled.
        - ``TimeoutError``: if the request was not completed in within the given
          ``CreateDelegationTokenOptions.timeout_ms()``.

        ``options``: the options to use when creating delegation token (Java's
        ``createDelegationToken()`` passes ``new CreateDelegationTokenOptions()``).
        Returns the ``CreateDelegationTokenResult``, its future complete *(deviation:
        see ``create_topics``)*.
        """
        if options is None:
            options = CreateDelegationTokenOptions()
        args = _conversions.create_delegation_token_args(options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_create_delegation_token_async(c_admin, *args, cb),
            _conversions.free_create_delegation_token)
        return _conversions.create_delegation_token_result(payload, Sync)

    def renew_delegation_token(self, *, hmac: bytes,
                               options: RenewDelegationTokenOptions | None = None
                               ) -> RenewDelegationTokenResult[Sync]:
        """Renew a Delegation Token.

        This operation is supported by brokers with version 1.1.0 or higher.

        The following errors can be anticipated when calling ``result()`` on the future
        obtained from the ``expiry_timestamp()`` method of the returned
        ``RenewDelegationTokenResult``:

        - ``UnsupportedByAuthenticationError``: if the request sent on PLAINTEXT/1-way
          SSL channels or delegation token authenticated channels.
        - ``DelegationTokenDisabledError``: if the delegation token feature is disabled.
        - ``DelegationTokenNotFoundError``: if the delegation token is not found on
          server.
        - ``DelegationTokenOwnerMismatchError``: if the authenticated user is not
          owner/renewer of the token.
        - ``DelegationTokenExpiredError``: if the delegation token is expired.
        - ``TimeoutError``: if the request was not completed in within the given
          ``RenewDelegationTokenOptions.timeout_ms()``.

        ``hmac``: HMAC of the Delegation token; ``options``: the options to use when
        renewing delegation token (Java's ``renewDelegationToken(hmac)`` passes ``new
        RenewDelegationTokenOptions()``). Returns the ``RenewDelegationTokenResult``,
        its future complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = RenewDelegationTokenOptions()
        args = _conversions.renew_delegation_token_args(hmac, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_renew_delegation_token_async(c_admin, *args, cb),
            _conversions.free_renew_delegation_token)
        return _conversions.renew_delegation_token_result(payload, Sync)

    def expire_delegation_token(self, *, hmac: bytes,
                                options: ExpireDelegationTokenOptions | None = None
                                ) -> ExpireDelegationTokenResult[Sync]:
        """Expire a Delegation Token.

        This operation is supported by brokers with version 1.1.0 or higher. Without
        ``options`` it expires the token immediately.

        The following errors can be anticipated when calling ``result()`` on the future
        obtained from the ``expiry_timestamp()`` method of the returned
        ``ExpireDelegationTokenResult``:

        - ``UnsupportedByAuthenticationError``: if the request sent on PLAINTEXT/1-way
          SSL channels or delegation token authenticated channels.
        - ``DelegationTokenDisabledError``: if the delegation token feature is disabled.
        - ``DelegationTokenNotFoundError``: if the delegation token is not found on
          server.
        - ``DelegationTokenOwnerMismatchError``: if the authenticated user is not
          owner/renewer of the requested token.
        - ``DelegationTokenExpiredError``: if the delegation token is expired.
        - ``TimeoutError``: if the request was not completed in within the given
          ``ExpireDelegationTokenOptions.timeout_ms()``.

        ``hmac``: HMAC of the Delegation token; ``options``: the options to use when
        expiring delegation token (Java's ``expireDelegationToken(hmac)`` passes ``new
        ExpireDelegationTokenOptions()``). Returns the ``ExpireDelegationTokenResult``,
        its future complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = ExpireDelegationTokenOptions()
        args = _conversions.expire_delegation_token_args(hmac, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_expire_delegation_token_async(c_admin, *args, cb),
            _conversions.free_expire_delegation_token)
        return _conversions.expire_delegation_token_result(payload, Sync)

    def describe_delegation_token(self, *,
                                  options: DescribeDelegationTokenOptions | None = None
                                  ) -> DescribeDelegationTokenResult[Sync]:
        """Describe the Delegation Tokens.

        This operation is supported by brokers with version 1.1.0 or higher. Without
        ``options`` it returns all the user owned tokens and tokens where user have
        Describe permission.

        The following errors can be anticipated when calling ``result()`` on the future
        obtained from the ``delegation_tokens()`` method of the returned
        ``DescribeDelegationTokenResult``:

        - ``UnsupportedByAuthenticationError``: if the request sent on PLAINTEXT/1-way
          SSL channels or delegation token authenticated channels.
        - ``DelegationTokenDisabledError``: if the delegation token feature is disabled.
        - ``TimeoutError``: if the request was not completed in within the given
          ``DescribeDelegationTokenOptions.timeout_ms()``.

        ``options``: the options to use when describing delegation tokens (Java's
        ``describeDelegationToken()`` passes ``new DescribeDelegationTokenOptions()``).
        Returns the ``DescribeDelegationTokenResult``, its future complete *(deviation:
        see ``create_topics``)*.
        """
        if options is None:
            options = DescribeDelegationTokenOptions()
        args = _conversions.describe_delegation_token_args(options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_delegation_token_async(c_admin, *args, cb),
            _conversions.free_describe_delegation_token)
        return _conversions.describe_delegation_token_result(payload, Sync)

    def describe_consumer_groups(self, *, group_ids: Iterable[str],
                                 options: DescribeConsumerGroupsOptions | None = None
                                 ) -> DescribeConsumerGroupsResult[Sync]:
        """Describe some consumer groups in the cluster.

        ``group_ids``: the IDs of the groups to describe; ``options``: the options to
        use when describing the groups (Java's ``describeConsumerGroups(groupIds)``
        passes ``new DescribeConsumerGroupsOptions()``). Returns the
        ``DescribeConsumerGroupsResult``, every future of which is complete
        *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DescribeConsumerGroupsOptions()
        ids = list(group_ids)
        args = _conversions.describe_consumer_groups_args(ids, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_consumer_groups_async(c_admin, *args, cb),
            _conversions.free_describe_consumer_groups)
        return _conversions.describe_consumer_groups_result(ids, payload, Sync)

    @overload
    def list_consumer_group_offsets(self, *, group_id: str,
                                    options: ListConsumerGroupOffsetsOptions | None = None
                                    ) -> ListConsumerGroupOffsetsResult[Sync]: ...
    @overload
    def list_consumer_group_offsets(
        self, *, group_specs: Mapping[str, ListConsumerGroupOffsetsSpec],
        options: ListConsumerGroupOffsetsOptions | None = None
    ) -> ListConsumerGroupOffsetsResult[Sync]: ...

    @java_forms(Form("group_id", "options"), Form("group_id"),
                Form("group_specs", "options"), Form("group_specs"))
    def list_consumer_group_offsets(
        self, *, group_id: str | None = None,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec] | None = None,
        options: ListConsumerGroupOffsetsOptions | None = None
    ) -> ListConsumerGroupOffsetsResult[Sync]:
        """List the consumer group offsets available in the cluster: of all
        partitions of the one group ``group_id``, or, for the specified consumer
        groups, of the topic partitions each ``group_specs`` entry names.

        ``group_id``: the group; Java's ``listConsumerGroupOffsets(groupId, options)``
        lists it with ``new ListConsumerGroupOffsetsSpec()``, using the provided
        options with the batched API, which uses topic partitions from the group spec
        and ignores any topic partitions set in the options. ``group_specs``: map of
        consumer group ids to a spec that specifies the topic partitions of the group
        to list offsets for. ``options``: the options to use when listing the
        consumer group offsets (Java's short forms pass ``new
        ListConsumerGroupOffsetsOptions()``). Returns the
        ``ListConsumerGroupOffsetsResult``, every future of which is complete
        *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = ListConsumerGroupOffsetsOptions()
        if group_specs is None:
            assert group_id is not None
            group_specs = {group_id: ListConsumerGroupOffsetsSpec()}
        specs = dict(group_specs)
        args = _conversions.list_consumer_group_offsets_args(specs, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_list_consumer_group_offsets_async(c_admin, *args, cb),
            _conversions.free_list_consumer_group_offsets)
        return _conversions.list_consumer_group_offsets_result(specs, payload, Sync)

    def delete_consumer_groups(self, *, group_ids: Iterable[str],
                               options: DeleteConsumerGroupsOptions | None = None
                               ) -> DeleteConsumerGroupsResult[Sync]:
        """Delete consumer groups from the cluster.

        ``group_ids``: the groups to delete; ``options``: the options to use when
        deleting a consumer group (Java's ``deleteConsumerGroups(groupIds)`` passes
        ``new DeleteConsumerGroupsOptions()``). Returns the
        ``DeleteConsumerGroupsResult``, every future of which is complete
        *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DeleteConsumerGroupsOptions()
        ids = list(group_ids)
        args = _conversions.delete_consumer_groups_args(ids, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_delete_consumer_groups_async(c_admin, *args, cb),
            _conversions.free_delete_consumer_groups)
        return _conversions.delete_consumer_groups_result(ids, payload, Sync)

    def delete_consumer_group_offsets(self, *, group_id: str,
                                      partitions: Iterable[TopicPartition],
                                      options: DeleteConsumerGroupOffsetsOptions | None = None
                                      ) -> DeleteConsumerGroupOffsetsResult[Sync]:
        """Delete committed offsets for a set of partitions in a consumer group. This
        will succeed at the partition level only if the group is not actively
        subscribed to the corresponding topic.

        ``group_id``: the group; ``partitions``: the partitions whose offsets to
        delete; ``options``: the options to use when deleting offsets in a consumer
        group (Java's ``deleteConsumerGroupOffsets(groupId, partitions)`` passes
        ``new DeleteConsumerGroupOffsetsOptions()``). Returns the
        ``DeleteConsumerGroupOffsetsResult``, its future complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = DeleteConsumerGroupOffsetsOptions()
        tps = list(partitions)
        args = _conversions.delete_consumer_group_offsets_args(group_id, tps, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_delete_consumer_group_offsets_async(
                c_admin, *args, cb),
            _conversions.free_delete_consumer_group_offsets)
        return _conversions.delete_consumer_group_offsets_result(tps, payload, Sync)

    def list_groups(self, *, options: ListGroupsOptions | None = None) -> ListGroupsResult[Sync]:
        """List the groups available in the cluster.

        ``options``: the options to use when listing the groups (Java's
        ``listGroups()`` passes ``new ListGroupsOptions()``). Returns the
        ``ListGroupsResult``, its futures complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = ListGroupsOptions()
        args = _conversions.list_groups_args(options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_list_groups_async(c_admin, *args, cb),
            _conversions.free_list_groups)
        return _conversions.list_groups_result(payload, Sync)

    def elect_leaders(self, *, election_type: ElectionType,
                      partitions: Iterable[TopicPartition] | None,
                      options: ElectLeadersOptions | None = None) -> ElectLeadersResult[Sync]:
        """Elect a replica as leader for the given ``partitions``, or for all
        partitions if the argument to ``partitions`` is ``None``.

        This operation is not transactional so it may succeed for some partitions
        while fail for others.

        It may take several seconds after this method returns success for all the
        brokers in the cluster to become aware that the partitions have new leaders.
        During this time, ``describe_topics()`` may not return information about the
        partitions' new leaders.

        This operation is supported by brokers with version 2.2.0 or later if
        preferred election is use; otherwise the brokers most be 2.4.0 or higher.

        The following errors can be anticipated when calling ``result()`` on the
        future obtained from the returned ``ElectLeadersResult``:

        - ``ClusterAuthorizationError`` if the authenticated user didn't have alter
          access to the cluster.
        - ``UnknownTopicOrPartitionError`` if the topic or partition did not exist
          within the cluster.
        - ``InvalidTopicError`` if the topic was already queued for deletion.
        - ``NotControllerError`` if the request was sent to a broker that was not the
          controller for the cluster.
        - ``TimeoutError`` if the request timed out before the election was complete.
        - ``LeaderNotAvailableError`` if the preferred leader was not alive or not in
          the ISR.

        ``election_type``: the type of election to conduct; ``partitions``: the
        topics and partitions for which to conduct elections; ``options``: the options
        to use when electing the leaders (Java's ``electLeaders(electionType,
        partitions)`` passes ``new ElectLeadersOptions()``). Returns the
        ``ElectLeadersResult``, its future complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = ElectLeadersOptions()
        args = _conversions.elect_leaders_args(election_type, partitions, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_elect_leaders_async(c_admin, *args, cb),
            _conversions.free_elect_leaders)
        return _conversions.elect_leaders_result(payload, Sync)

    def alter_partition_reassignments(
            self, *, reassignments: Mapping[TopicPartition, NewPartitionReassignment | None],
            options: AlterPartitionReassignmentsOptions | None = None
            ) -> AlterPartitionReassignmentsResult[Sync]:
        """Change the reassignments for one or more partitions. Providing ``None``
        (Java's ``Optional.empty()``) will **revert** the reassignment for the
        associated partition.

        The following errors can be anticipated when calling ``result()`` on the
        futures obtained from the returned ``AlterPartitionReassignmentsResult``:

        - ``ClusterAuthorizationError`` If the authenticated user didn't have alter
          access to the cluster.
        - ``UnknownTopicOrPartitionError`` If the topic or partition does not exist
          within the cluster.
        - ``TimeoutError`` if the request timed out before the controller could record
          the new assignments.
        - ``InvalidReplicaAssignmentError`` If the specified assignment was not valid.
        - ``InvalidReplicationFactorError`` If the replication factor was changed in
          an invalid way. Only raised when
          ``AlterPartitionReassignmentsOptions.allow_replication_factor_change()`` is
          set to false and the request is attempting to alter reassignments (not
          cancel)
        - ``UnsupportedVersionError`` If
          ``AlterPartitionReassignmentsOptions.allow_replication_factor_change()`` was
          changed outside the default and the server does not support the option (e.g
          due to an old Kafka version).
        - ``NoReassignmentInProgressError`` If there was an attempt to cancel a
          reassignment for a partition which was not being reassigned.

        ``reassignments``: the reassignments to add, modify, or remove, see
        ``NewPartitionReassignment``; ``options``: the options to use (Java's
        ``alterPartitionReassignments(reassignments)`` passes
        ``new AlterPartitionReassignmentsOptions()``). Returns the result, every
        future of which is complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = AlterPartitionReassignmentsOptions()
        partitions = list(reassignments)
        args = _conversions.alter_partition_reassignments_args(reassignments, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_alter_partition_reassignments_async(
                c_admin, *args, cb),
            _conversions.free_alter_partition_reassignments)
        return _conversions.alter_partition_reassignments_result(partitions, payload, Sync)

    def list_partition_reassignments(
            self, *, partitions: Iterable[TopicPartition] | None = None,
            options: ListPartitionReassignmentsOptions | None = None
            ) -> ListPartitionReassignmentsResult[Sync]:
        """List the current reassignments for the given partitions, or all of the
        current partition reassignments.

        The following errors can be anticipated when calling ``result()`` on the
        futures obtained from the returned ``ListPartitionReassignmentsResult``:

        - ``ClusterAuthorizationError`` If the authenticated user doesn't have alter
          access to the cluster.
        - ``UnknownTopicOrPartitionError`` If a given topic or partition does not
          exist.
        - ``TimeoutError`` If the request timed out before the controller could list
          the current reassignments.

        ``partitions``: the partitions we want to get reassignment for, or ``None``
        (Java's ``listPartitionReassignments()`` and
        ``listPartitionReassignments(options)``, which pass ``Optional.empty()``) if we
        want to get the reassignments for all partitions in the cluster; ``options``:
        the options to use (Java's short forms pass
        ``new ListPartitionReassignmentsOptions()``). Returns the result, its future
        complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = ListPartitionReassignmentsOptions()
        args = _conversions.list_partition_reassignments_args(partitions, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_list_partition_reassignments_async(
                c_admin, *args, cb),
            _conversions.free_list_partition_reassignments)
        return _conversions.list_partition_reassignments_result(payload, Sync)

    def remove_members_from_consumer_group(
        self, *, group_id: str, options: RemoveMembersFromConsumerGroupOptions
    ) -> RemoveMembersFromConsumerGroupResult[Sync]:
        """Remove members from the consumer group by given member identities.

        For possible error codes, refer to Java's ``LeaveGroupResponse``.

        ``group_id``: the ID of the group to remove member from; ``options``: the
        options to carry removing members' information. Returns the
        ``RemoveMembersFromConsumerGroupResult``, its futures complete *(deviation:
        see ``create_topics``)*.
        """
        args = _conversions.remove_members_from_consumer_group_args(group_id, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_remove_members_from_consumer_group_async(
                c_admin, *args, cb),
            _conversions.free_remove_members_from_consumer_group)
        return _conversions.remove_members_from_consumer_group_result(options, payload, Sync)

    def alter_consumer_group_offsets(self, *, group_id: str,
                                     offsets: Mapping[TopicPartition, OffsetAndMetadata],
                                     options: AlterConsumerGroupOffsetsOptions | None = None
                                     ) -> AlterConsumerGroupOffsetsResult[Sync]:
        """Alters offsets for the specified group. In order to succeed, the group
        must be empty.

        This operation is not transactional so it may succeed for some partitions
        while fail for others.

        ``group_id``: the group for which to alter offsets; ``offsets``: a map of
        offsets by partition with associated metadata, partitions not specified in
        the map being ignored; ``options``: the options to use when altering the
        offsets (Java's ``alterConsumerGroupOffsets(groupId, offsets)`` passes ``new
        AlterConsumerGroupOffsetsOptions()``). Returns the
        ``AlterConsumerGroupOffsetsResult``, its future complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = AlterConsumerGroupOffsetsOptions()
        args = _conversions.alter_consumer_group_offsets_args(group_id, offsets, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_alter_consumer_group_offsets_async(
                c_admin, *args, cb),
            _conversions.free_alter_consumer_group_offsets)
        return _conversions.alter_consumer_group_offsets_result(payload, Sync)

    def list_offsets(self, *, topic_partition_offsets: Mapping[TopicPartition, OffsetSpec],
                     options: ListOffsetsOptions | None = None) -> ListOffsetsResult[Sync]:
        """List offset for the specified partitions. This operation enables to find
        the beginning offset, end offset as well as the offset matching a timestamp
        in partitions.

        ``topic_partition_offsets``: the mapping from partition to the ``OffsetSpec``
        to look up; ``options``: the options to use when retrieving the offsets
        (Java's ``listOffsets(topicPartitionOffsets)`` passes
        ``new ListOffsetsOptions()``). Returns the ``ListOffsetsResult``, every future
        of which is complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = ListOffsetsOptions()
        partitions = list(topic_partition_offsets)
        args = _conversions.list_offsets_args(topic_partition_offsets, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_list_offsets_async(c_admin, *args, cb),
            _conversions.free_list_offsets)
        return _conversions.list_offsets_result(partitions, payload, Sync)

    def describe_client_quotas(self, *, filter: ClientQuotaFilter,  # noqa: A002 - Java's name
                               options: DescribeClientQuotasOptions | None = None
                               ) -> DescribeClientQuotasResult[Sync]:
        """Describes all entities matching the provided filter that have at least one
        client quota configuration value defined.

        The following errors can be anticipated when calling ``result()`` on the future
        from the returned ``DescribeClientQuotasResult``:

        - ``ClusterAuthorizationError``: if the authenticated user didn't have describe
          access to the cluster.
        - ``InvalidRequestError``: if the request details are invalid. e.g., an invalid
          entity type was specified.
        - ``TimeoutError``: if the request timed out before the describe could finish.

        This operation is supported by brokers with version 2.6.0 or higher.

        ``filter``: the filter to apply to match entities; ``options``: the options to
        use (Java's ``describeClientQuotas(filter)`` passes ``new
        DescribeClientQuotasOptions()``). Returns the ``DescribeClientQuotasResult``
        containing the result, its future complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = DescribeClientQuotasOptions()
        args = _conversions.describe_client_quotas_args(filter, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_client_quotas_async(c_admin, *args, cb),
            _conversions.free_describe_client_quotas)
        return _conversions.describe_client_quotas_result(payload, Sync)

    def alter_client_quotas(self, *, entries: Iterable[ClientQuotaAlteration],
                            options: AlterClientQuotasOptions | None = None
                            ) -> AlterClientQuotasResult[Sync]:
        """Alters client quota configurations with the specified alterations.

        Alterations for a single entity are atomic, but across entities is not
        guaranteed. The resulting per-entity error code should be evaluated to resolve
        the success or failure of all updates.

        The following errors can be anticipated when calling ``result()`` on the
        futures obtained from the returned ``AlterClientQuotasResult``:

        - ``ClusterAuthorizationError``: if the authenticated user didn't have alter
          access to the cluster.
        - ``InvalidRequestError``: if the request details are invalid. e.g., a
          configuration key was specified more than once for an entity.
        - ``TimeoutError``: if the request timed out before the alterations could
          finish. It cannot be guaranteed whether the update succeed or not.

        This operation is supported by brokers with version 2.6.0 or higher.

        ``entries``: the alterations to perform; ``options``: the options to use (Java's
        ``alterClientQuotas(entries)`` passes ``new AlterClientQuotasOptions()``).
        Returns the ``AlterClientQuotasResult`` containing the result, every future of
        which is complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = AlterClientQuotasOptions()
        alterations = list(entries)
        args = _conversions.alter_client_quotas_args(alterations, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_alter_client_quotas_async(c_admin, *args, cb),
            _conversions.free_alter_client_quotas)
        return _conversions.alter_client_quotas_result(alterations, payload, Sync)

    @overload
    def describe_user_scram_credentials(self, *, users: Sequence[str] | None = None
                                        ) -> DescribeUserScramCredentialsResult[Sync]: ...
    @overload
    def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None,
            options: DescribeUserScramCredentialsOptions
    ) -> DescribeUserScramCredentialsResult[Sync]: ...

    @java_forms(Form(), Form("users"), Form("users", "options"))
    def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None = UNSET,
            options: DescribeUserScramCredentialsOptions | None = None
    ) -> DescribeUserScramCredentialsResult[Sync]:
        """Describe SASL/SCRAM credentials.

        The following errors can be anticipated when calling ``result()`` on the
        futures from the returned ``DescribeUserScramCredentialsResult``:

        - ``ClusterAuthorizationError``: if the authenticated user didn't have describe
          access to the cluster.
        - ``ResourceNotFoundError``: if the user did not exist/had no SCRAM credentials.
        - ``DuplicateResourceError``: if the user was requested to be described more
          than once in the original request.
        - ``TimeoutError``: if the request timed out before the describe operation could
          finish.

        This operation is supported by brokers with version 2.7.0 or higher.

        ``users``: the users for which credentials are to be described; all users'
        credentials are described if ``None`` or empty (Java's
        ``describeUserScramCredentials()`` passes ``null``); ``options``: the options to
        use when describing the credentials (Java's short forms pass ``new
        DescribeUserScramCredentialsOptions()``). Java has no form taking ``options``
        alone. Returns the ``DescribeUserScramCredentialsResult``, its futures complete
        *(deviation: see ``create_topics``)*.
        """
        if users is UNSET:
            users = None
        if options is None:
            options = DescribeUserScramCredentialsOptions()
        args = _conversions.describe_user_scram_credentials_args(users, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_user_scram_credentials_async(
                c_admin, *args, cb),
            _conversions.free_describe_user_scram_credentials)
        return _conversions.describe_user_scram_credentials_result(payload, Sync)

    def alter_user_scram_credentials(
            self, *, alterations: Sequence[UserScramCredentialAlteration],
            options: AlterUserScramCredentialsOptions | None = None
    ) -> AlterUserScramCredentialsResult[Sync]:
        """Alter SASL/SCRAM credentials.

        The following errors can be anticipated when calling ``result()`` on any of the
        futures from the returned ``AlterUserScramCredentialsResult``:

        - ``NotControllerError``: if the request is not sent to the Controller broker.
        - ``ClusterAuthorizationError``: if the authenticated user didn't have alter
          access to the cluster.
        - ``UnsupportedByAuthenticationError``: if the user authenticated with a
          delegation token.
        - ``UnsupportedSaslMechanismError``: if the requested SCRAM mechanism is
          unrecognized or otherwise unsupported.
        - ``UnacceptableCredentialError``: if the username is empty or the requested
          number of iterations is too small or too large.
        - ``TimeoutError``: if the request timed out before the describe could finish.

        This operation is supported by brokers with version 2.7.0 or higher.

        ``alterations``: the alterations to be applied; ``options``: the options to use
        when altering the credentials (Java's ``alterUserScramCredentials(alterations)``
        passes ``new AlterUserScramCredentialsOptions()``). Returns the
        ``AlterUserScramCredentialsResult``, every future of which is complete
        *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = AlterUserScramCredentialsOptions()
        rows = list(alterations)
        args = _conversions.alter_user_scram_credentials_args(rows, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_alter_user_scram_credentials_async(
                c_admin, *args, cb),
            _conversions.free_alter_user_scram_credentials)
        return _conversions.alter_user_scram_credentials_result(rows, payload, Sync)

    def describe_features(self, *, options: DescribeFeaturesOptions | None = None
                          ) -> DescribeFeaturesResult[Sync]:
        """Describes finalized as well as supported features. The request is issued to
        any random broker.

        The following errors can be anticipated when calling ``result()`` on the future
        from the returned ``DescribeFeaturesResult``:

        - ``TimeoutError``: if the request timed out before the describe operation could
          finish.

        ``options``: the options to use (Java's ``describeFeatures()`` passes ``new
        DescribeFeaturesOptions()``). Returns the ``DescribeFeaturesResult`` containing
        the result, its future complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DescribeFeaturesOptions()
        args = _conversions.describe_features_args(options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_features_async(c_admin, *args, cb),
            _conversions.free_describe_features)
        return _conversions.describe_features_result(payload, Sync)

    def update_features(self, *, feature_updates: Mapping[str, FeatureUpdate],
                        options: UpdateFeaturesOptions | None = None
                        ) -> UpdateFeaturesResult[Sync]:
        """Applies specified updates to finalized features. This operation is not
        transactional so some updates may succeed while the rest may fail.

        The API takes in a map of finalized feature names to ``FeatureUpdate`` that
        needs to be applied. Each entry in the map specifies the finalized feature to be
        added or updated or deleted, along with the new max feature version level value.
        This request is issued only to the controller since the API is only served by
        the controller. The return value contains an error code for each supplied
        ``FeatureUpdate``, and the code indicates if the update succeeded or failed in
        the controller.

        - Downgrading a feature version level is not a common operation and should only
          be performed when necessary. It is permitted only if the ``FeatureUpdate``
          specifies the ``upgrade_type`` as either ``FeatureUpdate.UpgradeType.SAFE_DOWNGRADE``
          or ``FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE``. ``SAFE_DOWNGRADE`` allows
          downgrades that do not lead to metadata loss; ``UNSAFE_DOWNGRADE`` permits
          downgrades that might result in metadata loss. Note that even with these
          settings, certain downgrades may still be rejected by the controller if they
          are considered unsafe or impossible.
        - Deleting a finalized feature version is also not a common operation. To delete
          a feature, set the ``max_version_level`` to zero and specify the
          ``upgrade_type`` as either ``SAFE_DOWNGRADE`` or ``UNSAFE_DOWNGRADE``.
        - The ``UPGRADE`` type cannot be used when the ``max_version_level`` is zero.
          Attempting to do so will result in an ``IllegalArgumentError``.

        The following errors can be anticipated when calling ``result()`` on the futures
        obtained from the returned ``UpdateFeaturesResult``:

        - ``ClusterAuthorizationError``: if the authenticated user didn't have alter
          access to the cluster.
        - ``InvalidRequestError``: if the request details are invalid. e.g., a
          non-existing finalized feature is attempted to be deleted or downgraded.
        - ``TimeoutError``: if the request timed out before the updates could finish. It
          cannot be guaranteed whether the updates succeeded or not.
        - ``FeatureUpdateFailedError``: this means there was an unexpected error
          encountered when the update was applied on the controller. There is no
          guarantee on whether the update succeeded or failed. The best way to find out
          is to issue a ``describe_features`` call to the cluster.

        This operation is supported by brokers with version 2.7.0 or higher.

        An empty ``feature_updates``, or a blank feature name, raises
        ``IllegalArgumentError`` (``Feature updates can not be null or empty.``,
        ``Provided feature can not be empty.``), as Java's throws before it makes the
        call.

        ``feature_updates``: the map of finalized feature name to ``FeatureUpdate``;
        ``options``: the options to use (Java's ``updateFeatures(featureUpdates)``
        passes ``new UpdateFeaturesOptions()``). Returns the ``UpdateFeaturesResult``
        containing the result, every future of which is complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = UpdateFeaturesOptions()
        updates = dict(feature_updates)
        args = _conversions.update_features_args(updates, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_update_features_async(c_admin, *args, cb),
            _conversions.free_update_features)
        return _conversions.update_features_result(updates, payload, Sync)

    def describe_producers(self, *, partitions: Iterable[TopicPartition],
                           options: DescribeProducersOptions | None = None
                           ) -> DescribeProducersResult[Sync]:
        """Describe active producer state on a set of topic partitions. Unless a
        specific broker is requested through ``DescribeProducersOptions.broker_id()``,
        this will query the partition leader to find the producer state.

        ``partitions``: the set of partitions to query; ``options``: options to control
        the method behavior (Java's ``describeProducers(partitions)`` passes ``new
        DescribeProducersOptions()``). Returns the result, every future of which is
        complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DescribeProducersOptions()
        topic_partitions = list(partitions)
        args = _conversions.describe_producers_args(topic_partitions, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_producers_async(c_admin, *args, cb),
            _conversions.free_describe_producers)
        return _conversions.describe_producers_result(topic_partitions, payload, Sync)

    def describe_transactions(self, *, transactional_ids: Iterable[str],
                              options: DescribeTransactionsOptions | None = None
                              ) -> DescribeTransactionsResult[Sync]:
        """Describe the state of a set of transactional IDs from the respective
        transaction coordinators, which are dynamically discovered.

        ``transactional_ids``: the set of transactional IDs to query; ``options``:
        options to control the method behavior (Java's
        ``describeTransactions(transactionalIds)`` passes ``new
        DescribeTransactionsOptions()``). Returns the result, every future of which is
        complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DescribeTransactionsOptions()
        ids = list(transactional_ids)
        args = _conversions.describe_transactions_args(ids, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_transactions_async(c_admin, *args, cb),
            _conversions.free_describe_transactions)
        return _conversions.describe_transactions_result(ids, payload, Sync)

    def abort_transaction(self, *, spec: AbortTransactionSpec,
                          options: AbortTransactionOptions | None = None
                          ) -> AbortTransactionResult[Sync]:
        """Forcefully abort a transaction which is open on a topic partition. This will
        send a ``WriteTxnMarkers`` request to the partition leader in order to abort the
        transaction. This requires administrative privileges.

        ``spec``: the transaction specification including topic partition and producer
        details; ``options``: options to control the method behavior (including
        filters) (Java's ``abortTransaction(spec)`` passes ``new
        AbortTransactionOptions()``). Returns the result, its future complete
        *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = AbortTransactionOptions()
        args = _conversions.abort_transaction_args(spec, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_abort_transaction_async(c_admin, *args, cb),
            _conversions.free_abort_transaction)
        return _conversions.abort_transaction_result(spec, payload, Sync)

    def list_transactions(self, *, options: ListTransactionsOptions | None = None
                          ) -> ListTransactionsResult[Sync]:
        """List active transactions in the cluster. This will query all potential
        transaction coordinators in the cluster and collect the state of all
        transactions. Users should typically attempt to reduce the size of the result set
        using ``ListTransactionsOptions.filter_producer_ids()`` or
        ``ListTransactionsOptions.filter_states()`` or
        ``ListTransactionsOptions.filter_on_duration()``.

        ``options``: options to control the method behavior (including filters) (Java's
        ``listTransactions()`` passes ``new ListTransactionsOptions()``). Returns the
        result, its futures complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = ListTransactionsOptions()
        args = _conversions.list_transactions_args(options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_list_transactions_async(c_admin, *args, cb),
            _conversions.free_list_transactions)
        return _conversions.list_transactions_result(payload, Sync)

    def fence_producers(self, *, transactional_ids: Iterable[str],
                        options: FenceProducersOptions | None = None
                        ) -> FenceProducersResult[Sync]:
        """Fence out all active producers that use any of the provided transactional
        IDs.

        ``transactional_ids``: the IDs of the producers to fence; ``options``: the
        options to use when fencing the producers (Java's
        ``fenceProducers(transactionalIds)`` passes ``new FenceProducersOptions()``).
        Returns the ``FenceProducersResult``, every future of which is complete
        *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = FenceProducersOptions()
        ids = list(transactional_ids)
        args = _conversions.fence_producers_args(ids, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_fence_producers_async(c_admin, *args, cb),
            _conversions.free_fence_producers)
        return _conversions.fence_producers_result(ids, payload, Sync)

    @overload
    def list_config_resources(self, *, config_resource_types: Iterable[ConfigResource.Type],
                              options: ListConfigResourcesOptions
                              ) -> ListConfigResourcesResult[Sync]: ...
    @overload
    def list_config_resources(self) -> ListConfigResourcesResult[Sync]: ...

    @java_forms(Form("config_resource_types", "options",
                     defaults={"config_resource_types": (), "options": None}),
                Form())
    def list_config_resources(
            self, *, config_resource_types: Iterable[ConfigResource.Type] = UNSET,
            options: ListConfigResourcesOptions | None = None
            ) -> ListConfigResourcesResult[Sync]:
        """List the configuration resources available in the cluster which matches
        config resource type. If no config resource types are specified, all
        configuration resources will be listed.

        ``config_resource_types``: the set of configuration resource types to list;
        ``options``: the options to use when listing the configuration resources
        (Java's ``listConfigResources()`` passes ``Set.of()`` and
        ``new ListConfigResourcesOptions()``). Returns the
        ``ListConfigResourcesResult``, its future complete *(deviation: see
        ``create_topics``)*.
        """
        if options is None:
            options = ListConfigResourcesOptions()
        args = _conversions.list_config_resources_args(config_resource_types, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_list_config_resources_async(c_admin, *args, cb),
            _conversions.free_list_config_resources)
        return _conversions.list_config_resources_result(payload, Sync)

    def describe_classic_groups(self, *, group_ids: Iterable[str],
                                options: DescribeClassicGroupsOptions | None = None
                                ) -> DescribeClassicGroupsResult[Sync]:
        """Describe some classic groups in the cluster.

        ``group_ids``: the IDs of the groups to describe; ``options``: the options to
        use when describing the groups (Java's ``describeClassicGroups(groupIds)``
        passes ``new DescribeClassicGroupsOptions()``). Returns the
        ``DescribeClassicGroupsResult``, every future of which is complete
        *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = DescribeClassicGroupsOptions()
        ids = list(group_ids)
        args = _conversions.describe_classic_groups_args(ids, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_describe_classic_groups_async(c_admin, *args, cb),
            _conversions.free_describe_classic_groups)
        return _conversions.describe_classic_groups_result(ids, payload, Sync)

    def force_terminate_transaction(self, *, transactional_id: str,
                                    options: TerminateTransactionOptions | None = None
                                    ) -> TerminateTransactionResult[Sync]:
        """Force terminate a transaction for the given transactional ID.

        This operation aborts any ongoing transaction associated with the transactional
        ID. It's similar to ``fence_producers`` but only targets a single transactional
        ID to handle long-running transactions when 2PC is enabled.

        ``transactional_id``: the ID of the transaction to terminate; ``options``: the
        options to use when terminating the transaction (Java's
        ``forceTerminateTransaction(transactionalId)`` passes ``new
        TerminateTransactionOptions()``). Returns the ``TerminateTransactionResult``, its
        future complete *(deviation: see ``create_topics``)*.
        """
        if options is None:
            options = TerminateTransactionOptions()
        args = _conversions.force_terminate_transaction_args(transactional_id, options)
        payload = self._wait_for(
            lambda c_admin, cb: _lib.Admin_force_terminate_transaction_async(
                c_admin, *args, cb),
            _conversions.free_force_terminate_transaction)
        return _conversions.force_terminate_transaction_result(payload, Sync)
