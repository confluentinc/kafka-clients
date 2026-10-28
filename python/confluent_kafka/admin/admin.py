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

An RPC's ``options`` left out is Java's short form, which passes ``new
XxxOptions()``; an explicit ``None`` is Java's ``null``, which
``KafkaAdminClient`` dereferences, and raises ``TypeError("options must not be
None")``. Every other ``None`` is Java's ``null`` too: where Java gives it a
meaning (``elect_leaders(partitions=None)``, all partitions) it keeps it, and
where Java dereferences it, it raises ``TypeError("<param> must not be None")``,
in Java's order, after the closed check (CLAUDE.md, Python Binding Conventions,
Implementation over the FFI, exception 4 and Order).

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

from confluent_kafka._args import MISSING, Form, java_forms, reject_str
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
from confluent_kafka.admin.unregister_controller_options import UnregisterControllerOptions
from confluent_kafka.admin.update_features_options import UpdateFeaturesOptions
from confluent_kafka.illegal_argument_error import IllegalArgumentError
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

__all__ = ["Admin"]

_LOG = logging.getLogger("confluent_kafka.admin")

#: Java's message for a call on a closing ``KafkaAdminClient``
#: (``org.apache.kafka.clients.admin.KafkaAdminClient.AdminClientRunnable#call``).
CLOSED_MESSAGE = "Cannot accept new calls when AdminClient is closing."


def _closed_error() -> IllegalStateError:
    """The error of an RPC once ``close()`` has begun (``_rpc_use``)."""
    return IllegalStateError(message=CLOSED_MESSAGE)

#: ``close()``'s timeout: Java's ``Duration.ofMillis(Long.MAX_VALUE)`` crosses as -1.
_CLOSE_FOREVER_MS = -1

#: Java's admin thread name prefix (``KafkaAdminClient.NETWORK_THREAD_PREFIX``).
_PUMP_THREAD_NAME = "kafka-admin-client-thread"


class _AdminState:
    """The native admin handle, the closed flag and the uses in flight, shared by
    ``Admin`` and ``AsyncAdmin``.

    The handle lives until the teardown frees it. Every RPC is one *use* of it,
    counted under ``_lifecycle_lock`` around its extension call, which returns at
    once with the call's C result; a use cannot start once ``_closed`` is set. A
    run of the client's queued callbacks is a use too (:meth:`_pump_use`), which can
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
        # The lifecycle lock is the C RLock, taken directly (never through the
        # Condition's Python-level __enter__ / __exit__, where a Ctrl+C could
        # leave it held); the Condition over it only waits and wakes.
        self._lifecycle_lock = threading.RLock()
        self._lifecycle = threading.Condition(self._lifecycle_lock)
        self._uses = 0
        # The use of every RPC, built once: a call use holds no per-use state,
        # so one serves every RPC at once.
        self._rpc_use = _lib.Use(self, _lib.USE_CALL, "_c_admin", error=_closed_error)

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

    def _pump_use(self) -> Any:
        """A run of the client's queued callbacks, counted as a use of the
        handle: ``with self._pump_use() as counted:``, ``counted`` being
        ``False`` (and nothing counted) once the teardown has begun (it runs
        what is left itself)."""
        return _lib.Use(self, _lib.USE_PUMP, "_c_admin", flag="_tearing_down")

    def _check_not_closed(self) -> None:
        if self._closed:
            raise IllegalStateError(message=CLOSED_MESSAGE)

    def _begin_close(self) -> bool:
        """Set ``_closed``; whether this call did (and so must tear down)."""
        with self._lifecycle_lock:
            if self._closed:
                return False
            self._closed = True
            return True

    def _teardown(self) -> None:
        """Wait until no use is left (none can start any more), then free the
        handle, which runs the callbacks still pending on this thread."""
        with self._lifecycle_lock:
            self._tearing_down = True
            while self._uses:
                self._lifecycle.wait()
            c_admin = self._c_admin
        if c_admin:
            _lib.Admin_destroy(c_admin)

    def _call(self, entry: Callable[..., Any], *args: Any) -> Any:
        """One RPC: ``entry(handle, *args)``, an ``Admin_<rpc>`` of the extension,
        which returns the C result at once (Java's RPCs return at once)."""
        with self._rpc_use as c_admin:
            return entry(c_admin, *args)


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
            with state._pump_use() as counted:
                if not counted:
                    break
                try:
                    _lib.Admin__execute_callbacks(state._c_admin)
                except Exception:  # noqa: BLE001 - the pump thread has no caller
                    _LOG.exception("Error running the admin client's callbacks")
        after = self._after
        if after is not None:
            after()


def close_timeout_ms(timeout: Duration | None) -> int:
    """``close(Duration)``'s timeout in milliseconds (``-1`` for ``close()``, the
    timeout left out); a ``None`` is Java's null ``Duration``, which
    ``KafkaAdminClient.close(Duration)`` dereferences (``timeout.toMillis()``):
    ``TypeError("timeout must not be None")``; a negative one raises
    ``IllegalArgumentError("The timeout cannot be negative.")``, as Java does."""
    if timeout is MISSING:
        return _CLOSE_FOREVER_MS
    if timeout is None:
        raise TypeError("timeout must not be None")
    return duration_to_ms(timeout, default_ms=0)


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

    @overload
    def close(self) -> None: ...
    @overload
    def close(self, *, timeout: Duration) -> None: ...

    def close(self, *, timeout: Duration = MISSING) -> None:
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

    @overload
    def create_topics(self, *, new_topics: Iterable[NewTopic]) -> CreateTopicsResult[Sync]: ...
    @overload
    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions) -> CreateTopicsResult[Sync]: ...

    def create_topics(self, *, new_topics: Iterable[NewTopic],
                      options: CreateTopicsOptions = MISSING) -> CreateTopicsResult[Sync]:
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
        self._check_not_closed()
        _conversions.non_null(new_topics, "new_topics")
        args = _conversions.create_topics_args(new_topics)
        reads = _conversions.any_topic_name(row[0] for row in args[0])
        options = _conversions.options_or_new(options, CreateTopicsOptions, read=reads)
        native = self._call(_lib.Admin_create_topics, *args, options._setters_ran())
        return _conversions.create_topics_result(native, Sync, None)

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
        pass ``new DeleteTopicsOptions()``). Returns the ``DeleteTopicsResult``.
        """
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
        return _conversions.delete_topics_result(native, by_ids, Sync, None)

    @overload
    def list_topics(self) -> ListTopicsResult[Sync]: ...
    @overload
    def list_topics(self, *, options: ListTopicsOptions) -> ListTopicsResult[Sync]: ...

    def list_topics(self, *, options: ListTopicsOptions = MISSING) -> ListTopicsResult[Sync]:
        """List the topics available in the cluster.

        ``options``: the options to use when listing the topics (Java's
        ``listTopics()`` passes ``new ListTopicsOptions()``). Returns the
        ``ListTopicsResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, ListTopicsOptions)
        native = self._call(_lib.Admin_list_topics, options._setters_ran())
        return _conversions.list_topics_result(native, Sync, None)

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

    @java_forms(Form("topic_names"), Form("topic_names", "options"),
                Form("topics"), Form("topics", "options"))
    def describe_topics(self, *, topic_names: Iterable[str] = MISSING,
                        topics: TopicCollection = MISSING,
                        options: DescribeTopicsOptions = MISSING
                        ) -> DescribeTopicsResult[Sync]:
        """Describe some topics in the cluster.

        When using topic IDs, this operation is supported by brokers with version
        3.1.0 or higher.

        ``topic_names``: the names of the topics to describe (Java's
        ``describeTopics(Collection<String>)``, which passes
        ``TopicCollection.ofTopicNames(topicNames)``), or ``topics``: the topics to
        describe; ``options``: the options to use when describing the topics (Java's
        short forms pass ``new DescribeTopicsOptions()``). Returns the
        ``DescribeTopicsResult``.
        """
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
            native, form == _conversions.DESCRIBE_TOPIC_ID_COLLECTION, Sync, None)

    @overload
    def describe_cluster(self) -> DescribeClusterResult[Sync]: ...
    @overload
    def describe_cluster(self, *,
                         options: DescribeClusterOptions) -> DescribeClusterResult[Sync]: ...

    def describe_cluster(self, *, options: DescribeClusterOptions = MISSING
                         ) -> DescribeClusterResult[Sync]:
        """Get information about the nodes in the cluster.

        To obtain broker cluster information, you must configure
        ``bootstrap.servers``. To obtain controller cluster information, you must
        configure ``bootstrap.controllers``.

        ``options``: the options to use when getting information about the cluster
        (Java's ``describeCluster()`` passes ``new DescribeClusterOptions()``).
        Returns the ``DescribeClusterResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeClusterOptions)
        native = self._call(_lib.Admin_describe_cluster, options._setters_ran())
        return _conversions.describe_cluster_result(native, Sync, None)

    @overload
    def describe_acls(self, *, filter: AclBindingFilter) -> DescribeAclsResult[Sync]: ...
    @overload
    def describe_acls(self, *, filter: AclBindingFilter,
                      options: DescribeAclsOptions) -> DescribeAclsResult[Sync]: ...

    def describe_acls(self, *, filter: AclBindingFilter,  # noqa: A002 - Java's name
                      options: DescribeAclsOptions = MISSING) -> DescribeAclsResult[Sync]:
        """Lists access control lists (ACLs) according to the supplied filter.

        Note: it may take some time for changes made by ``create_acls`` or
        ``delete_acls`` to be reflected in the output of ``describe_acls``.

        This operation is supported by brokers with version 0.11.0.0 or higher.

        ``filter``: the filter to use; ``options``: the options to use when listing the
        ACLs (Java's ``describeAcls(filter)`` passes ``new DescribeAclsOptions()``).
        Returns the ``DescribeAclsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(filter, "filter")
        args = _conversions.describe_acls_args(filter)
        reads = not filter.is_unknown()
        options = _conversions.options_or_new(options, DescribeAclsOptions, read=reads)
        native = self._call(_lib.Admin_describe_acls, *args, options._setters_ran())
        return _conversions.describe_acls_result(native, Sync, None)

    @overload
    def create_acls(self, *, acls: Iterable[AclBinding]) -> CreateAclsResult[Sync]: ...
    @overload
    def create_acls(self, *, acls: Iterable[AclBinding],
                    options: CreateAclsOptions) -> CreateAclsResult[Sync]: ...

    def create_acls(self, *, acls: Iterable[AclBinding],
                    options: CreateAclsOptions = MISSING) -> CreateAclsResult[Sync]:
        """Creates access control lists (ACLs) which are bound to specific resources.

        This operation is not transactional so it may succeed for some ACLs while fail
        for others.

        If you attempt to add an ACL that duplicates an existing ACL, no error will be
        raised, but no changes will be made.

        This operation is supported by brokers with version 0.11.0.0 or higher.

        ``acls``: the ACLs to create; ``options``: the options to use when creating the
        ACLs (Java's ``createAcls(acls)`` passes ``new CreateAclsOptions()``). Returns
        the ``CreateAclsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(acls, "acls")
        args = _conversions.create_acls_args(acls)
        options = _conversions.options_or_new(options, CreateAclsOptions)
        native = self._call(_lib.Admin_create_acls, *args, options._setters_ran())
        return _conversions.create_acls_result(native, Sync, None)

    @overload
    def delete_acls(self, *, filters: Iterable[AclBindingFilter]) -> DeleteAclsResult[Sync]: ...
    @overload
    def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                    options: DeleteAclsOptions) -> DeleteAclsResult[Sync]: ...

    def delete_acls(self, *, filters: Iterable[AclBindingFilter],
                    options: DeleteAclsOptions = MISSING) -> DeleteAclsResult[Sync]:
        """Deletes access control lists (ACLs) according to the supplied filters.

        This operation is not transactional so it may succeed for some ACLs while fail
        for others.

        This operation is supported by brokers with version 0.11.0.0 or higher.

        ``filters``: the filters to use; ``options``: the options to use when deleting
        the ACLs (Java's ``deleteAcls(filters)`` passes ``new DeleteAclsOptions()``).
        Returns the ``DeleteAclsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(filters, "filters")
        args = _conversions.delete_acls_args(filters)
        options = _conversions.options_or_new(options, DeleteAclsOptions)
        native = self._call(_lib.Admin_delete_acls, *args, options._setters_ran())
        return _conversions.delete_acls_result(native, Sync, None)

    @overload
    def describe_configs(self, *,
                         resources: Iterable[ConfigResource]) -> DescribeConfigsResult[Sync]: ...
    @overload
    def describe_configs(self, *, resources: Iterable[ConfigResource],
                         options: DescribeConfigsOptions) -> DescribeConfigsResult[Sync]: ...

    def describe_configs(self, *, resources: Iterable[ConfigResource],
                         options: DescribeConfigsOptions = MISSING
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
        ``DescribeConfigsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(resources, "resources")
        args = _conversions.describe_configs_args(list(resources))
        options = _conversions.options_or_new(options, DescribeConfigsOptions, read=bool(args[0]))
        native = self._call(_lib.Admin_describe_configs, *args, options._setters_ran())
        return _conversions.describe_configs_result(native, Sync, None)

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
        ``new AlterConfigsOptions()``). Returns the ``AlterConfigsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(configs, "configs")
        _conversions.check_keys(configs, "configs")  # nodeFor(resource), before options
        # Java reads options per node (3006), the ops only in createRequest.
        options = _conversions.options_or_new(options, AlterConfigsOptions, read=bool(configs))
        args = _conversions.incremental_alter_configs_args(configs)
        native = self._call(_lib.Admin_incremental_alter_configs, *args, options._setters_ran())
        return _conversions.incremental_alter_configs_result(native, Sync, None)

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

    def alter_replica_log_dirs(
            self, *, replica_assignment: Mapping[TopicPartitionReplica, str],
            options: AlterReplicaLogDirsOptions = MISSING
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
        ``AlterReplicaLogDirsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(replica_assignment, "replica_assignment")
        # replica.brokerId() (3048) before options (3070); the log dir is not read.
        _conversions.check_keys(replica_assignment, "replica_assignment")
        reads = bool(replica_assignment)
        options = _conversions.options_or_new(options, AlterReplicaLogDirsOptions, read=reads)
        args = _conversions.alter_replica_log_dirs_args(replica_assignment)
        native = self._call(_lib.Admin_alter_replica_log_dirs, *args, options._setters_ran())
        return _conversions.alter_replica_log_dirs_result(native, Sync, None)

    @overload
    def describe_log_dirs(self, *, brokers: Iterable[int]) -> DescribeLogDirsResult[Sync]: ...
    @overload
    def describe_log_dirs(self, *, brokers: Iterable[int],
                          options: DescribeLogDirsOptions) -> DescribeLogDirsResult[Sync]: ...

    def describe_log_dirs(self, *, brokers: Iterable[int],
                          options: DescribeLogDirsOptions = MISSING
                          ) -> DescribeLogDirsResult[Sync]:
        """Query the information of all log directories on the given set of brokers.

        This operation is supported by brokers with version 1.0.0 or higher.

        ``brokers``: a list of brokers; ``options``: the options to use when querying
        log dir info (Java's ``describeLogDirs(brokers)`` passes
        ``new DescribeLogDirsOptions()``). Returns the ``DescribeLogDirsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(brokers, "brokers")
        broker_ids = list(brokers)
        # new Call(…, calcDeadlineMs(now, options.timeoutMs()), new ConstantNodeIdProvider(
        # brokerId)) reads options before it unboxes the broker id (3128-3129).
        options = _conversions.options_or_new(options, DescribeLogDirsOptions,
                                              read=bool(broker_ids))
        args = _conversions.describe_log_dirs_args(broker_ids)
        native = self._call(_lib.Admin_describe_log_dirs, *args, options._setters_ran())
        return _conversions.describe_log_dirs_result(native, Sync, None)

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

    def describe_replica_log_dirs(
            self, *, replicas: Iterable[TopicPartitionReplica],
            options: DescribeReplicaLogDirsOptions = MISSING
            ) -> DescribeReplicaLogDirsResult[Sync]:
        """Query the replica log directory information for the specified replicas.

        This operation is supported by brokers with version 1.0.0 or higher.

        ``replicas``: the replicas to query; ``options``: the options to use when
        querying replica log dir info (Java's ``describeReplicaLogDirs(replicas)``
        passes ``new DescribeReplicaLogDirsOptions()``). Returns the
        ``DescribeReplicaLogDirsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(replicas, "replicas")
        args = _conversions.describe_replica_log_dirs_args(list(replicas))
        reads = bool(args[0])
        options = _conversions.options_or_new(options, DescribeReplicaLogDirsOptions, read=reads)
        native = self._call(_lib.Admin_describe_replica_log_dirs, *args, options._setters_ran())
        return _conversions.describe_replica_log_dirs_result(native, Sync, None)

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
        Returns the ``CreatePartitionsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(new_partitions, "new_partitions")
        args = _conversions.create_partitions_args(new_partitions)
        options = _conversions.options_or_new(options, CreatePartitionsOptions, read=bool(args[0]))
        native = self._call(_lib.Admin_create_partitions, *args, options._setters_ran())
        return _conversions.create_partitions_result(native, Sync, None)

    @overload
    def delete_records(
        self, *,
        records_to_delete: Mapping[TopicPartition, RecordsToDelete],
    ) -> DeleteRecordsResult[Sync]: ...
    @overload
    def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                       options: DeleteRecordsOptions) -> DeleteRecordsResult[Sync]: ...

    def delete_records(self, *, records_to_delete: Mapping[TopicPartition, RecordsToDelete],
                       options: DeleteRecordsOptions = MISSING
                       ) -> DeleteRecordsResult[Sync]:
        """Delete records whose offset is smaller than the given offset of the
        corresponding partition.

        This operation is supported by brokers with version 0.11.0.0 or higher.

        ``records_to_delete``: the topic partitions and related offsets from which
        records deletion starts; ``options``: the options to use when deleting records
        (Java's ``deleteRecords(recordsToDelete)`` passes
        ``new DeleteRecordsOptions()``). Returns the ``DeleteRecordsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(records_to_delete, "records_to_delete")
        # PartitionLeaderFuture's toUnmodifiableMap rejects a null key (3385) before
        # options (3388); the values are read by the handler.
        _conversions.check_keys(records_to_delete, "records_to_delete")
        options = _conversions.options_or_new(options, DeleteRecordsOptions)
        native = self._call(_lib.Admin_delete_records,
                            *_conversions.delete_records_args(records_to_delete),
                            options._setters_ran())
        return _conversions.delete_records_result(native, Sync, None)

    @overload
    def create_delegation_token(self) -> CreateDelegationTokenResult[Sync]: ...
    @overload
    def create_delegation_token(
        self, *,
        options: CreateDelegationTokenOptions,
    ) -> CreateDelegationTokenResult[Sync]: ...

    def create_delegation_token(self, *, options: CreateDelegationTokenOptions = MISSING
                                ) -> CreateDelegationTokenResult[Sync]:
        """Create a Delegation Token.

        This operation is supported by brokers with version 1.1.0 or higher.

        The following errors can be anticipated when calling ``get()`` on the future
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
        Returns the ``CreateDelegationTokenResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, CreateDelegationTokenOptions)
        native = self._call(_lib.Admin_create_delegation_token,
                            _conversions.create_delegation_token_options(options))
        return _conversions.create_delegation_token_result(native, Sync, None)

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
        """Renew a Delegation Token.

        This operation is supported by brokers with version 1.1.0 or higher.

        The following errors can be anticipated when calling ``get()`` on the future
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
.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, RenewDelegationTokenOptions)
        _conversions.non_null(hmac, "hmac")
        native = self._call(_lib.Admin_renew_delegation_token, hmac, options._setters_ran())
        return _conversions.renew_delegation_token_result(native, Sync, None)

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
        """Expire a Delegation Token.

        This operation is supported by brokers with version 1.1.0 or higher. Without
        ``options`` it expires the token immediately.

        The following errors can be anticipated when calling ``get()`` on the future
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
.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, ExpireDelegationTokenOptions)
        _conversions.non_null(hmac, "hmac")
        native = self._call(_lib.Admin_expire_delegation_token, hmac, options._setters_ran())
        return _conversions.expire_delegation_token_result(native, Sync, None)

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
        """Describe the Delegation Tokens.

        This operation is supported by brokers with version 1.1.0 or higher. Without
        ``options`` it returns all the user owned tokens and tokens where user have
        Describe permission.

        The following errors can be anticipated when calling ``get()`` on the future
        obtained from the ``delegation_tokens()`` method of the returned
        ``DescribeDelegationTokenResult``:

        - ``UnsupportedByAuthenticationError``: if the request sent on PLAINTEXT/1-way
          SSL channels or delegation token authenticated channels.
        - ``DelegationTokenDisabledError``: if the delegation token feature is disabled.
        - ``TimeoutError``: if the request was not completed in within the given
          ``DescribeDelegationTokenOptions.timeout_ms()``.

        ``options``: the options to use when describing delegation tokens (Java's
        ``describeDelegationToken()`` passes ``new DescribeDelegationTokenOptions()``).
        Returns the ``DescribeDelegationTokenResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeDelegationTokenOptions)
        native = self._call(_lib.Admin_describe_delegation_token,
                            _conversions.describe_delegation_token_options(options))
        return _conversions.describe_delegation_token_result(native, Sync, None)

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
        """Describe some consumer groups in the cluster.

        ``group_ids``: the IDs of the groups to describe; ``options``: the options to
        use when describing the groups (Java's ``describeConsumerGroups(groupIds)``
        passes ``new DescribeConsumerGroupsOptions()``). Returns the
        ``DescribeConsumerGroupsResult``.
        """
        reject_str("group_ids", group_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(group_ids, "group_ids")
        options = _conversions.options_or_new(options, DescribeConsumerGroupsOptions)
        native = self._call(_lib.Admin_describe_consumer_groups, _conversions.group_ids_list(group_ids),
                            options._setters_ran())
        return _conversions.describe_consumer_groups_result(native, Sync, None)

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

    @java_forms(Form("group_id", "options"), Form("group_id"),
                Form("group_specs", "options"), Form("group_specs"))
    def list_consumer_group_offsets(
        self, *, group_id: str = MISSING,
        group_specs: Mapping[str, ListConsumerGroupOffsetsSpec] = MISSING,
        options: ListConsumerGroupOffsetsOptions = MISSING
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
        ``ListConsumerGroupOffsetsResult``.
        """
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
            return _conversions.list_consumer_group_offsets_result([group_id], native, Sync, None)
        assert group_specs is not None  # the non_null above
        return _conversions.list_consumer_group_offsets_result(group_specs, native, Sync, None)

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
        """Delete consumer groups from the cluster.

        ``group_ids``: the groups to delete; ``options``: the options to use when
        deleting a consumer group (Java's ``deleteConsumerGroups(groupIds)`` passes
        ``new DeleteConsumerGroupsOptions()``). Returns the
        ``DeleteConsumerGroupsResult``.
        """
        reject_str("group_ids", group_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(group_ids, "group_ids")
        options = _conversions.options_or_new(options, DeleteConsumerGroupsOptions)
        native = self._call(_lib.Admin_delete_consumer_groups, _conversions.group_ids_list(group_ids),
                            options._setters_ran())
        return _conversions.delete_consumer_groups_result(native, Sync, None)

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
        """Delete committed offsets for a set of partitions in a consumer group. This
        will succeed at the partition level only if the group is not actively
        subscribed to the corresponding topic.

        ``group_id``: the group; ``partitions``: the partitions whose offsets to
        delete; ``options``: the options to use when deleting offsets in a consumer
        group (Java's ``deleteConsumerGroupOffsets(groupId, partitions)`` passes
        ``new DeleteConsumerGroupOffsetsOptions()``). Returns the
        ``DeleteConsumerGroupOffsetsResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, DeleteConsumerGroupOffsetsOptions)
        _conversions.non_null(group_id, "group_id")
        _conversions.non_null(partitions, "partitions")
        tps = list(partitions)
        native = self._call(_lib.Admin_delete_consumer_group_offsets,
                            *_conversions.delete_consumer_group_offsets_args(group_id, tps),
                            options._setters_ran())
        return _conversions.delete_consumer_group_offsets_result(tps, native, Sync, None)

    @overload
    def list_groups(self) -> ListGroupsResult[Sync]: ...
    @overload
    def list_groups(self, *, options: ListGroupsOptions) -> ListGroupsResult[Sync]: ...

    def list_groups(self, *, options: ListGroupsOptions = MISSING) -> ListGroupsResult[Sync]:
        """List the groups available in the cluster.

        ``options``: the options to use when listing the groups (Java's
        ``listGroups()`` passes ``new ListGroupsOptions()``). Returns the
        ``ListGroupsResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, ListGroupsOptions)
        native = self._call(_lib.Admin_list_groups, _conversions.list_groups_options(options))
        return _conversions.list_groups_result(native, Sync, None)

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
        ``ElectLeadersResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, ElectLeadersOptions)
        _conversions.non_null(election_type, "election_type")
        native = self._call(_lib.Admin_elect_leaders,
                            *_conversions.elect_leaders_args(election_type, partitions),
                            options._setters_ran())
        return _conversions.elect_leaders_result(native, Sync, None)

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
        ``new AlterPartitionReassignmentsOptions()``). Returns the result.
        """
        self._check_not_closed()
        _conversions.non_null(reassignments, "reassignments")
        args = _conversions.alter_partition_reassignments_args(reassignments)
        options = _conversions.options_or_new(options, AlterPartitionReassignmentsOptions)
        native = self._call(_lib.Admin_alter_partition_reassignments, *args, options._setters_ran())
        return _conversions.alter_partition_reassignments_result(native, Sync, None)

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
    @overload
    def list_partition_reassignments(
        self, *,
        partitions: Iterable[TopicPartition] | None,
        options: ListPartitionReassignmentsOptions,
    ) -> ListPartitionReassignmentsResult[Sync]: ...

    def list_partition_reassignments(
            self, *, partitions: Iterable[TopicPartition] | None = MISSING,
            options: ListPartitionReassignmentsOptions = MISSING
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

        ``partitions``: the partitions we want to get reassignment for, left out
        (Java's ``listPartitionReassignments()`` and
        ``listPartitionReassignments(options)``, which pass ``Optional.empty()``) if we
        want to get the reassignments for all partitions in the cluster; a ``None`` with
        ``options`` given is Java's ``listPartitionReassignments(Optional<Set>, options)``
        with ``Optional.empty()`` (all partitions), and without ``options`` Java's
        ``Optional.of(null)``, raising ``TypeError``; ``options``:
        the options to use (Java's short forms pass
        ``new ListPartitionReassignmentsOptions()``). Returns the result.
        """
        self._check_not_closed()
        targets: Iterable[TopicPartition] | None = None
        # Left out (Java's (options) form), or None with options given (Java's
        # (Optional<Set>, options) form with Optional.empty()): every partition.
        if partitions is not MISSING and (partitions is not None or options is MISSING):
            _conversions.non_null(partitions, "partitions")  # Optional.of(partitions)
            targets = partitions
        args = _conversions.list_partition_reassignments_args(targets)
        reads = _conversions.all_partitions_valid(args[0])
        options = _conversions.options_or_new(options, ListPartitionReassignmentsOptions, read=reads)
        native = self._call(_lib.Admin_list_partition_reassignments, *args, options._setters_ran())
        return _conversions.list_partition_reassignments_result(native, Sync, None)

    def remove_members_from_consumer_group(
        self, *, group_id: str, options: RemoveMembersFromConsumerGroupOptions
    ) -> RemoveMembersFromConsumerGroupResult[Sync]:
        """Remove members from the consumer group by given member identities.

        For possible error codes, refer to Java's ``LeaveGroupResponse``.

        ``group_id``: the ID of the group to remove member from; ``options``: the
        options to carry removing members' information. Returns the
        ``RemoveMembersFromConsumerGroupResult``.
        """
        self._check_not_closed()
        # Java reads options.reason() first; the group id only later, in the call.
        _conversions.non_null(options, "options")
        _conversions.non_null(group_id, "group_id")
        args = _conversions.remove_members_from_consumer_group_args(group_id, options)
        native = self._call(_lib.Admin_remove_members_from_consumer_group, *args,
                            options._setters_ran())
        return _conversions.remove_members_from_consumer_group_result(options, native, Sync, None)

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
        """Alters offsets for the specified group. In order to succeed, the group
        must be empty.

        This operation is not transactional so it may succeed for some partitions
        while fail for others.

        ``group_id``: the group for which to alter offsets; ``offsets``: a map of
        offsets by partition with associated metadata, partitions not specified in
        the map being ignored; ``options``: the options to use when altering the
        offsets (Java's ``alterConsumerGroupOffsets(groupId, offsets)`` passes ``new
        AlterConsumerGroupOffsetsOptions()``). Returns the
        ``AlterConsumerGroupOffsetsResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, AlterConsumerGroupOffsetsOptions)
        _conversions.non_null(group_id, "group_id")
        _conversions.non_null(offsets, "offsets")
        native = self._call(_lib.Admin_alter_consumer_group_offsets,
                            *_conversions.alter_consumer_group_offsets_args(group_id, offsets),
                            options._setters_ran())
        return _conversions.alter_consumer_group_offsets_result(native, Sync, None)

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
        """List offset for the specified partitions. This operation enables to find
        the beginning offset, end offset as well as the offset matching a timestamp
        in partitions.

        ``topic_partition_offsets``: the mapping from partition to the ``OffsetSpec``
        to look up; ``options``: the options to use when retrieving the offsets
        (Java's ``listOffsets(topicPartitionOffsets)`` passes
        ``new ListOffsetsOptions()``). Returns the ``ListOffsetsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(topic_partition_offsets, "topic_partition_offsets")
        args = _conversions.list_offsets_args(topic_partition_offsets)
        options = _conversions.options_or_new(options, ListOffsetsOptions)
        partitions = list(topic_partition_offsets)
        native = self._call(_lib.Admin_list_offsets, *args, options.isolation_level().id(), options._setters_ran())
        return _conversions.list_offsets_result(native, partitions, Sync, None)

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
        """Describes all entities matching the provided filter that have at least one
        client quota configuration value defined.

        The following errors can be anticipated when calling ``get()`` on the future
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
        containing the result.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeClientQuotasOptions)
        _conversions.non_null(filter, "filter")
        native = self._call(_lib.Admin_describe_client_quotas,
                            *_conversions.describe_client_quotas_args(filter),
                            options._setters_ran())
        return _conversions.describe_client_quotas_result(native, Sync, None)

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
        """Alters client quota configurations with the specified alterations.

        Alterations for a single entity are atomic, but across entities is not
        guaranteed. The resulting per-entity error code should be evaluated to resolve
        the success or failure of all updates.

        The following errors can be anticipated when calling ``get()`` on the
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
        Returns the ``AlterClientQuotasResult`` containing the result.
        """
        self._check_not_closed()
        _conversions.non_null(entries, "entries")
        args = _conversions.alter_client_quotas_args(entries)
        options = _conversions.options_or_new(options, AlterClientQuotasOptions)
        native = self._call(_lib.Admin_alter_client_quotas, *args, options._setters_ran())
        return _conversions.alter_client_quotas_result(native, Sync, None)

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

    @java_forms(Form(), Form("users"), Form("users", "options"))
    def describe_user_scram_credentials(
            self, *, users: Sequence[str] | None = MISSING,
            options: DescribeUserScramCredentialsOptions = MISSING
    ) -> DescribeUserScramCredentialsResult[Sync]:
        """Describe SASL/SCRAM credentials.

        The following errors can be anticipated when calling ``get()`` on the
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
        alone. Returns the ``DescribeUserScramCredentialsResult``.
        """
        if users is not MISSING and users is not None:
            reject_str("users", users)  # the shape check precedes the closed check
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeUserScramCredentialsOptions)
        if users is MISSING:
            users = None  # Java's describeUserScramCredentials() passes null: all users
        native = self._call(_lib.Admin_describe_user_scram_credentials,
                            *_conversions.describe_user_scram_credentials_args(users),
                            options._setters_ran())
        return _conversions.describe_user_scram_credentials_result(native, Sync, None)

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
        """Alter SASL/SCRAM credentials.

        The following errors can be anticipated when calling ``get()`` on any of the
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
        ``AlterUserScramCredentialsResult``.
        """
        self._check_not_closed()
        _conversions.non_null(alterations, "alterations")
        args = _conversions.alter_user_scram_credentials_args(alterations)
        options = _conversions.options_or_new(options, AlterUserScramCredentialsOptions)
        native = self._call(_lib.Admin_alter_user_scram_credentials, *args, options._setters_ran())
        return _conversions.alter_user_scram_credentials_result(native, Sync, None)

    @overload
    def describe_features(self) -> DescribeFeaturesResult[Sync]: ...
    @overload
    def describe_features(self, *,
                          options: DescribeFeaturesOptions) -> DescribeFeaturesResult[Sync]: ...

    def describe_features(self, *, options: DescribeFeaturesOptions = MISSING
                          ) -> DescribeFeaturesResult[Sync]:
        """Describes finalized as well as supported features. The request is issued to
        any random broker.

        The following errors can be anticipated when calling ``get()`` on the future
        from the returned ``DescribeFeaturesResult``:

        - ``TimeoutError``: if the request timed out before the describe operation could
          finish.

        ``options``: the options to use (Java's ``describeFeatures()`` passes ``new
        DescribeFeaturesOptions()``). Returns the ``DescribeFeaturesResult`` containing
        the result.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, DescribeFeaturesOptions)
        native = self._call(_lib.Admin_describe_features, options._setters_ran())
        return _conversions.describe_features_result(native, Sync, None)

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

        The following errors can be anticipated when calling ``get()`` on the futures
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
        containing the result.
        """
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
        return _conversions.update_features_result(native, Sync, None)

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
        """Unregister a controller.

        The following errors can be anticipated when calling ``get()`` on the future
        from the returned ``UnregisterControllerResult``:

        - ``TimeoutError`` If the request timed out before the unregister operation
          could finish.
        - ``UnsupportedVersionError`` If the software is too old to support the
          unregistration API.
        - ``ControllerIdNotRegisteredError`` If the requested controller id is not
          currently registered.
        - ``NotControllerError`` If the request does not arrive at the active
          controller.
        - ``InvalidRequestError`` If the request tries to unregister the current active
          controller id.

        ``controller_id``: the controller id to unregister; ``options``: the options to
        use (Java's ``unregisterController(controllerId)`` passes
        ``new UnregisterControllerOptions()``). Returns the
        ``UnregisterControllerResult`` containing the result.
        """
        self._check_not_closed()
        _conversions.non_null(controller_id, "controller_id")
        options = _conversions.options_or_new(options, UnregisterControllerOptions)
        native = self._call(_lib.Admin_unregister_controller, controller_id,
                            options._setters_ran())
        return _conversions.unregister_controller_result(native, Sync, None)

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
        """Describe active producer state on a set of topic partitions. Unless a
        specific broker is requested through ``DescribeProducersOptions.broker_id()``,
        this will query the partition leader to find the producer state.

        ``partitions``: the set of partitions to query; ``options``: options to control
        the method behavior (Java's ``describeProducers(partitions)`` passes ``new
        DescribeProducersOptions()``). Returns the result.
        """
        self._check_not_closed()
        _conversions.non_null(partitions, "partitions")
        targets = list(partitions)
        # PartitionLeaderFuture's toUnmodifiableMap rejects a null key (4997) before
        # options (4998).
        args = _conversions.describe_producers_args(targets)
        options = _conversions.options_or_new(options, DescribeProducersOptions)
        native = self._call(_lib.Admin_describe_producers, *args, options._setters_ran())
        return _conversions.describe_producers_result(native, Sync, None, targets)

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
        """Describe the state of a set of transactional IDs from the respective
        transaction coordinators, which are dynamically discovered.

        ``transactional_ids``: the set of transactional IDs to query; ``options``:
        options to control the method behavior (Java's
        ``describeTransactions(transactionalIds)`` passes ``new
        DescribeTransactionsOptions()``). Returns the result.
        """
        reject_str("transactional_ids", transactional_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(transactional_ids, "transactional_ids")
        options = _conversions.options_or_new(options, DescribeTransactionsOptions)
        ids = _conversions.transactional_ids_list(transactional_ids)
        native = self._call(_lib.Admin_describe_transactions,
                            *_conversions.describe_transactions_args(ids), options._setters_ran())
        return _conversions.describe_transactions_result(native, Sync, None, ids)

    @overload
    def abort_transaction(self, *, spec: AbortTransactionSpec) -> AbortTransactionResult[Sync]: ...
    @overload
    def abort_transaction(self, *, spec: AbortTransactionSpec,
                          options: AbortTransactionOptions) -> AbortTransactionResult[Sync]: ...

    def abort_transaction(self, *, spec: AbortTransactionSpec,
                          options: AbortTransactionOptions = MISSING
                          ) -> AbortTransactionResult[Sync]:
        """Forcefully abort a transaction which is open on a topic partition. This will
        send a ``WriteTxnMarkers`` request to the partition leader in order to abort the
        transaction. This requires administrative privileges.

        ``spec``: the transaction specification including topic partition and producer
        details; ``options``: options to control the method behavior (including
        filters) (Java's ``abortTransaction(spec)`` passes ``new
        AbortTransactionOptions()``). Returns the result.
        """
        self._check_not_closed()
        _conversions.non_null(spec, "spec")
        options = _conversions.options_or_new(options, AbortTransactionOptions)
        native = self._call(_lib.Admin_abort_transaction, *_conversions.abort_transaction_args(spec),
                            options._setters_ran())
        return _conversions.abort_transaction_result(native, Sync, None, spec)

    @overload
    def list_transactions(self) -> ListTransactionsResult[Sync]: ...
    @overload
    def list_transactions(self, *,
                          options: ListTransactionsOptions) -> ListTransactionsResult[Sync]: ...

    def list_transactions(self, *, options: ListTransactionsOptions = MISSING
                          ) -> ListTransactionsResult[Sync]:
        """List active transactions in the cluster. This will query all potential
        transaction coordinators in the cluster and collect the state of all
        transactions. Users should typically attempt to reduce the size of the result set
        using ``ListTransactionsOptions.filter_producer_ids()`` or
        ``ListTransactionsOptions.filter_states()`` or
        ``ListTransactionsOptions.filter_on_duration()``.

        ``options``: options to control the method behavior (including filters) (Java's
        ``listTransactions()`` passes ``new ListTransactionsOptions()``). Returns the
        result.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, ListTransactionsOptions)
        native = self._call(_lib.Admin_list_transactions,
                            _conversions.list_transactions_options(options))
        return _conversions.list_transactions_result(native, Sync, None)

    @overload
    def fence_producers(self, *,
                        transactional_ids: Iterable[str]) -> FenceProducersResult[Sync]: ...
    @overload
    def fence_producers(self, *, transactional_ids: Iterable[str],
                        options: FenceProducersOptions) -> FenceProducersResult[Sync]: ...

    def fence_producers(self, *, transactional_ids: Iterable[str],
                        options: FenceProducersOptions = MISSING
                        ) -> FenceProducersResult[Sync]:
        """Fence out all active producers that use any of the provided transactional
        IDs.

        ``transactional_ids``: the IDs of the producers to fence; ``options``: the
        options to use when fencing the producers (Java's
        ``fenceProducers(transactionalIds)`` passes ``new FenceProducersOptions()``).
        Returns the ``FenceProducersResult``.
        """
        reject_str("transactional_ids", transactional_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(transactional_ids, "transactional_ids")
        options = _conversions.options_or_new(options, FenceProducersOptions)
        ids = _conversions.transactional_ids_list(transactional_ids)
        native = self._call(_lib.Admin_fence_producers, *_conversions.fence_producers_args(ids),
                            options._setters_ran())
        return _conversions.fence_producers_result(native, Sync, None, ids)

    @overload
    def list_config_resources(
        self, *,
        config_resource_types: Iterable[ConfigResource.Type],
        options: ListConfigResourcesOptions,
    ) -> ListConfigResourcesResult[Sync]: ...
    @overload
    def list_config_resources(self) -> ListConfigResourcesResult[Sync]: ...

    @java_forms(Form("config_resource_types", "options",
                     defaults={"config_resource_types": (), "options": MISSING}),
                Form())
    def list_config_resources(
            self, *, config_resource_types: Iterable[ConfigResource.Type] = MISSING,
            options: ListConfigResourcesOptions = MISSING
            ) -> ListConfigResourcesResult[Sync]:
        """List the configuration resources available in the cluster which matches
        config resource type. If no config resource types are specified, all
        configuration resources will be listed.

        ``config_resource_types``: the set of configuration resource types to list;
        ``options``: the options to use when listing the configuration resources
        (Java's ``listConfigResources()`` passes ``Set.of()`` and
        ``new ListConfigResourcesOptions()``). Returns the
        ``ListConfigResourcesResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, ListConfigResourcesOptions)
        _conversions.non_null(config_resource_types, "config_resource_types")
        native = self._call(_lib.Admin_list_config_resources,
                            *_conversions.list_config_resources_args(config_resource_types),
                            options._setters_ran())
        return _conversions.list_config_resources_result(native, Sync, None)

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
        """Describe some classic groups in the cluster.

        ``group_ids``: the IDs of the groups to describe; ``options``: the options to
        use when describing the groups (Java's ``describeClassicGroups(groupIds)``
        passes ``new DescribeClassicGroupsOptions()``). Returns the
        ``DescribeClassicGroupsResult``.
        """
        reject_str("group_ids", group_ids)  # the shape check precedes the closed check
        self._check_not_closed()
        _conversions.non_null(group_ids, "group_ids")
        options = _conversions.options_or_new(options, DescribeClassicGroupsOptions)
        native = self._call(_lib.Admin_describe_classic_groups, _conversions.group_ids_list(group_ids),
                            options._setters_ran())
        return _conversions.describe_classic_groups_result(native, Sync, None)

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
        """Force terminate a transaction for the given transactional ID.

        This operation aborts any ongoing transaction associated with the transactional
        ID. It's similar to ``fence_producers`` but only targets a single transactional
        ID to handle long-running transactions when 2PC is enabled.

        ``transactional_id``: the ID of the transaction to terminate; ``options``: the
        options to use when terminating the transaction (Java's
        ``forceTerminateTransaction(transactionalId)`` passes ``new
        TerminateTransactionOptions()``). Returns the ``TerminateTransactionResult``.
        """
        self._check_not_closed()
        options = _conversions.options_or_new(options, TerminateTransactionOptions)
        _conversions.non_null(transactional_id, "transactional_id")
        native = self._call(_lib.Admin_force_terminate_transaction, transactional_id,
                            options._setters_ran())
        return _conversions.force_terminate_transaction_result(native, Sync, None)
