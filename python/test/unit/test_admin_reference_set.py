# Copyright 2025 Confluent Inc.
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

"""The admin Options / Result reference set (``confluent_kafka/admin/_reference_set.md``).

Behaviour tests of each hand-written reference class: the keyword constructor forms
and their ``java_forms`` messages, the getters and setters, ``__eq__`` / ``__hash__`` /
``__str__``, and the futures of the result classes for the sync and the async client
kinds. Java's own tests of these classes are translated here too:
``ListGroupsOptionsTest``, ``RemoveMembersFromConsumerGroupOptionsTest`` and
``DeleteTopicsResultTest``; the other reference classes have no Java test.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from confluent_kafka import ExecutionError
from confluent_kafka.admin._futures import Async, Sync, _kind_of
from confluent_kafka.admin.abstract_options import AbstractOptions
from confluent_kafka.admin.alter_configs_result import AlterConfigsResult
from confluent_kafka.admin.config import Config
from confluent_kafka.admin.create_delegation_token_options import CreateDelegationTokenOptions
from confluent_kafka.admin.create_topics_options import CreateTopicsOptions
from confluent_kafka.admin.create_topics_result import (
    CreateTopicsResult,
    _TopicMetadataAndConfig,
)
from confluent_kafka.admin.delete_records_options import DeleteRecordsOptions
from confluent_kafka.admin.delete_topics_result import DeleteTopicsResult
from confluent_kafka.admin.describe_cluster_result import DescribeClusterResult
from confluent_kafka.admin.describe_features_options import DescribeFeaturesOptions
from confluent_kafka.admin.describe_topics_options import DescribeTopicsOptions
from confluent_kafka.admin.list_groups_options import ListGroupsOptions
from confluent_kafka.admin.list_offsets_result import ListOffsetsResult
from confluent_kafka.admin.list_topics_options import ListTopicsOptions
from confluent_kafka.admin.list_topics_result import ListTopicsResult
from confluent_kafka.admin.member_to_remove import MemberToRemove
from confluent_kafka.admin.remove_members_from_consumer_group_options import (
    RemoveMembersFromConsumerGroupOptions,
)
from confluent_kafka.admin.topic_listing import TopicListing
from confluent_kafka.common import (
    AclOperation,
    GroupState,
    GroupType,
    KafkaPrincipal,
    Node,
    TopicPartition,
    Uuid,
)
from confluent_kafka.common.config import ConfigResource
from confluent_kafka.common.errors import TopicAuthorizationError
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError

# Fix round 1 additions.
from confluent_kafka.admin._internals import _CoordinatorKey, _sub_level_error
from confluent_kafka.admin.delete_consumer_group_offsets_result import (
    DeleteConsumerGroupOffsetsResult,
)
from confluent_kafka.admin.delete_consumer_groups_result import (
    DeleteConsumerGroupsResult,
)
from confluent_kafka.admin.describe_producers_options import DescribeProducersOptions
from confluent_kafka.admin.describe_transactions_result import (
    DescribeTransactionsResult,
)
from confluent_kafka.admin.list_offsets_options import ListOffsetsOptions
from confluent_kafka.admin.list_transactions_options import ListTransactionsOptions
from confluent_kafka.admin.list_transactions_result import ListTransactionsResult
from confluent_kafka.admin.transaction_description import TransactionDescription
from confluent_kafka.admin.transaction_listing import TransactionListing
from confluent_kafka.admin.transaction_state import TransactionState
from confluent_kafka.common import AsyncKafkaFuture, IsolationLevel, KafkaFuture
from confluent_kafka.common.kafka_future import _KafkaFutureImpl
from confluent_kafka.common.errors import UnknownTopicOrPartitionError

# ``ConsumerProtocol.PROTOCOL_TYPE``.
PROTOCOL_TYPE = "consumer"


def _new_future(kind: type[Sync] | type[Async]) -> Any:
    """Java's ``new KafkaFutureImpl<>()`` of the kind: a future over a
    ``_KafkaFutureImpl``, completed through it."""
    return kind._FUTURE._of_impl(_KafkaFutureImpl())


def _complete(future: Any, value: Any) -> bool:
    return bool(future._impl.complete(value))


def _complete_with_error(future: Any, error: BaseException) -> bool:
    return bool(future._impl.complete_with_error(error))


def _done(kind: type[Sync] | type[Async], value: Any) -> Any:
    return kind._FUTURE.completed_future(value=value)


def _failed(kind: type[Sync] | type[Async], error: BaseException) -> Any:
    future = _new_future(kind)
    _complete_with_error(future, error)
    return future


def _result(future: Any, timeout: float = 1) -> Any:
    """A sync future's value; its failure raised as it is (``get()``'s cause)."""
    try:
        return future.get(timeout=timeout)
    except ExecutionError as error:
        assert error.__cause__ is not None
        raise error.__cause__ from None


def _exception(future: Any, timeout: float = 1) -> BaseException | None:
    """A sync future's failure (``get()``'s cause), or ``None``."""
    try:
        future.get(timeout=timeout)
    except ExecutionError as error:
        return error.__cause__
    return None


async def _await(future: Any) -> Any:
    """An ``AsyncKafkaFuture``'s value; its failure raised as it is."""
    assert isinstance(future, AsyncKafkaFuture)
    try:
        return await future.get(timeout=1)
    except ExecutionError as error:
        assert error.__cause__ is not None
        raise error.__cause__ from None


# --------------------------------------------------------------------------------------
# AbstractOptions and the Options shapes
# --------------------------------------------------------------------------------------


class TestAbstractOptions:
    def test_construction_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match=r"^AbstractOptions is an abstract class; "
                                            r"use one of its subclasses$"):
            AbstractOptions()

    def test_subclasses_are_abstract_options(self) -> None:
        for cls in (DeleteRecordsOptions, CreateTopicsOptions, DescribeTopicsOptions,
                    DescribeFeaturesOptions, ListGroupsOptions, ListTopicsOptions,
                    CreateDelegationTokenOptions, RemoveMembersFromConsumerGroupOptions):
            assert isinstance(cls(), AbstractOptions)


class TestDeleteRecordsOptions:
    """Shape: only the inherited ``timeoutMs``."""

    def test_defaults(self) -> None:
        assert DeleteRecordsOptions().timeout_ms() is None

    def test_keyword_constructor(self) -> None:
        assert DeleteRecordsOptions(timeout_ms=5000).timeout_ms() == 5000
        # An explicit None calls the setter with Java's null.
        assert DeleteRecordsOptions(timeout_ms=None).timeout_ms() is None

    def test_setter_returns_self_and_none_sets_null(self) -> None:
        options = DeleteRecordsOptions()
        assert options.timeout_ms(timeout_ms=10) is options
        assert options.timeout_ms() == 10
        assert options.timeout_ms(timeout_ms=None) is options
        assert options.timeout_ms() is None

    def test_positional_calls_raise_type_error(self) -> None:
        with pytest.raises(TypeError):
            DeleteRecordsOptions(5000)  # type: ignore[misc]
        with pytest.raises(TypeError):
            DeleteRecordsOptions().timeout_ms(5000)  # type: ignore[call-overload]
        with pytest.raises(TypeError):
            DeleteRecordsOptions(validate_only=True)  # type: ignore[call-arg]


class TestCreateTopicsOptions:
    """Shapes: re-declared ``timeoutMs``; setter and getter with different names."""

    def test_defaults(self) -> None:
        options = CreateTopicsOptions()
        assert options.timeout_ms() is None
        assert options.should_validate_only() is False
        assert options.should_retry_on_quota_violation() is True

    def test_keyword_constructor(self) -> None:
        options = CreateTopicsOptions(timeout_ms=5000, validate_only=True,
                                      retry_on_quota_violation=False)
        assert options.timeout_ms() == 5000
        assert options.should_validate_only() is True
        assert options.should_retry_on_quota_violation() is False

    def test_keyword_left_out_keeps_java_starting_value(self) -> None:
        options = CreateTopicsOptions(validate_only=True)
        assert options.timeout_ms() is None
        assert options.should_retry_on_quota_violation() is True

    def test_setters_chain(self) -> None:
        options = CreateTopicsOptions()
        assert (options.timeout_ms(timeout_ms=1).validate_only(validate_only=True)
                .retry_on_quota_violation(retry_on_quota_violation=False)) is options
        assert options.timeout_ms() == 1
        assert options.should_validate_only() is True
        assert options.should_retry_on_quota_violation() is False

    def test_setters_are_keyword_only(self) -> None:
        with pytest.raises(TypeError):
            CreateTopicsOptions().validate_only(True)  # type: ignore[misc]
        with pytest.raises(TypeError):
            CreateTopicsOptions().validate_only()  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            CreateTopicsOptions(5000)  # type: ignore[misc]


class TestDescribeTopicsOptions:
    """Shape: one-name setter + getter pairs (not nullable)."""

    def test_defaults(self) -> None:
        options = DescribeTopicsOptions()
        assert options.include_authorized_operations() is False
        assert options.partition_size_limit_per_response() == 2000

    def test_keyword_constructor(self) -> None:
        options = DescribeTopicsOptions(timeout_ms=7, include_authorized_operations=True,
                                        partition_size_limit_per_response=10)
        assert options.timeout_ms() == 7
        assert options.include_authorized_operations() is True
        assert options.partition_size_limit_per_response() == 10

    def test_getter_and_setter_share_a_name(self) -> None:
        options = DescribeTopicsOptions()
        assert options.include_authorized_operations(include_authorized_operations=True) is options
        assert options.include_authorized_operations() is True
        assert options.partition_size_limit_per_response(
            partition_size_limit_per_response=5) is options
        assert options.partition_size_limit_per_response() == 5
        assert options.timeout_ms(timeout_ms=3) is options

    def test_positional_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            DescribeTopicsOptions().include_authorized_operations(True)  # type: ignore[call-overload]


class TestDescribeFeaturesOptions:
    """Shape: a starting value with no value of the setter's type (``= ...``)."""

    def test_defaults(self) -> None:
        assert DescribeFeaturesOptions().node_id() is None

    def test_setter_stores_the_node_id(self) -> None:
        options = DescribeFeaturesOptions()
        assert options.node_id(node_id=3) is options
        assert options.node_id() == 3

    def test_keyword_constructor(self) -> None:
        assert DescribeFeaturesOptions(node_id=1).node_id() == 1
        assert DescribeFeaturesOptions(timeout_ms=2).timeout_ms() == 2


class TestListGroupsOptions:
    """Shapes: setters storing a changed value; static factories. The Java tests are
    ``ListGroupsOptionsTest``."""

    def _change_all(self, options: ListGroupsOptions) -> None:
        options.in_group_states(group_states={GroupState.STABLE})
        options.with_types(types={GroupType.CONSUMER})
        options.with_protocol_types(protocol_types={PROTOCOL_TYPE})
        assert options.group_states() == {GroupState.STABLE}
        assert options.types() == {GroupType.CONSUMER}
        assert options.protocol_types() == {PROTOCOL_TYPE}

    def test_for_consumer_groups(self) -> None:
        options = ListGroupsOptions.for_consumer_groups()
        assert not options.group_states()
        assert options.types() == {GroupType.CONSUMER, GroupType.CLASSIC}
        assert options.protocol_types() == {"", PROTOCOL_TYPE}
        self._change_all(options)

    def test_for_share_groups(self) -> None:
        options = ListGroupsOptions.for_share_groups()
        assert not options.group_states()
        assert options.types() == {GroupType.SHARE}
        assert not options.protocol_types()
        self._change_all(options)

    def test_for_streams_groups(self) -> None:
        options = ListGroupsOptions.for_streams_groups()
        assert not options.group_states()
        assert options.types() == {GroupType.STREAMS}
        assert not options.protocol_types()
        self._change_all(options)

    def test_group_states(self) -> None:
        options = ListGroupsOptions()
        assert not options.group_states()
        options.in_group_states(group_states={GroupState.DEAD})
        assert options.group_states() == {GroupState.DEAD}
        group_states = set(GroupState)
        options = ListGroupsOptions().in_group_states(group_states=group_states)
        assert options.group_states() == group_states

    def test_consumer_group_states(self) -> None:
        options = ListGroupsOptions()
        assert not options.group_states()
        group_states = GroupState.group_states_for_type(type=GroupType.CONSUMER)
        options = ListGroupsOptions().in_group_states(group_states=group_states)
        assert options.group_states() == group_states

    def test_protocol_types(self) -> None:
        options = ListGroupsOptions()
        assert not options.protocol_types()
        options.with_protocol_types(protocol_types={PROTOCOL_TYPE})
        assert options.protocol_types() == {PROTOCOL_TYPE}
        protocol_types = {"", "consumer", "share"}
        options = ListGroupsOptions().with_protocol_types(protocol_types=protocol_types)
        assert options.protocol_types() == protocol_types

    def test_types(self) -> None:
        options = ListGroupsOptions()
        assert not options.types()
        options.with_types(types={GroupType.CLASSIC})
        assert options.types() == {GroupType.CLASSIC}
        group_types = set(GroupType)
        options = ListGroupsOptions().with_types(types=group_types)
        assert options.types() == group_types

    # The rules' own behaviour, beyond ListGroupsOptionsTest.

    def test_null_or_empty_is_the_empty_set(self) -> None:
        options = ListGroupsOptions(types={GroupType.SHARE})
        assert options.with_types(types=None).types() == set()
        options = ListGroupsOptions(types={GroupType.SHARE})
        assert options.with_types(types=()).types() == set()

    def test_setter_copies_and_getter_returns_a_new_set(self) -> None:
        given = {GroupType.CLASSIC}
        options = ListGroupsOptions().with_types(types=given)
        given.add(GroupType.SHARE)
        assert options.types() == {GroupType.CLASSIC}
        options.types().add(GroupType.STREAMS)
        assert options.types() == {GroupType.CLASSIC}

    def test_null_argument_is_not_checked(self) -> None:
        # An argument Java dereferences implicitly is not
        # checked; Python raises what it naturally raises.
        with pytest.raises(TypeError):
            ListGroupsOptions().with_types(types=5)  # type: ignore[arg-type]

    def test_keyword_constructor_is_named_after_fields(self) -> None:
        options = ListGroupsOptions(timeout_ms=1, group_states=[GroupState.EMPTY],
                                    types=[GroupType.CONSUMER], protocol_types=["consumer"])
        assert options.timeout_ms() == 1
        assert options.group_states() == {GroupState.EMPTY}
        assert options.types() == {GroupType.CONSUMER}
        assert options.protocol_types() == {"consumer"}
        # An explicit None calls the setter with Java's null: the empty set.
        assert ListGroupsOptions(types=None).types() == set()


class TestListTopicsOptions:
    """Shape: ``equals`` / ``hashCode`` / ``toString``."""

    def test_defaults_and_setter(self) -> None:
        options = ListTopicsOptions()
        assert options.should_list_internal() is False
        assert options.list_internal(list_internal=True) is options
        assert options.should_list_internal() is True
        assert ListTopicsOptions(list_internal=True).should_list_internal() is True

    def test_equals_and_hash_read_list_internal_only(self) -> None:
        a = ListTopicsOptions(list_internal=True, timeout_ms=1)
        b = ListTopicsOptions(list_internal=True, timeout_ms=2)
        assert a == b
        assert hash(a) == hash(b)
        assert a != ListTopicsOptions()
        assert a != CreateTopicsOptions()
        assert a != "ListTopicsOptions(listInternal=True)"

    def test_str(self) -> None:
        assert str(ListTopicsOptions()) == "ListTopicsOptions(listInternal=False)"
        assert str(ListTopicsOptions(list_internal=True)) == "ListTopicsOptions(listInternal=True)"


class TestCreateDelegationTokenOptions:
    """Shape: several Java setters for one field (the deprecated one not generated);
    a nullable argument of a setter sharing its getter's name."""

    def test_defaults(self) -> None:
        options = CreateDelegationTokenOptions()
        assert options.max_lifetime_ms() == -1
        assert options.renewers() == []
        assert options.owner() is None

    def test_keyword_constructor(self) -> None:
        owner = KafkaPrincipal(principal_type="User", name="alice")
        renewer = KafkaPrincipal(principal_type="User", name="bob")
        options = CreateDelegationTokenOptions(max_lifetime_ms=1000, renewers=[renewer],
                                               owner=owner)
        assert options.max_lifetime_ms() == 1000
        assert options.renewers() == [renewer]
        assert options.owner() == owner

    def test_owner_none_sets_null(self) -> None:
        options = CreateDelegationTokenOptions(owner=KafkaPrincipal(principal_type="User",
                                                                    name="alice"))
        assert options.owner(owner=None) is options
        assert options.owner() is None

    def test_deprecated_setter_is_not_generated(self) -> None:
        assert not hasattr(CreateDelegationTokenOptions, "maxlife_time_ms")


class TestRemoveMembersFromConsumerGroupOptions:
    """Shape: Java constructors that take arguments (no keyword constructor) and a
    setter returning ``void``. The Java test is
    ``RemoveMembersFromConsumerGroupOptionsTest``."""

    def test_constructor(self) -> None:
        options = RemoveMembersFromConsumerGroupOptions(
            members=[MemberToRemove(group_instance_id="instance-1")])
        assert options.members() == {MemberToRemove(group_instance_id="instance-1")}
        # Construct will fail if illegal empty members provided
        with pytest.raises(IllegalArgumentError,
                           match=r"^Invalid empty members has been provided$"):
            RemoveMembersFromConsumerGroupOptions(members=[])

    def test_no_members_removes_all(self) -> None:
        options = RemoveMembersFromConsumerGroupOptions()
        assert options.members() == set()
        assert options.remove_all() is True
        # Left out (or MISSING) is Java's (); a None is Java's null, whose
        # members.isEmpty() throws.
        from confluent_kafka._args import MISSING

        assert RemoveMembersFromConsumerGroupOptions(members=MISSING).remove_all() is True
        with pytest.raises(TypeError, match=r"^members must not be None$"):
            RemoveMembersFromConsumerGroupOptions(members=None)  # type: ignore[call-overload]
        assert RemoveMembersFromConsumerGroupOptions(
            members=[MemberToRemove(group_instance_id="i")]).remove_all() is False

    def test_reason_setter_returns_none(self) -> None:
        options = RemoveMembersFromConsumerGroupOptions()
        assert options.reason() is None
        assert options.reason(reason="bye") is None
        assert options.reason() == "bye"
        options.reason(reason=None)
        assert options.reason() is None

    def test_no_setter_keywords(self) -> None:
        with pytest.raises(TypeError):
            RemoveMembersFromConsumerGroupOptions(timeout_ms=1)  # type: ignore[call-arg]
        assert RemoveMembersFromConsumerGroupOptions().timeout_ms(timeout_ms=1).timeout_ms() == 1


# --------------------------------------------------------------------------------------
# The private future helpers
# --------------------------------------------------------------------------------------


class TestFutureHelpers:
    """The client-kind marker (``_futures``); the futures are the Phase 1
    ``KafkaFuture`` / ``AsyncKafkaFuture``, tested by ``test_kafka_future.py``."""

    def test_markers_have_no_instances(self) -> None:
        with pytest.raises(TypeError):
            Sync()
        with pytest.raises(TypeError):
            Async()

    def test_markers_name_their_future_class(self) -> None:
        assert Sync._FUTURE is KafkaFuture
        assert Async._FUTURE is AsyncKafkaFuture

    def test_kind_of(self) -> None:
        assert _kind_of([]) is Sync
        assert _kind_of([_done(Sync, 1)]) is Sync
        assert _kind_of([_done(Async, 1)]) is Async


# --------------------------------------------------------------------------------------
# The Result shapes
# --------------------------------------------------------------------------------------


def _resource(name: str) -> ConfigResource:
    return ConfigResource(type=ConfigResource.Type.TOPIC, name=name)


class TestResultConstruction:
    @pytest.mark.parametrize("cls, method", [
        (AlterConfigsResult, "incremental_alter_configs()"),
        (ListTopicsResult, "list_topics()"),
        (DescribeClusterResult, "describe_cluster()"),
        (DeleteTopicsResult, "delete_topics()"),
        (CreateTopicsResult, "create_topics()"),
    ])
    def test_init_names_the_method_that_returns_it(self, cls: type, method: str) -> None:
        with pytest.raises(TypeError) as info:
            cls()
        assert str(info.value) == (f"{cls.__name__} cannot be constructed directly; use the "
                                   f"admin client's {method}")


class TestAlterConfigsResult:
    """Shape: per-key futures map plus ``allOf``."""

    def test_sync(self) -> None:
        error = TopicAuthorizationError(message="denied")
        futures = {_resource("a"): _done(Sync, None), _resource("b"): _failed(Sync, error)}
        result = AlterConfigsResult._new(futures=futures, kind=Sync)
        assert result.values() == futures
        assert _result(result.values()[_resource("a")]) is None
        assert _exception(result.all()) is error

    def test_sync_all_succeeds(self) -> None:
        result = AlterConfigsResult._new(futures={_resource("a"): _done(Sync, None)}, kind=Sync)
        assert _result(result.all()) is None
        assert _result(AlterConfigsResult._new(futures={}, kind=Sync).all()) is None

    async def test_async(self) -> None:
        result = AlterConfigsResult._new(futures={_resource("a"): _done(Async, None)}, kind=Async)
        assert isinstance(result.all(), AsyncKafkaFuture)
        assert (await _await(result.all())) is None
        empty = AlterConfigsResult._new(futures={}, kind=Async)
        assert isinstance(empty.all(), AsyncKafkaFuture)
        assert (await _await(empty.all())) is None


class TestListTopicsResult:
    """Shape: one future plus ``thenApply`` views."""

    LISTINGS = {
        "a": TopicListing(name="a", topic_id=Uuid(most_sig_bits=1, least_sig_bits=1),
                          internal=False),
        "b": TopicListing(name="b", topic_id=Uuid(most_sig_bits=2, least_sig_bits=2),
                          internal=True),
    }

    def test_sync(self) -> None:
        result = ListTopicsResult._new(future=_done(Sync, dict(self.LISTINGS)), kind=Sync)
        assert _result(result.names_to_listings()) == self.LISTINGS
        assert _result(result.names()) == {"a", "b"}
        assert sorted(listing.name() for listing in _result(result.listings())) == ["a", "b"]
        assert isinstance(_result(result.listings()), list)

    def test_failure_propagates(self) -> None:
        error = TopicAuthorizationError(message="denied")
        result = ListTopicsResult._new(future=_failed(Sync, error), kind=Sync)
        assert _exception(result.names()) is error
        assert _exception(result.listings()) is error

    async def test_async(self) -> None:
        result = ListTopicsResult._new(future=_done(Async, dict(self.LISTINGS)), kind=Async)
        assert (await _await(result.names())) == {"a", "b"}
        assert len(await _await(result.listings())) == 2


class TestDescribeClusterResult:
    """Shape: several independent futures."""

    def test_sync(self) -> None:
        node = Node(id=1, host="h", port=9092)
        error = TopicAuthorizationError(message="x")
        result = DescribeClusterResult._new(
            nodes=_done(Sync, [node]), controller=_done(Sync, node),
            cluster_id=_done(Sync, "cid"), authorized_operations=_failed(Sync, error),
            kind=Sync)
        assert _result(result.nodes()) == [node]
        assert _result(result.controller()) == node
        assert _result(result.cluster_id()) == "cid"
        assert _exception(result.authorized_operations()) is error

    async def test_async(self) -> None:
        result = DescribeClusterResult._new(
            nodes=_done(Async, []), controller=_done(Async, None),
            cluster_id=_done(Async, "cid"),
            authorized_operations=_done(Async, {AclOperation.READ}), kind=Async)
        assert (await _await(result.cluster_id())) == "cid"
        assert (await _await(result.authorized_operations())) == {AclOperation.READ}


class TestDeleteTopicsResult:
    """Shape: two exclusive maps, private static factories. The Java tests are
    ``DeleteTopicsResultTest``."""

    def test_delete_topics_result_with_names(self) -> None:
        future = _new_future(Sync)
        _complete(future, None)
        topic_names = {"foo": future}
        topic_name_futures = DeleteTopicsResult._of_topic_names(name_futures=topic_names,
                                                                kind=Sync)
        assert topic_name_futures.topic_name_values() == topic_names
        assert topic_name_futures.topic_id_values() is None
        assert topic_name_futures.all().is_done()

    def test_delete_topics_result_with_ids(self) -> None:
        future = _new_future(Sync)
        _complete(future, None)
        topic_ids = {Uuid.random_uuid(): future}
        topic_id_futures = DeleteTopicsResult._of_topic_ids(topic_id_futures=topic_ids, kind=Sync)
        assert topic_id_futures.topic_id_values() == topic_ids
        assert topic_id_futures.topic_name_values() is None
        assert topic_id_futures.all().is_done()

    def test_invalid_configurations(self) -> None:
        with pytest.raises(IllegalArgumentError,
                           match=r"^topicIdFutures and nameFutures cannot both be None\.$"):
            DeleteTopicsResult._new(topic_id_futures=None, name_futures=None, kind=Sync)
        with pytest.raises(IllegalArgumentError,
                           match=r"^topicIdFutures and nameFutures cannot both be specified\.$"):
            DeleteTopicsResult._new(topic_id_futures={}, name_futures={}, kind=Sync)

    async def test_async(self) -> None:
        result = DeleteTopicsResult._of_topic_names(name_futures={"foo": _done(Async, None)},
                                                    kind=Async)
        assert (await _await(result.all())) is None


class TestCreateTopicsResult:
    """Shape: futures carrying the private helper ``_TopicMetadataAndConfig``."""

    TOPIC_ID = Uuid(most_sig_bits=3, least_sig_bits=4)

    def _result(self, kind: type[Sync] | type[Async]) -> Any:
        ok = _TopicMetadataAndConfig(topic_id=self.TOPIC_ID, num_partitions=3,
                                     replication_factor=2, config=Config(entries=[]))
        denied = _TopicMetadataAndConfig(exception=TopicAuthorizationError(message="denied"))
        return CreateTopicsResult._new(futures={"ok": _done(kind, ok),
                                                "denied": _done(kind, denied)}, kind=kind)

    def test_sync_getters(self) -> None:
        result = self._result(Sync)
        assert _result(result.topic_id(topic="ok")) == self.TOPIC_ID
        assert _result(result.num_partitions(topic="ok")) == 3
        assert _result(result.replication_factor(topic="ok")) == 2
        assert _result(result.config(topic="ok")) == Config(entries=[])
        assert set(result.values()) == {"ok", "denied"}
        assert _result(result.values()["ok"]) is None
        # A future holding an error-carrying helper succeeds; its getters fail.
        assert _result(result.values()["denied"]) is None
        assert _result(result.all()) is None
        for getter in (result.topic_id, result.num_partitions, result.replication_factor,
                       result.config):
            assert str(_exception(getter(topic="denied"))) == "denied"

    def test_failed_future_fails_every_view(self) -> None:
        error = TopicAuthorizationError(message="x")
        result = CreateTopicsResult._new(futures={"t": _failed(Sync, error)}, kind=Sync)
        assert _exception(result.all()) is error
        assert _exception(result.values()["t"]) is error
        assert _exception(result.num_partitions(topic="t")) is error

    def test_unknown_topic_raises_illegal_argument_error(self) -> None:
        with pytest.raises(IllegalArgumentError) as info:
            self._result(Sync).config(topic="missing")
        assert str(info.value) == "Topic missing was not part of the createTopics request"

    def test_topic_metadata_and_config_forms(self) -> None:
        failed = _TopicMetadataAndConfig(exception=TopicAuthorizationError(message="d"))
        assert failed._topic_id == Uuid.ZERO_UUID
        assert failed._num_partitions == -1
        assert failed._replication_factor == -1
        with pytest.raises(IllegalArgumentError) as info:
            _TopicMetadataAndConfig(topic_id=self.TOPIC_ID)  # type: ignore[call-overload]
        assert str(info.value) == (
            "TopicMetadataAndConfig() takes one of (topic_id, num_partitions, "
            "replication_factor, config), (exception); got (topic_id)")

    async def test_async(self) -> None:
        result = self._result(Async)
        assert (await _await(result.num_partitions(topic="ok"))) == 3
        assert (await _await(result.all())) is None
        with pytest.raises(TopicAuthorizationError):
            await _await(result.config(topic="denied"))


class TestListOffsetsResult:
    """Shapes: a nested value class; a public constructor; ``allOf`` + ``thenApply``."""

    TP0 = TopicPartition(topic="t", partition=0)
    TP1 = TopicPartition(topic="t", partition=1)

    def test_result_info(self) -> None:
        info = ListOffsetsResult.ListOffsetsResultInfo(offset=5, timestamp=10, leader_epoch=2)
        assert (info.offset(), info.timestamp(), info.leader_epoch()) == (5, 10, 2)
        assert str(info) == "ListOffsetsResultInfo(offset=5, timestamp=10, leaderEpoch=Optional[2])"
        empty = ListOffsetsResult.ListOffsetsResultInfo(offset=5, timestamp=10, leader_epoch=None)
        assert str(empty) == ("ListOffsetsResultInfo(offset=5, timestamp=10, "
                              "leaderEpoch=Optional.empty)")
        with pytest.raises(TypeError):
            ListOffsetsResult.ListOffsetsResultInfo(5, 10, 2)  # type: ignore[misc]

    def test_public_constructor_sync(self) -> None:
        info0 = ListOffsetsResult.ListOffsetsResultInfo(offset=1, timestamp=2, leader_epoch=None)
        info1 = ListOffsetsResult.ListOffsetsResultInfo(offset=3, timestamp=4, leader_epoch=0)
        result = ListOffsetsResult(futures={self.TP0: _done(Sync, info0),
                                            self.TP1: _done(Sync, info1)})
        assert _result(result.partition_result(partition=self.TP0)) is info0
        assert _result(result.all()) == {self.TP0: info0, self.TP1: info1}

    def test_partition_not_attempted(self) -> None:
        result = ListOffsetsResult(futures={})
        with pytest.raises(IllegalArgumentError) as info:
            result.partition_result(partition=self.TP0)
        assert str(info.value) == 'List Offsets for partition "t-0" was not attempted'

    def test_all_fails_with_the_first_error(self) -> None:
        error = TopicAuthorizationError(message="denied")
        info = ListOffsetsResult.ListOffsetsResultInfo(offset=1, timestamp=2, leader_epoch=None)
        result = ListOffsetsResult._new(futures={self.TP0: _done(Sync, info),
                                                 self.TP1: _failed(Sync, error)}, kind=Sync)
        assert _exception(result.all()) is error

    async def test_async(self) -> None:
        info = ListOffsetsResult.ListOffsetsResultInfo(offset=1, timestamp=2, leader_epoch=None)
        result = ListOffsetsResult(futures={self.TP0: _done(Async, info)})
        assert isinstance(result.all(), AsyncKafkaFuture)
        assert (await _await(result.all())) == {self.TP0: info}
        empty = ListOffsetsResult._new(futures={}, kind=Async)
        assert (await _await(empty.all())) == {}


# --------------------------------------------------------------------------------------
# Fix round 1: the shapes the first set did not cover
# --------------------------------------------------------------------------------------


class TestInternals:
    def test_coordinator_key(self) -> None:
        key = _CoordinatorKey.by_transactional_id(transactional_id="t")
        assert key == _CoordinatorKey.by_transactional_id(transactional_id="t")
        assert hash(key) == hash(_CoordinatorKey.by_transactional_id(transactional_id="t"))
        assert key != _CoordinatorKey.by_group_id(group_id="t")
        assert str(key) == "CoordinatorKey(idValue='t', type=TRANSACTION)"

    def test_sub_level_error(self) -> None:
        tp = TopicPartition(topic="t", partition=0)
        error = UnknownTopicOrPartitionError(message="x")
        assert _sub_level_error({tp: error}, tp, "missing") is error
        assert _sub_level_error({tp: None}, tp, "missing") is None
        missing = _sub_level_error({}, tp, "missing")
        assert isinstance(missing, IllegalArgumentError) and str(missing) == "missing"


class TestDeleteConsumerGroupOffsetsResult:
    """Shape: a new future completed from ``whenComplete``; Java's internal ``Errors``."""

    TP0 = TopicPartition(topic="t", partition=0)
    TP1 = TopicPartition(topic="t", partition=1)
    TP2 = TopicPartition(topic="t", partition=2)

    def _result(self, kind: type[Sync] | type[Async], errors: Any) -> Any:
        return DeleteConsumerGroupOffsetsResult._new(
            future=_done(kind, errors), partitions={self.TP0, self.TP1}, kind=kind)

    def test_partition_result(self) -> None:
        error = UnknownTopicOrPartitionError(message="gone")
        result = self._result(Sync, {self.TP0: None, self.TP1: error})
        assert _result(result.partition_result(partition=self.TP0)) is None
        assert _exception(result.partition_result(partition=self.TP1)) is error
        with pytest.raises(IllegalArgumentError) as info:
            result.partition_result(partition=self.TP2)
        assert str(info.value) == "Partition t-2 was not included in the original request"

    def test_all(self) -> None:
        assert _result(self._result(Sync, {self.TP0: None, self.TP1: None}).all()) is None
        error = UnknownTopicOrPartitionError(message="gone")
        assert _exception(self._result(Sync, {self.TP0: None, self.TP1: error}).all()) is error

    def test_missing_from_response(self) -> None:
        result = self._result(Sync, {self.TP0: None})
        assert str(_exception(result.partition_result(partition=self.TP1))) == (
            'Offset deletion result for partition "t-1" was not included in the response')
        assert isinstance(_exception(result.all()), IllegalArgumentError)

    def test_top_level_failure_and_pending(self) -> None:
        error = IllegalStateError(message="top")
        failed = DeleteConsumerGroupOffsetsResult._new(
            future=_failed(Sync, error), partitions={self.TP0}, kind=Sync)
        assert _exception(failed.all()) is error
        source = _new_future(Sync)
        pending = DeleteConsumerGroupOffsetsResult._new(future=source, partitions={self.TP0},
                                                        kind=Sync)
        all_future = pending.all()
        assert not all_future.is_done()
        _complete(source, {self.TP0: None})
        assert _result(all_future) is None

    async def test_async(self) -> None:
        result = self._result(Async, {self.TP0: None, self.TP1: None})
        assert isinstance(result.all(), AsyncKafkaFuture)
        assert (await _await(result.all())) is None
        assert (await _await(result.partition_result(partition=self.TP1))) is None


class TestDescribeTransactionsResult:
    """Shape: a map keyed by the private helper ``_CoordinatorKey``."""

    def _description(self) -> TransactionDescription:
        return TransactionDescription(coordinator_id=1, state=TransactionState.ONGOING,
                                      producer_id=2, producer_epoch=3,
                                      transaction_timeout_ms=4, transaction_start_time_ms=None,
                                      topic_partitions=[])

    def test_sync(self) -> None:
        description = self._description()
        key = _CoordinatorKey.by_transactional_id(transactional_id="tx")
        result = DescribeTransactionsResult._new(futures={key: _done(Sync, description)},
                                                 kind=Sync)
        assert _result(result.description(transactional_id="tx")) is description
        assert _result(result.all()) == {"tx": description}
        with pytest.raises(IllegalArgumentError) as info:
            result.description(transactional_id="other")
        assert str(info.value) == "TransactionalId `other` was not included in the request"

    async def test_async(self) -> None:
        description = self._description()
        key = _CoordinatorKey.by_transactional_id(transactional_id="tx")
        result = DescribeTransactionsResult._new(futures={key: _done(Async, description)},
                                                 kind=Async)
        assert (await _await(result.all())) == {"tx": description}


class TestListTransactionsResult:
    """Shape: a future of a map of futures."""

    L1 = TransactionListing(transactional_id="a", producer_id=1,
                            transaction_state=TransactionState.ONGOING)
    L2 = TransactionListing(transactional_id="b", producer_id=2,
                            transaction_state=TransactionState.ONGOING)

    def _result(self, kind: type[Sync] | type[Async], by_broker: Any) -> Any:
        return ListTransactionsResult._new(future=_done(kind, by_broker), kind=kind)

    def test_sync(self) -> None:
        brokers = {1: _done(Sync, [self.L1]), 2: _done(Sync, [self.L2])}
        result = self._result(Sync, brokers)
        by_broker = _result(result.by_broker_id())
        assert by_broker == brokers and by_broker is not brokers
        assert _result(result.all_by_broker_id()) == {1: [self.L1], 2: [self.L2]}
        assert sorted(x.transactional_id() for x in _result(result.all())) == ["a", "b"]

    def test_failures(self) -> None:
        error = IllegalStateError(message="broker")
        result = self._result(Sync, {1: _done(Sync, [self.L1]), 2: _failed(Sync, error)})
        assert _exception(result.all()) is error
        top = IllegalStateError(message="top")
        failed = ListTransactionsResult._new(future=_failed(Sync, top), kind=Sync)
        assert _exception(failed.by_broker_id()) is top
        assert _exception(failed.all_by_broker_id()) is top

    def test_broker_future_completing_later(self) -> None:
        later = _new_future(Sync)
        result = self._result(Sync, {1: _done(Sync, [self.L1]), 2: later})
        all_future = result.all()
        assert not all_future.is_done()
        _complete(later, [self.L2])
        assert len(_result(all_future)) == 2

    def test_no_brokers_never_completes_as_in_java(self) -> None:
        # Java completes allByBrokerId() only from a broker future's whenComplete.
        assert not self._result(Sync, {}).all_by_broker_id().is_done()

    async def test_async(self) -> None:
        result = self._result(Async, {1: _done(Async, [self.L1])})
        by_broker = await _await(result.by_broker_id())
        assert isinstance(by_broker[1], AsyncKafkaFuture)
        assert (await _await(result.all())) == [self.L1]


class TestDeleteConsumerGroupsResult:
    """Shape: a getter returning a copy of the stored map."""

    def test_deleted_groups_is_a_copy(self) -> None:
        futures = {"g": _done(Sync, None)}
        result = DeleteConsumerGroupsResult._new(futures=futures, kind=Sync)
        copy = result.deleted_groups()
        assert copy == futures
        copy.clear()
        assert result.deleted_groups() == futures
        assert _result(result.all()) is None

    async def test_async(self) -> None:
        result = DeleteConsumerGroupsResult._new(futures={"g": _done(Async, None)}, kind=Async)
        assert (await _await(result.all())) is None


class TestListTransactionsOptions:
    """Shapes: set fields hashed by ``hashCode`` (stored as ``frozenset``);
    ``toString`` with a field ``equals`` leaves out."""

    def test_defaults(self) -> None:
        options = ListTransactionsOptions()
        assert options.filtered_states() == set()
        assert options.filtered_producer_ids() == set()
        assert options.filtered_duration() == -1
        assert options.filtered_transactional_id_pattern() is None

    def test_keyword_constructor_and_setters(self) -> None:
        options = ListTransactionsOptions(filtered_states=[TransactionState.ONGOING],
                                          filtered_producer_ids=[1, 2], filtered_duration=5,
                                          filtered_transactional_id_pattern="tx.*")
        assert options.filtered_states() == {TransactionState.ONGOING}
        assert options.filtered_producer_ids() == {1, 2}
        assert options.filtered_duration() == 5
        assert options.filtered_transactional_id_pattern() == "tx.*"
        assert options.filter_on_transactional_id_pattern(pattern=None) is options
        assert options.filtered_transactional_id_pattern() is None

    def test_equals_and_hash(self) -> None:
        a = ListTransactionsOptions(filtered_producer_ids=[1, 2], timeout_ms=1)
        b = ListTransactionsOptions(filtered_producer_ids=[2, 1], timeout_ms=2)
        assert a == b and hash(a) == hash(b)
        assert a != ListTransactionsOptions()

    def test_str(self) -> None:
        options = ListTransactionsOptions(filtered_producer_ids=[7], timeout_ms=3)
        assert str(options) == (
            "ListTransactionsOptions(filteredStates=[], filteredProducerIds=[7], "
            "filteredDuration=-1, filteredTransactionalIdPattern=None, timeoutMs=3)")

    def test_null_argument_is_not_checked(self) -> None:
        with pytest.raises(TypeError):
            ListTransactionsOptions(filtered_states=None)  # type: ignore[arg-type]


class TestDescribeProducersOptions:
    """Shape: ``OptionalInt`` in ``toString``; ``equals`` with ``timeoutMs``."""

    def test_broker_id(self) -> None:
        assert DescribeProducersOptions().broker_id() is None
        options = DescribeProducersOptions()
        assert options.broker_id(broker_id=5) is options
        assert options.broker_id() == 5

    def test_str(self) -> None:
        assert str(DescribeProducersOptions()) == (
            "DescribeProducersOptions(brokerId=OptionalInt.empty, timeoutMs=None)")
        assert str(DescribeProducersOptions(broker_id=5, timeout_ms=10)) == (
            "DescribeProducersOptions(brokerId=OptionalInt[5], timeoutMs=10)")

    def test_equals_reads_timeout(self) -> None:
        assert DescribeProducersOptions(broker_id=1) == DescribeProducersOptions(broker_id=1)
        assert hash(DescribeProducersOptions(broker_id=1)) == hash(
            DescribeProducersOptions(broker_id=1))
        assert DescribeProducersOptions(broker_id=1, timeout_ms=1) != DescribeProducersOptions(
            broker_id=1)


class TestListOffsetsOptions:
    """Shape: constructor-only class with a Java-given enum default."""

    def test_constructors(self) -> None:
        assert ListOffsetsOptions().isolation_level() is IsolationLevel.READ_UNCOMMITTED
        options = ListOffsetsOptions(isolation_level=IsolationLevel.READ_COMMITTED)
        assert options.isolation_level() is IsolationLevel.READ_COMMITTED
        assert options.timeout_ms(timeout_ms=5).timeout_ms() == 5
        with pytest.raises(TypeError):
            ListOffsetsOptions(IsolationLevel.READ_COMMITTED)  # type: ignore[misc]
        with pytest.raises(TypeError):
            ListOffsetsOptions(timeout_ms=1)  # type: ignore[call-arg]
