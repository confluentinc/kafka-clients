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

"""The hand-written admin Result classes (outside every generator pattern).

Behaviour tests of each class for the sync (``KafkaFuture``) and the async
(``AsyncKafkaFuture``) client kinds, and the private Java internals they use. Java's own tests of
these classes are translated here: ``DescribeTopicsResultTest``,
``DescribeUserScramCredentialsResultTest`` and ``RemoveMembersFromConsumerGroupResultTest``
(the other classes have no Java test).
"""

from __future__ import annotations

import asyncio
import gc
from typing import Any

import pytest

from confluent_kafka.admin import (
    AlterConsumerGroupOffsetsResult,
    AlterUserScramCredentialsResult,
    DeleteAclsResult,
    DescribeReplicaLogDirsResult,
    DescribeTopicsResult,
    DescribeUserScramCredentialsResult,
    FenceProducersResult,
    GroupListing,
    ListConsumerGroupOffsetsResult,
    ListGroupsResult,
    MemberToRemove,
    RemoveMembersFromConsumerGroupResult,
    ScramCredentialInfo,
    ScramMechanism,
    TopicDescription,
    UserScramCredentialsDescription,
)
from confluent_kafka import ExecutionError
from confluent_kafka._args import MISSING
from confluent_kafka.admin._futures import Async, Sync
from confluent_kafka.admin._internals import (
    _CoordinatorKey,
    _DescribeUserScramCredentialsResponseData,
    _errors_exception,
    _errors_for_code,
    _MemberIdentity,
    _ProducerIdAndEpoch,
    _to_member_identity,
)
from confluent_kafka.common import (
    AccessControlEntry,
    AccessControlEntryFilter,
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    GroupState,
    GroupType,
    PatternType,
    ResourcePattern,
    ResourcePatternFilter,
    ResourceType,
    TopicPartition,
    TopicPartitionReplica,
    Uuid,
)
from confluent_kafka.common.errors import (
    DuplicateResourceError,
    FencedInstanceIdError,
    GroupAuthorizationError,
    InvalidCommitOffsetSizeError,
    ResourceNotFoundError,
    TopicAuthorizationError,
    UnknownServerError,
    UnknownTopicOrPartitionError,
)
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture, _KafkaFutureImpl
from confluent_kafka.consumer import OffsetAndMetadata
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError

KINDS = [Sync, Async]

_ResponseData = _DescribeUserScramCredentialsResponseData


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


def _future_type(kind: type[Sync] | type[Async]) -> type:
    return AsyncKafkaFuture if kind is Async else KafkaFuture


def _value(future: Any) -> Any:
    """The value of a done future of either kind (``get_now``)."""
    assert future.is_done()
    return future.get_now(value_if_absent=None)


def _error(future: Any) -> BaseException:
    """The failure of a done future of either kind (the cause of ``get_now``'s
    ``ExecutionError``)."""
    assert future.is_done()
    try:
        future.get_now(value_if_absent=None)
    except ExecutionError as error:
        assert error.__cause__ is not None
        return error.__cause__
    raise AssertionError("the future succeeded")


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


async def _unretrieved_logs(body: Any) -> list[str]:
    """The "never retrieved" reports asyncio makes for the futures ``body()`` creates
    and drops, through the running loop's exception handler."""
    loop = asyncio.get_running_loop()
    reports: list[str] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: reports.append(context.get("message", "")))
    try:
        body()
        await asyncio.sleep(0)  # run the done callbacks
        gc.collect()
    finally:
        loop.set_exception_handler(previous)
    return [r for r in reports if "never retrieved" in r]


def _run_kind(kind: type[Sync] | type[Async], body: Any) -> None:
    """Run ``body(kind)``; an async kind needs a running loop for its futures."""
    if kind is Sync:
        body(kind)
        return

    async def main() -> None:
        body(kind)

    asyncio.run(main())


TP0 = TopicPartition(topic="t", partition=0)
TP1 = TopicPartition(topic="t", partition=1)
TP2 = TopicPartition(topic="t", partition=2)


# --------------------------------------------------------------------------------------
# Construction
# --------------------------------------------------------------------------------------


class TestConstruction:
    @pytest.mark.parametrize("cls, method", [
        (AlterConsumerGroupOffsetsResult, "alter_consumer_group_offsets()"),
        (DeleteAclsResult, "delete_acls()"),
        (DeleteAclsResult.FilterResult, "delete_acls()"),
        (DeleteAclsResult.FilterResults, "delete_acls()"),
        (DescribeReplicaLogDirsResult, "describe_replica_log_dirs()"),
        (DescribeReplicaLogDirsResult.ReplicaLogDirInfo, "describe_replica_log_dirs()"),
        (DescribeTopicsResult, "describe_topics()"),
        (DescribeUserScramCredentialsResult, "describe_user_scram_credentials()"),
        (FenceProducersResult, "fence_producers()"),
        (ListConsumerGroupOffsetsResult, "list_consumer_group_offsets()"),
        (ListGroupsResult, "list_groups()"),
        (RemoveMembersFromConsumerGroupResult, "remove_members_from_consumer_group()"),
    ])
    def test_init_names_the_method_that_returns_it(self, cls: type, method: str) -> None:
        with pytest.raises(TypeError) as info:
            cls()
        assert str(info.value) == (f"{cls.__name__} cannot be constructed directly; use the "
                                   f"admin client's {method}")

    def test_positional_calls_raise_type_error(self) -> None:
        result = FenceProducersResult._new(futures={}, kind=Sync)
        with pytest.raises(TypeError):
            result.producer_id("t")  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            AlterUserScramCredentialsResult({})  # type: ignore[call-overload]
        offsets = ListConsumerGroupOffsetsResult._new(futures={}, kind=Sync)
        with pytest.raises(TypeError):
            offsets.partitions_to_offset_and_metadata("g")  # type: ignore[call-arg]


# --------------------------------------------------------------------------------------
# The private Java internals
# --------------------------------------------------------------------------------------


class TestInternals:
    def test_errors_exception(self) -> None:
        assert _errors_exception(None, "m") is None
        error = TopicAuthorizationError(message="denied")
        assert _errors_exception(error, None) is error
        built = _errors_exception(error, "new message")
        assert type(built) is TopicAuthorizationError
        assert built is not error
        assert str(built) == "new message"

    def test_errors_for_code(self) -> None:
        assert _errors_for_code(0, "ignored") is None
        error = _errors_for_code(91, "gone")
        assert type(error) is ResourceNotFoundError
        assert str(error) == "gone"
        assert type(_errors_for_code(92, "")) is DuplicateResourceError
        # Errors.forCode of an unknown code is UNKNOWN_SERVER_ERROR; a client-side
        # (negative) FFI id is no wire code.
        assert type(_errors_for_code(32000, "x")) is UnknownServerError
        assert type(_errors_for_code(-4, "x")) is UnknownServerError

    def test_errors_for_code_null_message_is_the_default(self) -> None:
        # Errors.exception(null) returns the cached instance with the default message.
        duplicate = _errors_for_code(92, None)
        assert type(duplicate) is DuplicateResourceError
        assert str(duplicate) == "A request illegally referred to the same resource twice."
        missing = _errors_for_code(91, None)
        assert type(missing) is ResourceNotFoundError
        assert str(missing) == "A request illegally referred to a resource that does not exist."
        unknown = _errors_for_code(32000, None)
        assert type(unknown) is UnknownServerError
        assert str(unknown) == (
            "The server experienced an unexpected error when processing the request.")
        assert str(_errors_for_code(92, "")) == ""

    def test_producer_id_and_epoch(self) -> None:
        p = _ProducerIdAndEpoch(producer_id=5, epoch=3)
        assert (p.producer_id, p.epoch) == (5, 3)
        assert str(p) == "(producerId=5, epoch=3)"
        assert p == _ProducerIdAndEpoch(producer_id=5, epoch=3)
        assert hash(p) == hash(_ProducerIdAndEpoch(producer_id=5, epoch=3))
        assert p != _ProducerIdAndEpoch(producer_id=5, epoch=4)
        assert p.is_valid()
        assert not _ProducerIdAndEpoch.NONE.is_valid()
        assert str(_ProducerIdAndEpoch.NONE) == "(producerId=-1, epoch=-1)"

    def test_member_identity(self) -> None:
        identity = _to_member_identity(MemberToRemove(group_instance_id="instance-1"))
        assert identity.member_id() == ""
        assert identity.group_instance_id() == "instance-1"
        assert identity.reason() is None
        assert identity == _MemberIdentity(member_id="", group_instance_id="instance-1")
        assert hash(identity) == hash(_MemberIdentity(group_instance_id="instance-1"))
        assert identity != _MemberIdentity(group_instance_id="instance-2")
        assert str(identity) == (
            "MemberIdentity(memberId='', groupInstanceId='instance-1', reason=None)")

    def test_response_data_defaults(self) -> None:
        data = _ResponseData()
        assert (data.throttle_time_ms(), data.error_code(), data.error_message(),
                data.results()) == (0, 0, "", [])
        user = _ResponseData.DescribeUserScramCredentialsResult()
        assert (user.user(), user.error_code(), user.error_message(),
                user.credential_infos()) == ("", 0, "", [])
        info = _ResponseData.CredentialInfo()
        assert (info.mechanism(), info.iterations()) == (0, 0)


# --------------------------------------------------------------------------------------
# AlterConsumerGroupOffsetsResult
# --------------------------------------------------------------------------------------


class TestAlterConsumerGroupOffsetsResult:
    @pytest.mark.parametrize("kind", KINDS)
    def test_partition_result(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            error = UnknownTopicOrPartitionError(message="gone")
            result = AlterConsumerGroupOffsetsResult._new(
                future=_done(kind, {TP0: None, TP1: error}), kind=kind)
            ok = result.partition_result(partition=TP0)
            assert isinstance(ok, _future_type(kind))
            assert _value(ok) is None
            assert _error(result.partition_result(partition=TP1)) is error
            missing = _error(result.partition_result(partition=TP2))
            assert type(missing) is IllegalArgumentError
            assert str(missing) == 'Alter offset for partition "t-2" was not attempted'
        _run_kind(kind, body)

    @pytest.mark.parametrize("kind", KINDS)
    def test_all(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            ok = AlterConsumerGroupOffsetsResult._new(
                future=_done(kind, {TP0: None, TP1: None}), kind=kind)
            assert _value(ok.all()) is None
            error = InvalidCommitOffsetSizeError(message="too big")
            failed = AlterConsumerGroupOffsetsResult._new(
                future=_done(kind, {TP0: None, TP1: error, TP2: error}), kind=kind)
            all_error = _error(failed.all())
            # error.exception(message): a new error of the same class with Java's message.
            assert type(all_error) is InvalidCommitOffsetSizeError
            assert all_error is not error
            assert str(all_error) == (
                "Failed altering group offsets for the following partitions: [t-1, t-2]")
        _run_kind(kind, body)

    def test_top_level_failure_and_pending(self) -> None:
        error = GroupAuthorizationError(message="denied")
        result = AlterConsumerGroupOffsetsResult._new(future=_failed(Sync, error), kind=Sync)
        assert _exception(result.all()) is error
        assert _exception(result.partition_result(partition=TP0)) is error
        source = _new_future(Sync)
        pending = AlterConsumerGroupOffsetsResult._new(future=source, kind=Sync)
        partition = pending.partition_result(partition=TP0)
        all_future = pending.all()
        assert not partition.is_done() and not all_future.is_done()
        _complete(source, {TP0: None})
        assert _result(partition) is None
        assert _result(all_future) is None

    async def test_async_awaits(self) -> None:
        result = AlterConsumerGroupOffsetsResult._new(future=_done(Async, {TP0: None}),
                                                      kind=Async)
        assert (await _await(result.all())) is None
        assert (await _await(result.partition_result(partition=TP0))) is None


# --------------------------------------------------------------------------------------
# AlterUserScramCredentialsResult
# --------------------------------------------------------------------------------------


class TestAlterUserScramCredentialsResult:
    def test_public_constructor_reads_the_kind(self) -> None:
        error = ResourceNotFoundError(message="no user")
        futures = {"a": _done(Sync, None), "b": _failed(Sync, error)}
        result = AlterUserScramCredentialsResult(futures=futures)
        assert result.values() == futures
        assert _exception(result.all()) is error
        empty = AlterUserScramCredentialsResult(futures={})
        assert isinstance(empty.all(), KafkaFuture)
        assert _result(empty.all()) is None

    def test_values_is_unmodifiable(self) -> None:
        futures = {"a": _done(Sync, None)}
        result = AlterUserScramCredentialsResult(futures=futures)
        result.values().clear()
        futures.clear()
        assert list(result.values()) == ["a"]

    def test_require_non_null(self) -> None:
        with pytest.raises(TypeError) as info:
            AlterUserScramCredentialsResult(futures=None)  # type: ignore[call-overload]
        assert str(info.value) == "futures must not be None"
        with pytest.raises(TypeError) as none:
            AlterUserScramCredentialsResult._new(futures=None, kind=Sync)  # type: ignore[arg-type]
        assert str(none.value) == "futures must not be None"

    async def test_async(self) -> None:
        result = AlterUserScramCredentialsResult(futures={"a": _done(Async, None)})
        assert isinstance(result.all(), AsyncKafkaFuture)
        assert (await _await(result.all())) is None
        empty = AlterUserScramCredentialsResult._new(futures={}, kind=Async)
        assert isinstance(empty.all(), AsyncKafkaFuture)
        assert (await _await(empty.all())) is None


# --------------------------------------------------------------------------------------
# DeleteAclsResult
# --------------------------------------------------------------------------------------


def _binding(name: str) -> AclBinding:
    return AclBinding(
        pattern=ResourcePattern(resource_type=ResourceType.TOPIC, name=name,
                                pattern_type=PatternType.LITERAL),
        entry=AccessControlEntry(principal="User:alice", host="*", operation=AclOperation.READ,
                                 permission_type=AclPermissionType.ALLOW))


def _filter(name: str) -> AclBindingFilter:
    return AclBindingFilter(
        pattern_filter=ResourcePatternFilter(resource_type=ResourceType.TOPIC, name=name,
                                             pattern_type=PatternType.LITERAL),
        entry_filter=AccessControlEntryFilter.ANY)


class TestDeleteAclsResult:
    def test_filter_result(self) -> None:
        binding = _binding("a")
        ok = DeleteAclsResult.FilterResult._new(binding=binding, exception=None)
        assert ok.binding() is binding and ok.exception() is None
        error = TopicAuthorizationError(message="denied")
        failed = DeleteAclsResult.FilterResult._new(binding=None, exception=error)
        assert failed.binding() is None and failed.exception() is error
        results = DeleteAclsResult.FilterResults._new(values=[ok, failed])
        assert results.values() == [ok, failed]

    @pytest.mark.parametrize("kind", KINDS)
    def test_values_and_all(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            a = DeleteAclsResult.FilterResults._new(values=[
                DeleteAclsResult.FilterResult._new(binding=_binding("a"), exception=None),
                DeleteAclsResult.FilterResult._new(binding=_binding("b"), exception=None)])
            empty = DeleteAclsResult.FilterResults._new(values=[])
            futures = {_filter("a"): _done(kind, a), _filter("x"): _done(kind, empty)}
            result = DeleteAclsResult._new(futures=futures, kind=kind)
            assert result.values() == futures
            all_future = result.all()
            assert isinstance(all_future, _future_type(kind))
            assert _value(all_future) == [_binding("a"), _binding("b")]
            assert _value(DeleteAclsResult._new(futures={}, kind=kind).all()) == []
        _run_kind(kind, body)

    def test_all_throws_the_filter_result_exception(self) -> None:
        error = TopicAuthorizationError(message="denied")
        results = DeleteAclsResult.FilterResults._new(values=[
            DeleteAclsResult.FilterResult._new(binding=_binding("a"), exception=None),
            DeleteAclsResult.FilterResult._new(binding=None, exception=error)])
        result = DeleteAclsResult._new(futures={_filter("a"): _done(Sync, results)}, kind=Sync)
        assert _exception(result.all()) is error

    def test_all_fails_with_a_failed_filter_future(self) -> None:
        error = TopicAuthorizationError(message="denied")
        result = DeleteAclsResult._new(futures={_filter("a"): _failed(Sync, error)}, kind=Sync)
        assert _exception(result.all()) is error

    def test_get_acl_bindings_internal_error(self) -> None:
        # Unreachable through all() (allOf fails first); the private helper wraps it.
        error = TopicAuthorizationError(message="denied")
        result = DeleteAclsResult._new(futures={_filter("a"): _failed(Sync, error)}, kind=Sync)
        with pytest.raises(KafkaError) as info:
            result._get_acl_bindings(result.values())
        assert type(info.value) is KafkaError
        assert str(info.value) == "DeleteAclsResult#all: internal error"
        # Java wraps what ``get()`` threw: the ``ExecutionException``.
        assert type(info.value.__cause__) is ExecutionError
        assert info.value.__cause__.__cause__ is error


# --------------------------------------------------------------------------------------
# DescribeReplicaLogDirsResult
# --------------------------------------------------------------------------------------


class TestDescribeReplicaLogDirsResult:
    Info = DescribeReplicaLogDirsResult.ReplicaLogDirInfo

    def test_replica_log_dir_info_forms(self) -> None:
        empty = self.Info._new()
        assert (empty.current_replica_log_dir(), empty.current_replica_offset_lag(),
                empty.future_replica_log_dir(), empty.future_replica_offset_lag()) == (
            None, -1, None, -1)
        info = self.Info._new(current_replica_log_dir="/a", current_replica_offset_lag=3,
                              future_replica_log_dir="/b", future_replica_offset_lag=4)
        assert (info.current_replica_log_dir(), info.current_replica_offset_lag(),
                info.future_replica_log_dir(), info.future_replica_offset_lag()) == (
            "/a", 3, "/b", 4)
        nulls = self.Info._new(current_replica_log_dir=None, current_replica_offset_lag=0,
                               future_replica_log_dir=None, future_replica_offset_lag=0)
        assert nulls.current_replica_log_dir() is None
        assert nulls.current_replica_offset_lag() == 0

    def test_replica_log_dir_info_illegal_combination(self) -> None:
        with pytest.raises(IllegalArgumentError) as info:
            self.Info._new(current_replica_log_dir="/a")  # type: ignore[call-overload]
        assert str(info.value) == (
            "ReplicaLogDirInfo() takes one of (), (current_replica_log_dir, "
            "current_replica_offset_lag, future_replica_log_dir, future_replica_offset_lag); "
            "got (current_replica_log_dir)")
        with pytest.raises(TypeError):
            self.Info._new("/a", 0, None, 0)  # type: ignore[call-overload]

    def test_replica_log_dir_info_str(self) -> None:
        assert str(self.Info._new()) == "ReplicaLogDirInfo(currentReplicaLogDir=None)"
        assert str(self.Info._new(current_replica_log_dir="/a", current_replica_offset_lag=0,
                                  future_replica_log_dir=None, future_replica_offset_lag=0)
                   ) == "ReplicaLogDirInfo(currentReplicaLogDir=/a)"
        assert str(self.Info._new(current_replica_log_dir="/a", current_replica_offset_lag=0,
                                  future_replica_log_dir="/b", future_replica_offset_lag=7)
                   ) == "(currentReplicaLogDir=/a, futureReplicaLogDir=/b, futureReplicaOffsetLag=7)"

    @pytest.mark.parametrize("kind", KINDS)
    def test_values_and_all(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            r0 = TopicPartitionReplica(topic="t", partition=0, broker_id=1)
            r1 = TopicPartitionReplica(topic="t", partition=1, broker_id=1)
            info = self.Info._new()
            futures = {r0: _done(kind, info)}
            result = DescribeReplicaLogDirsResult._new(futures=futures, kind=kind)
            assert result.values() == futures
            assert _value(result.all()) == {r0: info}
            error = UnknownTopicOrPartitionError(message="gone")
            failed = DescribeReplicaLogDirsResult._new(
                futures={r0: _done(kind, info), r1: _failed(kind, error)}, kind=kind)
            assert _error(failed.all()) is error
            empty = DescribeReplicaLogDirsResult._new(futures={}, kind=kind)
            assert isinstance(empty.all(), _future_type(kind))
            assert _value(empty.all()) == {}
        _run_kind(kind, body)


# --------------------------------------------------------------------------------------
# DescribeTopicsResult (translates DescribeTopicsResultTest)
# --------------------------------------------------------------------------------------


class TestDescribeTopicsResult:
    def _description(self) -> TopicDescription:
        return TopicDescription(name="foo", internal=False, partitions=[])

    def test_null_all_topic_names(self) -> None:
        result = DescribeTopicsResult._of_topic_ids(
            topic_id_futures={Uuid.random_uuid(): _done(Sync, self._description())},
            kind=Sync)
        assert result.all_topic_names() is None

    def test_null_all_topic_ids(self) -> None:
        result = DescribeTopicsResult._of_topic_names(
            name_futures={"foo": _done(Sync, self._description())}, kind=Sync)
        assert result.all_topic_ids() is None

    def test_constructor_checks(self) -> None:
        with pytest.raises(IllegalArgumentError) as info:
            DescribeTopicsResult._new(topic_id_futures={}, name_futures={}, kind=Sync)
        assert str(info.value) == "topicIdFutures and nameFutures cannot both be specified."
        with pytest.raises(IllegalArgumentError) as info:
            DescribeTopicsResult._new(topic_id_futures=None, name_futures=None, kind=Sync)
        assert str(info.value) == "topicIdFutures and nameFutures cannot both be None."

    @pytest.mark.parametrize("kind", KINDS)
    def test_by_names_and_ids(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            description = self._description()
            names = {"foo": _done(kind, description)}
            by_names = DescribeTopicsResult._of_topic_names(name_futures=names, kind=kind)
            assert by_names.topic_name_values() == names
            assert by_names.topic_id_values() is None
            assert _value(by_names.all_topic_names()) == {"foo": description}
            topic_id = Uuid.random_uuid()
            error = UnknownTopicOrPartitionError(message="gone")
            ids = {topic_id: _done(kind, description), Uuid.random_uuid(): _failed(kind, error)}
            by_ids = DescribeTopicsResult._of_topic_ids(topic_id_futures=ids, kind=kind)
            assert by_ids.topic_id_values() == ids
            assert by_ids.topic_name_values() is None
            assert _error(by_ids.all_topic_ids()) is error
            empty = DescribeTopicsResult._of_topic_names(name_futures={}, kind=kind)
            assert isinstance(empty.all_topic_names(), _future_type(kind))
            assert _value(empty.all_topic_names()) == {}
        _run_kind(kind, body)


# --------------------------------------------------------------------------------------
# DescribeUserScramCredentialsResult (translates DescribeUserScramCredentialsResultTest)
# --------------------------------------------------------------------------------------


def _user(user: str, *, error_code: int = 0, error_message: str | None = "",
          credentials: list[tuple[int, int]] | None = None
          ) -> _ResponseData.DescribeUserScramCredentialsResult:
    return _ResponseData.DescribeUserScramCredentialsResult(
        user=user, error_code=error_code, error_message=error_message,
        credential_infos=[_ResponseData.CredentialInfo(mechanism=m, iterations=i)
                          for m, i in (credentials or [])])


class TestDescribeUserScramCredentialsResult:
    SCRAM_SHA_256 = ScramMechanism.SCRAM_SHA_256
    ITERATIONS = 4096

    def _result(self, kind: type[Sync] | type[Async], data_future: Any) -> Any:
        return DescribeUserScramCredentialsResult._new(data_future=data_future, kind=kind)

    def test_require_non_null(self) -> None:
        with pytest.raises(TypeError) as none:
            DescribeUserScramCredentialsResult._new(data_future=None,  # type: ignore[arg-type]
                                                    kind=Sync)
        assert str(none.value) == "data_future must not be None"

    @pytest.mark.parametrize("kind", KINDS)
    def test_top_level_error(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            error = RuntimeError()
            results = self._result(kind, _failed(kind, error))
            assert _error(results.all()) is error
            assert _error(results.users()) is error
            assert _error(results.description(user_name="whatever")) is error
        _run_kind(kind, body)

    @pytest.mark.parametrize("kind", KINDS)
    def test_user_level_errors(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            good_user = "goodUser"
            unknown_user = "unknownUser"
            failed_user = "failedUser"
            data = _ResponseData(error_code=0, results=[
                _user(good_user, credentials=[(self.SCRAM_SHA_256.type(), self.ITERATIONS)]),
                _user(unknown_user, error_code=91),
                _user(failed_user, error_code=92)])
            results = self._result(kind, _done(kind, data))
            all_error = _error(results.all())
            assert type(all_error) is DuplicateResourceError
            assert str(all_error) == ""
            assert _value(results.users()) == [good_user, failed_user], (
                "Expected 2 users with credentials")
            good_user_description = _value(results.description(user_name=good_user))
            assert good_user_description == UserScramCredentialsDescription(
                name=good_user, credential_infos=[
                    ScramCredentialInfo(mechanism=self.SCRAM_SHA_256,
                                        iterations=self.ITERATIONS)])
            assert type(_error(results.description(user_name=failed_user))) is (
                DuplicateResourceError)
            assert type(_error(results.description(user_name=unknown_user))) is (
                ResourceNotFoundError)
        _run_kind(kind, body)

    @pytest.mark.parametrize("kind", KINDS)
    def test_successful_description(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            good_user = "goodUser"
            unknown_user = "unknownUser"
            data = _ResponseData(error_code=0, results=[
                _user(good_user, credentials=[(self.SCRAM_SHA_256.type(), self.ITERATIONS)])])
            results = self._result(kind, _done(kind, data))
            assert _value(results.users()) == [good_user], "Expected 1 user with credentials"
            all_results = _value(results.all())
            assert len(all_results) == 1
            good_user_description_via_all = all_results[good_user]
            assert good_user_description_via_all == UserScramCredentialsDescription(
                name=good_user, credential_infos=[
                    ScramCredentialInfo(mechanism=self.SCRAM_SHA_256,
                                        iterations=self.ITERATIONS)])
            assert good_user_description_via_all == _value(
                results.description(user_name=good_user)), (
                "Expected same thing via all() and description()")
            missing = _error(results.description(user_name=unknown_user))
            assert type(missing) is ResourceNotFoundError
            assert str(missing) == "No such user: unknownUser"
        _run_kind(kind, body)

    def test_error_messages_and_unknown_mechanism(self) -> None:
        data = _ResponseData(results=[
            _user("u", error_code=92, error_message="dup"),
            _user("v", credentials=[(9, 1)])])
        results = self._result(Sync, _done(Sync, data))
        assert str(_exception(results.all())) == "dup"
        assert str(_exception(results.description(user_name="u"))) == "dup"
        description = _result(results.description(user_name="v"))
        assert description.credential_infos()[0].mechanism() is ScramMechanism.UNKNOWN

    def test_null_error_message_gives_the_default(self) -> None:
        data = _ResponseData(results=[_user("u", error_code=92, error_message=None)])
        results = self._result(Sync, _done(Sync, data))
        expected = "A request illegally referred to the same resource twice."
        assert str(_exception(results.all())) == expected
        assert str(_exception(results.description(user_name="u"))) == expected

    def test_all_includes_resource_not_found_users(self) -> None:
        # Java's all() puts every result into the map, RESOURCE_NOT_FOUND ones included.
        data = _ResponseData(results=[_user("a"), _user("gone", error_code=91)])
        results = self._result(Sync, _done(Sync, data))
        assert sorted(_result(results.all())) == ["a", "gone"]
        assert _result(results.users()) == ["a"]

    def test_pending(self) -> None:
        source = _new_future(Sync)
        results = self._result(Sync, source)
        users = results.users()
        assert not users.is_done()
        _complete(source, _ResponseData(results=[_user("a")]))
        assert _result(users) == ["a"]

    async def test_async_awaits(self) -> None:
        results = self._result(Async, _done(Async, _ResponseData(results=[_user("a")])))
        assert isinstance(results.users(), AsyncKafkaFuture)
        assert (await _await(results.users())) == ["a"]
        assert list(await _await(results.all())) == ["a"]


# --------------------------------------------------------------------------------------
# FenceProducersResult
# --------------------------------------------------------------------------------------


class TestFenceProducersResult:
    @pytest.mark.parametrize("kind", KINDS)
    def test_futures(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            error = TopicAuthorizationError(message="denied")
            key_a = _CoordinatorKey.by_transactional_id(transactional_id="a")
            key_b = _CoordinatorKey.by_transactional_id(transactional_id="b")
            result = FenceProducersResult._new(futures={
                key_a: _done(kind, _ProducerIdAndEpoch(producer_id=11, epoch=2)),
                key_b: _failed(kind, error)}, kind=kind)
            fenced = result.fenced_producers()
            assert sorted(fenced) == ["a", "b"]
            assert isinstance(fenced["a"], _future_type(kind))
            assert _value(fenced["a"]) is None
            assert _error(fenced["b"]) is error
            assert _value(result.producer_id(transactional_id="a")) == 11
            assert _value(result.epoch_id(transactional_id="a")) == 2
            assert _error(result.producer_id(transactional_id="b")) is error
            assert _error(result.all()) is error
            empty = FenceProducersResult._new(futures={}, kind=kind)
            assert isinstance(empty.all(), _future_type(kind))
            assert _value(empty.all()) is None
        _run_kind(kind, body)

    async def test_unread_fenced_producers_do_not_log(self) -> None:
        error = TopicAuthorizationError(message="denied")
        key = _CoordinatorKey.by_transactional_id(transactional_id="a")

        def unread() -> None:
            future: Any = _new_future(Async)
            _complete_with_error(future, error)
            FenceProducersResult._new(futures={key: future}, kind=Async).fenced_producers()

        assert await _unretrieved_logs(unread) == []
        future: Any = _new_future(Async)
        _complete_with_error(future, error)
        fenced = FenceProducersResult._new(futures={key: future}, kind=Async).fenced_producers()
        with pytest.raises(TopicAuthorizationError):
            await _await(fenced["a"])

    def test_unknown_transactional_id(self) -> None:
        result = FenceProducersResult._new(futures={}, kind=Sync)
        for method in (result.producer_id, result.epoch_id):
            with pytest.raises(IllegalArgumentError) as info:
                method(transactional_id="x")
            assert str(info.value) == "TransactionalId `x` was not included in the request"


# --------------------------------------------------------------------------------------
# ListConsumerGroupOffsetsResult
# --------------------------------------------------------------------------------------


class TestListConsumerGroupOffsetsResult:
    @pytest.mark.parametrize("kind", KINDS)
    def test_one_group(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            offsets = {TP0: OffsetAndMetadata(offset=5), TP1: None}
            future = _done(kind, offsets)
            result = ListConsumerGroupOffsetsResult._new(
                futures={_CoordinatorKey.by_group_id(group_id="g"): future}, kind=kind)
            assert result.partitions_to_offset_and_metadata() is future
            # An explicit None is Java's partitionsToOffsetAndMetadata(null): not requested.
            with pytest.raises(IllegalArgumentError) as info:
                result.partitions_to_offset_and_metadata(group_id=None)
            assert str(info.value) == "Offsets for consumer group 'None' were not requested."
            assert result.partitions_to_offset_and_metadata(group_id=MISSING) is future
            assert result.partitions_to_offset_and_metadata(group_id="g") is future
            assert _value(result.all()) == {"g": offsets}
        _run_kind(kind, body)

    @pytest.mark.parametrize("kind", KINDS)
    def test_several_groups(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            error = GroupAuthorizationError(message="denied")
            g1 = _done(kind, {TP0: OffsetAndMetadata(offset=1)})
            result = ListConsumerGroupOffsetsResult._new(futures={
                _CoordinatorKey.by_group_id(group_id="g1"): g1,
                _CoordinatorKey.by_group_id(group_id="g2"): _failed(kind, error)}, kind=kind)
            with pytest.raises(IllegalStateError) as info:
                result.partitions_to_offset_and_metadata()
            assert str(info.value) == (
                "Offsets from multiple consumer groups were requested. Use "
                "partitionsToOffsetAndMetadata(groupId) instead to get future for a specific "
                "group.")
            assert result.partitions_to_offset_and_metadata(group_id="g1") is g1
            with pytest.raises(IllegalArgumentError) as info2:
                result.partitions_to_offset_and_metadata(group_id="g3")
            assert str(info2.value) == "Offsets for consumer group 'g3' were not requested."
            assert _error(result.all()) is error
        _run_kind(kind, body)

    @pytest.mark.parametrize("kind", KINDS)
    def test_no_group(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            result = ListConsumerGroupOffsetsResult._new(futures={}, kind=kind)
            with pytest.raises(IllegalStateError):
                result.partitions_to_offset_and_metadata()
            assert isinstance(result.all(), _future_type(kind))
            assert _value(result.all()) == {}
        _run_kind(kind, body)


# --------------------------------------------------------------------------------------
# ListGroupsResult
# --------------------------------------------------------------------------------------


def _listing(group_id: str) -> GroupListing:
    return GroupListing(group_id=group_id, type=GroupType.CONSUMER, protocol="consumer",
                        group_state=GroupState.STABLE)


class TestListGroupsResult:
    @pytest.mark.parametrize("kind", KINDS)
    def test_valid_only(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            a, b = _listing("a"), _listing("b")
            result = ListGroupsResult._new(future=_done(kind, [a, b]), kind=kind)
            assert isinstance(result.all(), _future_type(kind))
            assert _value(result.all()) == [a, b]
            assert _value(result.valid()) == [a, b]
            assert _value(result.errors()) == []
        _run_kind(kind, body)

    @pytest.mark.parametrize("kind", KINDS)
    def test_with_errors(self, kind: type[Sync] | type[Async]) -> None:
        def body(kind: type[Sync] | type[Async]) -> None:
            a = _listing("a")
            first = GroupAuthorizationError(message="first")
            second = TopicAuthorizationError(message="second")
            result = ListGroupsResult._new(future=_done(kind, [first, a, second]), kind=kind)
            assert _error(result.all()) is first
            assert _value(result.valid()) == [a]
            assert _value(result.errors()) == [first, second]
        _run_kind(kind, body)

    def test_pending_and_failed_source(self) -> None:
        source = _new_future(Sync)
        result = ListGroupsResult._new(future=source, kind=Sync)
        assert not result.all().is_done()
        _complete(source, [_listing("a")])
        assert _result(result.valid()) == [_listing("a")]
        # As in Java, a failed source leaves the three futures pending.
        failed = ListGroupsResult._new(future=_failed(Sync, RuntimeError("x")), kind=Sync)
        assert not failed.all().is_done()
        assert not failed.valid().is_done()
        assert not failed.errors().is_done()

    async def test_unread_futures_do_not_log(self) -> None:
        error = GroupAuthorizationError(message="first")

        def with_errors() -> None:
            ListGroupsResult._new(future=_done(Async, [error, _listing("a")]), kind=Async)

        def failed_source() -> None:
            ListGroupsResult._new(future=_failed(Async, error), kind=Async)

        def pending_source() -> None:
            source: Any = _new_future(Async)
            ListGroupsResult._new(future=source, kind=Async)
            _complete_with_error(source, error)

        for body in (with_errors, failed_source, pending_source):
            assert await _unretrieved_logs(body) == [], body.__name__

    async def test_read_futures_still_fail(self) -> None:
        error = GroupAuthorizationError(message="first")
        result = ListGroupsResult._new(future=_done(Async, [error]), kind=Async)
        with pytest.raises(GroupAuthorizationError):
            await _await(result.all())
        assert _error(result.all()) is error

    async def test_async_awaits(self) -> None:
        result = ListGroupsResult._new(future=_done(Async, [_listing("a")]), kind=Async)
        assert (await _await(result.all())) == [_listing("a")]
        assert (await _await(result.errors())) == []


# --------------------------------------------------------------------------------------
# RemoveMembersFromConsumerGroupResult (translates RemoveMembersFromConsumerGroupResultTest)
# --------------------------------------------------------------------------------------


class TestRemoveMembersFromConsumerGroupResult:
    instance_one = MemberToRemove(group_instance_id="instance-1")
    instance_two = MemberToRemove(group_instance_id="instance-2")

    def setup_method(self) -> None:
        self.member_futures: Any = _new_future(Sync)
        self.members_to_remove = {self.instance_one, self.instance_two}
        self.errors_map: dict[_MemberIdentity, BaseException | None] = {
            _to_member_identity(self.instance_one): None,
            _to_member_identity(self.instance_two): FencedInstanceIdError(message="fenced"),
        }

    def _result(self) -> Any:
        return RemoveMembersFromConsumerGroupResult._new(
            future=self.member_futures, member_infos=self.members_to_remove, kind=Sync)

    def test_top_level_error_constructor(self) -> None:
        _complete_with_error(self.member_futures, GroupAuthorizationError(message="denied"))
        top_level_error_result = self._result()
        assert isinstance(_exception(top_level_error_result.all()), GroupAuthorizationError)

    def test_member_level_error_constructor(self) -> None:
        self._create_and_verify_member_level_error()

    def test_member_missing_error_in_request_constructor(self) -> None:
        del self.errors_map[_to_member_identity(self.instance_two)]
        _complete(self.member_futures, self.errors_map)
        assert _exception(self.member_futures) is None
        missing_member_result = self._result()
        all_error = _exception(missing_member_result.all())
        assert isinstance(all_error, IllegalArgumentError)
        assert str(all_error) == (
            "Member \"MemberIdentity(memberId='', groupInstanceId='instance-2', reason=None)\" "
            "was not included in the removal response")
        assert _result(missing_member_result.member_result(member=self.instance_one)) is None
        assert isinstance(
            _exception(missing_member_result.member_result(member=self.instance_two)),
            IllegalArgumentError)

    def test_member_level_error_in_response_constructor(self) -> None:
        member_level_error_result = self._create_and_verify_member_level_error()
        invalid = MemberToRemove(group_instance_id="invalid-instance-id")
        with pytest.raises(IllegalArgumentError) as info:
            member_level_error_result.member_result(member=invalid)
        # MemberToRemove has no Java toString: Python's default str stands in for
        # Object.toString.
        assert str(info.value) == (
            "Member " + str(invalid) + " was not included in the original request")

    def test_no_error_constructor(self) -> None:
        errors_map = {_to_member_identity(self.instance_one): None,
                      _to_member_identity(self.instance_two): None}
        no_error_result = self._result()
        _complete(self.member_futures, errors_map)
        assert _result(no_error_result.all()) is None
        assert _result(no_error_result.member_result(member=self.instance_one)) is None
        assert _result(no_error_result.member_result(member=self.instance_two)) is None

    def _create_and_verify_member_level_error(self) -> Any:
        _complete(self.member_futures, self.errors_map)
        assert _exception(self.member_futures) is None
        member_level_error_result = self._result()
        assert isinstance(_exception(member_level_error_result.all()), FencedInstanceIdError)
        assert _result(member_level_error_result.member_result(member=self.instance_one)) is None
        assert isinstance(
            _exception(member_level_error_result.member_result(member=self.instance_two)),
            FencedInstanceIdError)
        return member_level_error_result

    def test_remove_all_mode(self) -> None:
        error = FencedInstanceIdError(message="fenced")
        identity = _MemberIdentity(member_id="m-1")
        result = RemoveMembersFromConsumerGroupResult._new(
            future=_done(Sync, {_MemberIdentity(member_id="m-0"): None, identity: error}),
            member_infos=(), kind=Sync)
        all_error = _exception(result.all())
        assert type(all_error) is KafkaError
        assert str(all_error) == (
            "Encounter exception when trying to remove: "
            "MemberIdentity(memberId='m-1', groupInstanceId=None, reason=None)")
        assert all_error.__cause__ is error
        with pytest.raises(IllegalArgumentError) as info:
            result.member_result(member=self.instance_one)
        assert str(info.value) == (
            "The method: memberResult is not applicable in 'removeAll' mode")
        ok = RemoveMembersFromConsumerGroupResult._new(
            future=_done(Sync, {identity: None}), member_infos=(), kind=Sync)
        assert _result(ok.all()) is None

    async def test_async(self) -> None:
        result = RemoveMembersFromConsumerGroupResult._new(
            future=_done(Async, {_to_member_identity(self.instance_one): None}),
            member_infos={self.instance_one}, kind=Async)
        assert isinstance(result.all(), AsyncKafkaFuture)
        assert (await _await(result.all())) is None
        assert (await _await(result.member_result(member=self.instance_one))) is None
