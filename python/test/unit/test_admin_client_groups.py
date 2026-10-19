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

"""The admin client's consumer-group RPCs over the FFI, without a broker:
``describe_consumer_groups``, ``list_consumer_group_offsets``,
``delete_consumer_groups``, ``delete_consumer_group_offsets``, ``list_groups``,
``remove_members_from_consumer_group``, ``alter_consumer_group_offsets`` and
``describe_classic_groups``, sync and async (CLAUDE.md, Python Binding Conventions,
Tests and typing).

Every ``KafkaAdminClientTest.java`` (Apache Kafka 4.4.0) test of these RPCs scripts
broker responses with a ``MockClient``, which the binding cannot inject,
so none is translated here: ``testListGroups*``, ``testDescribeConsumerGroup*``,
``testDescribeMultipleConsumerGroups``, ``testDescribeNonConsumerGroups``,
``testListConsumerGroupOffsets*``, ``testBatchedListConsumerGroupOffsets*``,
``testDeleteConsumerGroups*``, ``testDeleteMultipleConsumerGroupsWithOlderBroker``,
``testDeleteConsumerGroupOffsets*``, ``testDescribeClassicGroups*``,
``testRemoveMembersFromGroup*`` and ``testAlterConsumerGroupOffsets*``. The core's
translations of them are in ``rust/src/admin/kafka_admin_client.rs``
(``test_list_groups*``, ``test_describe_consumer_groups*``,
``test_describe_classic_groups*``, ``test_list_consumer_group_offsets*``,
``test_batched_list_consumer_group_offsets*``, ``test_alter_consumer_group_offsets*``,
``test_delete_consumer_group_offsets*``, ``test_delete_consumer_groups*``,
``test_remove_members_from_group*``).

What is tested instead: each method's keyword-only signature and Java forms; the
closed client's error; that each call returns its Result at once, before its futures
complete, as Java's does; the inputs and the Options setters each call hands the
extension (its ``Admin_<rpc>`` entry wrapped by a recorder that then calls it); each
call's failure against an unreachable bootstrap address, with the core's translation
of Java's message, through ``get()`` and ``ExecutionError``; the conversion of the
extension's plain values into the value classes; and the results the client builds
over the C result's own per-key futures.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from typing import Any

import pytest

import _confluentkafka as _lib  # type: ignore[import-not-found]
from confluent_kafka import ExecutionError, IllegalArgumentError, IllegalStateError
from confluent_kafka.admin import (
    Admin,
    AlterConsumerGroupOffsetsOptions,
    AlterConsumerGroupOffsetsResult,
    AsyncAdmin,
    ClassicGroupDescription,
    ConsumerGroupDescription,
    DeleteConsumerGroupOffsetsOptions,
    DeleteConsumerGroupOffsetsResult,
    DeleteConsumerGroupsOptions,
    DescribeClassicGroupsOptions,
    DescribeConsumerGroupsOptions,
    GroupListing,
    ListConsumerGroupOffsetsOptions,
    ListConsumerGroupOffsetsSpec,
    ListGroupsOptions,
    ListGroupsResult,
    MemberAssignment,
    MemberDescription,
    MemberToRemove,
    RemoveMembersFromConsumerGroupOptions,
    RemoveMembersFromConsumerGroupResult,
    _conversions,
)
from confluent_kafka.admin._futures import Async, Sync
from confluent_kafka.common import (
    AclOperation,
    AsyncKafkaFuture,
    ClassicGroupState,
    GroupState,
    GroupType,
    KafkaError,
    KafkaFuture,
    Node,
    TopicPartition,
)
from confluent_kafka.common.errors import (
    GroupAuthorizationError,
    UnknownMemberIdError,
    UnknownTopicOrPartitionError,
)
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.kafka_future import _KafkaFutureImpl
from confluent_kafka.consumer import OffsetAndMetadata

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
#: ``default.api.timeout.ms`` small enough for the calls that use it (Java's
#: ``removeMembersFromConsumerGroup`` in ``removeAll`` mode describes the group with
#: ``new DescribeConsumerGroupsOptions()``), ``request.timeout.ms`` below it as
#: ``AdminClientConfig`` requires.
FAST_DEFAULT = {**UNREACHABLE, "default.api.timeout.ms": "300", "request.timeout.ms": "100"}
CLOSED = "Cannot accept new calls when AdminClient is closing."
SHORT = 300
#: How long a test waits for a future the core completes (seconds).
WAIT = 30.0
TP0 = TopicPartition(topic="t", partition=0)
TP1 = TopicPartition(topic="t", partition=1)
LCGO_FORMS = ("list_consumer_group_offsets() takes one of (group_id, options), (group_id), "
              "(group_specs, options), (group_specs); got ")


def _timed_out(api_name: str) -> str:
    """The core's translation of Java's driver timeout: the coordinator lookup of
    ``handler.apiName()`` never got a node (``AdminApiDriver`` names the call
    ``apiName + "(api=FIND_COORDINATOR)"``)."""
    return re.escape(f"Timed out waiting for a node assignment. Call: {api_name}"
                     "(api=FIND_COORDINATOR)")


def _fails(future: Any, cls: type[BaseException], match: str | None = None) -> BaseException:
    """Java's ``TestUtils.assertFutureThrows``: ``get()`` raises ``ExecutionError``
    whose cause is exactly of ``cls``, its text matching ``match``."""
    assert isinstance(future, KafkaFuture)
    with pytest.raises(ExecutionError) as info:
        future.get(timeout=WAIT)
    cause = info.value.__cause__
    assert type(cause) is cls, repr(cause)
    if match is not None:
        assert re.search(match, str(cause)), str(cause)
    return cause


async def _afails(future: Any, cls: type[BaseException], match: str | None = None
                  ) -> BaseException:
    """:func:`_fails` for an ``AsyncKafkaFuture``."""
    assert isinstance(future, AsyncKafkaFuture)
    with pytest.raises(ExecutionError) as info:
        await future.get(timeout=WAIT)
    cause = info.value.__cause__
    assert type(cause) is cls, repr(cause)
    if match is not None:
        assert re.search(match, str(cause)), str(cause)
    return cause


def _admin(configs: dict[str, Any] | None = None) -> Admin:
    return Admin.create(configs=dict(UNREACHABLE if configs is None else configs))


def _remove_options(members: list[MemberToRemove] | None = None, *, reason: str | None = None,
                    timeout_ms: int | None = SHORT) -> RemoveMembersFromConsumerGroupOptions:
    options = RemoveMembersFromConsumerGroupOptions(members=members)
    options.timeout_ms(timeout_ms=timeout_ms)
    if reason is not None:
        options.reason(reason=reason)
    return options


def _all_calls(admin: Admin) -> list[Callable[[], object]]:
    """One valid call of each method of this group."""
    offsets = {TP0: OffsetAndMetadata(offset=1)}
    return [
        lambda: admin.describe_consumer_groups(group_ids=["g"]),
        lambda: admin.list_consumer_group_offsets(group_id="g"),
        lambda: admin.list_consumer_group_offsets(
            group_specs={"g": ListConsumerGroupOffsetsSpec()}),
        lambda: admin.delete_consumer_groups(group_ids=["g"]),
        lambda: admin.delete_consumer_group_offsets(group_id="g", partitions=[TP0]),
        lambda: admin.list_groups(),
        lambda: admin.remove_members_from_consumer_group(group_id="g", options=_remove_options()),
        lambda: admin.alter_consumer_group_offsets(group_id="g", offsets=offsets),
        lambda: admin.describe_classic_groups(group_ids=["g"]),
    ]


# ---- signatures and forms ---------------------------------------------------


def test_positional_calls_raise_type_error() -> None:
    with _admin() as admin:
        calls: list[Callable[[], object]] = [
            lambda: admin.describe_consumer_groups(["g"]),  # type: ignore[call-arg]
            lambda: admin.list_consumer_group_offsets("g"),  # type: ignore[call-overload]
            lambda: admin.delete_consumer_groups(["g"]),  # type: ignore[call-arg]
            lambda: admin.delete_consumer_group_offsets("g", [TP0]),  # type: ignore[call-arg]
            lambda: admin.list_groups(ListGroupsOptions()),  # type: ignore[call-arg]
            lambda: admin.remove_members_from_consumer_group(  # type: ignore[call-arg]
                "g", _remove_options()),
            lambda: admin.alter_consumer_group_offsets("g", {}),  # type: ignore[call-arg]
            lambda: admin.describe_classic_groups(["g"]),  # type: ignore[call-arg]
        ]
        for call in calls:
            with pytest.raises(TypeError):
                call()


def test_remove_members_requires_its_options() -> None:
    """Java has only ``removeMembersFromConsumerGroup(groupId, options)``."""
    with _admin() as admin:
        with pytest.raises(TypeError):
            admin.remove_members_from_consumer_group(group_id="g")  # type: ignore[call-arg]


def test_list_consumer_group_offsets_rejects_what_java_has_no_overload_for() -> None:
    with _admin() as admin:
        with pytest.raises(IllegalArgumentError, match=re.escape(LCGO_FORMS + "()")):
            admin.list_consumer_group_offsets()  # type: ignore[call-overload]
        with pytest.raises(IllegalArgumentError,
                           match=re.escape(LCGO_FORMS + "(group_id, group_specs)")):
            admin.list_consumer_group_offsets(  # type: ignore[call-overload]
                group_id="g", group_specs={"g": ListConsumerGroupOffsetsSpec()})
        with pytest.raises(IllegalArgumentError, match=re.escape(LCGO_FORMS + "(options)")):
            admin.list_consumer_group_offsets(  # type: ignore[call-overload]
                options=ListConsumerGroupOffsetsOptions())
        # An explicit None is given: Java's listConsumerGroupOffsets((String) null),
        # whose null group id the FFI cannot carry (Signatures, exception 4).
        with pytest.raises(TypeError, match="^group_id must not be None$"):
            admin.list_consumer_group_offsets(group_id=None)  # type: ignore[call-overload]


async def test_async_list_consumer_group_offsets_rejects_the_same_forms() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        with pytest.raises(IllegalArgumentError, match=re.escape(LCGO_FORMS + "()")):
            admin.list_consumer_group_offsets()  # type: ignore[call-overload]


def test_calls_after_close_raise_illegal_state() -> None:
    admin = _admin()
    calls = _all_calls(admin)
    admin.close()
    for call in calls:
        with pytest.raises(IllegalStateError, match=CLOSED):
            call()


async def test_async_calls_after_close_raise_illegal_state() -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    await admin.close()
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.describe_consumer_groups(group_ids=["g"])
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_consumer_group_offsets(group_id="g")
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_groups()
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.remove_members_from_consumer_group(group_id="g", options=_remove_options())


# ---- the inputs and options each call hands the extension -------------------


class _Recorder:
    """Wraps one ``Admin_<rpc>`` entry of the extension: records what the call
    passes after the client handle (its inputs, then the Options setters that ran as
    ``(setter, value)`` pairs) and calls the entry."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, name: str):
        self.args: Any = None
        self._entry = getattr(_lib, name)
        monkeypatch.setattr(_lib, name, self)

    def __call__(self, c_admin: int, *args: Any) -> Any:
        self.args = args
        return self._entry(c_admin, *args)


def test_calls_return_their_result_at_once() -> None:
    """Java's RPCs return their Result before any future completes: with a long
    ``default.api.timeout.ms``, every future is still pending when the call
    returns. ``close(timeout=0)`` then aborts them, as Java's ``close(Duration.ZERO)``
    does."""
    admin = _admin({**UNREACHABLE, "default.api.timeout.ms": "60000"})
    try:
        described = admin.describe_consumer_groups(group_ids=["g"])
        assert not described.described_groups()["g"].is_done()
        assert not described.all().is_done()
        listed = admin.list_groups()
        assert not listed.all().is_done()
        offsets = admin.list_consumer_group_offsets(group_id="g")
        assert not offsets.partitions_to_offset_and_metadata().is_done()
        deleted = admin.delete_consumer_group_offsets(group_id="g", partitions=[TP0])
        assert not deleted.partition_result(partition=TP0).is_done()
    finally:
        admin.close(timeout=0)


def test_describe_consumer_groups_passes_its_options(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_describe_consumer_groups")
    with _admin() as admin:
        result = admin.describe_consumer_groups(
            group_ids=iter(["g1", "g2"]),
            options=DescribeConsumerGroupsOptions(timeout_ms=SHORT,
                                                  include_authorized_operations=True))
        assert rec.args == (["g1", "g2"], [("timeout_ms", SHORT),
                                           ("include_authorized_operations", True)])
        assert set(result.described_groups()) == {"g1", "g2"}
        for future in result.described_groups().values():
            _fails(future, KafkaTimeoutError, _timed_out("describeConsumerGroups"))
        _fails(result.all(), KafkaTimeoutError, _timed_out("describeConsumerGroups"))
        admin.describe_consumer_groups(group_ids=[])
        # Java's describeConsumerGroups(groupIds): new DescribeConsumerGroupsOptions().
        assert rec.args == ([], [])


def test_describe_classic_groups_passes_its_options(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_describe_classic_groups")
    with _admin() as admin:
        result = admin.describe_classic_groups(
            group_ids=["g1"],
            options=DescribeClassicGroupsOptions(timeout_ms=SHORT,
                                                 include_authorized_operations=True))
        assert rec.args == (["g1"], [("timeout_ms", SHORT),
                                     ("include_authorized_operations", True)])
        _fails(result.described_groups()["g1"], KafkaTimeoutError,
               _timed_out("describeClassicGroups"))
        admin.describe_classic_groups(group_ids=[])
        assert rec.args == ([], [])


def test_list_consumer_group_offsets_passes_its_specs(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_list_consumer_group_offsets")
    with _admin(FAST_DEFAULT) as admin:
        # Java's listConsumerGroupOffsets(groupId): its own entry point, default options.
        result = admin.list_consumer_group_offsets(group_id="g")
        assert rec.args == ("g", None, [])
        _fails(result.partitions_to_offset_and_metadata(), KafkaTimeoutError,
               _timed_out("offsetFetch"))
        admin.list_consumer_group_offsets(
            group_id="g", options=ListConsumerGroupOffsetsOptions(timeout_ms=7,
                                                                  require_stable=True))
        assert rec.args == ("g", None, [("timeout_ms", 7), ("require_stable", True)])
        result = admin.list_consumer_group_offsets(group_specs={
            "a": ListConsumerGroupOffsetsSpec(topic_partitions=[TP0, TP1]),
            "b": ListConsumerGroupOffsetsSpec()})
        assert rec.args is not None
        group_id, rows, pairs = rec.args
        assert group_id is None and pairs == []
        assert rows[1] == ("b", None)
        assert rows[0][0] == "a" and sorted(rows[0][1]) == [("t", 0), ("t", 1)]
        _fails(result.partitions_to_offset_and_metadata(group_id="a"), KafkaTimeoutError)
        _fails(result.all(), KafkaTimeoutError)
        with pytest.raises(IllegalStateError, match="Offsets from multiple consumer groups"):
            result.partitions_to_offset_and_metadata()


def test_delete_consumer_groups_passes_its_options(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_delete_consumer_groups")
    with _admin() as admin:
        result = admin.delete_consumer_groups(group_ids=["g1", "g2"],
                                              options=DeleteConsumerGroupsOptions(timeout_ms=9))
        assert rec.args == (["g1", "g2"], [("timeout_ms", 9)])
        assert set(result.deleted_groups()) == {"g1", "g2"}
        _fails(result.all(), KafkaTimeoutError, _timed_out("deleteConsumerGroups"))
        admin.delete_consumer_groups(group_ids=[])
        assert rec.args == ([], [])


def test_delete_consumer_group_offsets_passes_its_options(
        monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_delete_consumer_group_offsets")
    with _admin(FAST_DEFAULT) as admin:
        result = admin.delete_consumer_group_offsets(
            group_id="g", partitions={TP0},
            options=DeleteConsumerGroupOffsetsOptions(timeout_ms=SHORT))
        assert rec.args == ("g", [("t", 0)], [("timeout_ms", SHORT)])
        _fails(result.partition_result(partition=TP0), KafkaTimeoutError,
               _timed_out("offsetDelete"))
        _fails(result.all(), KafkaTimeoutError, _timed_out("offsetDelete"))
        with pytest.raises(IllegalArgumentError,
                           match=re.escape("Partition t-1 was not included in the original "
                                           "request")):
            result.partition_result(partition=TP1)
        admin.delete_consumer_group_offsets(group_id="g", partitions=[])
        assert rec.args == ("g", [], [])


def test_list_groups_passes_its_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_list_groups")
    with _admin(FAST_DEFAULT) as admin:
        admin.list_groups()
        # Java's listGroups(): new ListGroupsOptions(), no setter run.
        assert rec.args == ([],)
        options = ListGroupsOptions(timeout_ms=11, group_states=[GroupState.STABLE],
                                    types=[GroupType.CLASSIC], protocol_types=["consumer"])
        admin.list_groups(options=options)
        # The stored sets, each enum as its toString() name, which the extension
        # parses back (GroupState.parse / GroupType.parse).
        assert rec.args == ([("timeout_ms", 11), ("in_group_states", ["Stable"]),
                             ("with_types", ["Classic"]),
                             ("with_protocol_types", ["consumer"])],)
        # Java's null set: Set.of(), sent as the empty stored set.
        admin.list_groups(options=ListGroupsOptions().in_group_states(group_states=None))
        assert rec.args == ([("in_group_states", [])],)


def test_remove_members_passes_its_options(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_remove_members_from_consumer_group")
    with _admin(FAST_DEFAULT) as admin:
        member = MemberToRemove(group_instance_id="i1")
        result = admin.remove_members_from_consumer_group(
            group_id="g", options=_remove_options([member], reason="bye", timeout_ms=4))
        assert rec.args == ("g", ["i1"], [("timeout_ms", 4), ("reason", "bye")])
        _fails(result.all(), KafkaTimeoutError, _timed_out("leaveGroup"))
        _fails(result.member_result(member=member), KafkaTimeoutError, _timed_out("leaveGroup"))
        # removeAll mode: no members; a null timeout crosses as None (the core's -1:
        # default.api.timeout.ms).
        result = admin.remove_members_from_consumer_group(
            group_id="g", options=_remove_options(timeout_ms=None))
        assert rec.args == ("g", None, [("timeout_ms", None)])
        with pytest.raises(IllegalArgumentError,
                           match="The method: memberResult is not applicable in 'removeAll' mode"):
            result.member_result(member=member)


def test_alter_consumer_group_offsets_passes_its_offsets(
        monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_alter_consumer_group_offsets")
    with _admin(FAST_DEFAULT) as admin:
        result = admin.alter_consumer_group_offsets(
            group_id="g",
            offsets={TP0: OffsetAndMetadata(offset=5, leader_epoch=3, metadata="m"),
                     TP1: OffsetAndMetadata(offset=6)},
            options=AlterConsumerGroupOffsetsOptions(timeout_ms=SHORT))
        # (topic, partition, offset, metadata, leader_epoch), -1 for an absent epoch.
        assert rec.args == ("g", [("t", 0, 5, "m", 3), ("t", 1, 6, "", -1)],
                            [("timeout_ms", SHORT)])
        _fails(result.partition_result(partition=TP0), KafkaTimeoutError,
               _timed_out("offsetCommit"))
        _fails(result.all(), KafkaTimeoutError, _timed_out("offsetCommit"))
        admin.alter_consumer_group_offsets(group_id="g", offsets={})
        assert rec.args == ("g", [], [])


def test_negative_option_timeout_expires_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Java's ``calcDeadlineMs`` takes ``Math.max(0, timeoutMs)``: a given negative
    timeout expires at once (the extension sends it as 0), never as the C API's
    "not set" ``-1``, which would wait out ``default.api.timeout.ms``."""
    rec = _Recorder(monkeypatch, "Admin_delete_consumer_groups")
    with _admin({**UNREACHABLE, "default.api.timeout.ms": "600000"}) as admin:
        result = admin.delete_consumer_groups(group_ids=["g"],
                                              options=DeleteConsumerGroupsOptions(timeout_ms=-5))
        assert rec.args == (["g"], [("timeout_ms", -5)])
        _fails(result.all(), KafkaTimeoutError, _timed_out("deleteConsumerGroups"))


async def test_async_calls_pass_the_same_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_list_consumer_group_offsets")
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        result = admin.list_consumer_group_offsets(
            group_specs={"a": ListConsumerGroupOffsetsSpec(topic_partitions=[TP0])},
            options=ListConsumerGroupOffsetsOptions(timeout_ms=SHORT))
        assert rec.args == (None, [("a", [("t", 0)])], [("timeout_ms", SHORT)])
        future = result.partitions_to_offset_and_metadata()
        assert isinstance(future, AsyncKafkaFuture)
        await _afails(future, KafkaTimeoutError, _timed_out("offsetFetch"))
        await _afails(result.all(), KafkaTimeoutError, _timed_out("offsetFetch"))


# ---- against an unreachable bootstrap: each call times out ------------------


def test_describe_consumer_groups_times_out() -> None:
    with _admin() as admin:
        result = admin.describe_consumer_groups(
            group_ids=["g1", "g2"], options=DescribeConsumerGroupsOptions(timeout_ms=SHORT))
        for future in result.described_groups().values():
            _fails(future, KafkaTimeoutError, _timed_out("describeConsumerGroups"))
        _fails(result.all(), KafkaTimeoutError, _timed_out("describeConsumerGroups"))
        assert admin.describe_consumer_groups(group_ids=[]).all().get(timeout=WAIT) == {}


def test_list_consumer_group_offsets_times_out() -> None:
    with _admin() as admin:
        result = admin.list_consumer_group_offsets(
            group_id="g", options=ListConsumerGroupOffsetsOptions(timeout_ms=SHORT))
        _fails(result.partitions_to_offset_and_metadata(), KafkaTimeoutError,
               _timed_out("offsetFetch"))
        result = admin.list_consumer_group_offsets(
            group_specs={"a": ListConsumerGroupOffsetsSpec(topic_partitions=[TP0]),
                         "b": ListConsumerGroupOffsetsSpec()},
            options=ListConsumerGroupOffsetsOptions(timeout_ms=SHORT, require_stable=True))
        for group_id in ("a", "b"):
            _fails(result.partitions_to_offset_and_metadata(group_id=group_id),
                   KafkaTimeoutError, _timed_out("offsetFetch"))
        with pytest.raises(IllegalArgumentError,
                           match="Offsets for consumer group 'c' were not requested."):
            result.partitions_to_offset_and_metadata(group_id="c")
        assert admin.list_consumer_group_offsets(group_specs={}).all().get(timeout=WAIT) == {}


def test_delete_consumer_groups_times_out() -> None:
    with _admin() as admin:
        result = admin.delete_consumer_groups(
            group_ids=["g"], options=DeleteConsumerGroupsOptions(timeout_ms=SHORT))
        _fails(result.deleted_groups()["g"], KafkaTimeoutError, _timed_out("deleteConsumerGroups"))
        _fails(result.all(), KafkaTimeoutError, _timed_out("deleteConsumerGroups"))


def test_delete_consumer_group_offsets_times_out() -> None:
    with _admin() as admin:
        options = DeleteConsumerGroupOffsetsOptions(timeout_ms=SHORT)
        result = admin.delete_consumer_group_offsets(group_id="g", partitions=[TP0],
                                                     options=options)
        _fails(result.partition_result(partition=TP0), KafkaTimeoutError,
               _timed_out("offsetDelete"))
        _fails(result.all(), KafkaTimeoutError, _timed_out("offsetDelete"))
        # No partition: the call's own future still times out (the core's all()).
        result = admin.delete_consumer_group_offsets(group_id="g", partitions=[],
                                                     options=options)
        _fails(result.all(), KafkaTimeoutError, _timed_out("offsetDelete"))


def test_list_groups_fails_to_find_brokers() -> None:
    """Java's ``listGroups`` completes with the failure as an element:
    ``KafkaException("Failed to find brokers to send ListGroups", cause)``."""
    with _admin() as admin:
        result = admin.list_groups(options=ListGroupsOptions(timeout_ms=SHORT))
        assert result.valid().get(timeout=WAIT) == []
        errors = result.errors().get(timeout=WAIT)
        assert len(errors) == 1
        assert type(errors[0]) is KafkaError
        assert str(errors[0]) == "Failed to find brokers to send ListGroups"
        assert isinstance(errors[0].__cause__, KafkaTimeoutError)
        assert str(errors[0].__cause__) == (
            "Timed out waiting for a node assignment. Call: findAllBrokers")
        _fails(result.all(), KafkaError, "Failed to find brokers to send ListGroups")
        # Java's fields: the same future on every call.
        assert result.all() is result.all() and result.valid() is result.valid()


def test_remove_members_times_out() -> None:
    with _admin(FAST_DEFAULT) as admin:
        member = MemberToRemove(group_instance_id="i1")
        result = admin.remove_members_from_consumer_group(
            group_id="g", options=_remove_options([member]))
        _fails(result.member_result(member=member), KafkaTimeoutError, _timed_out("leaveGroup"))
        _fails(result.all(), KafkaTimeoutError, _timed_out("leaveGroup"))
        with pytest.raises(IllegalArgumentError, match="was not included in the original request"):
            result.member_result(member=MemberToRemove(group_instance_id="other"))


def test_remove_all_members_fails_to_describe_the_group() -> None:
    """``removeAll`` mode first describes the group with default options
    (``getMembersFromGroup``), so ``default.api.timeout.ms`` bounds it. The core
    reports Java's ``KafkaException("Encounter exception when trying to get members
    from group: g", cause)`` with "error" in place of "exception" (a core
    divergence from Java's text)."""
    with _admin(FAST_DEFAULT) as admin:
        result = admin.remove_members_from_consumer_group(group_id="g",
                                                          options=_remove_options())
        cause = _fails(result.all(), KafkaError)
        assert str(cause) == "Encounter error when trying to get members from group: g"
        assert isinstance(cause.__cause__, KafkaTimeoutError)
        assert re.fullmatch(_timed_out("describeConsumerGroups"), str(cause.__cause__))


def test_alter_consumer_group_offsets_times_out() -> None:
    """The driver's timeout fails Java's one future, so ``all()`` reports it as it
    is, not renamed into "Failed altering group offsets ...", and so does a
    partition not in the request (Java checks it only once that future
    succeeded)."""
    with _admin() as admin:
        result = admin.alter_consumer_group_offsets(
            group_id="g", offsets={TP0: OffsetAndMetadata(offset=5)},
            options=AlterConsumerGroupOffsetsOptions(timeout_ms=SHORT))
        _fails(result.partition_result(partition=TP0), KafkaTimeoutError,
               _timed_out("offsetCommit"))
        _fails(result.all(), KafkaTimeoutError, _timed_out("offsetCommit"))
        _fails(result.partition_result(partition=TP1), KafkaTimeoutError,
               _timed_out("offsetCommit"))


def test_describe_classic_groups_times_out() -> None:
    with _admin() as admin:
        result = admin.describe_classic_groups(
            group_ids=["g"], options=DescribeClassicGroupsOptions(timeout_ms=SHORT))
        _fails(result.described_groups()["g"], KafkaTimeoutError,
               _timed_out("describeClassicGroups"))
        assert admin.describe_classic_groups(group_ids=[]).all().get(timeout=WAIT) == {}


async def test_async_calls_time_out() -> None:
    async with AsyncAdmin.create(configs=FAST_DEFAULT) as admin:
        described = admin.describe_consumer_groups(
            group_ids=["g"], options=DescribeConsumerGroupsOptions(timeout_ms=SHORT))
        future = described.described_groups()["g"]
        assert isinstance(future, AsyncKafkaFuture)
        await _afails(described.all(), KafkaTimeoutError, _timed_out("describeConsumerGroups"))
        offsets = admin.list_consumer_group_offsets(group_id="g")
        await _afails(offsets.all(), KafkaTimeoutError, _timed_out("offsetFetch"))
        deleted = admin.delete_consumer_groups(group_ids=["g"])
        await _afails(deleted.all(), KafkaTimeoutError, _timed_out("deleteConsumerGroups"))
        offsets_deleted = admin.delete_consumer_group_offsets(group_id="g", partitions=[TP0])
        await _afails(offsets_deleted.partition_result(partition=TP0), KafkaTimeoutError,
                      _timed_out("offsetDelete"))
        listed = admin.list_groups()
        assert [str(e) for e in await listed.errors().get(timeout=WAIT)] == [
            "Failed to find brokers to send ListGroups"]
        await _afails(listed.all(), KafkaError, "Failed to find brokers to send ListGroups")
        member = MemberToRemove(group_instance_id="i1")
        removed = admin.remove_members_from_consumer_group(
            group_id="g", options=_remove_options([member]))
        await _afails(removed.member_result(member=member), KafkaTimeoutError,
                      _timed_out("leaveGroup"))
        altered = admin.alter_consumer_group_offsets(
            group_id="g", offsets={TP0: OffsetAndMetadata(offset=1)})
        await _afails(altered.all(), KafkaTimeoutError, _timed_out("offsetCommit"))
        classic = admin.describe_classic_groups(group_ids=["g"])
        await _afails(classic.all(), KafkaTimeoutError, _timed_out("describeClassicGroups"))


# ---- the extension's plain values into the value classes --------------------


_MEMBER = ("consumer-1", "instance-1", None, "client-1", "/10.0.0.1",
           [("t", 0), ("t", 1)], [("t", 1)], 7, None)
_NODE = (2, "broker2", 9092, "rack-a")


def test_consumer_group_description_from_the_extension() -> None:
    description = ("g", False, [_MEMBER], "uniform", "Consumer", "Stable", _NODE,
                   [AclOperation.READ.code(), AclOperation.DESCRIBE.code()], 5, 4)
    value = _conversions._consumer_group_description(description)
    expected_member = MemberDescription(
        member_id="consumer-1", group_instance_id="instance-1", rack_id=None,
        client_id="client-1", host="/10.0.0.1",
        assignment=MemberAssignment(topic_partitions=[TP0, TP1]),
        target_assignment=MemberAssignment(topic_partitions=[TP1]), member_epoch=7,
        upgraded=None)
    assert value == ConsumerGroupDescription(
        group_id="g", is_simple_consumer_group=False, members=[expected_member],
        partition_assignor="uniform", type=GroupType.CONSUMER, group_state=GroupState.STABLE,
        coordinator=Node(id=2, host="broker2", port=9092, rack="rack-a"),
        authorized_operations={AclOperation.READ, AclOperation.DESCRIBE}, group_epoch=5,
        target_assignment_epoch=4)
    coordinator = value.coordinator()
    assert coordinator is not None and coordinator.is_fenced() is False
    # authorizedOperations() is null unless requested; an absent coordinator is null.
    omitted = _conversions._consumer_group_description(
        ("g", True, [], "", "Classic", "Empty", None, None, None, None))
    assert omitted.authorized_operations() is None and omitted.coordinator() is None
    assert omitted.type() is GroupType.CLASSIC and omitted.group_state() is GroupState.EMPTY


def test_classic_group_description_keeps_a_null_operations_set() -> None:
    """Decision log 41: built through ``_new``, an authorized operations set the
    core reports as ``null`` stays ``None``, where the public constructor's "not
    given" would give ``set()``."""
    member = ("m", None, None, "c", "h", [("t", 0)], None, None, None)
    value = _conversions._classic_group_description(
        ("g", "range", "consumer", False, [member], "Stable", _NODE, None))
    assert isinstance(value, ClassicGroupDescription)
    assert value.authorized_operations() is None
    assert value.state() is ClassicGroupState.STABLE
    assert value.protocol() == "range" and value.protocol_data() == "consumer"
    assignment = value.members()[0].assignment()
    assert assignment is not None and assignment.topic_partitions() == {TP0}
    assert value.members()[0].target_assignment() is None
    with_ops = _conversions._classic_group_description(
        ("g", "", "", True, [], "Dead", None, [AclOperation.ALTER.code()]))
    assert with_ops.authorized_operations() == {AclOperation.ALTER}
    assert with_ops.is_simple_consumer_group() and with_ops.state() is ClassicGroupState.DEAD


def test_group_offsets_from_the_extension() -> None:
    value = _conversions._group_offsets(
        {("t", 0): (5, "meta", 3), ("t", 1): (6, "", None), ("u", 0): None})
    assert value == {TP0: OffsetAndMetadata(offset=5, leader_epoch=3, metadata="meta"),
                     TP1: OffsetAndMetadata(offset=6, leader_epoch=None, metadata=""),
                     TopicPartition(topic="u", partition=0): None}


def test_group_listings_from_the_extension() -> None:
    valid = [("g1", "Consumer", "consumer", "Stable", False), ("g2", None, "", None, False)]
    assert _conversions._group_listings(valid) == [
        GroupListing(group_id="g1", type=GroupType.CONSUMER, protocol="consumer",
                     group_state=GroupState.STABLE),
        GroupListing(group_id="g2", type=None, protocol="", group_state=None)]


# ---- the results over the C result's own futures ----------------------------


def _done(value: Any) -> KafkaFuture[Any]:
    impl: _KafkaFutureImpl[Any] = _KafkaFutureImpl()
    impl.complete(value)
    return KafkaFuture._of_impl(impl)


def _failed(error: BaseException) -> KafkaFuture[Any]:
    impl: _KafkaFutureImpl[Any] = _KafkaFutureImpl()
    impl.complete_with_error(error)
    return KafkaFuture._of_impl(impl)


def test_outcomes_carry_each_keys_outcome_as_it_stands() -> None:
    """Java's one ``Map<K, Errors>`` future, read from the core's per-key futures:
    a key's error is kept as it is (``Errors.exception()``, the call's failure or
    the result's absent-key ``IllegalArgumentException``), ``None`` for success; it
    completes once every key's future is done."""
    error = UnknownTopicOrPartitionError(message="This server does not host this "
                                                 "topic-partition.")
    pending: _KafkaFutureImpl[None] = _KafkaFutureImpl()
    other = TopicPartition(topic="u", partition=0)
    future = _conversions.outcomes(
        Sync, None, {TP0: _done(None), TP1: _failed(error), other: KafkaFuture._of_impl(pending)})
    assert isinstance(future, KafkaFuture)
    assert not future.is_done()
    pending.complete(None)
    outcome = future.get(timeout=WAIT)
    assert outcome == {TP0: None, TP1: error, other: None}
    assert outcome[TP1] is error


def test_delete_consumer_group_offsets_result_over_the_cores_partition_results() -> None:
    """Over :func:`outcomes`, ``DeleteConsumerGroupOffsetsResult`` answers each case
    as Java's does: a partition's own error, a partition the response left out (the
    core's absent-key error, Java's text), and the call's failure, which every
    partition carries."""
    absent = IllegalArgumentError(message='Offset deletion result for partition "t-1" was '
                                          'not included in the response')
    result = DeleteConsumerGroupOffsetsResult._new(
        future=_conversions.outcomes(Sync, None, {TP0: _done(None), TP1: _failed(absent)}),
        partitions=[TP0, TP1], kind=Sync)
    assert result.partition_result(partition=TP0).get(timeout=WAIT) is None
    assert _fails(result.partition_result(partition=TP1), IllegalArgumentError) is absent
    assert _fails(result.all(), IllegalArgumentError) is absent

    denied = GroupAuthorizationError(message="denied")
    result = DeleteConsumerGroupOffsetsResult._new(
        future=_conversions.outcomes(Sync, None, {TP0: _failed(denied), TP1: _failed(denied)}),
        partitions=[TP0, TP1], kind=Sync)
    assert _fails(result.partition_result(partition=TP0), GroupAuthorizationError) is denied
    assert _fails(result.all(), GroupAuthorizationError) is denied


def test_client_results_return_the_cores_futures() -> None:
    """The client's ``AlterConsumerGroupOffsetsResult``,
    ``RemoveMembersFromConsumerGroupResult`` and ``ListGroupsResult`` hand out the
    core's own futures: a new one from each ``partition_result`` / ``member_result``
    / ``all`` call, as Java's do, and Java's three fields on ``ListGroupsResult``."""
    asked: list[Any] = []
    error = UnknownMemberIdError(message="The coordinator is not aware of this member.")

    def partition_result(partition: TopicPartition) -> KafkaFuture[None]:
        asked.append(partition)
        return _done(None) if partition == TP0 else _failed(error)

    altered = AlterConsumerGroupOffsetsResult._of_client(
        partition_result=partition_result, all=lambda: _failed(error), kind=Sync)
    assert altered.partition_result(partition=TP0).get(timeout=WAIT) is None
    assert _fails(altered.partition_result(partition=TP1), UnknownMemberIdError) is error
    assert asked == [TP0, TP1]
    assert altered.all() is not altered.all()
    assert _fails(altered.all(), UnknownMemberIdError) is error

    member = MemberToRemove(group_instance_id="i1")
    members: list[MemberToRemove] = []

    def member_result(m: MemberToRemove) -> KafkaFuture[None]:
        members.append(m)
        return _done(None)

    removed = RemoveMembersFromConsumerGroupResult._of_client(
        member_result=member_result, all=lambda: _done(None), member_infos=[member], kind=Sync)
    assert removed.member_result(member=member).get(timeout=WAIT) is None
    assert members == [member]
    assert removed.all().get(timeout=WAIT) is None

    listing = GroupListing(group_id="g", type=None, protocol="", group_state=None)
    all_future, valid, errors = _done([listing]), _done([listing]), _done([])
    listed = ListGroupsResult._of_client(all=all_future, valid=valid, errors=errors, kind=Sync)
    assert listed.all() is all_future and listed.valid() is valid
    assert listed.errors() is errors


async def test_async_outcomes_are_async_futures() -> None:
    future = _conversions.outcomes(Async, asyncio.get_running_loop(), {"k": _done(None)})
    assert isinstance(future, AsyncKafkaFuture)
    assert await future.get(timeout=WAIT) == {"k": None}
