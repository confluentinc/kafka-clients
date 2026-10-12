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

Every ``KafkaAdminClientTest.java`` (Apache Kafka 4.3.1) test of these RPCs scripts
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
closed client's error; the arguments each Options object reaches the extension
with (the ``_async`` entry point replaced by a recorder that answers with a
whole-call error, which fails every future); each call's failure against an
unreachable bootstrap address, with the core's translation of Java's message; and
the conversion of the extension's plain values into the value classes and result
futures, fed with the shapes the C drains document.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from typing import Any

import pytest

import _confluentkafka as _lib  # type: ignore[import-not-found]
from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka.admin import (
    Admin,
    AlterConsumerGroupOffsetsOptions,
    AsyncAdmin,
    ClassicGroupDescription,
    ConsumerGroupDescription,
    DeleteConsumerGroupOffsetsOptions,
    DeleteConsumerGroupsOptions,
    DescribeClassicGroupsOptions,
    DescribeConsumerGroupsOptions,
    GroupListing,
    ListConsumerGroupOffsetsOptions,
    ListConsumerGroupOffsetsSpec,
    ListGroupsOptions,
    MemberAssignment,
    MemberDescription,
    MemberToRemove,
    RemoveMembersFromConsumerGroupOptions,
    _conversions,
)
from confluent_kafka.admin._futures import Sync
from confluent_kafka.common import (
    AclOperation,
    ClassicGroupState,
    GroupState,
    GroupType,
    KafkaError,
    Node,
    TopicPartition,
)
from confluent_kafka.common.errors import (
    GroupAuthorizationError,
    GroupIdNotFoundError,
    UnknownMemberIdError,
    UnknownTopicOrPartitionError,
)
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.consumer import OffsetAndMetadata

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
#: ``default.api.timeout.ms`` small enough for the calls that use it (Java's
#: ``removeMembersFromConsumerGroup`` in ``removeAll`` mode describes the group with
#: ``new DescribeConsumerGroupsOptions()``), ``request.timeout.ms`` below it as
#: ``AdminClientConfig`` requires.
FAST_DEFAULT = {**UNREACHABLE, "default.api.timeout.ms": "300", "request.timeout.ms": "100"}
CLOSED = "Cannot accept new calls when AdminClient is closing."
SHORT = 300
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
            lambda: admin.describe_consumer_groups(["g"]),  # type: ignore[call-arg, misc]
            lambda: admin.list_consumer_group_offsets("g"),  # type: ignore[call-overload]
            lambda: admin.delete_consumer_groups(["g"]),  # type: ignore[call-arg, misc]
            lambda: admin.delete_consumer_group_offsets("g", [TP0]),  # type: ignore[call-arg, misc]
            lambda: admin.list_groups(ListGroupsOptions()),  # type: ignore[call-arg, misc]
            lambda: admin.remove_members_from_consumer_group(  # type: ignore[call-arg, misc]
                "g", _remove_options()),
            lambda: admin.alter_consumer_group_offsets("g", {}),  # type: ignore[call-arg, misc]
            lambda: admin.describe_classic_groups(["g"]),  # type: ignore[call-arg, misc]
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
        # An explicit None is "not given" (Signatures).
        with pytest.raises(IllegalArgumentError, match=re.escape(LCGO_FORMS + "()")):
            admin.list_consumer_group_offsets(group_id=None)  # type: ignore[call-overload]


async def test_async_list_consumer_group_offsets_rejects_the_same_forms() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        with pytest.raises(IllegalArgumentError, match=re.escape(LCGO_FORMS + "()")):
            await admin.list_consumer_group_offsets()  # type: ignore[call-overload]


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
        await admin.describe_consumer_groups(group_ids=["g"])
    with pytest.raises(IllegalStateError, match=CLOSED):
        await admin.list_consumer_group_offsets(group_id="g")
    with pytest.raises(IllegalStateError, match=CLOSED):
        await admin.list_groups()
    with pytest.raises(IllegalStateError, match=CLOSED):
        await admin.remove_members_from_consumer_group(group_id="g", options=_remove_options())


# ---- the arguments each call hands the extension ----------------------------


class _Recorder:
    """Stands in for one ``Admin_*_async`` entry point: records the arguments and
    answers at once with a whole-call error (an owned handle), so no broker is
    needed and every future of the result fails with it."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, name: str, value_payload: bool = True):
        self.args: tuple[Any, ...] | None = None
        self._value_payload = value_payload
        monkeypatch.setattr(_lib, name, self)

    def __call__(self, c_admin: int, *rest: Any) -> None:
        *args, cb = rest
        self.args = tuple(args)
        error = _lib.KafkaError_new(GroupAuthorizationError._ffi_id, "denied")
        if self._value_payload:
            cb(0, error)
        else:
            cb(error)


def _assert_denied(future: Any) -> None:
    with pytest.raises(GroupAuthorizationError, match="denied"):
        future.result()


def test_describe_consumer_groups_passes_its_options(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_describe_consumer_groups_async")
    with _admin() as admin:
        result = admin.describe_consumer_groups(
            group_ids=iter(["g1", "g2"]),
            options=DescribeConsumerGroupsOptions(timeout_ms=1234,
                                                  include_authorized_operations=True))
        assert rec.args == (["g1", "g2"], 1234, True)
        assert set(result.described_groups()) == {"g1", "g2"}
        for future in result.described_groups().values():
            _assert_denied(future)
        _assert_denied(result.all())
        admin.describe_consumer_groups(group_ids=["g"])
        # Java's describeConsumerGroups(groupIds): new DescribeConsumerGroupsOptions().
        assert rec.args == (["g"], -1, False)


def test_describe_classic_groups_passes_its_options(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_describe_classic_groups_async")
    with _admin() as admin:
        result = admin.describe_classic_groups(
            group_ids=["g1"],
            options=DescribeClassicGroupsOptions(timeout_ms=5,
                                                 include_authorized_operations=True))
        assert rec.args == (["g1"], 5, True)
        _assert_denied(result.described_groups()["g1"])
        admin.describe_classic_groups(group_ids=["g"])
        assert rec.args == (["g"], -1, False)


def test_list_consumer_group_offsets_passes_its_specs(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_list_consumer_group_offsets_async")
    with _admin() as admin:
        # Java's listConsumerGroupOffsets(groupId): one group, new
        # ListConsumerGroupOffsetsSpec() (all partitions), default options.
        result = admin.list_consumer_group_offsets(group_id="g")
        assert rec.args == ([("g", True, [])], -1, False)
        _assert_denied(result.partitions_to_offset_and_metadata())
        admin.list_consumer_group_offsets(
            group_id="g", options=ListConsumerGroupOffsetsOptions(timeout_ms=7,
                                                                  require_stable=True))
        assert rec.args == ([("g", True, [])], 7, True)
        result = admin.list_consumer_group_offsets(group_specs={
            "a": ListConsumerGroupOffsetsSpec(topic_partitions=[TP0, TP1]),
            "b": ListConsumerGroupOffsetsSpec()})
        assert rec.args == ([("a", False, [("t", 0), ("t", 1)]), ("b", True, [])], -1, False)
        _assert_denied(result.partitions_to_offset_and_metadata(group_id="a"))
        _assert_denied(result.all())
        with pytest.raises(IllegalStateError):
            result.partitions_to_offset_and_metadata()


def test_delete_consumer_groups_passes_its_options(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_delete_consumer_groups_async")
    with _admin() as admin:
        result = admin.delete_consumer_groups(group_ids=["g1", "g2"],
                                              options=DeleteConsumerGroupsOptions(timeout_ms=9))
        assert rec.args == (["g1", "g2"], 9)
        assert set(result.deleted_groups()) == {"g1", "g2"}
        _assert_denied(result.all())
        admin.delete_consumer_groups(group_ids=[])
        assert rec.args == ([], -1)


def test_delete_consumer_group_offsets_passes_its_options(
        monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_delete_consumer_group_offsets_async")
    with _admin() as admin:
        result = admin.delete_consumer_group_offsets(
            group_id="g", partitions={TP0}, options=DeleteConsumerGroupOffsetsOptions(timeout_ms=3))
        assert rec.args == ("g", [("t", 0)], 3)
        _assert_denied(result.partition_result(partition=TP0))
        _assert_denied(result.all())
        with pytest.raises(IllegalArgumentError, match="was not included in the original request"):
            result.partition_result(partition=TP1)
        admin.delete_consumer_group_offsets(group_id="g", partitions=[])
        assert rec.args == ("g", [], -1)


def test_list_groups_passes_its_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_list_groups_async")
    with _admin() as admin:
        admin.list_groups()
        # Java's listGroups(): new ListGroupsOptions(), every filter empty.
        assert rec.args == ([], [], [], -1)
        options = ListGroupsOptions(timeout_ms=11, group_states=[GroupState.STABLE],
                                    types=[GroupType.CLASSIC], protocol_types=["consumer"])
        admin.list_groups(options=options)
        # The names KafkaAdminClient.listGroups sends: GroupState / GroupType toString().
        assert rec.args == (["Stable"], ["consumer"], ["Classic"], 11)


def test_remove_members_passes_its_options(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_remove_members_from_consumer_group_async")
    with _admin() as admin:
        member = MemberToRemove(group_instance_id="i1")
        result = admin.remove_members_from_consumer_group(
            group_id="g", options=_remove_options([member], reason="bye", timeout_ms=4))
        assert rec.args == ("g", False, ["i1"], "bye", 4)
        _assert_denied(result.all())
        _assert_denied(result.member_result(member=member))
        # removeAll mode: no members; a null reason crosses as None (the core
        # defaults it, KafkaAdminClient.java:4221).
        result = admin.remove_members_from_consumer_group(
            group_id="g", options=_remove_options(timeout_ms=None))
        assert rec.args == ("g", True, [], None, -1)
        _assert_denied(result.all())
        with pytest.raises(IllegalArgumentError,
                           match="The method: memberResult is not applicable in 'removeAll' mode"):
            result.member_result(member=member)


def test_alter_consumer_group_offsets_passes_its_offsets(
        monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_alter_consumer_group_offsets_async")
    with _admin() as admin:
        result = admin.alter_consumer_group_offsets(
            group_id="g",
            offsets={TP0: OffsetAndMetadata(offset=5, leader_epoch=3, metadata="m"),
                     TP1: OffsetAndMetadata(offset=6)},
            options=AlterConsumerGroupOffsetsOptions(timeout_ms=2))
        # (topic, partition, offset, metadata, has_leader_epoch, leader_epoch)
        assert rec.args == ("g", [("t", 0, 5, "m", True, 3), ("t", 1, 6, "", False, -1)], 2)
        _assert_denied(result.partition_result(partition=TP0))
        _assert_denied(result.all())
        admin.alter_consumer_group_offsets(group_id="g", offsets={})
        assert rec.args == ("g", [], -1)


def test_negative_option_timeout_is_sent_as_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Java's ``calcDeadlineMs`` takes ``Math.max(0, timeoutMs)``: a given negative
    timeout expires at once, never the FFI's "not set" ``-1``."""
    rec = _Recorder(monkeypatch, "Admin_delete_consumer_groups_async")
    with _admin() as admin:
        admin.delete_consumer_groups(group_ids=["g"],
                                     options=DeleteConsumerGroupsOptions(timeout_ms=-5))
        assert rec.args == (["g"], 0)


async def test_async_calls_pass_the_same_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder(monkeypatch, "Admin_list_consumer_group_offsets_async")
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        result = await admin.list_consumer_group_offsets(
            group_specs={"a": ListConsumerGroupOffsetsSpec(topic_partitions=[TP0])},
            options=ListConsumerGroupOffsetsOptions(timeout_ms=8))
        assert rec.args == ([("a", False, [("t", 0)])], 8, False)
        future = result.partitions_to_offset_and_metadata()
        assert isinstance(future, asyncio.Future) and future.done()
        with pytest.raises(GroupAuthorizationError, match="denied"):
            await result.all()


# ---- against an unreachable bootstrap: each call times out ------------------


def test_describe_consumer_groups_times_out() -> None:
    with _admin() as admin:
        result = admin.describe_consumer_groups(
            group_ids=["g1", "g2"], options=DescribeConsumerGroupsOptions(timeout_ms=SHORT))
        for future in result.described_groups().values():
            assert future.done()
            with pytest.raises(KafkaTimeoutError, match=_timed_out("describeConsumerGroups")):
                future.result()
        with pytest.raises(KafkaTimeoutError, match=_timed_out("describeConsumerGroups")):
            result.all().result()
        assert admin.describe_consumer_groups(group_ids=[]).all().result() == {}


def test_list_consumer_group_offsets_times_out() -> None:
    with _admin() as admin:
        result = admin.list_consumer_group_offsets(
            group_id="g", options=ListConsumerGroupOffsetsOptions(timeout_ms=SHORT))
        with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetFetch")):
            result.partitions_to_offset_and_metadata().result()
        result = admin.list_consumer_group_offsets(
            group_specs={"a": ListConsumerGroupOffsetsSpec(topic_partitions=[TP0]),
                         "b": ListConsumerGroupOffsetsSpec()},
            options=ListConsumerGroupOffsetsOptions(timeout_ms=SHORT, require_stable=True))
        for group_id in ("a", "b"):
            with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetFetch")):
                result.partitions_to_offset_and_metadata(group_id=group_id).result()
        with pytest.raises(IllegalArgumentError,
                           match="Offsets for consumer group 'c' were not requested."):
            result.partitions_to_offset_and_metadata(group_id="c")
        assert admin.list_consumer_group_offsets(group_specs={}).all().result() == {}


def test_delete_consumer_groups_times_out() -> None:
    with _admin() as admin:
        result = admin.delete_consumer_groups(
            group_ids=["g"], options=DeleteConsumerGroupsOptions(timeout_ms=SHORT))
        with pytest.raises(KafkaTimeoutError, match=_timed_out("deleteConsumerGroups")):
            result.deleted_groups()["g"].result()
        with pytest.raises(KafkaTimeoutError, match=_timed_out("deleteConsumerGroups")):
            result.all().result()


def test_delete_consumer_group_offsets_times_out() -> None:
    with _admin() as admin:
        options = DeleteConsumerGroupOffsetsOptions(timeout_ms=SHORT)
        result = admin.delete_consumer_group_offsets(group_id="g", partitions=[TP0],
                                                     options=options)
        with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetDelete")):
            result.partition_result(partition=TP0).result()
        with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetDelete")):
            result.all().result()
        # No partition: the call's own future still times out (the core's all()).
        result = admin.delete_consumer_group_offsets(group_id="g", partitions=[],
                                                     options=options)
        with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetDelete")):
            result.all().result()


def test_list_groups_fails_to_find_brokers() -> None:
    """Java's ``listGroups`` completes with the failure as an element:
    ``KafkaException("Failed to find brokers to send ListGroups", cause)``."""
    with _admin() as admin:
        result = admin.list_groups(options=ListGroupsOptions(timeout_ms=SHORT))
        assert result.valid().result() == []
        errors = result.errors().result()
        assert len(errors) == 1
        assert type(errors[0]) is KafkaError
        assert str(errors[0]) == "Failed to find brokers to send ListGroups"
        assert isinstance(errors[0].__cause__, KafkaTimeoutError)
        assert str(errors[0].__cause__) == (
            "Timed out waiting for a node assignment. Call: findAllBrokers")
        with pytest.raises(KafkaError, match="Failed to find brokers to send ListGroups"):
            result.all().result()


def test_remove_members_times_out() -> None:
    with _admin(FAST_DEFAULT) as admin:
        member = MemberToRemove(group_instance_id="i1")
        result = admin.remove_members_from_consumer_group(
            group_id="g", options=_remove_options([member]))
        with pytest.raises(KafkaTimeoutError, match=_timed_out("leaveGroup")):
            result.member_result(member=member).result()
        with pytest.raises(KafkaTimeoutError, match=_timed_out("leaveGroup")):
            result.all().result()
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
        with pytest.raises(KafkaError) as info:
            result.all().result()
        assert type(info.value) is KafkaError
        assert str(info.value) == "Encounter error when trying to get members from group: g"
        assert isinstance(info.value.__cause__, KafkaTimeoutError)
        assert re.fullmatch(_timed_out("describeConsumerGroups"), str(info.value.__cause__))


def test_alter_consumer_group_offsets_times_out() -> None:
    """The driver's timeout fails Java's one future, so ``all()`` reports it as it
    is, not renamed into "Failed altering group offsets ..."."""
    with _admin() as admin:
        result = admin.alter_consumer_group_offsets(
            group_id="g", offsets={TP0: OffsetAndMetadata(offset=5)},
            options=AlterConsumerGroupOffsetsOptions(timeout_ms=SHORT))
        with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetCommit")):
            result.partition_result(partition=TP0).result()
        with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetCommit")):
            result.all().result()


def test_describe_classic_groups_times_out() -> None:
    with _admin() as admin:
        result = admin.describe_classic_groups(
            group_ids=["g"], options=DescribeClassicGroupsOptions(timeout_ms=SHORT))
        with pytest.raises(KafkaTimeoutError, match=_timed_out("describeClassicGroups")):
            result.described_groups()["g"].result()
        assert admin.describe_classic_groups(group_ids=[]).all().result() == {}


async def test_async_calls_time_out() -> None:
    async with AsyncAdmin.create(configs=FAST_DEFAULT) as admin:
        described = await admin.describe_consumer_groups(
            group_ids=["g"], options=DescribeConsumerGroupsOptions(timeout_ms=SHORT))
        future = described.described_groups()["g"]
        assert isinstance(future, asyncio.Future) and future.done()
        with pytest.raises(KafkaTimeoutError, match=_timed_out("describeConsumerGroups")):
            await described.all()
        offsets = await admin.list_consumer_group_offsets(group_id="g")
        with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetFetch")):
            await offsets.all()
        deleted = await admin.delete_consumer_groups(group_ids=["g"])
        with pytest.raises(KafkaTimeoutError, match=_timed_out("deleteConsumerGroups")):
            await deleted.all()
        offsets_deleted = await admin.delete_consumer_group_offsets(group_id="g",
                                                                    partitions=[TP0])
        with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetDelete")):
            await offsets_deleted.partition_result(partition=TP0)
        listed = await admin.list_groups()
        assert [str(e) for e in await listed.errors()] == [
            "Failed to find brokers to send ListGroups"]
        with pytest.raises(KafkaError, match="Failed to find brokers to send ListGroups"):
            await listed.all()
        member = MemberToRemove(group_instance_id="i1")
        removed = await admin.remove_members_from_consumer_group(
            group_id="g", options=_remove_options([member]))
        with pytest.raises(KafkaTimeoutError, match=_timed_out("leaveGroup")):
            await removed.member_result(member=member)
        altered = await admin.alter_consumer_group_offsets(
            group_id="g", offsets={TP0: OffsetAndMetadata(offset=1)})
        with pytest.raises(KafkaTimeoutError, match=_timed_out("offsetCommit")):
            await altered.all()
        classic = await admin.describe_classic_groups(group_ids=["g"])
        with pytest.raises(KafkaTimeoutError, match=_timed_out("describeClassicGroups")):
            await classic.all()


# ---- the extension's plain values into the value classes --------------------


def _borrowed(cls: type[BaseException], message: str) -> tuple[Any, ...]:
    """A per-key error in the borrowed form ``borrowed_error_to_py`` gives."""
    from confluent_kafka.common.errors.api_error import ApiError

    return (getattr(cls, "_ffi_id"), message, 0, 0, None, int(issubclass(cls, ApiError)), None)


_MEMBER = ("consumer-1", "instance-1", None, "client-1", "/10.0.0.1",
           [("t", 0), ("t", 1)], [("t", 1)], 7, None)
_NODE = (2, "broker2", 9092, "rack-a")


def test_consumer_group_description_from_the_drain() -> None:
    description = ("g", False, [_MEMBER], "uniform", "Consumer", "Stable", _NODE,
                   [AclOperation.READ.code(), AclOperation.DESCRIBE.code()], 5, 4)
    error, value = _conversions._consumer_group_description((None, description))
    assert error is None
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
    assert value.coordinator() is not None and value.coordinator().is_fenced() is False
    # authorizedOperations() is null unless requested; an absent coordinator is null.
    _, omitted = _conversions._consumer_group_description(
        (None, ("g", True, [], "", "Classic", "Empty", None, None, None, None)))
    assert omitted.authorized_operations() is None and omitted.coordinator() is None
    assert omitted.type() is GroupType.CLASSIC and omitted.group_state() is GroupState.EMPTY
    error, value = _conversions._consumer_group_description(
        (_borrowed(GroupIdNotFoundError, "Group g not found."), None))
    assert type(error) is GroupIdNotFoundError and value is None


def test_classic_group_description_keeps_a_null_operations_set() -> None:
    """Decision log 41: built through ``_new``, an authorized operations set the
    core reports as ``null`` stays ``None``, where the public constructor's "not
    given" would give ``set()``."""
    member = ("m", None, None, "c", "h", [("t", 0)], None, None, None)
    raw = ("g", "range", "consumer", False, [member], "Stable", _NODE, None)
    error, value = _conversions._classic_group_description((None, raw))
    assert error is None
    assert isinstance(value, ClassicGroupDescription)
    assert value.authorized_operations() is None
    assert value.state() is ClassicGroupState.STABLE
    assert value.protocol() == "range" and value.protocol_data() == "consumer"
    assert value.members()[0].assignment().topic_partitions() == {TP0}
    assert value.members()[0].target_assignment() is None
    _, with_ops = _conversions._classic_group_description(
        (None, ("g", "", "", True, [], "Dead", None, [AclOperation.ALTER.code()])))
    assert with_ops.authorized_operations() == {AclOperation.ALTER}
    assert with_ops.is_simple_consumer_group() and with_ops.state() is ClassicGroupState.DEAD


def test_group_offsets_from_the_drain() -> None:
    error, value = _conversions._group_offsets(
        (None, {("t", 0): (5, "meta", 3), ("t", 1): (6, "", None), ("u", 0): None}))
    assert error is None
    assert value == {TP0: OffsetAndMetadata(offset=5, leader_epoch=3, metadata="meta"),
                     TP1: OffsetAndMetadata(offset=6, leader_epoch=None, metadata=""),
                     TopicPartition(topic="u", partition=0): None}


def test_group_listings_from_the_drain() -> None:
    valid = [("g1", "Consumer", "consumer", "Stable", False), ("g2", None, "", None, False)]
    errors = [_borrowed(GroupAuthorizationError, "denied")]
    results = _conversions._group_listings((valid, errors))
    assert results[:2] == [
        GroupListing(group_id="g1", type=GroupType.CONSUMER, protocol="consumer",
                     group_state=GroupState.STABLE),
        GroupListing(group_id="g2", type=None, protocol="", group_state=None)]
    assert type(results[2]) is GroupAuthorizationError and str(results[2]) == "denied"


def _fake_drain(drained: dict[Any, Any]) -> Callable[[int], dict[Any, Any]]:
    return lambda handle: drained


def test_sub_level_errors_become_the_map_of_javas_one_future() -> None:
    """A key's ``Errors.exception()`` (the code's default message) is an entry of
    Java's ``Map<K, Errors>``; ``None`` is ``Errors.NONE``."""
    drained = {("t", 0): None,
               ("t", 1): _borrowed(UnknownTopicOrPartitionError,
                                   "This server does not host this topic-partition.")}
    future = _conversions._sub_level_errors_future(
        (1, 0), _fake_drain(drained), _conversions.topic_partition_from_tuple,
        _conversions._alter_offset_absent, Sync)
    errors = future.result()
    assert errors[TP0] is None
    assert type(errors[TP1]) is UnknownTopicOrPartitionError
    from confluent_kafka.admin import AlterConsumerGroupOffsetsResult
    result = AlterConsumerGroupOffsetsResult._new(future=future, kind=Sync)
    assert result.partition_result(partition=TP0).result() is None
    with pytest.raises(UnknownTopicOrPartitionError,
                       match=re.escape("Failed altering group offsets for the following "
                                       "partitions: [t-1]")):
        result.all().result()


def test_a_failure_every_key_carries_fails_the_one_future() -> None:
    """An error that is not a code's ``Errors.exception()``, such as the driver's
    timeout (an ``ApiError`` with its own message) or a non-API error, is the
    failure of Java's one future."""
    timeout = _borrowed(KafkaTimeoutError, "Timed out waiting for a node assignment. Call: x")
    future = _conversions._sub_level_errors_future(
        (1, 0), _fake_drain({"i1": timeout, "i2": timeout}),
        _conversions._member_identity_key, _conversions._member_absent, Sync)
    with pytest.raises(KafkaTimeoutError, match="Timed out waiting for a node assignment"):
        future.result()
    # An IllegalArgumentError with any other text than the key's absent-key text.
    absent_t1 = 'Alter offset for partition "t-1" was not attempted'
    future = _conversions._sub_level_errors_future(
        (1, 0), _fake_drain({("t", 0): _borrowed(IllegalArgumentError, absent_t1)}),
        _conversions.topic_partition_from_tuple, _conversions._alter_offset_absent, Sync)
    with pytest.raises(IllegalArgumentError, match=re.escape(absent_t1)):
        future.result()


def test_a_key_absent_from_the_response_is_absent_from_the_map() -> None:
    """When the response leaves out a requested key, the core gives that key alone
    the Result's absent-key ``IllegalArgumentError``. The key is left out of the
    rebuilt map: the Result raises Java's own text for it and its siblings resolve
    (``AlterConsumerGroupOffsetsResult.java:50,71-74``,
    ``DeleteConsumerGroupOffsetsResult.java:85``,
    ``RemoveMembersFromConsumerGroupResult.java:104``)."""
    from confluent_kafka.admin import (
        AlterConsumerGroupOffsetsResult,
        DeleteConsumerGroupOffsetsResult,
        RemoveMembersFromConsumerGroupResult,
    )

    altered_text = 'Alter offset for partition "t-1" was not attempted'
    future = _conversions._sub_level_errors_future(
        (1, 0), _fake_drain({("t", 0): None,
                             ("t", 1): _borrowed(IllegalArgumentError, altered_text)}),
        _conversions.topic_partition_from_tuple, _conversions._alter_offset_absent, Sync)
    assert future.result() == {TP0: None}
    altered = AlterConsumerGroupOffsetsResult._new(future=future, kind=Sync)
    assert altered.partition_result(partition=TP0).result() is None
    # Java's all() reads only the map's values.
    assert altered.all().result() is None
    with pytest.raises(IllegalArgumentError, match=re.escape(altered_text)):
        altered.partition_result(partition=TP1).result()

    deleted_text = ('Offset deletion result for partition "t-1" was not included in the '
                    'response')
    future = _conversions._sub_level_errors_future(
        (1, 0), _fake_drain({("t", 0): None,
                             ("t", 1): _borrowed(IllegalArgumentError, deleted_text)}),
        _conversions.topic_partition_from_tuple, _conversions._offset_deletion_absent, Sync)
    deleted = DeleteConsumerGroupOffsetsResult._new(future=future, partitions=[TP0, TP1],
                                                    kind=Sync)
    assert deleted.partition_result(partition=TP0).result() is None
    with pytest.raises(IllegalArgumentError, match=re.escape(deleted_text)):
        deleted.partition_result(partition=TP1).result()
    with pytest.raises(IllegalArgumentError, match=re.escape(deleted_text)):
        deleted.all().result()

    member, absent = (MemberToRemove(group_instance_id="i1"),
                      MemberToRemove(group_instance_id="i2"))
    removed_text = ('Member "MemberIdentity(memberId=\'\', groupInstanceId=\'i2\', reason=null)" '
                    'was not included in the removal response')
    future = _conversions._sub_level_errors_future(
        (1, 0), _fake_drain({"i1": None, "i2": _borrowed(IllegalArgumentError, removed_text)}),
        _conversions._member_identity_key, _conversions._member_absent, Sync)
    removed = RemoveMembersFromConsumerGroupResult._new(future=future,
                                                       member_infos=[member, absent],
                                                       kind=Sync)
    assert removed.member_result(member=member).result() is None
    with pytest.raises(IllegalArgumentError, match=re.escape(removed_text)):
        removed.member_result(member=absent).result()
    with pytest.raises(IllegalArgumentError, match=re.escape(removed_text)):
        removed.all().result()


def test_member_results_are_keyed_as_java_looks_them_up() -> None:
    """``memberResult`` looks a member up by ``MemberToRemove.toMemberIdentity()``."""
    from confluent_kafka.admin import RemoveMembersFromConsumerGroupResult

    member = MemberToRemove(group_instance_id="i1")
    other = MemberToRemove(group_instance_id="i2")
    future = _conversions._sub_level_errors_future(
        (1, 0), _fake_drain({"i1": None, "i2": _borrowed(
            UnknownMemberIdError, "The coordinator is not aware of this member.")}),
        _conversions._member_identity_key, _conversions._member_absent, Sync)
    result = RemoveMembersFromConsumerGroupResult._new(future=future,
                                                       member_infos=[member, other],
                                                       kind=Sync)
    assert result.member_result(member=member).result() is None
    with pytest.raises(UnknownMemberIdError):
        result.member_result(member=other).result()
    with pytest.raises(UnknownMemberIdError):
        result.all().result()
