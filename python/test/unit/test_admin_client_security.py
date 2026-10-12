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

"""The admin client's ACL, delegation-token, quota, SCRAM, feature, producer and
transaction RPCs (``describe_acls`` … ``force_terminate_transaction``), sync and
async, without a broker.

``KafkaAdminClientTest.java`` (Apache Kafka 4.3.1): the tests of these RPCs that need
no scripted broker are ``testUpdateFeaturesShouldFailRequestForEmptyUpdates``,
``testUpdateFeaturesShouldFailRequestForInvalidFeatureName`` and
``testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion``,
ported in ``test_admin_java_ports.py``, and ``testEqualsOfClientQuotaFilterComponent``,
ported in ``test_admin_common_types.py``; the updateFeatures pair is repeated here for
the async peer. Every other test of these RPCs (``testDescribeAcls``,
``testCreateAcls(ToController)``, ``testDeleteAcls(ToController)``,
``testDescribeClientQuotas``, ``testAlterClientQuotas``,
``testDescribeUserScramCredentials``, ``testAlterUserScramCredentials(UnknownMechanism)``,
``testUpdateFeatures*`` with responses, ``testDescribeFeatures*``,
``testDescribeProducers*``, ``testDescribeTransactions``,
``testRetryDescribeTransactionsAfterNotCoordinatorError``, ``testAbortTransaction*``,
``testForceTerminateTransaction*``, ``testListTransactions``, ``testFenceProducers``)
scripts broker responses with a ``MockClient``, which the binding cannot inject; the
core's translations are in ``rust/src/admin/kafka_admin_client.rs`` under the same
names in snake_case (``test_describe_acls``, …, ``test_fence_producers``;
``test_update_features_handle_not_controller_error`` for
``testUpdateFeaturesHandleNotControllerException``).

Here, per RPC: keyword-only calls, the ``java_forms`` message of
``describe_user_scram_credentials``, the options and arguments as they are handed to
the extension, the call failing with Java's timeout message against an unreachable
bootstrap, the closed check, and the results' value classes built from the drains'
shapes (the success branch needs a broker).
"""

from __future__ import annotations

import asyncio
import os
import signal
import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Any

import pytest

import _confluentkafka as _lib  # type: ignore[import-not-found]
from confluent_kafka import IllegalArgumentError, IllegalStateError
from confluent_kafka.admin import (
    AbortTransactionOptions,
    AbortTransactionSpec,
    Admin,
    AlterClientQuotasOptions,
    AlterUserScramCredentialsOptions,
    AsyncAdmin,
    CreateAclsOptions,
    CreateDelegationTokenOptions,
    DeleteAclsOptions,
    DeleteAclsResult,
    DescribeAclsOptions,
    DescribeClientQuotasOptions,
    DescribeDelegationTokenOptions,
    DescribeFeaturesOptions,
    DescribeProducersOptions,
    DescribeProducersResult,
    DescribeTransactionsOptions,
    DescribeUserScramCredentialsOptions,
    ExpireDelegationTokenOptions,
    FeatureUpdate,
    FenceProducersOptions,
    ListTransactionsOptions,
    ProducerState,
    RenewDelegationTokenOptions,
    ScramCredentialInfo,
    ScramMechanism,
    TerminateTransactionOptions,
    TransactionDescription,
    TransactionListing,
    TransactionState,
    UpdateFeaturesOptions,
    UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
)
from confluent_kafka.admin import _conversions
from confluent_kafka.admin._futures import Async, Sync
from confluent_kafka.admin._internals import _CoordinatorKey, _ProducerIdAndEpoch
from confluent_kafka.common import (
    AccessControlEntry,
    AccessControlEntryFilter,
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    ClientQuotaAlteration,
    ClientQuotaEntity,
    ClientQuotaFilter,
    ClientQuotaFilterComponent,
    KafkaPrincipal,
    PatternType,
    ResourcePattern,
    ResourcePatternFilter,
    ResourceType,
    TopicPartition,
)
from confluent_kafka.common.errors import (
    ClusterAuthorizationError,
    InvalidRequestError,
    ResourceNotFoundError,
    UnknownServerError,
)
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CLOSED = "Cannot accept new calls when AdminClient is closing."
SHORT = 300


def _timed_out(call: str) -> str:
    return f"Timed out waiting for a node assignment. Call: {call}"


def _binding(name: str = "t") -> AclBinding:
    return AclBinding(
        pattern=ResourcePattern(resource_type=ResourceType.TOPIC, name=name,
                                pattern_type=PatternType.LITERAL),
        entry=AccessControlEntry(principal="User:a", host="*", operation=AclOperation.READ,
                                 permission_type=AclPermissionType.ALLOW))


TP = TopicPartition(topic="t", partition=0)
SPEC = AbortTransactionSpec(topic_partition=TP, producer_id=7, producer_epoch=1,
                            coordinator_epoch=2)
ENTITY = ClientQuotaEntity(entries={"user": "u", "client-id": None})
UPGRADE = FeatureUpdate.UpgradeType.UPGRADE


def _raw_error(cls: type[BaseException], message: str) -> tuple[Any, ...]:
    """A per-key error in the drains' borrowed form (``borrowed_error_to_py``)."""
    return (getattr(cls, "_ffi_id"), message, 0, 0, None, 1, None)


# ---- each RPC: (name, sync call, its futures, Java's call name) ---------------

Call = Callable[[Any], Any]


def _rpcs() -> list[tuple[str, Call, Callable[[Any], list[Any]], str]]:
    """Every RPC of the group with short-timeout options, the futures of its result
    and the name Java's ``Call`` (or its ``AdminApiDriver`` request) carries."""
    return [
        ("describe_acls",
         lambda a: a.describe_acls(filter=AclBindingFilter.ANY,
                                   options=DescribeAclsOptions(timeout_ms=SHORT)),
         lambda r: [r.values()], "describeAcls"),
        ("create_acls",
         lambda a: a.create_acls(acls=[_binding()], options=CreateAclsOptions(timeout_ms=SHORT)),
         lambda r: [*r.values().values(), r.all()], "createAcls"),
        ("delete_acls",
         lambda a: a.delete_acls(filters=[AclBindingFilter.ANY],
                                 options=DeleteAclsOptions(timeout_ms=SHORT)),
         lambda r: [*r.values().values(), r.all()], "deleteAcls"),
        ("create_delegation_token",
         lambda a: a.create_delegation_token(
             options=CreateDelegationTokenOptions(timeout_ms=SHORT)),
         lambda r: [r.delegation_token()], "createDelegationToken"),
        ("renew_delegation_token",
         lambda a: a.renew_delegation_token(
             hmac=b"\x00mac", options=RenewDelegationTokenOptions(timeout_ms=SHORT)),
         lambda r: [r.expiry_timestamp()], "renewDelegationToken"),
        ("expire_delegation_token",
         lambda a: a.expire_delegation_token(
             hmac=b"mac", options=ExpireDelegationTokenOptions(timeout_ms=SHORT)),
         lambda r: [r.expiry_timestamp()], "expireDelegationToken"),
        ("describe_delegation_token",
         lambda a: a.describe_delegation_token(
             options=DescribeDelegationTokenOptions(timeout_ms=SHORT)),
         lambda r: [r.delegation_tokens()], "describeDelegationToken"),
        ("describe_client_quotas",
         lambda a: a.describe_client_quotas(filter=ClientQuotaFilter.all(),
                                            options=DescribeClientQuotasOptions(
                                                timeout_ms=SHORT)),
         lambda r: [r.entities()], "describeClientQuotas"),
        ("alter_client_quotas",
         lambda a: a.alter_client_quotas(
             entries=[ClientQuotaAlteration(entity=ENTITY, ops=[
                 ClientQuotaAlteration.Op(key="producer_byte_rate", value=1024.0)])],
             options=AlterClientQuotasOptions(timeout_ms=SHORT)),
         lambda r: [*r.values().values(), r.all()], "alterClientQuotas"),
        ("describe_user_scram_credentials",
         lambda a: a.describe_user_scram_credentials(
             users=["u"], options=DescribeUserScramCredentialsOptions(timeout_ms=SHORT)),
         lambda r: [r.all(), r.users(), r.description(user_name="u")],
         "describeUserScramCredentials"),
        ("alter_user_scram_credentials",
         lambda a: a.alter_user_scram_credentials(
             alterations=[UserScramCredentialDeletion(user="u",
                                                      mechanism=ScramMechanism.SCRAM_SHA_256)],
             options=AlterUserScramCredentialsOptions(timeout_ms=SHORT)),
         lambda r: [*r.values().values(), r.all()], "alterUserScramCredentials"),
        ("describe_features",
         lambda a: a.describe_features(options=DescribeFeaturesOptions(timeout_ms=SHORT)),
         lambda r: [r.feature_metadata()], "describeFeatures"),
        ("update_features",
         lambda a: a.update_features(
             feature_updates={"f": FeatureUpdate(max_version_level=1, upgrade_type=UPGRADE)},
             options=UpdateFeaturesOptions(timeout_ms=SHORT)),
         lambda r: [*r.values().values(), r.all()], "updateFeatures"),
        ("describe_producers",
         lambda a: a.describe_producers(partitions=[TP],
                                        options=DescribeProducersOptions(timeout_ms=SHORT)),
         lambda r: [r.partition_result(partition=TP), r.all()],
         "describeProducers(api=METADATA)"),
        ("describe_transactions",
         lambda a: a.describe_transactions(transactional_ids=["x"],
                                           options=DescribeTransactionsOptions(
                                               timeout_ms=SHORT)),
         lambda r: [r.description(transactional_id="x"), r.all()],
         "describeTransactions(api=FIND_COORDINATOR)"),
        ("abort_transaction",
         lambda a: a.abort_transaction(spec=SPEC,
                                       options=AbortTransactionOptions(timeout_ms=SHORT)),
         lambda r: [r.all()], "abortTransaction(api=METADATA)"),
        ("list_transactions",
         lambda a: a.list_transactions(options=ListTransactionsOptions(timeout_ms=SHORT)),
         lambda r: [r.by_broker_id(), r.all(), r.all_by_broker_id()],
         "listTransactions(api=METADATA)"),
        ("fence_producers",
         lambda a: a.fence_producers(transactional_ids=["x"],
                                     options=FenceProducersOptions(timeout_ms=SHORT)),
         lambda r: [*r.fenced_producers().values(), r.producer_id(transactional_id="x"),
                    r.epoch_id(transactional_id="x"), r.all()],
         "fenceProducer(api=FIND_COORDINATOR)"),
        # Java's forceTerminateTransaction is fenceProducers of the one id
        # (KafkaAdminClient.java:4862-4877), so the call is FenceProducersHandler's.
        ("force_terminate_transaction",
         lambda a: a.force_terminate_transaction(
             transactional_id="x", options=TerminateTransactionOptions(timeout_ms=SHORT)),
         lambda r: [r.result()], "fenceProducer(api=FIND_COORDINATOR)"),
    ]


RPCS = _rpcs()
IDS = [name for name, *_ in RPCS]


@pytest.fixture(scope="module")
def admin() -> Any:
    client = Admin.create(configs=UNREACHABLE)
    yield client
    client.close(timeout=0)


@pytest.mark.parametrize(("name", "call", "futures", "java_call"), RPCS, ids=IDS)
def test_rpc_futures_fail_with_the_call_timeout(admin: Admin, name: str, call: Call,
                                                futures: Callable[[Any], list[Any]],
                                                java_call: str) -> None:
    """Every future of the result is complete when the call returns, failed with
    Java's message for a call that found no node (``Call`` / ``AdminApiDriver``)."""
    for future in futures(call(admin)):
        assert future.done()
        with pytest.raises(KafkaTimeoutError) as info:
            future.result()
        assert str(info.value) == _timed_out(java_call)


@pytest.mark.parametrize(("name", "call", "futures", "java_call"), RPCS, ids=IDS)
async def test_async_rpc_futures_fail_with_the_call_timeout(
        name: str, call: Call, futures: Callable[[Any], list[Any]], java_call: str) -> None:
    """The async peer: the same calls awaited; the futures are ``asyncio.Future``\\ s
    of this loop, every one complete."""
    async_admin = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        result = await call(async_admin)
        for future in futures(result):
            if not isinstance(future, asyncio.Future):
                # by_broker_id()'s map holds the per-broker futures; the top one failed.
                raise AssertionError(f"{name}: not an asyncio.Future: {future!r}")
            assert future.done()
            with pytest.raises(KafkaTimeoutError) as info:
                await future
            assert str(info.value) == _timed_out(java_call)
    finally:
        await async_admin.close(timeout=0)


@pytest.mark.parametrize(("name", "call", "futures", "java_call"), RPCS, ids=IDS)
def test_rpc_after_close_raises(name: str, call: Call, futures: Any, java_call: str) -> None:
    """Java fails the call's future with ``IllegalStateException`` on a closing
    client; here the call raises it."""
    client = Admin.create(configs=UNREACHABLE)
    client.close()
    with pytest.raises(IllegalStateError) as info:
        call(client)
    assert str(info.value) == CLOSED


@pytest.mark.parametrize(("name", "call", "futures", "java_call"), RPCS, ids=IDS)
async def test_async_rpc_after_close_raises(name: str, call: Call, futures: Any,
                                            java_call: str) -> None:
    client = AsyncAdmin.create(configs=UNREACHABLE)
    await client.close()
    with pytest.raises(IllegalStateError) as info:
        await call(client)
    assert str(info.value) == CLOSED


POSITIONAL: list[tuple[str, tuple[Any, ...]]] = [
    ("describe_acls", (AclBindingFilter.ANY,)),
    ("create_acls", ([_binding()],)),
    ("delete_acls", ([AclBindingFilter.ANY],)),
    ("create_delegation_token", (CreateDelegationTokenOptions(),)),
    ("renew_delegation_token", (b"mac",)),
    ("expire_delegation_token", (b"mac",)),
    ("describe_delegation_token", (DescribeDelegationTokenOptions(),)),
    ("describe_client_quotas", (ClientQuotaFilter.all(),)),
    ("alter_client_quotas", ([],)),
    ("describe_user_scram_credentials", (["u"],)),
    ("alter_user_scram_credentials", ([],)),
    ("describe_features", (DescribeFeaturesOptions(),)),
    ("update_features", ({},)),
    ("describe_producers", ([TP],)),
    ("describe_transactions", (["x"],)),
    ("abort_transaction", (SPEC,)),
    ("list_transactions", (ListTransactionsOptions(),)),
    ("fence_producers", (["x"],)),
    ("force_terminate_transaction", ("x",)),
]


@pytest.mark.parametrize(("name", "args"), POSITIONAL, ids=[n for n, _ in POSITIONAL])
def test_positional_calls_raise_type_error(admin: Admin, name: str,
                                           args: tuple[Any, ...]) -> None:
    with pytest.raises(TypeError):
        getattr(admin, name)(*args)


@pytest.mark.parametrize(("name", "args"), POSITIONAL, ids=[n for n, _ in POSITIONAL])
async def test_async_positional_calls_raise_type_error(name: str,
                                                       args: tuple[Any, ...]) -> None:
    client = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        with pytest.raises(TypeError):
            await getattr(client, name)(*args)
    finally:
        await client.close(timeout=0)


# ---- describe_user_scram_credentials: Java's three overloads -----------------

SCRAM_FORMS = ("describe_user_scram_credentials() takes one of (), (users), "
               "(users, options); got (options)")
SCRAM_TIMED_OUT = _timed_out("describeUserScramCredentials")


def test_describe_user_scram_credentials_forms() -> None:
    """``describeUserScramCredentials()``, ``(users)`` and ``(users, options)``; Java
    has no ``(options)``, and an explicit ``users=None`` is Java's ``null``, which
    selects ``(users)`` or ``(users, options)``."""
    with Admin.create(configs={**UNREACHABLE, "default.api.timeout.ms": str(SHORT),
                               "request.timeout.ms": "100"}) as client:
        options = DescribeUserScramCredentialsOptions(timeout_ms=SHORT)
        with pytest.raises(IllegalArgumentError) as info:
            client.describe_user_scram_credentials(options=options)
        assert str(info.value) == SCRAM_FORMS
        for result in (client.describe_user_scram_credentials(),
                       client.describe_user_scram_credentials(users=None),
                       client.describe_user_scram_credentials(users=["u"]),
                       client.describe_user_scram_credentials(users=None, options=options),
                       client.describe_user_scram_credentials(users=[], options=options)):
            with pytest.raises(KafkaTimeoutError) as timed_out:
                result.users().result()
            assert str(timed_out.value) == SCRAM_TIMED_OUT


async def test_async_describe_user_scram_credentials_forms() -> None:
    client = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        options = DescribeUserScramCredentialsOptions(timeout_ms=SHORT)
        with pytest.raises(IllegalArgumentError) as info:
            await client.describe_user_scram_credentials(options=options)
        assert str(info.value) == SCRAM_FORMS
        result = await client.describe_user_scram_credentials(users=None, options=options)
        with pytest.raises(KafkaTimeoutError):
            await result.all()
    finally:
        await client.close(timeout=0)


def test_describe_user_scram_credentials_null_users_is_every_user() -> None:
    """Java's ``null`` (``describeUserScramCredentials()``) and an empty list both
    describe every user: the extension gets an empty list."""
    options = DescribeUserScramCredentialsOptions(timeout_ms=5)
    assert _conversions.describe_user_scram_credentials_args(None, options) == ([], 5)
    assert _conversions.describe_user_scram_credentials_args(("a", "b"), options) == (
        ["a", "b"], 5)


# ---- update_features: the core's argument check is raised, as Java throws ----


async def test_async_update_features_should_fail_request_for_empty_updates() -> None:
    """``testUpdateFeaturesShouldFailRequestForEmptyUpdates`` on the async peer."""
    client = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        with pytest.raises(IllegalArgumentError) as info:
            await client.update_features(feature_updates={})
        assert str(info.value) == "Feature updates can not be null or empty."
        with pytest.raises(IllegalArgumentError) as info:
            await client.update_features(feature_updates={
                "feature": FeatureUpdate(max_version_level=2, upgrade_type=UPGRADE),
                "": FeatureUpdate(max_version_level=2, upgrade_type=UPGRADE)})
        assert str(info.value) == "Provided feature can not be empty."
    finally:
        await client.close(timeout=0)


@pytest.mark.parametrize("blank", [" ", "\t\n"])
def test_update_features_blank_feature_name(admin: Admin, blank: str) -> None:
    """Java's ``Utils.isBlank`` (``trim().isEmpty()``) rejects a blank name before
    the call (``KafkaAdminClient.java:4597``)."""
    with pytest.raises(IllegalArgumentError) as info:
        admin.update_features(feature_updates={
            blank: FeatureUpdate(max_version_level=2, upgrade_type=UPGRADE)})
    assert str(info.value) == "Provided feature can not be empty."


def test_describe_acls_unknown_filter_fails_the_future(admin: Admin) -> None:
    """Java's ``describeAcls`` fails the future, without a call, for a filter with
    UNKNOWN elements (``KafkaAdminClient.java:2571-2575``); the core does too."""
    unknown = AclBindingFilter(
        pattern_filter=ResourcePatternFilter(resource_type=ResourceType.UNKNOWN, name=None,
                                             pattern_type=PatternType.ANY),
        entry_filter=AccessControlEntryFilter.ANY)
    future = admin.describe_acls(filter=unknown).values()
    with pytest.raises(InvalidRequestError) as info:
        future.result()
    assert str(info.value) == "The AclBindingFilter must not contain UNKNOWN elements."


def test_alter_user_scram_credentials_call_failure_fails_every_user(admin: Admin) -> None:
    """``testAlterUserScramCredentialsUnknownMechanism`` without its scripted response:
    Java fails the users with an illegal alteration only in ``handleResponse``, so a
    call that fails (``handleFailure``: ``completeAllExceptionally``) fails every user,
    the UNKNOWN-mechanism ones included, with the call's error."""
    result = admin.alter_user_scram_credentials(
        alterations=[
            UserScramCredentialDeletion(user="user0", mechanism=ScramMechanism.UNKNOWN),
            UserScramCredentialUpsertion(
                user="user1", credential_info=ScramCredentialInfo(
                    mechanism=ScramMechanism.UNKNOWN, iterations=8192), password="password"),
            UserScramCredentialUpsertion(
                user="user2", credential_info=ScramCredentialInfo(
                    mechanism=ScramMechanism.SCRAM_SHA_256, iterations=4096),
                password="password")],
        options=AlterUserScramCredentialsOptions(timeout_ms=SHORT))
    values = result.values()
    assert set(values) == {"user0", "user1", "user2"}
    for future in [*values.values(), result.all()]:
        with pytest.raises(KafkaTimeoutError) as info:
            future.result()
        assert str(info.value) == _timed_out("alterUserScramCredentials")


# ---- the arguments handed to the extension -----------------------------------


def test_acl_arguments() -> None:
    """``AclBinding`` / ``AclBindingFilter`` as the seven-tuple, enums as codes and a
    filter's ``null`` strings ``None``; ``describe_acls`` takes it flat."""
    binding = _binding("orders")
    assert _conversions.create_acls_args([binding], CreateAclsOptions(timeout_ms=9)) == (
        [(2, "orders", 3, "User:a", "*", 3, 3)], 9)
    any_filter = AclBindingFilter.ANY
    assert _conversions.delete_acls_args([any_filter], DeleteAclsOptions()) == (
        [(1, None, 1, None, None, 1, 1)], -1)
    assert _conversions.describe_acls_args(binding.to_filter(), DescribeAclsOptions()) == (
        2, "orders", 3, "User:a", "*", 3, 3, -1)


def test_delegation_token_arguments() -> None:
    """Every option field is sent with its Java starting value: no renewers,
    no owner (two ``None``\\ s), ``-1`` lifetimes and periods; ``owners() != null`` is
    the describe flag."""
    renewer = KafkaPrincipal(principal_type="User", name="r")
    owner = KafkaPrincipal(principal_type="User", name="o")
    assert _conversions.create_delegation_token_args(CreateDelegationTokenOptions()) == (
        [], None, None, -1, -1)
    assert _conversions.create_delegation_token_args(CreateDelegationTokenOptions(
        timeout_ms=3, max_lifetime_ms=60000, renewers=[renewer], owner=owner)) == (
        [("User", "r")], "User", "o", 60000, 3)
    assert _conversions.renew_delegation_token_args(
        b"h", RenewDelegationTokenOptions()) == (b"h", -1, -1)
    assert _conversions.renew_delegation_token_args(
        b"h", RenewDelegationTokenOptions(renew_time_period_ms=10)) == (b"h", 10, -1)
    assert _conversions.expire_delegation_token_args(
        b"h", ExpireDelegationTokenOptions(expiry_time_period_ms=0)) == (b"h", 0, -1)
    assert _conversions.describe_delegation_token_args(
        DescribeDelegationTokenOptions()) == (False, None, -1)
    assert _conversions.describe_delegation_token_args(
        DescribeDelegationTokenOptions(owners=[owner])) == (True, [("User", "o")], -1)
    assert _conversions.describe_delegation_token_args(
        DescribeDelegationTokenOptions(owners=[])) == (True, [], -1)


def test_client_quota_arguments() -> None:
    """A component's match type as ``DescribeClientQuotasRequest`` picks it: exact for
    ``of_entity``, default for ``of_default_entity`` (an empty ``Optional``), specified
    for ``of_entity_type`` (a ``null`` match); an alteration's default entity name and
    a removing op as ``None``."""
    quota_filter = ClientQuotaFilter.contains_only(components=[
        ClientQuotaFilterComponent.of_entity(entity_type="user", entity_name="u"),
        ClientQuotaFilterComponent.of_default_entity(entity_type="client-id"),
        ClientQuotaFilterComponent.of_entity_type(entity_type="ip")])
    assert _conversions.describe_client_quotas_args(
        quota_filter, DescribeClientQuotasOptions(timeout_ms=1)) == (
        [("user", 0, "u"), ("client-id", 1, None), ("ip", 2, None)], True, 1)
    alteration = ClientQuotaAlteration(entity=ENTITY, ops=[
        ClientQuotaAlteration.Op(key="a", value=2.5), ClientQuotaAlteration.Op(key="b",
                                                                               value=None)])
    rows, timeout, validate_only = _conversions.alter_client_quotas_args(
        [alteration], AlterClientQuotasOptions(validate_only=True))
    assert (timeout, validate_only) == (-1, True)
    [(pairs, ops)] = rows
    assert sorted(pairs, key=str) == sorted([("user", "u"), ("client-id", None)], key=str)
    assert ops == [("a", 2.5), ("b", None)]
    assert _conversions.alter_client_quotas_args([], AlterClientQuotasOptions()) == (
        [], -1, False)


def test_scram_alteration_rows() -> None:
    """A deletion as ``is_deletion`` with no iterations, password or salt; an
    upsertion with its mechanism's type, iterations, password and salt (its
    constructor makes a random salt when none is given)."""
    deletion = UserScramCredentialDeletion(user="d", mechanism=ScramMechanism.SCRAM_SHA_512)
    upsertion = UserScramCredentialUpsertion(
        user="u", credential_info=ScramCredentialInfo(mechanism=ScramMechanism.SCRAM_SHA_256,
                                                      iterations=4096),
        password=b"pw", salt=b"")
    random_salt = UserScramCredentialUpsertion(
        user="r", credential_info=ScramCredentialInfo(mechanism=ScramMechanism.SCRAM_SHA_256,
                                                      iterations=8192), password="pw")
    rows, timeout = _conversions.alter_user_scram_credentials_args(
        [deletion, upsertion, random_salt], AlterUserScramCredentialsOptions(timeout_ms=4))
    assert timeout == 4
    assert rows[0] == ("d", True, 2, 0, None, None)
    assert rows[1] == ("u", False, 1, 4096, b"pw", b"")
    assert rows[2][:5] == ("r", False, 1, 8192, b"pw")
    assert rows[2][5] == random_salt.salt() and len(rows[2][5]) > 0


def test_feature_arguments() -> None:
    """``OptionalInt nodeId()`` as a flag and an ``int``; each update as ``(feature,
    max_version_level, upgrade_type code)``; ``validate_only`` starts ``False``."""
    assert _conversions.describe_features_args(DescribeFeaturesOptions()) == (False, 0, -1)
    assert _conversions.describe_features_args(DescribeFeaturesOptions(node_id=3)) == (
        True, 3, -1)
    downgrade = FeatureUpdate(max_version_level=0,
                              upgrade_type=FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)
    assert _conversions.update_features_args(
        {"f": FeatureUpdate(max_version_level=2, upgrade_type=UPGRADE), "g": downgrade},
        UpdateFeaturesOptions()) == ([("f", 2, 1), ("g", 0, 2)], -1, False)
    assert _conversions.update_features_args({}, UpdateFeaturesOptions(
        validate_only=True, timeout_ms=7)) == ([], 7, True)


def test_producer_and_transaction_arguments() -> None:
    """Partitions as ``(topic, partition)``, ``brokerId()`` as a flag and an
    ``int``; the spec's fields flat; the listing filters as names, ids, the
    duration and a ``None`` pattern for Java's ``null``."""
    assert _conversions.describe_producers_args([TP], DescribeProducersOptions()) == (
        [("t", 0)], False, 0, -1)
    assert _conversions.describe_producers_args([TP], DescribeProducersOptions(
        broker_id=5)) == ([("t", 0)], True, 5, -1)
    assert _conversions.describe_transactions_args(
        ("a", "b"), DescribeTransactionsOptions()) == (["a", "b"], -1)
    assert _conversions.abort_transaction_args(SPEC, AbortTransactionOptions()) == (
        "t", 0, 7, 1, 2, -1)
    assert _conversions.list_transactions_args(ListTransactionsOptions()) == (
        [], [], -1, None, -1)
    options = (ListTransactionsOptions(timeout_ms=1)
               .filter_states(states=[TransactionState.ONGOING])
               .filter_producer_ids(producer_id_filters=[9])
               .filter_on_duration(duration_ms=100)
               .filter_on_transactional_id_pattern(pattern="tx-.*"))
    assert _conversions.list_transactions_args(options) == (
        ["Ongoing"], [9], 100, "tx-.*", 1)
    assert _conversions.fence_producers_args(["x"], FenceProducersOptions()) == (["x"], -1)
    assert _conversions.force_terminate_transaction_args(
        "x", TerminateTransactionOptions(timeout_ms=2)) == ("x", 2)


def test_negative_timeout_is_sent_as_zero() -> None:
    """Java's ``calcDeadlineMs`` takes ``Math.max(0, timeoutMs)`` (decision log 56)."""
    assert _conversions.fence_producers_args(
        ["x"], FenceProducersOptions(timeout_ms=-5)) == (["x"], 0)


# ---- the result values, from the drains' shapes -------------------------------


def test_acl_values_from_the_drains() -> None:
    raw = (2, "orders", 3, "User:a", "*", 3, 3)
    assert _conversions._to_acl_binding(raw) == _binding("orders")
    assert _conversions._to_acl_binding_filter(
        (1, None, 1, None, None, 1, 1)) == AclBindingFilter.ANY
    error, results = _conversions._filter_results(
        (None, [(None, raw), (_raw_error(ClusterAuthorizationError, "denied"), None)]))
    assert error is None
    assert isinstance(results, DeleteAclsResult.FilterResults)
    first, second = results.values()
    assert first.binding() == _binding("orders") and first.exception() is None
    assert second.binding() is None
    assert isinstance(second.exception(), ClusterAuthorizationError)
    assert str(second.exception()) == "denied"
    # A matched ACL's error that is no ApiError fails the filter (decision log 53).
    error, results = _conversions._filter_results(
        (None, [((-1, "core", 0, 0, None, 0, None), None)]))
    assert results is None and str(error) == "core"


def test_delete_acls_drain_rows_are_error_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """A matched ACL's row is the C drain's ``error_value_pair(e, b)``: error first,
    binding second, like every other drained pair. Read the other way round, the
    seven-field binding was taken for the seven-field error tuple and its
    ``permission_type`` int for the cause, so every successful ``delete_acls``
    raised ``TypeError: cannot unpack non-iterable int object``. No broker-free call
    reaches a non-empty ``DeleteAclsResult``, so the order is pinned to the drain's
    source and the result path is driven over a drain of that shape."""
    source = (Path(__file__).resolve().parents[2] / "_confluentkafka.c").read_text()
    drain = source[source.index("static PyObject* py_DeleteAclsResult_drain("):]
    drain = drain[:drain.index("\n}\n")]
    assert "PyObject* row = error_value_pair(e, b);" in drain
    assert "PyObject* value = error_value_pair(err, results);" in drain

    raw_filter = (1, None, 1, None, None, 1, 1)
    raw_binding = (2, "orders", 3, "User:a", "*", 3, 3)
    denied = _raw_error(ClusterAuthorizationError, "denied")
    drained = {raw_filter: (None, [(None, raw_binding), (denied, None)])}
    monkeypatch.setattr(_lib, "DeleteAclsResult_drain", lambda handle: drained)
    result = _conversions.delete_acls_result([AclBindingFilter.ANY], (1, 0), Sync)
    first, second = result.values()[AclBindingFilter.ANY].result().values()
    assert first.binding() == _binding("orders") and first.exception() is None
    assert second.binding() is None
    assert isinstance(second.exception(), ClusterAuthorizationError)
    # Java's all() rethrows a matched ACL's own error.
    with pytest.raises(ClusterAuthorizationError, match="^denied$"):
        result.all().result()


def test_delegation_token_from_the_drain() -> None:
    owner = ("User", "o", False)
    raw = ("id1", owner, ("User", "req", True), [("User", "r", False)], 10, 20, 30,
           b"\x00\x01", "AAE=")
    token = _conversions._to_delegation_token(raw)
    info = token.token_info()
    assert info.token_id() == "id1"
    assert info.owner() == KafkaPrincipal(principal_type="User", name="o")
    assert info.token_requester().token_authenticated() is True
    assert info.renewers() == [KafkaPrincipal(principal_type="User", name="r")]
    assert (info.issue_timestamp(), info.expiry_timestamp(), info.max_timestamp()) == (
        10, 20, 30)
    assert token.hmac() == b"\x00\x01"
    assert token.hmac_as_base64_string() == "AAE="


def test_scram_descriptions_from_the_drain() -> None:
    """The drain's per-user rows become the response data the result reads: a user
    error as its code and message, which Java's ``Errors.forCode(code)
    .exception(message)`` turns back into the class. The two drains are the FFI's
    two shapes (``compose_scram_description_outcomes``): every row carries the
    user's own ``description(user)`` outcome, a RESOURCE_NOT_FOUND user its error."""
    not_found = _raw_error(ResourceNotFoundError, "no nobody")
    # all() succeeded: the described users in response order, then the not-found one.
    result = _new_scram_result(_conversions._scram_response_data({
        "a": (None, [(1, 4096), (2, 8192)]),
        "nobody": (not_found, [])}))
    # Java's all() keeps a RESOURCE_NOT_FOUND user, with no credentials;
    # users() leaves it out; description() raises it with the broker's message.
    described = result.all().result()
    assert list(described) == ["a", "nobody"]
    assert described["nobody"].credential_infos() == []
    assert result.users().result() == ["a"]
    with pytest.raises(ResourceNotFoundError) as info:
        result.description(user_name="nobody").result()
    assert str(info.value) == "no nobody"
    assert result.description(user_name="a").result().credential_infos() == [
        ScramCredentialInfo(mechanism=ScramMechanism.SCRAM_SHA_256, iterations=4096),
        ScramCredentialInfo(mechanism=ScramMechanism.SCRAM_SHA_512, iterations=8192)]
    # all() failed: the listed users, then the requested not-found one.
    failed = _new_scram_result(_conversions._scram_response_data({
        "a": (None, [(1, 4096)]),
        "c": (_raw_error(ClusterAuthorizationError, "denied c"), []),
        "nobody": (not_found, [])}))
    with pytest.raises(ClusterAuthorizationError) as denied:
        failed.all().result()
    assert str(denied.value) == "denied c"
    assert failed.users().result() == ["a", "c"]
    with pytest.raises(ResourceNotFoundError) as info:
        failed.description(user_name="nobody").result()
    assert str(info.value) == "no nobody"


def _new_scram_result(data: Any) -> Any:
    from confluent_kafka.admin import DescribeUserScramCredentialsResult

    return DescribeUserScramCredentialsResult._new(
        data_future=_conversions.future_of(Sync, data), kind=Sync)


def test_feature_metadata_from_the_drain() -> None:
    metadata = _conversions._to_feature_metadata(
        ([("metadata.version", 1, 7)], 42, [("metadata.version", 1, 21)]))
    assert metadata.finalized_features_epoch() == 42
    finalized = metadata.finalized_features()["metadata.version"]
    assert (finalized.min_version_level(), finalized.max_version_level()) == (1, 7)
    supported = metadata.supported_features()["metadata.version"]
    assert (supported.min_version(), supported.max_version()) == (1, 21)
    assert _conversions._to_feature_metadata(([], None, [])).finalized_features_epoch() is None


def test_producer_and_transaction_values_from_the_drains() -> None:
    error, state = _conversions._partition_producer_state(
        (None, [(5, 1, 9, 100, None, 7), (6, 2, 3, 200, 4, None)]))
    assert error is None
    assert isinstance(state, DescribeProducersResult.PartitionProducerState)
    assert state.active_producers() == [
        ProducerState(producer_id=5, producer_epoch=1, last_sequence=9, last_timestamp=100,
                      coordinator_epoch=None, current_transaction_start_offset=7),
        ProducerState(producer_id=6, producer_epoch=2, last_sequence=3, last_timestamp=200,
                      coordinator_epoch=4, current_transaction_start_offset=None)]
    error, description = _conversions._transaction_description(
        (None, (1, "Ongoing", 5, 0, 60000, None, [("t", 0)])))
    assert error is None
    assert description == TransactionDescription(
        coordinator_id=1, state=TransactionState.ONGOING, producer_id=5, producer_epoch=0,
        transaction_timeout_ms=60000, transaction_start_time_ms=None, topic_partitions=[TP])
    error, _ = _conversions._transaction_description(
        (_raw_error(UnknownServerError, "x"), None))
    assert isinstance(error, UnknownServerError)
    error, pid = _conversions._producer_id_and_epoch((None, (11, 3)))
    assert error is None and pid == _ProducerIdAndEpoch(producer_id=11, epoch=3)
    assert _conversions._transactional_id_key("x") == _CoordinatorKey.by_transactional_id(
        transactional_id="x")


def test_listings_by_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    """``list_transactions``' value: one future per broker, failed with that broker's
    error, the top-level future failing only on a whole-call error."""
    listing = TransactionListing(transactional_id="x", producer_id=5,
                                 transaction_state=TransactionState.EMPTY)
    payload_value = {1: (None, [("x", 5, "Empty")]),
                     2: (_raw_error(ClusterAuthorizationError, "no"), [])}
    monkeypatch.setattr(_lib, "ListTransactionsResult_drain", lambda handle: payload_value)
    result = _conversions.list_transactions_result((1, 0), Sync)
    by_broker = result.by_broker_id().result()
    assert by_broker[1].result() == [listing]
    with pytest.raises(ClusterAuthorizationError):
        by_broker[2].result()
    with pytest.raises(ClusterAuthorizationError):
        result.all().result()


# ---- Ctrl+C and cancellation free the late payload --------------------------


def test_ctrl_c_frees_a_late_void_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ctrl+C during ``abort_transaction`` (a ``void`` call) re-raises at once; its late
    ``(error_int,)`` payload is freed with ``free_void_payload`` when it arrives."""
    freed: list[tuple[Any, ...]] = []
    real = _conversions.free_abort_transaction

    def counting(payload: tuple[Any, ...]) -> None:
        freed.append(payload)
        real(payload)

    monkeypatch.setattr(_conversions, "free_abort_transaction", counting)
    client = Admin.create(configs=UNREACHABLE)
    armed = True

    def on_sigint(signum: int, frame: FrameType | None) -> None:
        if armed:
            raise KeyboardInterrupt

    previous = signal.signal(signal.SIGINT, on_sigint)
    timer = threading.Timer(0.2, lambda: os.kill(os.getpid(), signal.SIGINT))
    started = time.monotonic()
    try:
        timer.start()
        with pytest.raises(KeyboardInterrupt):
            client.abort_transaction(spec=SPEC, options=AbortTransactionOptions(timeout_ms=1500))
    finally:
        timer.cancel()
        timer.join()
        armed = False
        time.sleep(0.05)
        signal.signal(signal.SIGINT, previous)
    assert time.monotonic() - started < 1.5
    assert freed == []
    client.close(timeout=0)  # returns once the aborted call has called back
    assert len(freed) == 1 and len(freed[0]) == 1


async def test_async_cancellation_frees_a_late_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    freed: list[tuple[Any, ...]] = []
    real = _conversions.free_describe_acls

    def counting(payload: tuple[Any, ...]) -> None:
        freed.append(payload)
        real(payload)

    monkeypatch.setattr(_conversions, "free_describe_acls", counting)
    client = AsyncAdmin.create(configs=UNREACHABLE)
    task = asyncio.ensure_future(client.describe_acls(
        filter=AclBindingFilter.ANY, options=DescribeAclsOptions(timeout_ms=1500)))
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await client.close(timeout=0)
    for _ in range(300):
        if freed:
            break
        await asyncio.sleep(0.01)
    assert len(freed) == 1 and len(freed[0]) == 2


def test_async_kind_of_results() -> None:
    """The result builders bind the kind they are given (``Sync`` / ``Async``)."""
    assert _conversions.force_terminate_transaction_result((0,), Sync).result().result() is None
    loop = asyncio.new_event_loop()
    try:
        async def build() -> Any:
            return _conversions.abort_transaction_result(SPEC, (0,), Async)
        result = loop.run_until_complete(build())
        assert isinstance(result.all(), asyncio.Future)
    finally:
        loop.close()
