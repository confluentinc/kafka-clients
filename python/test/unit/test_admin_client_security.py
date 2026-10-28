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

``KafkaAdminClientTest.java`` (Apache Kafka 4.4.0): the tests of these RPCs that need
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
``describe_user_scram_credentials``, the plain arguments and options handed to the
extension, the call returning its result at once (Java's RPCs do) and its futures
failing later with Java's timeout message against an unreachable bootstrap, the closed
check, and the results' value classes built from the plain values the futures'
deliveries convert (the success branch needs a broker).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import pytest

from confluent_kafka import ExecutionError, IllegalArgumentError, IllegalStateError
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
    DescribeUserScramCredentialsResult,
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
from confluent_kafka.admin._futures import Sync
from confluent_kafka.admin._internals import _CoordinatorKey
from confluent_kafka.common import (
    AccessControlEntry,
    AccessControlEntryFilter,
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    AsyncKafkaFuture,
    ClientQuotaAlteration,
    ClientQuotaEntity,
    ClientQuotaFilter,
    ClientQuotaFilterComponent,
    KafkaFuture,
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
)
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError
from confluent_kafka.common.kafka_future import _KafkaFutureImpl

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CLOSED = "Cannot accept new calls when AdminClient is closing."
SHORT = 300
# How long a test waits for a future the client completes later.
WAIT = 10.0


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
    """A core error in the extension's borrowed form (``borrowed_error_to_py``)."""
    return (getattr(cls, "_ffi_id"), message, 0, 0, None, 1, None)


def _cause(future: Any, timeout: float = WAIT) -> BaseException:
    """``get()`` on a failed ``KafkaFuture``: Java's ``ExecutionException``, the
    stored error as its cause."""
    with pytest.raises(ExecutionError) as info:
        future.get(timeout=timeout)
    assert info.value.__cause__ is not None
    return info.value.__cause__


async def _async_cause(future: Any, timeout: float = WAIT) -> BaseException:
    with pytest.raises(ExecutionError) as info:
        await future.get(timeout=timeout)
    assert info.value.__cause__ is not None
    return info.value.__cause__


# ---- each RPC: (name, call, its futures, Java's call name) --------------------

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
        # Java's org.apache.kafka.clients.admin.KafkaAdminClient#forceTerminateTransaction
        # is fenceProducers of the one id, so the call is FenceProducersHandler's.
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
    """The call returns its result at once, as Java's does; every future of it is a
    ``KafkaFuture`` that fails later with Java's message for a call that found no
    node (``Call`` / ``AdminApiDriver``), raised by ``get()`` as the cause of an
    ``ExecutionError``."""
    started = time.monotonic()
    result = call(admin)
    assert time.monotonic() - started < SHORT / 1000
    for future in futures(result):
        assert isinstance(future, KafkaFuture)
        cause = _cause(future)
        assert type(cause) is KafkaTimeoutError
        assert str(cause) == _timed_out(java_call)


@pytest.mark.parametrize(("name", "call", "futures", "java_call"), RPCS, ids=IDS)
async def test_async_rpc_futures_fail_with_the_call_timeout(
        name: str, call: Call, futures: Callable[[Any], list[Any]], java_call: str) -> None:
    """The async peer: the RPC is a plain ``def`` returning its result at once; the
    futures are ``AsyncKafkaFuture``\\ s, awaited through ``get()``."""
    async_admin = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        result = call(async_admin)
        for future in futures(result):
            assert isinstance(future, AsyncKafkaFuture)
            cause = await _async_cause(future)
            assert type(cause) is KafkaTimeoutError
            assert str(cause) == _timed_out(java_call)
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
        call(client)
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
            getattr(client, name)(*args)
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
            cause = _cause(result.users())
            assert type(cause) is KafkaTimeoutError
            assert str(cause) == SCRAM_TIMED_OUT


async def test_async_describe_user_scram_credentials_forms() -> None:
    client = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        options = DescribeUserScramCredentialsOptions(timeout_ms=SHORT)
        with pytest.raises(IllegalArgumentError) as info:
            client.describe_user_scram_credentials(options=options)
        assert str(info.value) == SCRAM_FORMS
        result = client.describe_user_scram_credentials(users=None, options=options)
        cause = await _async_cause(result.all())
        assert type(cause) is KafkaTimeoutError
    finally:
        await client.close(timeout=0)


def test_describe_user_scram_credentials_null_users_is_every_user() -> None:
    """Java's ``null`` (``describeUserScramCredentials()``) crosses as ``None`` (the C
    API's ``NULL`` list: every user, as an empty list is)."""
    assert _conversions.describe_user_scram_credentials_args(None) == (None,)
    assert _conversions.describe_user_scram_credentials_args(("a", "b")) == (["a", "b"],)


# ---- update_features: the core's argument check is raised, as Java throws ----


async def test_async_update_features_should_fail_request_for_empty_updates() -> None:
    """``testUpdateFeaturesShouldFailRequestForEmptyUpdates`` on the async peer."""
    client = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        with pytest.raises(IllegalArgumentError) as info:
            client.update_features(feature_updates={})
        assert str(info.value) == "Feature updates can not be null or empty."
        with pytest.raises(IllegalArgumentError) as info:
            client.update_features(feature_updates={
                "feature": FeatureUpdate(max_version_level=2, upgrade_type=UPGRADE),
                "": FeatureUpdate(max_version_level=2, upgrade_type=UPGRADE)})
        assert str(info.value) == "Provided feature can not be empty."
    finally:
        await client.close(timeout=0)


@pytest.mark.parametrize("blank", [" ", "\t\n"])
def test_update_features_blank_feature_name(admin: Admin, blank: str) -> None:
    """Java's ``Utils.isBlank`` (``trim().isEmpty()``) rejects a blank name before
    the call (``org.apache.kafka.clients.admin.KafkaAdminClient#updateFeatures``)."""
    with pytest.raises(IllegalArgumentError) as info:
        admin.update_features(feature_updates={
            blank: FeatureUpdate(max_version_level=2, upgrade_type=UPGRADE)})
    assert str(info.value) == "Provided feature can not be empty."


def test_describe_acls_unknown_filter_fails_the_future(admin: Admin) -> None:
    """Java's ``org.apache.kafka.clients.admin.KafkaAdminClient#describeAcls`` fails
    the future, without a call, for a filter with UNKNOWN elements; the core does too."""
    unknown = AclBindingFilter(
        pattern_filter=ResourcePatternFilter(resource_type=ResourceType.UNKNOWN, name=None,
                                             pattern_type=PatternType.ANY),
        entry_filter=AccessControlEntryFilter.ANY)
    cause = _cause(admin.describe_acls(filter=unknown).values())
    assert type(cause) is InvalidRequestError
    assert str(cause) == "The AclBindingFilter must not contain UNKNOWN elements."


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
        cause = _cause(future)
        assert type(cause) is KafkaTimeoutError
        assert str(cause) == _timed_out("alterUserScramCredentials")


def test_fence_producers_unknown_id_raises(admin: Admin) -> None:
    """Java's ``FenceProducersResult.producerId`` / ``epochId`` throw
    ``IllegalArgumentException`` for an id not in the request."""
    result = admin.fence_producers(transactional_ids=["x"],
                                   options=FenceProducersOptions(timeout_ms=SHORT))
    for accessor in (result.producer_id, result.epoch_id):
        with pytest.raises(IllegalArgumentError) as info:
            accessor(transactional_id="y")
        assert str(info.value) == "TransactionalId `y` was not included in the request"


# ---- the arguments and options handed to the extension ------------------------


def test_acl_arguments() -> None:
    """``AclBinding`` / ``AclBindingFilter`` as the seven-tuple, enums as codes and a
    filter's ``null`` strings ``None``."""
    binding = _binding("orders")
    assert _conversions.create_acls_args([binding]) == ([(2, "orders", 3, "User:a", "*", 3, 3)],)
    assert _conversions.delete_acls_args([AclBindingFilter.ANY]) == (
        [(1, None, 1, None, None, 1, 1)],)
    assert _conversions.describe_acls_args(binding.to_filter()) == (
        (2, "orders", 3, "User:a", "*", 3, 3),)


def test_delegation_token_options() -> None:
    """Only the setters that ran cross, each with its value read back from the
    options: the renewers and the owner as ``(principal_type, name)``, a ``None`` owner
    as Java's ``null``; the owners of the describe options, ``None`` for Java's
    ``null`` (every owner)."""
    renewer = KafkaPrincipal(principal_type="User", name="r")
    owner = KafkaPrincipal(principal_type="User", name="o")
    assert _conversions.create_delegation_token_options(CreateDelegationTokenOptions()) == []
    options = (CreateDelegationTokenOptions(timeout_ms=3)
               .renewers(renewers=iter([renewer])).owner(owner=owner)
               .max_lifetime_ms(max_lifetime_ms=60000))
    assert _conversions.create_delegation_token_options(options) == [
        ("timeout_ms", 3), ("renewers", [("User", "r")]), ("owner", ("User", "o")),
        ("max_lifetime_ms", 60000)]
    assert _conversions.create_delegation_token_options(
        CreateDelegationTokenOptions().owner(owner=None)) == [("owner", None)]
    assert RenewDelegationTokenOptions().renew_time_period_ms(
        renew_time_period_ms=10)._setters_ran() == [("renew_time_period_ms", 10)]
    assert ExpireDelegationTokenOptions().expiry_time_period_ms(
        expiry_time_period_ms=0)._setters_ran() == [("expiry_time_period_ms", 0)]
    assert _conversions.describe_delegation_token_options(
        DescribeDelegationTokenOptions()) == []
    assert _conversions.describe_delegation_token_options(
        DescribeDelegationTokenOptions().owners(owners=[owner])) == [
        ("owners", [("User", "o")])]
    assert _conversions.describe_delegation_token_options(
        DescribeDelegationTokenOptions().owners(owners=None)) == [("owners", None)]


def test_client_quota_arguments() -> None:
    """A component's match type as ``DescribeClientQuotasRequest`` picks it: exact for
    ``of_entity``, default for ``of_default_entity`` (an empty ``Optional``), specified
    for ``of_entity_type`` (a ``null`` match); an alteration's default entity name and
    a removing op as ``None``."""
    quota_filter = ClientQuotaFilter.contains_only(components=[
        ClientQuotaFilterComponent.of_entity(entity_type="user", entity_name="u"),
        ClientQuotaFilterComponent.of_default_entity(entity_type="client-id"),
        ClientQuotaFilterComponent.of_entity_type(entity_type="ip")])
    assert _conversions.describe_client_quotas_args(quota_filter) == (
        [("user", 0, "u"), ("client-id", 1, None), ("ip", 2, None)], True)
    alteration = ClientQuotaAlteration(entity=ENTITY, ops=[
        ClientQuotaAlteration.Op(key="a", value=2.5), ClientQuotaAlteration.Op(key="b",
                                                                               value=None)])
    (rows,) = _conversions.alter_client_quotas_args([alteration])
    [(pairs, ops)] = rows
    assert sorted(pairs, key=str) == sorted([("user", "u"), ("client-id", None)], key=str)
    assert ops == [("a", 2.5), ("b", None)]
    assert _conversions.alter_client_quotas_args([]) == ([],)
    assert AlterClientQuotasOptions(validate_only=True)._setters_ran() == [
        ("validate_only", True)]


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
    (rows,) = _conversions.alter_user_scram_credentials_args(
        [deletion, upsertion, random_salt])
    assert rows[0] == ("d", True, 2, 0, None, None)
    assert rows[1] == ("u", False, 1, 4096, b"pw", b"")
    assert rows[2][:5] == ("r", False, 1, 8192, b"pw")
    assert rows[2][5] == random_salt.salt() and len(rows[2][5]) > 0


def test_feature_arguments() -> None:
    """Each update as ``(feature, max_version_level, upgrade_type code)``; the
    ``nodeId`` and ``validate_only`` setters cross only when they ran."""
    downgrade = FeatureUpdate(max_version_level=0,
                              upgrade_type=FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)
    assert _conversions.update_features_args(
        {"f": FeatureUpdate(max_version_level=2, upgrade_type=UPGRADE), "g": downgrade}) == (
        [("f", 2, 1), ("g", 0, 2)],)
    assert _conversions.update_features_args({}) == ([],)
    assert DescribeFeaturesOptions()._setters_ran() == []
    assert DescribeFeaturesOptions().node_id(node_id=3)._setters_ran() == [("node_id", 3)]
    assert UpdateFeaturesOptions(validate_only=True, timeout_ms=7)._setters_ran() == [
        ("timeout_ms", 7), ("validate_only", True)]


def test_producer_and_transaction_arguments() -> None:
    """Distinct partitions as ``(topic, partition)``; distinct ids; the spec's fields
    as one tuple; the listing filters as state names and producer ids read back from
    the options (a one-shot iterable given to a setter is not read twice)."""
    assert _conversions.describe_producers_args([TP, TP]) == ([("t", 0)],)
    assert DescribeProducersOptions().broker_id(broker_id=5)._setters_ran() == [
        ("broker_id", 5)]
    assert _conversions.describe_transactions_args(("a", "b", "a")) == (["a", "b"],)
    assert _conversions.abort_transaction_args(SPEC) == (("t", 0, 7, 1, 2),)
    assert _conversions.list_transactions_options(ListTransactionsOptions()) == []
    options = (ListTransactionsOptions(timeout_ms=1)
               .filter_states(states=(s for s in [TransactionState.ONGOING]))
               .filter_producer_ids(producer_id_filters=iter([9]))
               .filter_on_duration(duration_ms=100)
               .filter_on_transactional_id_pattern(pattern="tx-.*"))
    assert _conversions.list_transactions_options(options) == [
        ("timeout_ms", 1), ("filter_states", ["Ongoing"]), ("filter_producer_ids", [9]),
        ("filter_on_duration", 100), ("filter_on_transactional_id_pattern", "tx-.*")]
    assert _conversions.fence_producers_args(["x", "x"]) == (["x"],)


# ---- the result values, from the plain values the deliveries convert ----------


def test_acl_values() -> None:
    raw = (2, "orders", 3, "User:a", "*", 3, 3)
    assert _conversions._to_acl_binding(raw) == _binding("orders")
    assert _conversions._acl_bindings([raw]) == [_binding("orders")]
    assert _conversions._to_acl_binding_filter(
        (1, None, 1, None, None, 1, 1)) == AclBindingFilter.ANY
    results = _conversions._filter_results(
        [(None, raw), (_raw_error(ClusterAuthorizationError, "denied"), None)])
    assert isinstance(results, DeleteAclsResult.FilterResults)
    first, second = results.values()
    assert first.binding() == _binding("orders") and first.exception() is None
    assert second.binding() is None
    assert isinstance(second.exception(), ClusterAuthorizationError)
    assert str(second.exception()) == "denied"
    # A matched ACL's error that is no ApiError fails the filter (decision log 53).
    with pytest.raises(Exception) as info:
        _conversions._filter_results([((-1, "core", 0, 0, None, 0, None), None)])
    assert str(info.value) == "core"


def test_delegation_token_values() -> None:
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
    assert _conversions._delegation_tokens([raw]) == [token]


def test_client_quota_values() -> None:
    entities = _conversions._quota_entities({(("user", "u"), ("client-id", None)): [
        ("producer_byte_rate", 1024.0)]})
    assert entities == {ENTITY: {"producer_byte_rate": 1024.0}}


def test_scram_values() -> None:
    """``all()``'s map and ``description(user)``'s rows become
    ``UserScramCredentialsDescription``\\ s; a user with no credential (Java's
    ``RESOURCE_NOT_FOUND`` user in ``all()``) has none."""
    described = _conversions._scram_descriptions({"a": [(1, 4096), (2, 8192)], "nobody": []})
    assert list(described) == ["a", "nobody"]
    assert described["a"].name() == "a"
    assert described["a"].credential_infos() == [
        ScramCredentialInfo(mechanism=ScramMechanism.SCRAM_SHA_256, iterations=4096),
        ScramCredentialInfo(mechanism=ScramMechanism.SCRAM_SHA_512, iterations=8192)]
    assert described["nobody"].credential_infos() == []


def test_scram_result_of_the_client_reads_the_core_futures() -> None:
    """The client's ``DescribeUserScramCredentialsResult``: ``all()``, ``users()`` and
    ``description(user_name)`` are the core's own futures, asked for at each call."""
    asked: list[tuple[str, str | None]] = []

    def future(accessor: str, user: str | None) -> KafkaFuture[Any]:
        asked.append((accessor, user))
        impl: _KafkaFutureImpl[Any] = _KafkaFutureImpl()
        if accessor == "description":
            impl.complete_with_error(ResourceNotFoundError(message="no nobody"))
        else:
            impl.complete({"all": {}, "users": ["a"]}[accessor])
        return KafkaFuture._of_impl(impl)

    result = DescribeUserScramCredentialsResult._of_client(future=future, kind=Sync)
    assert result.all().get() == {}
    assert result.users().get() == ["a"]
    cause = _cause(result.description(user_name="nobody"))
    assert type(cause) is ResourceNotFoundError and str(cause) == "no nobody"
    assert asked == [("all", None), ("users", None), ("description", "nobody")]


def test_feature_metadata_values() -> None:
    metadata = _conversions._to_feature_metadata(
        ([("metadata.version", 1, 7)], 42, [("metadata.version", 1, 21)]))
    assert metadata.finalized_features_epoch() == 42
    finalized = metadata.finalized_features()["metadata.version"]
    assert (finalized.min_version_level(), finalized.max_version_level()) == (1, 7)
    supported = metadata.supported_features()["metadata.version"]
    assert (supported.min_version(), supported.max_version()) == (1, 21)
    assert _conversions._to_feature_metadata(([], None, [])).finalized_features_epoch() is None


def test_producer_and_transaction_values() -> None:
    state = _conversions._partition_producer_state(
        [(5, 1, 9, 100, None, 7), (6, 2, 3, 200, 4, None)])
    assert isinstance(state, DescribeProducersResult.PartitionProducerState)
    assert state.active_producers() == [
        ProducerState(producer_id=5, producer_epoch=1, last_sequence=9, last_timestamp=100,
                      coordinator_epoch=None, current_transaction_start_offset=7),
        ProducerState(producer_id=6, producer_epoch=2, last_sequence=3, last_timestamp=200,
                      coordinator_epoch=4, current_transaction_start_offset=None)]
    description = _conversions._transaction_description(
        (1, "Ongoing", 5, 0, 60000, None, [("t", 0)]))
    assert description == TransactionDescription(
        coordinator_id=1, state=TransactionState.ONGOING, producer_id=5, producer_epoch=0,
        transaction_timeout_ms=60000, transaction_start_time_ms=None, topic_partitions=[TP])
    assert _conversions._transaction_listings([("x", 5, "Empty")]) == [
        TransactionListing(transactional_id="x", producer_id=5,
                           transaction_state=TransactionState.EMPTY)]
    assert _conversions._transactional_id_key("x") == _CoordinatorKey.by_transactional_id(
        transactional_id="x")
