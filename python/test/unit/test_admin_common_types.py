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

"""The ``org.apache.kafka.common`` value classes and enums the admin family
reaches.

Ports Java's ``AclBindingTest``, ``AclOperationTest``, ``AclPermissionTypeTest``,
``ResourcePatternFilterTest``, ``ResourcePatternTest`` (all in
``common/acl``), ``ResourceTypeTest``, ``ConfigResourceTest`` and
``KafkaPrincipalTest`` in full, and ``TopicCollectionTest`` (in
``clients/admin``). The other classes (``TopicPartitionInfo``, ``TopicPartitionReplica``, the quota types,
``DelegationToken``, ``TokenInformation``, ``ElectionType``, ``GroupState``,
``GroupType``, ``ClassicGroupState``, ``IsolationLevel``) have no Java test of
their own in Kafka 4.3.1; they get smoke tests here: construction, the Java
forms, ``equals`` / ``hashCode`` / ``toString`` as Java's bodies give them, and
the ``Serializable`` pickle round trips.
"""

from __future__ import annotations

import copy
import inspect
import pickle
from collections.abc import Callable
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError, NullPointerError
from confluent_kafka.common import (
    AccessControlEntry,
    AccessControlEntryFilter,
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    ClassicGroupState,
    ClientQuotaAlteration,
    ClientQuotaEntity,
    ClientQuotaFilter,
    ClientQuotaFilterComponent,
    DelegationToken,
    ElectionType,
    GroupState,
    GroupType,
    IsolationLevel,
    KafkaPrincipal,
    Node,
    PatternType,
    ResourcePattern,
    ResourcePatternFilter,
    ResourceType,
    TokenInformation,
    TopicCollection,
    TopicPartition,
    TopicPartitionInfo,
    TopicPartitionReplica,
    Uuid,
)
from confluent_kafka.common.config import ConfigResource
from confluent_kafka.consumer import OffsetAndMetadata

LITERAL = PatternType.LITERAL
PREFIXED = PatternType.PREFIXED
TOPIC = ResourceType.TOPIC
GROUP = ResourceType.GROUP


# --- AclBindingTest -------------------------------------------------------

ACL1 = AclBinding(
    pattern=ResourcePattern(resource_type=TOPIC, name="mytopic", pattern_type=LITERAL),
    entry=AccessControlEntry(principal="User:ANONYMOUS", host="", operation=AclOperation.ALL,
                             permission_type=AclPermissionType.ALLOW))
ACL2 = AclBinding(
    pattern=ResourcePattern(resource_type=TOPIC, name="mytopic", pattern_type=LITERAL),
    entry=AccessControlEntry(principal="User:*", host="", operation=AclOperation.READ,
                             permission_type=AclPermissionType.ALLOW))
ACL3 = AclBinding(
    pattern=ResourcePattern(resource_type=TOPIC, name="mytopic2", pattern_type=LITERAL),
    entry=AccessControlEntry(principal="User:ANONYMOUS", host="127.0.0.1",
                             operation=AclOperation.READ, permission_type=AclPermissionType.DENY))
UNKNOWN_ACL = AclBinding(
    pattern=ResourcePattern(resource_type=TOPIC, name="mytopic2", pattern_type=LITERAL),
    entry=AccessControlEntry(principal="User:ANONYMOUS", host="127.0.0.1",
                             operation=AclOperation.UNKNOWN,
                             permission_type=AclPermissionType.DENY))
ANY_ANONYMOUS = AclBindingFilter(
    pattern_filter=ResourcePatternFilter.ANY,
    entry_filter=AccessControlEntryFilter(principal="User:ANONYMOUS", host=None,
                                          operation=AclOperation.ANY,
                                          permission_type=AclPermissionType.ANY))
ANY_DENY = AclBindingFilter(
    pattern_filter=ResourcePatternFilter.ANY,
    entry_filter=AccessControlEntryFilter(principal=None, host=None, operation=AclOperation.ANY,
                                          permission_type=AclPermissionType.DENY))
ANY_MYTOPIC = AclBindingFilter(
    pattern_filter=ResourcePatternFilter(resource_type=TOPIC, name="mytopic", pattern_type=LITERAL),
    entry_filter=AccessControlEntryFilter(principal=None, host=None, operation=AclOperation.ANY,
                                          permission_type=AclPermissionType.ANY))


def test_acl_binding_matching() -> None:
    """AclBindingTest.testMatching."""
    assert ACL1 == ACL1
    acl1_copy = AclBinding(
        pattern=ResourcePattern(resource_type=TOPIC, name="mytopic", pattern_type=LITERAL),
        entry=AccessControlEntry(principal="User:ANONYMOUS", host="", operation=AclOperation.ALL,
                                 permission_type=AclPermissionType.ALLOW))
    assert ACL1 == acl1_copy
    assert acl1_copy == ACL1
    assert hash(ACL1) == hash(acl1_copy)
    assert ACL2 == ACL2
    assert ACL1 != ACL2
    assert ACL2 != ACL1
    assert AclBindingFilter.ANY.matches(binding=ACL1)
    assert AclBindingFilter.ANY != ACL1
    assert AclBindingFilter.ANY.matches(binding=ACL2)
    assert AclBindingFilter.ANY != ACL2
    assert AclBindingFilter.ANY.matches(binding=ACL3)
    assert AclBindingFilter.ANY != ACL3
    assert AclBindingFilter.ANY == AclBindingFilter.ANY
    assert ANY_ANONYMOUS.matches(binding=ACL1)
    assert ANY_ANONYMOUS != ACL1
    assert not ANY_ANONYMOUS.matches(binding=ACL2)
    assert ANY_ANONYMOUS != ACL2
    assert ANY_ANONYMOUS.matches(binding=ACL3)
    assert ANY_ANONYMOUS != ACL3
    assert not ANY_DENY.matches(binding=ACL1)
    assert not ANY_DENY.matches(binding=ACL2)
    assert ANY_DENY.matches(binding=ACL3)
    assert ANY_MYTOPIC.matches(binding=ACL1)
    assert ANY_MYTOPIC.matches(binding=ACL2)
    assert not ANY_MYTOPIC.matches(binding=ACL3)
    assert ANY_ANONYMOUS.matches(binding=UNKNOWN_ACL)
    assert ANY_DENY.matches(binding=UNKNOWN_ACL)
    assert UNKNOWN_ACL == UNKNOWN_ACL
    assert not ANY_MYTOPIC.matches(binding=UNKNOWN_ACL)


def test_acl_binding_unknowns() -> None:
    """AclBindingTest.testUnknowns."""
    assert not ACL1.is_unknown()
    assert not ACL2.is_unknown()
    assert not ACL3.is_unknown()
    assert not ANY_ANONYMOUS.is_unknown()
    assert not ANY_DENY.is_unknown()
    assert not ANY_MYTOPIC.is_unknown()
    assert UNKNOWN_ACL.is_unknown()


def test_acl_binding_matches_at_most_one() -> None:
    """AclBindingTest.testMatchesAtMostOne."""
    assert ACL1.to_filter().find_indefinite_field() is None
    assert ACL2.to_filter().find_indefinite_field() is None
    assert ACL3.to_filter().find_indefinite_field() is None
    assert not ANY_ANONYMOUS.matches_at_most_one()
    assert not ANY_DENY.matches_at_most_one()
    assert not ANY_MYTOPIC.matches_at_most_one()


def test_acl_binding_should_not_throw_on_unknown_pattern_type() -> None:
    """AclBindingTest.shouldNotThrowOnUnknownPatternType."""
    AclBinding(pattern=ResourcePattern(resource_type=TOPIC, name="foo",
                                       pattern_type=PatternType.UNKNOWN), entry=ACL1.entry())


def test_acl_binding_should_not_throw_on_unknown_resource_type() -> None:
    """AclBindingTest.shouldNotThrowOnUnknownResourceType."""
    AclBinding(pattern=ResourcePattern(resource_type=ResourceType.UNKNOWN, name="foo",
                                       pattern_type=LITERAL), entry=ACL1.entry())


@pytest.mark.parametrize(("resource_type", "pattern_type", "message"), [
    (TOPIC, PatternType.MATCH, "patternType must not be MATCH"),
    (TOPIC, PatternType.ANY, "patternType must not be ANY"),
    (ResourceType.ANY, LITERAL, "resourceType must not be ANY"),
])
def test_acl_binding_should_throw_on_filter_only_values(
        resource_type: ResourceType, pattern_type: PatternType, message: str) -> None:
    """AclBindingTest.shouldThrowOnMatchPatternType, shouldThrowOnAnyPatternType,
    shouldThrowOnAnyResourceType."""
    with pytest.raises(IllegalArgumentError) as e:
        AclBinding(pattern=ResourcePattern(resource_type=resource_type, name="foo",
                                           pattern_type=pattern_type), entry=ACL1.entry())
    assert str(e.value) == message


def test_acl_binding_to_string() -> None:
    assert str(ACL3) == ("(pattern=ResourcePattern(resourceType=TOPIC, name=mytopic2, "
                         "patternType=LITERAL), entry=(principal=User:ANONYMOUS, "
                         "host=127.0.0.1, operation=READ, permissionType=DENY))")
    assert str(AclBindingFilter.ANY) == (
        "(patternFilter=ResourcePattern(resourceType=ANY, name=<any>, patternType=ANY), "
        "entryFilter=(principal=<any>, host=<any>, operation=ANY, permissionType=ANY))")


def test_access_control_entry_checks() -> None:
    with pytest.raises(IllegalArgumentError) as e:
        AccessControlEntry(principal="User:a", host="*", operation=AclOperation.ANY,
                           permission_type=AclPermissionType.ALLOW)
    assert str(e.value) == "operation must not be ANY"
    with pytest.raises(IllegalArgumentError) as e:
        AccessControlEntry(principal="User:a", host="*", operation=AclOperation.READ,
                           permission_type=AclPermissionType.ANY)
    assert str(e.value) == "permissionType must not be ANY"
    with pytest.raises(NullPointerError):
        AccessControlEntry(principal=None, host="*", operation=AclOperation.READ,  # type: ignore[arg-type]
                           permission_type=AclPermissionType.ALLOW)
    entry = ACL3.entry()
    assert entry.to_filter().matches(other=entry)
    assert entry.to_filter().matches_at_most_one()
    assert entry.to_filter() == AccessControlEntryFilter(
        principal="User:ANONYMOUS", host="127.0.0.1", operation=AclOperation.READ,
        permission_type=AclPermissionType.DENY)
    assert AccessControlEntryFilter.ANY.find_indefinite_field() == "Principal is NULL"


def test_acl_types_positional_call_rejected() -> None:
    with pytest.raises(TypeError):
        AclBinding(ACL1.pattern(), ACL1.entry())  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        AclOperation.from_code(3)  # type: ignore[call-arg]


# --- AclOperationTest / AclPermissionTypeTest / ResourceTypeTest ----------

ACL_OPERATION_INFOS = [
    (AclOperation.UNKNOWN, 0, "unknown", True),
    (AclOperation.ANY, 1, "any", False),
    (AclOperation.ALL, 2, "all", False),
    (AclOperation.READ, 3, "read", False),
    (AclOperation.WRITE, 4, "write", False),
    (AclOperation.CREATE, 5, "create", False),
    (AclOperation.DELETE, 6, "delete", False),
    (AclOperation.ALTER, 7, "alter", False),
    (AclOperation.DESCRIBE, 8, "describe", False),
    (AclOperation.CLUSTER_ACTION, 9, "cluster_action", False),
    (AclOperation.DESCRIBE_CONFIGS, 10, "describe_configs", False),
    (AclOperation.ALTER_CONFIGS, 11, "alter_configs", False),
    (AclOperation.IDEMPOTENT_WRITE, 12, "idempotent_write", False),
    (AclOperation.CREATE_TOKENS, 13, "create_tokens", False),
    (AclOperation.DESCRIBE_TOKENS, 14, "describe_tokens", False),
    (AclOperation.TWO_PHASE_COMMIT, 15, "two_phase_commit", False),
]


def test_acl_operation() -> None:
    """AclOperationTest: testIsUnknown, testCode, testName, testExhaustive."""
    for operation, code, name, unknown in ACL_OPERATION_INFOS:
        assert operation.is_unknown() == unknown, f"{operation} unknown == {unknown}"
        assert operation.code() == code
        assert AclOperation.from_code(code=code) is operation
        assert AclOperation.from_string(str=name) is operation
    assert AclOperation.from_code(code=120) is AclOperation.UNKNOWN
    assert AclOperation.from_string(str="something") is AclOperation.UNKNOWN
    assert list(AclOperation) == [info[0] for info in ACL_OPERATION_INFOS]
    assert str(AclOperation.CLUSTER_ACTION) == "CLUSTER_ACTION"


ACL_PERMISSION_TYPE_INFOS = [
    (AclPermissionType.UNKNOWN, 0, "unknown", True),
    (AclPermissionType.ANY, 1, "any", False),
    (AclPermissionType.DENY, 2, "deny", False),
    (AclPermissionType.ALLOW, 3, "allow", False),
]


def test_acl_permission_type() -> None:
    """AclPermissionTypeTest: testIsUnknown, testCode, testName, testExhaustive."""
    for ty, code, name, unknown in ACL_PERMISSION_TYPE_INFOS:
        assert ty.is_unknown() == unknown
        assert ty.code() == code
        assert AclPermissionType.from_code(code=code) is ty
        assert AclPermissionType.from_string(str=name) is ty
    assert AclPermissionType.from_code(code=120) is AclPermissionType.UNKNOWN
    assert AclPermissionType.from_string(str="something") is AclPermissionType.UNKNOWN
    assert list(AclPermissionType) == [info[0] for info in ACL_PERMISSION_TYPE_INFOS]


RESOURCE_TYPE_INFOS = [
    (ResourceType.UNKNOWN, 0, "unknown", True),
    (ResourceType.ANY, 1, "any", False),
    (ResourceType.TOPIC, 2, "topic", False),
    (ResourceType.GROUP, 3, "group", False),
    (ResourceType.CLUSTER, 4, "cluster", False),
    (ResourceType.TRANSACTIONAL_ID, 5, "transactional_id", False),
    (ResourceType.DELEGATION_TOKEN, 6, "delegation_token", False),
    (ResourceType.USER, 7, "user", False),
]


def test_resource_type() -> None:
    """ResourceTypeTest: testIsUnknown, testCode, testName, testExhaustive."""
    for resource_type, code, name, unknown in RESOURCE_TYPE_INFOS:
        assert resource_type.is_unknown() == unknown
        assert resource_type.code() == code
        assert ResourceType.from_code(code=code) is resource_type
        assert ResourceType.from_string(str=name) is resource_type
    assert ResourceType.from_code(code=120) is ResourceType.UNKNOWN
    assert ResourceType.from_string(str="something") is ResourceType.UNKNOWN
    assert list(ResourceType) == [info[0] for info in RESOURCE_TYPE_INFOS]


def test_pattern_type() -> None:
    assert [p.code() for p in PatternType] == [0, 1, 2, 3, 4]
    assert PatternType.from_code(code=4) is PREFIXED
    assert PatternType.from_code(code=99) is PatternType.UNKNOWN
    assert PatternType.from_string(name="LITERAL") is LITERAL
    assert PatternType.from_string(name="literal") is PatternType.UNKNOWN  # case-sensitive
    assert [p for p in PatternType if p.is_specific()] == [LITERAL, PREFIXED]
    assert PatternType.UNKNOWN.is_unknown() and not LITERAL.is_unknown()


# --- ResourcePatternFilterTest --------------------------------------------

def _filter(resource_type: ResourceType, name: str | None,
            pattern_type: PatternType) -> ResourcePatternFilter:
    return ResourcePatternFilter(resource_type=resource_type, name=name, pattern_type=pattern_type)


def _pattern(resource_type: ResourceType, name: str, pattern_type: PatternType) -> ResourcePattern:
    return ResourcePattern(resource_type=resource_type, name=name, pattern_type=pattern_type)


def test_resource_pattern_filter_unknown() -> None:
    """ResourcePatternFilterTest.shouldBeUnknownIfResourceTypeUnknown,
    shouldBeUnknownIfPatternTypeUnknown."""
    assert _filter(ResourceType.UNKNOWN, None, LITERAL).is_unknown()
    assert _filter(GROUP, None, PatternType.UNKNOWN).is_unknown()


@pytest.mark.parametrize(("filter_args", "pattern_args", "expected"), [
    # shouldNotMatchIfDifferentResourceType
    ((TOPIC, "Name", LITERAL), (GROUP, "Name", LITERAL), False),
    # shouldNotMatchIfDifferentName
    ((TOPIC, "Different", PREFIXED), (TOPIC, "Name", PREFIXED), False),
    # shouldNotMatchIfDifferentNameCase
    ((TOPIC, "NAME", LITERAL), (TOPIC, "Name", LITERAL), False),
    # shouldNotMatchIfDifferentPatternType
    ((TOPIC, "Name", LITERAL), (TOPIC, "Name", PREFIXED), False),
    # shouldMatchWhereResourceTypeIsAny
    ((ResourceType.ANY, "Name", PREFIXED), (TOPIC, "Name", PREFIXED), True),
    # shouldMatchWhereResourceNameIsAny
    ((TOPIC, None, PREFIXED), (TOPIC, "Name", PREFIXED), True),
    # shouldMatchWherePatternTypeIsAny
    ((TOPIC, None, PatternType.ANY), (TOPIC, "Name", PREFIXED), True),
    # shouldMatchWherePatternTypeIsMatch
    ((TOPIC, None, PatternType.MATCH), (TOPIC, "Name", PREFIXED), True),
    # shouldMatchLiteralIfExactMatch
    ((TOPIC, "Name", LITERAL), (TOPIC, "Name", LITERAL), True),
    # shouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeAny
    ((TOPIC, "Name", PatternType.ANY), (TOPIC, "Name", LITERAL), True),
    # shouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeMatch
    ((TOPIC, "Name", PatternType.MATCH), (TOPIC, "Name", LITERAL), True),
    # shouldNotMatchLiteralIfNamePrefixed
    ((TOPIC, "Name-something", PatternType.MATCH), (TOPIC, "Name", LITERAL), False),
    # shouldMatchLiteralWildcardIfExactMatch
    ((TOPIC, "*", LITERAL), (TOPIC, "*", LITERAL), True),
    # shouldNotMatchLiteralWildcardAgainstOtherName
    ((TOPIC, "Name", LITERAL), (TOPIC, "*", LITERAL), False),
    # shouldNotMatchLiteralWildcardTheWayAround
    ((TOPIC, "*", LITERAL), (TOPIC, "Name", LITERAL), False),
    # shouldNotMatchLiteralWildcardIfFilterHasPatternTypeOfAny
    ((TOPIC, "Name", PatternType.ANY), (TOPIC, "*", LITERAL), False),
    # shouldMatchLiteralWildcardIfFilterHasPatternTypeOfMatch
    ((TOPIC, "Name", PatternType.MATCH), (TOPIC, "*", LITERAL), True),
    # shouldMatchPrefixedIfExactMatch
    ((TOPIC, "Name", PREFIXED), (TOPIC, "Name", PREFIXED), True),
    # shouldNotMatchIfBothPrefixedAndFilterIsPrefixOfResource
    ((TOPIC, "Name", PREFIXED), (TOPIC, "Name-something", PREFIXED), False),
    # shouldNotMatchIfBothPrefixedAndResourceIsPrefixOfFilter
    ((TOPIC, "Name-something", PREFIXED), (TOPIC, "Name", PREFIXED), False),
    # shouldNotMatchPrefixedIfNamePrefixedAnyFilterTypeIsAny
    ((TOPIC, "Name-something", PatternType.ANY), (TOPIC, "Name", PREFIXED), False),
    # shouldMatchPrefixedIfNamePrefixedAnyFilterTypeIsMatch
    ((TOPIC, "Name-something", PatternType.MATCH), (TOPIC, "Name", PREFIXED), True),
])
def test_resource_pattern_filter_matches(
        filter_args: tuple[ResourceType, str | None, PatternType],
        pattern_args: tuple[ResourceType, str, PatternType], expected: bool) -> None:
    """ResourcePatternFilterTest: every matching case, one row each."""
    assert _filter(*filter_args).matches(pattern=_pattern(*pattern_args)) is expected


def test_resource_pattern_filter_indefinite_fields() -> None:
    assert ResourcePatternFilter.ANY.find_indefinite_field() == "Resource type is ANY."
    assert _filter(TOPIC, None, LITERAL).find_indefinite_field() == "Resource name is NULL."
    assert _filter(TOPIC, "t", PatternType.MATCH).find_indefinite_field() == (
        "Resource pattern type is MATCH.")
    assert _filter(TOPIC, "t", LITERAL).matches_at_most_one()
    assert str(_filter(TOPIC, None, LITERAL)) == (
        "ResourcePattern(resourceType=TOPIC, name=<any>, patternType=LITERAL)")
    with pytest.raises(NullPointerError) as e:
        ResourcePatternFilter(resource_type=None, name=None, pattern_type=LITERAL)  # type: ignore[arg-type]
    assert str(e.value) == "resourceType"


# --- ResourcePatternTest --------------------------------------------------

@pytest.mark.parametrize(("resource_type", "pattern_type", "message"), [
    (ResourceType.ANY, LITERAL, "resourceType must not be ANY"),  # shouldThrowIfResourceTypeIsAny
    (TOPIC, PatternType.MATCH, "patternType must not be MATCH"),  # shouldThrowIfPatternTypeIsMatch
    (TOPIC, PatternType.ANY, "patternType must not be ANY"),  # shouldThrowIfPatternTypeIsAny
])
def test_resource_pattern_should_throw(resource_type: ResourceType, pattern_type: PatternType,
                                       message: str) -> None:
    """ResourcePatternTest: the three IllegalArgumentException cases."""
    with pytest.raises(IllegalArgumentError) as e:
        _pattern(resource_type, "name", pattern_type)
    assert str(e.value) == message


def test_resource_pattern_should_throw_if_resource_name_is_null() -> None:
    """ResourcePatternTest.shouldThrowIfResourceNameIsNull."""
    with pytest.raises(NullPointerError) as e:
        ResourcePattern(resource_type=TOPIC, name=None,  # type: ignore[arg-type]
                        pattern_type=PatternType.ANY)
    assert str(e.value) == "name"


def test_resource_pattern_value_semantics() -> None:
    p = _pattern(TOPIC, "t", LITERAL)
    assert p == _pattern(TOPIC, "t", LITERAL) and hash(p) == hash(_pattern(TOPIC, "t", LITERAL))
    assert p != _pattern(TOPIC, "t", PREFIXED)
    assert p.to_filter() == _filter(TOPIC, "t", LITERAL)
    assert str(p) == "ResourcePattern(resourceType=TOPIC, name=t, patternType=LITERAL)"
    assert ResourcePattern.WILDCARD_RESOURCE == "*"


# --- ConfigResourceTest ---------------------------------------------------

def test_config_resource_should_get_type_from_id() -> None:
    """ConfigResourceTest.shouldGetTypeFromId."""
    assert ConfigResource.Type.for_id(id=2) is ConfigResource.Type.TOPIC
    assert ConfigResource.Type.for_id(id=4) is ConfigResource.Type.BROKER
    assert ConfigResource.Type.for_id(id=16) is ConfigResource.Type.CLIENT_METRICS
    assert ConfigResource.Type.for_id(id=32) is ConfigResource.Type.GROUP


def test_config_resource_should_return_unknown_for_unknown_code() -> None:
    """ConfigResourceTest.shouldReturnUnknownForUnknownCode."""
    assert ConfigResource.Type.for_id(id=-1) is ConfigResource.Type.UNKNOWN
    assert ConfigResource.Type.for_id(id=0) is ConfigResource.Type.UNKNOWN
    assert ConfigResource.Type.for_id(id=1) is ConfigResource.Type.UNKNOWN


def test_config_resource_should_round_trip_every_type() -> None:
    """ConfigResourceTest.shouldRoundTripEveryType."""
    for t in ConfigResource.Type:
        assert ConfigResource.Type.for_id(id=t.id()) is t, str(t)


def test_config_resource_value_semantics() -> None:
    r = ConfigResource(type=ConfigResource.Type.TOPIC, name="t")
    assert r == ConfigResource(type=ConfigResource.Type.TOPIC, name="t")
    assert hash(r) == hash(ConfigResource(type=ConfigResource.Type.TOPIC, name="t"))
    assert r != ConfigResource(type=ConfigResource.Type.BROKER, name="t")
    assert str(r) == "ConfigResource(type=TOPIC, name='t')"
    assert not r.is_default()
    assert ConfigResource(type=ConfigResource.Type.BROKER, name="").is_default()
    assert [t.name for t in ConfigResource.Type] == [
        "GROUP", "CLIENT_METRICS", "BROKER_LOGGER", "BROKER", "TOPIC", "UNKNOWN"]
    with pytest.raises(NullPointerError) as e:
        ConfigResource(type=None, name="t")  # type: ignore[arg-type]
    assert str(e.value) == "type should not be null"
    with pytest.raises(NullPointerError) as e:
        ConfigResource(type=ConfigResource.Type.TOPIC, name=None)  # type: ignore[arg-type]
    assert str(e.value) == "name should not be null"


# --- KafkaPrincipalTest ---------------------------------------------------

def test_kafka_principal_equals_and_hash_code() -> None:
    """KafkaPrincipalTest.testEqualsAndHashCode."""
    name = "KafkaUser"
    principal1 = KafkaPrincipal(principal_type=KafkaPrincipal.USER_TYPE, name=name)
    principal2 = KafkaPrincipal(principal_type=KafkaPrincipal.USER_TYPE, name=name)
    assert hash(principal1) == hash(principal2)
    assert principal1 == principal2


def test_kafka_principal_surface() -> None:
    p = KafkaPrincipal(principal_type="User", name="alice")
    assert str(p) == "User:alice"
    assert p.name() == "alice" and p.principal_type() == "User"
    assert p.token_authenticated() is False
    assert p.token_authenticated(token_authenticated=True) is None
    assert p.token_authenticated() is True
    assert p == KafkaPrincipal(principal_type="User", name="alice", token_authenticated=False)
    assert str(KafkaPrincipal.ANONYMOUS) == "User:ANONYMOUS"
    with pytest.raises(NullPointerError) as e:
        KafkaPrincipal(principal_type=None, name="a")  # type: ignore[arg-type]
    assert str(e.value) == "Principal type cannot be null"
    with pytest.raises(NullPointerError) as e:
        KafkaPrincipal(principal_type="User", name=None)  # type: ignore[arg-type]
    assert str(e.value) == "Principal name cannot be null"


# --- Delegation tokens ----------------------------------------------------

def test_token_information_and_delegation_token() -> None:
    owner = KafkaPrincipal(principal_type="User", name="owner")
    renewer = KafkaPrincipal(principal_type="User", name="renewer")
    info = TokenInformation(token_id="id1", owner=owner, renewers=[renewer], issue_timestamp=1,
                            max_timestamp=3, expiry_timestamp=2)
    assert info.token_requester() is owner  # Java's six-argument form passes owner
    requester = KafkaPrincipal(principal_type="User", name="req")
    info2 = TokenInformation.from_record(token_id="id1", owner=owner, token_requester=requester,
                                         renewers=[renewer], issue_timestamp=1, max_timestamp=3,
                                         expiry_timestamp=2)
    assert info2.token_requester() == requester
    assert info.renewers() == [renewer] and info.renewers_as_string() == ["User:renewer"]
    assert info.owner_as_string() == "User:owner"
    assert info.owner_or_renewer(principal=renewer) and not info.owner_or_renewer(principal=requester)
    assert str(info) == ("TokenInformation{owner=User:owner, tokenRequester=User:owner, "
                         "renewers=[User:renewer], issueTimestamp=1, maxTimestamp=3, "
                         "expiryTimestamp=2, tokenId='id1'}")
    same = TokenInformation(token_id="id1", owner=owner, renewers=[renewer], issue_timestamp=1,
                            max_timestamp=3, expiry_timestamp=2)
    assert info == same and hash(info) == hash(same)
    same.set_expiry_timestamp(expiry_timestamp=9)
    assert same.expiry_timestamp() == 9
    assert info == same  # Java's equals ignores the expiry timestamp

    token = DelegationToken(token_information=info, hmac=b"\x01\x02\xff")
    assert token.token_info() is info
    assert token.hmac() == b"\x01\x02\xff"
    assert token.hmac_as_base64_string() == "AQL/"
    assert token == DelegationToken(token_information=info, hmac=bytes(bytearray(b"\x01\x02\xff")))
    assert token != DelegationToken(token_information=info, hmac=b"\x01")
    assert str(token) == "DelegationToken{tokenInformation=" + str(info) + ", hmac=[*******]}"


# --- Client quotas --------------------------------------------------------

def test_client_quota_types() -> None:
    entity = ClientQuotaEntity(entries={ClientQuotaEntity.USER: "alice",
                                        ClientQuotaEntity.CLIENT_ID: None})
    assert entity.entries() == {"user": "alice", "client-id": None}
    assert str(entity) == "ClientQuotaEntity(entries={user=alice, client-id=null})"
    assert entity == ClientQuotaEntity(entries={"client-id": None, "user": "alice"})
    assert ClientQuotaEntity.is_valid_entity_type(entity_type="ip")
    assert not ClientQuotaEntity.is_valid_entity_type(entity_type="other")

    op = ClientQuotaAlteration.Op(key="producer_byte_rate", value=1024)
    assert op.value() == 1024.0 and isinstance(op.value(), float)
    assert str(op) == "ClientQuotaAlteration.Op(key=producer_byte_rate, value=1024.0)"
    assert op == ClientQuotaAlteration.Op(key="producer_byte_rate", value=1024.0)
    assert str(ClientQuotaAlteration.Op(key="k", value=None)) == (
        "ClientQuotaAlteration.Op(key=k, value=null)")
    alteration = ClientQuotaAlteration(entity=entity, ops=[op])
    assert alteration.entity() is entity and alteration.ops() == [op]
    assert str(alteration) == ("ClientQuotaAlteration(entity=" + str(entity) + ", ops=["
                               + str(op) + "])")

    exact = ClientQuotaFilterComponent.of_entity(entity_type="user", entity_name="alice")
    default = ClientQuotaFilterComponent.of_default_entity(entity_type="user")
    any_name = ClientQuotaFilterComponent.of_entity_type(entity_type="user")
    assert exact.match() == "alice" and default.match() is None and any_name.match() is None
    assert default != any_name  # Optional.empty() vs null
    assert str(exact) == "ClientQuotaFilterComponent(entityType=user, match=Optional[alice])"
    assert str(default) == "ClientQuotaFilterComponent(entityType=user, match=Optional.empty)"
    assert str(any_name) == "ClientQuotaFilterComponent(entityType=user, match=null)"
    with pytest.raises(NullPointerError):
        ClientQuotaFilterComponent.of_entity(entity_type="user", entity_name=None)  # type: ignore[arg-type]
    with pytest.raises(TypeError) as t:
        ClientQuotaFilterComponent()
    assert str(t.value) == (
        "ClientQuotaFilterComponent cannot be constructed directly; use "
        "ClientQuotaFilterComponent.of_entity(...), "
        "ClientQuotaFilterComponent.of_default_entity(...) or "
        "ClientQuotaFilterComponent.of_entity_type(...)")

    contains = ClientQuotaFilter.contains(components=[exact])
    assert not contains.strict() and contains.components() == [exact]
    assert ClientQuotaFilter.contains_only(components=[exact]).strict()
    assert ClientQuotaFilter.all() == ClientQuotaFilter.contains(components=[])
    assert str(contains) == ("ClientQuotaFilter(components=[" + str(exact) + "], strict=false)")
    with pytest.raises(TypeError) as t:
        ClientQuotaFilter()
    assert str(t.value) == (
        "ClientQuotaFilter cannot be constructed directly; use ClientQuotaFilter.contains(...), "
        "ClientQuotaFilter.contains_only(...) or ClientQuotaFilter.all()")


# --- TopicCollection, TopicPartitionInfo, TopicPartitionReplica ------------

def test_topic_collection_java() -> None:
    """TopicCollectionTest.testTopicCollection."""
    topic_ids = [Uuid.random_uuid(), Uuid.random_uuid(), Uuid.random_uuid()]
    topic_names = ["foo", "bar"]
    id_collection = TopicCollection.of_topic_ids(topics=topic_ids)
    name_collection = TopicCollection.of_topic_names(topics=topic_names)
    assert all(t in id_collection.topic_ids() for t in topic_ids)
    assert all(t in name_collection.topic_names() for t in topic_names)


def test_topic_collection() -> None:
    ids = [Uuid.random_uuid(), Uuid.random_uuid()]
    by_id = TopicCollection.of_topic_ids(topics=ids)
    assert isinstance(by_id, TopicCollection) and isinstance(by_id, TopicCollection.TopicIdCollection)
    assert by_id.topic_ids() == ids
    by_name = TopicCollection.of_topic_names(topics=("a", "b"))
    assert isinstance(by_name, TopicCollection.TopicNameCollection)
    assert by_name.topic_names() == ["a", "b"]
    assert type(by_name).__qualname__ == "TopicCollection.TopicNameCollection"
    with pytest.raises(TypeError) as e:
        TopicCollection()
    assert str(e.value) == ("TopicCollection cannot be constructed directly; use "
                            "TopicCollection.of_topic_ids(...) or "
                            "TopicCollection.of_topic_names(...)")
    with pytest.raises(TypeError) as e:
        TopicCollection.TopicIdCollection()
    assert str(e.value) == ("TopicCollection.TopicIdCollection cannot be constructed directly; "
                            "use TopicCollection.of_topic_ids(...)")
    # Java does not override equals: identity equality.
    assert TopicCollection.of_topic_names(topics=["a"]) != TopicCollection.of_topic_names(topics=["a"])


def test_topic_partition_info() -> None:
    n0 = Node(id=0, host="h0", port=9092)
    n1 = Node(id=1, host="h1", port=9092)
    info = TopicPartitionInfo(partition=3, leader=n0, replicas=[n0, n1], isr=[n0])
    assert info.partition() == 3 and info.leader() == n0
    assert info.replicas() == [n0, n1] and info.isr() == [n0]
    assert info.elr() is None and info.last_known_elr() is None
    assert str(info) == ("(partition=3, leader=" + str(n0) + ", replicas=" + str(n0) + ", "
                         + str(n1) + ", isr=" + str(n0) + ", elr=N/A, lastKnownElr=N/A)")
    full = TopicPartitionInfo(partition=3, leader=None, replicas=[n0], isr=[], elr=[n1],
                              last_known_elr=[])
    assert full.leader() is None and full.elr() == [n1] and full.last_known_elr() == []
    assert str(full) == ("(partition=3, leader=null, replicas=" + str(n0) + ", isr=, elr="
                         + str(n1) + ", lastKnownElr=)")
    assert info == TopicPartitionInfo(partition=3, leader=n0, replicas=(n0, n1), isr=[n0])
    assert hash(info) == hash(TopicPartitionInfo(partition=3, leader=n0, replicas=[n0, n1],
                                                 isr=[n0]))
    assert info != TopicPartitionInfo(partition=3, leader=n0, replicas=[n0, n1], isr=[n0],
                                      elr=[], last_known_elr=[])
    with pytest.raises(IllegalArgumentError) as e:
        TopicPartitionInfo(partition=3, leader=n0, replicas=[n0], isr=[n0],  # type: ignore[call-overload]
                           elr=[n1])
    assert str(e.value) == (
        "TopicPartitionInfo() takes one of (partition, leader, replicas, isr, elr, "
        "last_known_elr), (partition, leader, replicas, isr); got (partition, leader, "
        "replicas, isr, elr)")
    with pytest.raises(IllegalArgumentError) as e:
        TopicPartitionInfo(partition=3, leader=n0, replicas=[n0], isr=[n0],  # type: ignore[call-overload]
                           last_known_elr=[n1])
    assert str(e.value) == (
        "TopicPartitionInfo() takes one of (partition, leader, replicas, isr, elr, "
        "last_known_elr), (partition, leader, replicas, isr); got (partition, leader, "
        "replicas, isr, last_known_elr)")


# Pickles of this release, kept to check later releases still load them.
_TOPIC_PARTITION_PICKLE = (
    b"\x80\x02cfunctools\npartial\nq\x00cconfluent_kafka.common.topic_partition\n"
    b"TopicPartition\nq\x01\x85q\x02Rq\x03(h\x01)}q\x04(X\x05\x00\x00\x00topicq\x05X\x01"
    b"\x00\x00\x00tq\x06X\t\x00\x00\x00partitionq\x07K\x01uNtq\x08b)Rq\t.")
_TOPIC_PARTITION_REPLICA_PICKLE = (
    b"\x80\x02cfunctools\npartial\nq\x00cconfluent_kafka.common.topic_partition_replica\n"
    b"TopicPartitionReplica\nq\x01\x85q\x02Rq\x03(h\x01)}q\x04(X\x05\x00\x00\x00topicq\x05"
    b"X\x01\x00\x00\x00tq\x06X\t\x00\x00\x00partitionq\x07K\x01X\t\x00\x00\x00broker_idq"
    b"\x08K\x02uNtq\tb)Rq\n.")


_OFFSET_AND_METADATA_PICKLE = (
    b"\x80\x02cfunctools\npartial\nq\x00cconfluent_kafka.consumer.offset_and_metadata\n"
    b"OffsetAndMetadata\nq\x01\x85q\x02Rq\x03(h\x01)}q\x04(X\x06\x00\x00\x00offsetq\x05"
    b"K\x05X\x0c\x00\x00\x00leader_epochq\x06K\x03X\x08\x00\x00\x00metadataq\x07X\x01"
    b"\x00\x00\x00mq\x08uNtq\tb)Rq\n.")


def test_topic_partition_replica() -> None:
    r = TopicPartitionReplica(topic="t", partition=1, broker_id=2)
    assert (r.topic(), r.partition(), r.broker_id()) == ("t", 1, 2)
    assert str(r) == "t-1-2"
    assert r == TopicPartitionReplica(topic="t", partition=1, broker_id=2)
    assert hash(r) == hash(TopicPartitionReplica(topic="t", partition=1, broker_id=2))
    assert r != TopicPartitionReplica(topic="t", partition=1, broker_id=3)
    with pytest.raises(NullPointerError):
        TopicPartitionReplica(topic=None, partition=1, broker_id=2)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        TopicPartitionReplica("t", 1, 2)  # type: ignore[call-arg]


@pytest.mark.parametrize("value", [
    TopicPartition(topic="t", partition=1),
    TopicPartitionReplica(topic="t", partition=1, broker_id=2),
    OffsetAndMetadata(offset=239, leader_epoch=15, metadata="blah"),
    OffsetAndMetadata(offset=239, metadata="blah"),
    OffsetAndMetadata(offset=239),
    OffsetAndMetadata(offset=239, leader_epoch=-1, metadata="m"),
])
def test_serializable_round_trip(value: object) -> None:
    """Java's ``Serializable``: pickle, copy and deepcopy rebuild through the
    public constructor."""
    for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
        clone = pickle.loads(pickle.dumps(value, protocol=protocol))
        assert type(clone) is type(value) and clone == value
    assert copy.copy(value) == value and copy.deepcopy(value) == value


def test_serializable_loads_stored_pickle() -> None:
    assert pickle.loads(_TOPIC_PARTITION_PICKLE) == TopicPartition(topic="t", partition=1)
    assert pickle.loads(_TOPIC_PARTITION_REPLICA_PICKLE) == TopicPartitionReplica(
        topic="t", partition=1, broker_id=2)
    assert pickle.loads(_OFFSET_AND_METADATA_PICKLE) == OffsetAndMetadata(
        offset=5, leader_epoch=3, metadata="m")


def test_offset_and_metadata_pickle_keeps_raw_leader_epoch() -> None:
    """Java saves the ``leaderEpoch`` field as given (a negative one included);
    the rebuilt object keeps it, so it reads as Java's would."""
    clone = pickle.loads(pickle.dumps(OffsetAndMetadata(offset=1, leader_epoch=-1, metadata="")))
    assert clone.leader_epoch() is None
    assert clone == OffsetAndMetadata(offset=1, leader_epoch=None, metadata="")


# --- Group / election / isolation enums -----------------------------------

def test_group_state_and_types() -> None:
    assert [str(s) for s in GroupState] == [
        "Unknown", "PreparingRebalance", "CompletingRebalance", "Stable", "Dead", "Empty",
        "Assigning", "Reconciling", "NotReady"]
    assert GroupState.parse(name="stable") is GroupState.STABLE
    assert GroupState.parse(name="notready") is GroupState.NOT_READY
    assert GroupState.parse(name="bogus") is GroupState.UNKNOWN
    assert GroupState.group_states_for_type(type=GroupType.SHARE) == {
        GroupState.STABLE, GroupState.DEAD, GroupState.EMPTY}
    assert GroupState.NOT_READY in GroupState.group_states_for_type(type=GroupType.STREAMS)
    with pytest.raises(IllegalArgumentError) as e:
        GroupState.group_states_for_type(type=GroupType.UNKNOWN)
    assert str(e.value) == "Group type not known"
    assert GroupType.parse(name="CONSUMER") is GroupType.CONSUMER
    assert GroupType.parse(name=None) is GroupType.UNKNOWN
    assert str(GroupType.CLASSIC) == "Classic"
    assert ClassicGroupState.parse(name="PREPARINGREBALANCE") is ClassicGroupState.PREPARING_REBALANCE
    assert ClassicGroupState.parse(name="x") is ClassicGroupState.UNKNOWN
    assert str(ClassicGroupState.COMPLETING_REBALANCE) == "CompletingRebalance"
    assert ClassicGroupState.STABLE.value == "STABLE"


def test_election_type_and_isolation_level() -> None:
    assert ElectionType.PREFERRED.value == 0 and ElectionType.UNCLEAN.value == 1
    assert ElectionType.value_of(value=1) is ElectionType.UNCLEAN
    with pytest.raises(IllegalArgumentError) as e:
        ElectionType.value_of(value=5)
    assert str(e.value) == "Value 5 must be one of [PREFERRED, UNCLEAN]"
    assert IsolationLevel.for_id(id=1) is IsolationLevel.READ_COMMITTED
    assert IsolationLevel.READ_UNCOMMITTED.id() == 0
    assert str(IsolationLevel.READ_COMMITTED) == "read_committed"
    with pytest.raises(IllegalArgumentError) as e:
        IsolationLevel.for_id(id=7)
    assert str(e.value) == "Unknown isolation level 7"


# --- Keyword-only signatures ----------------------------------------------

_NODE = Node(id=0, host="h", port=1)
_PRINCIPAL = KafkaPrincipal(principal_type="User", name="a")
_INFO = TokenInformation(token_id="i", owner=_PRINCIPAL, renewers=[], issue_timestamp=1,
                         max_timestamp=2, expiry_timestamp=3)
_COMPONENT = ClientQuotaFilterComponent.of_entity_type(entity_type="user")
_ENTITY = ClientQuotaEntity(entries={})

#: The public classes of this file's subject; every public method, constructor
#: and static of each is swept below.
_CLASSES: list[type] = [
    AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation,
    AclPermissionType, ClassicGroupState, ClientQuotaAlteration, ClientQuotaAlteration.Op,
    ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent, ConfigResource,
    ConfigResource.Type, DelegationToken, ElectionType, GroupState, GroupType, IsolationLevel,
    KafkaPrincipal, OffsetAndMetadata, PatternType, ResourcePattern, ResourcePatternFilter,
    ResourceType, TokenInformation, TopicCollection, TopicCollection.TopicIdCollection,
    TopicCollection.TopicNameCollection, TopicPartition, TopicPartitionInfo,
    TopicPartitionReplica,
]

#: Classes whose ``__init__`` raises ``TypeError`` (abstract or private
#: constructors): their constructor takes ``*args, **kwargs`` by design.
_NOT_CONSTRUCTIBLE = {TopicCollection, TopicCollection.TopicIdCollection,
                      TopicCollection.TopicNameCollection, ClientQuotaFilter,
                      ClientQuotaFilterComponent}


def _own_callables(cls: type) -> list[tuple[str, Callable[..., Any]]]:
    out = []
    for name, member in vars(cls).items():
        if name.startswith("_") and name != "__init__":
            continue
        if name == "__init__" and cls in _NOT_CONSTRUCTIBLE:
            continue
        if isinstance(member, (staticmethod, classmethod)):
            out.append((name, member.__func__))
        elif inspect.isfunction(member):
            out.append((name, member))
    return out


@pytest.mark.parametrize("cls", _CLASSES, ids=lambda c: c.__qualname__)
def test_every_parameter_is_keyword_only(cls: type) -> None:
    """Every parameter of every public method, constructor and static is
    keyword-only (``self`` aside)."""
    callables = _own_callables(cls)
    assert callables or cls in _NOT_CONSTRUCTIBLE
    for name, fn in callables:
        params = list(inspect.signature(fn).parameters.values())
        if params and params[0].name in ("self", "cls"):
            params = params[1:]
        kinds = {p.name: p.kind for p in params}
        assert all(k is inspect.Parameter.KEYWORD_ONLY for k in kinds.values()), (
            f"{cls.__qualname__}.{name}: {kinds}")


_POSITIONAL_CALLS: list[tuple[str, Callable[[], object]]] = [
    ("AccessControlEntry", lambda: AccessControlEntry(
        "User:a", "*", AclOperation.READ, AclPermissionType.ALLOW)),  # type: ignore[call-arg]
    ("AccessControlEntryFilter", lambda: AccessControlEntryFilter(
        None, None, AclOperation.ANY, AclPermissionType.ANY)),  # type: ignore[call-arg]
    ("AccessControlEntryFilter.matches",
     lambda: AccessControlEntryFilter.ANY.matches(ACL1.entry())),  # type: ignore[call-arg]
    ("AclBinding", lambda: AclBinding(ACL1.pattern(), ACL1.entry())),  # type: ignore[call-arg]
    ("AclBindingFilter", lambda: AclBindingFilter(
        ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY)),  # type: ignore[call-arg]
    ("AclBindingFilter.matches", lambda: AclBindingFilter.ANY.matches(ACL1)),  # type: ignore[call-arg]
    ("AclOperation.from_code", lambda: AclOperation.from_code(3)),  # type: ignore[call-arg]
    ("AclOperation.from_string", lambda: AclOperation.from_string("read")),  # type: ignore[call-arg]
    ("AclPermissionType.from_code", lambda: AclPermissionType.from_code(3)),  # type: ignore[call-arg]
    ("AclPermissionType.from_string",
     lambda: AclPermissionType.from_string("allow")),  # type: ignore[call-arg]
    ("ClassicGroupState.parse", lambda: ClassicGroupState.parse("Stable")),  # type: ignore[call-arg]
    ("ClientQuotaAlteration", lambda: ClientQuotaAlteration(_ENTITY, [])),  # type: ignore[call-arg]
    ("ClientQuotaAlteration.Op", lambda: ClientQuotaAlteration.Op("k", 1.0)),  # type: ignore[call-arg]
    ("ClientQuotaEntity", lambda: ClientQuotaEntity({})),  # type: ignore[call-arg]
    ("ClientQuotaEntity.is_valid_entity_type",
     lambda: ClientQuotaEntity.is_valid_entity_type("user")),  # type: ignore[call-arg]
    ("ClientQuotaFilter.contains",
     lambda: ClientQuotaFilter.contains([_COMPONENT])),  # type: ignore[call-arg]
    ("ClientQuotaFilter.contains_only",
     lambda: ClientQuotaFilter.contains_only([_COMPONENT])),  # type: ignore[call-arg]
    ("ClientQuotaFilterComponent.of_entity",
     lambda: ClientQuotaFilterComponent.of_entity("user", "a")),  # type: ignore[call-arg]
    ("ClientQuotaFilterComponent.of_default_entity",
     lambda: ClientQuotaFilterComponent.of_default_entity("user")),  # type: ignore[call-arg]
    ("ClientQuotaFilterComponent.of_entity_type",
     lambda: ClientQuotaFilterComponent.of_entity_type("user")),  # type: ignore[call-arg]
    ("ConfigResource", lambda: ConfigResource(ConfigResource.Type.TOPIC, "t")),  # type: ignore[call-arg]
    ("ConfigResource.Type.for_id", lambda: ConfigResource.Type.for_id(2)),  # type: ignore[call-arg]
    ("DelegationToken", lambda: DelegationToken(_INFO, b"")),  # type: ignore[call-arg]
    ("ElectionType.value_of", lambda: ElectionType.value_of(0)),  # type: ignore[call-arg]
    ("GroupState.parse", lambda: GroupState.parse("Stable")),  # type: ignore[call-arg]
    ("GroupState.group_states_for_type",
     lambda: GroupState.group_states_for_type(GroupType.SHARE)),  # type: ignore[call-arg]
    ("GroupType.parse", lambda: GroupType.parse("Consumer")),  # type: ignore[call-arg]
    ("IsolationLevel.for_id", lambda: IsolationLevel.for_id(0)),  # type: ignore[call-arg]
    ("KafkaPrincipal", lambda: KafkaPrincipal("User", "a")),  # type: ignore[call-arg]
    ("KafkaPrincipal.token_authenticated",
     lambda: _PRINCIPAL.token_authenticated(True)),  # type: ignore[call-overload]
    ("PatternType.from_code", lambda: PatternType.from_code(3)),  # type: ignore[call-arg]
    ("PatternType.from_string", lambda: PatternType.from_string("LITERAL")),  # type: ignore[call-arg]
    ("ResourcePattern", lambda: ResourcePattern(TOPIC, "t", LITERAL)),  # type: ignore[call-arg]
    ("ResourcePatternFilter", lambda: ResourcePatternFilter(TOPIC, "t", LITERAL)),  # type: ignore[call-arg]
    ("ResourcePatternFilter.matches",
     lambda: ResourcePatternFilter.ANY.matches(ACL1.pattern())),  # type: ignore[call-arg]
    ("ResourceType.from_code", lambda: ResourceType.from_code(2)),  # type: ignore[call-arg]
    ("ResourceType.from_string", lambda: ResourceType.from_string("topic")),  # type: ignore[call-arg]
    ("TokenInformation", lambda: TokenInformation(
        "i", _PRINCIPAL, None, [], 1, 2, 3)),  # type: ignore[call-arg]
    ("TokenInformation.from_record", lambda: TokenInformation.from_record(
        "i", _PRINCIPAL, _PRINCIPAL, [], 1, 2, 3)),  # type: ignore[call-arg]
    ("TokenInformation.set_expiry_timestamp",
     lambda: _INFO.set_expiry_timestamp(5)),  # type: ignore[call-arg]
    ("TokenInformation.owner_or_renewer",
     lambda: _INFO.owner_or_renewer(_PRINCIPAL)),  # type: ignore[call-arg]
    ("TopicCollection.of_topic_ids", lambda: TopicCollection.of_topic_ids([])),  # type: ignore[call-arg]
    ("TopicCollection.of_topic_names",
     lambda: TopicCollection.of_topic_names([])),  # type: ignore[call-arg]
    ("TopicPartitionInfo", lambda: TopicPartitionInfo(
        0, _NODE, [], [])),  # type: ignore[call-overload]
    ("TopicPartitionReplica", lambda: TopicPartitionReplica("t", 1, 2)),  # type: ignore[call-arg]
    ("OffsetAndMetadata", lambda: OffsetAndMetadata(5)),  # type: ignore[call-overload]
]


@pytest.mark.parametrize(("name", "call"), _POSITIONAL_CALLS, ids=[c[0] for c in _POSITIONAL_CALLS])
def test_positional_call_raises_type_error(name: str, call: Callable[[], object]) -> None:
    """A positional call of each public method, constructor and static that
    takes parameters raises ``TypeError``."""
    with pytest.raises(TypeError):
        call()
