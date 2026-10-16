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

"""Smoke tests of the 45 generated admin ``*Options`` classes (``cargo xtask
generate-admin-options``).

Every class is imported from ``confluent_kafka.admin``, constructed with no argument
and with each keyword of its constructor alone, and its positional calls are
rejected. For every class, each getter's starting value equals the default its stub
shows (Java's starting value), and the setters the constructor keywords call are
chained on one object. These are the Python counterparts of the Rust core's
``defaults_match_java`` / ``fluent_setters`` tests, since Java has none. Java has a test for only two
Options classes, ``ListGroupsOptionsTest`` and ``RemoveMembersFromConsumerGroupOptionsTest``;
both are reference classes and are translated in ``test_admin_reference_set.py``.
"""

from __future__ import annotations

import ast
import inspect
import sys
from pathlib import Path
from typing import Any

import pytest

import confluent_kafka.admin as admin
from confluent_kafka.admin import (
    AbortTransactionOptions,
    AbstractOptions,
    AlterPartitionReassignmentsOptions,
    DescribeClusterOptions,
    DescribeDelegationTokenOptions,
    ExpireDelegationTokenOptions,
    FenceProducersOptions,
    ListConsumerGroupOffsetsOptions,
    MemberToRemove,
    TerminateTransactionOptions,
    TransactionState,
)
from confluent_kafka.common import GroupState, GroupType, IsolationLevel, KafkaPrincipal

OPTIONS = [
    "AbortTransactionOptions", "AlterClientQuotasOptions", "AlterConfigsOptions",
    "AlterConsumerGroupOffsetsOptions", "AlterPartitionReassignmentsOptions",
    "AlterReplicaLogDirsOptions", "AlterUserScramCredentialsOptions", "CreateAclsOptions",
    "CreateDelegationTokenOptions", "CreatePartitionsOptions", "CreateTopicsOptions",
    "DeleteAclsOptions", "DeleteConsumerGroupOffsetsOptions", "DeleteConsumerGroupsOptions",
    "DeleteRecordsOptions", "DeleteTopicsOptions", "DescribeAclsOptions",
    "DescribeClassicGroupsOptions", "DescribeClientQuotasOptions", "DescribeClusterOptions",
    "DescribeConfigsOptions", "DescribeConsumerGroupsOptions", "DescribeDelegationTokenOptions",
    "DescribeFeaturesOptions", "DescribeLogDirsOptions", "DescribeProducersOptions",
    "DescribeReplicaLogDirsOptions", "DescribeTopicsOptions", "DescribeTransactionsOptions",
    "DescribeUserScramCredentialsOptions", "ElectLeadersOptions", "ExpireDelegationTokenOptions",
    "FenceProducersOptions", "ListConfigResourcesOptions", "ListConsumerGroupOffsetsOptions",
    "ListGroupsOptions", "ListOffsetsOptions", "ListPartitionReassignmentsOptions",
    "ListTopicsOptions", "ListTransactionsOptions", "RemoveMembersFromConsumerGroupOptions",
    "RenewDelegationTokenOptions", "TerminateTransactionOptions", "UpdateFeaturesOptions",
]

_PRINCIPAL = KafkaPrincipal(principal_type="User", name="alice")

# A sample argument per annotation of a constructor keyword.
_SAMPLES: dict[str, Any] = {
    "int | None": 1234,
    "bool": True,
    "int": 7,
    "str | None": "txn-.*",
    "Sequence[KafkaPrincipal]": [_PRINCIPAL],
    "Sequence[KafkaPrincipal] | None": [_PRINCIPAL],
    "KafkaPrincipal | None": _PRINCIPAL,
    "Iterable[GroupState] | None": [GroupState.STABLE],
    "Iterable[GroupType] | None": [GroupType.CONSUMER],
    "Iterable[str] | None": ["consumer"],
    "Iterable[TransactionState]": [TransactionState.ONGOING],
    "Iterable[int]": [5, 6],
    "Iterable[MemberToRemove] | None": [MemberToRemove(group_instance_id="i")],
    "IsolationLevel": IsolationLevel.READ_COMMITTED,
}


def _keywords(cls: type) -> list[inspect.Parameter]:
    return [p for p in inspect.signature(cls).parameters.values()
            if p.kind is inspect.Parameter.KEYWORD_ONLY]


def _read(obj: Any, keyword: str) -> Any:
    for name in ("should_" + keyword, keyword):
        getter = getattr(obj, name, None)
        if callable(getter):
            return getter()
    raise AssertionError(f"{type(obj).__name__}: no getter for {keyword}")


@pytest.mark.parametrize("name", OPTIONS)
def test_exported_and_constructible(name: str) -> None:
    cls = getattr(admin, name)
    assert name in admin.__all__
    assert issubclass(cls, AbstractOptions)
    options = cls()
    assert options.timeout_ms() is None


@pytest.mark.parametrize("name", OPTIONS)
def test_every_keyword_alone(name: str) -> None:
    cls = getattr(admin, name)
    for param in _keywords(cls):
        value = _SAMPLES[str(param.annotation)]
        options = cls(**{param.name: value})
        got = _read(options, param.name)
        if isinstance(got, (set, frozenset)):
            assert got == set(value)
        elif isinstance(got, list):
            assert got == list(value)
        else:
            assert got == value, (name, param.name)


@pytest.mark.parametrize("name", OPTIONS)
def test_positional_construction_raises_type_error(name: str) -> None:
    cls = getattr(admin, name)
    with pytest.raises(TypeError):
        cls(1)


def test_abstract_options_is_abstract() -> None:
    with pytest.raises(TypeError, match="AbstractOptions is an abstract class"):
        AbstractOptions()


def test_timeout_ms_keyword_and_setter() -> None:
    options = TerminateTransactionOptions(timeout_ms=10)
    assert options.timeout_ms() == 10
    assert options.timeout_ms(timeout_ms=None) is options
    assert options.timeout_ms() is None
    with pytest.raises(TypeError):
        options.timeout_ms(5)  # type: ignore[call-overload]


def test_java_starting_values() -> None:
    assert AlterPartitionReassignmentsOptions().allow_replication_factor_change() is True
    assert DescribeClusterOptions().include_authorized_operations() is False
    assert DescribeClusterOptions().include_fenced_brokers() is False
    assert DescribeDelegationTokenOptions().owners() is None
    assert ExpireDelegationTokenOptions().expiry_time_period_ms() == -1
    assert ListConsumerGroupOffsetsOptions().require_stable() is False


def test_setter_parameter_named_like_java() -> None:
    # Java: allowReplicationFactorChange(boolean allow); the keyword is named after the field.
    options = AlterPartitionReassignmentsOptions(allow_replication_factor_change=False)
    assert options.allow_replication_factor_change() is False
    assert options.allow_replication_factor_change(allow=True) is options
    assert options.allow_replication_factor_change() is True


def test_nullable_owners_sets_null() -> None:
    options = DescribeDelegationTokenOptions(owners=[_PRINCIPAL])
    assert options.owners() == [_PRINCIPAL]
    assert options.owners(owners=None) is options
    assert options.owners() is None


def test_to_string() -> None:
    assert str(AbortTransactionOptions()) == "AbortTransactionOptions(timeoutMs=null)"
    assert str(AbortTransactionOptions(timeout_ms=5)) == "AbortTransactionOptions(timeoutMs=5)"
    assert str(FenceProducersOptions()) == "FenceProducersOptions{timeoutMs=null}"
    assert str(TerminateTransactionOptions(timeout_ms=3)) == "TerminateTransactionOptions{timeoutMs=3}"


def _module_path(cls: type, suffix: str) -> Path:
    file = sys.modules[cls.__module__].__file__
    assert file is not None
    return Path(file).with_suffix(suffix)


def _init(cls: type, suffix: str) -> ast.FunctionDef | None:
    tree = ast.parse(_module_path(cls, suffix).read_text())
    node = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls.__name__)
    return next((f for f in node.body if isinstance(f, ast.FunctionDef) and f.name == "__init__"), None)


def _stub_defaults(cls: type) -> dict[str, Any]:
    """The constructor keywords' defaults the ``.pyi`` shows (``...`` kept as ``...``)."""
    init = _init(cls, ".pyi")
    if init is None:
        return {}
    out: dict[str, Any] = {}
    for arg, default in zip(init.args.kwonlyargs, init.args.kw_defaults):
        assert default is not None, (cls.__name__, arg.arg)
        if isinstance(default, ast.Attribute):
            out[arg.arg] = eval(ast.unparse(default), {"IsolationLevel": IsolationLevel})
        else:
            out[arg.arg] = ast.literal_eval(default)
    return out


def _keyword_setters(cls: type) -> list[tuple[str, str, str]]:
    """``(keyword, setter, setter parameter)`` per ``if kw is not UNSET: self.setter(p=kw)``."""
    init = _init(cls, ".py")
    assert init is not None
    out = []
    for stmt in init.body:
        if isinstance(stmt, ast.If) and isinstance(stmt.test, ast.Compare):
            call = stmt.body[0]
            assert isinstance(call, ast.Expr) and isinstance(call.value, ast.Call)
            func = call.value.func
            assert isinstance(func, ast.Attribute) and isinstance(stmt.test.left, ast.Name)
            out.append((stmt.test.left.id, func.attr, call.value.keywords[0].arg or ""))
    return out


def _normal(value: Any) -> Any:
    if isinstance(value, (set, frozenset, list, tuple)):
        return sorted(value, key=repr)
    return value


# Constructor-only classes whose keyword is not a setter field: ``members=None`` selects
# Java's ``()`` form, which stores ``Collections.emptySet()``.
_CONSTRUCTOR_STARTS: dict[tuple[str, str], Any] = {("RemoveMembersFromConsumerGroupOptions", "members"): set()}


@pytest.mark.parametrize("name", OPTIONS)
def test_starting_values_match_the_stub(name: str) -> None:
    cls = getattr(admin, name)
    options = cls()
    for keyword, default in _stub_defaults(cls).items():
        expected = _CONSTRUCTOR_STARTS.get((name, keyword), default)
        if expected is ...:
            continue
        assert _normal(_read(options, keyword)) == _normal(expected), (name, keyword)


@pytest.mark.parametrize("name", [n for n in OPTIONS if n not in (
    "ListOffsetsOptions", "RemoveMembersFromConsumerGroupOptions")])
def test_setters_chain(name: str) -> None:
    cls = getattr(admin, name)
    annotations = {p.name: str(p.annotation) for p in _keywords(cls)}
    options = cls()
    current = options
    for keyword, setter, param in _keyword_setters(cls):
        returned = getattr(current, setter)(**{param: _SAMPLES[annotations[keyword]]})
        assert returned is options or returned is None, (name, setter)
        current = options
    for keyword, _, _ in _keyword_setters(cls):
        assert _normal(_read(options, keyword)) == _normal(_SAMPLES[annotations[keyword]])


def test_constructor_only_classes() -> None:
    from confluent_kafka.admin import ListOffsetsOptions, RemoveMembersFromConsumerGroupOptions

    assert ListOffsetsOptions().isolation_level() is IsolationLevel.READ_UNCOMMITTED
    member = MemberToRemove(group_instance_id="i")
    assert RemoveMembersFromConsumerGroupOptions(members=[member]).members() == {member}
    assert RemoveMembersFromConsumerGroupOptions().remove_all() is True


# ---- the setters that ran (the client hands them to the extension) -------------------


def test_setters_ran_in_the_order_they_last_ran() -> None:
    """``AbstractOptions`` records each setter that ran, the latest last; the
    client runs the same setters on the C options (CLAUDE.md conversion table, "an
    Options object"). A field never set is not sent: the core keeps Java's starting
    value."""
    options = admin.CreateTopicsOptions()
    assert options._setters_ran() == []
    options.validate_only(validate_only=True)
    options.timeout_ms(timeout_ms=5)
    options.validate_only(validate_only=False)
    assert options._setters_ran() == [("timeout_ms", 5), ("validate_only", False)]
    # A getter is no setter.
    options.timeout_ms()
    options.should_validate_only()
    assert options._setters_ran() == [("timeout_ms", 5), ("validate_only", False)]
    # The keyword constructor calls the setters.
    assert admin.DescribeClusterOptions(include_fenced_brokers=True)._setters_ran() == [
        ("include_fenced_brokers", True)]
    assert admin.DeleteRecordsOptions(timeout_ms=None)._setters_ran() == [("timeout_ms", None)]


def test_setters_ran_carry_what_the_setter_stored() -> None:
    """A collection argument crosses as the copy Java's setter stores
    (``Set.copyOf``, ``new HashSet<>(states)``), so a one-shot iterator is read once
    and arrives whole."""
    states = admin.ListTransactionsOptions().filter_states(
        states=(s for s in [TransactionState.ONGOING, TransactionState.EMPTY]))
    assert states._setters_ran() == [
        ("filter_states", frozenset({TransactionState.ONGOING, TransactionState.EMPTY}))]
    groups = admin.ListGroupsOptions().in_group_states(
        group_states=(g for g in [GroupState.STABLE]))
    assert groups._setters_ran() == [("in_group_states", frozenset({GroupState.STABLE}))]
    # Java's null set is Set.of().
    assert admin.ListGroupsOptions().in_group_states(group_states=None)._setters_ran() == [
        ("in_group_states", frozenset())]
