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

"""``MISSING`` and Java's ``null`` in the generated modules (``cargo xtask
generate-error-codes`` / ``generate-admin-options`` / ``generate-admin-results``):
every omittable parameter defaults to ``MISSING``; an explicit ``None`` selects
the Java constructor or setter that takes it; a ``None`` a setter dereferences
raises ``TypeError``; and the stubs are one per Java constructor, with no
defaults (CLAUDE.md, Python Binding Conventions, Signatures and Errors)."""

from __future__ import annotations

import ast
import copy
import inspect
import pickle
from pathlib import Path
from typing import Any

import pytest

import confluent_kafka
from confluent_kafka import IllegalArgumentError
from confluent_kafka._args import MISSING
from confluent_kafka.admin import (
    CreateDelegationTokenOptions,
    CreateTopicsOptions,
    DescribeFeaturesOptions,
    DescribeTopicsOptions,
    ListGroupsOptions,
    ListOffsetsOptions,
    ListTransactionsOptions,
)
from confluent_kafka.common import GroupState, IsolationLevel
from confluent_kafka.common.errors import (
    AuthenticationError,
    TimeoutError as KafkaTimeoutError,
    TopicAuthorizationError,
)

_PACKAGE = Path(confluent_kafka.__file__).parent


def _stubs(relative: str, cls: str) -> list[ast.FunctionDef]:
    tree = ast.parse((_PACKAGE / relative).read_text())
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == cls:
            return [f for f in node.body if isinstance(f, ast.FunctionDef) and f.name == "__init__"
                    and any(isinstance(d, ast.Name) and d.id == "overload"
                            for d in f.decorator_list)]
    raise AssertionError(cls)


def _names(stub: ast.FunctionDef) -> list[str]:
    return [a.arg for a in stub.args.kwonlyargs]


# ---- the errors ------------------------------------------------------------------
def test_error_signatures_default_to_missing() -> None:
    params = inspect.signature(KafkaTimeoutError).parameters
    assert params["message"].default is MISSING and params["cause"].default is MISSING


def test_error_stubs_are_one_per_java_constructor() -> None:
    # TimeoutException(), (message, cause), (message), (cause), in Java's order;
    # an optional cause is two stubs, and no stub shows a default.
    stubs = _stubs("common/errors/timeout_error.pyi", "TimeoutError")
    assert [_names(s) for s in stubs] == [[], ["message", "cause"], ["message"], ["cause"]]
    for s in stubs:
        assert not [d for d in s.args.kw_defaults if d is not None]
    stubs = _stubs("common/errors/topic_authorization_error.pyi", "TopicAuthorizationError")
    assert [_names(s) for s in stubs] == [
        ["message", "unauthorized_topics"], ["unauthorized_topics"], ["message"]]


def test_an_explicit_none_message_is_javas_null_message() -> None:
    # Java's (String) null: getMessage() is null, str() is "".
    e = KafkaTimeoutError(message=None)
    assert str(e) == "" and e.__cause__ is None
    cause = ValueError("boom")
    # Java's (Throwable) passes cause.toString() as the message; (null, cause) does not.
    assert str(KafkaTimeoutError(cause=cause)) == "builtins.ValueError: boom"
    assert str(KafkaTimeoutError(message=None, cause=cause)) == ""
    assert KafkaTimeoutError(message="m", cause=None).__cause__ is None
    assert str(KafkaTimeoutError(message=MISSING)) == ""


def test_a_null_set_is_given() -> None:
    # TopicAuthorizationException(String message) passes emptySet(); a given
    # None is Java's (message, null), whose set stays null.
    assert TopicAuthorizationError(message="x").unauthorized_topics() == set()
    assert TopicAuthorizationError(message="x", unauthorized_topics=None).unauthorized_topics() is None
    with pytest.raises(IllegalArgumentError):
        TopicAuthorizationError()  # type: ignore[call-overload]


def test_errors_pickle_with_their_given_arguments() -> None:
    for e in (KafkaTimeoutError(message=None), AuthenticationError(cause=ValueError("v")),
              TopicAuthorizationError(message="x", unauthorized_topics=None)):
        for copied in (copy.copy(e), pickle.loads(pickle.dumps(e))):
            assert type(copied) is type(e) and str(copied) == str(e)


# ---- the Options ---------------------------------------------------------------
def test_options_keywords_default_to_missing() -> None:
    params = inspect.signature(CreateTopicsOptions).parameters
    assert all(p.default is MISSING for p in params.values())


def test_a_merged_getter_setter_reads_when_left_out() -> None:
    o = DescribeTopicsOptions()
    assert o.include_authorized_operations() is False
    assert o.include_authorized_operations(include_authorized_operations=MISSING) is False
    o.include_authorized_operations(include_authorized_operations=True)
    assert o.include_authorized_operations() is True
    # A boolean: Java cannot pass null.
    with pytest.raises(TypeError, match=r"^include_authorized_operations must not be None$"):
        o.include_authorized_operations(include_authorized_operations=None)  # type: ignore[call-overload]
    with pytest.raises(TypeError, match=r"^node_id must not be None$"):
        DescribeFeaturesOptions().node_id(node_id=None)  # type: ignore[call-overload]


def test_a_nullable_setter_argument_sets_null() -> None:
    o = CreateTopicsOptions(timeout_ms=5)
    assert o.timeout_ms(timeout_ms=None).timeout_ms() is None
    assert CreateDelegationTokenOptions().owner(owner=None).owner() is None


def test_setters_reject_a_null_they_dereference() -> None:
    with pytest.raises(TypeError, match=r"^validate_only must not be None$"):
        CreateTopicsOptions().validate_only(validate_only=None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=r"^validate_only must not be None$"):
        CreateTopicsOptions(validate_only=None)  # type: ignore[arg-type]
    # new HashSet<>(null)
    with pytest.raises(TypeError, match=r"^states must not be None$"):
        ListTransactionsOptions().filter_states(states=None)  # type: ignore[arg-type]
    # a collection the admin client iterates later: stored, as Java's setter does
    assert CreateDelegationTokenOptions().renewers(renewers=None).renewers() is None  # type: ignore[call-overload]


def test_set_copy_of_rejects_a_null_element() -> None:
    # (x == null || x.isEmpty()) ? Set.of() : Set.copyOf(x): a null is empty, a
    # null element throws.
    assert ListGroupsOptions().in_group_states(group_states=None).group_states() == set()
    with pytest.raises(TypeError, match=r"^group_states must not contain None$"):
        ListGroupsOptions().in_group_states(group_states=[GroupState.STABLE, None])  # type: ignore[list-item]
    with pytest.raises(TypeError, match=r"^protocol_types must not contain None$"):
        ListGroupsOptions(protocol_types=["consumer", None])  # type: ignore[list-item]


def test_list_offsets_options_constructors() -> None:
    # () passes READ_UNCOMMITTED; a given None is stored, as Java's final field.
    assert ListOffsetsOptions().isolation_level() is IsolationLevel.READ_UNCOMMITTED
    assert ListOffsetsOptions(isolation_level=MISSING).isolation_level() is (
        IsolationLevel.READ_UNCOMMITTED)
    assert ListOffsetsOptions(
        isolation_level=IsolationLevel.READ_COMMITTED).isolation_level() is (
        IsolationLevel.READ_COMMITTED)
    stubs = _stubs("admin/list_offsets_options.pyi", "ListOffsetsOptions")
    assert [_names(s) for s in stubs] == [[], ["isolation_level"]]


def test_the_setter_built_keyword_constructor_keeps_its_one_stub() -> None:
    # The one stub that shows defaults: the fields' Java starting values.
    tree = ast.parse((_PACKAGE / "admin/create_topics_options.pyi").read_text())
    inits = [f for c in tree.body if isinstance(c, ast.ClassDef)
             for f in c.body if isinstance(f, ast.FunctionDef) and f.name == "__init__"]
    assert len(inits) == 1
    assert [ast.unparse(d) for d in inits[0].args.kw_defaults if d is not None] == [
        "None", "False", "True"]


def test_a_given_value_equal_to_the_starting_value_calls_the_setter() -> None:
    # A keyword given runs its setter even with the starting value; left out, none.
    o: Any = CreateTopicsOptions(validate_only=False)
    assert o._setters_ran() == [("validate_only", False)]
    assert CreateTopicsOptions(validate_only=MISSING)._setters_ran() == []
