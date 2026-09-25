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

"""Tests for ``confluent_kafka._args``: the ``java_forms`` overload matcher and
``UNSET`` (CLAUDE.md, Python Binding Conventions, Signatures)."""

from __future__ import annotations

import asyncio
import copy
import inspect
import pickle
import warnings
from typing import Any

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka._args import UNSET, Form, java_forms


class _Close:
    """``close()``, ``close(Duration timeout)``, ``close(CloseOptions option)``."""

    @java_forms(Form(), Form("timeout"), Form("option"))
    def close(self, *, timeout: float | None = None,
              option: object | None = None) -> tuple[Any, Any]:
        return timeout, option


class _Acknowledge:
    """``acknowledge(record)``, ``acknowledge(record, type)``,
    ``acknowledge(topic, partition, offset, type)``: ``type`` is required by
    the last form and defaulted by the first, so its default is ``UNSET``."""

    @java_forms(Form("record", "type", defaults={"type": "ACCEPT"}),
                Form("topic", "partition", "offset", "type"))
    def acknowledge(self, *, record: object | None = None,
                    topic: str | None = None, partition: int | None = None,
                    offset: int | None = None, type: str = UNSET) -> Any:  # noqa: A002
        return record, topic, partition, offset, type


class _OffsetAndMetadata:
    """Java's three ``OffsetAndMetadata`` constructors."""

    @java_forms(Form("offset", "leader_epoch", "metadata",
                     defaults={"leader_epoch": None}),
                Form("offset", "metadata", defaults={"metadata": ""}),
                Form("offset"))
    def __init__(self, *, offset: int, leader_epoch: int | None = None,
                 metadata: str = UNSET, _java_form: int = -1) -> None:
        self.values = (offset, leader_epoch, metadata)
        self.form = _java_form


def test_exact_message_lists_every_overload_and_the_given_names() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        _Close().close(timeout=1.0, option=object())
    assert str(exc.value) == (
        "close() takes one of (), (timeout), (option); got (timeout, option)")


def test_every_java_overload_is_accepted() -> None:
    c = _Close()
    assert c.close() == (None, None)
    assert c.close(timeout=2.0) == (2.0, None)
    marker = object()
    assert c.close(option=marker) == (None, marker)


def test_constructor_message_uses_the_class_name() -> None:
    with pytest.raises(IllegalArgumentError) as exc:
        _OffsetAndMetadata(offset=1, leader_epoch=3)
    assert str(exc.value) == (
        "_OffsetAndMetadata() takes one of (offset, leader_epoch, metadata), "
        "(offset, metadata), (offset); got (offset, leader_epoch)")


def test_java_given_defaults_are_filled_for_the_matched_overload() -> None:
    # (offset, metadata) matches (offset, leader_epoch, metadata), whose
    # leader_epoch Java-given default is Optional.empty(); the longest wins.
    o = _OffsetAndMetadata(offset=1, metadata="m")
    assert o.values == (1, None, "m")
    assert o.form == 0
    # (offset) matches (offset, metadata) with metadata="" filled.
    o = _OffsetAndMetadata(offset=1)
    assert o.values == (1, None, "")
    assert o.form == 1


def test_strict_matching_rejects_a_default_another_overload_receives() -> None:
    # Nothing passes `type` to acknowledge(topic, partition, offset, type).
    with pytest.raises(IllegalArgumentError) as exc:
        _Acknowledge().acknowledge(topic="t", partition=0, offset=1)
    assert str(exc.value) == (
        "acknowledge() takes one of (record, type), "
        "(topic, partition, offset, type); got (topic, partition, offset)")


def test_unset_tells_a_given_value_equal_to_the_default_from_not_given() -> None:
    a = _Acknowledge()
    assert a.acknowledge(record="r") == ("r", None, None, None, "ACCEPT")
    assert a.acknowledge(record="r", type="ACCEPT") == ("r", None, None, None, "ACCEPT")
    assert a.acknowledge(topic="t", partition=0, offset=1, type="ACCEPT") == (
        None, "t", 0, 1, "ACCEPT")


def test_a_value_equal_to_a_none_default_is_not_given() -> None:
    # timeout=None is left at its default: close(), not close(timeout).
    assert _Close().close(timeout=None, option=None) == (None, None)


def test_a_constant_default_needs_the_same_type_to_read_as_not_given() -> None:
    class Node:
        @java_forms(Form("id", "is_fenced", defaults={"is_fenced": False}),
                    Form("id", "rack"))
        def __init__(self, *, id: int, is_fenced: bool = False,  # noqa: A002
                     rack: str | None = None) -> None:
            self.fenced = is_fenced

    assert Node(id=1, is_fenced=False).fenced is False
    # 0 == False, but 0 is not left at the bool default: it is given.
    assert Node(id=1, is_fenced=0).fenced == 0  # type: ignore[arg-type]
    with pytest.raises(IllegalArgumentError):
        Node(id=1, is_fenced=0, rack="r")  # type: ignore[arg-type]


def test_most_parameters_win_and_the_earliest_declared_on_a_tie() -> None:
    class Tie:
        @java_forms(Form("a", "b", defaults={"b": 1}),
                    Form("a", "c", defaults={"c": 2}))
        def f(self, *, a: int, b: int | None = None, c: int | None = None,
              _java_form: int = -1) -> Any:
            return _java_form, b, c

    # {a} matches both two-parameter forms; the first declared is used.
    assert Tie().f(a=0) == (0, 1, None)
    assert Tie().f(a=0, c=5) == (1, None, 5)


def test_positional_unknown_and_missing_arguments_are_type_errors() -> None:
    with pytest.raises(TypeError):
        _Close().close(1.0)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        _Close().close(nope=1)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        _OffsetAndMetadata()  # type: ignore[call-arg]


def test_the_form_keyword_is_private() -> None:
    with pytest.raises(TypeError):
        _OffsetAndMetadata(offset=1, _java_form=2)
    assert "_java_form" not in inspect.signature(_OffsetAndMetadata).parameters


def test_a_deprecated_overload_warns_when_called() -> None:
    class Records:
        @java_forms(Form("records", deprecated="Since 4.0. Use (records, next_offsets)."),
                    Form("records", "next_offsets"))
        def __init__(self, *, records: object,
                     next_offsets: object | None = None) -> None:
            pass

    with pytest.warns(DeprecationWarning) as record:
        Records(records={})
    assert str(record[0].message) == (
        "Records(records) is deprecated. Since 4.0. Use (records, next_offsets).")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        Records(records={}, next_offsets={})


def test_a_coroutine_function_stays_a_coroutine_function() -> None:
    class Async:
        @java_forms(Form(), Form("timeout"), Form("option"))
        async def close(self, *, timeout: float | None = None,
                        option: object | None = None) -> float | None:
            return timeout

    assert inspect.iscoroutinefunction(Async.close)
    assert asyncio.run(Async().close(timeout=3.0)) == 3.0
    with pytest.raises(IllegalArgumentError):
        asyncio.run(Async().close(timeout=1.0, option=object()))


def test_the_check_is_built_when_the_class_is_defined() -> None:
    with pytest.raises(TypeError, match="'nope' is not a keyword-only parameter"):
        class Broken:
            @java_forms(Form("nope"))
            def f(self, *, a: int) -> None:
                pass
    assert _Close.close.__java_forms__[1].params == ("timeout",)  # type: ignore[attr-defined]


def test_unset_is_a_singleton_that_pickles_to_itself() -> None:
    assert repr(UNSET) == "UNSET"
    assert copy.copy(UNSET) is UNSET
    assert pickle.loads(pickle.dumps(UNSET)) is UNSET


def test_illegal_argument_error_is_a_runtime_error() -> None:
    with pytest.raises(RuntimeError):
        _Close().close(timeout=1.0, option=object())
