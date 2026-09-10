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

"""Tests for the argument-combination helpers (`confluent_kafka._args`, rule 3.5)."""

from __future__ import annotations

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka._args import all_or_none, at_most_one, exactly_one


class TestExactlyOne:
    def test_one_given_returns_its_name(self) -> None:
        assert exactly_one("subscribe", topics=["t"], subscription_pattern=None) == "topics"

    def test_none_given_raises_with_exact_message(self) -> None:
        with pytest.raises(IllegalArgumentError) as exc:
            exactly_one("subscribe", topics=None, subscription_pattern=None)
        assert (
            str(exc.value)
            == "subscribe() takes exactly one of topics, subscription_pattern; got none"
        )

    def test_more_than_one_given_raises_naming_the_given(self) -> None:
        with pytest.raises(IllegalArgumentError) as exc:
            exactly_one("subscribe", topics=["t"], subscription_pattern="p")
        assert (
            str(exc.value)
            == "subscribe() takes exactly one of topics, subscription_pattern; "
            "got topics, subscription_pattern"
        )


class TestAtMostOne:
    def test_none_given_returns_none(self) -> None:
        assert at_most_one("seek", offset=None, offset_and_metadata=None) is None

    def test_one_given_returns_its_name(self) -> None:
        assert at_most_one("seek", offset=5, offset_and_metadata=None) == "offset"

    def test_more_than_one_raises_with_exact_message(self) -> None:
        with pytest.raises(IllegalArgumentError) as exc:
            at_most_one("seek", offset=5, offset_and_metadata=object())
        assert (
            str(exc.value)
            == "seek() takes at most one of offset, offset_and_metadata; "
            "got offset, offset_and_metadata"
        )


class TestAllOrNone:
    def test_all_given_returns_true(self) -> None:
        assert all_or_none("m", a=1, b=2, c=3) is True

    def test_none_given_returns_false(self) -> None:
        assert all_or_none("m", a=None, b=None, c=None) is False

    def test_partial_raises_with_exact_message(self) -> None:
        with pytest.raises(IllegalArgumentError) as exc:
            all_or_none("m", a=1, b=None, c=3)
        assert str(exc.value) == "m() needs all of a, b, c together; got a, c"


def test_helpers_raise_illegal_argument_error_which_is_runtime_error() -> None:
    # IllegalArgumentError is a JDK analog subclassing RuntimeError (spec §5.5).
    assert issubclass(IllegalArgumentError, RuntimeError)
    with pytest.raises(RuntimeError):
        exactly_one("m", a=None)
