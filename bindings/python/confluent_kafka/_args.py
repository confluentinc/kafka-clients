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

"""Argument-combination validation helpers.

Java resolves overloads at compile time; a Python method that collapses a Java
overload set (rule 3.3) accepts the union of every overload's parameters and
must reject, at runtime, any combination that matches no Java overload
(rule 3.5). These three helpers are the single, uniform mechanism every
collapsed method calls first, so two Actors cannot diverge on the message form.

A candidate is "given" when its value is not ``None``. On a mismatch the
helpers raise :class:`~confluent_kafka.IllegalArgumentError` (the JDK analog of
Java's ``IllegalArgumentException``), naming every alternative.
"""

from __future__ import annotations

from . import IllegalArgumentError

__all__ = ["exactly_one", "at_most_one", "all_or_none"]


def _given(candidates: dict[str, object]) -> list[str]:
    """The names of the candidates whose value is not ``None``, in Java's
    declared order (the order the keyword arguments were passed in)."""
    return [name for name, value in candidates.items() if value is not None]


def _format_given(given: list[str]) -> str:
    """Render the given-candidate list for a message, ``'none'`` when empty."""
    return ", ".join(given) if given else "none"


def exactly_one(method: str, **candidates: object) -> str:
    """Require that exactly one candidate is given; return its name.

    Mirrors a Java overload set where the alternatives are mutually exclusive
    and one is mandatory (e.g. ``subscribe`` takes ``topics`` xor
    ``subscription_pattern``). Raises ``IllegalArgumentError`` naming every
    alternative when zero or more than one is given.
    """
    given = _given(candidates)
    if len(given) != 1:
        alternatives = ", ".join(candidates)
        raise IllegalArgumentError(
            f"{method}() takes exactly one of {alternatives}; "
            f"got {_format_given(given)}"
        )
    return given[0]


def at_most_one(method: str, **candidates: object) -> str | None:
    """Require that at most one candidate is given; return its name or ``None``.

    Mirrors a Java overload set where the alternatives are mutually exclusive
    but all optional. Raises ``IllegalArgumentError`` naming every alternative
    when more than one is given.
    """
    given = _given(candidates)
    if len(given) > 1:
        alternatives = ", ".join(candidates)
        raise IllegalArgumentError(
            f"{method}() takes at most one of {alternatives}; "
            f"got {_format_given(given)}"
        )
    return given[0] if given else None


def all_or_none(method: str, **group: object) -> bool:
    """Require that the whole group is given, or none of it; return whether it
    is given.

    Mirrors a Java overload where a set of parameters must appear together.
    Raises ``IllegalArgumentError`` naming the group when it is given partially.
    """
    given = _given(group)
    if given and len(given) != len(group):
        members = ", ".join(group)
        raise IllegalArgumentError(
            f"{method}() needs all of {members} together; "
            f"got {_format_given(given)}"
        )
    return bool(given)
