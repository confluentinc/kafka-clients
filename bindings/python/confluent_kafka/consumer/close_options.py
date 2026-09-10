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

"""``CloseOptions`` — the option object accepted by ``Consumer.close()``.

Translated from ``org.apache.kafka.clients.consumer.CloseOptions`` (Apache Kafka
4.3.1). Java's builder shape is kept (rule 3.14): a private constructor (so
``CloseOptions()`` raises ``TypeError``), static ``timeout`` /
``group_membership_operation`` factories, fluent ``with_timeout`` /
``with_group_membership_operation`` returning ``Self``, and the getters
``group_membership_operation()`` / ``timeout()``. The builder methods take their
one argument positionally (rule 3.2(b)).

Java overloads the name ``timeout`` — a static factory ``timeout(Duration)`` and
an instance getter ``timeout()``. Python cannot bind one name to both a
staticmethod and an instance method, so a small descriptor (``_StaticOrInstance``)
dispatches: ``CloseOptions.timeout(d)`` builds a new instance, while
``opts.timeout()`` reads. This is the only faithful way to keep both Java forms
under the single Java name.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from typing import Any

from confluent_kafka import Duration


class _StaticOrInstance:
    """A descriptor dispatching ``Name.method(x)`` (static factory) vs
    ``instance.method()`` (getter) for Java's overloaded ``timeout`` name."""

    def __init__(self, static_fn: Callable[..., Any],
                 instance_fn: Callable[..., Any]) -> None:
        self._static_fn = static_fn
        self._instance_fn = instance_fn

    def __get__(self, obj: Any, objtype: Any = None) -> Callable[..., Any]:
        if obj is None:
            return self._static_fn

        def _bound() -> Any:
            return self._instance_fn(obj)

        return _bound


class CloseOptions:
    """The option object accepted by ``Consumer.close()``.

    Java: ``org.apache.kafka.clients.consumer.CloseOptions``.
    """

    class GroupMembershipOperation(Enum):
        """The group membership operation to apply upon leaving the group.

        Java nested enum ``CloseOptions.GroupMembershipOperation``:

        - ``LEAVE_GROUP``: the consumer leaves the group.
        - ``REMAIN_IN_GROUP``: the consumer remains in the group.
        - ``DEFAULT``: static members remain; dynamic members leave.
        """

        LEAVE_GROUP = "LEAVE_GROUP"
        REMAIN_IN_GROUP = "REMAIN_IN_GROUP"
        DEFAULT = "DEFAULT"

    __slots__ = ("_operation", "_timeout")

    # A private sentinel so only the internal ``_create`` factory may build an
    # instance; the public ``__init__`` always raises (Java's ctor is private).
    _TOKEN: Any = object()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if not (len(args) == 1 and args[0] is CloseOptions._TOKEN and not kwargs):
            raise TypeError(
                "CloseOptions cannot be constructed directly; use "
                "CloseOptions.timeout(...) or "
                "CloseOptions.group_membership_operation(...)"
            )
        self._operation = CloseOptions.GroupMembershipOperation.DEFAULT
        self._timeout: Duration | None = None

    @classmethod
    def _create(cls) -> CloseOptions:
        return cls(cls._TOKEN)

    @staticmethod
    def _timeout_factory(timeout: Duration | None) -> CloseOptions:
        """Java ``static CloseOptions timeout(Duration)`` — a new instance with
        the given timeout (``None`` leaves the default empty timeout)."""
        return CloseOptions._create().with_timeout(timeout)

    @staticmethod
    def group_membership_operation(
        operation: CloseOptions.GroupMembershipOperation,
    ) -> CloseOptions:
        """Java ``static CloseOptions groupMembershipOperation(...)``."""
        return CloseOptions._create().with_group_membership_operation(operation)

    def with_timeout(self, timeout: Duration | None) -> CloseOptions:
        """Java fluent ``withTimeout(Duration)`` — ``None`` uses the default."""
        self._timeout = timeout
        return self

    def with_group_membership_operation(
        self, operation: CloseOptions.GroupMembershipOperation,
    ) -> CloseOptions:
        """Java fluent ``withGroupMembershipOperation(...)``."""
        if operation is None:
            raise TypeError("operation should not be null")
        self._operation = operation
        return self

    def _group_membership_operation_getter(
        self,
    ) -> CloseOptions.GroupMembershipOperation:
        return self._operation

    def _timeout_getter(self) -> Duration | None:
        return self._timeout

    # Java overloads the name ``groupMembershipOperation`` too: a static factory
    # and an instance getter. Same dispatch as ``timeout``.
    group_membership_operation = _StaticOrInstance(  # type: ignore[assignment]
        group_membership_operation.__func__,  # type: ignore[attr-defined]
        _group_membership_operation_getter,
    )
    timeout = _StaticOrInstance(
        _timeout_factory,
        _timeout_getter,
    )

    def __repr__(self) -> str:
        return (f"CloseOptions(timeout={self._timeout!r}, "
                f"operation={self._operation})")
