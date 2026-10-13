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

"""``CloseOptions``: Java's ``org.apache.kafka.clients.consumer.CloseOptions``.

Java's constructor is private, so ``CloseOptions()`` raises ``TypeError`` naming
the static factories (CLAUDE.md, Python Binding Conventions, Class family). In a
class with fluent setters every one-argument method takes its argument
positionally (Signatures). Java's static factory ``timeout(Duration)`` and
instance getter ``timeout()`` share one name, and so do
``groupMembershipOperation(...)`` and ``groupMembershipOperation()``: each is one
attribute, the factory on the class and the getter on an instance. A returned
``Duration`` is a ``float`` of seconds.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from enum import Enum
from typing import Any, Generic, TypeVar, overload

from confluent_kafka import Duration
from confluent_kafka.null_pointer_error import NullPointerError

__all__ = ["CloseOptions"]

_F = TypeVar("_F", bound=Callable[..., Any])
_G = TypeVar("_G", bound=Callable[..., Any])


class _FactoryOrGetter(Generic[_F, _G]):
    """One name that is a static factory on the class and a getter on an
    instance."""

    def __init__(self, factory: _F, getter: Callable[[Any], Any]) -> None:
        self._factory = factory
        self._getter = getter

    @overload
    def __get__(self, obj: None, owner: type) -> _F: ...
    @overload
    def __get__(self, obj: object, owner: type | None = None) -> _G: ...

    def __get__(self, obj: object | None, owner: type | None = None) -> Any:
        if obj is None:
            return self._factory
        getter = self._getter

        def bound() -> Any:
            return getter(obj)

        return bound


class CloseOptions:
    """The options of ``Consumer.close()``: the group membership operation to
    apply upon leaving the group, and the maximum amount of time to wait for
    the close process to complete (the consumer's default when not set).

    Java: ``org.apache.kafka.clients.consumer.CloseOptions``.
    """

    class GroupMembershipOperation(Enum):
        """Enum to specify the group membership operation upon leaving group.

        - ``LEAVE_GROUP``: means the consumer will leave the group.
        - ``REMAIN_IN_GROUP``: means the consumer will remain in the group.
        - ``DEFAULT``: applies the default behavior: static members remain in
          the group, dynamic members leave it.
        """

        LEAVE_GROUP = "LEAVE_GROUP"
        REMAIN_IN_GROUP = "REMAIN_IN_GROUP"
        DEFAULT = "DEFAULT"

    __slots__ = ("_operation", "_timeout")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "CloseOptions cannot be constructed directly; use "
            "CloseOptions.timeout(...) or CloseOptions.group_membership_operation(...)")

    @classmethod
    def _new(cls) -> CloseOptions:
        """Java's private ``CloseOptions()``: the DEFAULT operation and no
        timeout."""
        options = cls.__new__(cls)
        options._operation = CloseOptions.GroupMembershipOperation.DEFAULT
        options._timeout = None
        return options

    @staticmethod
    def _timeout_factory(timeout: Duration | None, /) -> CloseOptions:
        """A new ``CloseOptions`` with a custom timeout: the maximum time to
        wait for the consumer to close."""
        return CloseOptions._new().with_timeout(timeout)

    @staticmethod
    def _group_membership_operation_factory(
            operation: CloseOptions.GroupMembershipOperation, /) -> CloseOptions:
        """A new ``CloseOptions`` with the specified group membership
        operation: one of ``LEAVE_GROUP``, ``REMAIN_IN_GROUP`` or ``DEFAULT``."""
        return CloseOptions._new().with_group_membership_operation(operation)

    #: ``CloseOptions.timeout(timeout)`` (static factory) /
    #: ``options.timeout()`` (the timeout in seconds, ``None`` when not set).
    timeout: _FactoryOrGetter[Callable[[Duration | None], CloseOptions],
                              Callable[[], float | None]]
    #: ``CloseOptions.group_membership_operation(operation)`` (static factory) /
    #: ``options.group_membership_operation()`` (the operation).
    group_membership_operation: _FactoryOrGetter[
        Callable[[CloseOptions.GroupMembershipOperation], CloseOptions],
        Callable[[], CloseOptions.GroupMembershipOperation]]

    def with_timeout(self, timeout: Duration | None, /) -> CloseOptions:
        """Fluent method to set the timeout for the close process: the maximum
        time to wait for the consumer to close. If ``None``, the default
        timeout will be used."""
        self._timeout = timeout
        return self

    def with_group_membership_operation(
            self, operation: CloseOptions.GroupMembershipOperation, /) -> CloseOptions:
        """Fluent method to set the group membership operation upon shutdown."""
        if operation is None:
            raise NullPointerError(message="operation should not be null")
        self._operation = operation
        return self

    def _group_membership_operation_getter(self) -> CloseOptions.GroupMembershipOperation:
        return self._operation

    def _timeout_seconds(self) -> float | None:
        timeout = self._timeout
        if timeout is None:
            return None
        if isinstance(timeout, timedelta):
            return timeout.total_seconds()
        return float(timeout)

    def _timeout_getter(self) -> Duration | None:
        """The timeout as given (a ``Duration``), for the consumer's close."""
        return self._timeout


CloseOptions.timeout = _FactoryOrGetter(
    CloseOptions._timeout_factory, CloseOptions._timeout_seconds)
CloseOptions.group_membership_operation = _FactoryOrGetter(
    CloseOptions._group_membership_operation_factory,
    CloseOptions._group_membership_operation_getter)
