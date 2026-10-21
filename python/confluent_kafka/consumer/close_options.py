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

Java builds it with setters, so the constructor also takes one keyword per setter
field, ``operation`` and ``timeout`` in Java's field order (CLAUDE.md, Python
Binding Conventions, Signatures): each keyword given, ``None`` included, calls its
setter (``with_group_membership_operation``, ``with_timeout``); one left out leaves
the field at Java's starting value, so ``CloseOptions()`` is Java's private
``CloseOptions()``. Java's static factory ``timeout(Duration)`` and instance getter
``timeout()`` share one name, and so do ``groupMembershipOperation(...)`` and
``groupMembershipOperation()``: each is one attribute, the factory on the class and
the getter on an instance. The factories and the setters take their one argument by
keyword, like every other method. A returned ``Duration`` is a ``float`` of seconds.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, overload

from confluent_kafka import Duration
from confluent_kafka._args import MISSING

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


class _TimeoutFactory(Protocol):
    """``CloseOptions.timeout(timeout=...)``, Java's static ``timeout(Duration)``."""

    def __call__(self, *, timeout: Duration | None) -> CloseOptions: ...


class _TimeoutGetter(Protocol):
    """``options.timeout()``, Java's ``Optional<Duration> timeout()``."""

    def __call__(self) -> float | None: ...


class _OperationFactory(Protocol):
    """``CloseOptions.group_membership_operation(operation=...)``, Java's static
    ``groupMembershipOperation(GroupMembershipOperation)``."""

    def __call__(self, *, operation: CloseOptions.GroupMembershipOperation) -> CloseOptions: ...


class _OperationGetter(Protocol):
    """``options.group_membership_operation()``, Java's
    ``groupMembershipOperation()``."""

    def __call__(self) -> CloseOptions.GroupMembershipOperation: ...


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

    if TYPE_CHECKING:
        # The stub shows the fields' Java starting values (Signatures, classes Java
        # builds with setters): operation = DEFAULT, timeout = Optional.empty().
        def __init__(self, *,
                     operation: CloseOptions.GroupMembershipOperation = GroupMembershipOperation.DEFAULT,
                     timeout: Duration | None = None) -> None: ...
    else:
        def __init__(self, *, operation: CloseOptions.GroupMembershipOperation = MISSING,
                     timeout: Duration | None = MISSING) -> None:
            """Java's private ``CloseOptions()``, the ``DEFAULT`` operation and no
            timeout, then each keyword given calls its setter: ``operation`` the
            group membership operation upon shutdown (``None`` raises
            ``TypeError``, as the setter does), ``timeout`` the maximum time
            to wait for the consumer to close (``None`` for the default timeout)."""
            self._operation = CloseOptions.GroupMembershipOperation.DEFAULT
            self._timeout: Duration | None = None
            if operation is not MISSING:
                self.with_group_membership_operation(operation=operation)
            if timeout is not MISSING:
                self.with_timeout(timeout=timeout)

    @staticmethod
    def _timeout_factory(*, timeout: Duration | None) -> CloseOptions:
        """A new ``CloseOptions`` with a custom timeout: the maximum time to
        wait for the consumer to close."""
        return CloseOptions().with_timeout(timeout=timeout)

    @staticmethod
    def _group_membership_operation_factory(
            *, operation: CloseOptions.GroupMembershipOperation) -> CloseOptions:
        """A new ``CloseOptions`` with the specified group membership
        operation: one of ``LEAVE_GROUP``, ``REMAIN_IN_GROUP`` or ``DEFAULT``."""
        return CloseOptions().with_group_membership_operation(operation=operation)

    #: ``CloseOptions.timeout(timeout=...)`` (static factory) /
    #: ``options.timeout()`` (the timeout in seconds, ``None`` when not set).
    timeout: _FactoryOrGetter[_TimeoutFactory, _TimeoutGetter]
    #: ``CloseOptions.group_membership_operation(operation=...)`` (static factory) /
    #: ``options.group_membership_operation()`` (the operation).
    group_membership_operation: _FactoryOrGetter[_OperationFactory, _OperationGetter]

    def with_timeout(self, *, timeout: Duration | None) -> CloseOptions:
        """Fluent method to set the timeout for the close process: the maximum
        time to wait for the consumer to close. If ``None``, the default
        timeout will be used."""
        self._timeout = timeout
        return self

    def with_group_membership_operation(
            self, *, operation: CloseOptions.GroupMembershipOperation) -> CloseOptions:
        """Fluent method to set the group membership operation upon shutdown."""
        if operation is None:
            raise TypeError("operation must not be None")
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
