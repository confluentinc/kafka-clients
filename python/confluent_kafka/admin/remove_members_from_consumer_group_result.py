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

"""``RemoveMembersFromConsumerGroupResult``: Java's
``org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult``.

Hand-written: outside every generator pattern (``_reference_set.md``, Coverage), as its
map is keyed by the generated ``LeaveGroupRequestData.MemberIdentity``
(``_MemberIdentity``, ``_internals``), it calls ``MemberToRemove``'s package-private
``toMemberIdentity()`` (``_to_member_identity``) and wraps a member error in
``KafkaException(message, cause)``. Java's constructor is package-private: ``__init__``
raises ``TypeError`` and ``_new`` builds it (CLAUDE.md, Python Binding Conventions, Class
family). The map's values are Java's internal ``Errors``, carried as what
``Errors.exception()`` gives (``None`` for ``NONE``). ``MemberToRemove`` has no Java
``toString``, so a message naming one shows Python's default ``str``, the counterpart of
Java's ``Object.toString``.

The client's result is built by ``_of_client`` instead: the C API hands out no
``Map<MemberIdentity, Errors>`` future, but the core's own result, whose
``memberResult(member)`` and ``all()`` carry Java's outcome and raise Java's
``IllegalArgumentException``, so each call of ``member_result`` and ``all`` returns
the core's future of the same name.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._java import java_str
from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.common.kafka_future import _KafkaFutureImpl
from confluent_kafka.admin._internals import (
    _MemberIdentity,
    _sub_level_error,
    _to_member_identity,
)
from confluent_kafka.common.kafka_error import KafkaError
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.admin.member_to_remove import MemberToRemove
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["RemoveMembersFromConsumerGroupResult"]

_MemberErrors = Mapping[_MemberIdentity, "BaseException | None"]


class RemoveMembersFromConsumerGroupResult(Generic[KindT]):
    """The result of the ``Admin.remove_members_from_consumer_group()`` call.

    The API of this class is evolving, see ``Admin`` for details.

    Java: ``org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupResult``.
    """

    __slots__ = ("_future", "_member_infos", "_kind", "_client_member_result",
                 "_client_all")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "RemoveMembersFromConsumerGroupResult cannot be constructed directly; use the "
            "admin client's remove_members_from_consumer_group()")

    @classmethod
    def _new(cls, *, future: KafkaFuture[dict[_MemberIdentity, BaseException | None]] | AsyncKafkaFuture[dict[_MemberIdentity, BaseException | None]],
             member_infos: Iterable[MemberToRemove],
             kind: type[Sync] | type[Async]) -> RemoveMembersFromConsumerGroupResult[Any]:
        """Java's package-private ``RemoveMembersFromConsumerGroupResult(future,
        memberInfos)``."""
        result = cls.__new__(cls)
        result._future = future
        result._member_infos = set(member_infos)
        result._kind = kind
        result._client_member_result = None
        result._client_all = None
        return result

    @classmethod
    def _of_client(cls, *, member_result: Callable[[MemberToRemove], Any],
                   all: Callable[[], Any],  # noqa: A002 - Java's method name
                   member_infos: Iterable[MemberToRemove],
                   kind: type[Sync] | type[Async]) -> RemoveMembersFromConsumerGroupResult[Any]:
        """The client's result: ``member_result(member)`` and ``all()`` return the
        core's ``memberResult(member)`` and ``all()`` futures."""
        result = cls.__new__(cls)
        result._future = None
        result._member_infos = set(member_infos)
        result._kind = kind
        result._client_member_result = member_result
        result._client_all = all
        return result

    def all(self) -> KafkaFuture[None] | AsyncKafkaFuture[None]:
        """Returns a future which indicates whether the request was 100% success, i.e. no
        either top level or member level error. If not, the first member error shall be
        returned."""
        if self._client_all is not None:
            return self._client_all()  # type: ignore[no-any-return]
        result: _KafkaFutureImpl[None] = _KafkaFutureImpl()

        def action(member_errors: _MemberErrors | None, throwable: BaseException | None
                   ) -> None:
            if throwable is not None:
                result.complete_with_error(throwable)
                return
            assert member_errors is not None
            if self._remove_all():
                for member, exception in member_errors.items():
                    if exception is not None:
                        ex = KafkaError(
                            message="Encounter exception when trying to remove: "
                            + java_str(member), cause=exception)
                        result.complete_with_error(ex)
                        return
            else:
                for member_to_remove in self._member_infos:
                    if self._maybe_complete_exceptionally(
                            member_errors, _to_member_identity(member_to_remove), result):
                        return
            result.complete(None)

        self._future.when_complete(action=action)
        return self._kind._FUTURE._of_impl(result)

    def member_result(self, *, member: MemberToRemove) -> KafkaFuture[None] | AsyncKafkaFuture[None]:
        """Returns the selected member future."""
        if self._client_member_result is not None:
            return self._client_member_result(member)  # type: ignore[no-any-return]
        if self._remove_all():
            raise IllegalArgumentError(
                message="The method: memberResult is not applicable in 'removeAll' mode")
        if member not in self._member_infos:
            raise IllegalArgumentError(
                message="Member " + java_str(member) + " was not included in the original "
                "request")
        result: _KafkaFutureImpl[None] = _KafkaFutureImpl()

        def action(member_errors: _MemberErrors | None, throwable: BaseException | None
                   ) -> None:
            if throwable is not None:
                result.complete_with_error(throwable)
                return
            assert member_errors is not None
            if not self._maybe_complete_exceptionally(
                    member_errors, _to_member_identity(member), result):
                result.complete(None)

        self._future.when_complete(action=action)
        return self._kind._FUTURE._of_impl(result)

    def _maybe_complete_exceptionally(self, member_errors: _MemberErrors,
                                      member: _MemberIdentity, result: KafkaFuture[None] | AsyncKafkaFuture[None]) -> bool:
        exception = _sub_level_error(
            member_errors, member,
            "Member \"" + java_str(member) + "\" was not included in the removal response")
        if exception is not None:
            result.complete_with_error(exception)
            return True
        return False

    def _remove_all(self) -> bool:
        return not self._member_infos
