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

"""Private translations of the Java internals the admin result classes use.

- ``_CoordinatorKey``: ``org.apache.kafka.clients.admin.internals.CoordinatorKey``, with
  ``FindCoordinatorRequest.CoordinatorType`` as ``_CoordinatorKey.CoordinatorType``.
- ``_sub_level_error``: ``KafkaAdminClient.getSubLevelError`` (package-private static).
- ``_errors_exception`` / ``_errors_for_code``: ``Errors.exception(message)`` and
  ``Errors.forCode(code).exception(message)``.
- ``_ProducerIdAndEpoch``: ``org.apache.kafka.common.utils.ProducerIdAndEpoch``.
- ``_MemberIdentity``: the generated ``LeaveGroupRequestData.MemberIdentity``;
  ``_to_member_identity``: ``MemberToRemove``'s package-private ``toMemberIdentity()``.
- ``_DescribeUserScramCredentialsResponseData``: the generated
  ``DescribeUserScramCredentialsResponseData``, with its nested
  ``DescribeUserScramCredentialsResult`` and ``CredentialInfo``.
- ``_ConsumerProtocol``: the one constant of
  ``org.apache.kafka.clients.consumer.internals.ConsumerProtocol`` the mock uses.

Java's internal enum ``org.apache.kafka.common.protocol.Errors`` is carried as what its
``exception()`` gives: the error the client builds from the FFI's per-key error
(``_errors.from_ffi_error`` / ``class_for_ffi_id``, the error registry), ``None`` for
``Errors.NONE``. So ``errors.get(key).exception()`` is ``errors[key]``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import Enum
from typing import TYPE_CHECKING, ClassVar, TypeVar

from confluent_kafka._java import java_str
from confluent_kafka.illegal_argument_error import IllegalArgumentError

if TYPE_CHECKING:
    from confluent_kafka.admin.member_to_remove import MemberToRemove

__all__ = [
    "_ConsumerProtocol",
    "_CoordinatorKey",
    "_DescribeUserScramCredentialsResponseData",
    "_MemberIdentity",
    "_ProducerIdAndEpoch",
    "_errors_exception",
    "_errors_for_code",
    "_sub_level_error",
    "_to_member_identity",
]

_K = TypeVar("_K")


class _ConsumerProtocol:
    """Java's ``ConsumerProtocol``, for ``MockAdminClient.listGroups``: only its
    ``PROTOCOL_TYPE`` (``ConsumerProtocol.java:45``)."""

    PROTOCOL_TYPE: ClassVar[str] = "consumer"


class _CoordinatorKey:
    """Java's ``CoordinatorKey``: a coordinator lookup key, a group or transactional id
    and its coordinator type."""

    class CoordinatorType(Enum):
        """Java's ``FindCoordinatorRequest.CoordinatorType`` (ids 0, 1, 2)."""

        GROUP = "GROUP"
        TRANSACTION = "TRANSACTION"
        SHARE = "SHARE"

        def __str__(self) -> str:
            return self.name

    __slots__ = ("id_value", "type")

    def __init__(self, *, type: _CoordinatorKey.CoordinatorType,  # noqa: A002 - Java's name
                 id_value: str) -> None:
        """Java's private ``CoordinatorKey(type, idValue)``; Java's ``public final``
        fields are attributes, as the class is private."""
        self.id_value = id_value
        self.type = type

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, _CoordinatorKey)
        return self.id_value == other.id_value and self.type == other.type

    def __hash__(self) -> int:
        return hash((self.id_value, self.type))

    def __str__(self) -> str:
        return ("CoordinatorKey(" + "idValue='" + java_str(self.id_value) + "'"
                + ", type=" + java_str(self.type) + ")")

    @staticmethod
    def by_group_id(*, group_id: str) -> _CoordinatorKey:
        return _CoordinatorKey(type=_CoordinatorKey.CoordinatorType.GROUP, id_value=group_id)

    @staticmethod
    def by_transactional_id(*, transactional_id: str) -> _CoordinatorKey:
        return _CoordinatorKey(type=_CoordinatorKey.CoordinatorType.TRANSACTION,
                               id_value=transactional_id)


def _sub_level_error(sub_level_errors: Mapping[_K, BaseException | None], sub_key: _K,
                     key_not_found_msg: str) -> BaseException | None:
    """Java's ``KafkaAdminClient.getSubLevelError``: ``IllegalArgumentException`` when
    the key is missing, else the key's ``Errors.exception()`` (``None`` for ``NONE``)."""
    if sub_key not in sub_level_errors:
        return IllegalArgumentError(message=key_not_found_msg)
    return sub_level_errors[sub_key]


def _errors_exception(error: BaseException | None, message: str | None) -> BaseException | None:
    """Java's ``Errors.exception(message)`` on the ``Errors`` carried as ``error``
    (what its ``exception()`` gives, ``None`` for ``NONE``): ``None`` for ``NONE``,
    ``error`` itself for a ``null`` message, else a new error of the same class with
    ``message``, as the ``Errors`` entry's builder makes it."""
    if error is None:
        return None
    if message is None:
        return error
    from confluent_kafka._errors import construct

    return construct(type(error), message, None)


def _errors_for_code(code: int, message: str | None) -> BaseException | None:
    """Java's ``Errors.forCode(code).exception(message)``: ``None`` for ``NONE``
    (``0``); else the error class of the code built with ``message``, or, for a
    ``null`` message, with the code's ``Errors`` default message (Java returns the
    cached instance built with it). An unknown code is ``UNKNOWN_SERVER_ERROR``, as
    ``Errors.forCode`` makes it. The FFI ids of the API errors are their wire codes."""
    if code == 0:
        return None
    from confluent_kafka._error_registry import DEFAULT_MESSAGES
    from confluent_kafka._errors import class_for_ffi_id, construct
    from confluent_kafka.common.errors import UnknownServerError
    from confluent_kafka.common.kafka_error import KafkaError

    cls = class_for_ffi_id(code)
    if cls is KafkaError or code not in DEFAULT_MESSAGES:
        cls = UnknownServerError
        code = -1
    return construct(cls, DEFAULT_MESSAGES[code] if message is None else message, None)


class _ProducerIdAndEpoch:
    """Java's ``org.apache.kafka.common.utils.ProducerIdAndEpoch``; Java's ``public
    final`` fields are attributes, as the class is private."""

    #: Java's ``NONE``: ``(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH)``.
    NONE: ClassVar[_ProducerIdAndEpoch]

    __slots__ = ("producer_id", "epoch")

    def __init__(self, *, producer_id: int, epoch: int) -> None:
        self.producer_id = producer_id
        self.epoch = epoch

    def is_valid(self) -> bool:
        # RecordBatch.NO_PRODUCER_ID < producerId
        return -1 < self.producer_id

    def __str__(self) -> str:
        return "(producerId=" + str(self.producer_id) + ", epoch=" + str(self.epoch) + ")"

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, _ProducerIdAndEpoch)
        return self.producer_id == other.producer_id and self.epoch == other.epoch

    def __hash__(self) -> int:
        return hash((self.producer_id, self.epoch))


_ProducerIdAndEpoch.NONE = _ProducerIdAndEpoch(producer_id=-1, epoch=-1)

# Java's ``JoinGroupRequest.UNKNOWN_MEMBER_ID``.
_UNKNOWN_MEMBER_ID = ""


def _str_field(value: str | None) -> str:
    """A generated message's ``toString`` of a string field."""
    return "None" if value is None else "'" + value + "'"


class _MemberIdentity:
    """Java's generated ``LeaveGroupRequestData.MemberIdentity``: the spec's defaults
    (``MemberId`` ``""``, ``GroupInstanceId`` and ``Reason`` ``null``), its getters,
    and its generated ``equals`` / ``hashCode`` / ``toString``."""

    __slots__ = ("_member_id", "_group_instance_id", "_reason")

    def __init__(self, *, member_id: str = "", group_instance_id: str | None = None,
                 reason: str | None = None) -> None:
        self._member_id = member_id
        self._group_instance_id = group_instance_id
        self._reason = reason

    def member_id(self) -> str:
        return self._member_id

    def group_instance_id(self) -> str | None:
        return self._group_instance_id

    def reason(self) -> str | None:
        return self._reason

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, _MemberIdentity)
        return (self._member_id == other._member_id
                and self._group_instance_id == other._group_instance_id
                and self._reason == other._reason)

    def __hash__(self) -> int:
        return hash((self._member_id, self._group_instance_id, self._reason))

    def __str__(self) -> str:
        return ("MemberIdentity(" + "memberId=" + _str_field(self._member_id)
                + ", groupInstanceId=" + _str_field(self._group_instance_id)
                + ", reason=" + _str_field(self._reason) + ")")


def _to_member_identity(member: MemberToRemove) -> _MemberIdentity:
    """Java's package-private ``MemberToRemove.toMemberIdentity()``."""
    return _MemberIdentity(group_instance_id=member.group_instance_id(),
                           member_id=_UNKNOWN_MEMBER_ID)


class _DescribeUserScramCredentialsResponseData:
    """Java's generated ``DescribeUserScramCredentialsResponseData``: its fields with
    the spec's defaults (a nullable string with no ``"default"`` is ``""``) and
    getters."""

    class CredentialInfo:
        """``DescribeUserScramCredentialsResponseData.CredentialInfo``."""

        __slots__ = ("_mechanism", "_iterations")

        def __init__(self, *, mechanism: int = 0, iterations: int = 0) -> None:
            self._mechanism = mechanism
            self._iterations = iterations

        def mechanism(self) -> int:
            return self._mechanism

        def iterations(self) -> int:
            return self._iterations

    class DescribeUserScramCredentialsResult:
        """``DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult``."""

        __slots__ = ("_user", "_error_code", "_error_message", "_credential_infos")

        def __init__(self, *, user: str = "", error_code: int = 0,
                     error_message: str | None = "",
                     credential_infos: Iterable[
                         _DescribeUserScramCredentialsResponseData.CredentialInfo] = ()
                     ) -> None:
            self._user = user
            self._error_code = error_code
            self._error_message = error_message
            self._credential_infos = list(credential_infos)

        def user(self) -> str:
            return self._user

        def error_code(self) -> int:
            return self._error_code

        def error_message(self) -> str | None:
            return self._error_message

        def credential_infos(self) -> list[_DescribeUserScramCredentialsResponseData.CredentialInfo]:
            return self._credential_infos

    __slots__ = ("_throttle_time_ms", "_error_code", "_error_message", "_results")

    def __init__(self, *, throttle_time_ms: int = 0, error_code: int = 0,
                 error_message: str | None = "",
                 results: Iterable[
                     _DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult
                 ] = ()) -> None:
        self._throttle_time_ms = throttle_time_ms
        self._error_code = error_code
        self._error_message = error_message
        self._results = list(results)

    def throttle_time_ms(self) -> int:
        return self._throttle_time_ms

    def error_code(self) -> int:
        return self._error_code

    def error_message(self) -> str | None:
        return self._error_message

    def results(self) -> list[_DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult]:
        return self._results
