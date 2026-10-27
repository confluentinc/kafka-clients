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

"""``DescribeUserScramCredentialsResult``: Java's
``org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult``.

Hand-written: outside every generator pattern (``_reference_set.md``, Coverage), as it
holds the generated ``DescribeUserScramCredentialsResponseData`` (the private helper
``_DescribeUserScramCredentialsResponseData``, ``_internals``) and builds errors with
``Errors.forCode(code).exception(message)`` (``_errors_for_code``). Java's constructor is
package-private: ``__init__`` raises ``TypeError`` and ``_new`` builds it (CLAUDE.md,
Python Binding Conventions, Class family); its ``Objects.requireNonNull(dataFuture)``
raises ``TypeError``. The private static ``getScramCredentialInfosFor`` is
``_get_scram_credential_infos_for``. Java collects the users into a ``List``, so
``users()`` is a ``list[str]``.

The admin client builds it with the private ``_of_client``: its ``all()``, ``users()``
and ``description(user_name)`` are then the core's own futures, each the core's
translation of Java's dependant of the response data, as the C result hands out no
response data. ``all()`` is not composed from ``users()`` and the per-user futures:
Java's ``all()`` also maps a requested user the broker reports as not found
(``RESOURCE_NOT_FOUND``) to an empty description, while ``users()`` leaves such a user
out.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._java import java_str
from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.admin._internals import (
    _DescribeUserScramCredentialsResponseData,
    _errors_for_code,
)
from confluent_kafka.common.kafka_future import _KafkaFutureImpl

if TYPE_CHECKING:
    from confluent_kafka.admin.scram_credential_info import ScramCredentialInfo
    from confluent_kafka.admin.user_scram_credentials_description import (
        UserScramCredentialsDescription,
    )
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DescribeUserScramCredentialsResult"]

# Java's ``Errors.NONE.code()`` and ``Errors.RESOURCE_NOT_FOUND.code()``.
_NONE_CODE = 0
_RESOURCE_NOT_FOUND_CODE = 91

_UserResult = _DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult


class DescribeUserScramCredentialsResult(Generic[KindT]):
    """The result of the ``Admin.describe_user_scram_credentials()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult``.
    """

    __slots__ = ("_client", "_data_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeUserScramCredentialsResult cannot be constructed directly; use the "
            "admin client's describe_user_scram_credentials()")

    @classmethod
    def _new(cls, *, data_future: KafkaFuture[_DescribeUserScramCredentialsResponseData] | AsyncKafkaFuture[_DescribeUserScramCredentialsResponseData],
             kind: type[Sync] | type[Async]) -> DescribeUserScramCredentialsResult[Any]:
        """Java's package-private ``DescribeUserScramCredentialsResult(dataFuture)``:
        ``data_future`` is the future indicating response data from the call."""
        if data_future is None:
            raise TypeError("data_future must not be None")
        result = cls.__new__(cls)
        result._data_future = data_future
        result._kind = kind
        result._client = None
        return result

    @classmethod
    def _of_client(cls, *, future: Callable[[str, str | None], Any],
                   kind: type[Sync] | type[Async]) -> DescribeUserScramCredentialsResult[Any]:
        """The admin client's result: ``future(accessor, user_name)`` is the core's own
        future of ``all()``, ``users()`` or ``description(user_name)``."""
        result = cls.__new__(cls)
        result._data_future = None
        result._kind = kind
        result._client = future
        return result

    def all(self) -> KafkaFuture[dict[str, UserScramCredentialsDescription]] | AsyncKafkaFuture[dict[str, UserScramCredentialsDescription]]:
        """A future for the results of all described users with map keys (one per user)
        being consistent with the contents of the list returned by ``users()``. The future
        will complete successfully only if all such user descriptions complete
        successfully."""
        if self._client is not None:
            return self._client("all", None)  # type: ignore[no-any-return]
        from confluent_kafka.admin.user_scram_credentials_description import (
            UserScramCredentialsDescription,
        )

        retval: _KafkaFutureImpl[dict[str, UserScramCredentialsDescription]] = _KafkaFutureImpl()

        def action(data: _DescribeUserScramCredentialsResponseData | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                retval.complete_with_error(throwable)
                return
            assert data is not None
            # Check to make sure every individual described user succeeded. Note that a
            # successfully described user is one that appears with *either* a NONE error
            # code or a RESOURCE_NOT_FOUND error code. The RESOURCE_NOT_FOUND means the
            # client explicitly requested a describe of that particular user but it could
            # not be described because it does not exist; such a user will not appear as
            # a key in the returned map.
            first_failed_describe = next(
                (result for result in data.results()
                 if result.error_code() != _NONE_CODE
                 and result.error_code() != _RESOURCE_NOT_FOUND_CODE), None)
            if first_failed_describe is not None:
                error = _errors_for_code(first_failed_describe.error_code(),
                                         first_failed_describe.error_message())
                assert error is not None
                retval.complete_with_error(error)
            else:
                retval_map: dict[str, UserScramCredentialsDescription] = {}
                for user_result in data.results():
                    retval_map[user_result.user()] = UserScramCredentialsDescription(
                        name=user_result.user(),
                        credential_infos=self._get_scram_credential_infos_for(user_result))
                retval.complete(retval_map)

        assert self._data_future is not None
        self._data_future.when_complete(action=action)
        return self._kind._FUTURE._of_impl(retval)

    def users(self) -> KafkaFuture[list[str]] | AsyncKafkaFuture[list[str]]:
        """A future indicating the distinct users that meet the request criteria and that
        have at least one credential. The future will not complete successfully if the
        user is not authorized to perform the describe operation; otherwise, it will
        complete successfully as long as the list of users with credentials can be
        successfully determined within some hard-coded timeout period. Note that the
        returned list will not include users that do not exist/have no credentials: a
        request to describe an explicit list of users, none of which existed/had a
        credential, will result in a future that returns an empty list being returned
        here. A returned list will include users that have a credential but that could
        not be described."""
        if self._client is not None:
            return self._client("users", None)  # type: ignore[no-any-return]
        retval: _KafkaFutureImpl[list[str]] = _KafkaFutureImpl()

        def action(data: _DescribeUserScramCredentialsResponseData | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                retval.complete_with_error(throwable)
            else:
                assert data is not None
                retval.complete([result.user() for result in data.results()
                                   if result.error_code() != _RESOURCE_NOT_FOUND_CODE])

        assert self._data_future is not None
        self._data_future.when_complete(action=action)
        return self._kind._FUTURE._of_impl(retval)

    def description(self, *, user_name: str) -> KafkaFuture[UserScramCredentialsDescription] | AsyncKafkaFuture[UserScramCredentialsDescription]:
        """A future indicating the description results for the given user
        (``user_name``: the name of the user description being requested). The future
        will complete exceptionally if the future returned by ``users()`` completes
        exceptionally. Note that if the given user does not exist in the list of described
        users then the returned future will complete exceptionally with
        ``ResourceNotFoundError``."""
        if self._client is not None and user_name is None:
            # A null user cannot cross the FFI and is never a described user: Java's
            # answer, the request's error or ``ResourceNotFoundError``, composed over
            # ``users()``, which fails exactly when the request does (exception 4).
            return self._no_such_user(self._client("users", None), user_name)
        if self._client is not None:
            return self._client("description", user_name)  # type: ignore[no-any-return]
        from confluent_kafka.admin.user_scram_credentials_description import (
            UserScramCredentialsDescription,
        )

        retval: _KafkaFutureImpl[UserScramCredentialsDescription] = _KafkaFutureImpl()

        def action(data: _DescribeUserScramCredentialsResponseData | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                retval.complete_with_error(throwable)
                return
            assert data is not None
            # it is possible that there is no future for this user (for example, the
            # original describe request was for users 1, 2, and 3 but this is looking for
            # user 4), so explicitly take care of that case
            user_result = next(
                (result for result in data.results() if result.user() == user_name), None)
            if user_result is None:
                from confluent_kafka.common.errors import ResourceNotFoundError

                retval.complete_with_error(ResourceNotFoundError(
                    message="No such user: " + java_str(user_name)))
            elif user_result.error_code() != _NONE_CODE:
                # RESOURCE_NOT_FOUND is included here
                error = _errors_for_code(user_result.error_code(), user_result.error_message())
                assert error is not None
                retval.complete_with_error(error)
            else:
                retval.complete(UserScramCredentialsDescription(
                    name=user_result.user(),
                    credential_infos=self._get_scram_credential_infos_for(user_result)))

        assert self._data_future is not None
        self._data_future.when_complete(action=action)
        return self._kind._FUTURE._of_impl(retval)

    def _no_such_user(self, users: KafkaFuture[list[str]] | AsyncKafkaFuture[list[str]],
                      user_name: str | None
                      ) -> KafkaFuture[UserScramCredentialsDescription] | AsyncKafkaFuture[UserScramCredentialsDescription]:
        """A future failed as ``users`` fails, else with Java's
        ``ResourceNotFoundException("No such user: " + userName)``."""
        from confluent_kafka.common.errors import ResourceNotFoundError

        retval: _KafkaFutureImpl[UserScramCredentialsDescription] = _KafkaFutureImpl()

        def action(_users: list[str] | None, throwable: BaseException | None) -> None:
            if throwable is not None:
                retval.complete_with_error(throwable)
            else:
                retval.complete_with_error(ResourceNotFoundError(
                    message="No such user: " + java_str(user_name)))

        users.when_complete(action=action)
        return self._kind._FUTURE._of_impl(retval)

    @staticmethod
    def _get_scram_credential_infos_for(user_result: _UserResult) -> list[ScramCredentialInfo]:
        from confluent_kafka.admin.scram_credential_info import ScramCredentialInfo
        from confluent_kafka.admin.scram_mechanism import ScramMechanism

        return [ScramCredentialInfo(mechanism=ScramMechanism.from_type(type=c.mechanism()),
                                    iterations=c.iterations())
                for c in user_result.credential_infos()]
