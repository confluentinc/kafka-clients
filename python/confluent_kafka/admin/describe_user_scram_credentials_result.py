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
raises ``NullPointerError()``. The private static ``getScramCredentialInfosFor`` is
``_get_scram_credential_infos_for``. Java collects the users into a ``List``, so
``users()`` is a ``list[str]``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka._java import java_str
from confluent_kafka.admin._futures import (
    AnyFuture,
    Async,
    KindT,
    Sync,
    _complete,
    _complete_with_error,
    _new_future_like,
    _when_complete,
)
from confluent_kafka.admin._internals import (
    _DescribeUserScramCredentialsResponseData,
    _errors_for_code,
)
from confluent_kafka.null_pointer_error import NullPointerError

if TYPE_CHECKING:
    from confluent_kafka.admin.scram_credential_info import ScramCredentialInfo
    from confluent_kafka.admin.user_scram_credentials_description import (
        UserScramCredentialsDescription,
    )

__all__ = ["DescribeUserScramCredentialsResult"]

# Java's ``Errors.NONE.code()`` and ``Errors.RESOURCE_NOT_FOUND.code()``.
_NONE_CODE = 0
_RESOURCE_NOT_FOUND_CODE = 91

_UserResult = _DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult


class DescribeUserScramCredentialsResult(Generic[KindT]):
    """The result of the ``Admin.describe_user_scram_credentials()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult``.
    """

    __slots__ = ("_data_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeUserScramCredentialsResult cannot be constructed directly; use the "
            "admin client's describe_user_scram_credentials()")

    @classmethod
    def _new(cls, *, data_future: AnyFuture[_DescribeUserScramCredentialsResponseData],
             kind: type[Sync] | type[Async]) -> DescribeUserScramCredentialsResult[Any]:
        """Java's package-private ``DescribeUserScramCredentialsResult(dataFuture)``:
        ``data_future`` is the future indicating response data from the call."""
        if data_future is None:
            raise NullPointerError()
        result = cls.__new__(cls)
        result._data_future = data_future
        result._kind = kind
        return result

    def all(self) -> AnyFuture[dict[str, UserScramCredentialsDescription]]:
        """A future for the results of all described users with map keys (one per user)
        being consistent with the contents of the list returned by ``users()``. The future
        will complete successfully only if all such user descriptions complete
        successfully."""
        from confluent_kafka.admin.user_scram_credentials_description import (
            UserScramCredentialsDescription,
        )

        retval: AnyFuture[dict[str, UserScramCredentialsDescription]] = (
            _new_future_like(self._data_future))

        def action(data: _DescribeUserScramCredentialsResponseData | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                _complete_with_error(retval, throwable)
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
                _complete_with_error(retval, error)
            else:
                retval_map: dict[str, UserScramCredentialsDescription] = {}
                for user_result in data.results():
                    retval_map[user_result.user()] = UserScramCredentialsDescription(
                        name=user_result.user(),
                        credential_infos=self._get_scram_credential_infos_for(user_result))
                _complete(retval, retval_map)

        _when_complete(self._data_future, action)
        return retval

    def users(self) -> AnyFuture[list[str]]:
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
        retval: AnyFuture[list[str]] = _new_future_like(self._data_future)

        def action(data: _DescribeUserScramCredentialsResponseData | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                _complete_with_error(retval, throwable)
            else:
                assert data is not None
                _complete(retval, [result.user() for result in data.results()
                                   if result.error_code() != _RESOURCE_NOT_FOUND_CODE])

        _when_complete(self._data_future, action)
        return retval

    def description(self, *, user_name: str) -> AnyFuture[UserScramCredentialsDescription]:
        """A future indicating the description results for the given user
        (``user_name``: the name of the user description being requested). The future
        will complete exceptionally if the future returned by ``users()`` completes
        exceptionally. Note that if the given user does not exist in the list of described
        users then the returned future will complete exceptionally with
        ``ResourceNotFoundError``."""
        from confluent_kafka.admin.user_scram_credentials_description import (
            UserScramCredentialsDescription,
        )

        retval: AnyFuture[UserScramCredentialsDescription] = (
            _new_future_like(self._data_future))

        def action(data: _DescribeUserScramCredentialsResponseData | None,
                   throwable: BaseException | None) -> None:
            if throwable is not None:
                _complete_with_error(retval, throwable)
                return
            assert data is not None
            # it is possible that there is no future for this user (for example, the
            # original describe request was for users 1, 2, and 3 but this is looking for
            # user 4), so explicitly take care of that case
            user_result = next(
                (result for result in data.results() if result.user() == user_name), None)
            if user_result is None:
                from confluent_kafka.common.errors import ResourceNotFoundError

                _complete_with_error(retval, ResourceNotFoundError(
                    message="No such user: " + java_str(user_name)))
            elif user_result.error_code() != _NONE_CODE:
                # RESOURCE_NOT_FOUND is included here
                error = _errors_for_code(user_result.error_code(), user_result.error_message())
                assert error is not None
                _complete_with_error(retval, error)
            else:
                _complete(retval, UserScramCredentialsDescription(
                    name=user_result.user(),
                    credential_infos=self._get_scram_credential_infos_for(user_result)))

        _when_complete(self._data_future, action)
        return retval

    @staticmethod
    def _get_scram_credential_infos_for(user_result: _UserResult) -> list[ScramCredentialInfo]:
        from confluent_kafka.admin.scram_credential_info import ScramCredentialInfo
        from confluent_kafka.admin.scram_mechanism import ScramMechanism

        return [ScramCredentialInfo(mechanism=ScramMechanism.from_type(type=c.mechanism()),
                                    iterations=c.iterations())
                for c in user_result.credential_infos()]
