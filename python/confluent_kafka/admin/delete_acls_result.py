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

"""``DeleteAclsResult``: Java's ``org.apache.kafka.clients.admin.DeleteAclsResult``.

Hand-written: outside every generator pattern (``_reference_set.md``, Coverage), as
``all()`` wraps a failure in ``KafkaException(message, cause)`` and throws
``FilterResult.exception()``. Java's constructors (of the class and of its public
nested ``FilterResult`` / ``FilterResults``) are package-private: each ``__init__``
raises ``TypeError`` and ``_new`` builds it (CLAUDE.md, Python Binding Conventions,
Class family). The private ``getAclBindings`` is ``_get_acl_bindings``; Java builds the
bindings as an ``ArrayList``, so ``all()``'s ``Collection<AclBinding>`` is a ``list``
(Types).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync, _Result
from confluent_kafka.common.kafka_error import KafkaError

if TYPE_CHECKING:
    from confluent_kafka.common.acl import AclBinding, AclBindingFilter
    from confluent_kafka.common.errors import ApiError
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["DeleteAclsResult"]


class DeleteAclsResult(_Result, Generic[KindT]):
    """The result of the ``Admin.delete_acls()`` call.

    Java: ``org.apache.kafka.clients.admin.DeleteAclsResult``.
    """

    class FilterResult:
        """A class containing either the deleted ACL binding or an exception if the
        delete failed.

        Java: ``org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult``.
        """

        __slots__ = ("_binding", "_exception")

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise TypeError(
                "DeleteAclsResult.FilterResult cannot be constructed directly; use "
                "DeleteAclsResult.FilterResults.values()")

        @classmethod
        def _new(cls, *, binding: AclBinding | None,
                 exception: ApiError | None) -> DeleteAclsResult.FilterResult:
            """Java's package-private ``FilterResult(binding, exception)``."""
            result = cls.__new__(cls)
            result._binding = binding
            result._exception = exception
            return result

        def binding(self) -> AclBinding | None:
            """Return the deleted ACL binding or ``None`` if there was an error."""
            return self._binding

        def exception(self) -> ApiError | None:
            """Return an exception if the ACL delete was not successful or ``None`` if it
            was."""
            return self._exception

    class FilterResults:
        """A class containing the results of the delete ACLs operation.

        Java: ``org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults``.
        """

        __slots__ = ("_values",)

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise TypeError(
                "DeleteAclsResult.FilterResults cannot be constructed directly; use the "
                "futures of DeleteAclsResult.values()")

        @classmethod
        def _new(cls, *, values: Iterable[DeleteAclsResult.FilterResult]
                 ) -> DeleteAclsResult.FilterResults:
            """Java's package-private ``FilterResults(values)``."""
            result = cls.__new__(cls)
            result._values = None if values is None else list(values)
            return result

        def values(self) -> list[DeleteAclsResult.FilterResult]:
            """Return a list of delete ACLs results for a given filter."""
            return self._values

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DeleteAclsResult cannot be constructed directly; use the admin client's "
            "delete_acls()")

    @classmethod
    def _new(cls, *,
             futures: Mapping[AclBindingFilter, KafkaFuture[DeleteAclsResult.FilterResults] | AsyncKafkaFuture[DeleteAclsResult.FilterResults]],
             kind: type[Sync] | type[Async]) -> DeleteAclsResult[Any]:
        """Java's package-private ``DeleteAclsResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = None if futures is None else dict(futures)
        result._kind = kind
        return result

    def values(self) -> dict[AclBindingFilter, KafkaFuture[DeleteAclsResult.FilterResults] | AsyncKafkaFuture[DeleteAclsResult.FilterResults]]:
        """Return a map from acl filters to futures which can be used to check the status
        of the deletions by each filter."""
        return self._futures

    def all(self) -> KafkaFuture[list[AclBinding]] | AsyncKafkaFuture[list[AclBinding]]:
        """Return a future which succeeds only if all the ACLs deletions succeed, and
        which contains all the deleted ACLs. Note that it if the filters don't match any
        ACLs, this is not considered an error."""
        futures = self._futures
        return self._kind._FUTURE.all_of(futures=tuple(futures.values())).then_apply(function=lambda v: self._get_acl_bindings(futures))

    def _get_acl_bindings(
            self, futures: Mapping[AclBindingFilter, KafkaFuture[DeleteAclsResult.FilterResults] | AsyncKafkaFuture[DeleteAclsResult.FilterResults]]
    ) -> list[AclBinding]:
        acls: list[AclBinding] = []
        for value in futures.values():
            try:
                results = value.get_now(value_if_absent=None)
            except BaseException as e:  # noqa: BLE001 - Java catches Throwable
                # This should be unreachable, since the future returned by
                # KafkaFuture#allOf should have failed if any Future failed.
                raise KafkaError(message="DeleteAclsResult#all: internal error",
                                 cause=e) from e
            for result in results.values():
                exception = result.exception()
                if exception is not None:
                    raise exception
                acls.append(result.binding())
        return acls
