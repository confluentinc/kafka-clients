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

"""``ListGroupsResult``: Java's ``org.apache.kafka.clients.admin.ListGroupsResult``.

Hand-written: outside every generator pattern (``_reference_set.md``, Coverage), as the
constructor body splits a ``Collection<Object>`` by ``instanceof Throwable`` into three
futures (Java's ``KafkaFutureImpl`` fields) completed from a ``thenApply``. Java's
constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new`` builds it
(CLAUDE.md, Python Binding Conventions, Class family). Java builds the listings and the
errors as ``List``\\ s, so each ``Collection`` is a ``list`` (Types); ``all()`` and
``valid()`` yield the same list, as Java's do. As in Java, the three futures stay pending
when the given future fails (Java discards the ``thenApply`` future that would carry the
failure). The futures it builds without a caller asking (``all()`` and the
discarded ``thenApply``) do not log an unread error, as Java's do not.

The client's result is built by ``_of_client`` instead, over the core's own
result's ``all()``, ``valid()`` and ``errors()`` futures, which the core builds from
Java's ``Collection<Object>`` as this constructor does.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import Async, KindT, Sync
from confluent_kafka.common.kafka_future import _KafkaFutureImpl

if TYPE_CHECKING:
    from confluent_kafka.admin.group_listing import GroupListing
    from confluent_kafka.common.kafka_future import AsyncKafkaFuture, KafkaFuture

__all__ = ["ListGroupsResult"]


class ListGroupsResult(Generic[KindT]):
    """The result of the ``Admin.list_groups()`` call.

    Java: ``org.apache.kafka.clients.admin.ListGroupsResult``.
    """

    __slots__ = ("_all", "_valid", "_errors", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ListGroupsResult cannot be constructed directly; use the admin client's "
            "list_groups()")

    @classmethod
    def _new(cls, *, future: KafkaFuture[Iterable[object]] | AsyncKafkaFuture[Iterable[object]],
             kind: type[Sync] | type[Async]) -> ListGroupsResult[Any]:
        """Java's package-private ``ListGroupsResult(future)``."""
        result = cls.__new__(cls)
        all_future: _KafkaFutureImpl[list[GroupListing]] = _KafkaFutureImpl()
        valid: _KafkaFutureImpl[list[GroupListing]] = _KafkaFutureImpl()
        errors: _KafkaFutureImpl[list[BaseException]] = _KafkaFutureImpl()
        result._all = kind._FUTURE._of_impl(all_future)
        result._valid = kind._FUTURE._of_impl(valid)
        result._errors = kind._FUTURE._of_impl(errors)
        result._kind = kind

        def split(results: Iterable[object]) -> None:
            cur_errors: list[BaseException] = []
            cur_valid: list[GroupListing] = []
            for result_object in results:
                if isinstance(result_object, BaseException):
                    cur_errors.append(result_object)
                else:
                    cur_valid.append(result_object)
            if cur_errors:
                all_future.complete_with_error(cur_errors[0])
            else:
                all_future.complete(cur_valid)
            valid.complete(cur_valid)
            errors.complete(cur_errors)
            return None

        # Java discards this future.
        future.then_apply(function=split)
        return result

    @classmethod
    def _of_client(
            cls, *,
            all: KafkaFuture[list[GroupListing]] | AsyncKafkaFuture[list[GroupListing]],
            valid: KafkaFuture[list[GroupListing]] | AsyncKafkaFuture[list[GroupListing]],
            errors: KafkaFuture[list[BaseException]] | AsyncKafkaFuture[list[BaseException]],
            kind: type[Sync] | type[Async]) -> ListGroupsResult[Any]:
        """The client's result over the core's ``all()``, ``valid()`` and
        ``errors()`` futures."""
        result = cls.__new__(cls)
        result._all = all
        result._valid = valid
        result._errors = errors
        result._kind = kind
        return result

    def all(self) -> KafkaFuture[list[GroupListing]] | AsyncKafkaFuture[list[GroupListing]]:
        """Returns a future that yields either an exception, or the full set of group
        listings.

        In the event of a failure, the future yields nothing but the first exception which
        occurred.
        """
        return self._all

    def valid(self) -> KafkaFuture[list[GroupListing]] | AsyncKafkaFuture[list[GroupListing]]:
        """Returns a future which yields just the valid listings.

        This future never fails with an error, no matter what happens. Errors are
        completely ignored. If nothing can be fetched, an empty collection is yielded. If
        there is an error, but some results can be returned, this future will yield those
        partial results. When using this future, it is a good idea to also check the
        errors future so that errors can be displayed and handled.
        """
        return self._valid

    def errors(self) -> KafkaFuture[list[BaseException]] | AsyncKafkaFuture[list[BaseException]]:
        """Returns a future which yields just the errors which occurred.

        If this future yields a non-empty collection, it is very likely that elements are
        missing from the ``valid()`` set.

        This future itself never fails with an error. In the event of an error, this
        future will successfully yield a collection containing at least one exception.
        """
        return self._errors
