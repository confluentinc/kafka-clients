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
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import (
    AnyFuture,
    Async,
    KindT,
    Sync,
    _complete,
    _complete_with_error,
    _mark_retrieved,
    _new_future_like,
    _then_apply,
)

if TYPE_CHECKING:
    from confluent_kafka.admin.group_listing import GroupListing

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
    def _new(cls, *, future: AnyFuture[Iterable[object]],
             kind: type[Sync] | type[Async]) -> ListGroupsResult[Any]:
        """Java's package-private ``ListGroupsResult(future)``."""
        result = cls.__new__(cls)
        all_future: AnyFuture[list[GroupListing]] = _new_future_like(future)
        valid: AnyFuture[list[GroupListing]] = _new_future_like(future)
        errors: AnyFuture[list[BaseException]] = _new_future_like(future)
        result._all = all_future
        result._valid = valid
        result._errors = errors
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
                _complete_with_error(all_future, cur_errors[0])
            else:
                _complete(all_future, cur_valid)
            _complete(valid, cur_valid)
            _complete(errors, cur_errors)
            return None

        # Java discards this future; ``all()`` is built here whether or not a caller
        # reads it. Neither logs an unread error (``_mark_retrieved``).
        _mark_retrieved(_then_apply(future, split))
        _mark_retrieved(all_future)
        return result

    def all(self) -> AnyFuture[list[GroupListing]]:
        """Returns a future that yields either an exception, or the full set of group
        listings.

        In the event of a failure, the future yields nothing but the first exception which
        occurred.
        """
        return self._all

    def valid(self) -> AnyFuture[list[GroupListing]]:
        """Returns a future which yields just the valid listings.

        This future never fails with an error, no matter what happens. Errors are
        completely ignored. If nothing can be fetched, an empty collection is yielded. If
        there is an error, but some results can be returned, this future will yield those
        partial results. When using this future, it is a good idea to also check the
        errors future so that errors can be displayed and handled.
        """
        return self._valid

    def errors(self) -> AnyFuture[list[BaseException]]:
        """Returns a future which yields just the errors which occurred.

        If this future yields a non-empty collection, it is very likely that elements are
        missing from the ``valid()`` set.

        This future itself never fails with an error. In the event of an error, this
        future will successfully yield a collection containing at least one exception.
        """
        return self._errors
