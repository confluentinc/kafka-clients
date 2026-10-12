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

"""``FenceProducersResult``: Java's ``org.apache.kafka.clients.admin.FenceProducersResult``.

Hand-written: outside every generator pattern (``_reference_set.md``, Coverage), as its
futures carry Java's internal ``ProducerIdAndEpoch`` (the private helper
``_ProducerIdAndEpoch``, ``_internals``) and its getters go through the private
``findAndApply(…, KafkaFuture.BaseFunction)`` (``_find_and_apply``, the function a
plain callable). Java's constructor is package-private: ``__init__`` raises
``TypeError`` and ``_new`` builds it (CLAUDE.md, Python Binding Conventions, Class
family). Its map is keyed by Java's internal ``CoordinatorKey`` (``_CoordinatorKey``).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Generic, TypeVar

from confluent_kafka.admin._futures import (
    AnyFuture,
    Async,
    KindT,
    Sync,
    _all_of,
    _mark_retrieved,
    _then_apply,
)
from confluent_kafka.admin._internals import _CoordinatorKey, _ProducerIdAndEpoch
from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["FenceProducersResult"]

_T = TypeVar("_T")


class FenceProducersResult(Generic[KindT]):
    """The result of the ``Admin.fence_producers()`` call.

    Java: ``org.apache.kafka.clients.admin.FenceProducersResult``.
    """

    __slots__ = ("_futures", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "FenceProducersResult cannot be constructed directly; use the admin client's "
            "fence_producers()")

    @classmethod
    def _new(cls, *, futures: Mapping[_CoordinatorKey, AnyFuture[_ProducerIdAndEpoch]],
             kind: type[Sync] | type[Async]) -> FenceProducersResult[Any]:
        """Java's package-private ``FenceProducersResult(futures)``."""
        result = cls.__new__(cls)
        result._futures = dict(futures)
        result._kind = kind
        return result

    def fenced_producers(self) -> dict[str, AnyFuture[None]]:
        """Return a map from transactional ID to futures which can be used to check the
        status of individual fencings."""
        fenced: dict[str, AnyFuture[None]] = {}
        for key, future in self._futures.items():
            derived: AnyFuture[None] = _then_apply(future, lambda p: None)
            # A key the caller leaves unread does not log its error, as with the
            # client's own per-key futures.
            _mark_retrieved(derived)
            fenced[key.id_value] = derived
        return fenced

    def producer_id(self, *, transactional_id: str) -> AnyFuture[int]:
        """Returns a future that provides the producer ID generated while initializing the
        given transaction when the request completes."""
        return self._find_and_apply(transactional_id, lambda p: p.producer_id)

    def epoch_id(self, *, transactional_id: str) -> AnyFuture[int]:
        """Returns a future that provides the epoch ID generated while initializing the
        given transaction when the request completes."""
        return self._find_and_apply(transactional_id, lambda p: p.epoch)

    def all(self) -> AnyFuture[None]:
        """Return a future which succeeds only if all the producer fencings succeed."""
        return _all_of(self._futures.values(), self._kind)

    def _find_and_apply(self, transactional_id: str,
                        followup: Callable[[_ProducerIdAndEpoch], _T]) -> AnyFuture[_T]:
        key = _CoordinatorKey.by_transactional_id(transactional_id=transactional_id)
        future = self._futures.get(key)
        if future is None:
            raise IllegalArgumentError(
                message="TransactionalId " + "`" + transactional_id
                + "` was not included in the request")
        return _then_apply(future, followup)
