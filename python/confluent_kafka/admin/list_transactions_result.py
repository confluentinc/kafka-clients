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

"""``ListTransactionsResult``: Java's ``org.apache.kafka.clients.admin.ListTransactionsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). It holds a future of a
map of futures (one per broker), both of the client's kind, so ``by_broker_id()`` is a
future of a ``dict`` of futures of that kind. The methods make a new future completed
from ``whenComplete`` (``_new_future_like`` / ``_when_complete``); ``all()`` is a
``thenApply`` of ``all_by_broker_id()``. Java builds the listings as ``List``\\ s, so a
``Collection<TransactionListing>`` is a ``list`` (Types).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import (
    AnyFuture,
    Async,
    KindT,
    Sync,
    _complete,
    _complete_with_error,
    _new_future_like,
    _then_apply,
    _when_complete,
)

if TYPE_CHECKING:
    from confluent_kafka.admin.transaction_listing import TransactionListing

__all__ = ["ListTransactionsResult"]

_BrokerFutures = Mapping[int, AnyFuture[list["TransactionListing"]]]


class ListTransactionsResult(Generic[KindT]):
    """The result of the ``Admin.list_transactions()`` call.

    Java: ``org.apache.kafka.clients.admin.ListTransactionsResult``.
    """

    __slots__ = ("_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "ListTransactionsResult cannot be constructed directly; use the admin client's "
            "list_transactions()")

    @classmethod
    def _new(cls, *, future: AnyFuture[_BrokerFutures],
             kind: type[Sync] | type[Async]) -> ListTransactionsResult[Any]:
        """Java's package-private ``ListTransactionsResult(future)``."""
        result = cls.__new__(cls)
        result._future = future
        result._kind = kind
        return result

    def all(self) -> AnyFuture[list[TransactionListing]]:
        """Get all transaction listings. If any of the underlying requests fail, then the
        future returned from this method will also fail with the first encountered error.

        The future completes when all transaction listings are available and fails after
        any non-retriable error.
        """
        def flatten(by_broker: Mapping[int, list[TransactionListing]]) -> list[TransactionListing]:
            all_listings: list[TransactionListing] = []
            for listings in by_broker.values():
                all_listings.extend(listings)
            return all_listings

        return _then_apply(self.all_by_broker_id(), flatten)

    def by_broker_id(self) -> AnyFuture[dict[int, AnyFuture[list[TransactionListing]]]]:
        """Get a future which returns a map containing the underlying listing future for
        each broker in the cluster. This is useful, for example, if a partial listing of
        transactions is sufficient, or if you want more granular error details.

        The top-level future may fail if the admin client is unable to lookup the
        available brokers in the cluster; the futures in the map complete individually.
        """
        result: AnyFuture[dict[int, AnyFuture[list[TransactionListing]]]] = (
            _new_future_like(self._future))

        def action(broker_futures: _BrokerFutures | None, exception: BaseException | None) -> None:
            if broker_futures is not None:
                _complete(result, dict(broker_futures))
            else:
                assert exception is not None
                _complete_with_error(result, exception)

        _when_complete(self._future, action)
        return result

    def all_by_broker_id(self) -> AnyFuture[dict[int, list[TransactionListing]]]:
        """Get all transaction listings in a map which is keyed by the ID of respective
        broker that is currently managing them. If any of the underlying requests fail,
        then the future returned from this method will also fail with the first
        encountered error.

        This future completes when all transaction listings are available and fails
        after any non-retriable error.
        """
        all_future: AnyFuture[dict[int, list[TransactionListing]]] = (
            _new_future_like(self._future))
        all_listings_map: dict[int, list[TransactionListing]] = {}

        def action(by_broker: _BrokerFutures | None,
                   top_level_exception: BaseException | None) -> None:
            if top_level_exception is not None:
                _complete_with_error(all_future, top_level_exception)
                return
            assert by_broker is not None
            remaining_responses = set(by_broker.keys())

            def on_broker(broker_id: int) -> Any:
                def broker_action(listings: list[TransactionListing] | None,
                                  broker_exception: BaseException | None) -> None:
                    if broker_exception is not None:
                        _complete_with_error(all_future, broker_exception)
                    elif not all_future.done():
                        assert listings is not None
                        all_listings_map[broker_id] = listings
                        remaining_responses.discard(broker_id)
                        if not remaining_responses:
                            _complete(all_future, all_listings_map)
                return broker_action

            for broker_id, future in by_broker.items():
                _when_complete(future, on_broker(broker_id))

        _when_complete(self._future, action)
        return all_future
