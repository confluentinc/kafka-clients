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

"""``ListTransactionsOptions``: Java's ``org.apache.kafka.clients.admin.ListTransactionsOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter
field, named after the field (``filtered_states`` calls ``filter_states``, …) (CLAUDE.md,
Python Binding Conventions, Signatures). Java's ``hashCode`` hashes the two set
fields, so they are stored as ``frozenset``\\ s (hashable) and their getters return a new
``set``. ``new HashSet<>(null)`` is an implicit dereference, not a check: a ``None``
argument raises what ``frozenset(None)`` raises. Like Java, ``toString`` prints
``timeoutMs`` but ``equals`` / ``hashCode`` leave it out.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from confluent_kafka._args import UNSET
from confluent_kafka._java import java_str
from confluent_kafka.admin.abstract_options import AbstractOptions

if TYPE_CHECKING:
    from confluent_kafka.admin.transaction_state import TransactionState

__all__ = ["ListTransactionsOptions"]


class ListTransactionsOptions(AbstractOptions):
    """Options for ``Admin.list_transactions()``.

    Java: ``org.apache.kafka.clients.admin.ListTransactionsOptions``.
    """

    __slots__ = ("_filtered_states", "_filtered_producer_ids", "_filtered_duration",
                 "_filtered_transactional_id_pattern")

    def __init__(self, *, timeout_ms: int | None = UNSET,
                 filtered_states: Iterable[TransactionState] = UNSET,
                 filtered_producer_ids: Iterable[int] = UNSET,
                 filtered_duration: int = UNSET,
                 filtered_transactional_id_pattern: str | None = UNSET) -> None:
        super().__init__()
        self._filtered_states: frozenset[TransactionState] = frozenset()
        self._filtered_producer_ids: frozenset[int] = frozenset()
        self._filtered_duration = -1
        self._filtered_transactional_id_pattern: str | None = None
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
        if filtered_states is not UNSET:
            self.filter_states(states=filtered_states)
        if filtered_producer_ids is not UNSET:
            self.filter_producer_ids(producer_id_filters=filtered_producer_ids)
        if filtered_duration is not UNSET:
            self.filter_on_duration(duration_ms=filtered_duration)
        if filtered_transactional_id_pattern is not UNSET:
            self.filter_on_transactional_id_pattern(pattern=filtered_transactional_id_pattern)

    def filter_states(self, *, states: Iterable[TransactionState]) -> ListTransactionsOptions:
        """Filter only the transactions that are in a specific set of states. If no filter
        is specified or if the passed set of states is empty, then transactions in all
        states will be returned.

        :param states: the set of states to filter by
        :return: this object
        """
        self._filtered_states = frozenset(states)
        self._record("filter_states", self._filtered_states)
        return self

    def filter_producer_ids(self, *, producer_id_filters: Iterable[int]) -> ListTransactionsOptions:
        """Filter only the transactions from producers in a specific set of producerIds.
        If no filter is specified or if the passed collection of producerIds is empty,
        then the transactions of all producerIds will be returned.

        :param producer_id_filters: the set of producerIds to filter by
        :return: this object
        """
        self._filtered_producer_ids = frozenset(producer_id_filters)
        self._record("filter_producer_ids", self._filtered_producer_ids)
        return self

    def filter_on_duration(self, *, duration_ms: int) -> ListTransactionsOptions:
        """Filter only the transactions that are running longer than the specified
        duration. If no filter is specified or if the passed duration ms is less than 0,
        then the all transactions will be returned.

        :param duration_ms: the duration in milliseconds to filter by
        :return: this object
        """
        self._filtered_duration = duration_ms
        self._record("filter_on_duration", self._filtered_duration)
        return self

    def filter_on_transactional_id_pattern(self, *, pattern: str | None) -> ListTransactionsOptions:
        """Filter only the transactions that match with the given transactional ID
        pattern. If the filter is null or if the passed string is empty, then all the
        transactions will be returned.

        :param pattern: the transactional ID regular expression pattern to filter by
        :return: this object
        """
        self._filtered_transactional_id_pattern = pattern
        self._record("filter_on_transactional_id_pattern", self._filtered_transactional_id_pattern)
        return self

    def filtered_states(self) -> set[TransactionState]:
        """Returns the set of states to be filtered or empty if no states have been
        specified (empty means that no states are filtered and all transactions will be
        returned)."""
        return set(self._filtered_states)

    def filtered_producer_ids(self) -> set[int]:
        """Returns the set of producerIds that are being filtered or empty if none have
        been specified (empty means that no producerIds are filtered and all transactions
        will be returned)."""
        return set(self._filtered_producer_ids)

    def filtered_duration(self) -> int:
        """Returns the duration ms value being filtered (negative value means
        transactions are not filtered by duration)."""
        return self._filtered_duration

    def filtered_transactional_id_pattern(self) -> str | None:
        """Returns transactional ID being filtered (empty means no transactional IDs are
        filtered and all transactions will be returned)."""
        return self._filtered_transactional_id_pattern

    def __str__(self) -> str:
        return ("ListTransactionsOptions(" + "filteredStates=" + java_str(self._filtered_states)
                + ", filteredProducerIds=" + java_str(self._filtered_producer_ids)
                + ", filteredDuration=" + java_str(self._filtered_duration)
                + ", filteredTransactionalIdPattern="
                + java_str(self._filtered_transactional_id_pattern)
                + ", timeoutMs=" + java_str(self._timeout_ms) + ")")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ListTransactionsOptions)
        return (self._filtered_states == other._filtered_states
                and self._filtered_producer_ids == other._filtered_producer_ids
                and self._filtered_duration == other._filtered_duration
                and self._filtered_transactional_id_pattern
                == other._filtered_transactional_id_pattern)

    def __hash__(self) -> int:
        return hash((self._filtered_states, self._filtered_producer_ids, self._filtered_duration,
                     self._filtered_transactional_id_pattern))
