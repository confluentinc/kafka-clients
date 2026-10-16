# Copyright 2026 Confluent Inc.
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

"""``TransactionListing``: Java's
``org.apache.kafka.clients.admin.TransactionListing``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from confluent_kafka._java import java_str

if TYPE_CHECKING:
    from .transaction_state import TransactionState

__all__ = ["TransactionListing"]


class TransactionListing:
    """A listing of a transaction: its transactional id, producer id and state.

    Java: ``org.apache.kafka.clients.admin.TransactionListing``.
    """

    __slots__ = ("_transactional_id", "_producer_id", "_transaction_state")

    def __init__(self, *, transactional_id: str, producer_id: int,
                 transaction_state: TransactionState) -> None:
        self._transactional_id = transactional_id
        self._producer_id = producer_id
        self._transaction_state = transaction_state

    def transactional_id(self) -> str:
        return self._transactional_id

    def producer_id(self) -> int:
        return self._producer_id

    def state(self) -> TransactionState:
        return self._transaction_state

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, TransactionListing)
        return (self._producer_id == other._producer_id
                and self._transactional_id == other._transactional_id
                and self._transaction_state is other._transaction_state)

    def __hash__(self) -> int:
        return hash((self._transactional_id, self._producer_id, self._transaction_state))

    def __str__(self) -> str:
        return ("TransactionListing(transactionalId='" + java_str(self._transactional_id)
                + "', producerId=" + str(self._producer_id)
                + ", transactionState=" + java_str(self._transaction_state) + ")")
