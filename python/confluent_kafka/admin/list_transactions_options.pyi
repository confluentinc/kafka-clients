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

# Stub for confluent_kafka.admin.ListTransactionsOptions.

from collections.abc import Iterable

from confluent_kafka.admin.abstract_options import AbstractOptions
from confluent_kafka.admin.transaction_state import TransactionState

__all__ = ["ListTransactionsOptions"]

class ListTransactionsOptions(AbstractOptions):
    def __init__(self, *, timeout_ms: int | None = None,
                 filtered_states: Iterable[TransactionState] = (),
                 filtered_producer_ids: Iterable[int] = (),
                 filtered_duration: int = -1,
                 filtered_transactional_id_pattern: str | None = None) -> None: ...
    def filter_states(self, *, states: Iterable[TransactionState]) -> ListTransactionsOptions: ...
    def filter_producer_ids(self, *,
                            producer_id_filters: Iterable[int]) -> ListTransactionsOptions: ...
    def filter_on_duration(self, *, duration_ms: int) -> ListTransactionsOptions: ...
    def filter_on_transactional_id_pattern(self, *,
                                           pattern: str | None) -> ListTransactionsOptions: ...
    def filtered_states(self) -> set[TransactionState]: ...
    def filtered_producer_ids(self) -> set[int]: ...
    def filtered_duration(self) -> int: ...
    def filtered_transactional_id_pattern(self) -> str | None: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
