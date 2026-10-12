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

"""``TransactionState``: Java's ``org.apache.kafka.clients.admin.TransactionState``.

Java's constants carry a ``String`` and no integer, so an ``enum.Enum`` valued
by the constant's name; Java's ``name`` field is reached through ``__str__``
(``toString``) and ``parse`` (CLAUDE.md, Python Binding Conventions, Class
family).
"""

from __future__ import annotations

from enum import Enum

__all__ = ["TransactionState"]


class TransactionState(Enum):
    """The state of a transaction.

    Java: ``org.apache.kafka.clients.admin.TransactionState``.
    """

    ONGOING = "ONGOING"
    PREPARE_ABORT = "PREPARE_ABORT"
    PREPARE_COMMIT = "PREPARE_COMMIT"
    COMPLETE_ABORT = "COMPLETE_ABORT"
    COMPLETE_COMMIT = "COMPLETE_COMMIT"
    EMPTY = "EMPTY"
    PREPARE_EPOCH_FENCE = "PREPARE_EPOCH_FENCE"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return _NAMES[self]

    @staticmethod
    def parse(*, name: str) -> TransactionState:
        """The state whose Java name is ``name``, else ``UNKNOWN``."""
        return _NAME_TO_ENUM.get(name, TransactionState.UNKNOWN)


_NAMES: dict[TransactionState, str] = {
    TransactionState.ONGOING: "Ongoing",
    TransactionState.PREPARE_ABORT: "PrepareAbort",
    TransactionState.PREPARE_COMMIT: "PrepareCommit",
    TransactionState.COMPLETE_ABORT: "CompleteAbort",
    TransactionState.COMPLETE_COMMIT: "CompleteCommit",
    TransactionState.EMPTY: "Empty",
    TransactionState.PREPARE_EPOCH_FENCE: "PrepareEpochFence",
    TransactionState.UNKNOWN: "Unknown",
}
_NAME_TO_ENUM: dict[str, TransactionState] = {v: k for k, v in _NAMES.items()}
