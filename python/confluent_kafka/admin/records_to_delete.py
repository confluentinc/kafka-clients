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

"""``RecordsToDelete``: Java's ``org.apache.kafka.clients.admin.RecordsToDelete``.

Java's constructor is private, so ``RecordsToDelete()`` raises ``TypeError``
naming the static factory; Java's private constructor is ``_new`` (CLAUDE.md,
Python Binding Conventions, Class family). Java's static
factory ``beforeOffset(long)`` and instance getter ``beforeOffset()`` share one
name: ``before_offset`` is the factory on the class and the getter on an
instance (Signatures).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from ._values import FactoryOrGetter

__all__ = ["RecordsToDelete"]


class _BeforeOffsetFactory(Protocol):
    def __call__(self, *, offset: int) -> RecordsToDelete: ...


class RecordsToDelete:
    """Describe records to delete in a call to the admin client's
    ``delete_records()``.

    Java: ``org.apache.kafka.clients.admin.RecordsToDelete``.
    """

    __slots__ = ("_offset",)

    _offset: int

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "RecordsToDelete cannot be constructed directly; use "
            "RecordsToDelete.before_offset(...)")

    @classmethod
    def _new(cls, *, offset: int) -> RecordsToDelete:
        """Java's private ``RecordsToDelete(long offset)``."""
        records = cls.__new__(cls)
        records._offset = offset
        return records

    @staticmethod
    def _before_offset_factory(*, offset: int) -> RecordsToDelete:
        """Delete all the records before the given ``offset``. Use ``-1`` to
        truncate to the high watermark."""
        return RecordsToDelete._new(offset=offset)

    def _before_offset_getter(self) -> int:
        return self._offset

    #: ``RecordsToDelete.before_offset(offset=…)`` (static factory: delete all
    #: the records before the given offset; ``-1`` truncates to the high
    #: watermark) / ``records.before_offset()`` (the offset before which all
    #: records will be deleted).
    before_offset: FactoryOrGetter[_BeforeOffsetFactory, Callable[[], int]]

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, RecordsToDelete)
        return self._offset == other._offset

    def __hash__(self) -> int:
        return hash(self._offset)

    def __str__(self) -> str:
        return "(beforeOffset = " + str(self._offset) + ")"


RecordsToDelete.before_offset = FactoryOrGetter(
    RecordsToDelete._before_offset_factory, RecordsToDelete._before_offset_getter)
