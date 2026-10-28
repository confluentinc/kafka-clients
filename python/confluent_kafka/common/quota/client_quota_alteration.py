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

"""``ClientQuotaAlteration``: Java's
``org.apache.kafka.common.quota.ClientQuotaAlteration``, with its nested
``ClientQuotaAlteration.Op``.

In ``confluent_kafka.common.quota``, the module of its Java package.
``ClientQuotaAlteration`` overrides only ``toString``, so it keeps identity
equality, as in Java. ``Op``'s value is Java's ``Double``: an ``int`` given for
it is kept as a ``float``, so ``str()`` prints it as Java does (``1.0``).
"""

from __future__ import annotations

from collections.abc import Iterable

from confluent_kafka._java import java_str
from confluent_kafka.common.quota.client_quota_entity import ClientQuotaEntity

__all__ = ["ClientQuotaAlteration"]


class ClientQuotaAlteration:
    """Describes a configuration alteration to be made to a client quota
    entity.

    Java: ``org.apache.kafka.common.quota.ClientQuotaAlteration``.
    """

    class Op:
        """Java: ``org.apache.kafka.common.quota.ClientQuotaAlteration.Op``."""

        __slots__ = ("_key", "_value")

        def __init__(self, *, key: str, value: float | None) -> None:
            """``key`` the quota type to alter; ``value``: if set then the
            existing value is updated, otherwise if ``None``, the existing
            value is cleared."""
            self._key = key
            self._value = None if value is None else float(value)

        def key(self) -> str:
            """The quota type to alter."""
            return self._key

        def value(self) -> float | None:
            """If set then the existing value is updated, otherwise if
            ``None``, the existing value is cleared."""
            return self._value

        def __eq__(self, other: object) -> bool:
            if self is other:
                return True
            if type(other) is not type(self):
                return NotImplemented
            assert isinstance(other, ClientQuotaAlteration.Op)
            return self._key == other._key and self._value == other._value

        def __hash__(self) -> int:
            return hash((self._key, self._value))

        def __str__(self) -> str:
            return ("ClientQuotaAlteration.Op(key=" + java_str(self._key) + ", value="
                    + java_str(self._value) + ")")

    __slots__ = ("_entity", "_ops")

    def __init__(self, *, entity: ClientQuotaEntity,
                 ops: Iterable[ClientQuotaAlteration.Op]) -> None:
        """``entity`` the entity whose config will be modified; ``ops`` the
        alteration to perform."""
        self._entity = entity
        # A None is stored, as Java's constructor stores it (Class family).
        self._ops: tuple[ClientQuotaAlteration.Op, ...] | None = None if ops is None else tuple(ops)

    def entity(self) -> ClientQuotaEntity:
        """The entity whose config will be modified."""
        return self._entity

    def ops(self) -> list[ClientQuotaAlteration.Op]:
        """The alteration to perform."""
        return None if self._ops is None else list(self._ops)  # type: ignore[return-value]  # a None given, as Java returns it

    def __str__(self) -> str:
        return ("ClientQuotaAlteration(entity=" + java_str(self._entity) + ", ops="
                + java_str(None if self._ops is None else list(self._ops)) + ")")
