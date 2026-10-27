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

"""``ClientQuotaEntity``: Java's ``org.apache.kafka.common.quota.ClientQuotaEntity``.

In ``confluent_kafka.common.quota``, the module of its Java package. The
entries are copied on construction and handed out as a new ``dict``, so the
object stays as Java's: its field is ``final`` and it has no setter.
"""

from __future__ import annotations

from collections.abc import Mapping

from confluent_kafka._java import java_str

__all__ = ["ClientQuotaEntity"]


class ClientQuotaEntity:
    """Describes a client quota entity, which is a mapping of entity types to
    their names.

    Java: ``org.apache.kafka.common.quota.ClientQuotaEntity``.
    """

    #: The type of an entity entry.
    USER = "user"
    CLIENT_ID = "client-id"
    IP = "ip"

    __slots__ = ("_entries",)

    @staticmethod
    def is_valid_entity_type(*, entity_type: str) -> bool:
        return entity_type in (ClientQuotaEntity.USER, ClientQuotaEntity.CLIENT_ID,
                               ClientQuotaEntity.IP)

    def __init__(self, *, entries: Mapping[str, str | None]) -> None:
        """Constructs a quota entity for the given types and names. If a name is
        ``None``, then it is mapped to the built-in default entity name.
        ``entries`` maps entity type to its name."""
        self._entries = dict(entries)

    def entries(self) -> dict[str, str | None]:
        """Map of entity type to its name."""
        return dict(self._entries)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ClientQuotaEntity)
        return self._entries == other._entries

    def __hash__(self) -> int:
        return hash(frozenset(self._entries.items()))

    def __str__(self) -> str:
        return "ClientQuotaEntity(entries=" + java_str(self._entries) + ")"
