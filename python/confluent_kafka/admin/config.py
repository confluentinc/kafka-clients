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

"""``Config``: Java's ``org.apache.kafka.clients.admin.Config``."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from confluent_kafka._java import java_str

from ._values import frozen

if TYPE_CHECKING:
    from .config_entry import ConfigEntry

__all__ = ["Config"]


class Config:
    """A configuration object containing the configuration entries for a
    resource.

    Java: ``org.apache.kafka.clients.admin.Config``.
    """

    __slots__ = ("_entries",)

    def __init__(self, *, entries: Iterable[ConfigEntry]) -> None:
        """Create a configuration instance with the provided entries."""
        self._entries: dict[str, ConfigEntry] = {}
        for entry in entries:
            self._entries[entry.name()] = entry

    def entries(self) -> list[ConfigEntry]:
        """Configuration entries for a resource."""
        return list(self._entries.values())

    def get(self, *, name: str) -> ConfigEntry | None:
        """Get the configuration entry with the provided name or ``None`` if
        there isn't one."""
        return self._entries.get(name)

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, Config)
        return self._entries == other._entries

    def __hash__(self) -> int:
        return hash(frozen(self._entries))

    def __str__(self) -> str:
        return "Config(entries=" + java_str(list(self._entries.values())) + ")"
