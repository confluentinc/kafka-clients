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

"""``OffsetResetStrategy``: Java's ``org.apache.kafka.clients.consumer.OffsetResetStrategy``.

An ``enum.Enum`` valued by the constant's name, members in Java's order
(CLAUDE.md, Python Binding Conventions, Class family). Java deprecates the enum
itself; the deprecated ``MockConsumer`` constructor that takes it warns when
called.
"""

from __future__ import annotations

from enum import Enum

__all__ = ["OffsetResetStrategy"]


class OffsetResetStrategy(Enum):
    """Deprecated: since 4.0; will be removed in a future release. Not required
    by Kafka client users; no replacement is provided.

    Java: ``org.apache.kafka.clients.consumer.OffsetResetStrategy``.
    """

    LATEST = "LATEST"
    EARLIEST = "EARLIEST"
    NONE = "NONE"

    def __str__(self) -> str:
        return self.name.lower()
