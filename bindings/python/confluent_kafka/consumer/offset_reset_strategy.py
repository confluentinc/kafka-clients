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

"""``OffsetResetStrategy`` — the value of ``auto.offset.reset``.

Translated from ``org.apache.kafka.clients.consumer.OffsetResetStrategy``
(Apache Kafka 4.3.1). Java's enum is ``@Deprecated`` since 4.0 (no replacement);
kept per rule 3.13 because ``MockConsumer``'s deprecated constructor takes it.
Members are declared in Java's order (``LATEST``, ``EARLIEST``, ``NONE``);
``__str__`` returns the lowercase name (Java ``toString``).
"""

from __future__ import annotations

from enum import Enum, auto


class OffsetResetStrategy(Enum):
    """The value of ``auto.offset.reset``.

    Java: ``org.apache.kafka.clients.consumer.OffsetResetStrategy``.

    .. deprecated::
        ``@Deprecated`` since 4.0; will be removed in a future release. Not
        required by Kafka client users; no replacement is provided. Kept per
        rule 3.13.
    """

    LATEST = auto()
    EARLIEST = auto()
    NONE = auto()

    def __str__(self) -> str:
        # Java toString: super.toString().toLowerCase(Locale.ROOT)
        return self.name.lower()
