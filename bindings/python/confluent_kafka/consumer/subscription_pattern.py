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

"""``SubscriptionPattern`` — a broker-side RE2/J regex for ``subscribe()``.

Translated from ``org.apache.kafka.clients.consumer.SubscriptionPattern``
(Apache Kafka 4.3.1). It just wraps the pattern string; all RE2/J validation is
delegated to the broker. Value equality and hashable over the pattern string.
"""

from __future__ import annotations


class SubscriptionPattern:
    """A regular expression compatible with Google RE2/J, used to subscribe to
    topics.

    Java: ``org.apache.kafka.clients.consumer.SubscriptionPattern``.
    """

    __slots__ = ("_pattern",)

    def __init__(self, *, pattern: str) -> None:
        self._pattern = pattern

    def pattern(self) -> str:
        """The RE2/J-compatible pattern string."""
        return self._pattern

    def __str__(self) -> str:
        # Java toString returns the pattern.
        return self._pattern

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SubscriptionPattern):
            return NotImplemented
        return self._pattern == other._pattern

    def __hash__(self) -> int:
        return hash(self._pattern)

    def __repr__(self) -> str:
        return self._pattern
