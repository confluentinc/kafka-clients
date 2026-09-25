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

"""The ``confluent_kafka`` package root.

Mirrors Java's package tree with the ``clients`` segment dropped (spec §4). The
root holds the JDK-type analogs the API surface uses — classes that come from
Java itself rather than a Kafka package, so they have no Kafka package to mirror
(``IllegalStateError``, ``IllegalArgumentError``, ``ConcurrentModificationError``
and the Java-base ``TimeoutError``) — and the ``Duration`` alias.

The everyday client names (``KafkaConsumer``, ``KafkaProducer``, ``KafkaError``,
…) are **not** re-exported here yet: whether the root re-exports them is not
decided (spec §4), so importing them goes through their own module for now
(``from confluent_kafka.consumer import KafkaConsumer``).
"""

from __future__ import annotations

from datetime import timedelta

# The JDK-type analogs live at the package root (spec §4 / §5.5). They are
# generated — names, parent chains and ``_ffi_id`` — from the Java exception
# sources by ``cargo xtask generate-error-codes``; see
# ``confluent_kafka/_generated_errors.py``.
from ._generated_errors import (
    ConcurrentModificationError,
    IllegalArgumentError,
    IllegalStateError,
    TimeoutError,
)

# ``Duration`` is Java's ``java.time.Duration`` mapped to a number of seconds or
# a ``timedelta`` (rule 3.7). A negative value is rejected where it is consumed.
Duration = float | timedelta

__all__ = [
    "ConcurrentModificationError",
    "Duration",
    "IllegalArgumentError",
    "IllegalStateError",
    "TimeoutError",
]
