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

"""``confluent_kafka.producer`` — mirror of
``org.apache.kafka.clients.producer`` (the ``clients`` segment dropped, spec §4).

``ProducerRecord`` / ``RecordMetadata`` are the value types (P2); ``Producer`` /
``KafkaProducer`` / ``MockProducer`` and their ``Async`` peers, plus the
``DeliveryCallback`` alias, are the client family (P4).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from .async_producer import AsyncProducer
from .kafka_producer import AsyncKafkaProducer, KafkaProducer
from .mock_producer import AsyncMockProducer, MockProducer
from .producer import Producer
from .producer_record import ProducerRecord
from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka.common.errors import KafkaError

# Java: org.apache.kafka.clients.producer.Callback. Defined here beside its use
# (the ``on_delivery`` parameter of ``send``), spec §6.1.
DeliveryCallback = Callable[["RecordMetadata | None", "KafkaError | None"], None]

__all__ = [
    "AsyncKafkaProducer",
    "AsyncMockProducer",
    "AsyncProducer",
    "DeliveryCallback",
    "KafkaProducer",
    "MockProducer",
    "Producer",
    "ProducerRecord",
    "RecordMetadata",
]
