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

"""``confluent_kafka.producer``: Java's ``org.apache.kafka.clients.producer``.

The records (``ProducerRecord``, ``RecordMetadata``), the client family
(``Producer``, ``KafkaProducer``, ``MockProducer`` and their ``Async`` peers),
the delivery callback alias and the errors Java declares in this package
(CLAUDE.md, Python Binding Conventions, Modules).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from .async_producer import AsyncProducer as AsyncProducer
from .kafka_producer import AsyncKafkaProducer as AsyncKafkaProducer
from .kafka_producer import KafkaProducer as KafkaProducer
from .mock_producer import AsyncMockProducer as AsyncMockProducer
from .mock_producer import MockProducer as MockProducer
from .producer import Producer as Producer
from .producer_record import ProducerRecord as ProducerRecord
from .record_metadata import RecordMetadata as RecordMetadata

if TYPE_CHECKING:
    from confluent_kafka.common.kafka_error import KafkaError

# Java: org.apache.kafka.clients.producer.Callback. Defined here beside its use
# (the ``callback`` parameter of ``send``).
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

# BEGIN GENERATED ERRORS (cargo xtask generate-error-codes; do not edit)
from .buffer_exhausted_error import BufferExhaustedError as BufferExhaustedError
__all__ += [
    "BufferExhaustedError",
]
# END GENERATED ERRORS
