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

"""``confluent_kafka.consumer`` — mirror of
``org.apache.kafka.clients.consumer`` (the ``clients`` segment dropped, spec §4).

The record and offset value types, ``CloseOptions``, ``SubscriptionPattern`` and
``OffsetResetStrategy`` live here (P2). ``Consumer`` / ``KafkaConsumer`` /
``MockConsumer`` and their ``Async`` peers, ``ConsumerRebalanceListener`` and the
rest of §6.2 land here in P5. It also carries the consumer-package exceptions,
which Java places in this package (``CommitFailedException``,
``OffsetOutOfRangeException``, …).
"""

from __future__ import annotations

from ._generated_errors import *  # noqa: F401,F403 -- re-export the consumer errors
from ._generated_errors import __all__ as _errors_all
from .async_consumer import AsyncConsumer
from .async_kafka_consumer import AsyncKafkaConsumer
from .async_mock_consumer import AsyncMockConsumer
from .close_options import CloseOptions
from .consumer import Consumer
from .consumer_group_metadata import ConsumerGroupMetadata
from .consumer_rebalance_listener import CommitCallback, ConsumerRebalanceListener
from .consumer_record import ConsumerRecord
from .consumer_records import ConsumerRecords
from .kafka_consumer import KafkaConsumer
from .mock_consumer import MockConsumer
from .offset_and_metadata import OffsetAndMetadata
from .offset_and_timestamp import OffsetAndTimestamp
from .offset_reset_strategy import OffsetResetStrategy
from .subscription_pattern import SubscriptionPattern

__all__ = [
    *_errors_all,
    "AsyncConsumer",
    "AsyncKafkaConsumer",
    "AsyncMockConsumer",
    "CloseOptions",
    "CommitCallback",
    "Consumer",
    "ConsumerGroupMetadata",
    "ConsumerRebalanceListener",
    "ConsumerRecord",
    "ConsumerRecords",
    "KafkaConsumer",
    "MockConsumer",
    "OffsetAndMetadata",
    "OffsetAndTimestamp",
    "OffsetResetStrategy",
    "SubscriptionPattern",
]
