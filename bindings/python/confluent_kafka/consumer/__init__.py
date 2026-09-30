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

"""``confluent_kafka.consumer``: Java's ``org.apache.kafka.clients.consumer``.

The records and offset types, ``CloseOptions``, ``SubscriptionPattern``,
``OffsetResetStrategy``, the client family (``Consumer``, ``KafkaConsumer``,
``MockConsumer`` and their ``Async`` peers), ``ConsumerRebalanceListener``, the
``OffsetCommitCallback`` alias, and the errors Java declares in this package
(``CommitFailedException``, ``OffsetOutOfRangeException``, …) (CLAUDE.md,
Python Binding Conventions, Modules).
"""

from __future__ import annotations

from .async_consumer import AsyncConsumer as AsyncConsumer
from .async_kafka_consumer import AsyncKafkaConsumer as AsyncKafkaConsumer
from .async_mock_consumer import AsyncMockConsumer as AsyncMockConsumer
from .close_options import CloseOptions as CloseOptions
from .consumer import Consumer as Consumer
from .consumer_group_metadata import ConsumerGroupMetadata as ConsumerGroupMetadata
from .consumer_rebalance_listener import ConsumerRebalanceListener as ConsumerRebalanceListener
from .consumer_record import ConsumerRecord as ConsumerRecord
from .consumer_records import ConsumerRecords as ConsumerRecords
from .kafka_consumer import KafkaConsumer as KafkaConsumer
from .mock_consumer import MockConsumer as MockConsumer
from .offset_and_metadata import OffsetAndMetadata as OffsetAndMetadata
from .offset_and_timestamp import OffsetAndTimestamp as OffsetAndTimestamp
from .offset_commit_callback import OffsetCommitCallback as OffsetCommitCallback
from .offset_reset_strategy import OffsetResetStrategy as OffsetResetStrategy
from .subscription_pattern import SubscriptionPattern as SubscriptionPattern

__all__ = [
    "AsyncConsumer",
    "AsyncKafkaConsumer",
    "AsyncMockConsumer",
    "CloseOptions",
    "Consumer",
    "ConsumerGroupMetadata",
    "ConsumerRebalanceListener",
    "ConsumerRecord",
    "ConsumerRecords",
    "KafkaConsumer",
    "MockConsumer",
    "OffsetAndMetadata",
    "OffsetAndTimestamp",
    "OffsetCommitCallback",
    "OffsetResetStrategy",
    "SubscriptionPattern",
]

# BEGIN GENERATED ERRORS (cargo xtask generate-error-codes; do not edit)
from .commit_failed_error import CommitFailedError as CommitFailedError
from .invalid_offset_error import InvalidOffsetError as InvalidOffsetError
from .log_truncation_error import LogTruncationError as LogTruncationError
from .no_offset_for_partition_error import NoOffsetForPartitionError as NoOffsetForPartitionError
from .offset_out_of_range_error import OffsetOutOfRangeError as OffsetOutOfRangeError
from .retriable_commit_failed_error import RetriableCommitFailedError as RetriableCommitFailedError
__all__ += [
    "CommitFailedError",
    "InvalidOffsetError",
    "LogTruncationError",
    "NoOffsetForPartitionError",
    "OffsetOutOfRangeError",
    "RetriableCommitFailedError",
]
# END GENERATED ERRORS
