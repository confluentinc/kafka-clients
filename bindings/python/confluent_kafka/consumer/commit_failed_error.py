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

"""``CommitFailedError``: Java's ``org.apache.kafka.clients.consumer.CommitFailedException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.kafka_error import KafkaError

__all__ = ["CommitFailedError"]


class CommitFailedError(KafkaError):
    """This exception is raised when an offset commit with
    ``KafkaConsumer.commitSync()`` fails with an unrecoverable error. This can
    happen when a group rebalance completes before the commit could be
    successfully applied. In this case, the commit cannot generally be retried
    because some of the partitions may have already been assigned to another
    member in the group.

    Java: ``org.apache.kafka.clients.consumer.CommitFailedException``.
    """

    __module__ = "confluent_kafka.consumer"

    _ffi_id: ClassVar[int] = -19  # kafka_common_ErrorCode_CONSUMER_COMMIT_FAILED

    def __init__(
        self,
        *,
        message: str | None = None,
    ) -> None:
        if message is not None:
            _throwable.init(self, message, None)
        else:
            _throwable.init(self, "Commit cannot be completed since the group has already " + "rebalanced and assigned the partitions to another member. This means that the time " + "between subsequent calls to poll() was longer than the configured max.poll.interval.ms, " + "which typically implies that the poll loop is spending too much time message processing. " + "You can address this either by increasing max.poll.interval.ms or by reducing the maximum " + "size of batches returned in poll() with max.poll.records.", None)
        self._java_kwargs = _throwable.kwargs(message=message)
