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

"""``UnknownTopicOrPartitionError``: Java's ``org.apache.kafka.common.errors.UnknownTopicOrPartitionException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.invalid_metadata_error import InvalidMetadataError

__all__ = ["UnknownTopicOrPartitionError"]


class UnknownTopicOrPartitionError(InvalidMetadataError):
    """This topic/partition doesn't exist. This exception is used in contexts
    where a topic doesn't seem to exist based on possibly stale metadata. This
    exception is retriable because the topic or partition might subsequently be
    created.

    Java: ``org.apache.kafka.common.errors.UnknownTopicOrPartitionException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 3  # kafka_common_ErrorCode_UNKNOWN_TOPIC_OR_PARTITION

    def __init__(
        self,
        *,
        message: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        if message is not None:
            _throwable.init(self, message, cause)
        elif message is None and cause is not None:
            _throwable.init(self, _throwable.cause_message(cause), cause)
        else:
            _throwable.init(self, None, None)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
