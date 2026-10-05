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

"""``NotLeaderOrFollowerError``: Java's ``org.apache.kafka.common.errors.NotLeaderOrFollowerException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.invalid_metadata_error import InvalidMetadataError

__all__ = ["NotLeaderOrFollowerError"]


class NotLeaderOrFollowerError(InvalidMetadataError):
    """Broker returns this error if a request could not be processed because the
    broker is not the leader or follower for a topic partition. This could be a
    transient exception during leader elections and reassignments. For
    `Produce` and other requests which are intended only for the leader, this
    exception indicates that the broker is not the current leader. For consumer
    `Fetch` requests which may be satisfied by a leader or follower, this
    exception indicates that the broker is not a replica of the topic
    partition.

    Java: ``org.apache.kafka.common.errors.NotLeaderOrFollowerException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 6  # kafka_common_ErrorCode_NOT_LEADER_OR_FOLLOWER

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
