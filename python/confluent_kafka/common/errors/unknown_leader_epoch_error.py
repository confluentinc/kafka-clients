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

"""``UnknownLeaderEpochError``: Java's ``org.apache.kafka.common.errors.UnknownLeaderEpochException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.retriable_error import RetriableError

__all__ = ["UnknownLeaderEpochError"]


class UnknownLeaderEpochError(RetriableError):
    """The request contained a leader epoch which is larger than that on the
    broker that received the request. This can happen if the client observes a
    metadata update before it has been propagated to all brokers. Clients need
    not refresh metadata before retrying.

    Java: ``org.apache.kafka.common.errors.UnknownLeaderEpochException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 75  # kafka_common_ErrorCode_UNKNOWN_LEADER_EPOCH

    def __init__(
        self,
        *,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
