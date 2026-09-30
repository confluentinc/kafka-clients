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

"""``ReplicaNotAvailableError``: Java's ``org.apache.kafka.common.errors.ReplicaNotAvailableException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka._args import Form, java_forms
from confluent_kafka.common.errors.invalid_metadata_error import InvalidMetadataError

__all__ = ["ReplicaNotAvailableError"]


class ReplicaNotAvailableError(InvalidMetadataError):
    """The replica is not available for the requested topic partition. This may be
    a transient exception during reassignments. From version 2.6 onwards, Fetch
    requests and other requests intended only for the leader or follower of the
    topic partition return ``NotLeaderOrFollowerException`` if the broker is a
    not a replica of the partition.

    Java: ``org.apache.kafka.common.errors.ReplicaNotAvailableException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 9  # kafka_common_ErrorCode_REPLICA_NOT_AVAILABLE

    @java_forms(
        Form("message"),
        Form("message", "cause"),
        Form("cause"),
    )
    def __init__(
        self,
        *,
        message: str | None = None,
        cause: BaseException | None = None,
        _java_form: int = -1,
    ) -> None:
        if _java_form == 0:
            _throwable.init(self, message, None)
        elif _java_form == 1:
            _throwable.init(self, message, cause)
        else:
            _throwable.init(self, _throwable.cause_message(cause), cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
