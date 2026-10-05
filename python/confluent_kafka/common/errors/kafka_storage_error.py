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

"""``KafkaStorageError``: Java's ``org.apache.kafka.common.errors.KafkaStorageException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.invalid_metadata_error import InvalidMetadataError

__all__ = ["KafkaStorageError"]


class KafkaStorageError(InvalidMetadataError):
    """Miscellaneous disk-related IOException occurred when handling a request.
    Client should request metadata update and retry if the response shows
    KafkaStorageException

    Here are the guidelines on how to handle KafkaStorageException and
    IOException:

    1) If the server has not finished loading logs, IOException does not need
    to be converted to KafkaStorageException 2) After the server has finished
    loading logs, IOException should be caught and trigger
    LogDirFailureChannel.maybeAddOfflineLogDir() Then the IOException should
    either be swallowed and logged, or be converted and re-thrown as
    KafkaStorageException 3) It is preferred for IOException to be caught in
    Log rather than in ReplicaManager or LogSegment.

    Java: ``org.apache.kafka.common.errors.KafkaStorageException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 56  # kafka_common_ErrorCode_KAFKA_STORAGE_ERROR

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
