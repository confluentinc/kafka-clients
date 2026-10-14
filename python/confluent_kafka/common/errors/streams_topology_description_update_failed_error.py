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

"""``StreamsTopologyDescriptionUpdateFailedError``: Java's ``org.apache.kafka.common.errors.StreamsTopologyDescriptionUpdateFailedException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.api_error import ApiError

__all__ = ["StreamsTopologyDescriptionUpdateFailedError"]


class StreamsTopologyDescriptionUpdateFailedError(ApiError):
    """Indicates that the streams group topology description plugin failed to
    process a StreamsGroupTopologyDescriptionUpdate request. The accompanying
    error message describes the underlying cause; the broker tracks the
    transient-vs-permanent distinction internally and does not reflect it on
    the wire.

    Java:
    ``org.apache.kafka.common.errors.StreamsTopologyDescriptionUpdateFailedException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 135  # kafka_common_Error_STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED

    def __init__(
        self,
        *,
        message: str,
    ) -> None:
        _throwable.init(self, message, None)
        self._java_kwargs = _throwable.kwargs(message=message)
