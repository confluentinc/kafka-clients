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

"""``WakeupError``: Java's ``org.apache.kafka.common.errors.WakeupException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.kafka_error import KafkaError

__all__ = ["WakeupError"]


class WakeupError(KafkaError):
    """Exception used to indicate preemption of a blocking operation by an
    external thread. For example,
    ``org.apache.kafka.clients.consumer.KafkaConsumer.wakeup`` can be used to
    break out of an active
    ``org.apache.kafka.clients.consumer.KafkaConsumer.poll(java.time.Duration)``,
    which would raise an instance of this exception.

    Java: ``org.apache.kafka.common.errors.WakeupException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = -18  # kafka_common_ErrorCode_WAKEUP

    def __init__(self) -> None:
        _throwable.init(self, None, None)
        self._java_kwargs = _throwable.kwargs()
