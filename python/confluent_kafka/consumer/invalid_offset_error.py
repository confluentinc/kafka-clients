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

"""``InvalidOffsetError``: Java's ``org.apache.kafka.clients.consumer.InvalidOffsetException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from confluent_kafka import _throwable
from confluent_kafka.common.kafka_error import KafkaError

if TYPE_CHECKING:
    from confluent_kafka.common.topic_partition import TopicPartition

__all__ = ["InvalidOffsetError"]


class InvalidOffsetError(KafkaError):
    """Thrown when the offset for a set of partitions is invalid (either undefined
    or out of range), and no reset policy has been configured.

    Java: ``org.apache.kafka.clients.consumer.InvalidOffsetException``.

    A catch-only base: constructing it raises ``TypeError``.
    """

    __module__ = "confluent_kafka.consumer"

    def __init__(
        self,
        *,
        message: str,
    ) -> None:
        if type(self) is InvalidOffsetError:
            raise TypeError(
                "InvalidOffsetError is an abstract catch-only base; it is never raised directly"
            )
        _throwable.init(self, message, None)
        self._java_kwargs = _throwable.kwargs(message=message)

    def partitions(self) -> set[TopicPartition]:
        """Java's ``partitions()``."""
        raise NotImplementedError
