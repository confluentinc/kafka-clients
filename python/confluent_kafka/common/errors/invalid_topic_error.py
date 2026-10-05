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

"""``InvalidTopicError``: Java's ``org.apache.kafka.common.errors.InvalidTopicException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.common.errors.invalid_configuration_error import InvalidConfigurationError

__all__ = ["InvalidTopicError"]


class InvalidTopicError(InvalidConfigurationError):
    """The client has attempted to perform an operation on an invalid topic. For
    example the topic name is too long, contains invalid characters etc. This
    exception is not retriable because the operation won't suddenly become
    valid.

    Java: ``org.apache.kafka.common.errors.InvalidTopicException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 17  # kafka_common_ErrorCode_INVALID_TOPIC_ERROR

    @java_forms(
        Form(),
        Form("message", "cause"),
        Form("message"),
        Form("cause"),
        Form("invalid_topics"),
        Form("message", "invalid_topics"),
    )
    def __init__(
        self,
        *,
        message: str | None = UNSET,
        cause: BaseException | None = None,
        invalid_topics: Iterable[str] = UNSET,
        _java_form: int = -1,
    ) -> None:
        invalid_topics = _throwable.materialize(invalid_topics)
        if _java_form == 0:
            _throwable.init(self, None, None)
            self._invalid_topics = set()
        elif _java_form == 1:
            _throwable.init(self, message, cause)
            self._invalid_topics = set()
        elif _java_form == 2:
            _throwable.init(self, message, None)
            self._invalid_topics = set()
        elif _java_form == 3:
            _throwable.init(self, _throwable.cause_message(cause), cause)
            self._invalid_topics = set()
        elif _java_form == 4:
            _throwable.init(self, "Invalid topics: " + java_str(invalid_topics), None)
            self._invalid_topics = _throwable.copy_set(invalid_topics)
        else:
            _throwable.init(self, message, None)
            self._invalid_topics = _throwable.copy_set(invalid_topics)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause, invalid_topics=invalid_topics)

    def invalid_topics(self) -> set[str]:
        """Java's ``invalidTopics()``."""
        return self._invalid_topics
