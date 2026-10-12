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

"""``TopicAuthorizationError``: Java's ``org.apache.kafka.common.errors.TopicAuthorizationException``.

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
from confluent_kafka.common.errors.authorization_error import AuthorizationError

__all__ = ["TopicAuthorizationError"]


class TopicAuthorizationError(AuthorizationError):
    """Java: ``org.apache.kafka.common.errors.TopicAuthorizationException``."""

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 29  # kafka_common_ErrorCode_TOPIC_AUTHORIZATION_FAILED

    @java_forms(
        Form("message", "unauthorized_topics", defaults={"unauthorized_topics": ()}),
        Form("unauthorized_topics"),
        Form("message"),
    )
    def __init__(
        self,
        *,
        message: str | None = UNSET,
        unauthorized_topics: Iterable[str] = UNSET,
        _java_form: int = -1,
    ) -> None:
        unauthorized_topics = _throwable.materialize(unauthorized_topics)
        if _java_form == 0:
            _throwable.init(self, message, None)
            self._unauthorized_topics = _throwable.copy_set(unauthorized_topics)
        else:
            _throwable.init(self, "Not authorized to access topics: " + java_str(unauthorized_topics), None)
            self._unauthorized_topics = _throwable.copy_set(unauthorized_topics)
        self._java_kwargs = _throwable.kwargs(message=message, unauthorized_topics=unauthorized_topics)

    def unauthorized_topics(self) -> set[str]:
        """Java's ``unauthorizedTopics()``."""
        return self._unauthorized_topics
