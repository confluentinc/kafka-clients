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

"""``RefreshRetriableError``: Java's ``org.apache.kafka.common.errors.RefreshRetriableException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from confluent_kafka import _throwable
from confluent_kafka.common.errors.retriable_error import RetriableError

__all__ = ["RefreshRetriableError"]


class RefreshRetriableError(RetriableError):
    """Indicates that an operation failed due to outdated or invalid metadata,
    requiring a refresh (e.g., refreshing producer metadata) before retrying
    the request. The request can be modified or updated with fresh metadata
    before being retried.

    Java: ``org.apache.kafka.common.errors.RefreshRetriableException``.

    A catch-only base: constructing it raises ``TypeError``.
    """

    __module__ = "confluent_kafka.common.errors"

    def __init__(
        self,
        *,
        message: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        if type(self) is RefreshRetriableError:
            raise TypeError(
                "RefreshRetriableError is an abstract catch-only base; it is never raised directly"
            )
        if message is not None:
            _throwable.init(self, message, cause)
        elif message is None and cause is not None:
            _throwable.init(self, _throwable.cause_message(cause), cause)
        else:
            _throwable.init(self, None, None)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
