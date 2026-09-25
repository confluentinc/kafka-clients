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

"""``GroupAuthorizationError``: Java's ``org.apache.kafka.common.errors.GroupAuthorizationException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.authorization_error import AuthorizationError

__all__ = ["GroupAuthorizationError"]


class GroupAuthorizationError(AuthorizationError):
    """Java: ``org.apache.kafka.common.errors.GroupAuthorizationException``."""

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 30  # kafka_common_ErrorCode_GROUP_AUTHORIZATION_FAILED

    def __init__(
        self,
        *,
        message: str,
        group_id: str | None = None,
    ) -> None:
        _throwable.init(self, message, None)
        self._group_id = group_id
        self._java_kwargs = _throwable.kwargs(message=message, group_id=group_id)

    def group_id(self) -> str | None:
        """Java's ``groupId()``."""
        return self._group_id
