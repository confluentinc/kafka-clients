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

"""``IllegalStateError``: Java's ``java.lang.IllegalStateException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import Any, ClassVar

from confluent_kafka import _throwable

__all__ = ["IllegalStateError"]


class IllegalStateError(RuntimeError):
    """Java's built-in ``java.lang.IllegalStateException``."""

    __module__ = "confluent_kafka"

    _ffi_id: ClassVar[int] = -4  # kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE

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

    def __str__(self) -> str:
        """Java's ``getMessage()``; ``""`` when it is ``null``."""
        return _throwable.message_text(self)

    def __reduce__(self) -> str | tuple[Any, ...]:
        """Rebuild from the constructor arguments by name (``copy``, ``pickle``)."""
        return _throwable.reduce(self)
