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

"""Generated JDK-analog errors for the ``confluent_kafka`` root.

GENERATED, DO NOT EDIT. Produced from the Java exception sources by
`cargo xtask generate-error-codes`, cross-checked against the FFI
`kafka_common_ErrorCode_t` enum, and validated for staleness by
`cargo xtask check-generated`.
"""

from __future__ import annotations

import builtins
from typing import ClassVar


__all__ = [
    "ConcurrentModificationError",
    "IllegalArgumentError",
    "IllegalStateError",
    "TimeoutError",
]


class ConcurrentModificationError(RuntimeError):
    """Mirrors Java's ``java.util.ConcurrentModificationException``."""

    _ffi_id: ClassVar[int] = -2  # kafka_common_ErrorCode_LOCAL_CONCURRENT_MODIFICATION


class IllegalArgumentError(RuntimeError):
    """Mirrors Java's ``java.lang.IllegalArgumentException``."""

    _ffi_id: ClassVar[int] = -3  # kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT


class IllegalStateError(RuntimeError):
    """Mirrors Java's ``java.lang.IllegalStateException``."""

    _ffi_id: ClassVar[int] = -4  # kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE


class TimeoutError(builtins.TimeoutError):
    """Mirrors Java's ``java.util.concurrent.TimeoutException``."""

    _ffi_id: ClassVar[int] = -5  # kafka_common_ErrorCode_LOCAL_TIMEOUT
