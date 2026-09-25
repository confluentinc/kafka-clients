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

"""Generated config error(s) for ``confluent_kafka.common.config``.

GENERATED, DO NOT EDIT. Produced from the Java exception sources by
`cargo xtask generate-error-codes`, cross-checked against the FFI
`kafka_common_ErrorCode_t` enum, and validated for staleness by
`cargo xtask check-generated`.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka.common.errors._base import KafkaError

__all__ = [
    "ConfigError",
]


class ConfigError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.common.config.ConfigException``."""

    _ffi_id: ClassVar[int] = -10  # kafka_common_ErrorCode_CONFIG
