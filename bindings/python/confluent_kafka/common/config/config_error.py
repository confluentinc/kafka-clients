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

"""``ConfigError``: Java's ``org.apache.kafka.common.config.ConfigException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import Any, ClassVar

from confluent_kafka import _throwable
from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka._java import java_str
from confluent_kafka.common.kafka_error import KafkaError

__all__ = ["ConfigError"]


class ConfigError(KafkaError):
    """Thrown if the user supplies an invalid configuration

    Java: ``org.apache.kafka.common.config.ConfigException``.
    """

    __module__ = "confluent_kafka.common.config"

    _ffi_id: ClassVar[int] = -10  # kafka_common_ErrorCode_CONFIG

    @java_forms(
        Form("message"),
        Form("name", "value"),
        Form("name", "value", "message", defaults={"message": None}),
    )
    def __init__(
        self,
        *,
        name: str | None = None,
        value: Any = None,
        message: str | None = UNSET,
        _java_form: int = -1,
    ) -> None:
        if _java_form == 0:
            _throwable.init(self, message, None)
        else:
            _throwable.init(self, "Invalid value " + java_str(value) + " for configuration " + java_str(name) + ("" if message is None else ": " + java_str(message)), None)
        self._java_kwargs = _throwable.kwargs(name=name, value=value, message=message)
