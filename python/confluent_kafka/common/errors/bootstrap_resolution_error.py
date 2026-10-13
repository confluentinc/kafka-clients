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

"""``BootstrapResolutionError``: Java's ``org.apache.kafka.common.errors.BootstrapResolutionException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.kafka_error import KafkaError

__all__ = ["BootstrapResolutionError"]


class BootstrapResolutionError(KafkaError):
    """Indicates that the ``org.apache.kafka.clients.NetworkClient`` was unable to
    resolve a DNS address within the time specified by
    ``org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG``.

    This is an unrecoverable error: the failure is permanently attached to the
    client and is re-thrown on every subsequent API call. Callers must close
    the client and construct a new one after resolving the underlying DNS or
    ``bootstrap.servers`` configuration issue.

    This exception is only thrown when ``bootstrap.resolve.timeout.ms`` is set
    to a positive value. This feature is evolving and may undergo
    compatibility-breaking changes in a minor release.

    Java: ``org.apache.kafka.common.errors.BootstrapResolutionException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = -29  # kafka_common_ErrorCode_BOOTSTRAP_RESOLUTION

    def __init__(
        self,
        *,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
