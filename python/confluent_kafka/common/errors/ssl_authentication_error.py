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

"""``SslAuthenticationError``: Java's ``org.apache.kafka.common.errors.SslAuthenticationException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.authentication_error import AuthenticationError

__all__ = ["SslAuthenticationError"]


class SslAuthenticationError(AuthenticationError):
    """This exception indicates that SSL handshake has failed. See ``getCause()``
    for the ``SSLException`` that caused this failure.

    SSL handshake failures in clients may indicate client authentication
    failure due to untrusted certificates if server is configured to request
    client certificates. Handshake failures could also indicate misconfigured
    security including protocol/cipher suite mismatch, server certificate
    authentication failure or server host name verification failure.

    Java: ``org.apache.kafka.common.errors.SslAuthenticationException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = -16  # kafka_common_ErrorCode_SSL_AUTHENTICATION

    def __init__(
        self,
        *,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
