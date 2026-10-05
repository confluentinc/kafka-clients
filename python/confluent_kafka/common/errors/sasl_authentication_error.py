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

"""``SaslAuthenticationError``: Java's ``org.apache.kafka.common.errors.SaslAuthenticationException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka.common.errors.authentication_error import AuthenticationError

__all__ = ["SaslAuthenticationError"]


class SaslAuthenticationError(AuthenticationError):
    """This exception indicates that SASL authentication has failed. The error
    message in the exception indicates the actual cause of failure.

    SASL authentication failures typically indicate invalid credentials, but
    could also include other failures specific to the SASL mechanism used for
    authentication.

    Note:If ``SaslServer.evaluateResponse(byte[])`` throws this exception
    during authentication, the message from the exception will be sent to
    clients in the SaslAuthenticate response. Custom ``SaslServer``
    implementations may throw this exception in order to provide custom error
    messages to clients, but should take care not to include any
    security-critical information in the message that should not be leaked to
    unauthenticated clients.

    Java: ``org.apache.kafka.common.errors.SaslAuthenticationException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = 58  # kafka_common_ErrorCode_SASL_AUTHENTICATION_FAILED

    def __init__(
        self,
        *,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
