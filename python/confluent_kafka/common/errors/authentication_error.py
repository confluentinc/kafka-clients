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

"""``AuthenticationError``: Java's ``org.apache.kafka.common.errors.AuthenticationException``.

GENERATED, DO NOT EDIT. Produced from the Java source by
``cargo xtask generate-error-codes`` (CLAUDE.md, Python Binding Conventions,
Errors) and validated for staleness by ``cargo xtask check-generated``.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka import _throwable
from confluent_kafka._args import UNSET, Form, java_forms
from confluent_kafka.common.errors.invalid_configuration_error import InvalidConfigurationError

__all__ = ["AuthenticationError"]


class AuthenticationError(InvalidConfigurationError):
    """This exception indicates that SASL authentication has failed. On
    authentication failure, clients abort the operation requested and raise one
    of the subclasses of this exception:

    ``SaslAuthenticationException`` if SASL handshake fails with invalid
    credentials or any other failure specific to the SASL mechanism used for
    authentication ``UnsupportedSaslMechanismException`` if the SASL mechanism
    requested by the client is not supported on the broker.
    ``IllegalSaslStateException`` if an unexpected request is received on
    during SASL handshake. This could be due to misconfigured security
    protocol. ``SslAuthenticationException`` if SSL handshake failed due to any
    ``SSLException``.

    Java: ``org.apache.kafka.common.errors.AuthenticationException``.
    """

    __module__ = "confluent_kafka.common.errors"

    _ffi_id: ClassVar[int] = -7  # kafka_common_ErrorCode_AUTHENTICATION

    @java_forms(
        Form("message"),
        Form("cause"),
        Form("message", "cause"),
    )
    def __init__(
        self,
        *,
        message: str | None = UNSET,
        cause: BaseException | None = UNSET,
        _java_form: int = -1,
    ) -> None:
        if _java_form == 0:
            _throwable.init(self, message, None)
        elif _java_form == 1:
            _throwable.init(self, _throwable.cause_message(cause), cause)
        else:
            _throwable.init(self, message, cause)
        self._java_kwargs = _throwable.kwargs(message=message, cause=cause)
