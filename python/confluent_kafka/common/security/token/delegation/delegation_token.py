# Copyright 2026 Confluent Inc.
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

"""``DelegationToken``: Java's
``org.apache.kafka.common.security.token.delegation.DelegationToken``.

In ``confluent_kafka.common.security.token.delegation``, the module of its Java package. Java's ``byte[] hmac`` is ``bytes``, in
and out (CLAUDE.md, Python Binding Conventions, Types); ``__eq__`` compares it
in constant time, as Java's ``MessageDigest.isEqual`` does.
"""

from __future__ import annotations

import base64
import hmac as _hmac

from confluent_kafka._java import java_str
from confluent_kafka.common.security.token.delegation.token_information import TokenInformation

__all__ = ["DelegationToken"]


class DelegationToken:
    """A class representing a delegation token.

    Java: ``org.apache.kafka.common.security.token.delegation.DelegationToken``.
    """

    __slots__ = ("_token_information", "_hmac")

    def __init__(self, *, token_information: TokenInformation, hmac: bytes) -> None:
        self._token_information = token_information
        self._hmac = bytes(hmac)

    def token_info(self) -> TokenInformation:
        return self._token_information

    def hmac(self) -> bytes:
        return self._hmac

    def hmac_as_base64_string(self) -> str:
        return base64.b64encode(self._hmac).decode("ascii")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, DelegationToken)
        return (self._token_information == other._token_information
                and _hmac.compare_digest(self._hmac, other._hmac))

    def __hash__(self) -> int:
        return hash((self._token_information, self._hmac))

    def __str__(self) -> str:
        return ("DelegationToken{tokenInformation=" + java_str(self._token_information)
                + ", hmac=[*******]}")
