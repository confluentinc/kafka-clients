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

"""``ScramMechanism``: Java's ``org.apache.kafka.clients.admin.ScramMechanism``.

An ``enum.IntEnum`` valued by Java's one integer field, ``type`` (CLAUDE.md,
Python Binding Conventions, Class family). Its other field,
``mechanismName``, is reached through Java's own ``mechanism_name()``.
"""

from __future__ import annotations

from enum import IntEnum

__all__ = ["ScramMechanism"]


class ScramMechanism(IntEnum):
    """Representation of a SASL/SCRAM Mechanism.

    See KIP-554: Add Broker-side SCRAM Config API.

    This code is duplicated in
    ``org.apache.kafka.common.security.scram.internals.ScramMechanism``. The
    type field in both files must match and must not change. The type field is
    used both for passing ScramCredentialUpsertion and for the internal
    UserScramCredentialRecord. Do not change the type field.

    Java: ``org.apache.kafka.clients.admin.ScramMechanism``.
    """

    UNKNOWN = 0
    SCRAM_SHA_256 = 1
    SCRAM_SHA_512 = 2

    @staticmethod
    def from_type(*, type: int) -> ScramMechanism:  # noqa: A002 - Java's name
        """The instance corresponding to the given type indicator, otherwise
        ``UNKNOWN``."""
        for scram_mechanism in ScramMechanism:
            if scram_mechanism.value == type:
                return scram_mechanism
        return ScramMechanism.UNKNOWN

    @staticmethod
    def from_mechanism_name(*, mechanism_name: str) -> ScramMechanism:
        """The corresponding SASL SCRAM mechanism enum, otherwise ``UNKNOWN``
        (RFC 5802, section 4)."""
        for mechanism in ScramMechanism:
            if mechanism.mechanism_name() == mechanism_name:
                return mechanism
        return ScramMechanism.UNKNOWN

    def mechanism_name(self) -> str:
        """The corresponding SASL SCRAM mechanism name (RFC 5802, section 4)."""
        return self.name.replace("_", "-")

    def type(self) -> int:  # noqa: A003 - Java's name
        """The type indicator for this SASL SCRAM mechanism."""
        return self.value

    def __str__(self) -> str:
        return self.name
