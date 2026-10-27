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

"""``UserScramCredentialsDescription``: Java's
``org.apache.kafka.clients.admin.UserScramCredentialsDescription``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from confluent_kafka._args import copy_of
from confluent_kafka._java import java_str

if TYPE_CHECKING:
    from .scram_credential_info import ScramCredentialInfo

__all__ = ["UserScramCredentialsDescription"]


class UserScramCredentialsDescription:
    """Representation of all SASL/SCRAM credentials associated with a user that
    can be retrieved, or an error indicating why credentials could not be
    retrieved.

    See KIP-554: Add Broker-side SCRAM Config API.

    Java: ``org.apache.kafka.clients.admin.UserScramCredentialsDescription``.
    """

    __slots__ = ("_name", "_credential_infos")

    def __init__(self, *, name: str, credential_infos: Sequence[ScramCredentialInfo]) -> None:
        """``name``: the required user name; ``credential_infos``: the required
        SASL/SCRAM credential representations for the user."""
        if name is None:
            raise TypeError("name must not be None")
        if credential_infos is None:
            raise TypeError("credential_infos must not be None")
        self._name = name
        self._credential_infos = copy_of(credential_infos, "credential_infos")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, UserScramCredentialsDescription)
        return self._name == other._name and self._credential_infos == other._credential_infos

    def __hash__(self) -> int:
        return hash((self._name, self._credential_infos))

    def __str__(self) -> str:
        return ("UserScramCredentialsDescription{name='" + self._name + "', credentialInfos="
                + java_str(self._credential_infos) + "}")

    def name(self) -> str:
        """The user name."""
        return self._name

    def credential_infos(self) -> list[ScramCredentialInfo]:
        """The always non-null list of SASL/SCRAM credential representations
        for the user."""
        return list(self._credential_infos)
