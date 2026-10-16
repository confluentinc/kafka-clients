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

"""``UserScramCredentialDeletion``: Java's
``org.apache.kafka.clients.admin.UserScramCredentialDeletion``.

Java declares no ``equals`` / ``hashCode`` / ``toString``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from confluent_kafka.null_pointer_error import NullPointerError

from .user_scram_credential_alteration import UserScramCredentialAlteration

if TYPE_CHECKING:
    from .scram_mechanism import ScramMechanism

__all__ = ["UserScramCredentialDeletion"]


class UserScramCredentialDeletion(UserScramCredentialAlteration):
    """A request to delete a SASL/SCRAM credential for a user.

    See KIP-554: Add Broker-side SCRAM Config API.

    Java: ``org.apache.kafka.clients.admin.UserScramCredentialDeletion``.
    """

    __slots__ = ("_mechanism",)

    def __init__(self, *, user: str, mechanism: ScramMechanism) -> None:
        """``user``: the mandatory user; ``mechanism``: the mandatory
        mechanism."""
        super().__init__(user=user)
        if mechanism is None:
            raise NullPointerError()
        self._mechanism = mechanism

    def mechanism(self) -> ScramMechanism:
        """The always non-null mechanism."""
        return self._mechanism
