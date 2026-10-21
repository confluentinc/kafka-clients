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

"""``ScramCredentialInfo``: Java's
``org.apache.kafka.clients.admin.ScramCredentialInfo``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from confluent_kafka._java import java_str

if TYPE_CHECKING:
    from .scram_mechanism import ScramMechanism

__all__ = ["ScramCredentialInfo"]


class ScramCredentialInfo:
    """Mechanism and iterations for a SASL/SCRAM credential associated with a
    user.

    See KIP-554: Add Broker-side SCRAM Config API.

    Java: ``org.apache.kafka.clients.admin.ScramCredentialInfo``.
    """

    __slots__ = ("_mechanism", "_iterations")

    def __init__(self, *, mechanism: ScramMechanism, iterations: int) -> None:
        """``mechanism``: the required mechanism; ``iterations``: the number of
        iterations used when creating the credential."""
        if mechanism is None:
            raise TypeError("mechanism must not be None")
        self._mechanism = mechanism
        self._iterations = iterations

    def mechanism(self) -> ScramMechanism:
        """The mechanism."""
        return self._mechanism

    def iterations(self) -> int:
        """The number of iterations used when creating the credential."""
        return self._iterations

    def __str__(self) -> str:
        return ("ScramCredentialInfo{mechanism=" + java_str(self._mechanism)
                + ", iterations=" + str(self._iterations) + "}")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ScramCredentialInfo)
        return self._iterations == other._iterations and self._mechanism is other._mechanism

    def __hash__(self) -> int:
        return hash((self._mechanism, self._iterations))
