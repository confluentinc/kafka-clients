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

"""``UserScramCredentialAlteration``: Java's
``org.apache.kafka.clients.admin.UserScramCredentialAlteration``.

Java's class is ``abstract``, so constructing it raises ``TypeError``; its
subclasses construct as Java's do. Its protected
constructor is the ``__init__`` its subclasses call.
"""

from __future__ import annotations

from confluent_kafka.null_pointer_error import NullPointerError

__all__ = ["UserScramCredentialAlteration"]


class UserScramCredentialAlteration:
    """A request to alter a user's SASL/SCRAM credentials.

    See KIP-554: Add Broker-side SCRAM Config API.

    Java: ``org.apache.kafka.clients.admin.UserScramCredentialAlteration``.
    """

    __slots__ = ("_user",)

    def __init__(self, *, user: str) -> None:
        """``user``: the mandatory user. Java's protected constructor, for the
        subclasses."""
        if type(self) is UserScramCredentialAlteration:
            raise TypeError(
                "UserScramCredentialAlteration cannot be constructed directly; use one of "
                "its subclasses")
        if user is None:
            raise NullPointerError()
        self._user = user

    def user(self) -> str:
        """The always non-null user."""
        return self._user
