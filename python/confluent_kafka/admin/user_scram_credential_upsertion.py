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

"""``UserScramCredentialUpsertion``: Java's
``org.apache.kafka.clients.admin.UserScramCredentialUpsertion``.

Java's three constructors ``(user, credentialInfo, String password)``,
``(user, credentialInfo, byte[] password)`` and ``(user, credentialInfo, byte[]
password, byte[] salt)`` are one keyword-only constructor (CLAUDE.md, Python
Binding Conventions, Signatures): ``password`` is ``str | bytes``, dispatched by
``isinstance``, and ``salt``, which the shorter forms fill with a random salt
(a computed value), defaults to ``None``. A ``str`` password with a ``salt`` is
no Java constructor and raises ``TypeError``. The random salt is Java's
``ScramFormatter.secureRandomBytes``: the UTF-8 bytes of a random 130-bit
integer written in base 36. Java declares no ``equals`` / ``hashCode`` /
``toString``.
"""

from __future__ import annotations

import secrets
from typing import TYPE_CHECKING, overload


from .user_scram_credential_alteration import UserScramCredentialAlteration

if TYPE_CHECKING:
    from .scram_credential_info import ScramCredentialInfo

__all__ = ["UserScramCredentialUpsertion"]

_DIGITS = "0123456789abcdefghijklmnopqrstuvwxyz"


def _generate_random_salt() -> bytes:
    """``ScramFormatter.secureRandomBytes(new SecureRandom())``:
    ``new BigInteger(130, random).toString(Character.MAX_RADIX)`` as UTF-8."""
    n = secrets.randbits(130)
    text = ""
    while True:
        n, d = divmod(n, 36)
        text = _DIGITS[d] + text
        if n == 0:
            break
    return text.encode("utf_8")


class UserScramCredentialUpsertion(UserScramCredentialAlteration):
    """A request to update/insert a SASL/SCRAM credential for a user.

    See KIP-554: Add Broker-side SCRAM Config API.

    Java: ``org.apache.kafka.clients.admin.UserScramCredentialUpsertion``.
    """

    __slots__ = ("_info", "_salt", "_password")

    @overload
    def __init__(self, *, user: str, credential_info: ScramCredentialInfo,
                 password: str) -> None: ...
    @overload
    def __init__(self, *, user: str, credential_info: ScramCredentialInfo,
                 password: bytes) -> None: ...
    @overload
    def __init__(self, *, user: str, credential_info: ScramCredentialInfo, password: bytes,
                 salt: bytes) -> None: ...

    def __init__(self, *, user: str, credential_info: ScramCredentialInfo,
                 password: str | bytes, salt: bytes | None = None) -> None:
        """``user``: the user for which the credential is to be
        updated/inserted; ``credential_info``: the mechanism and iterations to
        be used; ``password``: the password; ``salt``: the salt to be used, a
        random salt when it is not given."""
        if isinstance(password, str):
            if salt is not None:
                raise TypeError(
                    "UserScramCredentialUpsertion() takes a bytes password with a salt; "
                    "got a str password")
            password = password.encode("utf_8")
        if salt is None:
            salt = _generate_random_salt()
        if user is None:
            raise TypeError("user must not be None")
        super().__init__(user=user)
        if credential_info is None:
            raise TypeError("credential_info must not be None")
        if password is None:
            raise TypeError("password must not be None")
        self._info = credential_info
        self._password = bytes(password)
        self._salt = bytes(salt)

    def credential_info(self) -> ScramCredentialInfo:
        """The mechanism and iterations."""
        return self._info

    def salt(self) -> bytes:
        """The salt."""
        return self._salt

    def password(self) -> bytes:
        """The password."""
        return self._password
