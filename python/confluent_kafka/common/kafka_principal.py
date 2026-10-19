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

"""``KafkaPrincipal``: Java's ``org.apache.kafka.common.security.auth.KafkaPrincipal``.

In ``confluent_kafka.common``, the nearest module above
``common.security.auth``. Java's ``java.security.Principal`` is dropped; the
class keeps the methods it declares, and ``Principal.implies(Subject)`` is not
generated (CLAUDE.md, Python Binding Conventions, Types, JDK interfaces).

Java's two constructors are one keyword-only constructor; ``(principalType,
name)`` passes ``false`` for ``tokenAuthenticated``. Java's
``tokenAuthenticated()`` getter and ``tokenAuthenticated(boolean)`` setter are
one method: no argument reads, one argument sets and returns ``None`` (Java's
``void``). The flag is mutable, as in Java, and takes no part in ``__eq__`` /
``__hash__``.
"""

from __future__ import annotations

from typing import ClassVar, overload

from confluent_kafka._args import MISSING

__all__ = ["KafkaPrincipal"]


class KafkaPrincipal:
    """Principals in Kafka are defined by a type and a name. The principal
    type will always be ``"User"`` for the simple authorizer that is enabled by
    default, but custom authorizers can leverage different principal types
    (such as to enable group or role-based ACLs). The ``KafkaPrincipalBuilder``
    interface is used when you need to derive a different principal type from
    the authentication context, or when you need to represent relations
    between different principals. For example, you could extend
    ``KafkaPrincipal`` in order to link a user principal to one or more role
    principals.

    For custom extensions of ``KafkaPrincipal``, there two key points to keep
    in mind:

    1. To be compatible with the ACL APIs provided by Kafka (including the
       command line tool), each ACL can only represent a permission granted to
       a single principal (consisting of a principal type and name). It is
       possible to use richer ACL semantics, but you must implement your own
       mechanisms for adding and removing ACLs.
    2. In general, ``KafkaPrincipal`` extensions are only useful when the
       corresponding Authorizer is also aware of the extension. If you have a
       ``KafkaPrincipalBuilder`` which derives user groups from the
       authentication context (e.g. from an SSL client certificate), then you
       need a custom authorizer which is capable of using the additional group
       information.

    Java: ``org.apache.kafka.common.security.auth.KafkaPrincipal``.
    """

    USER_TYPE = "User"
    ANONYMOUS: ClassVar[KafkaPrincipal]

    __slots__ = ("_principal_type", "_name", "_token_authenticated")

    def __init__(self, *, principal_type: str, name: str,
                 token_authenticated: bool = MISSING) -> None:
        if token_authenticated is MISSING:
            token_authenticated = False  # Java's (principalType, name) passes false
        if principal_type is None:
            raise TypeError("principal_type must not be None")
        self._principal_type = principal_type
        if name is None:
            raise TypeError("name must not be None")
        self._name = name
        self._token_authenticated = token_authenticated

    def __str__(self) -> str:
        return self._principal_type + ":" + self._name

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, KafkaPrincipal)
        return self._principal_type == other._principal_type and self._name == other._name

    def __hash__(self) -> int:
        return hash((self._principal_type, self._name))

    def name(self) -> str:
        return self._name

    def principal_type(self) -> str:
        return self._principal_type

    @overload
    def token_authenticated(self) -> bool: ...
    @overload
    def token_authenticated(self, *, token_authenticated: bool) -> None: ...

    def token_authenticated(self, *, token_authenticated: bool = MISSING) -> bool | None:
        if token_authenticated is MISSING:
            return self._token_authenticated
        if token_authenticated is None:  # a boolean: Java cannot pass null
            raise TypeError("token_authenticated must not be None")
        self._token_authenticated = token_authenticated
        return None


KafkaPrincipal.ANONYMOUS = KafkaPrincipal(principal_type=KafkaPrincipal.USER_TYPE,
                                          name="ANONYMOUS")
