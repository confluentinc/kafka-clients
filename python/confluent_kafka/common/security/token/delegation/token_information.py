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

"""``TokenInformation``: Java's
``org.apache.kafka.common.security.token.delegation.TokenInformation``.

In ``confluent_kafka.common.security.token.delegation``, the module of its Java package. Java's two constructors are one
keyword-only constructor: ``(tokenId, owner, renewers, issueTimestamp,
maxTimestamp, expiryTimestamp)`` passes ``owner`` as the token requester, so
leaving ``token_requester`` out selects that form; a ``None`` is Java's null,
stored as given. A ``None`` ``renewers`` is stored too, as Java's field, and the
methods that iterate it raise ``TypeError`` where Java's dereference throws. The expiry
timestamp is mutable through ``set_expiry_timestamp``, as in Java.

As in Java, ``__eq__`` ignores the expiry timestamp while ``__hash__`` includes
it (``org.apache.kafka.common.security.token.delegation.TokenInformation#equals``,
``#hashCode``).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import cast, overload

from confluent_kafka._args import MISSING
from confluent_kafka._java import java_str
from confluent_kafka.common.security.auth.kafka_principal import KafkaPrincipal

__all__ = ["TokenInformation"]


class TokenInformation:
    """A class representing a delegation token details.

    Java: ``org.apache.kafka.common.security.token.delegation.TokenInformation``.
    """

    __slots__ = ("_owner", "_token_requester", "_renewers", "_issue_timestamp",
                 "_max_timestamp", "_expiry_timestamp", "_token_id")

    @overload
    def __init__(self, *, token_id: str, owner: KafkaPrincipal,
                 renewers: Iterable[KafkaPrincipal], issue_timestamp: int, max_timestamp: int,
                 expiry_timestamp: int) -> None: ...
    @overload
    def __init__(self, *, token_id: str, owner: KafkaPrincipal, token_requester: KafkaPrincipal,
                 renewers: Iterable[KafkaPrincipal], issue_timestamp: int, max_timestamp: int,
                 expiry_timestamp: int) -> None: ...
    def __init__(self, *, token_id: str, owner: KafkaPrincipal,
                 token_requester: KafkaPrincipal = MISSING,
                 renewers: Iterable[KafkaPrincipal], issue_timestamp: int, max_timestamp: int,
                 expiry_timestamp: int) -> None:
        self._token_id = token_id
        self._owner = owner
        # Java's shorter constructor passes the owner as the token requester;
        # a None is Java's null, stored as given.
        self._token_requester = owner if token_requester is MISSING else token_requester
        self._renewers: tuple[KafkaPrincipal, ...] | None = (
            None if renewers is None else tuple(renewers))
        self._issue_timestamp = issue_timestamp
        self._max_timestamp = max_timestamp
        self._expiry_timestamp = expiry_timestamp

    @staticmethod
    def from_record(*, token_id: str, owner: KafkaPrincipal, token_requester: KafkaPrincipal,
                    renewers: Iterable[KafkaPrincipal], issue_timestamp: int, max_timestamp: int,
                    expiry_timestamp: int) -> TokenInformation:
        """Convert record elements into a ``TokenInformation``."""
        return TokenInformation(token_id=token_id, owner=owner, token_requester=token_requester,
                                renewers=renewers, issue_timestamp=issue_timestamp,
                                max_timestamp=max_timestamp, expiry_timestamp=expiry_timestamp)

    def owner(self) -> KafkaPrincipal:
        return self._owner

    def owner_as_string(self) -> str:
        if self._owner is None:
            # Java's owner.toString() dereferences it (Types).
            raise TypeError("owner must not be None")
        return str(self._owner)

    def token_requester(self) -> KafkaPrincipal:
        return self._token_requester

    def token_requester_as_string(self) -> str:
        if self._token_requester is None:
            # Java's tokenRequester.toString() dereferences it (Types).
            raise TypeError("token_requester must not be None")
        return str(self._token_requester)

    def renewers(self) -> list[KafkaPrincipal]:
        # Java returns its field, null included.
        return cast("list[KafkaPrincipal]", None if self._renewers is None else list(self._renewers))

    def renewers_as_string(self) -> list[str]:
        return [str(renewer) for renewer in self._iterated_renewers()]

    def _iterated_renewers(self) -> tuple[KafkaPrincipal, ...]:
        """The renewers where Java iterates them: a null one throws."""
        if self._renewers is None:
            raise TypeError("renewers must not be None")
        return self._renewers

    def issue_timestamp(self) -> int:
        return self._issue_timestamp

    def expiry_timestamp(self) -> int:
        return self._expiry_timestamp

    def set_expiry_timestamp(self, *, expiry_timestamp: int) -> None:
        self._expiry_timestamp = expiry_timestamp

    def token_id(self) -> str:
        return self._token_id

    def max_timestamp(self) -> int:
        return self._max_timestamp

    def owner_or_renewer(self, *, principal: KafkaPrincipal) -> bool:
        return (self._owner == principal or self._token_requester == principal
                or principal in self._iterated_renewers())

    def __str__(self) -> str:
        return ("TokenInformation{owner=" + java_str(self._owner)
                + ", tokenRequester=" + java_str(self._token_requester)
                + ", renewers=" + java_str(None if self._renewers is None
                                           else list(self._renewers))
                + ", issueTimestamp=" + str(self._issue_timestamp)
                + ", maxTimestamp=" + str(self._max_timestamp)
                + ", expiryTimestamp=" + str(self._expiry_timestamp)
                + ", tokenId='" + java_str(self._token_id) + "'}")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, TokenInformation)
        return (self._issue_timestamp == other._issue_timestamp
                and self._max_timestamp == other._max_timestamp
                and self._owner == other._owner
                and self._token_requester == other._token_requester
                and self._renewers == other._renewers
                and self._token_id == other._token_id)

    def __hash__(self) -> int:
        return hash((self._owner, self._token_requester, self._renewers, self._issue_timestamp,
                     self._max_timestamp, self._expiry_timestamp, self._token_id))
