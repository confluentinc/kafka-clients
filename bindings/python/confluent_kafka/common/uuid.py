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

"""``Uuid`` — a 128-bit immutable universally unique identifier.

Translated from ``org.apache.kafka.common.Uuid`` (Apache Kafka 4.3.1). Ported
faithfully, including the base64 ``__str__`` / ``from_string`` round-trip, the
signed 64-bit ``(msb, lsb)`` ordering (Java ``Comparable<Uuid>``), the reserved
set, and the ``random_uuid`` reserved-range/leading-dash rejection rule.

Java's ``long`` is signed; ``most_significant_bits()`` and
``least_significant_bits()`` return signed 64-bit values, matching Java's
accessors. Comparisons therefore use signed ordering, exactly as Java's
``compareTo`` does.
"""

from __future__ import annotations

import base64
import uuid as _uuid
from typing import ClassVar

from confluent_kafka import IllegalArgumentError

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_UINT64_MASK = (1 << 64) - 1


def _to_signed64(value: int) -> int:
    """Interpret the low 64 bits of ``value`` as a signed 64-bit integer."""
    value &= _UINT64_MASK
    if value > _INT64_MAX:
        value -= 1 << 64
    return value


def _to_unsigned64(value: int) -> int:
    """Interpret a (possibly negative) 64-bit value as unsigned."""
    return value & _UINT64_MASK


class Uuid:
    """A 128-bit immutable universally unique identifier.

    Java: ``org.apache.kafka.common.Uuid`` (``Uuid(long mostSigBits, long
    leastSigBits)``). ``toString()`` prints a URL-safe base64 encoding of the
    16 bytes; ``fromString`` decodes it.
    """

    __slots__ = ("_msb", "_lsb")

    # Reserved instances (declared here; the constant objects are bound below,
    # after the class body, because they are Uuid instances themselves).
    ZERO_UUID: ClassVar[Uuid]
    ONE_UUID: ClassVar[Uuid]
    METADATA_TOPIC_ID: ClassVar[Uuid]
    RESERVED: ClassVar[frozenset[Uuid]]

    def __init__(self, *, most_significant_bits: int,
                 least_significant_bits: int) -> None:
        # Java stores the two longs verbatim (signed). Normalize any value the
        # caller passes into the signed 64-bit range so equality/ordering match
        # Java regardless of whether an unsigned form was supplied.
        self._msb = _to_signed64(most_significant_bits)
        self._lsb = _to_signed64(least_significant_bits)

    @staticmethod
    def _unsafe_random_uuid() -> Uuid:
        j = _uuid.uuid4()
        raw = j.int  # 128-bit unsigned
        msb = (raw >> 64) & _UINT64_MASK
        lsb = raw & _UINT64_MASK
        return Uuid(most_significant_bits=msb, least_significant_bits=lsb)

    @staticmethod
    def random_uuid() -> Uuid:
        """A type-4 (pseudo-randomly generated) UUID.

        Java ``randomUuid``: never equal to ``ZERO_UUID`` / ``ONE_UUID``, and
        never one whose string representation starts with a dash (``-``).
        """
        candidate = Uuid._unsafe_random_uuid()
        while candidate in Uuid.RESERVED or str(candidate).startswith("-"):
            candidate = Uuid._unsafe_random_uuid()
        return candidate

    @staticmethod
    def from_string(*, s: str) -> Uuid:
        """Create a ``Uuid`` from the base64 string encoding ``__str__`` emits.

        Java ``fromString``: rejects strings longer than 24 characters, and
        rejects a decoding that is not exactly 16 bytes, both with
        ``IllegalArgumentException``.
        """
        if len(s) > 24:
            raise IllegalArgumentError(
                f"Input string with prefix `{s[:24]}` is too long to be "
                f"decoded as a base64 UUID"
            )
        # Java's URL decoder tolerates the missing padding; add it back for
        # Python's strict decoder.
        padded = s + "=" * (-len(s) % 4)
        try:
            raw = base64.urlsafe_b64decode(padded)
        except Exception as exc:  # noqa: BLE001 - mirror Java's IllegalArgumentException reject
            raise IllegalArgumentError(
                f"Input string `{s}` could not be decoded as a base64 UUID"
            ) from exc
        if len(raw) != 16:
            raise IllegalArgumentError(
                f"Input string `{s}` decoded as {len(raw)} bytes, which is not "
                f"equal to the expected 16 bytes of a base64-encoded UUID"
            )
        msb = int.from_bytes(raw[0:8], "big", signed=True)
        lsb = int.from_bytes(raw[8:16], "big", signed=True)
        return Uuid(most_significant_bits=msb, least_significant_bits=lsb)

    def most_significant_bits(self) -> int:
        return self._msb

    def least_significant_bits(self) -> int:
        return self._lsb

    def _bytes(self) -> bytes:
        return (_to_unsigned64(self._msb).to_bytes(8, "big")
                + _to_unsigned64(self._lsb).to_bytes(8, "big"))

    def __str__(self) -> str:
        # Java: Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
        return base64.urlsafe_b64encode(self._bytes()).rstrip(b"=").decode("ascii")

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if not isinstance(other, Uuid):
            return NotImplemented
        return self._msb == other._msb and self._lsb == other._lsb

    def __hash__(self) -> int:
        # Java: xor = msb ^ lsb (signed long); return (int)(xor>>32) ^ (int)xor.
        xor = _to_signed64(_to_unsigned64(self._msb) ^ _to_unsigned64(self._lsb))
        high = xor >> 32  # arithmetic shift, mirrors Java's signed >>
        low = xor & 0xFFFFFFFF
        result = (high ^ low) & 0xFFFFFFFF
        # Java's hashCode is a signed 32-bit int; keep the value stable but let
        # Python own the final hash slot.
        if result > 0x7FFFFFFF:
            result -= 1 << 32
        return result

    def _compare(self, other: Uuid) -> int:
        # Java compareTo: signed comparison of msb then lsb.
        if self._msb > other._msb:
            return 1
        if self._msb < other._msb:
            return -1
        if self._lsb > other._lsb:
            return 1
        if self._lsb < other._lsb:
            return -1
        return 0

    def __lt__(self, other: Uuid) -> bool:
        return self._compare(other) < 0

    def __le__(self, other: Uuid) -> bool:
        return self._compare(other) <= 0

    def __gt__(self, other: Uuid) -> bool:
        return self._compare(other) > 0

    def __ge__(self, other: Uuid) -> bool:
        return self._compare(other) >= 0

    def __repr__(self) -> str:
        return str(self)


# The reserved constants (Java statics). ``METADATA_TOPIC_ID == ONE_UUID``.
Uuid.ZERO_UUID = Uuid(most_significant_bits=0, least_significant_bits=0)
Uuid.ONE_UUID = Uuid(most_significant_bits=0, least_significant_bits=1)
Uuid.METADATA_TOPIC_ID = Uuid.ONE_UUID
Uuid.RESERVED = frozenset({Uuid.ZERO_UUID, Uuid.ONE_UUID})
