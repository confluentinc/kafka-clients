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

"""``Headers`` — the record-header alias and its write-side validator.

Mirrors ``org.apache.kafka.common.header.Headers`` / ``Header`` (Apache Kafka
4.3.1) as a lightweight Python alias rather than a class (spec §5.2). A header
is a ``(key, value)`` pair: the key is a ``str``; the value is a byte-like when
written and a ``memoryview`` (into the fetch buffer) when read. A ``None`` value
is a legal header value (Java allows null header values).

Read form:  ``Sequence[tuple[str, memoryview]]``
Write form: ``Iterable[tuple[str, bytes | bytearray | memoryview | None]]``
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from confluent_kafka import IllegalArgumentError

# The public alias used across the record types. On the read path a record hands
# back header values as ``memoryview``s that borrow the fetch batch; on the
# write path any byte-like (or ``None``) is accepted (§5.2).
Headers = Sequence["tuple[str, memoryview]"]

# The byte-like types accepted for a written header value.
_ByteLike = (bytes, bytearray, memoryview)


def _validate_written_headers(
    headers: Iterable[tuple[str, bytes | bytearray | memoryview | None]],
) -> tuple[tuple[str, bytes | None], ...]:
    """Validate and normalize a written header iterable.

    Binding-internal (leading underscore): Java has no free header-validation
    function — ``RecordHeaders(Iterable<Header>)`` normalizes internally — so this
    helper is deliberately kept off the public ``confluent_kafka.common`` surface
    (rule 2 / DoD #7). It is imported only by the record modules within the
    package.

    Each element must be a ``(str, byte-like | None)`` 2-tuple. Byte-like values
    are copied into owned ``bytes``; ``None`` passes through. A malformed
    element raises :class:`~confluent_kafka.IllegalArgumentError` (the JDK analog
    of Java's ``IllegalArgumentException``).

    Returns a tuple of ``(key, bytes | None)`` pairs so the caller holds an
    immutable, owned copy.
    """
    result: list[tuple[str, bytes | None]] = []
    for index, item in enumerate(headers):
        if not isinstance(item, tuple) or len(item) != 2:
            raise IllegalArgumentError(
                f"header[{index}] must be a (key, value) tuple; got {item!r}"
            )
        key, value = item
        if not isinstance(key, str):
            raise IllegalArgumentError(
                f"header[{index}] key must be a str; got {type(key).__name__}"
            )
        if value is None:
            result.append((key, None))
        elif isinstance(value, _ByteLike):
            result.append((key, bytes(value)))
        else:
            raise IllegalArgumentError(
                f"header[{index}] value must be bytes-like or None; "
                f"got {type(value).__name__}"
            )
    return tuple(result)
