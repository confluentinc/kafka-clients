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

"""``Headers``: the record-header alias (Java's ``Headers`` / ``Header``).

Java's ``org.apache.kafka.common.header.Headers`` has no Python class
*(deviation)*; headers are ``(key, value)`` pairs (CLAUDE.md, Python Binding
Conventions, Types). They are written as
``Iterable[tuple[str, bytes | bytearray | memoryview | None]]`` and handed out
as the ``Headers`` alias, whose values are ``memoryview`` s (into the fetch
batch for fetched records) or ``None``, Java's null header value. ``headers()``
is ``()`` when empty, never ``None``. Header values are not copied (CLAUDE.md
§12): a written value is viewed, not duplicated.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from confluent_kafka.null_pointer_error import NullPointerError

__all__ = ["Headers"]

#: The headers handed out: ``(key, value)`` pairs, ``value`` a ``memoryview``
#: or ``None``.
Headers = Sequence[tuple[str, "memoryview | None"]]

_BYTE_LIKE = (bytes, bytearray, memoryview)


def _read_headers(
    headers: Iterable[tuple[str, bytes | bytearray | memoryview | None]] | None,
) -> tuple[tuple[str, memoryview | None], ...]:
    """Java's ``new RecordHeaders(Iterable<Header>)``: the written headers in
    their read form. ``None`` is no headers, as in Java. A ``None`` key raises
    ``NullPointerError`` with ``RecordHeader``'s message; an element that is not
    a ``(str, bytes-like | None)`` pair is a ``TypeError``."""
    if headers is None:
        return ()
    result: list[tuple[str, memoryview | None]] = []
    for index, item in enumerate(headers):
        if not isinstance(item, tuple) or len(item) != 2:
            raise TypeError(f"header[{index}] must be a (key, value) tuple; got {item!r}")
        key, value = item
        if key is None:
            # RecordHeader: Objects.requireNonNull(key, "Null header keys are not permitted")
            raise NullPointerError(message="Null header keys are not permitted")
        if not isinstance(key, str):
            raise TypeError(f"header[{index}] key must be a str; got {type(key).__name__}")
        if value is None:
            result.append((key, None))
        elif isinstance(value, _BYTE_LIKE):
            result.append((key, memoryview(value).toreadonly()))
        else:
            raise TypeError(
                f"header[{index}] value must be bytes-like or None; got {type(value).__name__}")
    return tuple(result)


def _headers_to_string(headers: Sequence[tuple[str, memoryview | None]]) -> str:
    """Java's ``RecordHeaders.toString()``:
    ``RecordHeaders(headers = [RecordHeader(key = k, value = [1, 2])], isReadOnly = false)``,
    each value as ``Arrays.toString(byte[])`` (signed bytes)."""
    parts = []
    for key, value in headers:
        if value is None:
            text = "null"
        else:
            text = "[" + ", ".join(str(b - 256 if b > 127 else b) for b in value.tobytes()) + "]"
        parts.append(f"RecordHeader(key = {key}, value = {text})")
    return f"RecordHeaders(headers = [{', '.join(parts)}], isReadOnly = false)"


def _headers_hash(headers: Sequence[tuple[str, memoryview | None]]) -> int:
    """A hash over the header keys and value bytes (a ``memoryview`` of a
    ``bytearray`` is not hashable itself)."""
    return hash(tuple((k, None if v is None else v.tobytes()) for k, v in headers))
