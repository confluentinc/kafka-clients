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

"""Charset handling shared by the string and UUID serdes.

Java names a charset by ``Charset.forName(name)``; Python by
``codecs.lookup(name)``, which accepts Java's names (``UTF-8``, ``UTF-16``) as
well as its own (``utf_8``). The bytes follow Java's ``String.getBytes`` and
``new String(bytes, charset)``:

- an unmappable character is replaced (``errors="replace"``, Java's
  ``REPLACE`` action) rather than raising, and malformed input decodes to
  U+FFFD;
- ``UTF-16`` encodes big-endian after a ``FE FF`` byte-order mark (none for an
  empty string) and decodes big-endian unless a mark says otherwise, and
  ``UTF-32`` encodes big-endian with no mark, where Python's codecs of the same
  name use the machine's byte order.
"""

from __future__ import annotations

import codecs

from confluent_kafka.common.errors.serialization_error import SerializationError

_UTF16_BE_BOM = b"\xfe\xff"
_UTF16_LE_BOM = b"\xff\xfe"
_UTF32_BE_BOM = b"\x00\x00\xfe\xff"
_UTF32_LE_BOM = b"\xff\xfe\x00\x00"


def normalize_encoding(name: str) -> str:
    """Validate ``name`` as a charset, as Java's ``StringSerializer.configure``
    does, or raise ``SerializationError(message="Unsupported encoding <name>")``
    with the lookup failure as its cause."""
    try:
        codecs.lookup(name)
    except LookupError as exc:
        raise SerializationError(message=f"Unsupported encoding {name}") from exc
    return name


def encode(text: str, encoding: str) -> bytes:
    """``text.getBytes(charset)``; raises ``LookupError`` for an unknown name."""
    codec = codecs.lookup(encoding).name
    if codec == "utf-16":
        return _UTF16_BE_BOM + text.encode("utf_16_be", "replace") if text else b""
    if codec == "utf-32":
        return text.encode("utf_32_be", "replace")
    return text.encode(encoding, "replace")


def decode(data: memoryview | bytes, encoding: str) -> str:
    """``new String(bytes, charset)``; raises ``LookupError`` for an unknown
    name."""
    codec = codecs.lookup(encoding).name
    if codec == "utf-16":
        raw = bytes(data)
        if raw.startswith(_UTF16_LE_BOM):
            return raw[2:].decode("utf_16_le", "replace")
        if raw.startswith(_UTF16_BE_BOM):
            raw = raw[2:]
        return raw.decode("utf_16_be", "replace")
    if codec == "utf-32":
        raw = bytes(data)
        if raw.startswith(_UTF32_LE_BOM):
            return raw[4:].decode("utf_32_le", "replace")
        if raw.startswith(_UTF32_BE_BOM):
            raw = raw[4:]
        return raw.decode("utf_32_be", "replace")
    return codecs.decode(data, encoding, "replace")
