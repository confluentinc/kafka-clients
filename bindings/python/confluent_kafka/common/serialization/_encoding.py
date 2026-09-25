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

"""Charset-name validation shared by the string / UUID serdes.

Java's ``StringSerializer.configure`` calls ``Charset.forName(name)`` and wraps
an ``UnsupportedCharsetException`` / ``IllegalCharsetNameException`` in a
``SerializationException`` **at configure time** — the bad name is rejected then,
not on the first ``serialize``. This helper reproduces that: it resolves the name
with Python's ``codecs.lookup`` (the analog of ``Charset.forName``) and raises
``SerializationError`` on an unknown one, returning the name unchanged so the
serde can hand it to ``str.encode`` / ``bytes.decode``.
"""

from __future__ import annotations

import codecs

from confluent_kafka.common.errors._generated import SerializationError


def normalize_encoding(name: str) -> str:
    """Validate ``name`` as a codec, or raise ``SerializationError``.

    Returns the name unchanged (``str.encode`` / ``bytes.decode`` accept any
    alias ``codecs.lookup`` accepts). Java accepts both its own names (``UTF-8``)
    and Python's (``utf_8``); ``codecs.lookup`` normalizes aliases, so both work.
    """
    try:
        codecs.lookup(name)
    except LookupError as exc:
        raise SerializationError(f"Unsupported encoding {name}") from exc
    return name
