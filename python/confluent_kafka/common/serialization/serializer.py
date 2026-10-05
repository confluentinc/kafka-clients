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

"""``Serializer``: Java's ``org.apache.kafka.common.serialization.Serializer``.

*(deviation)* Java's interface, with its three ``serialize`` overloads, is one
callable shape whose headers are always passed (CLAUDE.md, Python Binding
Conventions, Serialization). A serializer is any callable of that shape; the
protocol exists for the type checker. A ``None`` value is Java's null, and the
serializer decides what it maps to.
"""

from __future__ import annotations

from typing import Protocol, TypeVar

from confluent_kafka.common.headers import Headers

__all__ = ["Serializer"]

T_contra = TypeVar("T_contra", contravariant=True)


class Serializer(Protocol[T_contra]):
    """An interface for converting objects to bytes.

    Java: ``org.apache.kafka.common.serialization.Serializer<T>``.
    """

    def __call__(self, topic: str, value: T_contra | None,
                 headers: Headers | None = None) -> bytes | None:
        """Convert ``value`` into bytes: ``topic`` is the topic associated
        with the data, ``headers`` the headers associated with the record, and
        ``value`` the typed data. Returns the serialized bytes."""
        ...
