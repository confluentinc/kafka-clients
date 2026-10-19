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
Conventions, Serialization). A serializer is any callable of that shape: it is
called with keyword arguments, ``serializer(topic=..., value=...,
headers=...)``, and must accept ``**kwargs``, so a parameter Java adds later
reaches it as one more keyword; a client given one without ``**kwargs``
raises ``IllegalArgumentError("<param> must accept **kwargs")``. The protocol
exists for the type checker. The producer calls a serializer with the
record's key or value as given, ``None`` included (Java's null, as Java's
``KafkaProducer`` does), and the serializer decides what null maps to.
"""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

from confluent_kafka.common.headers import Headers

__all__ = ["Serializer"]

T_contra = TypeVar("T_contra", contravariant=True)


class Serializer(Protocol[T_contra]):
    """An interface for converting objects to bytes.

    Java: ``org.apache.kafka.common.serialization.Serializer<T>``.
    """

    def __call__(self, *, topic: str, value: T_contra | None,
                 headers: Headers | None = None, **kwargs: Any) -> bytes | None:
        """Convert ``value`` into bytes: ``topic`` is the topic associated
        with the data, ``headers`` the headers associated with the record (its
        own ``Headers``, which a serializer may change: they turn read-only
        once the record is sent, as Java's ``KafkaProducer`` seals them), and ``value`` the typed data. Returns the
        serialized bytes."""
        ...
