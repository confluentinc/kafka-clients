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

"""``Deserializer``: Java's ``org.apache.kafka.common.serialization.Deserializer``.

*(deviation)* Java's interface, with its three ``deserialize`` overloads, is one
callable shape whose headers are always passed (CLAUDE.md, Python Binding
Conventions, Serialization). A deserializer is any callable of that shape: it
is called with keyword arguments, ``deserializer(topic=..., data=...,
headers=...)``, and must accept ``**kwargs``, so a parameter Java adds later
reaches it as one more keyword; a client given one without ``**kwargs``
raises ``IllegalArgumentError("<param> must accept **kwargs")``. The protocol
exists for the type checker. The consumer does not call a
deserializer for a null key or value: the record's field is ``None``, as in
Java's ``CompletedFetch``. ``None`` data reaches a deserializer only when it is
called directly, and then it decides what null maps to.
"""

from __future__ import annotations

from typing import Any, Protocol, TypeVar

from confluent_kafka.common.headers import Headers

__all__ = ["Deserializer"]

T_co = TypeVar("T_co", covariant=True)


class Deserializer(Protocol[T_co]):
    """An interface for converting bytes to objects.

    Java: ``org.apache.kafka.common.serialization.Deserializer<T>``.
    """

    def __call__(self, *, topic: str, data: memoryview | None,
                 headers: Headers | None = None, **kwargs: Any) -> T_co | None:
        """Deserialize a record value from a ``memoryview`` into a value or
        object: ``topic`` is the topic associated with the data, ``headers``
        the headers associated with the record (its own ``Headers``, which
        stay writable, as Java's ``CompletedFetch`` leaves them), and ``data``
        the serialized
        bytes, a view into the fetch batch. Returns the deserialized typed
        data; may be ``None``."""
        ...
