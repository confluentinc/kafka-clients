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

"""``Callback``: Java's ``org.apache.kafka.clients.producer.Callback``.

A single-method callback interface, so a ``Protocol`` of the interface's name
whose ``__call__`` takes Java's parameters keyword-only plus ``**kwargs``
(CLAUDE.md, Python Binding Conventions, Idiom translations): the producer calls
it as ``callback(metadata=..., exception=...)``, Java's ``onCompletion(RecordMetadata
metadata, Exception exception)``, and rejects a callback without ``**kwargs``
at ``send()`` with ``IllegalArgumentError("callback must accept **kwargs")``,
so a parameter Java adds later reaches it without breaking it::

    def on_done(*, metadata, exception, **_):
        ...

    producer.send(record=r, callback=on_done)

A callback interface that the user can implement to allow code to execute when
the request is complete. This callback will generally execute in the background
I/O thread so it should be fast: on ``KafkaProducer`` it runs on the producer's
background completion thread, on ``AsyncKafkaProducer`` on the event loop, one
at a time in completion order; an exception it raises is not propagated: the
core logs it once, with Java's message and the exception's text (Threads and
callbacks). On ``MockProducer`` and ``AsyncMockProducer`` it runs in the call
that completes the record and propagates out of it, as from Java's mock. The
record's future completes, and ``flush()``, the transaction-end calls and
``close()`` return, only after the callbacks of the records they cover
returned, as Java fires the callbacks before completing the future. By rule 3
the asyncio producers also accept an ``async def`` callback, which they await
on the event loop; the sync producers reject a coroutine function with
``TypeError`` at ``send()``. This protocol stays the synchronous form; its
asyncio form is ``AsyncCallback``.

The callback is called when the record sent to the server has been
acknowledged. ``metadata`` is the metadata for the record that was sent (i.e.
the partition and offset); it is never ``None``: when ``exception`` is not
``None`` it carries the special -1 value for every field but the topic and the
partition, and -1 as the partition if none could be chosen (Java builds
``RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)``). ``exception`` is the
exception thrown during processing of this record, ``None`` if no error occurred.
"""

from __future__ import annotations

from typing import Any, Protocol

from .record_metadata import RecordMetadata

__all__ = ["Callback"]


class Callback(Protocol):
    """Java's ``Callback.onCompletion(RecordMetadata metadata, Exception exception)``."""

    def __call__(self, *, metadata: RecordMetadata, exception: Exception | None,
                 **kwargs: Any) -> None:
        """A callback method the user can implement to provide asynchronous
        handling of request completion. This method will be called when the
        record sent to the server has been acknowledged."""
        ...
