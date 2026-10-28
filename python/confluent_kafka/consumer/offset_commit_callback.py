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

"""``OffsetCommitCallback``: Java's
``org.apache.kafka.clients.consumer.OffsetCommitCallback``.

A single-method callback interface, so a ``Protocol`` of the interface's name
whose ``__call__`` takes Java's parameters keyword-only plus ``**kwargs``
(CLAUDE.md, Python Binding Conventions, Idiom translations), called as
``callback(offsets=..., exception=...)`` — Java's ``onComplete(offsets,
exception)``; ``commit_nowait()`` rejects a callback without ``**kwargs`` with
``IllegalArgumentError("callback must accept **kwargs")``. ``offsets`` is ``None`` where Java passes ``null``: when the commit
failed, and for an explicit empty ``offsets`` (whose commit completes at once
with ``null``, ``AsyncKafkaConsumer.commit``); ``MockConsumer`` passes the offsets
given, as Java's mock does. ``exception`` is ``None`` when the commit completed
successfully.

The callback runs on the caller's thread, inside the call that delivers it
(``poll()``, ``commit()``, ``commit_nowait()``, ``close()``, …), as Java runs it
on the thread calling ``poll()`` (Threads and callbacks). An exception it
raises propagates out of that call, as Java's ``onComplete`` is called without
a ``try`` (``poll()`` wraps it in a ``KafkaError`` unless it is one); the
callbacks queued behind it run inside a later call. ``MockConsumer`` and
``AsyncMockConsumer`` run it inside ``commit_nowait()``, as Java's mock does,
which raises its exception.

On ``AsyncKafkaConsumer`` and ``AsyncMockConsumer`` the callback passed to
``commit_nowait()`` may also be ``async def`` (rule 3, as for a listener
method): it is awaited on the event loop, inside the call that delivers it,
callbacks still starting one at a time in the order the commits complete. The
synchronous consumers reject one: ``KafkaConsumer`` closes its coroutine and
the delivering call raises ``TypeError``, ``MockConsumer`` raises ``TypeError``
from ``commit_nowait()``. This protocol stays Java's
plain callback; its asyncio form is ``AsyncOffsetCommitCallback``.
"""

from __future__ import annotations

from typing import Any, Protocol

from confluent_kafka.common.topic_partition import TopicPartition

from .offset_and_metadata import OffsetAndMetadata

__all__ = ["OffsetCommitCallback"]


class OffsetCommitCallback(Protocol):
    """A callback interface that the user can implement to trigger custom
    actions when a commit request completes (Java's ``onComplete``)."""

    def __call__(self, *, offsets: dict[TopicPartition, OffsetAndMetadata] | None,
                 exception: Exception | None, **kwargs: Any) -> None:
        """A callback method the user can implement to provide asynchronous
        handling of commit request completion. ``offsets`` is ``None`` where
        Java passes ``null``; ``exception`` is ``None`` when the commit
        completed successfully."""
        ...
