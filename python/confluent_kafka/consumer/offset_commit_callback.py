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

A single-method callback interface, so a ``Callable`` alias of the interface's
name (CLAUDE.md, Python Binding Conventions, Idiom translations), called
positionally as ``callback(offsets, exception)`` — Java's ``onComplete(offsets,
exception)``. ``offsets`` is ``None`` where Java passes ``null``: when the commit
failed, and for an explicit empty ``offsets`` (whose commit completes at once
with ``null``, ``AsyncKafkaConsumer.commit``); ``MockConsumer`` passes the offsets
given, as Java's mock does. ``exception`` is ``None`` when the commit completed
successfully.

The callback runs on the caller's thread, inside the call that delivers it
(``poll()``, ``commit()``, ``commit_nowait()``, ``close()``, …), as Java runs it
on the thread calling ``poll()`` (Threads and callbacks). An exception it
raises propagates out of that call, as Java's ``onComplete`` is called without
a ``try`` (``poll()`` wraps it in a ``KafkaError`` unless it is one); the
callbacks queued behind it run inside a later call. ``AsyncMockConsumer``
raises the exception of an ``async def`` callback from its next awaited call,
since the ``commit_nowait()`` Java's mock raises it from has returned.

On ``AsyncKafkaConsumer`` and ``AsyncMockConsumer`` the callback passed to
``commit_nowait()`` may also be ``async def`` (rule 3, as for a listener
method): it is awaited on the event loop, inside the call that delivers it,
callbacks still starting one at a time in the order the commits complete. The
synchronous consumers reject one: ``KafkaConsumer`` closes its coroutine and
the delivering call raises ``TypeError``, ``MockConsumer`` raises ``TypeError``
from ``commit_nowait()``. This alias stays Java's
plain callback; ``AsyncConsumer.commit_nowait`` spells out the coroutine form
beside it.
"""

from __future__ import annotations

from collections.abc import Callable

from confluent_kafka.common.topic_partition import TopicPartition

from .offset_and_metadata import OffsetAndMetadata

__all__ = ["OffsetCommitCallback"]

#: A callback the user implements to handle the completion of a commit
#: requested with ``commit_nowait()``: ``callback(offsets, exception)``.
OffsetCommitCallback = Callable[
    [dict[TopicPartition, OffsetAndMetadata] | None, Exception | None], None
]
