# Copyright 2026 Confluent Inc.
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

"""``AsyncOffsetCommitCallback``: the asyncio form of :class:`OffsetCommitCallback`
(CLAUDE.md, Python Binding Conventions, Class family).

By rule 3 a callback interface an async class takes has a public asyncio form
beside it, named ``Async`` + the interface's name and placed in its module,
whose method is ``async def``: ``AsyncConsumer.commit_nowait(callback=…)`` takes
an :class:`OffsetCommitCallback` or an ``AsyncOffsetCommitCallback``. It is
awaited on the event loop, inside the call that delivers it (``poll()``,
``commit()``, ``commit_nowait()``, ``close()``, …), with Java's ``onComplete``
parameters keyword-only, plus ``**kwargs``; callbacks still start one at a time
in the order the commits complete. ``AsyncMockConsumer`` awaits it inside
``commit_nowait()``, as Java's mock runs the callback inside ``commitAsync``. A
plain :class:`OffsetCommitCallback` given to an asyncio consumer runs on the
event loop and must not block it, as Java's callbacks must be fast. The
synchronous consumers reject an ``async def`` callback with ``TypeError``.
"""

from __future__ import annotations

from typing import Any, Protocol

from confluent_kafka.common.topic_partition import TopicPartition

from .offset_and_metadata import OffsetAndMetadata

__all__ = ["AsyncOffsetCommitCallback"]


class AsyncOffsetCommitCallback(Protocol):
    """The ``async def`` form of Java's ``OffsetCommitCallback.onComplete(Map
    offsets, Exception exception)``."""

    async def __call__(self, *, offsets: dict[TopicPartition, OffsetAndMetadata] | None,
                       exception: Exception | None, **kwargs: Any) -> None:
        """Awaited when a commit request completes: ``offsets`` and
        ``exception`` as for :class:`OffsetCommitCallback`."""
        ...
