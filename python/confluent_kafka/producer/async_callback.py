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

"""``AsyncCallback``: the asyncio form of :class:`Callback` (CLAUDE.md, Python
Binding Conventions, Class family).

By rule 3 a callback interface an async class takes has a public asyncio form
beside it, named ``Async`` + the interface's name and placed in its module,
whose method is ``async def``: ``AsyncProducer.send(callback=…)`` takes a
:class:`Callback` or an ``AsyncCallback``. An ``AsyncCallback`` is awaited on
the record's event loop, with Java's ``onCompletion`` parameters keyword-only,
plus ``**kwargs``, as :class:`Callback` is called; the record's future
completes, and ``flush()``, the transaction-end calls and ``close()`` return,
only after it ended. A plain :class:`Callback` given to an asyncio producer runs
on the event loop and must not block it, as Java's callback must be fast on the
producer's I/O thread. The synchronous producers reject an ``async def``
callback with ``TypeError`` at ``send()``.
"""

from __future__ import annotations

from typing import Any, Protocol

from .record_metadata import RecordMetadata

__all__ = ["AsyncCallback"]


class AsyncCallback(Protocol):
    """The ``async def`` form of Java's ``Callback.onCompletion(RecordMetadata
    metadata, Exception exception)``."""

    async def __call__(self, *, metadata: RecordMetadata, exception: Exception | None,
                       **kwargs: Any) -> None:
        """Awaited when the record sent to the server has been acknowledged, or
        has failed: ``metadata`` and ``exception`` as for :class:`Callback`."""
        ...
