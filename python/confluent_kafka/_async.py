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

"""Private: awaiting work that must end even when the awaiting task is
cancelled."""

from __future__ import annotations

import asyncio
from typing import TypeVar

_T = TypeVar("_T")


async def await_to_end(future: asyncio.Future[_T]) -> _T:
    """Await ``future`` to its end, even if the awaiting task is cancelled
    meanwhile, then raise the first cancellation: cancelling the task awaiting
    a call lets the call end, then raises ``CancelledError``.

    For work that goes on once started, such as a close's teardown in an
    executor. ``asyncio.shield`` alone keeps that work going but ends the wait,
    so the caller would carry on while it still runs. A cancellation wins over
    the work's own error, which asyncio then logs as never retrieved."""
    cancelled: asyncio.CancelledError | None = None
    while not future.done():
        try:
            # asyncio.wait neither cancels nor raises from the future.
            await asyncio.wait((future,))
        except asyncio.CancelledError as exc:
            if cancelled is None:
                cancelled = exc
    if cancelled is not None:
        raise cancelled
    return future.result()
