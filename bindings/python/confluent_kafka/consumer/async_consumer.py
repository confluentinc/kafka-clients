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

"""``AsyncConsumer`` — the non-instantiable base of the asyncio consumer family.

The async peer of ``Consumer`` (spec §3 principle 5): methods that perform I/O
or await the background task are coroutines; the pure state reads
(``assignment`` / ``subscription`` / ``paused`` / ``current_lag`` /
``group_metadata`` / ``metrics`` / ``wakeup`` / ``commit_nowait``) stay
synchronous, inherited from ``_ConsumerClientBase``. ``seek`` is a coroutine
despite doing no broker I/O — it awaits the background task applying the position
change (decision D21).

Construct an ``AsyncKafkaConsumer`` or ``AsyncMockConsumer``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Generic, TypeVar, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka import Duration
from confluent_kafka._args import at_most_one, exactly_one
from confluent_kafka._config import duration_to_ms
from confluent_kafka.common.metric import KafkaMetric
from confluent_kafka.common.partition_info import PartitionInfo
from confluent_kafka.common.topic_partition import TopicPartition

from ._client_base import _ConsumerClientBase
from .close_options import CloseOptions
from .consumer import _close_args
from .consumer_records import ConsumerRecords
from .consumer_rebalance_listener import CommitCallback, ConsumerRebalanceListener
from .offset_and_metadata import OffsetAndMetadata
from .offset_and_timestamp import OffsetAndTimestamp
from .subscription_pattern import SubscriptionPattern
from ._unsupported import raise_unsupported

K = TypeVar("K")
V = TypeVar("V")

_DEFAULT_API_TIMEOUT_MS = 60_000


class AsyncConsumer(_ConsumerClientBase, Generic[K, V]):
    """Non-instantiable base — the asyncio ``Consumer`` peer.

    Construct an ``AsyncKafkaConsumer`` or ``AsyncMockConsumer``.
    """

    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if type(self) is AsyncConsumer:
            raise TypeError(
                "AsyncConsumer is a non-instantiable base; use "
                "AsyncKafkaConsumer or AsyncMockConsumer"
            )

    # ---- subscription & assignment -------------------------------------
    @overload
    async def subscribe(self, *, topics: Iterable[str],
                        listener: ConsumerRebalanceListener | None = None) -> None: ...
    @overload
    async def subscribe(self, *, pattern: SubscriptionPattern,
                        listener: ConsumerRebalanceListener | None = None) -> None: ...

    async def subscribe(self, *, topics: Iterable[str] | None = None,
                        pattern: SubscriptionPattern | None = None,
                        listener: ConsumerRebalanceListener | None = None) -> None:
        self._check_closed()
        chosen = exactly_one("subscribe", topics=topics, pattern=pattern)
        self._listener = listener
        if chosen == "topics":
            topic_list = list(topics)  # type: ignore[arg-type]
            await self._run_async(*self._subscribe_topics_spec(topic_list, listener is not None))
        else:
            assert pattern is not None
            await self._run_async(*self._subscribe_pattern_spec(pattern.pattern(), listener is not None))

    async def unsubscribe(self) -> None:
        self._check_closed()
        await self._run_async(*self._unsubscribe_spec())

    async def assign(self, *, partitions: Iterable[TopicPartition]) -> None:
        self._check_closed()
        await self._run_async(*self._tp_op_spec(_lib.Consumer_assign_async, partitions))

    async def pause(self, *, partitions: Iterable[TopicPartition]) -> None:
        self._check_closed()
        await self._run_async(*self._tp_op_spec(_lib.Consumer_pause_async, partitions))

    async def resume(self, *, partitions: Iterable[TopicPartition]) -> None:
        self._check_closed()
        await self._run_async(*self._tp_op_spec(_lib.Consumer_resume_async, partitions))

    # ---- consume -------------------------------------------------------
    async def poll(self, *, timeout: Duration) -> ConsumerRecords[K, V]:
        self._check_closed()
        ms = duration_to_ms(timeout, default_ms=0)
        return await self._run_async(*self._poll_spec(ms))

    # ---- offsets -------------------------------------------------------
    @overload
    async def seek(self, *, partition: TopicPartition, offset: int) -> None: ...
    @overload
    async def seek(self, *, partition: TopicPartition,
                   offset_and_metadata: OffsetAndMetadata) -> None: ...

    async def seek(self, *, partition: TopicPartition, offset: int | None = None,
                   offset_and_metadata: OffsetAndMetadata | None = None) -> None:
        self._check_closed()
        exactly_one("seek", offset=offset, offset_and_metadata=offset_and_metadata)
        await self._run_async(*self._seek_spec(partition, offset, offset_and_metadata))

    async def commit(self, *,
                     offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
                     timeout: Duration | None = None) -> None:
        self._check_closed()
        await self._run_async(*self._commit_spec(offsets))

    def commit_nowait(self, *,
                      offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
                      on_commit: CommitCallback | None = None) -> None:
        """Java ``commitAsync`` — returns immediately, so a plain ``def`` on both
        classes (spec §3 principle 12)."""
        self._check_closed()
        adapter = self._wrap_commit_callback(on_commit)
        if offsets is None:
            error = (_lib.Consumer_commit_async(self._h)
                     if adapter is None
                     else _lib.Consumer_commit_async(self._h, adapter))
        else:
            from ._conversions import offsets_to_spec
            spec = offsets_to_spec(offsets)
            error = (_lib.Consumer_commit_async_offsets(self._h, spec)
                     if adapter is None
                     else _lib.Consumer_commit_async_offsets(self._h, spec, adapter))
        if error:
            from confluent_kafka.common.errors import from_ffi_error
            raise from_ffi_error(error)

    async def committed(self, *, partitions: Iterable[TopicPartition],
                        timeout: Duration | None = None
                        ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        self._check_closed()
        return await self._run_async(*self._committed_spec(partitions))

    async def position(self, *, partition: TopicPartition,
                       timeout: Duration | None = None) -> int:
        self._check_closed()
        return await self._run_async(*self._position_spec(partition))

    async def seek_to_beginning(self, *,
                                partitions: Iterable[TopicPartition]) -> None:
        self._check_closed()
        await self._run_async(*self._tp_op_spec(_lib.Consumer_seek_to_beginning_async, partitions))

    async def seek_to_end(self, *,
                          partitions: Iterable[TopicPartition]) -> None:
        self._check_closed()
        await self._run_async(*self._tp_op_spec(_lib.Consumer_seek_to_end_async, partitions))

    async def beginning_offsets(self, *, partitions: Iterable[TopicPartition],
                                timeout: Duration | None = None
                                ) -> dict[TopicPartition, int]:
        self._check_closed()
        return await self._run_async(*self._long_offsets_spec(
            _lib.Consumer_beginning_offsets_async, partitions))

    async def end_offsets(self, *, partitions: Iterable[TopicPartition],
                          timeout: Duration | None = None
                          ) -> dict[TopicPartition, int]:
        self._check_closed()
        return await self._run_async(*self._long_offsets_spec(
            _lib.Consumer_end_offsets_async, partitions))

    async def offsets_for_times(self, *, timestamps: Mapping[TopicPartition, int],
                                timeout: Duration | None = None
                                ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        self._check_closed()
        return await self._run_async(*self._offsets_for_times_spec(timestamps))

    # ---- metadata & observability --------------------------------------
    async def partitions_for(self, *, topic: str,
                             timeout: Duration | None = None
                             ) -> list[PartitionInfo]:
        self._check_closed()
        return await self._run_async(*self._partitions_for_spec(topic))

    async def list_topics(self, *, timeout: Duration | None = None
                          ) -> dict[str, list[PartitionInfo]]:
        self._check_closed()
        return await self._run_async(*self._list_topics_spec())

    def register_metric_for_subscription(self, *, metric: KafkaMetric) -> None:
        raise_unsupported("register_metric_for_subscription")

    def unregister_metric_from_subscription(self, *, metric: KafkaMetric) -> None:
        raise_unsupported("unregister_metric_from_subscription")

    async def client_instance_id(self, *, timeout: Duration | None = None) -> Any:
        if timeout is not None:
            duration_to_ms(timeout, default_ms=_DEFAULT_API_TIMEOUT_MS)
        raise_unsupported("client_instance_id")

    # ---- lifecycle -----------------------------------------------------
    @overload
    async def close(self, *, timeout: Duration | None = None) -> None: ...
    @overload
    async def close(self, *, option: CloseOptions) -> None: ...

    async def close(self, *, timeout: Duration | None = None,
                    option: CloseOptions | None = None) -> None:
        at_most_one("close", timeout=timeout, option=option)
        if self._closed or self._h is None:
            return
        self._closed = True
        try:
            timeout_ms, op_code = _close_args(timeout, option)
            await self._run_async(*self._close_spec(timeout_ms, op_code))
        finally:
            self._destroy()

    async def __aenter__(self) -> AsyncConsumer[K, V]:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
