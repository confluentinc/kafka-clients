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

"""``AsyncConsumer``: the asyncio peer of :class:`Consumer` (CLAUDE.md, Python
Binding Conventions, Class family).

The same methods, in the same order. A method is ``async def`` iff Java waits
in it — on the background thread (``addAndGet``), on the network or on a
listener it runs: ``subscribe``, ``assign``, ``unsubscribe``, ``poll``,
``commit``, ``seek``, ``seek_to_beginning`` / ``seek_to_end``, ``position``,
``committed``, ``partitions_for``, ``list_topics``, ``pause`` / ``resume``,
``offsets_for_times``, ``beginning_offsets`` / ``end_offsets`` and ``close``;
every other method is a plain ``def`` (``assignment()``, ``subscription()``,
``commit_nowait()``, ``metrics()``, ``paused()``, ``group_metadata()``,
``wakeup()``). A waiting method awaits the entry point's ``_async`` form; the
listener runs on the event loop and may be ``async def``.

Not generated, besides :class:`Consumer`'s omissions: ``current_lag()``, which
waits in Java (``AsyncKafkaConsumer.currentLag`` uses ``addAndGet``) but whose
``_async`` entry point, ``kafka_consumer_Consumer_current_lag_async``, is
missing (``ffi-overload-gaps.md``).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import java_forms
from confluent_kafka.concurrent_modification_error import ConcurrentModificationError

from ._base import _ConsumerState, close_args, poll_timeout_ms
from ._conversions import tp_to_spec
from .close_options import CloseOptions
from .consumer import CLOSE_FORMS, COMMIT_NOWAIT_FORMS, SEEK_FORMS, SUBSCRIBE_FORMS

if TYPE_CHECKING:
    from confluent_kafka import Duration
    from confluent_kafka.common.metric import Metric
    from confluent_kafka.common.metric_name import MetricName
    from confluent_kafka.common.partition_info import PartitionInfo
    from confluent_kafka.common.topic_partition import TopicPartition

    from ._poll import Deserialized
    from .consumer_group_metadata import ConsumerGroupMetadata
    from .consumer_rebalance_listener import ConsumerRebalanceListener
    from .consumer_records import ConsumerRecords
    from .offset_and_metadata import OffsetAndMetadata
    from .offset_and_timestamp import OffsetAndTimestamp
    from .offset_commit_callback import OffsetCommitCallback
    from .subscription_pattern import SubscriptionPattern

__all__ = ["AsyncConsumer"]

K = TypeVar("K")
V = TypeVar("V")

_LOG = logging.getLogger("confluent_kafka.consumer")


class AsyncConsumer(Generic[K, V], _ConsumerState):
    """The asyncio peer of the ``Consumer`` interface.

    Construct an ``AsyncKafkaConsumer`` or ``AsyncMockConsumer``; an async
    context manager whose exit closes. The async classes follow the sync
    thread rule per event loop: a call while another task is inside the
    consumer raises ``ConcurrentModificationError``, ``wakeup()`` excepted.
    """

    def __init__(self) -> None:
        if type(self) is AsyncConsumer:
            raise TypeError(
                "AsyncConsumer is a non-instantiable base; use AsyncKafkaConsumer or "
                "AsyncMockConsumer")
        _ConsumerState.__init__(self)

    def assignment(self) -> set[TopicPartition]:
        """See :meth:`Consumer.assignment`."""
        return self._c_assignment()

    def subscription(self) -> set[str]:
        """See :meth:`Consumer.subscription`."""
        return self._c_subscription()

    @overload
    async def subscribe(self, *, topics: Iterable[str],
                        callback: ConsumerRebalanceListener | None = None) -> None: ...
    @overload
    async def subscribe(self, *, pattern: SubscriptionPattern,
                        callback: ConsumerRebalanceListener | None = None) -> None: ...

    @java_forms(*SUBSCRIBE_FORMS)
    async def subscribe(self, *, topics: Iterable[str] | None = None,
                        pattern: SubscriptionPattern | None = None,
                        callback: ConsumerRebalanceListener | None = None) -> None:
        """See :meth:`Consumer.subscribe`. The listener's methods may be
        ``async def``; they are awaited on the event loop."""
        if topics is not None:
            await self._a_subscribe_topics(topics, callback)
        else:
            await self._a_subscribe_pattern(pattern, callback)

    async def assign(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.assign`."""
        await self._a_assign(partitions)

    async def unsubscribe(self) -> None:
        """See :meth:`Consumer.unsubscribe`."""
        await self._a_unsubscribe()

    async def poll(self, *, timeout: Duration) -> ConsumerRecords[K, V]:
        """See :meth:`Consumer.poll`."""
        return await self._a_poll(timeout)

    async def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None
                     ) -> None:
        """See :meth:`Consumer.commit` (Java's ``commitSync``)."""
        await self._a_commit(offsets)

    @overload
    def commit_nowait(self, *, callback: OffsetCommitCallback | None = None) -> None: ...
    @overload
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                      callback: OffsetCommitCallback) -> None: ...

    @java_forms(*COMMIT_NOWAIT_FORMS)
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
                      callback: OffsetCommitCallback | None = None) -> None:
        """See :meth:`Consumer.commit_nowait` (Java's ``commitAsync``, which
        does not wait: a plain ``def``). The callback runs on the event loop,
        inside a later call on this consumer."""
        self._c_commit_nowait(offsets, callback)

    @overload
    async def seek(self, *, partition: TopicPartition, offset: int) -> None: ...
    @overload
    async def seek(self, *, partition: TopicPartition,
                   offset_and_metadata: OffsetAndMetadata) -> None: ...

    @java_forms(*SEEK_FORMS)
    async def seek(self, *, partition: TopicPartition, offset: int | None = None,
                   offset_and_metadata: OffsetAndMetadata | None = None) -> None:
        """See :meth:`Consumer.seek` (Java waits for the background thread to
        apply it, ``addAndGet``)."""
        await self._a_seek(partition, offset, offset_and_metadata)

    async def seek_to_beginning(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.seek_to_beginning`."""
        await self._a_seek_to_beginning(partitions)

    async def seek_to_end(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.seek_to_end`."""
        await self._a_seek_to_end(partitions)

    async def position(self, *, partition: TopicPartition) -> int:
        """See :meth:`Consumer.position`."""
        return await self._a_position(partition)

    async def committed(self, *, partitions: Iterable[TopicPartition]
                        ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        """See :meth:`Consumer.committed`."""
        return await self._a_committed(partitions)

    def metrics(self) -> dict[MetricName, Metric]:
        """See :meth:`Consumer.metrics`."""
        return self._c_metrics()

    async def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """See :meth:`Consumer.partitions_for`."""
        return await self._a_partitions_for(topic)

    async def list_topics(self) -> dict[str, list[PartitionInfo]]:
        """See :meth:`Consumer.list_topics`."""
        return await self._a_list_topics()

    def paused(self) -> set[TopicPartition]:
        """See :meth:`Consumer.paused`."""
        return self._c_paused()

    async def pause(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.pause`."""
        await self._a_pause(partitions)

    async def resume(self, *, partitions: Iterable[TopicPartition]) -> None:
        """See :meth:`Consumer.resume`."""
        await self._a_resume(partitions)

    async def offsets_for_times(self, *, timestamps_to_search: Mapping[TopicPartition, int]
                                ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        """See :meth:`Consumer.offsets_for_times`."""
        return await self._a_offsets_for_times(timestamps_to_search)

    async def beginning_offsets(self, *, partitions: Iterable[TopicPartition]
                                ) -> dict[TopicPartition, int]:
        """See :meth:`Consumer.beginning_offsets`."""
        return await self._a_beginning_offsets(partitions)

    async def end_offsets(self, *, partitions: Iterable[TopicPartition]
                          ) -> dict[TopicPartition, int]:
        """See :meth:`Consumer.end_offsets`."""
        return await self._a_end_offsets(partitions)

    def group_metadata(self) -> ConsumerGroupMetadata:
        """See :meth:`Consumer.group_metadata`."""
        return self._c_group_metadata()

    @overload
    async def close(self, *, timeout: Duration | None = None) -> None: ...
    @overload
    async def close(self, *, option: CloseOptions) -> None: ...

    @java_forms(*CLOSE_FORMS)
    async def close(self, *, timeout: Duration | None = None,
                    option: CloseOptions | None = None) -> None:
        """See :meth:`Consumer.close`.

        Deprecated: ``close(timeout=…)``. This method has been deprecated since
        Kafka 4.1 and should use ``close(option=…)`` instead.
        """
        await self._a_close(timeout, option)

    async def __aenter__(self) -> AsyncConsumer[K, V]:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def wakeup(self) -> None:
        """See :meth:`Consumer.wakeup`."""
        self._c_wakeup()

    # ---- the FFI implementations of the waiting calls ------------------------
    async def _a_subscribe_topics(self, topics: Iterable[str],
                                  callback: ConsumerRebalanceListener | None) -> None:
        topic_list = list(topics)
        previous, self._listener = self._listener, callback
        try:
            await self._run_async(*self._subscribe_topics_spec(topic_list, callback is not None))
        except BaseException:
            self._listener = previous
            raise

    async def _a_subscribe_pattern(self, pattern: SubscriptionPattern | None,
                                   callback: ConsumerRebalanceListener | None) -> None:
        assert pattern is not None
        previous, self._listener = self._listener, callback
        try:
            await self._run_async(*self._subscribe_pattern_spec(pattern.pattern(),
                                                                callback is not None))
        except BaseException:
            self._listener = previous
            raise

    async def _tp_op(self, op: str, fn: Any, partitions: Iterable[TopicPartition]) -> None:
        partition_list = list(partitions)
        if self._in_callback():
            self._reentrant_use(op, partition_list)
            return
        await self._run_async(*self._void_spec(fn, tp_to_spec(partition_list)))

    async def _a_assign(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("assign", _lib.Consumer_assign_async, partitions)

    async def _a_unsubscribe(self) -> None:
        await self._run_async(*self._void_spec(_lib.Consumer_unsubscribe_async))

    async def _a_poll(self, timeout: Duration) -> ConsumerRecords[K, V]:
        result: Deserialized = await self._run_async(*self._poll_spec(poll_timeout_ms(timeout)))
        for partition, offset in result.rewind:
            try:
                await self._run_async(*self._seek_spec(partition, offset, None))
            except Exception:  # noqa: BLE001 - the deserialization error matters
                _LOG.exception("Could not seek %s back to offset %d", partition, offset)
        if result.error is not None and result.records.is_empty():
            raise result.error
        return result.records

    async def _a_commit(self, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None) -> None:
        if self._in_callback():
            self._reentrant_use("commit", offsets)
            return
        await self._run_async(*self._commit_spec(offsets))

    async def _a_seek(self, partition: TopicPartition, offset: int | None,
                      offset_and_metadata: OffsetAndMetadata | None) -> None:
        if self._in_callback():
            self._reentrant_use("seek", partition, offset, offset_and_metadata)
            return
        await self._run_async(*self._seek_spec(partition, offset, offset_and_metadata))

    async def _a_seek_to_beginning(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("seek_to_beginning", _lib.Consumer_seek_to_beginning_async, partitions)

    async def _a_seek_to_end(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("seek_to_end", _lib.Consumer_seek_to_end_async, partitions)

    async def _a_position(self, partition: TopicPartition) -> int:
        if self._in_callback():
            position: int = self._reentrant_use("position", partition)
            return position
        return await self._run_async(*self._position_spec(partition))

    async def _a_committed(self, partitions: Iterable[TopicPartition]
                           ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        partition_list = list(partitions)
        if self._in_callback():
            committed: dict[TopicPartition, OffsetAndMetadata | None] = self._reentrant_use(
                "committed", partition_list)
            return committed
        return await self._run_async(*self._committed_spec(partition_list))

    async def _a_partitions_for(self, topic: str) -> list[PartitionInfo]:
        return await self._run_async(*self._partitions_for_spec(topic))

    async def _a_list_topics(self) -> dict[str, list[PartitionInfo]]:
        return await self._run_async(*self._list_topics_spec())

    async def _a_pause(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("pause", _lib.Consumer_pause_async, partitions)

    async def _a_resume(self, partitions: Iterable[TopicPartition]) -> None:
        await self._tp_op("resume", _lib.Consumer_resume_async, partitions)

    async def _a_offsets_for_times(self, timestamps_to_search: Mapping[TopicPartition, int]
                                   ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        timestamps = dict(timestamps_to_search)
        if self._in_callback():
            found: dict[TopicPartition, OffsetAndTimestamp | None] = self._reentrant_use(
                "offsets_for_times", timestamps)
            return found
        return await self._run_async(*self._offsets_for_times_spec(timestamps))

    async def _long_offsets(self, op: str, fn: Any, partitions: Iterable[TopicPartition]
                            ) -> dict[TopicPartition, int]:
        partition_list = list(partitions)
        if self._in_callback():
            offsets: dict[TopicPartition, int] = self._reentrant_use(op, partition_list)
            return offsets
        return await self._run_async(*self._long_offsets_spec(fn, partition_list))

    async def _a_beginning_offsets(self, partitions: Iterable[TopicPartition]
                                   ) -> dict[TopicPartition, int]:
        return await self._long_offsets("beginning_offsets",
                                        _lib.Consumer_beginning_offsets_async, partitions)

    async def _a_end_offsets(self, partitions: Iterable[TopicPartition]
                             ) -> dict[TopicPartition, int]:
        return await self._long_offsets("end_offsets", _lib.Consumer_end_offsets_async,
                                        partitions)

    async def _a_close(self, timeout: Duration | None, option: CloseOptions | None) -> None:
        timeout_ms, operation = close_args(timeout, option)
        if not self._begin_close():
            return
        try:
            await self._run_async(*self._close_spec(timeout_ms, operation))
        except ConcurrentModificationError:
            self._abort_close()
            raise
        except BaseException:
            await self._finish_close_off_loop()
            raise
        await self._finish_close_off_loop()

    async def _finish_close_off_loop(self) -> None:
        """``_finish_close`` waits for the uses in flight and joins the core's
        tasks: off the event loop."""
        await asyncio.get_running_loop().run_in_executor(None, self._finish_close)
