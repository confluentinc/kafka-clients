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

"""``Consumer``: Java's ``org.apache.kafka.clients.consumer.Consumer``.

A Java client interface, so a non-instantiable base class with all its methods,
in Java's declaration order (CLAUDE.md, Python Binding Conventions, Class
family): ``KafkaConsumer`` adds only Java's constructors, ``MockConsumer`` its
constructors and the Java mock's methods. The methods here call the C FFI
(Implementation over the FFI); ``MockConsumer`` supplies the private hooks
they call instead. The Javadoc of each is ``KafkaConsumer``'s, which the
interface's points to.

Each overload set is one method (Signatures): ``subscribe(topics[, callback])``
/ ``subscribe(pattern[, callback])`` (the client-side ``java.util.regex.Pattern``
overloads are dropped, Types), ``seek(partition, offset | offset_and_metadata)``,
``commit_nowait()`` / ``(callback)`` / ``(offsets, callback)`` and ``close()`` /
``(timeout)`` (``@Deprecated``) / ``(option)``, each checked by ``java_forms``.
``commitSync`` / ``commitAsync`` are ``commit()`` / ``commit_nowait()``.

Not generated, their FFI entry point being missing (``ffi-overload-gaps.md``):
the ``Duration`` overloads of ``commitSync``, ``committed``, ``position``,
``beginningOffsets``, ``endOffsets``, ``offsetsForTimes``, ``partitionsFor`` and
``listTopics``; ``clientInstanceId(Duration)``;
``registerMetricForSubscription`` / ``unregisterMetricFromSubscription``.
Dropped: ``enforceRebalance()`` / ``enforceRebalance(String)``, whose
``AsyncKafkaConsumer`` body only logs that it is unsupported *(deviation)*.

Java's ``close(Duration)`` is ``close(CloseOptions.timeout(timeout))``
(``AsyncKafkaConsumer.close(Duration)``), and ``close(timeout=…)`` calls the
same ``kafka_consumer_Consumer_close_with_option`` entry point: the derived
``kafka_consumer_Consumer_close_with_timeout`` has no ``_async`` form, and its
plain form cannot deliver the listener's ``on_partitions_revoked`` on this
thread.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import Form, java_forms
from confluent_kafka.concurrent_modification_error import ConcurrentModificationError

from ._base import _ConsumerState, blank_null_topics, close_args, poll_timeout_ms
from ._conversions import tp_to_spec
from .close_options import CloseOptions

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

__all__ = ["Consumer"]

K = TypeVar("K")
V = TypeVar("V")

_LOG = logging.getLogger("confluent_kafka.consumer")

CLOSE_DEPRECATED = ("This method has been deprecated since Kafka 4.1 and should use "
                    "close(option=...) instead.")

SUBSCRIBE_FORMS = (Form("topics"), Form("topics", "callback"),
                   Form("pattern", "callback"), Form("pattern"))
COMMIT_NOWAIT_FORMS = (Form(), Form("callback"), Form("offsets", "callback"))
SEEK_FORMS = (Form("partition", "offset"), Form("partition", "offset_and_metadata"))
CLOSE_FORMS = (Form(), Form("timeout", deprecated=CLOSE_DEPRECATED), Form("option"))


class Consumer(Generic[K, V], _ConsumerState):
    """The interface for the ``KafkaConsumer``.

    Java: ``org.apache.kafka.clients.consumer.Consumer<K, V>`` (``Closeable``:
    a context manager whose exit closes).
    """

    def __init__(self) -> None:
        if type(self) is Consumer:
            raise TypeError(
                "Consumer is a non-instantiable base; use KafkaConsumer or MockConsumer")
        _ConsumerState.__init__(self)

    def assignment(self) -> set[TopicPartition]:
        """Get the set of partitions currently assigned to this consumer. If
        subscription happened by directly assigning partitions using
        ``assign()``, this returns the same set previously assigned. If topic
        subscription was used, this returns the set of partitions currently
        assigned to the consumer, which may be none if the assignment hasn't
        happened yet or the partitions are in the process of getting
        reassigned."""
        return self._c_assignment()

    def subscription(self) -> set[str]:
        """Get the current subscription, or an empty set if no such call has
        been made."""
        return self._c_subscription()

    @overload
    def subscribe(self, *, topics: Iterable[str],
                  callback: ConsumerRebalanceListener | None = None) -> None: ...
    @overload
    def subscribe(self, *, pattern: SubscriptionPattern,
                  callback: ConsumerRebalanceListener | None = None) -> None: ...

    @java_forms(*SUBSCRIBE_FORMS)
    def subscribe(self, *, topics: Iterable[str] | None = None,
                  pattern: SubscriptionPattern | None = None,
                  callback: ConsumerRebalanceListener | None = None) -> None:
        """Subscribe to the given list of ``topics`` to get dynamically assigned
        partitions, or to the topics matching the broker-side RE2/J ``pattern``.
        Topic subscriptions are not incremental: this replaces the current
        assignment, if there is one. An empty ``topics`` is the same as
        ``unsubscribe()``.

        The ``callback`` is a ``ConsumerRebalanceListener`` invoked when the
        partitions assigned to the consumer change, on the caller's thread,
        inside ``poll()`` and the other waiting calls; subscribing without one
        releases the previous listener.

        Raises ``IllegalArgumentError`` if ``topics`` contains a null or empty
        topic, ``IllegalStateError`` if ``subscribe()`` is called after
        ``assign()`` or with an incompatible subscription type, and
        ``InvalidGroupIdError`` without a ``group.id``.
        """
        if topics is not None:
            self._c_subscribe_topics(topics, callback)
        else:
            self._c_subscribe_pattern(pattern, callback)

    def assign(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Manually assign a list of partitions to this consumer. This
        interface does not allow for incremental assignment and will replace
        the previous assignment (if there is one). An empty ``partitions`` is
        the same as ``unsubscribe()``. Manual topic assignment does not use the
        consumer's group management functionality.

        Raises ``IllegalArgumentError`` if ``partitions`` is ``None`` or contains
        a ``None`` partition or a partition with a null or empty topic, and
        ``IllegalStateError`` if ``assign()`` is called after ``subscribe()``.
        """
        self._c_assign(partitions)

    def unsubscribe(self) -> None:
        """Unsubscribe from topics currently subscribed with ``subscribe()`` or
        ``assign()``. This also clears any partitions directly assigned through
        ``assign()``. The listener's ``on_partitions_revoked`` (or
        ``on_partitions_lost``) runs on this thread before it returns."""
        self._c_unsubscribe()

    def poll(self, *, timeout: Duration) -> ConsumerRecords[K, V]:
        """Fetch data for the topics or partitions specified using one of the
        subscribe / assign APIs. It is an error to not have subscribed to any
        topics or partitions before polling for data.

        On each poll, the consumer will try to use the last consumed offset as
        the starting offset and fetch sequentially. This method returns
        immediately if there are records available or if the position
        advances past control records or aborted transactions when
        ``isolation.level=read_committed``; otherwise it waits up to
        ``timeout`` for records. The deserializers run on this thread; a
        failing one raises ``RecordDeserializationError`` and leaves the
        position at the record.

        Raises ``InvalidOffsetError`` if the offset for a partition is undefined
        or out of range and no offset reset policy has been configured,
        ``WakeupError`` if ``wakeup()`` is called before or while this method is
        called, ``IllegalArgumentError`` if ``timeout`` is negative,
        ``IllegalStateError`` if the consumer is not subscribed to any topics or
        manually assigned any partitions, and ``KafkaError`` for any other
        unrecoverable error.
        """
        return self._c_poll(timeout)

    def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None
               ) -> None:
        """Commit the offsets returned on the last ``poll()`` for all the
        subscribed topics and partitions, or the given ``offsets``. This is a
        synchronous commit and will block until either the commit succeeds, an
        unrecoverable error is encountered (in which case it is raised), or the
        ``default.api.timeout.ms`` expires (``TimeoutError``). Java's
        ``commitSync``.

        The committed offset should be the next message your application will
        consume, i.e. ``last_processed_message_offset + 1``.

        Raises ``CommitFailedError`` if the commit failed and cannot be retried,
        ``RebalanceInProgressError`` while the consumer is in the middle of a
        rebalance, ``WakeupError`` if ``wakeup()`` is called before or while this
        method is called, ``AuthorizationError`` if not authorized, and
        ``KafkaError`` for any other unrecoverable error.
        """
        self._c_commit(offsets)

    @overload
    def commit_nowait(self, *, callback: OffsetCommitCallback | None = None) -> None: ...
    @overload
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                      callback: OffsetCommitCallback) -> None: ...

    @java_forms(*COMMIT_NOWAIT_FORMS)
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None = None,
                      callback: OffsetCommitCallback | None = None) -> None:
        """Commit the offsets returned on the last ``poll()`` for the subscribed
        topics and partitions, or the given ``offsets``, without waiting. Java's
        ``commitAsync``.

        This is an asynchronous call and will not block. Any errors encountered
        are either passed to the ``callback`` (if provided) or discarded.
        Offsets committed through multiple calls to this API are guaranteed to
        be sent in the same order as the invocations. The ``callback`` runs on
        the caller's thread, inside a later call on this consumer (``poll()``,
        ``commit()``, ``commit_nowait()``, ``close()``, …).

        Unlike Java's ``commitAsync``, the call may deliver a queued
        ``ConsumerRebalanceListener`` callback, on this thread, while it waits
        for the offsets to commit (``consumer-threading.md`` §31); a listener
        error it raised is then raised here.
        """
        self._c_commit_nowait(offsets, callback)

    @overload
    def seek(self, *, partition: TopicPartition, offset: int) -> None: ...
    @overload
    def seek(self, *, partition: TopicPartition,
             offset_and_metadata: OffsetAndMetadata) -> None: ...

    @java_forms(*SEEK_FORMS)
    def seek(self, *, partition: TopicPartition, offset: int | None = None,
             offset_and_metadata: OffsetAndMetadata | None = None) -> None:
        """Overrides the fetch offsets that the consumer will use on the next
        ``poll()``: to ``offset``, or to the offset (and leader epoch) of
        ``offset_and_metadata``. If this API is invoked for the same partition
        more than once, the latest offset will be used on the next poll. Note
        that you may lose data if this API is arbitrarily used in the middle of
        consumption, to reset the fetch offsets.

        Raises ``IllegalArgumentError`` if the offset is negative and
        ``IllegalStateError`` if the partition is not currently assigned to
        this consumer.
        """
        self._c_seek(partition, offset, offset_and_metadata)

    def seek_to_beginning(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Seek to the first offset for each of the given partitions. This
        function evaluates lazily, seeking to the first offset in all
        partitions only when ``poll()`` or ``position()`` are called. If no
        partitions are provided, seek to the first offset for all of the
        currently assigned partitions."""
        self._c_seek_to_beginning(partitions)

    def seek_to_end(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Seek to the last offset for each of the given partitions. This
        function evaluates lazily, seeking to the final offset in all
        partitions only when ``poll()`` or ``position()`` are called. If no
        partitions are provided, seek to the final offset for all of the
        currently assigned partitions."""
        self._c_seek_to_end(partitions)

    def position(self, *, partition: TopicPartition) -> int:
        """Get the offset of the next record that will be fetched (if a record
        with that offset exists). This method may issue a remote call to the
        server if there is no current position for the given partition, and
        waits up to ``default.api.timeout.ms``.

        Raises ``IllegalStateError`` if the partition is not assigned to this
        consumer, ``InvalidOffsetError`` if no offset is currently defined for
        it, ``WakeupError`` on ``wakeup()``, and ``TimeoutError`` if the
        position cannot be determined before the timeout expires.
        """
        return self._c_position(partition)

    def committed(self, *, partitions: Iterable[TopicPartition]
                  ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        """Retrieve the last committed offset for the given partitions (whether
        the commit happened by this process or another). The returned offset is
        used as the consumption position in case of a failure. A partition
        without a committed offset maps to ``None``.

        This call waits up to ``default.api.timeout.ms`` for the offsets.
        """
        return self._c_committed(partitions)

    def metrics(self) -> dict[MetricName, Metric]:
        """Get the metrics kept by the consumer."""
        return self._c_metrics()

    def partitions_for(self, *, topic: str) -> list[PartitionInfo]:
        """Get metadata about the partitions for a given topic. This method
        issues a remote call to the server if it does not already have any
        metadata about the given topic, and waits up to
        ``default.api.timeout.ms``. Returns an empty list if the topic is not
        found and ``allow.auto.create.topics`` is not set."""
        return self._c_partitions_for(topic)

    def list_topics(self) -> dict[str, list[PartitionInfo]]:
        """Get metadata about partitions for all topics that the user is
        authorized to view. This method issues a remote call to the server and
        waits up to ``default.api.timeout.ms``."""
        return self._c_list_topics()

    def paused(self) -> set[TopicPartition]:
        """Get the set of partitions that were previously paused by a call to
        ``pause()``."""
        return self._c_paused()

    def pause(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Suspend fetching from the requested partitions. Future calls to
        ``poll()`` will not return any records from these partitions until they
        have been resumed using ``resume()``. Note that this method does not
        affect partition subscription. Raises ``IllegalStateError`` if any of the
        partitions is not currently assigned to this consumer."""
        self._c_pause(partitions)

    def resume(self, *, partitions: Iterable[TopicPartition]) -> None:
        """Resume specified partitions which have been paused with ``pause()``.
        New calls to ``poll()`` will return records from these partitions if
        there are any to be fetched. If the partitions were not previously
        paused, this method is a no-op."""
        self._c_resume(partitions)

    def offsets_for_times(self, *, timestamps_to_search: Mapping[TopicPartition, int]
                          ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        """Look up the offsets for the given partitions by timestamp. The
        returned offset for each partition is the earliest offset whose
        timestamp is greater than or equal to the given timestamp in the
        corresponding partition, or ``None`` if there is no such message. This
        is a blocking call waiting up to ``default.api.timeout.ms``.

        Raises ``IllegalArgumentError`` if a target timestamp is negative and
        ``UnsupportedVersionError`` if the broker does not support looking up
        the offsets by timestamp.
        """
        return self._c_offsets_for_times(timestamps_to_search)

    def beginning_offsets(self, *, partitions: Iterable[TopicPartition]
                          ) -> dict[TopicPartition, int]:
        """Get the first offset for the given partitions. This method does not
        change the current consumer position of the partitions, and waits up to
        ``default.api.timeout.ms``."""
        return self._c_beginning_offsets(partitions)

    def end_offsets(self, *, partitions: Iterable[TopicPartition]) -> dict[TopicPartition, int]:
        """Get the end offsets for the given partitions: the offset of the
        upcoming message, i.e. the offset of the last available message + 1 (or
        the last stable offset under ``read_committed``). This method does not
        change the current consumer position of the partitions, and waits up to
        ``default.api.timeout.ms``."""
        return self._c_end_offsets(partitions)

    def current_lag(self, *, topic_partition: TopicPartition) -> int | None:
        """Get the consumer's current lag on the partition, from the locally
        cached end offset; ``None`` if the lag is not known, for example when
        the consumer has not yet fetched the partition (Java's
        ``OptionalLong.empty()``)."""
        return self._c_current_lag(topic_partition)

    def group_metadata(self) -> ConsumerGroupMetadata:
        """Return the current group metadata associated with this consumer.
        Raises ``InvalidGroupIdError`` if the consumer has no ``group.id``."""
        return self._c_group_metadata()

    @overload
    def close(self, *, timeout: Duration | None = None) -> None: ...
    @overload
    def close(self, *, option: CloseOptions) -> None: ...

    @java_forms(*CLOSE_FORMS)
    def close(self, *, timeout: Duration | None = None,
              option: CloseOptions | None = None) -> None:
        """Close the consumer, waiting up to the ``timeout`` (by default the
        default close timeout, 30 seconds) for any needed cleanup; the
        ``option`` also sets the group membership operation. If auto-commit is
        enabled, this commits the current offsets if possible within the
        timeout. The listener's ``on_partitions_revoked`` and the pending commit
        callbacks run on this thread. ``close()`` twice is harmless; any other
        call after it raises ``IllegalStateError``. Note that ``wakeup()`` cannot
        be used to interrupt close.

        Deprecated: ``close(timeout=…)``. This method has been deprecated since
        Kafka 4.1 and should use ``close(option=…)`` instead.

        Raises ``IllegalArgumentError`` if the timeout is negative, and
        ``KafkaError`` for any other error during close.
        """
        self._c_close(timeout, option)

    def __enter__(self) -> Consumer[K, V]:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def wakeup(self) -> None:
        """Wakeup the consumer. This method is thread-safe and is useful in
        particular to abort a long poll: the call waiting (or the next one)
        raises ``WakeupError``, and the call after proceeds normally."""
        self._c_wakeup()

    # ---- the FFI implementations of the waiting calls ------------------------
    def _c_subscribe_topics(self, topics: Iterable[str],
                            callback: ConsumerRebalanceListener | None) -> None:
        topic_list = blank_null_topics(topics)
        previous, self._listener = self._listener, callback
        try:
            self._run_sync(*self._subscribe_topics_spec(topic_list, callback is not None))
        except BaseException:
            self._listener = previous
            raise

    def _c_subscribe_pattern(self, pattern: SubscriptionPattern | None,
                             callback: ConsumerRebalanceListener | None) -> None:
        assert pattern is not None
        previous, self._listener = self._listener, callback
        try:
            self._run_sync(*self._subscribe_pattern_spec(pattern.pattern(), callback is not None))
        except BaseException:
            self._listener = previous
            raise

    def _tp_op(self, op: str, fn: Any, partitions: Iterable[TopicPartition]) -> None:
        partition_list = list(partitions)
        if self._in_callback():
            self._reentrant_use(op, partition_list)
            return
        self._run_sync(*self._void_spec(fn, tp_to_spec(partition_list)))

    def _c_assign(self, partitions: Iterable[TopicPartition]) -> None:
        self._tp_op("assign", _lib.Consumer_assign_async,
                    self._assign_partitions(partitions))

    def _c_unsubscribe(self) -> None:
        self._run_sync(*self._void_spec(_lib.Consumer_unsubscribe_async))

    def _c_poll(self, timeout: Duration) -> ConsumerRecords[K, V]:
        result: Deserialized = self._run_sync(*self._poll_spec(poll_timeout_ms(timeout)))
        for partition, offset in result.rewind:
            try:
                self._run_sync(*self._seek_spec(partition, offset, None))
            except Exception:  # noqa: BLE001 - the deserialization error matters
                _LOG.exception("Could not seek %s back to offset %d", partition, offset)
        if result.error is not None and result.records.is_empty():
            raise result.error
        return result.records

    def _c_commit(self, offsets: Mapping[TopicPartition, OffsetAndMetadata] | None) -> None:
        if self._in_callback():
            self._reentrant_use("commit", offsets)
            return
        self._run_sync(*self._commit_spec(offsets))

    def _c_seek(self, partition: TopicPartition, offset: int | None,
                offset_and_metadata: OffsetAndMetadata | None) -> None:
        if self._in_callback():
            self._reentrant_use("seek", partition, offset, offset_and_metadata)
            return
        self._run_sync(*self._seek_spec(partition, offset, offset_and_metadata))

    def _c_seek_to_beginning(self, partitions: Iterable[TopicPartition]) -> None:
        self._tp_op("seek_to_beginning", _lib.Consumer_seek_to_beginning_async, partitions)

    def _c_seek_to_end(self, partitions: Iterable[TopicPartition]) -> None:
        self._tp_op("seek_to_end", _lib.Consumer_seek_to_end_async, partitions)

    def _c_position(self, partition: TopicPartition) -> int:
        if self._in_callback():
            position: int = self._reentrant_use("position", partition)
            return position
        fetched: int = self._run_sync(*self._position_spec(partition))
        return fetched

    def _c_committed(self, partitions: Iterable[TopicPartition]
                     ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        partition_list = list(partitions)
        if self._in_callback():
            committed: dict[TopicPartition, OffsetAndMetadata | None] = self._reentrant_use(
                "committed", partition_list)
            return committed
        return self._run_sync(*self._committed_spec(partition_list))

    def _c_partitions_for(self, topic: str) -> list[PartitionInfo]:
        return self._run_sync(*self._partitions_for_spec(topic))

    def _c_list_topics(self) -> dict[str, list[PartitionInfo]]:
        return self._run_sync(*self._list_topics_spec())

    def _c_pause(self, partitions: Iterable[TopicPartition]) -> None:
        self._tp_op("pause", _lib.Consumer_pause_async, partitions)

    def _c_resume(self, partitions: Iterable[TopicPartition]) -> None:
        self._tp_op("resume", _lib.Consumer_resume_async, partitions)

    def _c_offsets_for_times(self, timestamps_to_search: Mapping[TopicPartition, int]
                             ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        timestamps = dict(timestamps_to_search)
        if self._in_callback():
            found: dict[TopicPartition, OffsetAndTimestamp | None] = self._reentrant_use(
                "offsets_for_times", timestamps)
            return found
        return self._run_sync(*self._offsets_for_times_spec(timestamps))

    def _long_offsets(self, op: str, fn: Any, partitions: Iterable[TopicPartition]
                      ) -> dict[TopicPartition, int]:
        partition_list = list(partitions)
        if self._in_callback():
            offsets: dict[TopicPartition, int] = self._reentrant_use(op, partition_list)
            return offsets
        return self._run_sync(*self._long_offsets_spec(fn, partition_list))

    def _c_beginning_offsets(self, partitions: Iterable[TopicPartition]
                             ) -> dict[TopicPartition, int]:
        return self._long_offsets("beginning_offsets", _lib.Consumer_beginning_offsets_async,
                                  partitions)

    def _c_end_offsets(self, partitions: Iterable[TopicPartition]) -> dict[TopicPartition, int]:
        return self._long_offsets("end_offsets", _lib.Consumer_end_offsets_async, partitions)

    def _c_close(self, timeout: Duration | None, option: CloseOptions | None) -> None:
        timeout_ms, operation = close_args(timeout, option)
        if not self._begin_close():
            return
        try:
            self._run_sync(*self._close_spec(timeout_ms, operation))
        except ConcurrentModificationError:
            # Another thread is inside the consumer: Java's close() fails in
            # acquire() and the consumer stays open.
            self._abort_close()
            raise
        except BaseException:
            self._finish_close()
            raise
        self._finish_close()
