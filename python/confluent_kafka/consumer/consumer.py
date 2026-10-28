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
they call instead, doing what Java's mock does. The Javadoc of each is
``KafkaConsumer``'s, which the interface's points to; a note that holds only for
``KafkaConsumer`` says so.

Each overload set is one method (Signatures): ``subscribe(topics[, callback])``
/ ``subscribe(pattern[, callback])`` (the client-side ``java.util.regex.Pattern``
overloads are dropped, Types), ``seek(partition, offset | offset_and_metadata)``
and ``commit_nowait()`` / ``(callback)`` / ``(offsets, callback)``, each checked
by ``java_forms``, and ``close()`` / ``(option)``.
``commitSync`` / ``commitAsync`` are ``commit()`` / ``commit_nowait()``.

The ``Duration`` overloads of ``commitSync``, ``position``, ``committed``,
``partitionsFor``, ``listTopics``, ``offsetsForTimes``, ``beginningOffsets`` and
``endOffsets`` are the optional ``timeout`` of their method (Signatures,
Timeouts): left out calls the overload without it, a given one the
``kafka_consumer_Consumer_*_with_timeout`` entry point; a ``None`` is Java's null
``Duration``, ``TypeError("timeout must not be None")``. Every parameter some
overload omits defaults to the private ``MISSING``; an explicit ``None`` is
Java's ``null`` (Signatures). On ``KafkaConsumer``, inside a callback this
consumer delivers, every one of them, ``partitions_for``, ``list_topics`` and
``group_metadata`` go through the core's ``ConsumerHandle``, its
``_with_timeout`` forms taking the ``timeout``; ``assign``, ``subscribe``,
``subscription``, ``unsubscribe``, ``poll`` and ``close``, which the handle has
no form of, raise ``ConcurrentModificationError`` there *(deviation: Java's
listener may call them; goes when the handle has them)*; ``MockConsumer``
allows every call there, as Java's re-entrant mock does.

Not generated, their FFI entry point being missing (the name looked for, after
the ``kafka_consumer_Consumer_`` prefix, in parentheses):
``clientInstanceId(Duration)`` (``client_instance_id``);
``registerMetricForSubscription`` / ``unregisterMetricFromSubscription``
(``register_metric_for_subscription`` /
``unregister_metric_from_subscription``).
Not generated, Java deprecating it (Class family): ``close(Duration)``.
Not generated, the C FFI having no entry point for it: the share-consumer
family (``ShareConsumer``, ``KafkaShareConsumer``, ``MockShareConsumer`` and
their ``Async`` peers, ``AcknowledgeType``, ``AcknowledgementCommitCallback``).
Dropped: ``enforceRebalance()`` / ``enforceRebalance(String)``, whose
``AsyncKafkaConsumer`` body only logs that it is unsupported *(deviation)*.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka._args import MISSING, Form, java_forms, reject_str
from confluent_kafka.concurrent_modification_error import ConcurrentModificationError

from ._base import (
    _ConsumerState, blank_null_topics, close_args, empty_input_timeout, poll_timeout_ms,
)
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

SUBSCRIBE_FORMS = (Form("topics"), Form("topics", "callback"),
                   Form("pattern", "callback"), Form("pattern"))
# commitAsync() passes a null callback to commitAsync(callback).
COMMIT_NOWAIT_FORMS = (Form(), Form("callback", defaults={"callback": None}),
                       Form("offsets", "callback"))
SEEK_FORMS = (Form("partition", "offset"), Form("partition", "offset_and_metadata"))


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
    def subscribe(self, *, topics: Iterable[str]) -> None: ...
    @overload
    def subscribe(self, *, topics: Iterable[str], callback: ConsumerRebalanceListener) -> None: ...
    @overload
    def subscribe(self, *, pattern: SubscriptionPattern,
                  callback: ConsumerRebalanceListener) -> None: ...
    @overload
    def subscribe(self, *, pattern: SubscriptionPattern) -> None: ...

    @java_forms(*SUBSCRIBE_FORMS)
    def subscribe(self, *, topics: Iterable[str] = MISSING,
                  pattern: SubscriptionPattern = MISSING,
                  callback: ConsumerRebalanceListener = MISSING) -> None:
        """Subscribe to the given list of ``topics`` to get dynamically assigned
        partitions, or to the topics matching the broker-side RE2/J ``pattern``.
        Topic subscriptions are not incremental: this replaces the current
        assignment, if there is one. An empty ``topics`` is the same as
        ``unsubscribe()``.

        The ``callback`` is a ``ConsumerRebalanceListener`` invoked when the
        partitions assigned to the consumer change, on the caller's thread: on
        ``KafkaConsumer`` inside ``poll()`` or ``unsubscribe()`` (and ``close()``
        revokes or loses the group assignment through it), as in Java; on
        ``MockConsumer`` inside ``rebalance()`` and ``lose_partitions()``, as in
        Java's mock. Subscribing without one releases the previous listener.

        Raises ``IllegalArgumentError`` if ``callback`` is ``None``
        ("RebalanceListener cannot be None"), if ``topics`` or ``pattern`` is
        ``None`` or ``topics`` contains a null or empty topic,
        ``IllegalStateError`` if ``subscribe()`` is called after ``assign()`` or
        with an incompatible subscription type, and ``InvalidGroupIdError``
        without a ``group.id``.
        """
        if topics is not MISSING:
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
        ``assign()``. On ``KafkaConsumer`` the listener's ``on_partitions_revoked``
        (or ``on_partitions_lost``) runs on this thread before it returns;
        ``MockConsumer`` runs no listener here, as Java's mock."""
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
        ``timeout`` for records. On ``KafkaConsumer`` the deserializers run on
        this thread; a failing one raises ``RecordDeserializationError`` and
        leaves the position at the record. ``MockConsumer`` returns the records
        as they were added, as Java's mock.

        Raises ``InvalidOffsetError`` if the offset for a partition is undefined
        or out of range and no offset reset policy has been configured,
        ``WakeupError`` if ``wakeup()`` is called before or while this method is
        called, ``IllegalArgumentError`` if ``timeout`` is negative,
        ``IllegalStateError`` if the consumer is not subscribed to any topics or
        manually assigned any partitions, and ``KafkaError`` for any other
        unrecoverable error.
        """
        return self._c_poll(timeout)

    @overload
    def commit(self) -> None: ...
    @overload
    def commit(self, *, timeout: Duration) -> None: ...
    @overload
    def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata]) -> None: ...
    @overload
    def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata], timeout: Duration
               ) -> None: ...
    def commit(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] = MISSING,
               timeout: Duration = MISSING) -> None:
        """Commit the offsets returned on the last ``poll()`` for all the
        subscribed topics and partitions, or the given ``offsets``. This is a
        synchronous commit and will block until either the commit succeeds, an
        unrecoverable error is encountered (in which case it is raised), or the
        ``timeout`` expires (``TimeoutError``), ``default.api.timeout.ms``
        without one. Java's ``commitSync``.

        The committed offset should be the next message your application will
        consume, i.e. ``last_processed_message_offset + 1``.

        Raises ``CommitFailedError`` if the commit failed and cannot be retried,
        ``RebalanceInProgressError`` while the consumer is in the middle of a
        rebalance, ``WakeupError`` if ``wakeup()`` is called before or while this
        method is called, ``AuthorizationError`` if not authorized,
        ``IllegalArgumentError`` if ``timeout`` is negative, ``TypeError`` if
        ``offsets`` or ``timeout`` is ``None``, and ``KafkaError`` for any other
        unrecoverable error.
        """
        self._c_commit(offsets, timeout)

    @overload
    def commit_nowait(self) -> None: ...
    @overload
    def commit_nowait(self, *, callback: OffsetCommitCallback | None) -> None: ...
    @overload
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                      callback: OffsetCommitCallback | None) -> None: ...

    @java_forms(*COMMIT_NOWAIT_FORMS)
    def commit_nowait(self, *, offsets: Mapping[TopicPartition, OffsetAndMetadata] = MISSING,
                      callback: OffsetCommitCallback | None = MISSING) -> None:
        """Commit the offsets returned on the last ``poll()`` for the subscribed
        topics and partitions, or the given ``offsets``, without waiting. Java's
        ``commitAsync``. ``commit_nowait(offsets=…, callback=None)`` is Java's
        ``commitAsync(offsets, null)``; ``offsets=None`` raises ``TypeError``, as
        Java's ``new HashMap<>(offsets)`` does.

        This is an asynchronous call and will not block. Any errors encountered
        are either passed to the ``callback`` (if provided) or discarded.
        Offsets committed through multiple calls to this API are guaranteed to
        be sent in the same order as the invocations. On ``KafkaConsumer`` the
        ``callback`` runs on the caller's thread, inside a later call on this
        consumer (``poll()``, ``commit()``, ``commit_nowait()``, ``close()``, …);
        ``MockConsumer`` runs it inside this call, as Java's mock runs it inside
        ``commitAsync``, and an exception it raises propagates out of this call.

        On ``KafkaConsumer``, called from inside a callback this consumer
        delivers (a listener method or another commit's ``callback``), it first
        runs the commit callbacks queued behind the running one, inside this
        call, as Java's ``commitAsync`` does; its own ``callback`` runs inside a
        later call.

        An exception the ``callback`` raises propagates out of the call that
        delivers it, as Java's ``onComplete`` is called without a ``try``:
        ``poll()`` raises it wrapped in a ``KafkaError`` (unless it is one, as
        Java's ``maybeWrapAsKafkaException``), ``close()`` wrapped in the
        ``KafkaError`` "Failed to close kafka consumer" (unless it is a
        ``KeyboardInterrupt``, as Java's ``close`` wraps every exception but an
        ``InterruptException``), ``commit()`` and ``commit_nowait()`` raise the
        very exception; the callbacks queued behind it run inside a later call.

        The ``callback`` is a plain function here: an ``async def`` one is
        rejected when it is delivered, its coroutine closed, not awaited, and
        the delivering call raises ``TypeError`` (``MockConsumer`` raises it
        from this call). Only ``AsyncConsumer`` awaits one.
        """
        self._c_commit_nowait(offsets, callback)

    @overload
    def seek(self, *, partition: TopicPartition, offset: int) -> None: ...
    @overload
    def seek(self, *, partition: TopicPartition,
             offset_and_metadata: OffsetAndMetadata) -> None: ...

    @java_forms(*SEEK_FORMS)
    def seek(self, *, partition: TopicPartition, offset: int = MISSING,
             offset_and_metadata: OffsetAndMetadata = MISSING) -> None:
        """Overrides the fetch offsets that the consumer will use on the next
        ``poll()``: to ``offset``, or to the offset (and leader epoch) of
        ``offset_and_metadata``. If this API is invoked for the same partition
        more than once, the latest offset will be used on the next poll. Note
        that you may lose data if this API is arbitrarily used in the middle of
        consumption, to reset the fetch offsets.

        Raises ``IllegalArgumentError`` if the offset is negative,
        ``IllegalStateError`` if the partition is not currently assigned to
        this consumer (a ``None`` partition never is), and ``TypeError`` if
        ``offset`` or ``offset_and_metadata`` is ``None``.
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

    @overload
    def position(self, *, partition: TopicPartition) -> int: ...
    @overload
    def position(self, *, partition: TopicPartition, timeout: Duration) -> int: ...
    def position(self, *, partition: TopicPartition, timeout: Duration = MISSING) -> int:
        """Get the offset of the next record that will be fetched (if a record
        with that offset exists). This method may issue a remote call to the
        server if there is no current position for the given partition, and
        waits up to ``timeout``, ``default.api.timeout.ms`` without one.

        Raises ``IllegalStateError`` if the partition is not assigned to this
        consumer (a ``None`` partition never is), ``InvalidOffsetError`` if no offset is currently defined for
        it, ``WakeupError`` on ``wakeup()``, ``IllegalArgumentError`` if
        ``timeout`` is negative, and ``TimeoutError`` if the position cannot be
        determined before the timeout expires.
        """
        return self._c_position(partition, timeout)

    @overload
    def committed(self, *, partitions: Iterable[TopicPartition]
                  ) -> dict[TopicPartition, OffsetAndMetadata | None]: ...
    @overload
    def committed(self, *, partitions: Iterable[TopicPartition], timeout: Duration
                  ) -> dict[TopicPartition, OffsetAndMetadata | None]: ...
    def committed(self, *, partitions: Iterable[TopicPartition],
                  timeout: Duration = MISSING
                  ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        """Retrieve the last committed offset for the given partitions (whether
        the commit happened by this process or another). The returned offset is
        used as the consumption position in case of a failure. A partition
        without a committed offset maps to ``None``.

        This call waits up to ``timeout`` for the offsets,
        ``default.api.timeout.ms`` without one, and raises ``TimeoutError`` if
        they cannot be fetched before it expires.
        """
        return self._c_committed(partitions, timeout)

    def metrics(self) -> dict[MetricName, Metric]:
        """Get the metrics kept by the consumer."""
        return self._c_metrics()

    @overload
    def partitions_for(self, *, topic: str) -> list[PartitionInfo]: ...
    @overload
    def partitions_for(self, *, topic: str, timeout: Duration) -> list[PartitionInfo]: ...
    def partitions_for(self, *, topic: str, timeout: Duration = MISSING
                       ) -> list[PartitionInfo]:
        """Get metadata about the partitions for a given topic. This method
        issues a remote call to the server if it does not already have any
        metadata about the given topic, and waits up to ``timeout``,
        ``default.api.timeout.ms`` without one (``TimeoutError`` once it
        expires; a zero ``timeout`` raises it at once when the metadata is not
        cached). Returns an empty list if the topic is not found and
        ``allow.auto.create.topics`` is not set. A ``None`` topic raises
        ``TypeError``, or ``TimeoutError`` for a zero ``timeout``, as Java's
        ``partitionsFor(null, timeout)`` does."""
        return self._c_partitions_for(topic, timeout)

    @overload
    def list_topics(self) -> dict[str, list[PartitionInfo]]: ...
    @overload
    def list_topics(self, *, timeout: Duration) -> dict[str, list[PartitionInfo]]: ...
    def list_topics(self, *, timeout: Duration = MISSING
                    ) -> dict[str, list[PartitionInfo]]:
        """Get metadata about partitions for all topics that the user is
        authorized to view. This method issues a remote call to the server and
        waits up to ``timeout``, ``default.api.timeout.ms`` without one
        (``TimeoutError`` once it expires, at once for a zero ``timeout``)."""
        return self._c_list_topics(timeout)

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

    @overload
    def offsets_for_times(self, *, timestamps_to_search: Mapping[TopicPartition, int]
                          ) -> dict[TopicPartition, OffsetAndTimestamp | None]: ...
    @overload
    def offsets_for_times(self, *, timestamps_to_search: Mapping[TopicPartition, int],
                          timeout: Duration) -> dict[TopicPartition, OffsetAndTimestamp | None]: ...
    def offsets_for_times(self, *, timestamps_to_search: Mapping[TopicPartition, int],
                          timeout: Duration = MISSING
                          ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        """Look up the offsets for the given partitions by timestamp. The
        returned offset for each partition is the earliest offset whose
        timestamp is greater than or equal to the given timestamp in the
        corresponding partition, or ``None`` if there is no such message. This
        is a blocking call waiting up to ``timeout``, ``default.api.timeout.ms``
        without one; a zero ``timeout`` returns every partition mapped to
        ``None`` at once.

        Raises ``IllegalArgumentError`` if a target timestamp or the
        ``timeout`` is negative, ``UnsupportedVersionError`` if the broker does
        not support looking up the offsets by timestamp, and ``TimeoutError``
        if the offsets cannot be fetched before the timeout expires.
        """
        return self._c_offsets_for_times(timestamps_to_search, timeout)

    @overload
    def beginning_offsets(self, *, partitions: Iterable[TopicPartition]
                          ) -> dict[TopicPartition, int]: ...
    @overload
    def beginning_offsets(self, *, partitions: Iterable[TopicPartition], timeout: Duration
                          ) -> dict[TopicPartition, int]: ...
    def beginning_offsets(self, *, partitions: Iterable[TopicPartition],
                          timeout: Duration = MISSING) -> dict[TopicPartition, int]:
        """Get the first offset for the given partitions. This method does not
        change the current consumer position of the partitions, and waits up to
        ``timeout``, ``default.api.timeout.ms`` without one (``TimeoutError``
        once it expires; a zero ``timeout`` returns an empty map at once)."""
        return self._c_beginning_offsets(partitions, timeout)

    @overload
    def end_offsets(self, *, partitions: Iterable[TopicPartition]) -> dict[TopicPartition, int]: ...
    @overload
    def end_offsets(self, *, partitions: Iterable[TopicPartition], timeout: Duration
                    ) -> dict[TopicPartition, int]: ...
    def end_offsets(self, *, partitions: Iterable[TopicPartition],
                    timeout: Duration = MISSING) -> dict[TopicPartition, int]:
        """Get the end offsets for the given partitions: the offset of the
        upcoming message, i.e. the offset of the last available message + 1 (or
        the last stable offset under ``read_committed``). This method does not
        change the current consumer position of the partitions, and waits up to
        ``timeout``, ``default.api.timeout.ms`` without one (``TimeoutError``
        once it expires; a zero ``timeout`` returns an empty map at once)."""
        return self._c_end_offsets(partitions, timeout)

    def current_lag(self, *, topic_partition: TopicPartition) -> int | None:
        """Get the consumer's current lag on the partition, from the locally
        cached end offset; ``None`` if the lag is not known, for example when
        the consumer has not yet fetched the partition (Java's
        ``OptionalLong.empty()``). When the end offset is not known yet, the
        call also asks the broker for it, so a later call can answer; it waits
        for the consumer's background task, as Java's ``currentLag`` does (on
        ``KafkaConsumer``; ``MockConsumer`` answers from the end offsets it was
        given, as Java's mock).

        Raises ``IllegalStateError`` if the partition is not assigned to this
        consumer."""
        return self._c_current_lag(topic_partition)

    def group_metadata(self) -> ConsumerGroupMetadata:
        """Return the current group metadata associated with this consumer.
        Raises ``InvalidGroupIdError`` if the consumer has no ``group.id``."""
        return self._c_group_metadata()

    @overload
    def close(self) -> None: ...
    @overload
    def close(self, *, option: CloseOptions) -> None: ...
    def close(self, *, option: CloseOptions = MISSING) -> None:
        """Close the consumer, waiting up to the ``option``'s timeout (by
        default the default close timeout, 30 seconds) for any needed cleanup;
        the ``option`` also sets the group membership operation. If auto-commit
        is enabled, this commits the current offsets if possible within the
        timeout. The listener's ``on_partitions_revoked`` and the pending commit
        callbacks run on this thread. ``close()`` twice is harmless. On
        ``KafkaConsumer`` any other call after it raises ``IllegalStateError``,
        except ``wakeup()``, which then does nothing, as Java's, and including
        ``metrics()`` *(deviation: Java's ``metrics()`` makes no closed check, since
        its object outlives ``close()``; here the native handle does not)*; a
        ``MockConsumer`` answers after ``close()`` as Java's mock does (``poll()``
        and ``commit()`` raise, ``assignment()`` and ``pause()`` answer). Note that
        ``wakeup()`` cannot be used to interrupt close.

        Raises ``IllegalArgumentError`` if the timeout is negative,
        ``TypeError`` if ``option`` is ``None`` (Java's ``option.timeout()`` on a
        null), and ``KafkaError`` for any other error during close.

        On ``MockConsumer`` ``close()`` only marks the mock closed, as Java's
        mock: it commits nothing, runs no callback, reads no ``option`` and
        keeps the listener and the poll tasks.
        """
        self._c_close(option)

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
                            callback: ConsumerRebalanceListener) -> None:
        reject_str("topics", topics)  # the shape check precedes the others
        listener = self._subscribe_listener(callback)
        self._check_not_null("subscribe_topics", topics)
        topic_list = blank_null_topics(topics)
        previous, self._listener = self._listener, listener
        try:
            self._run_sync(*self._subscribe_topics_spec(topic_list, listener))
        except BaseException:
            self._listener = previous
            raise

    def _c_subscribe_pattern(self, pattern: SubscriptionPattern,
                             callback: ConsumerRebalanceListener) -> None:
        listener = self._subscribe_listener(callback)
        self._check_not_null("subscribe_pattern", pattern)
        if pattern.pattern() is None:
            # Java's subscriptionPattern.pattern().isEmpty() dereferences it; the FFI
            # cannot carry it (Types; exception 4). Java checks it after acquire()
            # (subscribeToRegex), so the closed and the concurrency checks come first.
            with self._use():
                raise TypeError("SubscriptionPattern.pattern must not be None")
        previous, self._listener = self._listener, listener
        try:
            self._run_sync(*self._subscribe_pattern_spec(pattern.pattern(), listener))
        except BaseException:
            self._listener = previous
            raise

    def _tp_op(self, op: str, fn: Any, partitions: Iterable[TopicPartition]) -> None:
        self._check_not_null(op, partitions)
        partition_list = list(partitions)
        if self._in_callback():
            self._reentrant_use(op, partition_list)
            return
        self._run_sync(*self._void_spec(fn, tp_to_spec(partition_list)))

    def _c_assign(self, partitions: Iterable[TopicPartition]) -> None:
        # Not on the ConsumerHandle (KIP-1306's RebalanceConsumer has no
        # assign): from inside a callback it reaches the busy consumer and
        # reports ConcurrentModificationError, as Java's acquire() does from
        # another thread.
        partition_list = list(self._assign_partitions(partitions))
        self._run_sync(*self._void_spec(_lib.Consumer_assign_cb, tp_to_spec(partition_list)))

    def _c_unsubscribe(self) -> None:
        self._run_sync(*self._void_spec(_lib.Consumer_unsubscribe_cb))

    def _c_poll(self, timeout: Duration) -> ConsumerRecords[K, V]:
        # The closed check first, then Java's Timer check (Order).
        self._check_open()
        timeout_ms = poll_timeout_ms(timeout)
        result: Deserialized = self._run_sync(*self._poll_spec(timeout_ms),
                                              abandoned=self._fetched_positions)
        interrupted = self._rewind_sync(result.rewind)
        if interrupted is not None:
            # Ctrl+C during the seeks back: the records are not returned either.
            self._rewind_sync(result.unreturned())
            raise interrupted
        if result.error is not None and result.records.is_empty():
            raise result.error
        return result.records

    def _c_commit(self, offsets: Mapping[TopicPartition, OffsetAndMetadata],
                  timeout: Duration = MISSING) -> None:
        offsets = self._offsets_arg(offsets)
        timeout_ms = self._timeout_ms(timeout, timer=True, group=True)
        if self._in_callback():
            self._reentrant_use("commit", offsets, timeout_ms)
            return
        self._run_sync(*self._commit_spec(offsets, timeout_ms))

    def _c_seek(self, partition: TopicPartition, offset: int,
                offset_and_metadata: OffsetAndMetadata) -> None:
        to, oam = self._seek_args(partition, offset, offset_and_metadata)
        if self._in_callback():
            self._reentrant_use("seek", partition, to, oam)
            return
        self._run_sync(*self._seek_spec(partition, to, oam))

    def _c_seek_to_beginning(self, partitions: Iterable[TopicPartition]) -> None:
        self._tp_op("seek_to_beginning", _lib.Consumer_seek_to_beginning_cb, partitions)

    def _c_seek_to_end(self, partitions: Iterable[TopicPartition]) -> None:
        self._tp_op("seek_to_end", _lib.Consumer_seek_to_end_cb, partitions)

    def _c_position(self, partition: TopicPartition, timeout: Duration = MISSING) -> int:
        self._check_position_partition(partition)
        timeout_ms = self._timeout_ms(timeout, timer=True)
        if self._in_callback():
            position: int = self._reentrant_use("position", partition, timeout_ms)
            return position
        fetched: int = self._run_sync(*self._position_spec(partition, timeout_ms))
        return fetched

    def _c_committed(self, partitions: Iterable[TopicPartition],
                     timeout: Duration = MISSING
                     ) -> dict[TopicPartition, OffsetAndMetadata | None]:
        # Read a one-shot iterable once, so its emptiness is seen (Java's empty
        # Set returns {} before the timeout is read).
        partitions = cast("Iterable[TopicPartition]",
                          None if partitions is None else list(partitions))
        timeout = empty_input_timeout(partitions, timeout)
        self._check_committed_timeout(partitions, timeout)
        timeout_ms = self._timeout_ms(timeout, group=True)
        self._check_committed_partitions(partitions)
        partition_list = list(partitions)
        if self._in_callback():
            committed: dict[TopicPartition, OffsetAndMetadata | None] = self._reentrant_use(
                "committed", partition_list, timeout_ms)
            return committed
        return self._run_sync(*self._committed_spec(partition_list, timeout_ms))

    def _c_current_lag(self, topic_partition: TopicPartition) -> int | None:
        self._check_not_null("current_lag", topic_partition)
        if self._in_callback():
            in_callback: int | None = self._reentrant_use("current_lag", topic_partition)
            return in_callback
        lag: int | None = self._run_sync(*self._current_lag_spec(topic_partition))
        return lag

    def _c_partitions_for(self, topic: str, timeout: Duration = MISSING
                          ) -> list[PartitionInfo]:
        timeout_ms = self._timeout_ms(timeout)
        self._check_partitions_for_topic(topic, timeout_ms)
        if self._in_callback():
            infos: list[PartitionInfo] = self._reentrant_use("partitions_for", topic, timeout_ms)
            return infos
        return self._run_sync(*self._partitions_for_spec(topic, timeout_ms))

    def _c_list_topics(self, timeout: Duration = MISSING) -> dict[str, list[PartitionInfo]]:
        timeout_ms = self._timeout_ms(timeout)
        if self._in_callback():
            topics: dict[str, list[PartitionInfo]] = self._reentrant_use("list_topics", timeout_ms)
            return topics
        return self._run_sync(*self._list_topics_spec(timeout_ms))

    def _c_pause(self, partitions: Iterable[TopicPartition]) -> None:
        self._tp_op("pause", _lib.Consumer_pause_cb, partitions)

    def _c_resume(self, partitions: Iterable[TopicPartition]) -> None:
        self._tp_op("resume", _lib.Consumer_resume_cb, partitions)

    def _c_offsets_for_times(self, timestamps_to_search: Mapping[TopicPartition, int],
                             timeout: Duration = MISSING
                             ) -> dict[TopicPartition, OffsetAndTimestamp | None]:
        self._check_not_null("offsets_for_times", timestamps_to_search)
        timeout_ms = self._timeout_ms(empty_input_timeout(timestamps_to_search, timeout))
        timestamps = dict(timestamps_to_search)
        if self._in_callback():
            found: dict[TopicPartition, OffsetAndTimestamp | None] = self._reentrant_use(
                "offsets_for_times", timestamps, timeout_ms)
            return found
        return self._run_sync(*self._offsets_for_times_spec(timestamps, timeout_ms))

    def _long_offsets(self, op: str, fn: Any, partitions: Iterable[TopicPartition],
                      timeout: Duration) -> dict[TopicPartition, int]:
        self._check_not_null(op, partitions)
        partition_list = list(partitions)
        timeout_ms = self._timeout_ms(empty_input_timeout(partition_list, timeout))
        if self._in_callback():
            offsets: dict[TopicPartition, int] = self._reentrant_use(op, partition_list,
                                                                      timeout_ms)
            return offsets
        return self._run_sync(*self._long_offsets_spec(fn, partition_list, timeout_ms))

    def _c_beginning_offsets(self, partitions: Iterable[TopicPartition],
                             timeout: Duration = MISSING) -> dict[TopicPartition, int]:
        return self._long_offsets("beginning_offsets", _lib.Consumer_beginning_offsets_cb,
                                  partitions, timeout)

    def _c_end_offsets(self, partitions: Iterable[TopicPartition],
                       timeout: Duration = MISSING) -> dict[TopicPartition, int]:
        return self._long_offsets("end_offsets", _lib.Consumer_end_offsets_cb, partitions,
                                  timeout)

    def _c_close(self, option: CloseOptions) -> None:
        if self._is_closed():
            return
        timeout_ms, operation = close_args(option)
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
