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

"""The rebalance listener stays registered across ``subscribe(topics=[])``,
sync and asyncio, against a broker.

``subscribe(emptyList)`` is Java's ``unsubscribe()``
(``org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#subscribeInternal``),
which keeps the subscription's listener, and ``close()`` then calls it
(``AsyncKafkaConsumer#runRebalanceCallbacksOnClose``): with the group
assignment snapshot still holding the partition, ``on_partitions_lost``. The
binding is the listener's only owner here, as for a Java caller that keeps no
reference to it, so a binding that released it at ``subscribe([])`` made
``close()`` call a freed listener. A new listener given with the empty
``topics`` is ignored, as Java ignores it.

A failed subscribe keeps its listener alive too:
``SubscriptionState#subscribe`` registers it before the subscription-type
check throws, so after ``assign`` a ``MockConsumer`` still calls it. That case
runs over the FFI's ``MockConsumer`` and needs no broker; the broker cases skip
without Docker (``kafka_broker``).
"""

from __future__ import annotations

import asyncio
import gc
from typing import Any

import pytest

from confluent_kafka.common import TopicPartition
from confluent_kafka.consumer import ConsumerRebalanceListener

from .broker_helpers import (
    DEADLINE, MODES, aw, consumer_configs, make_consumer, new_topic,
)


class _Recording(ConsumerRebalanceListener):
    """Appends ``(event, partitions)`` to ``events``, which the test keeps
    while it drops the listener itself."""

    def __init__(self, events: list[tuple[str, frozenset[TopicPartition]]]) -> None:
        self._events = events

    def on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        self._events.append(("revoked", frozenset(partitions)))

    def on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        self._events.append(("assigned", frozenset(partitions)))

    def on_partitions_lost(self, partitions: set[TopicPartition]) -> None:
        self._events.append(("lost", frozenset(partitions)))


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("new_listener", [False, True], ids=["without", "with-new-listener"])
def test_close_after_empty_subscribe_calls_the_kept_listener(
        kafka_broker: Any, mode: str, new_listener: bool) -> None:
    topic = new_topic(kafka_broker, "py-listener-release")
    partition = TopicPartition(topic=topic, partition=0)
    events: list[tuple[str, frozenset[TopicPartition]]] = []
    ignored: list[tuple[str, frozenset[TopicPartition]]] = []

    async def body() -> None:
        consumer = make_consumer(mode, consumer_configs(kafka_broker))
        await aw(consumer.subscribe(topics=[topic], callback=_Recording(events)))
        loop = asyncio.get_running_loop()
        end = loop.time() + DEADLINE
        while consumer.assignment() != {partition}:
            assert loop.time() < end, f"assignment {consumer.assignment()}"
            await aw(consumer.poll(timeout=0.2))
        if new_listener:
            await aw(consumer.subscribe(topics=[], callback=_Recording(ignored)))
        else:
            await aw(consumer.subscribe(topics=[]))
        assert consumer.assignment() == set()
        # The binding now holds the only reference to either listener.
        gc.collect()
        await aw(consumer.close())

    asyncio.run(body())
    assert ("assigned", frozenset({partition})) in events, events
    assert events[-1] == ("lost", frozenset({partition})), events
    assert ignored == []


def _ffi_mock_consumer() -> Any:
    """A ``KafkaConsumer`` over the FFI's ``kafka_consumer_MockConsumer_t``, so
    the binding's listener registration runs without a broker (the public
    ``MockConsumer`` is pure Python and never reaches it)."""
    import _confluentkafka as _lib  # type: ignore[import-not-found]

    from confluent_kafka.consumer import Consumer, KafkaConsumer

    consumer: Any = KafkaConsumer.__new__(KafkaConsumer)
    Consumer.__init__(consumer)
    consumer._h = _lib.Consumer_MockConsumer_new("earliest")
    consumer._group_id_defined = True
    consumer._pending_notify_ref = consumer._on_pending_notify
    _lib.Consumer__set_callbacks_notify(consumer._h, consumer._pending_notify_ref)
    return consumer


def test_failed_subscribe_keeps_the_listener_the_mock_registered() -> None:
    """Java's ``SubscriptionState#subscribe`` registers the listener before its
    subscription-type check throws, so after ``assign`` a failing
    ``subscribe(topics, callback)`` still leaves that listener registered, and
    ``MockConsumer#rebalance`` calls it. The binding must keep its context
    alive; it used to free it. No broker needed."""
    import _confluentkafka as _lib  # type: ignore[import-not-found]

    from confluent_kafka import IllegalStateError
    from confluent_kafka.consumer import CloseOptions

    tp = TopicPartition(topic="t", partition=0)
    events: list[tuple[str, frozenset[TopicPartition]]] = []
    consumer = _ffi_mock_consumer()
    consumer.assign(partitions=[tp])
    with pytest.raises(IllegalStateError):
        consumer.subscribe(topics=["t"], callback=_Recording(events))
    # The binding now holds the only reference to the listener.
    gc.collect()
    # Revokes tp through the registered listener, then fails to take an
    # assignment for a consumer that is not subscribed; only the call counts.
    _lib.MockConsumer_rebalance(consumer._h, [])
    assert events == [("revoked", frozenset({tp}))], events
    consumer.close(option=CloseOptions.timeout(timeout=0))
