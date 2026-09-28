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

"""``SubscriptionState``: Java's
``org.apache.kafka.clients.consumer.internals.SubscriptionState`` (private).

An ``internals`` class, so not public (CLAUDE.md, Python Binding Conventions,
Scope). ``MockConsumer`` keeps its subscription, assignment and positions in
one, as Java's mock does; only the methods the mock calls are translated, with
Java's nested ``TopicPartitionState``, ``FetchStates`` and ``FetchPosition``.
The client-side ``java.util.regex.Pattern`` subscription is dropped with the
``subscribe(Pattern)`` overloads (Types).
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from confluent_kafka.common.topic_partition import TopicPartition
from confluent_kafka.illegal_argument_error import IllegalArgumentError
from confluent_kafka.illegal_state_error import IllegalStateError

from ._auto_offset_reset_strategy import AutoOffsetResetStrategy
from .offset_and_metadata import OffsetAndMetadata

if TYPE_CHECKING:
    from .consumer_rebalance_listener import ConsumerRebalanceListener
    from .subscription_pattern import SubscriptionPattern

__all__ = ["FetchPosition", "SubscriptionState"]

_SUBSCRIPTION_EXCEPTION_MESSAGE = (
    "Subscription to topics, partitions and pattern are mutually exclusive")


class _SubscriptionType(Enum):
    NONE = "NONE"
    AUTO_TOPICS = "AUTO_TOPICS"
    AUTO_PATTERN = "AUTO_PATTERN"
    AUTO_PATTERN_RE2J = "AUTO_PATTERN_RE2J"
    USER_ASSIGNED = "USER_ASSIGNED"
    AUTO_TOPICS_SHARE = "AUTO_TOPICS_SHARE"


class _FetchStates(Enum):
    """Java's ``FetchStates``: valid transitions, whether a position is
    required, whether it is valid for fetching."""

    INITIALIZING = "INITIALIZING"
    FETCHING = "FETCHING"
    AWAIT_RESET = "AWAIT_RESET"
    AWAIT_VALIDATION = "AWAIT_VALIDATION"

    def transition_to(self, new_state: _FetchStates) -> _FetchStates:
        return new_state if new_state in _VALID_TRANSITIONS[self] else self

    def requires_position(self) -> bool:
        return self in (_FetchStates.FETCHING, _FetchStates.AWAIT_VALIDATION)

    def has_valid_position(self) -> bool:
        return self is _FetchStates.FETCHING


_VALID_TRANSITIONS = {
    _FetchStates.INITIALIZING: (_FetchStates.FETCHING, _FetchStates.AWAIT_RESET,
                                _FetchStates.AWAIT_VALIDATION),
    _FetchStates.FETCHING: (_FetchStates.FETCHING, _FetchStates.AWAIT_RESET,
                            _FetchStates.AWAIT_VALIDATION),
    _FetchStates.AWAIT_RESET: (_FetchStates.FETCHING, _FetchStates.AWAIT_RESET),
    _FetchStates.AWAIT_VALIDATION: (_FetchStates.FETCHING, _FetchStates.AWAIT_RESET,
                                    _FetchStates.AWAIT_VALIDATION),
}


class FetchPosition:
    """Java's ``FetchPosition``: the offset and the epoch of the last record
    consumed (the current leader is not modelled: the mock has no metadata)."""

    __slots__ = ("offset", "offset_epoch")

    def __init__(self, offset: int, offset_epoch: int | None = None) -> None:
        self.offset = offset
        self.offset_epoch = offset_epoch


class _TopicPartitionState:
    __slots__ = ("_fetch_state", "position", "_paused", "_reset_strategy")

    def __init__(self) -> None:
        self._fetch_state = _FetchStates.INITIALIZING
        self.position: FetchPosition | None = None
        self._paused = False
        self._reset_strategy: AutoOffsetResetStrategy | None = None

    def _transition_state(self, new_state: _FetchStates, position: FetchPosition | None,
                          reset_strategy: AutoOffsetResetStrategy | None) -> None:
        next_state = self._fetch_state.transition_to(new_state)
        if next_state is new_state:
            self._fetch_state = next_state
            self.position = position
            self._reset_strategy = reset_strategy
            if self.position is None and next_state.requires_position():
                raise IllegalStateError(
                    message=f"Transitioned subscription state to {next_state.name}, "
                            "but position is null")
            if not next_state.requires_position():
                self.position = None

    def reset(self, strategy: AutoOffsetResetStrategy) -> None:
        self._transition_state(_FetchStates.AWAIT_RESET, self.position, strategy)

    def seek_validated(self, position: FetchPosition) -> None:
        self._transition_state(_FetchStates.FETCHING, position, None)

    def set_position(self, position: FetchPosition) -> None:
        if not self.has_valid_position():
            raise IllegalStateError(
                message="Cannot set a new position without a valid current position")
        self.position = position

    def has_valid_position(self) -> bool:
        return self._fetch_state.has_valid_position()

    def awaiting_reset(self) -> bool:
        return self._fetch_state is _FetchStates.AWAIT_RESET

    def is_paused(self) -> bool:
        return self._paused

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def reset_strategy(self) -> AutoOffsetResetStrategy | None:
        return self._reset_strategy


class SubscriptionState:
    """Java's ``SubscriptionState``, the parts ``MockConsumer`` uses."""

    def __init__(self, default_reset_strategy: AutoOffsetResetStrategy) -> None:
        self._default_reset_strategy = default_reset_strategy
        self._subscription_type = _SubscriptionType.NONE
        self._subscribed_re2j_pattern: SubscriptionPattern | None = None
        self._subscription: set[str] = set()
        self._assignment: dict[TopicPartition, _TopicPartitionState] = {}
        self._rebalance_listener: ConsumerRebalanceListener | None = None
        self._assignment_id = 0

    def _set_subscription_type(self, subscription_type: _SubscriptionType) -> None:
        if self._subscription_type is _SubscriptionType.NONE:
            self._subscription_type = subscription_type
        elif self._subscription_type is not subscription_type:
            raise IllegalStateError(message=_SUBSCRIPTION_EXCEPTION_MESSAGE)

    def subscribe(self, topics: set[str], listener: ConsumerRebalanceListener | None) -> bool:
        self._rebalance_listener = listener
        self._set_subscription_type(_SubscriptionType.AUTO_TOPICS)
        return self._change_subscription(topics)

    def subscribe_pattern(self, pattern: SubscriptionPattern,
                          listener: ConsumerRebalanceListener | None) -> None:
        """Java's ``subscribe(SubscriptionPattern, Optional)``."""
        self._rebalance_listener = listener
        self._set_subscription_type(_SubscriptionType.AUTO_PATTERN_RE2J)
        self._subscribed_re2j_pattern = pattern

    def _change_subscription(self, topics_to_subscribe: set[str]) -> bool:
        if self._subscription == topics_to_subscribe:
            return False
        self._subscription = topics_to_subscribe
        return True

    def assign_from_user(self, partitions: set[TopicPartition]) -> bool:
        self._set_subscription_type(_SubscriptionType.USER_ASSIGNED)
        if set(self._assignment) == partitions:
            return False
        self._assignment_id += 1
        manual_subscribed_topics: set[str] = set()
        partition_to_state: dict[TopicPartition, _TopicPartitionState] = {}
        for partition in partitions:
            state = self._assignment.get(partition)
            if state is None:
                state = _TopicPartitionState()
            partition_to_state[partition] = state
            manual_subscribed_topics.add(partition.topic())
        self._assignment = partition_to_state
        return self._change_subscription(manual_subscribed_topics)

    def assign_from_subscribed(self, assignments: list[TopicPartition]) -> None:
        if not self.has_auto_assigned_partitions():
            raise IllegalArgumentError(
                message="Attempt to dynamically assign partitions while manual assignment in use")
        assigned_partition_states: dict[TopicPartition, _TopicPartitionState] = {}
        for tp in assignments:
            state = self._assignment.get(tp)
            if state is None:
                state = _TopicPartitionState()
            assigned_partition_states[tp] = state
        self._assignment_id += 1
        self._assignment = assigned_partition_states

    def has_auto_assigned_partitions(self) -> bool:
        return self._subscription_type in (
            _SubscriptionType.AUTO_TOPICS, _SubscriptionType.AUTO_PATTERN,
            _SubscriptionType.AUTO_TOPICS_SHARE, _SubscriptionType.AUTO_PATTERN_RE2J)

    def unsubscribe(self) -> None:
        self._subscription = set()
        self._assignment.clear()
        self._subscription_type = _SubscriptionType.NONE
        self._assignment_id += 1

    def subscription(self) -> set[str]:
        if self.has_auto_assigned_partitions():
            return set(self._subscription)
        return set()

    def _assigned_state(self, tp: TopicPartition) -> _TopicPartitionState:
        state = self._assignment.get(tp)
        if state is None:
            raise IllegalStateError(message=f"No current assignment for partition {tp}")
        return state

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self._assigned_state(tp).seek_validated(FetchPosition(offset))

    def assigned_partitions(self) -> set[TopicPartition]:
        return set(self._assignment)

    def set_position(self, tp: TopicPartition, position: FetchPosition) -> None:
        """Java's ``position(TopicPartition, FetchPosition)``."""
        self._assigned_state(tp).set_position(position)

    def position(self, tp: TopicPartition) -> FetchPosition | None:
        return self._assigned_state(tp).position

    def all_consumed(self) -> dict[TopicPartition, OffsetAndMetadata]:
        all_consumed: dict[TopicPartition, OffsetAndMetadata] = {}
        for tp, state in self._assignment.items():
            if state.has_valid_position() and state.position is not None:
                all_consumed[tp] = OffsetAndMetadata(
                    offset=state.position.offset, leader_epoch=state.position.offset_epoch,
                    metadata="")
        return all_consumed

    def request_offset_reset(self, partition: TopicPartition,
                             strategy: AutoOffsetResetStrategy | None = None) -> None:
        """Java's ``requestOffsetReset(TopicPartition[, AutoOffsetResetStrategy])``."""
        self._assigned_state(partition).reset(
            self._default_reset_strategy if strategy is None else strategy)

    def request_offset_reset_partitions(self, partitions: list[TopicPartition],
                                        strategy: AutoOffsetResetStrategy) -> None:
        """Java's ``requestOffsetReset(Collection, AutoOffsetResetStrategy)``."""
        for tp in partitions:
            self._assigned_state(tp).reset(strategy)

    def is_offset_reset_needed(self, partition: TopicPartition) -> bool:
        return self._assigned_state(partition).awaiting_reset()

    def reset_strategy(self, partition: TopicPartition) -> AutoOffsetResetStrategy | None:
        return self._assigned_state(partition).reset_strategy()

    def is_assigned(self, tp: TopicPartition) -> bool:
        return tp in self._assignment

    def is_paused(self, tp: TopicPartition) -> bool:
        state = self._assignment.get(tp)
        return state is not None and state.is_paused()

    def has_valid_position(self, tp: TopicPartition) -> bool:
        state = self._assignment.get(tp)
        return state is not None and state.has_valid_position()

    def pause(self, tp: TopicPartition) -> None:
        self._assigned_state(tp).pause()

    def resume(self, tp: TopicPartition) -> None:
        self._assigned_state(tp).resume()

    def rebalance_listener(self) -> ConsumerRebalanceListener | None:
        return self._rebalance_listener

    def release_rebalance_listener(self) -> None:
        """Drop the listener (``close()`` releases every callback the client
        holds, CLAUDE.md, Python Binding Conventions, Threads and callbacks)."""
        self._rebalance_listener = None
