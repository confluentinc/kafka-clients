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

"""The generated admin ``*Result`` classes (``cargo xtask generate-admin-results``).

For each of the 24 classes the generator writes beyond the reference set: ``_new``
for both client kinds, every method's futures (success, a per-key error, ``all()``),
and the ``TypeError`` of ``__init__`` (or, for a public Java constructor, the kind
read from the futures given). None of these classes has a Java ``*ResultTest``; the
Java tests of the reference and hand-written classes are translated in
``test_admin_reference_set.py`` and ``test_admin_results_hand.py``.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import importlib
import re
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from confluent_kafka.admin import (
    AbortTransactionResult,
    AlterClientQuotasResult,
    AlterPartitionReassignmentsResult,
    AlterReplicaLogDirsResult,
    CreateAclsResult,
    CreateDelegationTokenResult,
    CreatePartitionsResult,
    DeleteRecordsResult,
    DescribeAclsResult,
    DescribeClassicGroupsResult,
    DescribeClientQuotasResult,
    DescribeConfigsResult,
    DescribeConsumerGroupsResult,
    DescribeDelegationTokenResult,
    DescribeFeaturesResult,
    DescribeLogDirsResult,
    DescribeProducersResult,
    ElectLeadersResult,
    ExpireDelegationTokenResult,
    ListConfigResourcesResult,
    ListPartitionReassignmentsResult,
    RenewDelegationTokenResult,
    TerminateTransactionResult,
    UpdateFeaturesResult,
)
from confluent_kafka.admin._futures import Async, Sync
from confluent_kafka.common import TopicPartition
from confluent_kafka.illegal_argument_error import IllegalArgumentError

KINDS = [Sync, Async]


def _run(kind: type[Sync] | type[Async], body: Callable[[], Awaitable[None]]) -> None:
    """Run ``body`` on an event loop (an async kind's futures need one; the sync
    kind's do not mind)."""
    asyncio.run(body())


def _pending(kind: type[Sync] | type[Async]) -> Any:
    if kind is Async:
        return asyncio.get_running_loop().create_future()
    return concurrent.futures.Future()


def _done(kind: type[Sync] | type[Async], value: Any) -> Any:
    future = _pending(kind)
    future.set_result(value)
    return future


def _failed(kind: type[Sync] | type[Async], error: BaseException) -> Any:
    future = _pending(kind)
    future.set_exception(error)
    return future


async def _get(future: Any) -> Any:
    if isinstance(future, asyncio.Future):
        return await asyncio.wait_for(future, 1)
    assert isinstance(future, concurrent.futures.Future)
    return future.result(timeout=1)


def _assert_kind(kind: type[Sync] | type[Async], future: Any) -> None:
    expected = asyncio.Future if kind is Async else concurrent.futures.Future
    assert isinstance(future, expected), type(future)


# -- __init__ ---------------------------------------------------------------

# (class, the admin method named by the TypeError)
NOT_PUBLIC = [
    (AbortTransactionResult, "abort_transaction"),
    (AlterPartitionReassignmentsResult, "alter_partition_reassignments"),
    (AlterReplicaLogDirsResult, "alter_replica_log_dirs"),
    (CreateAclsResult, "create_acls"),
    (CreateDelegationTokenResult, "create_delegation_token"),
    (CreatePartitionsResult, "create_partitions"),
    (DescribeAclsResult, "describe_acls"),
    (DescribeConfigsResult, "describe_configs"),
    (DescribeDelegationTokenResult, "describe_delegation_token"),
    (DescribeFeaturesResult, "describe_features"),
    (DescribeLogDirsResult, "describe_log_dirs"),
    (DescribeProducersResult, "describe_producers"),
    (ElectLeadersResult, "elect_leaders"),
    (ExpireDelegationTokenResult, "expire_delegation_token"),
    (ListConfigResourcesResult, "list_config_resources"),
    (ListPartitionReassignmentsResult, "list_partition_reassignments"),
    (RenewDelegationTokenResult, "renew_delegation_token"),
    (TerminateTransactionResult, "force_terminate_transaction"),
    (UpdateFeaturesResult, "update_features"),
]


@pytest.mark.parametrize("cls,method", NOT_PUBLIC, ids=[c.__name__ for c, _ in NOT_PUBLIC])
def test_init_raises_type_error(cls: type, method: str) -> None:
    message = (f"{cls.__name__} cannot be constructed directly; use the admin client's "
               f"{method}()")
    with pytest.raises(TypeError, match="^" + re.escape(message) + "$"):
        cls()
    with pytest.raises(TypeError, match="^" + re.escape(message) + "$"):
        cls(futures={})


# Java's public constructors: (class, keyword, a future map (True) or one future)
PUBLIC = [
    (AlterClientQuotasResult, "futures", True),
    (DeleteRecordsResult, "futures", True),
    (DescribeClassicGroupsResult, "futures", True),
    (DescribeConsumerGroupsResult, "futures", True),
    (DescribeClientQuotasResult, "entities", False),
]


@pytest.mark.parametrize("cls,keyword,is_map", PUBLIC, ids=[c.__name__ for c, _, _ in PUBLIC])
@pytest.mark.parametrize("kind", KINDS, ids=["sync", "async"])
def test_public_init_reads_the_kind(cls: type, keyword: str, is_map: bool,
                                    kind: type[Sync] | type[Async]) -> None:
    async def body() -> None:
        future = _done(kind, None)
        result = cls(**{keyword: {"a": future} if is_map else future})
        assert result._kind is kind
        with pytest.raises(TypeError):
            cls({"a": future} if is_map else future)

    _run(kind, body)


@pytest.mark.parametrize("cls", [c for c, _, m in PUBLIC if m], ids=lambda c: c.__name__)
def test_public_init_empty_map_is_the_sync_kind(cls: type) -> None:
    assert cls(futures={})._kind is Sync


@pytest.mark.parametrize("cls", [c for c, _ in NOT_PUBLIC] + [c for c, _, _ in PUBLIC],
                         ids=lambda c: c.__name__)
def test_new_sets_the_kind(cls: type) -> None:
    import inspect

    parameters = inspect.signature(cls._new).parameters
    # A map keyword gets an empty map, a future keyword `None` (never read).
    given = {k: {} if "Mapping" in str(p.annotation) else None
             for k, p in parameters.items() if k != "kind"}
    for kind in KINDS:
        result = cls._new(**given, kind=kind)
        assert type(result) is cls
        assert result._kind is kind


def test_new_rejects_positional_arguments() -> None:
    with pytest.raises(TypeError):
        AbortTransactionResult._new({}, Sync)  # type: ignore[misc]


# -- maps of futures with all() ------------------------------------------------

# (class, the map getter or None, the constructor keyword)
STORED_MAPS = [
    (AbortTransactionResult, None, "futures"),
    (AlterClientQuotasResult, "values", "futures"),
    (AlterPartitionReassignmentsResult, "values", "futures"),
    (AlterReplicaLogDirsResult, "values", "futures"),
    (CreateAclsResult, "values", "futures"),
    (CreatePartitionsResult, "values", "values"),
    (DeleteRecordsResult, "low_watermarks", "futures"),
    (UpdateFeaturesResult, "values", "futures"),
]


@pytest.mark.parametrize("cls,getter,keyword", STORED_MAPS, ids=[c.__name__ for c, _, _ in STORED_MAPS])
@pytest.mark.parametrize("kind", KINDS, ids=["sync", "async"])
def test_map_of_futures_and_all(cls: type, getter: str | None, keyword: str,
                                kind: type[Sync] | type[Async]) -> None:
    async def body() -> None:
        a, b = _done(kind, None), _done(kind, None)
        given = {"a": a, "b": b}
        result = cls._new(**{keyword: given}, kind=kind)
        if getter is not None:
            values = getattr(result, getter)()
            assert values == given
            assert values["a"] is a
        everything = result.all()
        _assert_kind(kind, everything)
        assert await _get(everything) is None

        # A per-key error fails all(); the other key's future is unaffected.
        error = RuntimeError("boom")
        failing = cls._new(**{keyword: {"a": _done(kind, None), "b": _failed(kind, error)}},
                           kind=kind)
        with pytest.raises(RuntimeError, match="^boom$"):
            await _get(failing.all())
        if getter is not None:
            assert await _get(getattr(failing, getter)()["a"]) is None

        # No keys: all() is already complete.
        empty = cls._new(**{keyword: {}}, kind=kind)
        assert empty.all().done()
        assert await _get(empty.all()) is None

    _run(kind, body)


def test_all_waits_for_pending_futures() -> None:
    pending: concurrent.futures.Future[None] = concurrent.futures.Future()
    result = CreateAclsResult._new(futures={"a": pending}, kind=Sync)
    everything = result.all()
    assert not everything.done()
    pending.set_result(None)
    assert everything.result(timeout=1) is None


# -- single stored futures -------------------------------------------------------

# (class, getter, constructor keyword)
SINGLE = [
    (CreateDelegationTokenResult, "delegation_token", "delegation_token"),
    (DescribeAclsResult, "values", "future"),
    (DescribeClientQuotasResult, "entities", "entities"),
    (DescribeDelegationTokenResult, "delegation_tokens", "delegation_tokens"),
    (DescribeFeaturesResult, "feature_metadata", "future"),
    (ExpireDelegationTokenResult, "expiry_timestamp", "expiry_timestamp"),
    (ListPartitionReassignmentsResult, "reassignments", "reassignments"),
    (RenewDelegationTokenResult, "expiry_timestamp", "expiry_timestamp"),
    (TerminateTransactionResult, "result", "future"),
    (ElectLeadersResult, "partitions", "election_future"),
]


@pytest.mark.parametrize("cls,getter,keyword", SINGLE, ids=[c.__name__ for c, _, _ in SINGLE])
@pytest.mark.parametrize("kind", KINDS, ids=["sync", "async"])
def test_stored_future_is_returned_as_given(cls: type, getter: str, keyword: str,
                                            kind: type[Sync] | type[Async]) -> None:
    async def body() -> None:
        value = object()
        future = _done(kind, value)
        result = cls._new(**{keyword: future}, kind=kind)
        assert getattr(result, getter)() is future
        assert await _get(getattr(result, getter)()) is value
        failed = cls._new(**{keyword: _failed(kind, RuntimeError("boom"))}, kind=kind)
        with pytest.raises(RuntimeError, match="^boom$"):
            await _get(getattr(failed, getter)())

    _run(kind, body)


# -- copy getters and collected all() -------------------------------------------

# (class, the map getter, copies?, the all() method)
COLLECTED = [
    (DescribeClassicGroupsResult, "described_groups", True, "all"),
    (DescribeConsumerGroupsResult, "described_groups", True, "all"),
    (DescribeConfigsResult, "values", False, "all"),
    (DescribeLogDirsResult, "descriptions", False, "all_descriptions"),
]


@pytest.mark.parametrize("cls,getter,copies,all_name", COLLECTED, ids=[c[0].__name__ for c in COLLECTED])
@pytest.mark.parametrize("kind", KINDS, ids=["sync", "async"])
def test_collected_all(cls: type, getter: str, copies: bool, all_name: str,
                       kind: type[Sync] | type[Async]) -> None:
    async def body() -> None:
        given = {"a": _done(kind, "A"), "b": _done(kind, "B")}
        result = cls._new(futures=given, kind=kind)
        values = getattr(result, getter)()
        assert values == given
        # Java's `new HashMap<>(futures)`: each call a new dict; else the stored one.
        assert (values is not getattr(result, getter)()) is copies
        if copies:
            values.clear()
            assert getattr(result, getter)() == given
        collected = getattr(result, all_name)()
        _assert_kind(kind, collected)
        assert await _get(collected) == {"a": "A", "b": "B"}

        failing = cls._new(futures={"a": _done(kind, "A"), "b": _failed(kind, RuntimeError("boom"))},
                           kind=kind)
        with pytest.raises(RuntimeError, match="^boom$"):
            await _get(getattr(failing, all_name)())
        assert await _get(getattr(cls._new(futures={}, kind=kind), all_name)()) == {}

    _run(kind, body)


# -- DescribeProducersResult -------------------------------------------------------

@pytest.mark.parametrize("kind", KINDS, ids=["sync", "async"])
def test_describe_producers_partition_result_and_all(kind: type[Sync] | type[Async]) -> None:
    async def body() -> None:
        tp0 = TopicPartition(topic="t", partition=0)
        tp1 = TopicPartition(topic="t", partition=1)
        state = DescribeProducersResult.PartitionProducerState(active_producers=[])
        future = _done(kind, state)
        result = DescribeProducersResult._new(futures={tp0: future}, kind=kind)
        assert result.partition_result(partition=tp0) is future
        with pytest.raises(IllegalArgumentError) as e:
            result.partition_result(partition=tp1)
        assert str(e.value) == "Topic partition t-1 was not included in the request"
        assert await _get(result.all()) == {tp0: state}
        failing = DescribeProducersResult._new(
            futures={tp0: future, tp1: _failed(kind, RuntimeError("boom"))}, kind=kind)
        with pytest.raises(RuntimeError, match="^boom$"):
            await _get(failing.all())

    _run(kind, body)


def test_partition_producer_state() -> None:
    state = DescribeProducersResult.PartitionProducerState(active_producers=())
    assert state.active_producers() == []
    assert str(state) == "PartitionProducerState(activeProducers=[])"
    with pytest.raises(TypeError):
        DescribeProducersResult.PartitionProducerState([])  # type: ignore[misc]


# -- whenComplete-built futures ----------------------------------------------------

@pytest.mark.parametrize("kind", KINDS, ids=["sync", "async"])
def test_elect_leaders_all(kind: type[Sync] | type[Async]) -> None:
    async def body() -> None:
        tp0 = TopicPartition(topic="t", partition=0)
        tp1 = TopicPartition(topic="t", partition=1)
        ok = ElectLeadersResult._new(election_future=_done(kind, {tp0: None, tp1: None}),
                                     kind=kind)
        everything = ok.all()
        _assert_kind(kind, everything)
        assert await _get(everything) is None

        error = RuntimeError("not elected")
        one_failed = ElectLeadersResult._new(election_future=_done(kind, {tp0: None, tp1: error}),
                                             kind=kind)
        with pytest.raises(RuntimeError) as e:
            await _get(one_failed.all())
        assert e.value is error

        top = ElectLeadersResult._new(election_future=_failed(kind, RuntimeError("top")), kind=kind)
        with pytest.raises(RuntimeError, match="^top$"):
            await _get(top.all())

    _run(kind, body)


def test_elect_leaders_all_completes_later() -> None:
    source: concurrent.futures.Future[Any] = concurrent.futures.Future()
    everything = ElectLeadersResult._new(election_future=source, kind=Sync).all()
    assert not everything.done()
    source.set_result({})
    assert everything.result(timeout=1) is None


@pytest.mark.parametrize("kind", KINDS, ids=["sync", "async"])
def test_list_config_resources_all(kind: type[Sync] | type[Async]) -> None:
    async def body() -> None:
        resources = ["r"]
        source = _done(kind, resources)
        result = ListConfigResourcesResult._new(future=source, kind=kind)
        everything = result.all()
        _assert_kind(kind, everything)
        assert everything is not source
        assert await _get(everything) is resources

        failed = ListConfigResourcesResult._new(future=_failed(kind, RuntimeError("boom")), kind=kind)
        with pytest.raises(RuntimeError, match="^boom$"):
            await _get(failed.all())

        cancelled = _pending(kind)
        cancelled.cancel()
        relayed = ListConfigResourcesResult._new(future=cancelled, kind=kind).all()
        assert relayed.cancelled()

    _run(kind, body)


def test_every_generated_module_has_a_stub() -> None:
    names = {c.__name__ for c, _ in NOT_PUBLIC} | {c.__name__ for c, _, _ in PUBLIC}
    assert len(names) == 24
    for name in names:
        module = importlib.import_module(
            "confluent_kafka.admin." + re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower())
        assert module.__all__ == [name]
        assert module.__file__ is not None
        with open(module.__file__ + "i") as stub:
            assert f"class {name}(Generic[KindT]):" in stub.read()


# -- maps of derived futures (CreateTopicsResult.values()) ----------------------------

def test_values_unread_failed_topic_logs_nothing() -> None:
    """Each derived per-topic future of ``values()`` is marked retrieved, as the client
    marks its own per-key futures: an unread failed topic does not make asyncio log
    "Future exception was never retrieved" (Java logs nothing); a reader still gets the
    error."""
    import gc

    from confluent_kafka.admin._futures import _mark_retrieved
    from confluent_kafka.admin.create_topics_result import CreateTopicsResult

    reports: list[dict[str, Any]] = []

    async def body() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda loop, ctx: reports.append(ctx))
        failed = _failed(Async, RuntimeError("boom"))
        _mark_retrieved(failed)
        result = CreateTopicsResult._new(futures={"t": failed, "u": _done(Async, None)}, kind=Async)
        values = result.values()
        _assert_kind(Async, values["t"])
        with pytest.raises(RuntimeError, match="^boom$"):
            await _get(result.values()["t"])
        del values
        result.values()
        gc.collect()
        await asyncio.sleep(0)
        gc.collect()

    asyncio.run(body())
    gc.collect()
    assert [r for r in reports if "never retrieved" in str(r.get("message", ""))] == []
