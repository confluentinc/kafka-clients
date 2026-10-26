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

"""The admin client family's lifecycle and its first two RPCs, ``create_topics``
and ``list_topics``, without a broker: ``KafkaAdminClientTest.java`` (Apache Kafka
4.4.0) tests that need no scripted broker, in Java order, asserting Java's
messages, then the binding's own lifecycle, configuration, pump thread,
thread-safety and Ctrl+C / cancellation behaviour (CLAUDE.md, Python Binding
Conventions).

Every RPC returns its Result at once, as Java's; a test reads a future with
``get()``, whose failure is ``ExecutionError`` with the core's error as its
``__cause__`` (``assert_future_throws``). Where Java drives a ``MockClient`` only
to make a call fail, the test uses an unreachable bootstrap address and a short
call timeout instead, and says so. The other ``KafkaAdminClientTest`` tests of
these two RPCs script broker responses with a ``MockClient``, which the binding
cannot inject; the core's translations of them are in
``rust/src/admin/kafka_admin_client.rs`` (``test_create_topics_*``,
``test_list_topics_*``). The RPC groups add their own tests in their own files.
"""

from __future__ import annotations

import asyncio
import os
import signal
import threading
import time
from types import FrameType
from typing import Any

import pytest

import _confluentkafka as _lib  # type: ignore[import-not-found]
from confluent_kafka import ExecutionError, IllegalArgumentError, IllegalStateError
from confluent_kafka._errors import from_borrowed_ffi_error
from confluent_kafka.admin import (
    Admin,
    AsyncAdmin,
    AsyncKafkaAdminClient,
    CreateTopicsOptions,
    KafkaAdminClient,
    ListTopicsOptions,
    NewTopic,
)
from confluent_kafka.common import AsyncKafkaFuture, KafkaError, KafkaFuture
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.errors import (
    InvalidTopicError,
    TopicAuthorizationError,
    UnknownServerError,
)
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError

from .admin_futures import (
    assert_future_throws,
    async_assert_future_throws,
    async_value,
    failure,
    value,
)

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CLOSED = "Cannot accept new calls when AdminClient is closing."
CREATE_TIMED_OUT = "Timed out waiting for a node assignment. Call: createTopics"
LIST_TIMED_OUT = "Timed out waiting for a node assignment. Call: listTopics"
# A call close() aborts: Java's AdminClientRunnable TimeoutException.
LIST_ABORTED = "The AdminClient thread has exited. Call: listTopics"
PUMP_THREAD = "kafka-admin-client-thread"
SHORT = 300


def _topic(name: str = "myTopic") -> NewTopic:
    return NewTopic(name=name, replicas_assignments={0: [0, 1, 2]})


def _admin(**extra: Any) -> Admin:
    return Admin.create(configs={**UNREACHABLE, **extra})


# ---- KafkaAdminClientTest ---------------------------------------------------


def test_default_api_timeout_and_request_timeout_conflicts() -> None:
    """``testDefaultApiTimeoutAndRequestTimeoutConflicts``: Java's
    ``createInternal`` wraps the ``ConfigException`` in a ``KafkaException``."""
    with pytest.raises(KafkaError) as info:
        _admin(**{"default.api.timeout.ms": "500"})
    assert isinstance(info.value.__cause__, ConfigError)


def test_close_admin_client() -> None:
    """``testCloseAdminClient``: a client closes at the end of its ``with``."""
    with _admin() as admin:
        assert isinstance(admin, KafkaAdminClient)
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_topics()


def test_close_admin_client_in_callback() -> None:
    """``testCloseAdminClientInCallback``: ``close()`` from a callback of a call's
    future returns: the callback runs on the client's pump thread, Java's admin
    thread, and ``close()`` there does not wait for its own thread, as Java's
    ``close`` does not join it; the client is freed once the close completed."""
    admin = _admin()
    result = admin.list_topics(options=ListTopicsOptions(timeout_ms=1000))
    called = threading.Event()
    threads: list[str] = []

    def on_done(_value: Any, _error: BaseException | None) -> None:
        threads.append(threading.current_thread().name)
        admin.close()
        called.set()

    result.listings().when_complete(action=on_done)
    assert called.wait(timeout=10)
    assert threads == [PUMP_THREAD]
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_topics()


def test_admin_client_failure_when_closed() -> None:
    """``testAdminClientFailureWhenClosed``: Java fails the call's future with
    ``IllegalStateException``; here the call raises it *(deviation: the native
    handle is gone after ``close()``)*."""
    admin = _admin()
    admin.close()
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.create_topics(new_topics=[_topic()],
                            options=CreateTopicsOptions(timeout_ms=10000))


def test_timeout_without_metadata() -> None:
    """``testTimeoutWithoutMetadata``: Java's ``MockClient`` answers no metadata;
    here the bootstrap address is unreachable."""
    with _admin(**{"request.timeout.ms": "10"}) as admin:
        future = admin.create_topics(new_topics=[_topic()],
                                     options=CreateTopicsOptions(timeout_ms=1000)).all()
        cause = assert_future_throws(future, KafkaTimeoutError)
        assert str(cause) == CREATE_TIMED_OUT


def test_invalid_topic_names() -> None:
    """``testInvalidTopicNames``, its ``createTopics`` part: an empty name fails its
    future with ``InvalidTopicException`` and sends nothing (the delete and describe
    parts belong to those RPCs; Java's ``null`` name cannot be built, as
    ``NewTopic(name=None)`` is not a Java-typed call here)."""
    with _admin() as admin:
        futures = admin.create_topics(
            new_topics=[NewTopic(name="", num_partitions=1, replication_factor=1)],
            options=CreateTopicsOptions(timeout_ms=SHORT)).values()
        assert_future_throws(futures[""], InvalidTopicError)


# ---- construction and configuration -----------------------------------------


def test_base_classes_cannot_be_built() -> None:
    with pytest.raises(TypeError) as info:
        Admin()
    assert str(info.value) == (
        "Admin is a non-instantiable base; use Admin.create or MockAdminClient")
    with pytest.raises(TypeError) as info:
        KafkaAdminClient()
    assert str(info.value) == (
        "KafkaAdminClient cannot be constructed directly; use Admin.create(...)")
    with pytest.raises(TypeError) as info:
        AsyncAdmin()
    assert str(info.value) == (
        "AsyncAdmin is a non-instantiable base; use AsyncAdmin.create or AsyncMockAdminClient")
    with pytest.raises(TypeError) as info:
        AsyncKafkaAdminClient()
    assert str(info.value) == (
        "AsyncKafkaAdminClient cannot be constructed directly; use AsyncAdmin.create(...)")


def test_create_returns_kafka_admin_client() -> None:
    admin = Admin.create(configs=UNREACHABLE)
    try:
        assert type(admin) is KafkaAdminClient
        assert isinstance(admin, Admin)
    finally:
        admin.close()


def test_positional_calls_raise_type_error() -> None:
    with pytest.raises(TypeError):
        Admin.create(UNREACHABLE)  # type: ignore[call-arg]
    with _admin() as admin:
        with pytest.raises(TypeError):
            admin.create_topics([_topic()])  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            admin.list_topics(ListTopicsOptions())  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            admin.close(1.0)  # type: ignore[call-arg]


def test_config_key_must_be_a_string() -> None:
    with pytest.raises(ConfigError, match="Key must be a string."):
        Admin.create(configs={**UNREACHABLE, 1: "x"})  # type: ignore[dict-item]


def test_config_values_checked_by_admin_client_config() -> None:
    """``AdminClientConfig``'s ``ConfigDef`` types: a wrong Python type for an
    ``INT`` raises Java's message; a ``LIST`` may be given as a list;
    ``bootstrap.controllers`` is an admin key of Java's ``ConfigDef``, so its type is
    checked although the core leaves it undefined; a ``None`` value is not set."""
    with pytest.raises(ConfigError) as info:
        _admin(**{"request.timeout.ms": 1.5})
    assert str(info.value) == (
        "Invalid value 1.5 for configuration request.timeout.ms: "
        "Expected value to be a 32-bit integer, but it was a builtins.float")
    with pytest.raises(ConfigError) as info:
        Admin.create(configs={"bootstrap.servers": "localhost:1",
                              "retries": True})
    assert "retries" in str(info.value)
    Admin.create(configs={"bootstrap.servers": ["localhost:1", "localhost:2"],
                          "client.id": None}).close()
    with pytest.raises(ConfigError) as info:
        Admin.create(configs={"bootstrap.controllers": 5})
    assert "bootstrap.controllers" in str(info.value)


def test_config_without_bootstrap() -> None:
    """The core fails the client; ``Admin_AdminClient_new`` hands its error back
    as an owned handle, raised as its class with its cause (``from_ffi_error``).
    ``bootstrap.controllers`` is not defined by the core, so an empty
    ``bootstrap.servers`` fails in ``ClientUtils.parseAndValidateAddresses``
    *(deviation: Java's ``determineBootstrapType`` fails first with "You must set
    either bootstrap.servers or bootstrap.controllers")*."""
    with pytest.raises(KafkaError) as info:
        Admin.create(configs={})
    assert str(info.value) == "Failed to create new KafkaAdminClient"
    assert type(info.value.__cause__) is ConfigError
    assert str(info.value.__cause__) == "No resolvable bootstrap urls given in bootstrap.servers"


# ---- close ------------------------------------------------------------------


def test_close_twice_and_negative_timeout() -> None:
    admin = _admin()
    with pytest.raises(IllegalArgumentError, match="The timeout cannot be negative."):
        admin.close(timeout=-1)
    admin.close(timeout=0.5)
    admin.close()
    # A second close() returns before checking its timeout (deviation).
    admin.close(timeout=-1)
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.create_topics(new_topics=[_topic()])


def test_close_aborts_calls_in_flight() -> None:
    """``close(timeout=0)`` aborts the call in flight (Java: with a
    ``TimeoutException`` naming the call): its future fails, and stays readable
    after the client is freed (the future owns its handle)."""
    admin = _admin()
    result = admin.list_topics(options=ListTopicsOptions(timeout_ms=20000))
    started = time.monotonic()
    admin.close(timeout=0)
    assert time.monotonic() - started < 10
    assert str(assert_future_throws(result.names(), KafkaTimeoutError)) == LIST_ABORTED
    assert result.names().is_done()


def test_close_waits_for_calls_in_flight() -> None:
    """``close()`` lets the calls in flight end first (Java's grace period without
    limit): their futures are done when it returns."""
    admin = _admin()
    result = admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT))
    names = result.names()
    admin.close()
    assert names.is_done()
    assert str(assert_future_throws(names, KafkaTimeoutError)) == LIST_TIMED_OUT


def test_close_fires_pending_deliveries() -> None:
    """A future still waiting for its delivery when the client is freed gets it
    from ``Admin_destroy`` (``kafka_common_KafkaFuture`` module docs): a pending
    ``get()`` ends."""
    admin = _admin()
    names = admin.list_topics(options=ListTopicsOptions(timeout_ms=20000)).names()
    seen: list[BaseException | None] = []
    waiter = threading.Thread(target=lambda: seen.append(failure(names)))
    waiter.start()
    time.sleep(0.1)
    admin.close(timeout=0)
    waiter.join(10)
    assert not waiter.is_alive()
    assert len(seen) == 1
    assert type(seen[0]) is KafkaTimeoutError and str(seen[0]) == LIST_ABORTED


# ---- create_topics / list_topics against an unreachable broker ---------------


def test_create_topics_returns_at_once_and_fails_later() -> None:
    """Java's RPC returns its Result at once and completes each future later."""
    with _admin() as admin:
        started = time.monotonic()
        result = admin.create_topics(new_topics=[_topic("a"), _topic("b")],
                                     options=CreateTopicsOptions(timeout_ms=2000))
        assert time.monotonic() - started < 1
        values = result.values()
        assert set(values) == {"a", "b"}
        assert all(isinstance(f, KafkaFuture) for f in values.values())
        assert not values["a"].is_done()
        for future in values.values():
            assert str(assert_future_throws(future, KafkaTimeoutError)) == CREATE_TIMED_OUT
            assert future.is_done() and future.is_completed_exceptionally()


def test_create_topics_refinements() -> None:
    """``config`` / ``topic_id`` / ``num_partitions`` / ``replication_factor`` are
    the core's per-topic futures: they fail as the topic does, and, for a topic not
    in the request, with the core's ``IllegalArgumentError`` at ``get()`` (no
    Python-side check)."""
    with _admin() as admin:
        result = admin.create_topics(new_topics=[_topic("a")],
                                     options=CreateTopicsOptions(timeout_ms=SHORT))
        for accessor in (result.config, result.topic_id, result.num_partitions,
                         result.replication_factor):
            future = accessor(topic="a")
            assert isinstance(future, KafkaFuture)
            assert str(assert_future_throws(future, KafkaTimeoutError)) == CREATE_TIMED_OUT
            unknown = assert_future_throws(accessor(topic="other"), IllegalArgumentError)
            assert str(unknown) == "Topic other was not part of the createTopics request"


def test_create_topics_refinement_follows_its_topics_cancellation() -> None:
    """A refinement is linked as a dependant of its topic's future (Futures):
    cancelling the stored future cancels it, never the reverse."""
    with _admin() as admin:
        result = admin.create_topics(new_topics=[_topic("a")],
                                     options=CreateTopicsOptions(timeout_ms=20000))
        config = result.config(topic="a")
        assert config.cancel(may_interrupt_if_running=True)
        assert not result._futures["a"].is_cancelled()
        topic_id = result.topic_id(topic="a")
        assert result._futures["a"].cancel(may_interrupt_if_running=True)
        assert topic_id.is_cancelled()


def test_create_topics_default_options_and_empty_batch() -> None:
    with _admin(**{"default.api.timeout.ms": str(SHORT), "request.timeout.ms": "100"}) as admin:
        result = admin.create_topics(new_topics=[])
        assert result.values() == {}
        assert value(result.all()) is None
        future = admin.create_topics(new_topics=[_topic()]).all()
        assert str(assert_future_throws(future, KafkaTimeoutError)) == CREATE_TIMED_OUT


def test_list_topics_future_fails_with_the_call_timeout() -> None:
    with _admin() as admin:
        result = admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT,
                                                             list_internal=True))
        for future in (result.names(), result.listings(), result.names_to_listings()):
            assert str(assert_future_throws(future, KafkaTimeoutError)) == LIST_TIMED_OUT


def test_negative_call_timeout_times_out_at_once() -> None:
    """A given negative ``timeout_ms`` is Java's ``Math.max(0, optionTimeoutMs)``
    (``calcDeadlineMs``): the call times out at once, not after
    ``default.api.timeout.ms``."""
    with _admin(**{"default.api.timeout.ms": "20000", "request.timeout.ms": "100"}) as admin:
        result = admin.list_topics(options=ListTopicsOptions(timeout_ms=-5))
        started = time.monotonic()
        assert str(assert_future_throws(result.names(), KafkaTimeoutError)) == LIST_TIMED_OUT
        assert time.monotonic() - started < 5


def test_from_borrowed_ffi_error_builds_class_payload_and_cause() -> None:
    cause_raw = (UnknownServerError._ffi_id, "inner", 0, 0, None, 0, None)
    raw = (TopicAuthorizationError._ffi_id, "Not authorized to access topics: [t]", 0, 0,
           {"unauthorized_topics": ["t"]}, 1, cause_raw)
    error = from_borrowed_ffi_error(raw)
    assert type(error) is TopicAuthorizationError
    assert error.unauthorized_topics() == {"t"}
    assert str(error) == "Not authorized to access topics: [t]"
    # A bare KafkaException shares UnknownServerException's id; it is no ApiException.
    assert type(error.__cause__) is KafkaError
    assert str(error.__cause__) == "inner"


# ---- the extension's admin layer ----------------------------------------------


def test_options_reach_the_extension_as_the_setters_that_ran() -> None:
    """The Options object records each setter that ran (the latest last), which
    the extension runs on the C options (CLAUDE.md conversion table, "an Options
    object"); a setter the extension does not know is a binding error."""
    options = CreateTopicsOptions(validate_only=True)
    options.timeout_ms(timeout_ms=5)
    options.validate_only(validate_only=False)
    assert options._setters_ran() == [("timeout_ms", 5), ("validate_only", False)]
    assert CreateTopicsOptions()._setters_ran() == []
    with _admin() as admin:
        with pytest.raises(RuntimeError) as info:
            admin._call(_lib.Admin_list_topics, [("bogus", 1)])
        assert str(info.value) == "ListTopicsOptions has no setter bogus in the extension"
        with pytest.raises(OverflowError):
            admin.list_topics(options=ListTopicsOptions(timeout_ms=1 << 31))


def test_result_accessors_of_the_extension() -> None:
    """The extension's ``AdminResult`` hands out the C result's futures by
    accessor name: ``futures`` for a map, ``future`` for one; a name it does not
    know, or the wrong kind, raises."""
    with _admin() as admin:
        native = admin._call(_lib.Admin_create_topics, [("t", 1, 1, None, None)],
                             [("timeout_ms", SHORT)])
        assert type(native).__name__ == "AdminResult"
        assert set(native.futures("values")) == {"t"}
        assert type(native.future("all")).__name__ == "KafkaFuture"
        with pytest.raises(AttributeError) as info:
            native.future("nope")
        assert str(info.value) == "CreateTopicsResult has no accessor nope in the extension"
        with pytest.raises(TypeError):
            native.future("values")
        with pytest.raises(TypeError):
            native.futures("all")


# ---- the pump thread ----------------------------------------------------------


def test_dependants_run_on_the_pump_thread() -> None:
    """The sync client's one daemon pump thread runs its queued callbacks, so a
    ``then_apply`` function or a ``when_complete`` action of a pending future runs
    there (Threads and callbacks); reading a sibling future that is done is safe
    there."""
    with _admin() as admin:
        result = admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT))
        names = result.names()
        seen: dict[str, Any] = {}

        def action(_value: Any, error: BaseException | None) -> None:
            seen["thread"] = threading.current_thread()
            seen["error"] = error
            seen["sibling"] = failure(result.names_to_listings(), timeout=1)

        dependant = result.names_to_listings().when_complete(action=action)
        assert str(assert_future_throws(dependant, KafkaTimeoutError)) == LIST_TIMED_OUT
        assert str(assert_future_throws(names, KafkaTimeoutError)) == LIST_TIMED_OUT
        assert seen["thread"].name == PUMP_THREAD and seen["thread"].daemon
        for error in (seen["error"], seen["sibling"]):
            assert type(error) is KafkaTimeoutError and str(error) == LIST_TIMED_OUT


def test_one_pump_thread_per_client() -> None:
    before = {t for t in threading.enumerate() if t.name == PUMP_THREAD}
    first, second = _admin(), _admin()
    try:
        pumps = {t for t in threading.enumerate() if t.name == PUMP_THREAD} - before
        assert len(pumps) == 2
    finally:
        first.close()
        second.close()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not ({t for t in threading.enumerate() if t.name == PUMP_THREAD} - before):
            break
        time.sleep(0.01)
    assert not ({t for t in threading.enumerate() if t.name == PUMP_THREAD} - before)


# ---- thread-safety and Ctrl+C -----------------------------------------------


def test_calls_from_many_threads() -> None:
    """``KafkaAdminClient`` is thread-safe: calls from several threads at once, then
    a ``close()`` from another."""
    admin = _admin()
    errors: list[BaseException] = []

    def call(i: int) -> None:
        try:
            if i % 2:
                future = admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT)).names()
                assert str(assert_future_throws(future, KafkaTimeoutError)) == LIST_TIMED_OUT
            else:
                future = admin.create_topics(new_topics=[_topic(f"t{i}")],
                                             options=CreateTopicsOptions(timeout_ms=SHORT)).all()
                assert str(assert_future_throws(future, KafkaTimeoutError)) == CREATE_TIMED_OUT
        except BaseException as e:  # noqa: BLE001 - collected for the assertion
            errors.append(e)

    threads = [threading.Thread(target=call, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(20)
    closer = threading.Thread(target=admin.close)
    closer.start()
    closer.join(20)
    assert not closer.is_alive()
    assert errors == []


def _with_sigint(after: float, run: Any) -> float:
    """Run ``run()`` with a SIGINT raised as ``KeyboardInterrupt`` after ``after``
    seconds; how long until it raised."""
    armed = True

    def on_sigint(signum: int, frame: FrameType | None) -> None:
        if armed:
            raise KeyboardInterrupt

    previous = signal.signal(signal.SIGINT, on_sigint)
    timer = threading.Timer(after, lambda: os.kill(os.getpid(), signal.SIGINT))
    started = time.monotonic()
    try:
        timer.start()
        with pytest.raises(KeyboardInterrupt):
            run()
        return time.monotonic() - started
    finally:
        timer.cancel()
        timer.join()
        armed = False
        time.sleep(0.05)  # a signal still pending runs the disarmed handler
        signal.signal(signal.SIGINT, previous)


def test_ctrl_c_ends_a_get_at_once() -> None:
    """Ctrl+C during ``get()`` ends the wait at once, as Java's ``Future.get()``
    throws ``InterruptedException``; the call goes on in the core."""
    with _admin() as admin:
        names = admin.list_topics(options=ListTopicsOptions(timeout_ms=20000)).names()
        assert _with_sigint(0.2, names.get) < 5
        assert not names.is_done()
        admin.close(timeout=0)
        assert str(assert_future_throws(names, KafkaTimeoutError)) == LIST_ABORTED


def test_ctrl_c_during_close_re_raises_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ctrl+C during ``close()`` re-raises at once; the close goes on in the core
    and the client is freed when it ends."""
    destroyed = threading.Event()
    destroy = _lib.Admin_destroy

    def counted_destroy(handle: int) -> None:
        destroy(handle)
        destroyed.set()

    monkeypatch.setattr(_lib, "Admin_destroy", counted_destroy)
    admin = _admin()
    names = admin.list_topics(options=ListTopicsOptions(timeout_ms=1500)).names()
    assert _with_sigint(0.2, admin.close) < 1.2
    assert not destroyed.is_set()
    assert destroyed.wait(10)
    assert names.is_done()
    assert str(assert_future_throws(names, KafkaTimeoutError)) == LIST_TIMED_OUT


# ---- the async peers --------------------------------------------------------


async def test_async_create_and_close() -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    assert type(admin) is AsyncKafkaAdminClient
    async with admin:
        pass
    with pytest.raises(IllegalStateError, match=CLOSED):
        admin.list_topics()
    await admin.close()
    with pytest.raises(IllegalArgumentError, match="The timeout cannot be negative."):
        await AsyncAdmin.create(configs=UNREACHABLE).close(timeout=-1)


def test_async_create_outside_a_loop() -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    asyncio.run(admin.close())


def test_async_rpc_outside_a_loop_completes_on_a_helper_thread() -> None:
    """An RPC of an async client made before any loop is bound completes its
    futures through the off-loop pump, so a later ``await get()`` on a loop ends."""
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    names = admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT)).names()
    assert isinstance(names, AsyncKafkaFuture)

    async def main() -> None:
        cause = await async_assert_future_throws(names, KafkaTimeoutError)
        assert str(cause) == LIST_TIMED_OUT
        await admin.close()

    asyncio.run(main())


async def test_async_create_topics_and_list_topics() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        created = admin.create_topics(new_topics=[_topic()],
                                      options=CreateTopicsOptions(timeout_ms=SHORT))
        future = created.values()["myTopic"]
        assert isinstance(future, AsyncKafkaFuture)
        assert str(await async_assert_future_throws(future, KafkaTimeoutError)) == (
            CREATE_TIMED_OUT)
        assert str(await async_assert_future_throws(created.all(), KafkaTimeoutError)) == (
            CREATE_TIMED_OUT)
        unknown = await async_assert_future_throws(created.config(topic="x"),
                                                   IllegalArgumentError)
        assert str(unknown) == "Topic x was not part of the createTopics request"
        listed = admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT))
        assert str(await async_assert_future_throws(listed.listings(), KafkaTimeoutError)) == (
            LIST_TIMED_OUT)


async def test_async_callbacks_run_on_the_loop() -> None:
    """The async client runs its queued callbacks on its event loop."""
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        loop_thread = threading.current_thread()
        seen: list[threading.Thread] = []
        names = admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT)).names()
        dependant = names.when_complete(
            action=lambda v, e: seen.append(threading.current_thread()))
        await async_assert_future_throws(dependant, KafkaTimeoutError)
        assert seen == [loop_thread]


async def test_async_calls_at_once() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        results = [admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT))
                   for _ in range(4)]
        created = [admin.create_topics(new_topics=[_topic(f"t{i}")],
                                       options=CreateTopicsOptions(timeout_ms=SHORT))
                   for i in range(4)]
        causes = await asyncio.gather(
            *(async_assert_future_throws(r.names(), KafkaTimeoutError) for r in results),
            *(async_assert_future_throws(c.all(), KafkaTimeoutError) for c in created))
        assert len(causes) == 8


async def test_async_cancelling_the_waiting_task_leaves_the_future() -> None:
    """Cancelling the task awaiting ``get()`` raises ``CancelledError`` at once and
    does not cancel the future (Futures)."""
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    names = admin.list_topics(options=ListTopicsOptions(timeout_ms=20000)).names()
    task = asyncio.ensure_future(async_value(names))
    await asyncio.sleep(0.2)
    started = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert time.monotonic() - started < 2
    assert not names.is_cancelled()
    await admin.close(timeout=0)
    cause = await asyncio.wait_for(_cause_of(names), 10)
    assert type(cause) is KafkaTimeoutError and str(cause) == LIST_ABORTED


async def _cause_of(future: Any) -> BaseException | None:
    try:
        await future.get()
    except ExecutionError as error:
        return error.__cause__
    return None


async def test_async_client_is_bound_to_one_loop() -> None:
    """An asyncio client is bound to the loop of its construction; a call from
    another loop raises ``RuntimeError``."""
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    errors: list[BaseException] = []

    def other_loop() -> None:
        async def call() -> None:
            admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT))
        try:
            asyncio.run(call())
        except RuntimeError as e:
            errors.append(e)

    thread = threading.Thread(target=other_loop)
    thread.start()
    thread.join(10)
    assert len(errors) == 1
    assert str(errors[0]) == "AsyncKafkaAdminClient is bound to a different event loop"
    await admin.close()


def test_async_client_whose_loop_ended_can_still_be_closed() -> None:
    """``close()`` alone may run on another loop once the bound loop is closed
    (``LoopBinding.bind_for_close``), as Java's ``close()`` always works; a
    close from another loop while the bound loop is open is refused."""
    async def use() -> AsyncAdmin:
        return AsyncAdmin.create(configs=UNREACHABLE)

    loop = asyncio.new_event_loop()
    admin = loop.run_until_complete(use())
    with pytest.raises(RuntimeError) as e:
        asyncio.run(admin.close())
    assert str(e.value) == "AsyncKafkaAdminClient is bound to a different event loop"
    loop.close()
    with pytest.raises(RuntimeError) as e:
        asyncio.run(_list_topics_on_loop(admin))
    assert str(e.value) == "AsyncKafkaAdminClient is bound to a different event loop"
    asyncio.run(admin.close())
    with pytest.raises(IllegalStateError):
        asyncio.run(_list_topics_on_loop(admin))


async def _list_topics_on_loop(admin: AsyncAdmin) -> None:
    admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT))
