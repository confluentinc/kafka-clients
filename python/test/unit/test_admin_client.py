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
4.3.1) tests that need no scripted broker, in Java order, asserting Java's
messages, then the binding's own lifecycle, configuration, thread-safety and
Ctrl+C / cancellation behaviour (CLAUDE.md, Python Binding Conventions).

Where Java drives a ``MockClient`` only to make a call fail, the test uses an
unreachable bootstrap address and a short call timeout instead, and says so. The
other ``KafkaAdminClientTest`` tests of these two RPCs script broker responses
with a ``MockClient``, which the binding cannot inject; the core's translations
of them are in ``rust/src/admin/kafka_admin_client.rs`` (``test_create_topics_*``,
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
from confluent_kafka import IllegalArgumentError, IllegalStateError, NullPointerError
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
from confluent_kafka.common import KafkaError
from confluent_kafka.common.config import ConfigError
from confluent_kafka.common.errors import (
    InvalidTopicError,
    TopicAuthorizationError,
    UnknownServerError,
)
from confluent_kafka.common.errors import TimeoutError as KafkaTimeoutError

UNREACHABLE = {"bootstrap.servers": "localhost:1"}
CLOSED = "Cannot accept new calls when AdminClient is closing."
CREATE_TIMED_OUT = "Timed out waiting for a node assignment. Call: createTopics"
LIST_TIMED_OUT = "Timed out waiting for a node assignment. Call: listTopics"
SHORT = 300


def _topic(name: str = "myTopic") -> NewTopic:
    return NewTopic(name=name, replicas_assignments={0: [0, 1, 2]})


def _admin(**extra: Any) -> Admin:
    return Admin.create(configs={**UNREACHABLE, **extra})


# ---- KafkaAdminClientTest ---------------------------------------------------


@pytest.mark.xfail(strict=True, reason=(
    "core gap: the Rust KafkaAdminClient does not reject default.api.timeout.ms "
    "below request.timeout.ms (Java's configureDefaultApiTimeoutMs)"))
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
    future returns. Here the future is complete when the call returns, so the
    callback runs on the calling thread at once."""
    admin = _admin()
    result = admin.list_topics(options=ListTopicsOptions(timeout_ms=1000))
    called = threading.Semaphore(0)

    def on_done(_: Any) -> None:
        admin.close()
        called.release()

    result.listings().add_done_callback(on_done)
    assert called.acquire(timeout=10)


def test_admin_client_failure_when_closed() -> None:
    """``testAdminClientFailureWhenClosed``: Java fails the call's future with
    ``IllegalStateException``; here the call raises it, as every future of a call
    is complete when it returns."""
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
        with pytest.raises(KafkaTimeoutError, match=CREATE_TIMED_OUT):
            future.result()


def test_invalid_topic_names() -> None:
    """``testInvalidTopicNames``, its ``createTopics`` part: an empty name fails its
    future with ``InvalidTopicException`` and sends nothing (the delete and describe
    parts belong to those RPCs; Java's ``null`` name cannot be built, as
    ``NewTopic(name=None)`` is not a Java-typed call here)."""
    with _admin() as admin:
        futures = admin.create_topics(
            new_topics=[NewTopic(name="", num_partitions=1, replication_factor=1)],
            options=CreateTopicsOptions(timeout_ms=SHORT)).values()
        with pytest.raises(InvalidTopicError):
            futures[""].result()


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


def test_config_values_coerced_by_admin_client_config() -> None:
    """``AdminClientConfig``'s ``ConfigDef`` types: a bad ``INT`` raises Java's
    message; a ``LIST`` may be given as a list; ``bootstrap.controllers`` is an admin
    key; a ``None`` value is not set."""
    with pytest.raises(ConfigError) as info:
        _admin(**{"request.timeout.ms": "abc"})
    assert str(info.value) == (
        "Invalid value abc for configuration request.timeout.ms: Not a number of type INT")
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
    with pytest.raises(ConfigError) as info:
        Admin.create(configs={})
    assert str(info.value) == "You must set either bootstrap.servers or bootstrap.controllers"


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


def test_close_waits_for_calls_in_flight() -> None:
    """``close(timeout=0)`` aborts the call in flight (Java: with a
    ``TimeoutException``), which returns with its future failed; the handle is
    freed only after it has."""
    admin = _admin()
    outcome: dict[str, Any] = {}

    def call() -> None:
        outcome["result"] = admin.list_topics(options=ListTopicsOptions(timeout_ms=20000))

    thread = threading.Thread(target=call)
    thread.start()
    time.sleep(0.3)
    started = time.monotonic()
    admin.close(timeout=0)
    thread.join(10)
    assert not thread.is_alive()
    assert time.monotonic() - started < 10
    with pytest.raises(KafkaError):
        outcome["result"].names().result()


# ---- create_topics / list_topics against an unreachable broker ---------------


def test_create_topics_futures_fail_with_the_call_timeout() -> None:
    with _admin() as admin:
        result = admin.create_topics(new_topics=[_topic("a"), _topic("b")],
                                     options=CreateTopicsOptions(timeout_ms=SHORT))
        assert set(result.values()) == {"a", "b"}
        for future in result.values().values():
            assert future.done()
            with pytest.raises(KafkaTimeoutError, match=CREATE_TIMED_OUT):
                future.result()
        for accessor in (result.config, result.topic_id, result.num_partitions,
                         result.replication_factor):
            with pytest.raises(KafkaTimeoutError, match=CREATE_TIMED_OUT):
                accessor(topic="a").result()
        # Java's ``futures.get(topic).thenApply(...)`` on a topic not in the call.
        with pytest.raises(NullPointerError):
            result.config(topic="other")


def test_create_topics_default_options_and_empty_batch() -> None:
    with _admin(**{"default.api.timeout.ms": str(SHORT), "request.timeout.ms": "100"}) as admin:
        result = admin.create_topics(new_topics=[])
        assert result.values() == {}
        assert result.all().result() is None
        with pytest.raises(KafkaTimeoutError, match=CREATE_TIMED_OUT):
            admin.create_topics(new_topics=[_topic()]).all().result()


def test_list_topics_future_fails_with_the_call_timeout() -> None:
    with _admin() as admin:
        result = admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT,
                                                             list_internal=True))
        for future in (result.names(), result.listings(), result.names_to_listings()):
            assert future.done()
            with pytest.raises(KafkaTimeoutError, match=LIST_TIMED_OUT):
                future.result()


def test_per_key_error_crosses_in_the_borrowed_form() -> None:
    """A per-key error of an admin result reaches Python as id, message, payload
    and cause (``borrowed_error_to_py``), read before the result handle is freed."""
    admin = _admin()
    try:
        spec = [("t", 1, 1, None, None)]
        payload = admin._wait_for(
            lambda c, cb: _lib.Admin_create_topics_async(c, spec, SHORT, False, True, cb),
            lambda p: None)
        drained = _lib.CreateTopicsResult_drain(payload[0])
        raw_error, value = drained["t"]
        assert value is None
        assert len(raw_error) == 7
        ffi_id, message, _retriable, _fatal, error_payload, is_api_error, cause = raw_error
        assert ffi_id == KafkaTimeoutError._ffi_id
        assert message == "Timed out waiting for a node assignment. Call: createTopics"
        assert error_payload is None and is_api_error == 1 and cause is None
    finally:
        admin.close()


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


# ---- thread-safety and Ctrl+C -----------------------------------------------


def test_calls_from_many_threads() -> None:
    """``KafkaAdminClient`` is thread-safe: calls from several threads at once, then
    a ``close()`` from another."""
    admin = _admin()
    errors: list[BaseException] = []

    def call(i: int) -> None:
        try:
            if i % 2:
                admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT)).names().exception()
            else:
                admin.create_topics(new_topics=[_topic(f"t{i}")],
                                    options=CreateTopicsOptions(timeout_ms=SHORT)).all().exception()
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


def _count_frees(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Counters of the late ``list_topics`` result's free and of the handle's
    ``Admin_destroy``."""
    from confluent_kafka.admin import _conversions

    counts = {"free": 0, "destroy": 0}
    free, destroy = _conversions.free_list_topics, _lib.Admin_destroy

    def counted_free(payload: Any) -> None:
        counts["free"] += 1
        free(payload)

    def counted_destroy(handle: int) -> None:
        counts["destroy"] += 1
        destroy(handle)

    monkeypatch.setattr(_conversions, "free_list_topics", counted_free)
    monkeypatch.setattr(_lib, "Admin_destroy", counted_destroy)
    return counts


def test_ctrl_c_re_raises_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ctrl+C during a call re-raises at once; the call goes on in the core, its
    late result is freed, and ``close()`` still frees the client."""
    counts = _count_frees(monkeypatch)
    admin = _admin()
    armed = True

    def on_sigint(signum: int, frame: FrameType | None) -> None:
        if armed:
            raise KeyboardInterrupt

    previous = signal.signal(signal.SIGINT, on_sigint)
    timer = threading.Timer(0.2, lambda: os.kill(os.getpid(), signal.SIGINT))
    started = time.monotonic()
    try:
        timer.start()
        with pytest.raises(KeyboardInterrupt):
            admin.list_topics(options=ListTopicsOptions(timeout_ms=20000))
    finally:
        timer.cancel()
        timer.join()
        armed = False
        time.sleep(0.05)  # a signal still pending runs the disarmed handler
        signal.signal(signal.SIGINT, previous)
    assert time.monotonic() - started < 5
    assert counts == {"free": 0, "destroy": 0}
    admin.close(timeout=0)
    # close() returns once the aborted call has called back: its result was freed.
    assert counts == {"free": 1, "destroy": 1}


# ---- the async peers --------------------------------------------------------


async def test_async_create_and_close() -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    assert type(admin) is AsyncKafkaAdminClient
    async with admin:
        pass
    with pytest.raises(IllegalStateError, match=CLOSED):
        await admin.list_topics()
    await admin.close()
    with pytest.raises(IllegalArgumentError, match="The timeout cannot be negative."):
        await AsyncAdmin.create(configs=UNREACHABLE).close(timeout=-1)


def test_async_create_outside_a_loop() -> None:
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    asyncio.run(admin.close())


async def test_async_create_topics_and_list_topics() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        created = await admin.create_topics(new_topics=[_topic()],
                                            options=CreateTopicsOptions(timeout_ms=SHORT))
        future = created.values()["myTopic"]
        assert isinstance(future, asyncio.Future) and future.done()
        assert isinstance(future.exception(), KafkaTimeoutError)
        with pytest.raises(KafkaTimeoutError, match=CREATE_TIMED_OUT):
            await created.all()
        listed = await admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT))
        with pytest.raises(KafkaTimeoutError, match=LIST_TIMED_OUT):
            await listed.listings()


async def test_async_calls_at_once() -> None:
    async with AsyncAdmin.create(configs=UNREACHABLE) as admin:
        results = await asyncio.gather(
            *(admin.list_topics(options=ListTopicsOptions(timeout_ms=SHORT)) for _ in range(4)),
            *(admin.create_topics(new_topics=[_topic(f"t{i}")],
                                  options=CreateTopicsOptions(timeout_ms=SHORT))
              for i in range(4)))
        assert len(results) == 8


async def test_async_cancellation_re_raises_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cancelling the awaiting task raises ``CancelledError`` at once; the late
    result is freed on the loop, and ``close()`` frees the client."""
    counts = _count_frees(monkeypatch)
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    task = asyncio.ensure_future(admin.list_topics(options=ListTopicsOptions(timeout_ms=20000)))
    await asyncio.sleep(0.2)
    started = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert time.monotonic() - started < 2
    await admin.close(timeout=0)
    for _ in range(100):  # the late result's delivery runs on this loop
        if counts["free"]:
            break
        await asyncio.sleep(0.01)
    assert counts == {"free": 1, "destroy": 1}


async def test_async_payload_delivered_just_before_a_cancellation_is_freed() -> None:
    """The completion fills the awaited future, then the task is cancelled before it
    resumes (an ``asyncio.wait_for`` timeout in the same loop iteration): the
    awaiting coroutine frees the payload."""
    admin = AsyncAdmin.create(configs=UNREACHABLE)
    try:
        freed: list[tuple[Any, ...]] = []
        captured: dict[str, Any] = {}
        task = asyncio.ensure_future(admin._await_for(
            lambda c_admin, cb: captured.__setitem__("cb", cb), freed.append))
        await asyncio.sleep(0)  # the task now awaits its future
        captured["cb"](0, 0)  # the completion: its delivery is scheduled on the loop
        await asyncio.sleep(0)  # the delivery fills the future; the task is not run yet
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert freed == [(0, 0)]
    finally:
        await admin.close(timeout=0)


def test_negative_call_timeout_times_out_at_once() -> None:
    """A given negative ``timeout_ms`` is Java's ``Math.max(0, optionTimeoutMs)``
    (``calcDeadlineMs``): the call times out at once, not after
    ``default.api.timeout.ms``."""
    from confluent_kafka.admin import _conversions

    assert _conversions.timeout_ms(ListTopicsOptions(timeout_ms=-5)) == 0
    assert _conversions.timeout_ms(ListTopicsOptions()) == -1
    with _admin(**{"default.api.timeout.ms": "20000"}) as admin:
        started = time.monotonic()
        result = admin.list_topics(options=ListTopicsOptions(timeout_ms=-5))
        assert time.monotonic() - started < 5
        with pytest.raises(KafkaTimeoutError, match=LIST_TIMED_OUT):
            result.names().result()


def test_void_future() -> None:
    """``_conversions.void_future``: a ``void`` call's ``(error_int,)`` payload as a
    completed ``KafkaFuture<Void>``, or one failed with the typed error."""
    from confluent_kafka.admin import _conversions
    from confluent_kafka.admin._futures import Sync

    assert _conversions.void_future((0,), Sync).result() is None
    error = _lib.KafkaError_new(KafkaTimeoutError._ffi_id, "late")
    failed = _conversions.void_future((error,), Sync)
    with pytest.raises(KafkaTimeoutError, match="late"):
        failed.result()
