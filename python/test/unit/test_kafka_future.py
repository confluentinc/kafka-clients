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

"""Java's ``KafkaFutureTest`` (25 tests) for both ``KafkaFuture`` and
``AsyncKafkaFuture``, plus the C-backed source, the keyword-only surface and the
``get`` timeout reading.

Java's ``KafkaFutureImpl`` is ``_KafkaFutureImpl``: a test builds one and the
public future of the kind over it (``_of_impl``), completes the former and
reads the latter. Every assertion on a failed future goes through ``get()`` and
its ``ExecutionError``, checking ``type(e.__cause__)`` exactly.

Java's ``CompletionException`` has no Python counterpart: Java's
``KafkaFutureImpl`` wraps and unwraps it only to undo ``CompletableFuture``'s
own unwrapping, so the "tricky" tests use a test-local ``CompletionError`` (an
error whose message is its cause's ``toString()``, as ``CompletionException
(Throwable)``) and check that it, too, comes back as the given error.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import math
import threading
import time
from collections.abc import Callable, Coroutine
from datetime import timedelta
from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]
import pytest

from confluent_kafka import ExecutionError
from confluent_kafka import TimeoutError as JavaTimeoutError
from confluent_kafka import _throwable
from confluent_kafka.common import AsyncKafkaFuture, KafkaFuture
from confluent_kafka import UnsupportedOperationError
from confluent_kafka.common.kafka_future import _KafkaFutureImpl

# KafkaFutureTest is @Timeout(120); every wait here is bounded far below that.
_WAIT = 300.0  # Java's get(5, TimeUnit.MINUTES)


class CompletionError(RuntimeError):
    """A stand-in for Java's ``java.util.concurrent.CompletionException``:
    ``CompletionException(Throwable cause)`` carries ``cause.toString()``."""

    def __init__(self, cause: BaseException) -> None:
        super().__init__(_throwable.to_string(cause))
        self.__cause__ = cause


# ---------------------------------------------------------------------------
# The two kinds
# ---------------------------------------------------------------------------


class Kind:
    """One future kind: how to wrap Python state and how to wait on it."""

    name: str
    cancelled: type[BaseException]

    def wrap(self, impl: _KafkaFutureImpl[Any]) -> Any:
        raise NotImplementedError

    def get(self, future: Any, timeout: float | None = None) -> Any:
        raise NotImplementedError

    def get_in_thread(self, future: Any) -> Any:
        """``future.get()`` from a thread of the test's own."""
        raise NotImplementedError

    def close(self) -> None:
        pass


class SyncKind(Kind):
    name = "sync"
    cancelled = concurrent.futures.CancelledError

    def wrap(self, impl: _KafkaFutureImpl[Any]) -> KafkaFuture[Any]:
        return KafkaFuture._of_impl(impl)

    def get(self, future: Any, timeout: float | None = None) -> Any:
        return future.get() if timeout is None else future.get(timeout=timeout)

    def get_in_thread(self, future: Any) -> Any:
        return future.get()

    @property
    def cls(self) -> Any:
        return KafkaFuture


class AsyncKind(Kind):
    """The async kind runs every ``get`` on one event loop for the test."""

    name = "async"
    cancelled = asyncio.CancelledError

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()

    def wrap(self, impl: _KafkaFutureImpl[Any]) -> AsyncKafkaFuture[Any]:
        return AsyncKafkaFuture._of_impl(impl)

    def run(self, coro: Coroutine[Any, Any, Any]) -> Any:
        return _outcome(self.loop.run_until_complete(_capture(coro)))

    def get(self, future: Any, timeout: float | None = None) -> Any:
        return self.run(future.get() if timeout is None else future.get(timeout=timeout))

    def get_in_thread(self, future: Any) -> Any:
        return _outcome(asyncio.run(_capture(future.get())))

    @property
    def cls(self) -> Any:
        return AsyncKafkaFuture

    def close(self) -> None:
        self.loop.close()


async def _capture(coro: Coroutine[Any, Any, Any]) -> tuple[bool, Any]:
    """Run ``coro`` and keep what it raised as the very object it raised.

    On Python 3.10 a ``CancelledError`` that leaves a task is replaced at the
    task boundary by a fresh one without its message (bpo-45390, fixed in
    3.11), so an error is caught inside the coroutine, where the user's code
    sees it, and raised again outside the loop by :func:`_outcome`."""
    try:
        return True, await coro
    except BaseException as error:  # noqa: BLE001 - raised again by _outcome
        return False, error


def _outcome(result: tuple[bool, Any]) -> Any:
    ok, value = result
    if ok:
        return value
    raise value


@pytest.fixture(params=["sync", "async"])
def kind(request: pytest.FixtureRequest) -> Any:
    k: Kind = SyncKind() if request.param == "sync" else AsyncKind()
    yield k
    k.close()


def new_future(kind: Kind) -> tuple[_KafkaFutureImpl[Any], Any]:
    """Java's ``new KafkaFutureImpl<>()``: the state and the public future over it."""
    impl: _KafkaFutureImpl[Any] = _KafkaFutureImpl()
    return impl, kind.wrap(impl)


# ---------------------------------------------------------------------------
# Java's helpers
# ---------------------------------------------------------------------------


def assert_is_successful(future: Any) -> None:
    """Asserts that the given future is done, didn't fail and wasn't cancelled."""
    assert future.is_done()
    assert not future.is_completed_exceptionally()
    assert not future.is_cancelled()


def assert_is_failed(future: Any) -> None:
    """Asserts that the given future is done, failed and wasn't cancelled."""
    assert future.is_done()
    assert not future.is_cancelled()
    assert future.is_completed_exceptionally()


def assert_is_cancelled(kind: Kind, future: Any) -> None:
    """Asserts that the given future is done, didn't fail and was cancelled."""
    assert future.is_done()
    assert future.is_cancelled()
    assert future.is_completed_exceptionally()
    with pytest.raises(kind.cancelled):
        future.get_now(value_if_absent=None)
    with pytest.raises(kind.cancelled):
        kind.get(future, 0)


def assert_is_not_completed(future: Any) -> None:
    assert not future.is_done()
    assert not future.is_completed_exceptionally()
    assert not future.is_cancelled()


def await_and_assert_result(kind: Kind, future: Any, expected: Any, alternative: Any) -> None:
    assert expected != alternative
    assert kind.get(future, _WAIT) == expected
    assert kind.get(future) == expected
    assert future.get_now(value_if_absent=alternative) == expected


def assert_future_throws(kind: Kind, future: Any, cls: type[BaseException],
                         timeout: float | None = None) -> BaseException:
    """Java's ``TestUtils.assertFutureThrows``: ``get()`` raises
    ``ExecutionError`` whose cause is exactly of ``cls`` (a subclass is
    rejected); the cause is returned."""
    with pytest.raises(ExecutionError) as info:
        kind.get(future, timeout)
    cause = info.value.__cause__
    assert cause is not None
    assert type(cause) is cls
    assert str(info.value) == _throwable.to_string(cause)
    return cause


def await_and_assert_failure(kind: Kind, future: Any, cls: type[BaseException],
                             message: str) -> BaseException:
    """Java's ``KafkaFutureTest.awaitAndAssertFailure``."""
    cause = assert_future_throws(kind, future, cls, _WAIT)
    assert str(cause) == message
    cause = assert_future_throws(kind, future, cls)
    assert str(cause) == message
    with pytest.raises(ExecutionError) as info:
        future.get_now(value_if_absent=None)
    assert type(info.value.__cause__) is cls
    assert str(info.value.__cause__) == message
    return info.value.__cause__


def await_and_assert_cancelled(kind: Kind, future: Any, message: str | None) -> None:
    for read in (lambda: kind.get(future, _WAIT), lambda: kind.get(future),
                 lambda: future.get_now(value_if_absent=None)):
        with pytest.raises(kind.cancelled) as info:
            read()
        assert type(info.value) is kind.cancelled
        assert _throwable.java_message(info.value) == message


class CompleterThread(threading.Thread):
    def __init__(self, impl: _KafkaFutureImpl[Any], value: Any,
                 error: BaseException | None = None) -> None:
        super().__init__()
        self.impl = impl
        self.value = value
        self.error = error
        self.test_error: BaseException | None = None

    def run(self) -> None:
        try:
            time.sleep(0.0000002)
            if self.error is None:
                self.impl.complete(self.value)
            else:
                self.impl.complete_with_error(self.error)
        except BaseException as e:  # noqa: BLE001 - reported to the test
            self.test_error = e


class WaiterThread(threading.Thread):
    def __init__(self, kind: Kind, future: Any, expected: Any) -> None:
        super().__init__()
        self.kind = kind
        self.future = future
        self.expected = expected
        self.test_error: BaseException | None = None

    def run(self) -> None:
        try:
            assert self.kind.get_in_thread(self.future) == self.expected
        except BaseException as e:  # noqa: BLE001 - reported to the test
            self.test_error = e


# ---------------------------------------------------------------------------
# KafkaFutureTest
# ---------------------------------------------------------------------------


def test_complete_futures(kind: Kind) -> None:
    impl123, future123 = new_future(kind)
    assert impl123.complete(123)
    assert not impl123.complete(456)
    assert not future123.cancel(may_interrupt_if_running=True)
    assert kind.get(future123) == 123
    assert_is_successful(future123)

    future456 = kind.cls.completed_future(value=456)
    assert not future456._impl.complete(789)
    assert not future456.cancel(may_interrupt_if_running=True)
    assert kind.get(future456) == 456
    assert_is_successful(future456)


def test_complete_futures_exceptionally(kind: Kind) -> None:
    fail_impl, future_fail = new_future(kind)
    assert fail_impl.complete_with_error(RuntimeError("We require more vespene gas"))
    assert_is_failed(future_fail)
    assert not fail_impl.complete_with_error(RuntimeError("We require more minerals"))
    assert not future_fail.cancel(may_interrupt_if_running=True)

    cause = assert_future_throws(kind, future_fail, RuntimeError)
    assert str(cause) == "We require more vespene gas"

    tricky_impl, tricky1 = new_future(kind)
    assert tricky_impl.complete_with_error(CompletionError(kind.cancelled()))
    assert_is_failed(tricky1)
    await_and_assert_failure(kind, tricky1, CompletionError, _throwable.to_string(kind.cancelled()))


def test_complete_futures_via_cancellation(kind: Kind) -> None:
    _, via_cancel = new_future(kind)
    assert via_cancel.cancel(may_interrupt_if_running=True)
    assert_is_cancelled(kind, via_cancel)
    await_and_assert_cancelled(kind, via_cancel, None)

    impl, via_cancelled_error = new_future(kind)
    assert impl.complete_with_error(kind.cancelled("We require more vespene gas"))
    assert_is_cancelled(kind, via_cancelled_error)
    await_and_assert_cancelled(kind, via_cancelled_error, "We require more vespene gas")


def test_to_string(kind: Kind) -> None:
    success_impl, success = new_future(kind)
    assert str(success) == "KafkaFuture{value=None,exception=None,done=False}"
    success_impl.complete(12)
    assert str(success) == "KafkaFuture{value=12,exception=None,done=True}"

    failure_impl, failure = new_future(kind)
    failure_impl.complete_with_error(RuntimeError("foo"))
    assert str(failure) == "KafkaFuture{value=None,exception=builtins.RuntimeError: foo,done=True}"

    tricky_impl, tricky1 = new_future(kind)
    tricky_impl.complete_with_error(CompletionError(kind.cancelled()))
    name = f"{kind.cancelled.__module__}.{kind.cancelled.__qualname__}"
    assert str(tricky1) == (
        f"KafkaFuture{{value=None,exception={__name__}.CompletionError: {name},done=True}}")

    _, cancelled = new_future(kind)
    cancelled.cancel(may_interrupt_if_running=True)
    assert str(cancelled) == f"KafkaFuture{{value=None,exception={name},done=True}}"


def test_completing_futures(kind: Kind) -> None:
    impl, future = new_future(kind)
    my_thread = CompleterThread(impl, "You must construct additional pylons.")
    assert_is_not_completed(future)
    assert future.get_now(value_if_absent="I am ready") == "I am ready"
    my_thread.start()
    await_and_assert_result(kind, future, "You must construct additional pylons.", "I am ready")
    assert_is_successful(future)
    my_thread.join()
    assert my_thread.test_error is None


def test_completing_futures_exceptionally(kind: Kind) -> None:
    impl, future = new_future(kind)
    my_thread = CompleterThread(impl, None, RuntimeError("Ultimate efficiency achieved."))
    assert_is_not_completed(future)
    assert future.get_now(value_if_absent="I am ready") == "I am ready"
    my_thread.start()
    await_and_assert_failure(kind, future, RuntimeError, "Ultimate efficiency achieved.")
    assert_is_failed(future)
    my_thread.join()
    assert my_thread.test_error is None


def test_completing_futures_via_cancellation(kind: Kind) -> None:
    impl, future = new_future(kind)
    my_thread = CompleterThread(impl, None, kind.cancelled("Ultimate efficiency achieved."))
    assert_is_not_completed(future)
    assert future.get_now(value_if_absent="I am ready") == "I am ready"
    my_thread.start()
    await_and_assert_cancelled(kind, future, "Ultimate efficiency achieved.")
    assert_is_cancelled(kind, future)
    my_thread.join()
    assert my_thread.test_error is None


def test_then_apply_on_succeeded_future(kind: Kind) -> None:
    impl, future = new_future(kind)
    doubled_future = future.then_apply(function=lambda integer: 2 * integer)
    assert not doubled_future.is_done()
    tripled_future = future.then_apply(function=lambda integer: 3 * integer)
    assert not tripled_future.is_done()
    impl.complete(21)
    assert future.get_now(value_if_absent=-1) == 21
    assert doubled_future.get_now(value_if_absent=-1) == 42
    assert tripled_future.get_now(value_if_absent=-1) == 63
    quadrupled_future = future.then_apply(function=lambda integer: 4 * integer)
    assert quadrupled_future.get_now(value_if_absent=-1) == 84


def test_then_apply_on_failed_future(kind: Kind) -> None:
    impl, future = new_future(kind)
    dependant_future = future.then_apply(function=lambda integer: 2 * integer)
    impl.complete_with_error(RuntimeError("We require more vespene gas"))
    assert_is_failed(future)
    assert_is_failed(dependant_future)
    await_and_assert_failure(kind, future, RuntimeError, "We require more vespene gas")
    await_and_assert_failure(kind, dependant_future, RuntimeError, "We require more vespene gas")


def test_then_apply_on_failed_future_tricky(kind: Kind) -> None:
    impl, future = new_future(kind)
    dependant_future = future.then_apply(function=lambda integer: 2 * integer)
    impl.complete_with_error(CompletionError(RuntimeError("We require more vespene gas")))
    assert_is_failed(future)
    assert_is_failed(dependant_future)
    message = "builtins.RuntimeError: We require more vespene gas"
    await_and_assert_failure(kind, future, CompletionError, message)
    await_and_assert_failure(kind, dependant_future, CompletionError, message)


def test_then_apply_on_failed_future_tricky2(kind: Kind) -> None:
    impl, future = new_future(kind)
    dependant_future = future.then_apply(function=lambda integer: 2 * integer)
    impl.complete_with_error(CompletionError(kind.cancelled()))
    assert_is_failed(future)
    assert_is_failed(dependant_future)
    message = _throwable.to_string(kind.cancelled())
    await_and_assert_failure(kind, future, CompletionError, message)
    await_and_assert_failure(kind, dependant_future, CompletionError, message)


def test_then_apply_on_succeeded_future_and_function_throws(kind: Kind) -> None:
    impl, future = new_future(kind)

    def function(integer: int) -> int:
        raise RuntimeError("We require more vespene gas")

    dependant_future = future.then_apply(function=function)
    impl.complete(21)
    assert_is_successful(future)
    assert_is_failed(dependant_future)
    await_and_assert_result(kind, future, 21, None)
    await_and_assert_failure(kind, dependant_future, RuntimeError, "We require more vespene gas")


def test_then_apply_on_succeeded_future_and_function_throws_completion_exception(kind: Kind) -> None:
    impl, future = new_future(kind)

    def function(integer: int) -> int:
        raise CompletionError(RuntimeError("We require more vespene gas"))

    dependant_future = future.then_apply(function=function)
    impl.complete(21)
    assert_is_successful(future)
    assert_is_failed(dependant_future)
    await_and_assert_result(kind, future, 21, None)
    cause = await_and_assert_failure(kind, dependant_future, CompletionError,
                                     "builtins.RuntimeError: We require more vespene gas")
    assert isinstance(cause.__cause__, RuntimeError)
    assert str(cause.__cause__) == "We require more vespene gas"


def test_then_apply_on_failed_future_function_not_called(kind: Kind) -> None:
    impl, future = new_future(kind)
    ran = [False]

    def function(integer: int) -> None:
        # Because the top level future failed, this should never be called.
        ran[0] = True

    dependant_future = future.then_apply(function=function)
    impl.complete_with_error(RuntimeError("We require more minerals"))
    assert_is_failed(future)
    assert_is_failed(dependant_future)
    await_and_assert_failure(kind, future, RuntimeError, "We require more minerals")
    await_and_assert_failure(kind, dependant_future, RuntimeError, "We require more minerals")
    assert not ran[0]


def test_then_apply_on_cancelled_future(kind: Kind) -> None:
    _, future = new_future(kind)
    dependant_future = future.then_apply(function=lambda integer: 2 * integer)
    future.cancel(may_interrupt_if_running=True)
    assert_is_cancelled(kind, future)
    assert_is_cancelled(kind, dependant_future)
    await_and_assert_cancelled(kind, future, None)
    await_and_assert_cancelled(kind, dependant_future, None)


def test_when_complete_on_succeeded_future(kind: Kind) -> None:
    impl, future = new_future(kind)
    err: list[BaseException | None] = [None]
    ran = [False]

    def action(integer: Any, ex: BaseException | None) -> None:
        ran[0] = True
        try:
            assert integer == 21
            if ex is not None:
                raise ex
        except BaseException as e:  # noqa: BLE001 - checked below
            err[0] = e

    dependant_future = future.when_complete(action=action)
    assert not dependant_future.is_done()
    assert impl.complete(21)
    assert ran[0]
    if err[0] is not None:
        raise err[0]


def test_when_complete_on_failed_future(kind: Kind) -> None:
    impl, future = new_future(kind)
    err: list[BaseException | None] = [None]
    ran = [False]

    def action(integer: Any, ex: BaseException | None) -> None:
        ran[0] = True
        err[0] = ex
        if integer is not None:
            err[0] = AssertionError()

    dependant_future = future.when_complete(action=action)
    assert not dependant_future.is_done()
    ex = RuntimeError("We require more vespene gas")
    assert impl.complete_with_error(ex)
    assert ran[0]
    assert err[0] is ex


def test_when_complete_on_succeeded_future_and_consumer_throws(kind: Kind) -> None:
    impl, future = new_future(kind)
    ran = [False]

    def action(integer: Any, ex: BaseException | None) -> None:
        ran[0] = True
        raise RuntimeError("We require more minerals")

    dependant_future = future.when_complete(action=action)
    assert not dependant_future.is_done()
    assert impl.complete(21)
    assert_is_successful(future)
    assert ran[0]
    assert_is_failed(dependant_future)
    await_and_assert_failure(kind, dependant_future, RuntimeError, "We require more minerals")


def test_when_complete_on_failed_future_and_consumer_throws(kind: Kind) -> None:
    impl, future = new_future(kind)
    ran = [False]

    def action(integer: Any, ex: BaseException | None) -> None:
        ran[0] = True
        raise RuntimeError("We require more minerals")

    dependant_future = future.when_complete(action=action)
    assert not dependant_future.is_done()
    assert impl.complete_with_error(RuntimeError("We require more vespene gas"))
    assert_is_failed(future)
    assert ran[0]
    assert_is_failed(dependant_future)
    await_and_assert_failure(kind, dependant_future, RuntimeError, "We require more vespene gas")


def test_when_complete_on_cancelled_future(kind: Kind) -> None:
    _, future = new_future(kind)
    err: list[BaseException | None] = [None]
    ran = [False]

    def action(integer: Any, ex: BaseException | None) -> None:
        ran[0] = True
        err[0] = ex
        if integer is not None:
            err[0] = AssertionError()

    dependant_future = future.when_complete(action=action)
    assert not dependant_future.is_done()
    assert future.cancel(may_interrupt_if_running=True)
    assert ran[0]
    assert isinstance(err[0], kind.cancelled)


def test_all_of_futures(kind: Kind) -> None:
    num_threads = 5
    pairs = [new_future(kind) for _ in range(num_threads)]
    impls = [p[0] for p in pairs]
    futures = [p[1] for p in pairs]
    all_future = kind.cls.all_of(futures=tuple(futures))
    completer_threads = [CompleterThread(impls[i], i) for i in range(num_threads)]
    waiter_threads = [WaiterThread(kind, futures[i], i) for i in range(num_threads)]
    assert not all_future.is_done()
    for t in waiter_threads:
        t.start()
    for t in completer_threads[:-1]:
        t.start()
    assert not all_future.is_done()
    completer_threads[-1].start()
    kind.get(all_future)
    assert_is_successful(all_future)
    for i in range(num_threads):
        assert kind.get(futures[i]) == i
    for i in range(num_threads):
        completer_threads[i].join()
        waiter_threads[i].join()
        assert completer_threads[i].test_error is None
        assert waiter_threads[i].test_error is None


def test_all_of_futures_with_failure(kind: Kind) -> None:
    num_threads = 5
    pairs = [new_future(kind) for _ in range(num_threads)]
    impls = [p[0] for p in pairs]
    futures = [p[1] for p in pairs]
    all_future = kind.cls.all_of(futures=tuple(futures))
    last_index = num_threads - 1
    completer_threads = [CompleterThread(impls[i], i) for i in range(last_index)]
    waiter_threads = [WaiterThread(kind, futures[i], i) for i in range(last_index)]
    completer_threads.append(CompleterThread(impls[last_index], None, RuntimeError("Last one failed")))
    waiter_threads.append(WaiterThread(kind, futures[last_index], last_index))
    assert not all_future.is_done()
    for t in waiter_threads:
        t.start()
    for t in completer_threads[:last_index]:
        t.start()
    assert not all_future.is_done()
    completer_threads[last_index].start()
    await_and_assert_failure(kind, all_future, RuntimeError, "Last one failed")
    assert_is_failed(all_future)
    for i in range(last_index):
        assert kind.get(futures[i]) == i
    assert_is_failed(futures[last_index])
    for i in range(num_threads):
        completer_threads[i].join()
        waiter_threads[i].join()
        assert completer_threads[i].test_error is None
        if i == last_index:
            error = waiter_threads[i].test_error
            assert type(error) is ExecutionError
            assert type(error.__cause__) is RuntimeError
            assert str(error.__cause__) == "Last one failed"
        else:
            assert waiter_threads[i].test_error is None


def test_all_of_futures_handles_zero_futures(kind: Kind) -> None:
    all_future = kind.cls.all_of(futures=())
    assert all_future.is_done()
    assert not all_future.is_cancelled()
    assert not all_future.is_completed_exceptionally()
    assert kind.get(all_future) is None


def test_future_timeout_with_zero_wait(kind: Kind) -> None:
    _, future = new_future(kind)
    with pytest.raises(JavaTimeoutError):
        kind.get(future, 0)


def test_leak_completable_future(kind: Kind) -> None:
    """Java's ``testLeakCompletableFuture``: the inner future
    ``to_completion_stage()`` returns cannot be completed by user code, which
    gets ``UnsupportedOperationException`` (``UnsupportedOperationError``).
    Java's ``completeOnTimeout`` / ``completeAsync`` / ``obtrude*`` /
    ``minimalCompletionStage`` have no counterpart on Python's futures."""
    impl, kfut = new_future(kind)
    if kind.name == "async":
        assert isinstance(kind, AsyncKind)

        async def stage() -> Any:
            return kfut.to_completion_stage()

        comfut = kind.run(stage())
    else:
        comfut = kfut.to_completion_stage()
    message = "User code should not complete futures returned from Kafka clients"
    with pytest.raises(UnsupportedOperationError, match=message):
        comfut.set_result("")
    with pytest.raises(UnsupportedOperationError, match=message):
        comfut.set_exception(RuntimeError())
    assert not kfut.is_done()
    assert not comfut.done()


# ---------------------------------------------------------------------------
# Beyond KafkaFutureTest
# ---------------------------------------------------------------------------


def test_the_constructors_are_not_public() -> None:
    with pytest.raises(TypeError, match=r"^KafkaFuture cannot be constructed directly; use "
                       r"KafkaFuture.completed_future\(...\) or KafkaFuture.all_of\(...\)$"):
        KafkaFuture()
    with pytest.raises(TypeError, match=r"^AsyncKafkaFuture cannot be constructed directly; use "
                       r"AsyncKafkaFuture.completed_future\(...\) or AsyncKafkaFuture.all_of\(...\)$"):
        AsyncKafkaFuture()


def test_every_parameter_is_keyword_only(kind: Kind) -> None:
    future = kind.cls.completed_future(value=1)
    calls: list[Callable[[], Any]] = [
        lambda: kind.cls.completed_future(1),
        lambda: kind.cls.all_of((future,)),
        lambda: future.then_apply(lambda v: v),
        lambda: future.when_complete(lambda v, e: None),
        lambda: future.cancel(True),
        lambda: future.get(1.0),
        lambda: future.get_now(0),
    ]
    for call in calls:
        with pytest.raises(TypeError):
            call()


def test_members_follow_java_declaration_order(kind: Kind) -> None:
    public = [name for name in vars(kind.cls) if not name.startswith("_")]
    assert public == [
        "BaseFunction", "BiConsumer", "completed_future", "all_of", "to_completion_stage",
        "then_apply", "when_complete", "cancel", "get", "get_now", "is_cancelled",
        "is_completed_exceptionally", "is_done"]


def test_get_is_async_only_on_the_async_kind() -> None:
    import inspect

    assert not inspect.iscoroutinefunction(KafkaFuture.get)
    assert inspect.iscoroutinefunction(AsyncKafkaFuture.get)
    for name in ("then_apply", "when_complete", "cancel", "get_now", "is_done"):
        assert not inspect.iscoroutinefunction(getattr(AsyncKafkaFuture, name))


def test_get_timeout_reading(kind: Kind) -> None:
    _, future = new_future(kind)
    for timeout in (0, -1, -0.5, timedelta(seconds=-3), timedelta(0), 0.0004):
        with pytest.raises(JavaTimeoutError) as info:
            kind.get(future, timeout)  # type: ignore[arg-type]
        assert str(info.value) == ""
    with pytest.raises(OverflowError):
        kind.get(future, math.inf)
    with pytest.raises(ValueError):
        kind.get(future, math.nan)
    with pytest.raises(OverflowError):
        kind.get(future, 1e17)  # 1e20 ms: past a signed 64-bit count
    done = kind.cls.completed_future(value="v")
    assert kind.get(done, 0) == "v"
    assert kind.get(done, -1) == "v"
    assert kind.get(done, timedelta(milliseconds=5)) == "v"


def test_a_negative_timeout_raises_at_once(kind: Kind) -> None:
    _, future = new_future(kind)
    start = time.monotonic()
    with pytest.raises(JavaTimeoutError):
        kind.get(future, -10)
    assert time.monotonic() - start < 1.0


def test_cancelling_a_dependant_leaves_its_source(kind: Kind) -> None:
    impl, future = new_future(kind)
    dependant = future.then_apply(function=lambda v: v)
    assert dependant.cancel(may_interrupt_if_running=False)
    assert dependant.is_cancelled()
    assert not future.is_done()
    impl.complete(3)
    assert kind.get(future) == 3
    assert dependant.is_cancelled()


def test_all_of_a_cancelled_source_is_cancelled(kind: Kind) -> None:
    _, a = new_future(kind)
    b_impl, b = new_future(kind)
    all_future = kind.cls.all_of(futures=(a, b))
    a.cancel(may_interrupt_if_running=False)
    assert not all_future.is_done()
    b_impl.complete(1)
    assert_is_cancelled(kind, all_future)


def test_cancelling_the_inner_future_cancels_the_future(kind: Kind) -> None:
    _, future = new_future(kind)
    if isinstance(kind, AsyncKind):
        async def cancel_inner() -> None:
            future.to_completion_stage().cancel()
            await asyncio.sleep(0)

        kind.run(cancel_inner())
    else:
        future.to_completion_stage().cancel()
    assert future.is_cancelled()


def test_the_inner_future_follows_the_future(kind: Kind) -> None:
    impl, future = new_future(kind)
    if isinstance(kind, AsyncKind):
        async def run() -> Any:
            inner = future.to_completion_stage()
            assert inner is future.to_completion_stage()
            threading.Thread(target=impl.complete, args=(7,)).start()
            return await inner

        assert kind.run(run()) == 7
    else:
        inner = future.to_completion_stage()
        impl.complete(7)
        assert inner.result() == 7


def test_waiting_task_cancellation_leaves_the_future() -> None:
    kind = AsyncKind()
    impl, future = new_future(kind)

    async def run() -> None:
        task = asyncio.ensure_future(future.get())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        kind.run(run())
        assert not future.is_done()
        impl.complete(1)
        assert kind.get(future) == 1
    finally:
        kind.close()


def test_ctrl_c_ends_a_sync_wait() -> None:
    impl: _KafkaFutureImpl[int] = _KafkaFutureImpl()
    future = KafkaFuture._of_impl(impl)
    import _thread

    threading.Timer(0.05, _thread.interrupt_main).start()
    with pytest.raises(KeyboardInterrupt):
        future.get(timeout=30)
    assert not future.is_done()


# ---------------------------------------------------------------------------
# The C-backed source
# ---------------------------------------------------------------------------


def native_future(kind: Kind, native: Any) -> Any:
    return KafkaFuture._of_native(native) if kind.name == "sync" else AsyncKafkaFuture._of_native(native)


def test_a_native_future_is_lazy(kind: Kind) -> None:
    native = _lib.KafkaFuture.completed_future(42)
    future = native_future(kind, native)
    assert future._impl is None and future._inner is None
    assert future.is_done()  # read from the C handle
    assert future._impl is None and future._inner is None
    # The other state reads settle the Python state from the done handle at
    # once, with no inner future.
    assert not future.is_cancelled()
    assert not future.is_completed_exceptionally()
    assert future._impl is not None and future._impl.is_done()
    assert future._inner is None
    assert not future.cancel(may_interrupt_if_running=True)  # done: nothing to cancel
    assert future.get_now(value_if_absent=-1) == 42
    assert future._inner is None
    assert kind.get(future) == 42
    assert_is_successful(future)


def test_a_native_future_delivers_through_get_cb(kind: Kind) -> None:
    native = _lib.KafkaFuture.completed_future(21)
    future = native_future(kind, native)
    # A dependant completes without anyone waiting on its source: the
    # registration is made by then_apply.
    doubled = future.then_apply(function=lambda v: 2 * v)
    assert doubled.is_done()
    assert doubled.get_now(value_if_absent=-1) == 42
    seen: list[Any] = []
    future.when_complete(action=lambda v, e: seen.append((v, e)))
    assert seen == [(21, None)]
    assert kind.get(future, 1.0) == 21


def test_a_failed_native_future_raises_execution_error(kind: Kind) -> None:
    error = RuntimeError("We require more vespene gas")
    native = _lib.KafkaFuture.completed_future(None, error)
    future = native_future(kind, native)
    assert future.is_done()
    cause = await_and_assert_failure(kind, future, RuntimeError, "We require more vespene gas")
    assert cause is error
    assert_is_failed(future)


def test_a_native_future_converts_with_the_creator_converter(kind: Kind) -> None:
    native = _lib.KafkaFuture.completed_future("value")
    addresses: list[int] = []

    def convert(address: int) -> str:
        addresses.append(address)
        return "converted"

    native.set_converter(convert)
    future = native_future(kind, native)
    assert kind.get(future) == "converted"
    assert addresses and addresses[0] != 0
    native2 = _lib.KafkaFuture.completed_future("value")
    native2.set_converter(lambda address: 1 / 0)
    assert_future_throws(kind, native_future(kind, native2), ZeroDivisionError)


def test_a_native_future_keeps_its_handle_alive() -> None:
    import gc
    import sys

    value = object()
    native = _lib.KafkaFuture.completed_future(value)
    refs = sys.getrefcount(value)
    future = KafkaFuture._of_native(native)
    del native
    gc.collect()
    assert future.get() is value
    assert sys.getrefcount(value) >= refs - 1
    del future
    gc.collect()


def test_a_native_void_future_delivers_none(kind: Kind) -> None:
    native = _lib.KafkaFuture.completed_future(None)
    assert kind.get(native_future(kind, native)) is None


def test_get_now_on_a_done_but_undelivered_native_future_answers_at_once(kind: Kind) -> None:
    """Java's ``getNow`` never blocks: a done handle whose ``get_cb`` delivery
    has not reached Python (simulated by a pending state, as a registration
    queued on a client's callback thread leaves it) is read at once."""
    native = _lib.KafkaFuture.completed_future(42)
    future = native_future(kind, native)
    pending: _KafkaFutureImpl[Any] = _KafkaFutureImpl()
    future._impl = pending  # the registration made, its delivery still queued
    seen: list[Any] = []
    future.when_complete(action=lambda v, e: seen.append(v))
    start = time.monotonic()
    assert future.get_now(value_if_absent=-1) == 42
    assert time.monotonic() - start < 1.0
    assert pending.is_done()
    assert seen == [42]
    assert not pending.complete(43)  # the late delivery finds the state complete
    assert future.get_now(value_if_absent=-1) == 42


def test_a_failed_native_future_never_waited_on_reports_its_failure(kind: Kind) -> None:
    future = native_future(kind, _lib.KafkaFuture.completed_future(None, RuntimeError("boom")))
    assert future.is_done()
    assert future.is_completed_exceptionally()
    assert not future.is_cancelled()
    assert str(future) == "KafkaFuture{value=None,exception=builtins.RuntimeError: boom,done=True}"
    assert str(native_future(kind, _lib.KafkaFuture.completed_future(7))) == (
        "KafkaFuture{value=7,exception=None,done=True}")


def test_get_on_a_done_but_undelivered_native_future_answers_at_once(kind: Kind) -> None:
    """A ``get()`` from the client's own callback thread: the handle is done
    but its ``get_cb`` delivery is queued behind the caller (simulated by a
    pending state), so the handle is read instead of waiting for it."""
    for timeout in (None, 5.0):
        future = native_future(kind, _lib.KafkaFuture.completed_future(42))
        future._impl = _KafkaFutureImpl()  # the registration made, its delivery queued
        start = time.monotonic()
        assert kind.get(future, timeout) == 42
        assert time.monotonic() - start < 1.0


def test_every_listener_runs_when_one_raises(kind: Kind) -> None:
    impl, future = new_future(kind)

    def interrupted(value: int) -> int:
        raise KeyboardInterrupt

    d1 = future.then_apply(function=interrupted)
    d2 = future.then_apply(function=lambda v: v + 1)
    inner_d2 = future.then_apply(function=lambda v: v * 3)
    if isinstance(kind, AsyncKind):
        async def stage() -> Any:
            return inner_d2.to_completion_stage()
        inner = kind.run(stage())
    else:
        inner = inner_d2.to_completion_stage()
    all_future = kind.cls.all_of(futures=(d1, d2))
    with pytest.raises(KeyboardInterrupt):
        impl.complete(1)
    assert d2.get_now(value_if_absent=None) == 2
    with pytest.raises(ExecutionError):
        d1.get_now(value_if_absent=None)
    assert all_future.is_done()
    assert kind.get(inner_d2) == 3
    if isinstance(kind, AsyncKind):
        async def settled() -> Any:
            await asyncio.sleep(0)
            return inner.result()
        assert kind.run(settled()) == 3
    else:
        assert inner.result() == 3
