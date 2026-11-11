// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

// Java's KafkaFutureTest (its 25 tests), then the JavaScript forms of the
// members, the two value sources (a record's future, Java's
// FutureRecordMetadata, and Java's KafkaFutureImpl), the timed get and the
// unhandled-rejection rules.
//
// Java's `new KafkaFutureImpl<>()` is `KafkaFuture.internalCreate(false)`, its
// `complete(v)` / `completeExceptionally(e)` the library-only
// `internalComplete(v, null)` / `internalComplete(null, e)`. Java's
// RuntimeException is JavaScript's Error. Every assertion on a failed future
// goes through get() and its ExecutionError, checking the cause's class
// exactly (a subclass is rejected), as Java's helpers do.
//
// Java's CompletionException has no JavaScript counterpart: Java's
// KafkaFutureImpl wraps and unwraps it only to undo CompletableFuture's own
// unwrapping, so the "tricky" tests use a test-local CompletionError (an
// error whose message is its cause's String(), as CompletionException's is
// the cause's toString()) and check that it, too, comes back as the given
// error. Java's CompleterThread and WaiterThread are a completion from a
// later macrotask and a get() started before it: JavaScript has one thread.

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const { CancellationError, ExecutionError, IllegalArgumentError, IllegalStateError, TimeoutError } = require('@confluentinc/kafka-javascript');
const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');

// Java's get(5, TimeUnit.MINUTES).
const WAIT = 5 * 60 * 1000;

/** Java's java.util.concurrent.CompletionException(Throwable cause): its message is cause.toString(). */
class CompletionError extends Error {
  constructor(cause) {
    super(String(cause), { cause });
    this.name = 'CompletionError';
  }
}

/** Java's `new KafkaFutureImpl<>()`. */
function newFuture() {
  return KafkaFuture.internalCreate(false);
}

const complete = (future, value) => future.internalComplete(value, null);
const completeExceptionally = (future, error) => future.internalComplete(null, error);

/** Asserts that the given future is done, didn't fail and wasn't cancelled. */
function assertIsSuccessful(future) {
  assert.equal(future.isDone(), true);
  assert.equal(future.isCompletedExceptionally(), false);
  assert.equal(future.isCancelled(), false);
}

/** Asserts that the given future is done, failed and wasn't cancelled. */
function assertIsFailed(future) {
  assert.equal(future.isDone(), true);
  assert.equal(future.isCancelled(), false);
  assert.equal(future.isCompletedExceptionally(), true);
}

/** Asserts that the given future is done, didn't fail and was cancelled. */
async function assertIsCancelled(future) {
  assert.equal(future.isDone(), true);
  assert.equal(future.isCancelled(), true);
  assert.equal(future.isCompletedExceptionally(), true);
  assert.throws(() => future.getNow(null), CancellationError);
  await assert.rejects(future.get(0), CancellationError);
}

function assertIsNotCompleted(future) {
  assert.equal(future.isDone(), false);
  assert.equal(future.isCompletedExceptionally(), false);
  assert.equal(future.isCancelled(), false);
}

async function awaitAndAssertResult(future, expectedResult, alternativeValue) {
  assert.notEqual(expectedResult, alternativeValue);
  assert.equal(await future.get(WAIT), expectedResult);
  assert.equal(await future.get(), expectedResult);
  assert.equal(future.getNow(alternativeValue), expectedResult);
}

/** The ExecutionError a rejection or a throw carries, checked: its cause is exactly of `cls`. */
function executionCause(error, cls) {
  assert.equal(error.constructor, ExecutionError);
  assert.ok(error.cause !== null && error.cause !== undefined);
  assert.equal(error.cause.constructor, cls, `cause is ${error.cause.constructor.name}, not ${cls.name}`);
  // Java's ExecutionException(Throwable): the cause's toString().
  assert.equal(error.message, String(error.cause));
  return error.cause;
}

/** Java's TestUtils.assertFutureThrows: get() rejects with ExecutionError whose cause is exactly `cls`. */
async function assertFutureThrows(future, cls, timeout) {
  let caught = null;
  try {
    await (timeout === undefined ? future.get() : future.get(timeout));
  } catch (error) {
    caught = error;
  }
  assert.ok(caught !== null, 'get() did not reject');
  return executionCause(caught, cls);
}

/** Java's KafkaFutureTest.awaitAndAssertFailure. */
async function awaitAndAssertFailure(future, expectedError, expectedMessage) {
  let cause = await assertFutureThrows(future, expectedError, WAIT);
  assert.equal(cause.message, expectedMessage);
  cause = await assertFutureThrows(future, expectedError);
  assert.equal(cause.message, expectedMessage);
  let thrown = null;
  try {
    future.getNow(null);
  } catch (error) {
    thrown = error;
  }
  assert.ok(thrown !== null, 'getNow() did not throw');
  cause = executionCause(thrown, expectedError);
  assert.equal(cause.message, expectedMessage);
  return cause;
}

/** Java's KafkaFutureTest.awaitAndAssertCancelled; Java's null message is ''. */
async function awaitAndAssertCancelled(future, expectedMessage) {
  const check = (error) => {
    assert.equal(error.constructor, CancellationError);
    assert.equal(error.message, expectedMessage);
    return true;
  };
  await assert.rejects(future.get(WAIT), check);
  await assert.rejects(future.get(), check);
  assert.throws(() => future.getNow(null), check);
}

/** Java's CompleterThread: completes the future from a later macrotask. */
function completer(future, value, error = null) {
  let started = null;
  return {
    start() {
      started = new Promise((resolve) => {
        setImmediate(() => {
          future.internalComplete(error === null ? value : null, error);
          resolve();
        });
      });
    },
    join() {
      return started;
    },
  };
}

/** Java's WaiterThread: a get() started now; join() gives what it got or threw. */
function waiter(future) {
  let outcome = null;
  return {
    start() {
      outcome = future.get().then(
        (value) => ({ value, error: null }),
        (error) => ({ value: null, error }),
      );
    },
    join() {
      return outcome;
    },
  };
}

// --- KafkaFutureTest --------------------------------------------------------

test('testCompleteFutures', async () => {
  const future123 = newFuture();
  assert.equal(complete(future123, 123), true);
  assert.equal(complete(future123, 456), false);
  assert.equal(future123.cancel(true), false);
  assert.equal(await future123.get(), 123);
  assertIsSuccessful(future123);

  const future456 = KafkaFuture.completedFuture(456);
  assert.equal(complete(future456, 789), false);
  assert.equal(future456.cancel(true), false);
  assert.equal(await future456.get(), 456);
  assertIsSuccessful(future456);
});

test('testCompleteFuturesExceptionally', async () => {
  const futureFail = newFuture();
  assert.equal(completeExceptionally(futureFail, new Error('We require more vespene gas')), true);
  assertIsFailed(futureFail);
  assert.equal(completeExceptionally(futureFail, new Error('We require more minerals')), false);
  assert.equal(futureFail.cancel(true), false);

  const cause = await assertFutureThrows(futureFail, Error);
  assert.equal(cause.message, 'We require more vespene gas');

  const tricky1 = newFuture();
  assert.equal(completeExceptionally(tricky1, new CompletionError(new CancellationError())), true);
  assertIsFailed(tricky1);
  await awaitAndAssertFailure(tricky1, CompletionError, 'CancellationError');
});

test('testCompleteFuturesViaCancellation', async () => {
  const viaCancel = newFuture();
  assert.equal(viaCancel.cancel(true), true);
  await assertIsCancelled(viaCancel);
  await awaitAndAssertCancelled(viaCancel, '');

  const viaCancellationException = newFuture();
  assert.equal(completeExceptionally(viaCancellationException, new CancellationError('We require more vespene gas')), true);
  await assertIsCancelled(viaCancellationException);
  await awaitAndAssertCancelled(viaCancellationException, 'We require more vespene gas');
});

test('testToString', () => {
  const success = newFuture();
  assert.equal(success.toString(), 'KafkaFuture{value=null,exception=null,done=false}');
  complete(success, 12);
  assert.equal(success.toString(), 'KafkaFuture{value=12,exception=null,done=true}');

  const failure = newFuture();
  completeExceptionally(failure, new Error('foo'));
  assert.equal(failure.toString(), 'KafkaFuture{value=null,exception=Error: foo,done=true}');

  const tricky1 = newFuture();
  completeExceptionally(tricky1, new CompletionError(new CancellationError()));
  assert.equal(tricky1.toString(), 'KafkaFuture{value=null,exception=CompletionError: CancellationError,done=true}');

  const cancelled = newFuture();
  cancelled.cancel(true);
  assert.equal(cancelled.toString(), 'KafkaFuture{value=null,exception=CancellationError,done=true}');
});

test('testCompletingFutures', async () => {
  const future = newFuture();
  const myThread = completer(future, 'You must construct additional pylons.');
  assertIsNotCompleted(future);
  assert.equal(future.getNow('I am ready'), 'I am ready');
  myThread.start();
  await awaitAndAssertResult(future, 'You must construct additional pylons.', 'I am ready');
  assertIsSuccessful(future);
  await myThread.join();
});

test('testCompletingFuturesExceptionally', async () => {
  const future = newFuture();
  const myThread = completer(future, null, new Error('Ultimate efficiency achieved.'));
  assertIsNotCompleted(future);
  assert.equal(future.getNow('I am ready'), 'I am ready');
  myThread.start();
  await awaitAndAssertFailure(future, Error, 'Ultimate efficiency achieved.');
  assertIsFailed(future);
  await myThread.join();
});

test('testCompletingFuturesViaCancellation', async () => {
  const future = newFuture();
  const myThread = completer(future, null, new CancellationError('Ultimate efficiency achieved.'));
  assertIsNotCompleted(future);
  assert.equal(future.getNow('I am ready'), 'I am ready');
  myThread.start();
  await awaitAndAssertCancelled(future, 'Ultimate efficiency achieved.');
  await assertIsCancelled(future);
  await myThread.join();
});

test('testThenApplyOnSucceededFuture', () => {
  const future = newFuture();
  const doubledFuture = future.thenApply((integer) => 2 * integer);
  assert.equal(doubledFuture.isDone(), false);
  const tripledFuture = future.thenApply((integer) => 3 * integer);
  assert.equal(tripledFuture.isDone(), false);
  complete(future, 21);
  assert.equal(future.getNow(-1), 21);
  assert.equal(doubledFuture.getNow(-1), 42);
  assert.equal(tripledFuture.getNow(-1), 63);
  const quadrupledFuture = future.thenApply((integer) => 4 * integer);
  assert.equal(quadrupledFuture.getNow(-1), 84);
});

test('testThenApplyOnFailedFuture', async () => {
  const future = newFuture();
  const dependantFuture = future.thenApply((integer) => 2 * integer);
  completeExceptionally(future, new Error('We require more vespene gas'));
  assertIsFailed(future);
  assertIsFailed(dependantFuture);
  await awaitAndAssertFailure(future, Error, 'We require more vespene gas');
  await awaitAndAssertFailure(dependantFuture, Error, 'We require more vespene gas');
});

test('testThenApplyOnFailedFutureTricky', async () => {
  const future = newFuture();
  const dependantFuture = future.thenApply((integer) => 2 * integer);
  completeExceptionally(future, new CompletionError(new Error('We require more vespene gas')));
  assertIsFailed(future);
  assertIsFailed(dependantFuture);
  await awaitAndAssertFailure(future, CompletionError, 'Error: We require more vespene gas');
  await awaitAndAssertFailure(dependantFuture, CompletionError, 'Error: We require more vespene gas');
});

test('testThenApplyOnFailedFutureTricky2', async () => {
  const future = newFuture();
  const dependantFuture = future.thenApply((integer) => 2 * integer);
  completeExceptionally(future, new CompletionError(new CancellationError()));
  assertIsFailed(future);
  assertIsFailed(dependantFuture);
  await awaitAndAssertFailure(future, CompletionError, 'CancellationError');
  await awaitAndAssertFailure(dependantFuture, CompletionError, 'CancellationError');
});

test('testThenApplyOnSucceededFutureAndFunctionThrows', async () => {
  const future = newFuture();
  const dependantFuture = future.thenApply(() => {
    throw new Error('We require more vespene gas');
  });
  complete(future, 21);
  assertIsSuccessful(future);
  assertIsFailed(dependantFuture);
  await awaitAndAssertResult(future, 21, null);
  await awaitAndAssertFailure(dependantFuture, Error, 'We require more vespene gas');
});

test('testThenApplyOnSucceededFutureAndFunctionThrowsCompletionException', async () => {
  const future = newFuture();
  const dependantFuture = future.thenApply(() => {
    throw new CompletionError(new Error('We require more vespene gas'));
  });
  complete(future, 21);
  assertIsSuccessful(future);
  assertIsFailed(dependantFuture);
  await awaitAndAssertResult(future, 21, null);
  const cause = await awaitAndAssertFailure(dependantFuture, CompletionError, 'Error: We require more vespene gas');
  assert.ok(cause.cause instanceof Error);
  assert.equal(cause.cause.message, 'We require more vespene gas');
});

test('testThenApplyOnFailedFutureFunctionNotCalled', async () => {
  const future = newFuture();
  const ran = [false];
  const dependantFuture = future.thenApply(() => {
    // Because the top level future failed, this should never be called.
    ran[0] = true;
    return null;
  });
  completeExceptionally(future, new Error('We require more minerals'));
  assertIsFailed(future);
  assertIsFailed(dependantFuture);
  await awaitAndAssertFailure(future, Error, 'We require more minerals');
  await awaitAndAssertFailure(dependantFuture, Error, 'We require more minerals');
  assert.equal(ran[0], false);
});

test('testThenApplyOnCancelledFuture', async () => {
  const future = newFuture();
  const dependantFuture = future.thenApply((integer) => 2 * integer);
  future.cancel(true);
  await assertIsCancelled(future);
  await assertIsCancelled(dependantFuture);
  await awaitAndAssertCancelled(future, '');
  await awaitAndAssertCancelled(dependantFuture, '');
});

test('testWhenCompleteOnSucceededFuture', () => {
  const future = newFuture();
  const err = [null];
  const ran = [false];
  const dependantFuture = future.whenComplete((integer, ex) => {
    ran[0] = true;
    try {
      assert.equal(integer, 21);
      if (ex !== null) throw ex;
    } catch (e) {
      err[0] = e;
    }
  });
  assert.equal(dependantFuture.isDone(), false);
  assert.equal(complete(future, 21), true);
  assert.equal(ran[0], true);
  if (err[0] !== null) throw err[0];
});

test('testWhenCompleteOnFailedFuture', () => {
  const future = newFuture();
  const err = [null];
  const ran = [false];
  const dependantFuture = future.whenComplete((integer, ex) => {
    ran[0] = true;
    err[0] = ex;
    if (integer !== null) err[0] = new assert.AssertionError({ message: 'a value' });
  });
  assert.equal(dependantFuture.isDone(), false);
  const ex = new Error('We require more vespene gas');
  assert.equal(completeExceptionally(future, ex), true);
  assert.equal(ran[0], true);
  // The raw failure, not an ExecutionError.
  assert.equal(err[0], ex);
});

test('testWhenCompleteOnSucceededFutureAndConsumerThrows', async () => {
  const future = newFuture();
  const ran = [false];
  const dependantFuture = future.whenComplete(() => {
    ran[0] = true;
    throw new Error('We require more minerals');
  });
  assert.equal(dependantFuture.isDone(), false);
  assert.equal(complete(future, 21), true);
  assertIsSuccessful(future);
  assert.equal(ran[0], true);
  assertIsFailed(dependantFuture);
  await awaitAndAssertFailure(dependantFuture, Error, 'We require more minerals');
});

test('testWhenCompleteOnFailedFutureAndConsumerThrows', async () => {
  const future = newFuture();
  const ran = [false];
  const dependantFuture = future.whenComplete(() => {
    ran[0] = true;
    throw new Error('We require more minerals');
  });
  assert.equal(dependantFuture.isDone(), false);
  assert.equal(completeExceptionally(future, new Error('We require more vespene gas')), true);
  assertIsFailed(future);
  assert.equal(ran[0], true);
  assertIsFailed(dependantFuture);
  await awaitAndAssertFailure(dependantFuture, Error, 'We require more vespene gas');
});

test('testWhenCompleteOnCancelledFuture', () => {
  const future = newFuture();
  const err = [null];
  const ran = [false];
  const dependantFuture = future.whenComplete((integer, ex) => {
    ran[0] = true;
    err[0] = ex;
    if (integer !== null) err[0] = new assert.AssertionError({ message: 'a value' });
  });
  assert.equal(dependantFuture.isDone(), false);
  assert.equal(future.cancel(true), true);
  assert.equal(ran[0], true);
  assert.ok(err[0] instanceof CancellationError);
});

test('testAllOfFutures', async () => {
  const numThreads = 5;
  const futures = [];
  for (let i = 0; i < numThreads; i++) futures.push(newFuture());
  const allFuture = KafkaFuture.allOf(futures);
  const completerThreads = [];
  const waiterThreads = [];
  for (let i = 0; i < numThreads; i++) {
    completerThreads.push(completer(futures[i], i));
    waiterThreads.push(waiter(futures[i]));
  }
  assert.equal(allFuture.isDone(), false);
  for (let i = 0; i < numThreads; i++) waiterThreads[i].start();
  for (let i = 0; i < numThreads - 1; i++) completerThreads[i].start();
  for (let i = 0; i < numThreads - 1; i++) await completerThreads[i].join();
  assert.equal(allFuture.isDone(), false);
  completerThreads[numThreads - 1].start();
  assert.equal(await allFuture.get(), null);
  assertIsSuccessful(allFuture);
  for (let i = 0; i < numThreads; i++) assert.equal(await futures[i].get(), i);
  for (let i = 0; i < numThreads; i++) {
    await completerThreads[i].join();
    assert.deepEqual(await waiterThreads[i].join(), { value: i, error: null });
  }
});

test('testAllOfFuturesWithFailure', async () => {
  const numThreads = 5;
  const futures = [];
  for (let i = 0; i < numThreads; i++) futures.push(newFuture());
  const allFuture = KafkaFuture.allOf(futures);
  const completerThreads = [];
  const waiterThreads = [];
  const lastIndex = numThreads - 1;
  for (let i = 0; i < lastIndex; i++) {
    completerThreads.push(completer(futures[i], i));
    waiterThreads.push(waiter(futures[i]));
  }
  completerThreads.push(completer(futures[lastIndex], null, new Error('Last one failed')));
  waiterThreads.push(waiter(futures[lastIndex]));
  assert.equal(allFuture.isDone(), false);
  for (let i = 0; i < numThreads; i++) waiterThreads[i].start();
  for (let i = 0; i < lastIndex; i++) completerThreads[i].start();
  for (let i = 0; i < lastIndex; i++) await completerThreads[i].join();
  assert.equal(allFuture.isDone(), false);
  completerThreads[lastIndex].start();
  await awaitAndAssertFailure(allFuture, Error, 'Last one failed');
  assertIsFailed(allFuture);
  for (let i = 0; i < lastIndex; i++) assert.equal(await futures[i].get(), i);
  assertIsFailed(futures[lastIndex]);
  for (let i = 0; i < numThreads; i++) {
    await completerThreads[i].join();
    const { value, error } = await waiterThreads[i].join();
    if (i === lastIndex) {
      assert.equal(error.constructor, ExecutionError);
      assert.equal(error.cause.constructor, Error);
      assert.equal(error.cause.message, 'Last one failed');
    } else {
      assert.deepEqual({ value, error }, { value: i, error: null });
    }
  }
});

test('testAllOfFuturesHandlesZeroFutures', async () => {
  const allFuture = KafkaFuture.allOf([]);
  assert.equal(allFuture.isDone(), true);
  assert.equal(allFuture.isCancelled(), false);
  assert.equal(allFuture.isCompletedExceptionally(), false);
  assert.equal(await allFuture.get(), null);
});

test('testFutureTimeoutWithZeroWait', async () => {
  const future = newFuture();
  await assert.rejects(future.get(0), (e) => e.constructor === TimeoutError && e.message === '');
});

// Java's testLeakCompletableFuture checks that user code cannot complete the
// CompletableFuture toCompletionStage() hands out (complete,
// completeExceptionally, completeOnTimeout, completeAsync, obtrudeValue,
// obtrudeException, minimalCompletionStage). The JavaScript stage is a
// Promise, which has no completion method at all: only its executor settles
// it. What is left to check is that settling the Promise is the future's
// alone, and that nothing done with the Promise completes the future.
test('testLeakCompletableFuture', async () => {
  const kfut = newFuture();
  const comfut = kfut.toCompletionStage();
  assert.ok(comfut instanceof Promise);
  for (const method of ['complete', 'completeExceptionally', 'completeOnTimeout', 'completeAsync', 'obtrudeValue', 'obtrudeException']) {
    assert.equal(comfut[method], undefined, method);
  }
  // A Promise chained from it does not complete the future either.
  void comfut.then(() => '');
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(kfut.isDone(), false);
  assert.equal(kfut.toCompletionStage(), comfut);
});

// --- The JavaScript forms ---------------------------------------------------

test('each member takes the bare value or its options object, as Java names the parameter', async () => {
  assert.equal(KafkaFuture.completedFuture({ value: 7 }).getNow(null), 7);
  // A plain object is always the options object: an object value goes by name.
  const plain = { a: 1 };
  assert.equal(KafkaFuture.completedFuture({ value: plain }).getNow(null), plain);
  assert.equal(KafkaFuture.completedFuture(null).getNow(1), null);
  // Anything but a plain object may be given bare: a class instance, an array, a primitive.
  const instance = new (class Box {})();
  assert.equal(KafkaFuture.completedFuture(instance).getNow(null), instance);
  const array = [1, 2];
  assert.equal(KafkaFuture.completedFuture(array).getNow(null), array);
  assert.equal(KafkaFuture.completedFuture('v').getNow(null), 'v');
  assert.equal(KafkaFuture.allOf({ futures: [KafkaFuture.completedFuture(1)] }).isDone(), true);
  const future = KafkaFuture.completedFuture(2);
  assert.equal(future.thenApply({ function: (v) => v + 1 }).getNow(null), 3);
  const seen = [];
  future.whenComplete({ action: (value, error) => seen.push(value, error) });
  assert.deepEqual(seen, [2, null]);
  assert.equal(future.getNow({ valueIfAbsent: 0 }), 2);
  assert.equal(newFuture().getNow({ valueIfAbsent: plain }), plain);
  assert.equal(newFuture().getNow(instance), instance);
  assert.equal(newFuture().getNow(array), array);
  assert.equal(await future.get({ timeout: 10 }), 2);
  assert.equal(newFuture().cancel({ mayInterruptIfRunning: false }), true);
});

test('a form no Java overload has is refused with the form error', async () => {
  const future = newFuture();
  assert.throws(() => KafkaFuture.completedFuture(), { name: 'IllegalArgumentError', message: 'completedFuture() takes one of (value); got ()' });
  assert.throws(() => KafkaFuture.completedFuture(1, 2), IllegalArgumentError);
  // A bare plain object is the options object, so a record-shaped value is a form error.
  assert.throws(() => KafkaFuture.completedFuture({ key: 'k', value: 'v' }), {
    name: 'IllegalArgumentError',
    message: 'completedFuture() takes one of (value); got (value, key)',
  });
  assert.throws(() => KafkaFuture.completedFuture({ a: 1 }), { message: 'completedFuture() takes one of (value); got (a)' });
  assert.throws(() => future.getNow({ valueIfAbsent: 1, other: 2 }), { message: 'getNow() takes one of (valueIfAbsent); got (valueIfAbsent, other)' });
  assert.throws(() => future.getNow({ b: 2 }), { message: 'getNow() takes one of (valueIfAbsent); got (b)' });
  assert.throws(() => KafkaFuture.allOf(), { message: 'allOf() takes one of (futures); got ()' });
  assert.throws(() => KafkaFuture.allOf([], []), { message: 'allOf() takes one of (futures); got 2 arguments, not one object of named parameters' });
  assert.throws(() => future.thenApply({ fn: () => 1 }), { message: 'thenApply() takes one of (function); got (fn)' });
  assert.throws(() => future.whenComplete({ callback: () => 1 }), { message: 'whenComplete() takes one of (action); got (callback)' });
  assert.throws(() => future.cancel(), { message: 'cancel() takes one of (mayInterruptIfRunning); got ()' });
  assert.throws(() => future.getNow(), { message: 'getNow() takes one of (valueIfAbsent); got ()' });
  // The readers take no argument, undefined included.
  for (const method of ['isDone', 'isCancelled', 'isCompletedExceptionally']) {
    for (const arg of [1, undefined]) {
      assert.throws(() => future[method](arg), { name: 'TypeError', message: `${method}() only reads: it takes no argument` }, method);
    }
  }
  // A method returning a Promise rejects instead of throwing.
  await assert.rejects(future.get(1, 2), { message: 'get() takes one of (), (timeout); got 2 arguments, not one object of named parameters' });
  await assert.rejects(future.get({ timeoutMs: 1 }), { message: 'get() takes one of (), (timeout); got (timeoutMs)' });
  await assert.rejects(future.toCompletionStage(1), { message: 'toCompletionStage() takes one of (); got a number' });
});

test('undefined is refused, null is Java\'s null', async () => {
  const future = newFuture();
  assert.throws(() => KafkaFuture.completedFuture(undefined), {
    name: 'TypeError',
    message: "value must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.throws(() => KafkaFuture.completedFuture({ value: undefined }), {
    name: 'TypeError',
    message: "value must not be undefined. Leave the key out, or pass null for Java's null",
  });
  assert.throws(() => future.getNow(undefined), {
    name: 'TypeError',
    message: "valueIfAbsent must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.throws(() => future.thenApply(undefined), {
    name: 'TypeError',
    message: "function must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.throws(() => future.thenApply(null), { name: 'TypeError', message: 'function must not be null' });
  // A bare value of the wrong type fits no form; named, it is the parameter's TypeError.
  assert.throws(() => future.thenApply(1), { name: 'IllegalArgumentError', message: 'thenApply() takes one of (function); got a number' });
  assert.throws(() => future.thenApply({ function: 1 }), { name: 'TypeError', message: 'function must be a function (value) => result, not number' });
  assert.throws(() => future.whenComplete(null), { name: 'TypeError', message: 'action must not be null' });
  assert.throws(() => future.whenComplete('x'), { name: 'IllegalArgumentError', message: 'whenComplete() takes one of (action); got a string' });
  assert.throws(() => future.whenComplete({ action: 'x' }), { name: 'TypeError', message: 'action must be a function (value, error) => void, not string' });
  assert.throws(() => future.cancel(null), { name: 'IllegalArgumentError', message: 'cancel() takes one of (mayInterruptIfRunning); got null' });
  assert.throws(() => future.cancel({ mayInterruptIfRunning: null }), { name: 'TypeError', message: 'mayInterruptIfRunning must be a boolean, not null' });
  assert.throws(() => future.cancel(1), { name: 'IllegalArgumentError', message: 'cancel() takes one of (mayInterruptIfRunning); got a number' });
  assert.throws(() => KafkaFuture.allOf(null), { name: 'TypeError', message: 'futures must not be null' });
  assert.throws(() => KafkaFuture.allOf(future), { name: 'IllegalArgumentError', message: 'allOf() takes one of (futures); got a KafkaFuture' });
  assert.throws(() => KafkaFuture.allOf({ futures: future }), { name: 'TypeError', message: 'futures must be an array of KafkaFuture, not object' });
  assert.throws(() => KafkaFuture.allOf([future, null]), { name: 'TypeError', message: 'futures must not contain null' });
  assert.throws(() => KafkaFuture.allOf([undefined]), { name: 'TypeError', message: 'futures must not contain undefined' });
  assert.throws(() => KafkaFuture.allOf([1]), { name: 'TypeError', message: 'futures must contain only KafkaFutures, not number' });
  // get(undefined) is refused: undefined is never "not given".
  complete(future, 'v');
  await assert.rejects(future.get(undefined), {
    name: 'TypeError',
    message: "timeout must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  assert.equal(await future.get(), 'v');
  await assert.rejects(future.get(null), { name: 'TypeError', message: 'timeout must not be null' });
  await assert.rejects(future.get({ timeout: undefined }), {
    name: 'TypeError',
    message: "timeout must not be undefined. Leave the key out, or pass null for Java's null",
  });
  await assert.rejects(future.get(NaN), { name: 'RangeError' });
  await assert.rejects(future.get('5'), { name: 'IllegalArgumentError', message: 'get() takes one of (), (timeout); got a string' });
  await assert.rejects(future.get({ timeout: '5' }), { name: 'TypeError', message: 'timeout must be a number of milliseconds or a Temporal.Duration' });
});

// --- The two value sources ---------------------------------------------------

test("a record's future is Java's FutureRecordMetadata: never cancelled", async () => {
  const record = KafkaFuture.internalCreate(true);
  assert.equal(record.cancel(true), false);
  assert.equal(record.cancel({ mayInterruptIfRunning: false }), false);
  assertIsNotCompleted(record);
  // Its timed get times out with FutureRecordMetadata's message.
  await assert.rejects(record.get(0), (e) => e.constructor === TimeoutError && e.message === 'Timeout after waiting for 0 ms.');
  await assert.rejects(record.get(-5), { message: 'Timeout after waiting for -5 ms.' });
  await assert.rejects(record.get(15), { message: 'Timeout after waiting for 15 ms.' });
  // A dependant of it is Java's KafkaFutureImpl, and can be cancelled.
  const dependant = record.thenApply((m) => m);
  assert.equal(dependant.cancel(true), true);
  assert.equal(record.isDone(), false);
  complete(record, 'metadata');
  assert.equal(await record.get(), 'metadata');
  await awaitAndAssertCancelled(dependant, '');
});

// Java's answers, from KafkaFutureImpl.cancel (CompletableFuture.cancel:
// `cancelled || isCancelled()`, where a dependant cancelled through its source
// or by its own function holds a CompletionException, so it is not "cancelled"
// to CompletableFuture), while isCancelled() answers true for each.
test("cancel() answers as Java's KafkaFutureImpl for each way a future is cancelled", () => {
  const src = newFuture();
  const dep = src.thenApply((x) => x * 2);
  assert.equal(src.cancel(true), true);
  assert.equal(dep.isCancelled(), true);
  assert.equal(dep.cancel(true), false);
  assert.equal(dep.cancel(false), false);

  const s2 = newFuture();
  const d2 = s2.thenApply(() => {
    throw new CancellationError('stop');
  });
  complete(s2, 1);
  assert.equal(d2.isCancelled(), true);
  assert.equal(d2.cancel(true), false);

  const d3 = newFuture().thenApply((x) => x);
  assert.equal(d3.cancel(true), true);
  assert.equal(d3.cancel(true), true);

  const s4 = newFuture();
  const d4 = s4.whenComplete(() => {});
  s4.cancel(true);
  assert.equal(d4.isCancelled(), true);
  assert.equal(d4.cancel(true), false);

  const s5 = newFuture();
  const d5 = s5.whenComplete(() => {
    throw new CancellationError('stop');
  });
  complete(s5, 1);
  assert.equal(d5.isCancelled(), true);
  assert.equal(d5.cancel(true), false);

  const s6 = newFuture();
  const all = KafkaFuture.allOf([s6, KafkaFuture.completedFuture(1)]);
  s6.cancel(true);
  assert.equal(all.isCancelled(), true);
  assert.equal(all.cancel(true), true);
  // allOf over a dependant cancelled through its source is cancelled on its own too.
  const s7 = newFuture();
  const all7 = KafkaFuture.allOf([s7.thenApply((x) => x)]);
  s7.cancel(true);
  assert.equal(all7.cancel(true), true);

  // A CancellationError completing a future that is not a dependant cancels it on its own.
  const s8 = newFuture();
  completeExceptionally(s8, new CancellationError('m'));
  assert.equal(s8.cancel(true), true);

  const failed = newFuture();
  completeExceptionally(failed, new Error('x'));
  assert.equal(failed.cancel(true), false);
});

// JavaScript can throw any value; Java and Python only a Throwable. Each entry:
// the value, the ExecutionError's message (the (cause) form's: '' for null and
// undefined) and what toString() shows of it.
const revoked = Proxy.revocable({}, {});
revoked.revoke();
const UNREADABLE = 'a value that cannot be converted to a string';
const THROWN = [
  ['undefined', undefined, '', 'undefined'],
  ['null', null, '', 'null'],
  ['Object.create(null)', Object.create(null), '[object Object]', '[object Object]'],
  ['a Symbol', Symbol('s'), 'Symbol(s)', 'Symbol(s)'],
  [
    'an object whose toString throws',
    {
      toString() {
        throw new Error('no');
      },
    },
    '[object Object]',
    '[object Object]',
  ],
  // instanceof reads the prototype chain, which these refuse.
  ['a revoked Proxy', revoked.proxy, UNREADABLE, UNREADABLE],
  [
    'a Proxy whose getPrototypeOf trap throws',
    new Proxy(
      {},
      {
        getPrototypeOf() {
          throw new Error('trap');
        },
      },
    ),
    '[object Object]',
    '[object Object]',
  ],
];

for (const [what, value, message, shown] of THROWN) {
  const check = (e) => {
    assert.equal(e.constructor, ExecutionError);
    assert.equal(e.cause, value);
    assert.equal(e.message, message);
    return true;
  };
  const assertFailedWith = async (dependant) => {
    assertIsFailed(dependant);
    // get() rejects (never throws), getNow() throws the ExecutionError, toString() does not throw.
    let got;
    assert.doesNotThrow(() => {
      got = dependant.get();
    });
    await assert.rejects(got, check);
    assert.throws(() => dependant.getNow(null), check);
    assert.equal(dependant.toString(), `KafkaFuture{value=null,exception=${shown},done=${value !== null}}`);
  };
  for (const via of ['thenApply', 'whenComplete']) {
    test(`a ${via} that throws ${what} fails its dependant; the source's other waiters still settle`, async () => {
      const src = newFuture();
      const before = src.get();
      const dependant = src[via](() => {
        throw value;
      });
      const waiting = dependant.get();
      const sibling = src.thenApply((v) => v + 1);
      const after = src.get();
      assert.equal(complete(src, 1), true);
      assert.equal(await before, 1);
      assert.equal(await after, 1);
      assert.equal(sibling.getNow(null), 2);
      await assert.rejects(waiting, check);
      await assertFailedWith(dependant);
    });
    test(`a ${via} on a done source whose function throws ${what} returns its failed dependant`, async () => {
      let dependant;
      assert.doesNotThrow(() => {
        dependant = KafkaFuture.completedFuture(1)[via](() => {
          throw value;
        });
      });
      await assertFailedWith(dependant);
    });
  }
}

test("an ExecutionError's message for a null cause is the (cause) form's, whether thrown or built", async () => {
  const future = KafkaFuture.completedFuture(1).thenApply(() => {
    throw null;
  });
  await assert.rejects(future.get(), (e) => e.message === new ExecutionError({ cause: null }).message && e.message === '');
});

// Java's KafkaFutureImpl: an explicit completion inside another future's
// function runs its own postComplete, so its dependants, and an allOf over it,
// are done when it returns.
test('cancel() inside a function completes the cancelled future\'s dependants and allOf before it returns', () => {
  for (const via of ['thenApply', 'whenComplete']) {
    const src = newFuture();
    const other = newFuture();
    const otherDep = other.thenApply((x) => x);
    const otherDeeper = otherDep.whenComplete(() => {});
    const otherAll = KafkaFuture.allOf([other]);
    const seen = [];
    src[via](() => {
      seen.push(other.cancel(true));
      seen.push(otherDep.isDone(), otherDep.isCancelled());
      seen.push(otherDeeper.isDone(), otherDeeper.isCancelled());
      seen.push(otherAll.isDone(), otherAll.isCancelled());
    });
    complete(src, 1);
    assert.deepEqual(seen, [true, true, true, true, true, true, true], via);
  }
});

// Java's allOf relays on its sources' results: it is done once both are,
// even when the canceller is a waiter on one source registered before it.
test('allOf is done once its last source is, whichever waiter completes that source', () => {
  for (const before of [true, false]) {
    const s1 = newFuture();
    const x = s1.thenApply((v) => v);
    const y = newFuture();
    let all = null;
    const seen = [];
    const canceller = () => {
      seen.push(y.cancel(true), all.isDone(), all.isCancelled());
    };
    if (before) x.whenComplete(canceller);
    all = KafkaFuture.allOf([x, y]);
    if (!before) x.whenComplete(canceller);
    complete(s1, 1);
    assert.deepEqual(seen, [true, true, true], before ? 'the canceller registered before allOf' : 'after');
  }
  // The first failure in the sources' order, whatever the completion order.
  const a = newFuture();
  const b = newFuture();
  const both = KafkaFuture.allOf([a, b]);
  completeExceptionally(b, new Error('b'));
  assert.equal(both.isDone(), false);
  completeExceptionally(a, new Error('a'));
  assert.throws(() => both.getNow(null), (e) => e.cause.message === 'a');
});

test('internalComplete inside a function completes its future\'s dependants before it returns', () => {
  const src = newFuture();
  const other = newFuture();
  const otherDep = other.thenApply((x) => x + 1);
  let seen = null;
  src.thenApply(() => {
    complete(other, 1);
    seen = otherDep.getNow(-1);
  });
  complete(src, 0);
  assert.equal(seen, 2);
});

test('cancelling the head of a chain of 100,000 dependants cancels them all without overflowing the stack', async () => {
  const head = newFuture();
  let tail = head;
  for (let i = 0; i < 100_000; i++) tail = tail.thenApply((v) => v + 1);
  assert.equal(head.cancel(true), true);
  await assertIsCancelled(tail);
});

test('a chain of 100,000 dependants completes without overflowing the stack, as Java\'s does', async () => {
  const src = newFuture();
  let tail = src;
  for (let i = 0; i < 100_000; i++) tail = tail.thenApply((v) => v + 1);
  const end = tail.get();
  assert.equal(complete(src, 0), true);
  assert.equal(tail.getNow(null), 100_000);
  assert.equal(await end, 100_000);
  // A failure goes down the whole chain the same way.
  const failing = newFuture();
  let last = failing;
  for (let i = 0; i < 100_000; i++) last = last.whenComplete(() => {});
  completeExceptionally(failing, new Error('deep'));
  await awaitAndAssertFailure(last, Error, 'deep');
});

test('cancelling a dependant cancels it and its own dependants, never its source', async () => {
  const source = newFuture();
  const dependant = source.thenApply((v) => v * 2);
  const grandchild = dependant.whenComplete(() => {});
  const sibling = source.thenApply((v) => v * 3);
  assert.equal(dependant.cancel(true), true);
  // Java's CompletableFuture.cancel answers isCancelled(): true again, for a future cancelled on its own.
  assert.equal(dependant.cancel(false), true);
  await assertIsCancelled(grandchild);
  assertIsNotCompleted(source);
  complete(source, 2);
  assert.equal(sibling.getNow(null), 6);
  await assertIsCancelled(dependant);
});

test('allOf fails with the first failure once every future completed; a cancellation cancels it', async () => {
  const a = newFuture();
  const b = newFuture();
  const all = KafkaFuture.allOf([a, b]);
  completeExceptionally(a, new Error('first'));
  assert.equal(all.isDone(), false);
  completeExceptionally(b, new Error('second'));
  await awaitAndAssertFailure(all, Error, 'first');

  const c = newFuture();
  const cancelled = KafkaFuture.allOf([c, KafkaFuture.completedFuture(1)]);
  c.cancel(true);
  await assertIsCancelled(cancelled);
});

test('a function that throws a CancellationError cancels the dependant, as Java does', async () => {
  const future = KafkaFuture.completedFuture(1);
  const dependant = future.thenApply(() => {
    throw new CancellationError('stop');
  });
  await assertIsCancelled(dependant);
  await awaitAndAssertCancelled(dependant, 'stop');
});

test('CancellationError is Java\'s CancellationException: an IllegalStateError', () => {
  const e = new CancellationError('m');
  assert.ok(e instanceof IllegalStateError);
  assert.equal(e.name, 'CancellationError');
  assert.equal(e.message, 'm');
  assert.equal(new CancellationError().message, '');
  assert.throws(() => new CancellationError({ cause: e }), { name: 'IllegalArgumentError', message: 'CancellationError() takes one of (), (message); got (cause)' });
});

test('toCompletionStage settles with the future: the value, or the failure itself', async () => {
  const ok = newFuture();
  const okStage = ok.toCompletionStage();
  complete(ok, 'v');
  assert.equal(await okStage, 'v');
  assert.equal(await KafkaFuture.completedFuture(3).toCompletionStage(), 3);
  const failed = newFuture();
  const failure = new Error('raw');
  const failedStage = failed.toCompletionStage();
  completeExceptionally(failed, failure);
  await assert.rejects(failedStage, (e) => e === failure);
  const cancelled = newFuture();
  cancelled.cancel(true);
  await assert.rejects(cancelled.toCompletionStage(), CancellationError);
});

// --- The timed get ------------------------------------------------------------

test('get(timeout) resolves when the future completes in time, and clears its timer', async () => {
  const future = newFuture();
  const got = future.get(60_000);
  const timers = () => process.getActiveResourcesInfo().filter((r) => r === 'Timeout').length;
  const before = timers();
  complete(future, 'v');
  assert.equal(await got, 'v');
  assert.equal(timers(), before - 1);
});

test('a timeout above the 2^31 - 1 ms a Node timer holds still waits, with no overflow warning', async () => {
  const warnings = [];
  const onWarning = (w) => warnings.push(w.name);
  process.on('warning', onWarning);
  try {
    for (const timeout of [2 ** 31, 2 ** 40, Number.MAX_SAFE_INTEGER, Infinity]) {
      const future = newFuture();
      const got = future.get(timeout);
      await new Promise((resolve) => setTimeout(resolve, 20));
      assert.equal(future.isDone(), false);
      complete(future, timeout);
      assert.equal(await got, timeout);
    }
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(warnings, []);
  } finally {
    process.off('warning', onWarning);
  }
});

test('a timed-out get leaves the future pending, and a later get still answers', async () => {
  const future = newFuture();
  await assert.rejects(future.get(10), (e) => e.constructor === TimeoutError && e.message === '');
  assert.equal(future.isDone(), false);
  const later = future.get();
  complete(future, 'late');
  assert.equal(await later, 'late');
});

// --- Unhandled rejections --------------------------------------------------------

// node:test fails a test on any unhandled rejection, so the check runs in a
// child process of its own.
test('a failed future nobody waits on, and its dependants, raise no unhandled rejection', () => {
  const script = `
    const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
    const unhandled = [];
    process.on('unhandledRejection', (reason) => unhandled.push(reason.constructor.name));
    const future = KafkaFuture.internalCreate(false);
    future.thenApply((v) => v);
    future.whenComplete(() => {});
    KafkaFuture.allOf([future]).thenApply((v) => v);
    future.internalComplete(null, new Error('unread'));
    setTimeout(() => {
      const quiet = unhandled.length;
      // A get() Promise the caller drops does raise one, as any rejected Promise.
      void future.get().then(() => {});
      setTimeout(() => process.stdout.write(JSON.stringify([quiet, unhandled])), 10);
    }, 10);
  `;
  const r = spawnSync(process.execPath, ['-e', script], { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8' });
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(JSON.parse(r.stdout), [0, ['ExecutionError']]);
});

// The cost of completing a record's future that something waits on: the
// delivery path of a caller that reads its futures (a get(), a thenApply, a
// toCompletionStage()) rather than a send callback. Completing such a future
// allocates nothing, so a million completions run no minor GC. They run in a
// child, with --expose-gc, so the heap is collected before the completions and
// only they are measured.
const GC_SCRIPT = `
const { KafkaFuture } = require('@confluentinc/kafka-javascript/common');
const { PerformanceObserver, constants } = require('node:perf_hooks');
const N = 1_000_000;
const futures = new Array(N);
for (let i = 0; i < N; i++) {
  const f = KafkaFuture.internalCreate(true);
  f.get();
  futures[i] = f;
}
let minor = 0;
const observer = new PerformanceObserver((list) => {
  for (const e of list.getEntries()) if (e.detail && e.detail.kind === constants.NODE_PERFORMANCE_GC_MINOR) minor++;
});
observer.observe({ entryTypes: ['gc'] });
global.gc();
setTimeout(() => {
  minor = 0;
  const start = process.hrtime.bigint();
  for (let i = 0; i < N; i++) futures[i].internalComplete(i, null);
  const ns = Number(process.hrtime.bigint() - start) / N;
  // The GC entries are delivered asynchronously.
  setTimeout(() => {
    observer.disconnect();
    process.stdout.write(JSON.stringify({ ns, minor }));
  }, 50);
}, 50);
`;

test("completing a million record futures, each with a get() waiting, runs no minor GC", (t) => {
  const r = spawnSync(process.execPath, ['--expose-gc', '-e', GC_SCRIPT], { cwd: path.join(__dirname, '..', '..', '..'), encoding: 'utf8', timeout: 120_000 });
  assert.equal(r.status, 0, r.stderr);
  const { ns, minor } = JSON.parse(r.stdout);
  t.diagnostic(`${ns.toFixed(1)} ns per completion`);
  assert.equal(minor, 0, `${minor} minor GC(s) while completing; ${ns.toFixed(1)} ns per completion`);
});
