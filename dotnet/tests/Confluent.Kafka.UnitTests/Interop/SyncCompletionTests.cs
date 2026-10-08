// Copyright 2025 Confluent Inc.
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

using System;
using System.Linq;
using System.Threading;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The latch behind the sync send's <see cref="KafkaFuture{T}"/>, <see cref="SyncCompletion{T}"/> (M11/P4.2 S1,
/// PLAN §7 N4–N6): <see cref="SyncCompletion{T}.Get"/> blocks every waiter until the first completion and then
/// releases all of them with the same outcome, the first completion wins in either order, and a completion racing a
/// <see cref="SyncCompletion{T}.Get"/> is never lost. Waiters run on dedicated threads (they block by design, so a
/// thread-pool thread would only add injection delay), and every wait is bounded by the suite's 30 s deadline, so a
/// lost wakeup fails the run instead of hanging it.
/// </summary>
public sealed class SyncCompletionTests
{
    // How many misses a hot spin takes in a row before it yields once (see OnMiss).
    private const int HotSpins = 1 << 16;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    // How long a blocked waiter must stay blocked before the latch is completed.
    private static readonly TimeSpan s_stillBlocked = TimeSpan.FromMilliseconds(100);

    [Theory]
    [InlineData("result")]
    [InlineData("exception")]
    public void Get_BlocksUntilCompleted_ThenReleasesEveryWaiter(string outcome)
    {
        const int Waiters = 8;

        SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>();
        RecordMetadata metadata = NewMetadata(0);
        KafkaException cause = new KafkaException(7, "The send failed.", isRetriable: false);
        object?[] outcomes = new object?[Waiters];
        int returned = 0;

        Thread[] waiters = Enumerable.Range(0, Waiters)
            .Select(index => StartThread(() =>
            {
                try
                {
                    outcomes[index] = completion.Get();
                }
                catch (Exception exception)
                {
                    outcomes[index] = exception;
                }

                Interlocked.Increment(ref returned);
            }))
            .ToArray();

        // Every waiter reaches the blocking wait before the latch completes, so this exercises the slow path, not
        // the already-done fast path.
        Assert.True(
            SpinWait.SpinUntil(() => waiters.All(IsBlocked), s_deadline),
            "The waiters did not all block in Get().");
        Assert.False(waiters[0].Join(s_stillBlocked));
        Assert.Equal(0, Volatile.Read(ref returned));

        bool won = outcome == "result" ? completion.TrySetResult(metadata) : completion.TrySetException(cause);
        Assert.True(won);

        JoinAll(waiters);
        Assert.Equal(Waiters, Volatile.Read(ref returned));
        object expected = outcome == "result" ? metadata : cause;
        Assert.All(outcomes, observed => Assert.Same(expected, observed));
    }

    [Fact]
    public void FirstCompletionWins_BothOrders()
    {
        RecordMetadata first = NewMetadata(1);
        RecordMetadata second = NewMetadata(2);
        KafkaException cause = new KafkaException(7, "The first failure.", isRetriable: false);
        KafkaException late = new KafkaException(8, "A later failure.", isRetriable: false);

        // A result first: a later exception and a later result are both refused.
        SyncCompletion<RecordMetadata> resultFirst = new SyncCompletion<RecordMetadata>();
        Assert.True(resultFirst.TrySetResult(first));
        Assert.False(resultFirst.TrySetException(late));
        Assert.False(resultFirst.TrySetResult(second));
        Assert.Same(first, resultFirst.Get());
        Assert.Same(first, resultFirst.Get());

        // An exception first: a later result and a later exception are both refused.
        SyncCompletion<RecordMetadata> exceptionFirst = new SyncCompletion<RecordMetadata>();
        Assert.True(exceptionFirst.TrySetException(cause));
        Assert.False(exceptionFirst.TrySetResult(first));
        Assert.False(exceptionFirst.TrySetException(late));
        Assert.Same(cause, Assert.Throws<KafkaException>(() => exceptionFirst.Get()));
        Assert.Same(cause, Assert.Throws<KafkaException>(() => exceptionFirst.Get()));

        // Concurrent completers, results and exceptions mixed: exactly one wins, and Get() reports that one.
        const int Rounds = 100;
        const int Completers = 4;
        for (int round = 0; round < Rounds; round++)
        {
            SyncCompletion<RecordMetadata> raced = new SyncCompletion<RecordMetadata>();
            object[] offered = Enumerable.Range(0, Completers)
                .Select(index => index % 2 == 0
                    ? (object)NewMetadata(index)
                    : new KafkaException(index, "Completer " + index + " failed.", isRetriable: false))
                .ToArray();
            bool[] wins = new bool[Completers];
            using Barrier start = new Barrier(Completers);

            Thread[] completers = Enumerable.Range(0, Completers)
                .Select(index => StartThread(() =>
                {
                    if (!start.SignalAndWait(s_deadline))
                    {
                        return;
                    }

                    wins[index] = offered[index] is RecordMetadata metadata
                        ? raced.TrySetResult(metadata)
                        : raced.TrySetException((KafkaException)offered[index]);
                }))
                .ToArray();

            JoinAll(completers);
            int winner = Assert.Single(Enumerable.Range(0, Completers), index => wins[index]);
            object observed = Outcome(raced);
            Assert.Same(offered[winner], observed);
        }
    }

    [Fact]
    public void SetRacesGet_NoLostWakeup()
    {
        const int Iterations = 1000;

        for (int iteration = 0; iteration < Iterations; iteration++)
        {
            SyncCompletion<RecordMetadata> completion = new SyncCompletion<RecordMetadata>();
            bool fail = iteration % 2 == 1;
            object offered = fail
                ? new KafkaException(iteration, "Iteration " + iteration + " failed.", isRetriable: false)
                : NewMetadata(iteration);
            object? observed = null;
            using Barrier start = new Barrier(2);

            // Get() on one thread, the completion on this one, released together so the two race.
            Thread getter = StartThread(() =>
            {
                if (start.SignalAndWait(s_deadline))
                {
                    observed = Outcome(completion);
                }
            });

            Assert.True(start.SignalAndWait(s_deadline), $"Iteration {iteration}: the getter thread did not start.");
            bool won = fail
                ? completion.TrySetException((KafkaException)offered)
                : completion.TrySetResult((RecordMetadata)offered);
            Assert.True(won);

            Assert.True(getter.Join(s_deadline), $"Iteration {iteration}: Get() was never released — a lost wakeup.");
            Assert.Same(offered, observed);
        }

        // The other half of the race: a completion against Get()'s lock-free fast path. A completer that published
        // `_done` before the value or the error (mutation M2′) mostly survived the loop above, which seldom lands a
        // getter's read BETWEEN the completer's two stores: a fresh thread and a barrier per iteration leave the two
        // far apart. Which release shape lands in that window more often differs by runtime, so both run.
        AssertTheFastPathNeverSeesDoneWithoutTheOutcome(getterSpinsOnIsDone: false);
        AssertTheFastPathNeverSeesDoneWithoutTheOutcome(getterSpinsOnIsDone: true);
    }

    // One long-lived getter thread and this completer thread, in lockstep over fresh latches: the completer publishes a
    // latch, waits for the getter to finish the previous round, releases it and completes the latch at once. The getter
    // then calls Get() straight away or, with getterSpinsOnIsDone, first spins on IsDone — the volatile read Get()'s
    // fast path makes. A read that lands between a reordered completer's two stores sees the latch done with its
    // outcome still missing (null). Results and exceptions alternate.
    private static void AssertTheFastPathNeverSeesDoneWithoutTheOutcome(bool getterSpinsOnIsDone)
    {
        const int Rounds = 20_000;

        object[] offered = Enumerable.Range(0, Rounds)
            .Select(round => round % 2 == 0
                ? (object)NewMetadata(round)
                : new KafkaException(round, "Round " + round + " failed.", isRetriable: false))
            .ToArray();
        SyncCompletion<RecordMetadata>?[] latches = new SyncCompletion<RecordMetadata>?[Rounds];
        object?[] observed = new object?[Rounds];
        int released = -1;
        int finished = -1;
        Exception? getterFailure = null;
        System.Diagnostics.Stopwatch elapsed = System.Diagnostics.Stopwatch.StartNew();

        Thread getter = StartThread(() =>
        {
            try
            {
                for (int round = 0; round < Rounds; round++)
                {
                    int misses = 0;
                    while (Volatile.Read(ref released) != round)
                    {
                        OnMiss(ref misses, elapsed, "the completer to release a round");
                    }

                    // Published before the release, so the acquiring read above makes it visible.
                    SyncCompletion<RecordMetadata> latch = latches[round]!;
                    if (getterSpinsOnIsDone)
                    {
                        while (!latch.IsDone)
                        {
                            OnMiss(ref misses, elapsed, "the completer to complete a latch");
                        }
                    }

                    observed[round] = Outcome(latch);
                    Volatile.Write(ref finished, round);
                }
            }
            catch (Exception exception)
            {
                Volatile.Write(ref getterFailure, exception);
            }
        });

        for (int round = 0; round < Rounds; round++)
        {
            SyncCompletion<RecordMetadata> latch = new SyncCompletion<RecordMetadata>();
            latches[round] = latch;
            int misses = 0;
            while (Volatile.Read(ref finished) != round - 1 && Volatile.Read(ref getterFailure) is null)
            {
                OnMiss(ref misses, elapsed, "the getter to finish a round");
            }

            Volatile.Write(ref released, round);
            bool won = offered[round] is RecordMetadata metadata
                ? latch.TrySetResult(metadata)
                : latch.TrySetException((KafkaException)offered[round]);
            Assert.True(won);
        }

        Assert.True(getter.Join(s_deadline), "The fast-path getter thread did not finish.");
        Assert.Null(Volatile.Read(ref getterFailure));
        int stale = Enumerable.Range(0, Rounds).Count(round => !ReferenceEquals(offered[round], observed[round]));
        Assert.True(
            stale == 0,
            $"{stale} of {Rounds} Get() calls (getter spinning on IsDone: {getterSpinsOnIsDone}) saw the latch done " +
            "without its outcome.");
    }

    // A miss of a hot spin: after HotSpins misses in a row, yield once — so a single-core machine still makes
    // progress — and fail past the deadline instead of spinning forever.
    private static void OnMiss(ref int misses, System.Diagnostics.Stopwatch elapsed, string waitingFor)
    {
        if (++misses < HotSpins)
        {
            return;
        }

        misses = 0;
        if (elapsed.Elapsed > s_deadline)
        {
            throw new TimeoutException($"Timed out waiting for {waitingFor}.");
        }

        Thread.Yield();
    }

    // Get()'s outcome as an object: the value, or the exception it rethrew.
    private static object Outcome(SyncCompletion<RecordMetadata> completion)
    {
        try
        {
            return completion.Get();
        }
        catch (Exception exception)
        {
            return exception;
        }
    }

    private static bool IsBlocked(Thread thread) => (thread.ThreadState & ThreadState.WaitSleepJoin) != 0;

    private static Thread StartThread(Action body)
    {
        Thread thread = new Thread(() => body()) { IsBackground = true };
        thread.Start();
        return thread;
    }

    private static void JoinAll(Thread[] threads)
    {
        foreach (Thread thread in threads)
        {
            Assert.True(thread.Join(s_deadline), $"A thread did not finish within {s_deadline}.");
        }
    }

    private static RecordMetadata NewMetadata(long offset) =>
        new RecordMetadata("sync-completion-topic", 0, offset, 1_700_000_000_000L);
}
