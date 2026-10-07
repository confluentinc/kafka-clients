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
