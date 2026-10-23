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
/// The sync projection, <see cref="ProjectedSyncFuture{TIn, TOut}"/> (M15/P14 S1, PLAN §3.3): it waits for its
/// source on the calling thread, applies its function there at most once — also under concurrent first callers — and
/// memoizes the value or the exception, while a source failure stays the source's own instance. Every call that can
/// block runs on a dedicated thread under a deadline (<see cref="BlockingCall"/>).
/// </summary>
public sealed class ProjectedSyncFutureTests
{
    private const int Callers = 8;

    [Fact]
    public void Get_WaitsForTheSource_ThenAppliesTheFunction_OnTheCallingThread()
    {
        SyncCompletion<int> source = new SyncCompletion<int>();
        int appliedOnThread = 0;
        ProjectedSyncFuture<int, string> projection = new ProjectedSyncFuture<int, string>(source, value =>
        {
            appliedOnThread = Environment.CurrentManagedThreadId;
            return "value-" + value;
        });

        BlockingCall waiting = BlockingCall.StartGet(() => projection.Get());
        waiting.AssertBlocked();
        Assert.Equal(0, Volatile.Read(ref appliedOnThread));

        Assert.True(source.TrySetResult(42));

        Assert.Equal("value-42", waiting.Join());

        // The caller's own thread applied it: no thread-pool thread, no Task.
        Assert.Equal(waiting.CallingThreadId, appliedOnThread);
    }

    [Fact]
    public void Get_AppliesTheFunctionOnce_AndReturnsTheSameValueInstance_OnEveryCall()
    {
        int applied = 0;
        ProjectedSyncFuture<int, object> projection = new ProjectedSyncFuture<int, object>(Completed(7), _ =>
        {
            Interlocked.Increment(ref applied);
            return new object();
        });

        object? first = BlockingCall.RunGet(() => projection.Get());
        object? second = BlockingCall.RunGet(() => projection.Get());
        object? third = BlockingCall.RunGet(() => projection.Get());

        Assert.NotNull(first);
        Assert.False(first is Exception, $"Get() threw: {first}");
        Assert.Same(first, second);
        Assert.Same(first, third);
        Assert.Equal(1, Volatile.Read(ref applied));
    }

    [Fact]
    public void Get_ConcurrentFirstCallers_OnACompletedSource_ApplyTheFunctionOnce()
    {
        // The function holds the lock until every other caller has parked waiting for it: a projection that let two
        // callers apply it would have each of them inside the function at once, and the count would exceed one.
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        int applied = 0;
        ProjectedSyncFuture<int, object> projection = new ProjectedSyncFuture<int, object>(Completed(1), _ =>
        {
            Interlocked.Increment(ref applied);
            Assert.True(release.Wait(BlockingCall.Deadline), "The test never released the projection.");
            return new object();
        });

        BlockingCall[] callers = Enumerable.Range(0, Callers)
            .Select(_ => BlockingCall.StartGet(() => projection.Get()))
            .ToArray();

        // Every caller is parked — one inside the function, the rest on the projection's lock.
        foreach (BlockingCall caller in callers)
        {
            caller.AssertBlocked();
        }

        release.Set();

        object?[] values = callers.Select(caller => caller.Join()).ToArray();
        Assert.Equal(1, Volatile.Read(ref applied));
        Assert.All(values, value => Assert.Same(values[0], value));
        Assert.False(values[0] is Exception, $"Get() threw: {values[0]}");
    }

    [Fact]
    public void Get_ConcurrentCallersWaitingOnAPendingSource_ApplyTheFunctionOnce_AndShareTheValue()
    {
        SyncCompletion<int> source = new SyncCompletion<int>();
        int applied = 0;
        ProjectedSyncFuture<int, object> projection = new ProjectedSyncFuture<int, object>(source, _ =>
        {
            Interlocked.Increment(ref applied);
            return new object();
        });

        BlockingCall[] callers = Enumerable.Range(0, Callers)
            .Select(_ => BlockingCall.StartGet(() => projection.Get()))
            .ToArray();
        foreach (BlockingCall caller in callers)
        {
            caller.AssertBlocked();
        }

        Assert.True(source.TrySetResult(3));

        object?[] values = callers.Select(caller => caller.Join()).ToArray();
        Assert.Equal(1, Volatile.Read(ref applied));
        Assert.All(values, value => Assert.Same(values[0], value));
    }

    [Fact]
    public void Get_FunctionThrows_RethrowsTheSameInstance_OnEveryCall_AppliedOnce()
    {
        int applied = 0;
        ProjectedSyncFuture<int, string> projection = new ProjectedSyncFuture<int, string>(Completed(5), _ =>
        {
            Interlocked.Increment(ref applied);
            throw new KafkaException(-1, "The projection failed: a key the operation did not request.", isRetriable: false);
        });

        KafkaException first = Assert.IsType<KafkaException>(BlockingCall.RunGet(() => projection.Get()));
        Assert.Equal("The projection failed: a key the operation did not request.", first.Message);
        for (int call = 0; call < 2; call++)
        {
            Assert.Same(first, BlockingCall.RunGet(() => projection.Get()));
        }

        Assert.Same(first, BlockingCall.RunWait(() => projection.Wait()));
        Assert.Equal(1, Volatile.Read(ref applied));
        Assert.True(projection.IsDone);
    }

    [Fact]
    public void Get_SourceFailed_RethrowsTheSourcesOwnInstance_AndNeverAppliesTheFunction()
    {
        KafkaException cause = new KafkaException(3, "This server does not host this topic-partition.", isRetriable: true);
        SyncCompletion<int> source = new SyncCompletion<int>();
        int applied = 0;
        ProjectedSyncFuture<int, string> projection = new ProjectedSyncFuture<int, string>(source, value =>
        {
            Interlocked.Increment(ref applied);
            return value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        });

        BlockingCall waiting = BlockingCall.StartGet(() => projection.Get());
        waiting.AssertBlocked();
        Assert.True(source.TrySetException(cause));

        Assert.Same(cause, waiting.Join());
        for (int call = 0; call < 2; call++)
        {
            KafkaException thrown = Assert.IsType<KafkaException>(BlockingCall.RunGet(() => projection.Get()));
            Assert.Same(cause, thrown);
            Assert.Equal("This server does not host this topic-partition.", thrown.Message);
        }

        Assert.Equal(0, Volatile.Read(ref applied));
    }

    [Theory]
    [InlineData("result")]
    [InlineData("exception")]
    public void IsDone_IsTheSources_WithoutApplyingTheFunction(string outcome)
    {
        SyncCompletion<int> source = new SyncCompletion<int>();
        int applied = 0;
        ProjectedSyncFuture<int, int> projection = new ProjectedSyncFuture<int, int>(source, value =>
        {
            Interlocked.Increment(ref applied);
            return value + 1;
        });

        Assert.False(projection.IsDone);

        Assert.True(outcome == "result"
            ? source.TrySetResult(1)
            : source.TrySetException(new KafkaException(-1, "The operation failed.", isRetriable: false)));

        // Done as soon as the source is, before anyone asked for the value: the projection adds no wait of its own.
        Assert.True(projection.IsDone);
        Assert.Equal(0, Volatile.Read(ref applied));
    }

    [Fact]
    public void Wait_IsGetWithTheValueDiscarded()
    {
        SyncCompletion<int> source = new SyncCompletion<int>();
        int applied = 0;
        ProjectedSyncFuture<int, int> projection = new ProjectedSyncFuture<int, int>(source, value =>
        {
            Interlocked.Increment(ref applied);
            return value * 2;
        });

        BlockingCall waiting = BlockingCall.StartWait(() => projection.Wait());
        waiting.AssertBlocked();
        Assert.True(source.TrySetResult(21));

        Assert.Same(BlockingCall.Returned, waiting.Join());
        Assert.Equal(42, BlockingCall.RunGet(() => projection.Get()));
        Assert.Equal(1, Volatile.Read(ref applied));
    }

    [Fact]
    public void Constructor_RejectsANullSourceOrFunction()
    {
        Assert.Equal(
            "source",
            Assert.Throws<ArgumentNullException>(() => new ProjectedSyncFuture<int, int>(null!, value => value)).ParamName);
        Assert.Equal(
            "project",
            Assert.Throws<ArgumentNullException>(() => new ProjectedSyncFuture<int, int>(Completed(1), null!)).ParamName);
    }

    private static SyncCompletion<int> Completed(int value)
    {
        SyncCompletion<int> completion = new SyncCompletion<int>();
        Assert.True(completion.TrySetResult(value));
        return completion;
    }
}
