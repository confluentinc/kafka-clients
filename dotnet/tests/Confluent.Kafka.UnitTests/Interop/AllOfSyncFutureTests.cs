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
using System.Collections.Generic;
using System.Linq;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The sync all-of, <see cref="AllOfSyncFuture"/> (M15/P14 S1, PLAN §3.3): it waits for <b>every</b> source in list
/// order — past an earlier failure — and then rethrows the first failure in <b>list</b> order (not in time), as the
/// source's own instance, which is what <c>await Task.WhenAll(...)</c> surfaces. Every call that can block runs on a
/// dedicated thread under a deadline (<see cref="BlockingCall"/>).
/// </summary>
public sealed class AllOfSyncFutureTests
{
    [Fact]
    public void Empty_IsDone_AndWaitReturnsAtOnce()
    {
        AllOfSyncFuture allOf = new AllOfSyncFuture(Array.Empty<SyncFutureSource>());

        Assert.True(allOf.IsDone);
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => allOf.Wait()));
    }

    [Fact]
    public void Wait_BlocksUntilEverySourceHasSucceeded_InAnyCompletionOrder()
    {
        SyncCompletion<bool>[] sources = Pending(3);
        AllOfSyncFuture allOf = new AllOfSyncFuture(sources);

        BlockingCall waiting = BlockingCall.StartWait(() => allOf.Wait());
        waiting.AssertBlocked();

        // Complete out of list order: the last, then the first — the middle one still holds it.
        Assert.True(sources[2].TrySetResult(true));
        Assert.True(sources[0].TrySetResult(true));
        waiting.AssertBlocked();
        Assert.False(allOf.IsDone);

        Assert.True(sources[1].TrySetResult(true));

        Assert.Same(BlockingCall.Returned, waiting.Join());
        Assert.True(allOf.IsDone);
    }

    [Fact]
    public void Wait_EarlierSourceFailed_StillWaitsForALaterPendingSource_ThenRethrowsTheEarlierFailure()
    {
        // "One key fails" (M15/P14 §7.7 M1): an all-of that returned at the first failure would leave a later key's
        // request still in flight when the caller is told the whole operation is over.
        KafkaException cause = new KafkaException(36, "Topic 'orders' already exists.", isRetriable: false);
        SyncCompletion<bool>[] sources = Pending(3);
        AllOfSyncFuture allOf = new AllOfSyncFuture(sources);

        Assert.True(sources[0].TrySetException(cause));
        Assert.True(sources[1].TrySetResult(true));

        BlockingCall waiting = BlockingCall.StartWait(() => allOf.Wait());
        waiting.AssertBlocked();
        Assert.False(allOf.IsDone);

        Assert.True(sources[2].TrySetResult(true));

        KafkaException thrown = Assert.IsType<KafkaException>(waiting.Join());
        Assert.Same(cause, thrown);
        Assert.Equal("Topic 'orders' already exists.", thrown.Message);
        Assert.True(allOf.IsDone);
    }

    [Theory]
    [InlineData(0, 2)]
    [InlineData(1, 2)]
    [InlineData(0, 1)]
    public void Wait_TwoSourcesFail_RethrowsTheFirstInListOrder_NotTheFirstInTime(int earlier, int later)
    {
        // "Two keys fail" (M15/P14 §7.7 M12): the later source in the list fails FIRST in time, so neither "the first
        // failure observed" nor "the last failure in the list" is the right answer — only the first in list order is.
        KafkaException earlierCause = new KafkaException(37, $"Key {earlier}: invalid partition count.", isRetriable: false);
        KafkaException laterCause = new KafkaException(38, $"Key {later}: invalid replication factor.", isRetriable: false);
        SyncCompletion<bool>[] sources = Pending(3);
        AllOfSyncFuture allOf = new AllOfSyncFuture(sources);

        BlockingCall waiting = BlockingCall.StartWait(() => allOf.Wait());
        waiting.AssertBlocked();

        Assert.True(sources[later].TrySetException(laterCause));
        Assert.True(sources[earlier].TrySetException(earlierCause));
        foreach (SyncCompletion<bool> source in sources.Where(source => !source.IsDone))
        {
            Assert.True(source.TrySetResult(true));
        }

        KafkaException thrown = Assert.IsType<KafkaException>(waiting.Join());
        Assert.Same(earlierCause, thrown);
        Assert.Equal($"Key {earlier}: invalid partition count.", thrown.Message);
    }

    [Fact]
    public void Wait_RethrowsTheSameInstance_OnEveryCall()
    {
        KafkaException cause = new KafkaException(29, "Topic authorization failed.", isRetriable: false);
        SyncCompletion<bool>[] sources = Pending(2);
        Assert.True(sources[0].TrySetResult(true));
        Assert.True(sources[1].TrySetException(cause));
        AllOfSyncFuture allOf = new AllOfSyncFuture(sources);

        for (int call = 0; call < 3; call++)
        {
            Assert.Same(cause, BlockingCall.RunWait(() => allOf.Wait()));
        }
    }

    [Fact]
    public void IsDone_IsFalseUntilEverySourceIsDone_EvenAfterAFailure()
    {
        SyncCompletion<bool>[] sources = Pending(2);
        AllOfSyncFuture allOf = new AllOfSyncFuture(sources);

        Assert.False(allOf.IsDone);
        Assert.True(sources[0].TrySetException(new KafkaException(-1, "The first key failed.", isRetriable: false)));
        Assert.False(allOf.IsDone);
        Assert.True(sources[1].TrySetResult(true));
        Assert.True(allOf.IsDone);
    }

    [Fact]
    public void Constructor_CopiesTheList_AndRejectsNull()
    {
        List<SyncFutureSource> list = new List<SyncFutureSource> { Done() };
        AllOfSyncFuture allOf = new AllOfSyncFuture(list);

        // A source added to the caller's list afterwards is not waited for.
        list.Add(new SyncCompletion<bool>());
        Assert.True(allOf.IsDone);
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => allOf.Wait()));

        Assert.Equal("sources", Assert.Throws<ArgumentNullException>(() => new AllOfSyncFuture(null!)).ParamName);
    }

    private static SyncCompletion<bool>[] Pending(int count) =>
        Enumerable.Range(0, count).Select(_ => new SyncCompletion<bool>()).ToArray();

    private static SyncCompletion<bool> Done()
    {
        SyncCompletion<bool> completion = new SyncCompletion<bool>();
        Assert.True(completion.TrySetResult(true));
        return completion;
    }
}
