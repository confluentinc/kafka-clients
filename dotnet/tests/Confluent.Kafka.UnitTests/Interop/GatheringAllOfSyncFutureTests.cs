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
/// The sync gathering all-of, <see cref="GatheringAllOfSyncFuture{TKey, TValue}"/> (M15/P14 S1, PLAN §3.3): all-of
/// semantics — every key waited for in key order, the first failure in key order rethrown as its own instance — then
/// one dictionary, keyed with the given comparer, built once and returned as the same instance on every call. Every
/// call that can block runs on a dedicated thread under a deadline (<see cref="BlockingCall"/>).
/// </summary>
public sealed class GatheringAllOfSyncFutureTests
{
    private const int Callers = 8;

    [Fact]
    public void Get_GathersEveryKeysValue_KeyedWithTheGivenComparer()
    {
        object first = new object();
        object second = new object();
        SyncCompletion<object>[] sources = Pending(2);
        GatheringAllOfSyncFuture<string, object> gathering = Gathering(
            StringComparer.OrdinalIgnoreCase, ("orders", sources[0]), ("payments", sources[1]));

        BlockingCall waiting = BlockingCall.StartGet(() => gathering.Get());
        waiting.AssertBlocked();
        Assert.True(sources[1].TrySetResult(second));
        waiting.AssertBlocked();
        Assert.True(sources[0].TrySetResult(first));

        IReadOnlyDictionary<string, object> gathered =
            Assert.IsAssignableFrom<IReadOnlyDictionary<string, object>>(waiting.Join());
        Assert.Equal(2, gathered.Count);
        Assert.Same(first, gathered["orders"]);
        Assert.Same(second, gathered["payments"]);

        // Keyed with the comparer it was given, not the default one.
        Assert.Same(first, gathered["ORDERS"]);
        Assert.True(gathered.ContainsKey("Payments"));
    }

    [Fact]
    public void Get_ReturnsTheSameDictionaryInstance_OnEveryCall()
    {
        GatheringAllOfSyncFuture<string, object> gathering = Gathering(
            StringComparer.Ordinal, ("a", Completed(new object())), ("b", Completed(new object())));

        object? first = BlockingCall.RunGet(() => gathering.Get());
        Assert.IsAssignableFrom<IReadOnlyDictionary<string, object>>(first);
        Assert.Same(first, BlockingCall.RunGet(() => gathering.Get()));
        Assert.Same(first, BlockingCall.RunGet(() => gathering.Get()));

        // Wait() is Get() with the value discarded: it neither rebuilds nor throws.
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => gathering.Wait()));
        Assert.Same(first, BlockingCall.RunGet(() => gathering.Get()));
    }

    [Fact]
    public void Get_ConcurrentFirstCallers_AllGetTheSameDictionaryInstance()
    {
        SyncCompletion<object>[] sources = Pending(2);
        GatheringAllOfSyncFuture<string, object> gathering = Gathering(
            StringComparer.Ordinal, ("a", sources[0]), ("b", sources[1]));

        BlockingCall[] callers = Enumerable.Range(0, Callers)
            .Select(_ => BlockingCall.StartGet(() => gathering.Get()))
            .ToArray();
        foreach (BlockingCall caller in callers)
        {
            caller.AssertBlocked();
        }

        Assert.True(sources[0].TrySetResult(new object()));
        Assert.True(sources[1].TrySetResult(new object()));

        object?[] gathered = callers.Select(caller => caller.Join()).ToArray();
        Assert.IsAssignableFrom<IReadOnlyDictionary<string, object>>(gathered[0]);
        Assert.All(gathered, dictionary => Assert.Same(gathered[0], dictionary));
    }

    [Fact]
    public void Get_EarlierKeyFailed_StillWaitsForALaterPendingKey_ThenRethrowsTheEarlierFailure()
    {
        KafkaException cause = new KafkaException(69, "The group id does not exist.", isRetriable: false);
        SyncCompletion<object>[] sources = Pending(3);
        GatheringAllOfSyncFuture<string, object> gathering = Gathering(
            StringComparer.Ordinal, ("g1", sources[0]), ("g2", sources[1]), ("g3", sources[2]));

        Assert.True(sources[0].TrySetException(cause));
        BlockingCall waiting = BlockingCall.StartGet(() => gathering.Get());
        waiting.AssertBlocked();
        Assert.False(gathering.IsDone);

        Assert.True(sources[2].TrySetResult(new object()));
        waiting.AssertBlocked();
        Assert.True(sources[1].TrySetResult(new object()));

        KafkaException thrown = Assert.IsType<KafkaException>(waiting.Join());
        Assert.Same(cause, thrown);
        Assert.Equal("The group id does not exist.", thrown.Message);

        // Memoized only on success: a failed gathering rethrows the same instance on every call.
        Assert.Same(cause, BlockingCall.RunGet(() => gathering.Get()));
    }

    [Fact]
    public void Get_TwoKeysFail_RethrowsTheFirstInKeyOrder_NotTheFirstInTime()
    {
        KafkaException earlierCause = new KafkaException(15, "Key g1: the coordinator is not available.", isRetriable: true);
        KafkaException laterCause = new KafkaException(16, "Key g3: this is not the correct coordinator.", isRetriable: true);
        SyncCompletion<object>[] sources = Pending(3);
        GatheringAllOfSyncFuture<string, object> gathering = Gathering(
            StringComparer.Ordinal, ("g1", sources[0]), ("g2", sources[1]), ("g3", sources[2]));

        BlockingCall waiting = BlockingCall.StartGet(() => gathering.Get());
        waiting.AssertBlocked();

        Assert.True(sources[2].TrySetException(laterCause));
        Assert.True(sources[1].TrySetResult(new object()));
        Assert.True(sources[0].TrySetException(earlierCause));

        Assert.Same(earlierCause, waiting.Join());
    }

    [Fact]
    public void Get_KeysEqualUnderTheComparer_TheLaterOneWins_WithoutThrowing()
    {
        // The indexer, not Add — Java's put: a key equal to an earlier one replaces it.
        object earlierValue = new object();
        object laterValue = new object();
        GatheringAllOfSyncFuture<string, object> gathering = Gathering(
            StringComparer.OrdinalIgnoreCase, ("topic", Completed(earlierValue)), ("TOPIC", Completed(laterValue)));

        IReadOnlyDictionary<string, object> gathered =
            Assert.IsAssignableFrom<IReadOnlyDictionary<string, object>>(BlockingCall.RunGet(() => gathering.Get()));
        Assert.Single(gathered);
        Assert.Same(laterValue, gathered["topic"]);
    }

    [Fact]
    public void IsDone_IsFalseUntilEveryKeyIsDone_EvenAfterAFailure()
    {
        SyncCompletion<object>[] sources = Pending(2);
        GatheringAllOfSyncFuture<string, object> gathering = Gathering(
            StringComparer.Ordinal, ("a", sources[0]), ("b", sources[1]));

        Assert.False(gathering.IsDone);
        Assert.True(sources[1].TrySetException(new KafkaException(-1, "Key b failed.", isRetriable: false)));
        Assert.False(gathering.IsDone);
        Assert.True(sources[0].TrySetResult(new object()));
        Assert.True(gathering.IsDone);
    }

    [Fact]
    public void Empty_IsDone_AndGathersAnEmptyDictionary()
    {
        GatheringAllOfSyncFuture<string, object> gathering = Gathering(StringComparer.Ordinal);

        Assert.True(gathering.IsDone);
        IReadOnlyDictionary<string, object> gathered =
            Assert.IsAssignableFrom<IReadOnlyDictionary<string, object>>(BlockingCall.RunGet(() => gathering.Get()));
        Assert.Empty(gathered);
        Assert.Same(gathered, BlockingCall.RunGet(() => gathering.Get()));
    }

    private static GatheringAllOfSyncFuture<string, object> Gathering(
        IEqualityComparer<string> comparer, params (string Key, SyncFutureSource<object> Source)[] entries) =>
        new GatheringAllOfSyncFuture<string, object>(
            entries.Select(entry => new KeyValuePair<string, SyncFutureSource<object>>(entry.Key, entry.Source)).ToArray(),
            comparer);

    private static SyncCompletion<object>[] Pending(int count) =>
        Enumerable.Range(0, count).Select(_ => new SyncCompletion<object>()).ToArray();

    private static SyncCompletion<object> Completed(object value)
    {
        SyncCompletion<object> completion = new SyncCompletion<object>();
        Assert.True(completion.TrySetResult(value));
        return completion;
    }
}
