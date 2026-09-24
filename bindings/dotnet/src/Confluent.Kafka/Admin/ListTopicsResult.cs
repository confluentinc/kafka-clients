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
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.ListTopics"/> — the .NET realization of Java's
/// <c>ListTopicsResult</c>: <b>one</b> awaitable over the whole cluster listing, handed
/// back the moment the request is submitted.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>One future, not one per topic — and that is the ABI's shape as much as Java's.</b>
/// Every other admin RPC bound so far pre-registers the caller's keys and resolves one
/// awaitable per key. <c>listTopics</c> cannot: the request carries no key array (the
/// topics are discovered from the response), and
/// <c>kafka_admin_ListTopicsResult_t</c> declares <b>no <c>get_error</c> at all</b>.
/// That absence is the ABI stating the semantics — there is no per-topic failure to
/// attribute, so either the call fails and <em>this one</em> task faults, or the whole
/// map succeeds.
/// </para>
/// <para>
/// ⚠ <b><see cref="Listings"/> and <see cref="Names"/> are projections of the same
/// future, not independent calls.</b> Java derives both from <c>namesToListings()</c>
/// with <c>thenApply(Map::values)</c> / <c>thenApply(Map::keySet)</c>
/// (<c>ListTopicsResult.java:46-55</c>), so the three can never disagree. Three
/// independent requests would be a different shape — and could report three different
/// cluster states.
/// </para>
/// </remarks>
public sealed class ListTopicsResult
{
    private readonly Task<IReadOnlyDictionary<string, TopicListing>> _future;

    /// <summary>
    /// Wraps the single awaitable — Java's package-private
    /// <c>ListTopicsResult(KafkaFuture&lt;Map&lt;String, TopicListing&gt;&gt;)</c>.
    /// </summary>
    internal ListTopicsResult(Task<IReadOnlyDictionary<string, TopicListing>> future)
    {
        _future = future;
    }

    /// <summary>
    /// The topic names mapped to their listings — Java's <c>namesToListings()</c>.
    /// </summary>
    /// <returns>
    /// A task yielding the map, or faulting with the call's own
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <remarks>
    /// This is the underlying future itself, so every call returns the <b>same</b>
    /// <see cref="Task"/> instance — no second task whose fault could go unobserved.
    /// </remarks>
    public Task<IReadOnlyDictionary<string, TopicListing>> NamesToListings() => _future;

    /// <summary>
    /// The listings alone — Java's <c>listings()</c>
    /// (<c>namesToListings().thenApply(Map::values)</c>).
    /// </summary>
    /// <returns>A task yielding the listings.</returns>
    public Task<IReadOnlyCollection<TopicListing>> Listings() =>
        Project(static map => (IReadOnlyCollection<TopicListing>)new List<TopicListing>(map.Values));

    /// <summary>
    /// The topic names alone — Java's <c>names()</c>
    /// (<c>namesToListings().thenApply(Map::keySet)</c>).
    /// </summary>
    /// <returns>A task yielding the names.</returns>
    /// <remarks>
    /// Java returns a <c>Set&lt;String&gt;</c>; <c>IReadOnlySet&lt;T&gt;</c> post-dates
    /// the netstandard2.0 floor, so this is an <c>IReadOnlyCollection&lt;string&gt;</c> —
    /// the same substitution the consumer surface already makes for
    /// <c>Subscription()</c> and friends (CLAUDE.md §3's idiom map). The names are
    /// distinct regardless, because they are the keys of a dictionary.
    /// </remarks>
    public Task<IReadOnlyCollection<string>> Names() =>
        Project(static map => (IReadOnlyCollection<string>)new List<string>(map.Keys));

    /// <summary>
    /// Java's <c>thenApply</c>, in C#: derive a new task from the one future, so a
    /// projection cannot observe a different cluster state than
    /// <see cref="NamesToListings"/> did.
    /// </summary>
    /// <remarks>
    /// An <c>async</c> method rather than <c>ContinueWith</c>: awaiting rethrows the
    /// underlying exception itself, so a faulted projection carries the same
    /// <see cref="KafkaException"/> as the source rather than an extra
    /// <see cref="AggregateException"/> wrapper. Each call allocates one task, exactly as
    /// each Java <c>thenApply</c> allocates one <c>KafkaFuture</c> — and admin is
    /// batch/administrative, with no per-record path to budget (<c>admin-client.md</c>
    /// §10).
    /// </remarks>
    private async Task<TResult> Project<TResult>(
        Func<IReadOnlyDictionary<string, TopicListing>, TResult> select)
    {
        IReadOnlyDictionary<string, TopicListing> map = await _future.ConfigureAwait(false);
        return select(map);
    }
}
