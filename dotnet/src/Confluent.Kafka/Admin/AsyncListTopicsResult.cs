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

using System.Collections.Generic;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.ListTopics"/> — Java's
/// <c>org.apache.kafka.clients.admin.ListTopicsResult</c>: one future over the whole cluster
/// listing.
/// </summary>
/// <remarks>
/// <see cref="Listings"/> and <see cref="Names"/> are projections of <see cref="NamesToListings"/>,
/// as in Java, so the three never disagree.
/// </remarks>
/// <seealso cref="ListTopicsResult"/>
public sealed class AsyncListTopicsResult
{
    private readonly AsyncKafkaFuture<IReadOnlyDictionary<string, TopicListing>> _future;

    internal AsyncListTopicsResult(
        AsyncKafkaFuture<IReadOnlyDictionary<string, TopicListing>> future, SafeAdminResultHandle? native = null)
    {
        _future = future;
        Native = native;
    }

    internal SafeAdminResultHandle? Native { get; }

    /// <summary>
    /// The topic names mapped to their listings — Java's <c>namesToListings()</c>.
    /// </summary>
    /// <returns>
    /// A future yielding the map, or faulting with the call's own
    /// <see cref="KafkaException"/>.
    /// </returns>
    /// <remarks>Every call returns the same future, as Java's accessor returns its stored field.</remarks>
    public AsyncKafkaFuture<IReadOnlyDictionary<string, TopicListing>> NamesToListings() => _future;

    /// <summary>
    /// The listings alone — Java's <c>listings()</c>.
    /// </summary>
    /// <returns>A future yielding the listings.</returns>
    public AsyncKafkaFuture<IReadOnlyCollection<TopicListing>> Listings() =>
        _future.ThenApply(static map => (IReadOnlyCollection<TopicListing>)new List<TopicListing>(map.Values));

    /// <summary>
    /// The topic names alone — Java's <c>names()</c>.
    /// </summary>
    /// <returns>A future yielding the names.</returns>
    public AsyncKafkaFuture<IReadOnlyCollection<string>> Names() =>
        _future.ThenApply(static map => (IReadOnlyCollection<string>)new List<string>(map.Keys));
}
