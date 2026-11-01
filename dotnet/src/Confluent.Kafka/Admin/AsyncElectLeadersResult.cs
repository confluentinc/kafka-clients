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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.ElectLeaders"/> — Java's
/// <c>org.apache.kafka.clients.admin.ElectLeadersResult</c>: one future over a map of the
/// per-partition outcomes.
/// </summary>
/// <remarks>
/// A per-partition failure is a map value, not a faulted future, as in Java, whose map values are
/// <c>Optional&lt;Throwable&gt;</c>. There is one future, as in Java, because a request with no
/// partitions elects every partition in the cluster, so the keys come from the response.
/// </remarks>
/// <seealso cref="ElectLeadersResult"/>
public sealed class AsyncElectLeadersResult
{
    private static readonly Func<IReadOnlyDictionary<TopicPartition, KafkaException?>, object?> s_firstFailure =
        ElectLeadersResult.FirstFailure;

    private readonly KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>> _partitions;
    private readonly SafeAdminResultHandle _native;

    private AsyncElectLeadersResult(
        KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>> partitions, SafeAdminResultHandle native)
    {
        _partitions = partitions;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// The partitions an election was attempted for, each mapped to its outcome — Java's
    /// <c>partitions()</c>.
    /// </summary>
    /// <returns>
    /// A future yielding the map. A <see langword="null"/> value means that partition's election
    /// succeeded (Java's empty <c>Optional</c>); a non-null value is that partition's own failure and
    /// does not fault this future, which faults only when the election could not be run at all.
    /// </returns>
    /// <remarks>Every call returns the same future, as Java's accessor returns its stored field.</remarks>
    public AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, KafkaException?>> Partitions() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, KafkaException?>>(_partitions);

    /// <summary>
    /// Completes when every partition's election succeeded, and faults with the first
    /// failure among them otherwise — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole election.</returns>
    /// <remarks>
    /// The first failure is the first in the order the core delivers the entries, by topic then
    /// partition; Java iterates a <c>HashMap</c> and promises no order. Every call returns a new
    /// future, as Java's <c>all()</c> does.
    /// </remarks>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(_partitions.ThenApply(s_firstFailure));

    internal static AsyncElectLeadersResult OfClient(
        (KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>> Partitions, SafeAdminResultHandle Result)
            native) =>
        new AsyncElectLeadersResult(native.Partitions, native.Result);
}
