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
/// The result of <see cref="IAdmin.DeleteRecords(IReadOnlyDictionary{TopicPartition, RecordsToDelete}, DeleteRecordsOptions)"/> — Java's
/// <c>org.apache.kafka.clients.admin.DeleteRecordsResult</c>: one future per topic partition.
/// </summary>
/// <seealso cref="AsyncDeleteRecordsResult"/>
public sealed class DeleteRecordsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, KafkaFuture<DeletedRecords>> _futures;
    private readonly IReadOnlyList<KafkaFutureImpl>? _nativeFutures;
    private readonly SafeAdminResultHandle? _native;

    /// <summary>Creates a result over the given futures, keyed by topic partition.</summary>
    /// <param name="futures">The future of each partition's deleted records. The map is copied.</param>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is <see langword="null"/>.</exception>
    public DeleteRecordsResult(IReadOnlyDictionary<TopicPartition, KafkaFuture<DeletedRecords>> futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        Dictionary<TopicPartition, KafkaFuture<DeletedRecords>> copy =
            new Dictionary<TopicPartition, KafkaFuture<DeletedRecords>>(futures.Count);
        foreach (KeyValuePair<TopicPartition, KafkaFuture<DeletedRecords>> entry in futures)
        {
            copy.Add(entry.Key, entry.Value);
        }

        _futures = copy;
    }

    private DeleteRecordsResult(
        IReadOnlyDictionary<TopicPartition, KafkaFuture<DeletedRecords>> lowWatermarks,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
    {
        _futures = lowWatermarks;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// One future per requested topic partition, each carrying that partition's own
    /// <see cref="DeletedRecords"/> or its own failure — Java's <c>lowWatermarks()</c>.
    /// </summary>
    /// <returns>One future per requested topic partition, each carrying that partition's own
    /// <see cref="DeletedRecords"/> or its own failure.</returns>
    public IReadOnlyDictionary<TopicPartition, KafkaFuture<DeletedRecords>> LowWatermarks() => _futures;

    /// <summary>
    /// Completes when every partition's deletion has succeeded, and fails with the first failure
    /// to complete if any partition failed — Java's <c>all()</c>, which carries no watermarks: read
    /// them from <see cref="LowWatermarks()"/>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public KafkaFuture All() =>
        _nativeFutures is not null
            ? new KafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures))
            : SyncAdminFutures.AllOf(_futures.Values);

    internal static DeleteRecordsResult OfClient(
        (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<DeletedRecords>> LowWatermarks, SafeAdminResultHandle Result) native)
    {
        Dictionary<TopicPartition, KafkaFuture<DeletedRecords>> lowWatermarks =
            new Dictionary<TopicPartition, KafkaFuture<DeletedRecords>>(native.LowWatermarks.Count);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.LowWatermarks.Count);
        foreach (KeyValuePair<TopicPartition, KafkaFutureImpl<DeletedRecords>> entry in native.LowWatermarks)
        {
            lowWatermarks.Add(entry.Key, new KafkaFuture<DeletedRecords>(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new DeleteRecordsResult(lowWatermarks, nativeFutures, native.Result);
    }
}
