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
/// The result of <see cref="IAsyncAdmin.DeleteRecords"/> — Java's
/// <c>org.apache.kafka.clients.admin.DeleteRecordsResult</c>: one future per topic partition.
/// </summary>
/// <seealso cref="DeleteRecordsResult"/>
public sealed class AsyncDeleteRecordsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>> _futures;
    private readonly IReadOnlyList<KafkaFutureImpl>? _nativeFutures;
    private readonly SafeAdminResultHandle? _native;

    // Internal, unlike Java's public constructor: a recorded deviation.
    internal AsyncDeleteRecordsResult(IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>> lowWatermarks)
    {
        _futures = lowWatermarks ?? throw new ArgumentNullException(nameof(lowWatermarks));
    }

    private AsyncDeleteRecordsResult(
        IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>> lowWatermarks,
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
    public IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>> LowWatermarks => _futures;

    /// <summary>
    /// Completes when every partition's deletion has succeeded, and faults with the first failure
    /// to complete if any partition failed — Java's <c>all()</c>, which carries no watermarks: read
    /// them from <see cref="LowWatermarks"/>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() =>
        _nativeFutures is not null
            ? new AsyncKafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures))
            : AsyncAdminFutures.AllOf(_futures.Values);

    internal static AsyncDeleteRecordsResult OfClient(
        (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<DeletedRecords>> LowWatermarks, SafeAdminResultHandle Result) native)
    {
        Dictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>> lowWatermarks =
            new Dictionary<TopicPartition, AsyncKafkaFuture<DeletedRecords>>(native.LowWatermarks.Count);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.LowWatermarks.Count);
        foreach (KeyValuePair<TopicPartition, KafkaFutureImpl<DeletedRecords>> entry in native.LowWatermarks)
        {
            lowWatermarks.Add(entry.Key, new AsyncKafkaFuture<DeletedRecords>(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new AsyncDeleteRecordsResult(lowWatermarks, nativeFutures, native.Result);
    }
}
