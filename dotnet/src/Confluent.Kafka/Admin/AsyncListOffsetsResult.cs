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
using System.Globalization;
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.ListOffsets"/> — Java's
/// <c>org.apache.kafka.clients.admin.ListOffsetsResult</c>: one future per partition.
/// </summary>
/// <remarks>
/// A partition that fails faults only its own future. <see cref="PartitionResult"/> throws for a
/// partition that was not requested, as Java's does.
/// </remarks>
/// <seealso cref="ListOffsetsResult"/>
public sealed class AsyncListOffsetsResult
{
    private readonly IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> _futures;
    private readonly IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>>? _nativeFutures;
    private readonly SafeAdminResultHandle? _native;

    // Internal, unlike Java's public constructor: a recorded deviation.
    internal AsyncListOffsetsResult(
        IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures)
    {
        _futures = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    private AsyncListOffsetsResult(
        IReadOnlyDictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures,
        IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> nativeFutures,
        SafeAdminResultHandle native)
    {
        _futures = futures;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// The future for one requested partition — Java's <c>partitionResult(TopicPartition)</c>.
    /// </summary>
    /// <param name="partition">A partition that was part of the request.</param>
    /// <returns>That partition's future, carrying its own value or its own failure.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="partition"/> was not requested, with Java's message.
    /// </exception>
    public AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> PartitionResult(TopicPartition partition)
    {
        if (!_futures.TryGetValue(partition, out AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo> future))
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "List Offsets for partition \"{0}\" was not attempted",
                    partition),
                nameof(partition));
        }

        return future;
    }

    /// <summary>
    /// Completes when every requested partition has resolved, yielding the whole map — Java's
    /// <c>all()</c>.
    /// </summary>
    /// <returns>A future yielding every partition's offset information.</returns>
    public AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>> All() =>
        _nativeFutures is not null
            ? new AsyncKafkaFuture<IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>>(
                KafkaFutureImpl.GatherAll(_nativeFutures, EqualityComparer<TopicPartition>.Default))
            : AsyncAdminFutures.GatherAll(_futures, EqualityComparer<TopicPartition>.Default);

    internal static AsyncListOffsetsResult OfClient(
        (IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> Futures,
            SafeAdminResultHandle Result) native)
    {
        Dictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>> futures =
            new Dictionary<TopicPartition, AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>>(native.Futures.Count);
        foreach (KeyValuePair<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> entry in native.Futures)
        {
            futures.Add(entry.Key, new AsyncKafkaFuture<ListOffsetsResult.ListOffsetsResultInfo>(entry.Value));
        }

        return new AsyncListOffsetsResult(futures, native.Futures, native.Result);
    }
}
