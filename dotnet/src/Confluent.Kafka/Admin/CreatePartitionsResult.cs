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
/// The result of <see cref="IAdmin.CreatePartitions"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreatePartitionsResult</c>: one future per topic.
/// </summary>
/// <remarks>
/// A topic's future completes when its partitions were created; a topic that fails fails only
/// its own future.
/// </remarks>
/// <seealso cref="AsyncCreatePartitionsResult"/>
public sealed class CreatePartitionsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture> _values;
    private readonly IReadOnlyList<KafkaFutureImpl> _nativeFutures;
    private readonly SafeAdminResultHandle _native;


    private CreatePartitionsResult(
        IReadOnlyDictionary<string, KafkaFuture> values, IReadOnlyList<KafkaFutureImpl> nativeFutures, SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested topic, keyed by topic name — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<string, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every topic's partitions have been created, and fails with the first
    /// failure to complete if any topic failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public KafkaFuture All() =>
        new KafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures));

    internal static CreatePartitionsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<string, KafkaFuture> values = new Dictionary<string, KafkaFuture>(native.Values.Count, StringComparer.Ordinal);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.Values.Count);
        foreach (KeyValuePair<string, KafkaFutureImpl<object?>> entry in native.Values)
        {
            values.Add(entry.Key, new KafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new CreatePartitionsResult(values, nativeFutures, native.Result);
    }
}
