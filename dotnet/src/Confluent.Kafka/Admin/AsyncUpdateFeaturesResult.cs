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
/// The result of <see cref="IAsyncAdmin.UpdateFeatures"/> — Java's
/// <c>org.apache.kafka.clients.admin.UpdateFeaturesResult</c>: one future per feature.
/// </summary>
/// <seealso cref="UpdateFeaturesResult"/>
public sealed class AsyncUpdateFeaturesResult
{
    private readonly IReadOnlyDictionary<string, AsyncKafkaFuture> _values;
    private readonly IReadOnlyList<KafkaFutureImpl> _nativeFutures;
    private readonly SafeAdminResultHandle _native;

    private AsyncUpdateFeaturesResult(
        IReadOnlyDictionary<string, AsyncKafkaFuture> values,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested feature — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<string, AsyncKafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every feature's update has been applied, and faults with the first failure to complete if any
    /// feature failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures));

    internal static AsyncUpdateFeaturesResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<string, AsyncKafkaFuture> values =
            new Dictionary<string, AsyncKafkaFuture>(native.Values.Count, StringComparer.Ordinal);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.Values.Count);
        foreach (KeyValuePair<string, KafkaFutureImpl<object?>> entry in native.Values)
        {
            values.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new AsyncUpdateFeaturesResult(values, nativeFutures, native.Result);
    }
}
