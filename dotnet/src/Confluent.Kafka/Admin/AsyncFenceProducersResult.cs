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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.FenceProducers"/> — Java's
/// <c>org.apache.kafka.clients.admin.FenceProducersResult</c>.
/// </summary>
/// <remarks>
/// Java stores one future per id carrying a <c>ProducerIdAndEpoch</c> but never publishes that
/// pair: <see cref="FencedProducers"/> erases it and <see cref="ProducerId"/> /
/// <see cref="EpochId"/> project one field each, so there is no public <c>ProducerIdAndEpoch</c>
/// type here.
/// </remarks>
/// <seealso cref="FenceProducersResult"/>
public sealed class AsyncFenceProducersResult
{
    private readonly IReadOnlyDictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> _futures;
    private readonly SafeAdminResultHandle _native;

    private AsyncFenceProducersResult(
        IReadOnlyDictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> futures, SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One payload-free future per requested transactional id — Java's
    /// <c>fencedProducers()</c>, whose values are <c>KafkaFuture&lt;Void&gt;</c>.
    /// </summary>
    /// <remarks>Each access builds a new map of new futures, as Java's <c>fencedProducers()</c> does.</remarks>
    public IReadOnlyDictionary<string, AsyncKafkaFuture> FencedProducers
    {
        get
        {
            Dictionary<string, AsyncKafkaFuture> fenced =
                new Dictionary<string, AsyncKafkaFuture>(_futures.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, KafkaFutureImpl<ProducerIdAndEpoch>> entry in _futures)
            {
                fenced.Add(entry.Key, new AsyncKafkaFuture(entry.Value.ThenApply<object?>(static _ => null)));
            }

            return fenced;
        }
    }

    /// <inheritdoc cref="FenceProducersResult.ProducerId"/>
    public AsyncKafkaFuture<long> ProducerId(string transactionalId) =>
        new AsyncKafkaFuture<long>(
            FenceProducersResult.Find(_futures, transactionalId).ThenApply(static value => value.ProducerId));

    /// <inheritdoc cref="FenceProducersResult.EpochId"/>
    public AsyncKafkaFuture<short> EpochId(string transactionalId) =>
        new AsyncKafkaFuture<short>(
            FenceProducersResult.Find(_futures, transactionalId).ThenApply(static value => value.Epoch));

    /// <inheritdoc cref="FenceProducersResult.All"/>
    public AsyncKafkaFuture All() =>
        new AsyncKafkaFuture(KafkaFutureImpl.AllOf(new List<KafkaFutureImpl>(_futures.Values)));

    internal static AsyncFenceProducersResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> Producers, SafeAdminResultHandle Result) native) =>
        new AsyncFenceProducersResult(native.Producers, native.Result);
}
