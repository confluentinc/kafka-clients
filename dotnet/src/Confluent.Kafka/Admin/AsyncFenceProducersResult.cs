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
/// The result of <see cref="IAsyncAdmin.FenceProducers(IReadOnlyCollection{string}, FenceProducersOptions)"/> — Java's
/// <c>org.apache.kafka.clients.admin.FenceProducersResult</c>.
/// </summary>
/// <remarks>
/// Java stores one future per id carrying a <c>ProducerIdAndEpoch</c> but never publishes that
/// pair: <see cref="FencedProducers()"/> erases it and <see cref="ProducerId"/> /
/// <see cref="EpochId"/> project one field each, so there is no public <c>ProducerIdAndEpoch</c>
/// type here.
/// </remarks>
/// <seealso cref="FenceProducersResult"/>
public sealed class AsyncFenceProducersResult
{
    private readonly IReadOnlyDictionary<string, IKafkaFutureImpl<ProducerIdAndEpoch>> _futures;
    private readonly SafeAdminResultHandle _native;

    private AsyncFenceProducersResult(
        IReadOnlyDictionary<string, IKafkaFutureImpl<ProducerIdAndEpoch>> futures, SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One payload-free future per requested transactional id — Java's
    /// <c>fencedProducers()</c>, whose values are <c>KafkaFuture&lt;Void&gt;</c>.
    /// </summary>
    /// <returns>One payload-free future per requested transactional id.</returns>
    /// <remarks>Each call builds a new map of new futures, as Java's <c>fencedProducers()</c> does.</remarks>
    public IReadOnlyDictionary<string, AsyncKafkaFuture> FencedProducers()
    {
        Dictionary<string, AsyncKafkaFuture> fenced =
            new Dictionary<string, AsyncKafkaFuture>(_futures.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, IKafkaFutureImpl<ProducerIdAndEpoch>> entry in _futures)
        {
            fenced.Add(entry.Key, AsyncAdminFutures.Of(entry.Value.ThenApplyFunc<object?>(static _ => null)));
        }

        return fenced;
    }

    /// <inheritdoc cref="FenceProducersResult.ProducerId"/>
    public AsyncKafkaFuture<long> ProducerId(string transactionalId) =>
        AsyncAdminFutures.Of(
            FenceProducersResult.Find(_futures, transactionalId).ThenApplyFunc(static value => value.ProducerId));

    /// <inheritdoc cref="FenceProducersResult.EpochId"/>
    public AsyncKafkaFuture<short> EpochId(string transactionalId) =>
        AsyncAdminFutures.Of(
            FenceProducersResult.Find(_futures, transactionalId).ThenApplyFunc(static value => value.Epoch));

    /// <inheritdoc cref="FenceProducersResult.All"/>
    public AsyncKafkaFuture All() =>
        AsyncAdminFutures.Of(FutureState.AllOf(new AsyncKafkaFutureImpl<object?>(), new List<IKafkaFutureImpl>(_futures.Values)));

    internal static AsyncFenceProducersResult OfClient(
        (IReadOnlyDictionary<string, IKafkaFutureImpl<ProducerIdAndEpoch>> Producers, SafeAdminResultHandle Result) native) =>
        new AsyncFenceProducersResult(native.Producers, native.Result);
}
