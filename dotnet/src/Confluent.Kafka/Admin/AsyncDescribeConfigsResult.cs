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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DescribeConfigs"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeConfigsResult</c>: one future per resource.
/// </summary>
/// <remarks>
/// A resource that fails faults only its own future. <see cref="All"/> carries the whole map, as
/// Java's does, where <see cref="AsyncAlterConfigsResult.All"/> carries nothing.
/// </remarks>
/// <seealso cref="DescribeConfigsResult"/>
public sealed class AsyncDescribeConfigsResult
{
    private readonly IReadOnlyDictionary<ConfigResource, AsyncKafkaFuture<Config>> _futures;
    private readonly IEqualityComparer<ConfigResource> _comparer;
    private readonly IReadOnlyDictionary<ConfigResource, KafkaFutureImpl<Config>> _nativeValues;
    private readonly SafeAdminResultHandle _native;


    private AsyncDescribeConfigsResult(
        IReadOnlyDictionary<ConfigResource, KafkaFutureImpl<Config>> nativeValues, SafeAdminResultHandle native)
    {
        IEqualityComparer<ConfigResource> comparer = EqualityComparer<ConfigResource>.Default;
        Dictionary<ConfigResource, AsyncKafkaFuture<Config>> futures =
            new Dictionary<ConfigResource, AsyncKafkaFuture<Config>>(nativeValues.Count, comparer);
        foreach (KeyValuePair<ConfigResource, KafkaFutureImpl<Config>> entry in nativeValues)
        {
            futures.Add(entry.Key, new AsyncKafkaFuture<Config>(entry.Value));
        }

        _futures = futures;
        _comparer = comparer;
        _nativeValues = nativeValues;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested resource, keyed by resource — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<ConfigResource, AsyncKafkaFuture<Config>> Values => _futures;

    /// <summary>
    /// Every resource's configuration in one map, succeeding only if every resource
    /// succeeded — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future yielding the whole map, or faulting with the first failure.</returns>
    public AsyncKafkaFuture<IReadOnlyDictionary<ConfigResource, Config>> All() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<ConfigResource, Config>>(KafkaFutureImpl.GatherAll(_nativeValues, _comparer));

    internal static AsyncDescribeConfigsResult OfClient(
        (IReadOnlyDictionary<ConfigResource, KafkaFutureImpl<Config>> Values, SafeAdminResultHandle Result) native) =>
        new AsyncDescribeConfigsResult(native.Values, native.Result);
}
