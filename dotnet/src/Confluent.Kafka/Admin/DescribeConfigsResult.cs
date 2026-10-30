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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeConfigs"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeConfigsResult</c>: one future per resource.
/// </summary>
/// <remarks>
/// A resource that fails fails only its own future. <see cref="All"/> carries the whole map, as
/// Java's does, where <see cref="AlterConfigsResult.All"/> carries nothing.
/// </remarks>
/// <seealso cref="AsyncDescribeConfigsResult"/>
public sealed class DescribeConfigsResult
{
    private readonly IReadOnlyDictionary<ConfigResource, KafkaFuture<Config>> _values;
    private readonly IEqualityComparer<ConfigResource> _comparer;
    private readonly IReadOnlyDictionary<ConfigResource, KafkaFutureImpl<Config>>? _nativeValues;
    private readonly SafeAdminResultHandle? _native;

    // Internal, unlike Java's protected constructor: a recorded deviation.
    internal DescribeConfigsResult(
        IReadOnlyDictionary<ConfigResource, KafkaFuture<Config>> values, IEqualityComparer<ConfigResource> comparer)
    {
        _values = values;
        _comparer = comparer;
    }

    private DescribeConfigsResult(
        IReadOnlyDictionary<ConfigResource, KafkaFutureImpl<Config>> nativeValues, SafeAdminResultHandle native)
    {
        IEqualityComparer<ConfigResource> comparer = EqualityComparer<ConfigResource>.Default;
        Dictionary<ConfigResource, KafkaFuture<Config>> values =
            new Dictionary<ConfigResource, KafkaFuture<Config>>(nativeValues.Count, comparer);
        foreach (KeyValuePair<ConfigResource, KafkaFutureImpl<Config>> entry in nativeValues)
        {
            values.Add(entry.Key, new KafkaFuture<Config>(entry.Value));
        }

        _values = values;
        _comparer = comparer;
        _nativeValues = nativeValues;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// One future per requested resource, keyed by resource — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<ConfigResource, KafkaFuture<Config>> Values => _values;

    /// <summary>
    /// Every resource's configuration in one map, succeeding only if every resource
    /// succeeded — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future yielding the whole map, or failing with the first failure to complete.</returns>
    public KafkaFuture<IReadOnlyDictionary<ConfigResource, Config>> All() =>
        _nativeValues is not null
            ? new KafkaFuture<IReadOnlyDictionary<ConfigResource, Config>>(KafkaFutureImpl.GatherAll(_nativeValues, _comparer))
            : SyncAdminFutures.GatherAll(_values, _comparer);

    internal static DescribeConfigsResult OfClient(
        (IReadOnlyDictionary<ConfigResource, KafkaFutureImpl<Config>> Values, SafeAdminResultHandle Result) native) =>
        new DescribeConfigsResult(native.Values, native.Result);
}
