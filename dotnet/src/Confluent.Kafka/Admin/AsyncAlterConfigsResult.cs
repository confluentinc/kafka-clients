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
/// The result of <see cref="IAsyncAdmin.IncrementalAlterConfigs"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterConfigsResult</c>: one future per resource.
/// </summary>
/// <remarks>
/// The type is named for Java's return type, not for the RPC. A resource that fails faults only
/// its own future, and <see cref="All"/> is a bare <see cref="AsyncKafkaFuture"/>, as in Java.
/// </remarks>
/// <seealso cref="AlterConfigsResult"/>
public sealed class AsyncAlterConfigsResult
{
    private readonly IReadOnlyDictionary<ConfigResource, AsyncKafkaFuture> _values;
    private readonly IReadOnlyList<KafkaFutureImpl> _nativeFutures;
    private readonly SafeAdminResultHandle _native;


    private AsyncAlterConfigsResult(
        IReadOnlyDictionary<ConfigResource, AsyncKafkaFuture> values,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested resource, keyed by resource — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<ConfigResource, AsyncKafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every resource has been altered, and faults with the first failure if
    /// any resource failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() =>
        new AsyncKafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures));

    internal static AsyncAlterConfigsResult OfClient(
        (IReadOnlyDictionary<ConfigResource, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<ConfigResource, AsyncKafkaFuture> values =
            new Dictionary<ConfigResource, AsyncKafkaFuture>(native.Values.Count, EqualityComparer<ConfigResource>.Default);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.Values.Count);
        foreach (KeyValuePair<ConfigResource, KafkaFutureImpl<object?>> entry in native.Values)
        {
            values.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new AsyncAlterConfigsResult(values, nativeFutures, native.Result);
    }
}
