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
/// The result of <see cref="IAsyncAdmin.ListConfigResources"/> — Java's
/// <c>org.apache.kafka.clients.admin.ListConfigResourcesResult</c>: one future over the whole
/// listing.
/// </summary>
/// <remarks>
/// Java declares only <c>all()</c>, so there is no per-resource view. The order of the entries is
/// not part of the contract, as Java returns a <c>Collection</c>.
/// </remarks>
/// <seealso cref="ListConfigResourcesResult"/>
public sealed class AsyncListConfigResourcesResult
{
    private readonly AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> _future;
    private readonly SafeAdminResultHandle? _native;

    internal AsyncListConfigResourcesResult(AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> future)
    {
        _future = future;
    }

    private AsyncListConfigResourcesResult(
        AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> future, SafeAdminResultHandle native)
    {
        _future = future;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// The full set of config resources — Java's <c>all()</c>.
    /// </summary>
    /// <returns>
    /// A new future on each call, yielding the resources, or faulting with the call's own
    /// <see cref="KafkaException"/>.
    /// </returns>
    public AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> All() => _future.ThenApply(static resources => resources);

    internal static AsyncListConfigResourcesResult OfClient(
        (KafkaFutureImpl<IReadOnlyCollection<ConfigResource>> All, SafeAdminResultHandle Result) native) =>
        new AsyncListConfigResourcesResult(
            new AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>>(native.All), native.Result);
}
