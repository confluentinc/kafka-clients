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
using System.Threading.Tasks;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.UpdateFeatures"/> — Java's
/// <c>org.apache.kafka.clients.admin.UpdateFeaturesResult</c>: one awaitable <b>per feature</b>
/// (<c>:29</c>).
/// </summary>
/// <remarks>
/// ⚠ See <see cref="AsyncAlterUserScramCredentialsResult"/> on the byte-identical accessor sets and
/// the wiring guard that keeps the two readers apart.
/// </remarks>
/// <seealso cref="UpdateFeaturesResult"/>
public sealed class AsyncUpdateFeaturesResult
{
    private readonly IReadOnlyDictionary<string, AsyncKafkaFuture> _values;

    internal AsyncUpdateFeaturesResult(
        IReadOnlyDictionary<string, Task<bool>> values, IEqualityComparer<string> comparer)
    {
        Dictionary<string, AsyncKafkaFuture> erased = new Dictionary<string, AsyncKafkaFuture>(values.Count, comparer);
        foreach (KeyValuePair<string, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
        }

        _values = erased;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncUpdateFeaturesResult From(VoidKeyedAdminOperation<string> operation) =>
        new AsyncUpdateFeaturesResult(operation.Tasks, operation.KeyComparer);

    /// <summary>
    /// One awaitable per requested feature — Java's <c>values()</c> (<c>:39</c>).
    /// </summary>
    public IReadOnlyDictionary<string, AsyncKafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every feature's update has been applied — Java's <c>all()</c>
    /// (<c>:46</c>).
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(AsyncAdminFutures.WhenAll(_values.Values));
}
