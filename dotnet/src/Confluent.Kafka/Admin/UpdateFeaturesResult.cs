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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.UpdateFeatures"/> — Java's
/// <c>org.apache.kafka.clients.admin.UpdateFeaturesResult</c>: one future <b>per feature</b>
/// (<c>:29</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ See <see cref="AlterUserScramCredentialsResult"/> on the byte-identical accessor sets and
/// the wiring guard that keeps the two readers apart.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its feature (or, for
/// <see cref="All"/>, every feature) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncUpdateFeaturesResult"/>
public sealed class UpdateFeaturesResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture> _values;

    /// <summary>
    /// Wraps one future per feature — Java's package-private
    /// <c>UpdateFeaturesResult(Map&lt;String, KafkaFuture&lt;Void&gt;&gt;)</c> (<c>:35</c>). The dictionary is
    /// held by reference, as Java holds its map.
    /// </summary>
    internal UpdateFeaturesResult(IReadOnlyDictionary<string, KafkaFuture> values)
    {
        _values = values;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    /// <remarks>
    /// Presents the bridge's <c>bool</c> latches as valueless <see cref="KafkaFuture"/>s: the <c>bool</c> is the
    /// void bridge's internal success token (result shape 2 — the ABI has no <c>get_value</c> here, so a null
    /// per-key error <em>is</em> the success value) and must not reach the public surface. Each void future is erased
    /// over its key's <b>same</b> latch — no second completion — with the bridge's own comparer, so the view and the
    /// sources cannot disagree about key identity.
    /// </remarks>
    internal static UpdateFeaturesResult From(VoidKeyedAdminOperation<string> operation) =>
        new UpdateFeaturesResult(SyncAdminFutures.Erase(operation.SyncSources, operation.KeyComparer));

    /// <summary>
    /// One future per requested feature — Java's <c>values()</c> (<c>:39</c>).
    /// </summary>
    public IReadOnlyDictionary<string, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every feature's update has been applied, and fails with the first failure in key order if any
    /// feature failed — Java's <c>all()</c> (<c>:46</c>).
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every feature.</returns>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);
}
