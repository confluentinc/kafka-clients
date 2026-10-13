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

using Confluent.Kafka.Internal;

using Metadata = Confluent.Kafka.Admin.FeatureMetadata;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeFeatures"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeFeaturesResult</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>ONE future over a composite, not a fan-out.</b> Java stores a single
/// <c>KafkaFuture&lt;FeatureMetadata&gt;</c> (<c>:28</c>) with one accessor (<c>:34</c>), even
/// though the ABI exposes two independent tables plus an optional scalar. The
/// <c>describeCluster</c> snapshot supplies the read-the-whole-root mechanism; it does not
/// supply its four-way fan-out, because Java publishes four futures there and one here.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the description has completed, and
/// needs no thread-pool thread: the future here is the operation's own latch, never a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDescribeFeaturesResult"/>
public sealed class DescribeFeaturesResult
{
    private readonly KafkaFuture<Metadata> _featureMetadata;

    internal DescribeFeaturesResult(KafkaFuture<Metadata> featureMetadata)
    {
        _featureMetadata = featureMetadata;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeFeaturesResult From(SingleAdminOperation<FeatureMetadata> operation) =>
        new DescribeFeaturesResult(new KafkaFuture<Metadata>(operation.SyncSource));

    /// <summary>
    /// The cluster's feature metadata — Java's <c>featureMetadata()</c> (<c>:34</c>).
    /// </summary>
    /// <returns>A future over the metadata.</returns>
    /// <remarks>
    /// This is a future over the operation's own latch, so every call returns an <b>equal</b> future — no second
    /// completion.
    /// </remarks>
    public KafkaFuture<Metadata> FeatureMetadata() => _featureMetadata;
}
