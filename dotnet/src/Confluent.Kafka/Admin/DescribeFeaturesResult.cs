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
using Confluent.Kafka.Internal.Interop;

using Metadata = Confluent.Kafka.Admin.FeatureMetadata;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeFeatures"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeFeaturesResult</c>: one future over the whole feature
/// metadata.
/// </summary>
/// <seealso cref="AsyncDescribeFeaturesResult"/>
public sealed class DescribeFeaturesResult
{
    private readonly KafkaFutureImpl<Metadata> _featureMetadata;
    private readonly SafeAdminResultHandle _native;

    private DescribeFeaturesResult(KafkaFutureImpl<Metadata> featureMetadata, SafeAdminResultHandle native)
    {
        _featureMetadata = featureMetadata;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// The cluster's feature metadata — Java's <c>featureMetadata()</c>.
    /// </summary>
    /// <returns>A future over the metadata.</returns>
    /// <remarks>Every call returns an equal future, as Java's accessor returns its stored field.</remarks>
    public KafkaFuture<Metadata> FeatureMetadata() => new KafkaFuture<Metadata>(_featureMetadata);

    internal static DescribeFeaturesResult OfClient(
        (KafkaFutureImpl<Metadata> FeatureMetadata, SafeAdminResultHandle Result) native) =>
        new DescribeFeaturesResult(native.FeatureMetadata, native.Result);
}
