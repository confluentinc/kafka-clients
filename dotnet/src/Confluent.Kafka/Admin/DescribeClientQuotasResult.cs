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
/// The result of <see cref="IAdmin.DescribeClientQuotas"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeClientQuotasResult</c>: one future over the whole
/// entity-to-quota map.
/// </summary>
/// <remarks>
/// Java publishes only <c>entities()</c>, so there is no <c>All()</c>; any failure fails this one
/// future.
/// </remarks>
/// <seealso cref="AsyncDescribeClientQuotasResult"/>
public sealed class DescribeClientQuotasResult
{
    private readonly KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> _future;
    private readonly SafeAdminResultHandle? _native;

    // Internal, unlike Java's public constructor: a recorded deviation.
    internal DescribeClientQuotasResult(
        KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> entities)
    {
        _future = entities != default ? entities : throw new ArgumentNullException(nameof(entities));
    }

    private DescribeClientQuotasResult(
        KafkaFutureImpl<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> entities, SafeAdminResultHandle native)
    {
        _future = new KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(entities);
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// The quotas of every entity matching the filter — Java's <c>entities()</c>.
    /// A quota type an entity has no value for is simply absent.
    /// </summary>
    /// <returns>
    /// A future yielding the map, or failing with the call's own
    /// <see cref="KafkaException"/>; on <see cref="MockAdminClient"/>, which does not implement the
    /// call, with a <see cref="NotSupportedException"/>.
    /// </returns>
    /// <remarks>Every call returns an equal future, as Java's accessor returns its stored field.</remarks>
    public KafkaFuture<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> Entities() =>
        _future;

    internal static DescribeClientQuotasResult OfClient(
        (KafkaFutureImpl<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> Entities,
        SafeAdminResultHandle Result) native) =>
        new DescribeClientQuotasResult(native.Entities, native.Result);
}
