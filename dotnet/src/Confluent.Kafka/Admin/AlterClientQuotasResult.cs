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
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.AlterClientQuotas"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterClientQuotasResult</c>: one future per quota entity.
/// </summary>
/// <remarks>
/// An entity that fails fails only its own future. The dictionary is keyed by
/// <see cref="ClientQuotaEntity"/> value equality.
/// </remarks>
/// <seealso cref="AsyncAlterClientQuotasResult"/>
public sealed class AlterClientQuotasResult
{
    private readonly IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture> _values;
    private readonly IReadOnlyList<KafkaFutureImpl>? _nativeFutures;
    private readonly SafeAdminResultHandle? _native;

    // Internal, unlike Java's public constructor: a recorded deviation. The dictionary is held
    // by reference, as Java holds its map.
    internal AlterClientQuotasResult(IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    private AlterClientQuotasResult(
        IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture> values,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// One future per requested entity, keyed by the entity — Java's <c>values()</c>.
    /// </summary>
    public IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every entity's quotas have been altered, and fails with the first
    /// failure to complete if any entity failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every entity.</returns>
    public KafkaFuture All() =>
        _nativeFutures is not null
            ? new KafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures))
            : SyncAdminFutures.AllOf(_values.Values);

    internal static AlterClientQuotasResult OfClient(
        (IReadOnlyDictionary<ClientQuotaEntity, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<ClientQuotaEntity, KafkaFuture> values = new Dictionary<ClientQuotaEntity, KafkaFuture>(native.Values.Count);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.Values.Count);
        foreach (KeyValuePair<ClientQuotaEntity, KafkaFutureImpl<object?>> entry in native.Values)
        {
            values.Add(entry.Key, new KafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new AlterClientQuotasResult(values, nativeFutures, native.Result);
    }
}
