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
/// The result of <see cref="IAdmin.AlterUserScramCredentials"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterUserScramCredentialsResult</c>: one future per user.
/// </summary>
/// <seealso cref="AsyncAlterUserScramCredentialsResult"/>
public sealed class AlterUserScramCredentialsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture> _values;
    private readonly IReadOnlyList<KafkaFutureImpl>? _nativeFutures;
    private readonly SafeAdminResultHandle? _native;

    // Internal, unlike Java's public constructor: a recorded deviation. The dictionary is held
    // by reference, as Java holds an unmodifiable view over its map.
    internal AlterUserScramCredentialsResult(IReadOnlyDictionary<string, KafkaFuture> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    private AlterUserScramCredentialsResult(
        IReadOnlyDictionary<string, KafkaFuture> values,
        IReadOnlyList<KafkaFutureImpl> nativeFutures,
        SafeAdminResultHandle native)
    {
        _values = values;
        _nativeFutures = nativeFutures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>One future per requested user — Java's <c>values()</c>.</summary>
    public IReadOnlyDictionary<string, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every user's alteration has been applied, and fails with the first failure to complete if
    /// any user failed — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every user.</returns>
    public KafkaFuture All() =>
        _nativeFutures is not null
            ? new KafkaFuture(KafkaFutureImpl.AllOf(_nativeFutures))
            : SyncAdminFutures.AllOf(_values.Values);

    internal static AlterUserScramCredentialsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<object?>> Values, SafeAdminResultHandle Result) native)
    {
        Dictionary<string, KafkaFuture> values = new Dictionary<string, KafkaFuture>(native.Values.Count, StringComparer.Ordinal);
        List<KafkaFutureImpl> nativeFutures = new List<KafkaFutureImpl>(native.Values.Count);
        foreach (KeyValuePair<string, KafkaFutureImpl<object?>> entry in native.Values)
        {
            values.Add(entry.Key, new KafkaFuture(entry.Value));
            nativeFutures.Add(entry.Value);
        }

        return new AlterUserScramCredentialsResult(values, nativeFutures, native.Result);
    }
}
