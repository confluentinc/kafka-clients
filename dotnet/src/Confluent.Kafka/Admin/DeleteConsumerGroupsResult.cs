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
/// The result of <see cref="IAdmin.DeleteConsumerGroups"/> — Java's
/// <c>org.apache.kafka.clients.admin.DeleteConsumerGroupsResult</c>: one future per group id.
/// </summary>
/// <seealso cref="AsyncDeleteConsumerGroupsResult"/>
public sealed class DeleteConsumerGroupsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture> _deletedGroups;
    private readonly SafeAdminResultHandle? _native;

    internal DeleteConsumerGroupsResult(IReadOnlyDictionary<string, KafkaFuture> deletedGroups)
    {
        if (deletedGroups is null)
        {
            throw new ArgumentNullException(nameof(deletedGroups));
        }

        _deletedGroups = deletedGroups;
    }

    private DeleteConsumerGroupsResult(IReadOnlyDictionary<string, KafkaFuture> deletedGroups, SafeAdminResultHandle native)
    {
        _deletedGroups = deletedGroups;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// One future per requested group id, keyed by group id — Java's <c>deletedGroups()</c>. A group that fails fails
    /// only its own future.
    /// </summary>
    public IReadOnlyDictionary<string, KafkaFuture> DeletedGroups => _deletedGroups;

    /// <summary>
    /// Completes when every group has been deleted, and fails with the first failure to complete otherwise — Java's
    /// <c>all()</c>. Each call builds a new future.
    /// </summary>
    /// <returns>A future over the whole batch.</returns>
    public KafkaFuture All()
    {
        KafkaFuture[] values = new KafkaFuture[_deletedGroups.Count];
        int next = 0;
        foreach (KafkaFuture future in _deletedGroups.Values)
        {
            values[next++] = future;
        }

        return KafkaFuture.AllOf(values);
    }

    internal static DeleteConsumerGroupsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<object?>> DeletedGroups, SafeAdminResultHandle Result) native)
    {
        Dictionary<string, KafkaFuture> futures = new Dictionary<string, KafkaFuture>(native.DeletedGroups.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, KafkaFutureImpl<object?>> entry in native.DeletedGroups)
        {
            futures.Add(entry.Key, new KafkaFuture(entry.Value));
        }

        return new DeleteConsumerGroupsResult(futures, native.Result);
    }
}
