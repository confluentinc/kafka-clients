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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeClassicGroups"/>, Java's <c>DescribeClassicGroupsResult</c>:
/// one future per group id.
/// </summary>
public sealed class DescribeClassicGroupsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture<ClassicGroupDescription>> _futures;
    private readonly SafeAdminResultHandle? _native;

    /// <summary>
    /// Creates a result over the given futures, keyed by group id, as Java's public constructor does.
    /// </summary>
    /// <param name="futures">The future of each group's description.</param>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is <see langword="null"/>.</exception>
    public DescribeClassicGroupsResult(IReadOnlyDictionary<string, KafkaFuture<ClassicGroupDescription>> futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        Dictionary<string, KafkaFuture<ClassicGroupDescription>> copy = new Dictionary<string, KafkaFuture<ClassicGroupDescription>>(futures.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, KafkaFuture<ClassicGroupDescription>> entry in futures)
        {
            copy.Add(entry.Key, entry.Value);
        }

        _futures = copy;
    }

    private DescribeClassicGroupsResult(IReadOnlyDictionary<string, KafkaFuture<ClassicGroupDescription>> futures, SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// The future of each group's description, keyed by group id.
    /// </summary>
    public IReadOnlyDictionary<string, KafkaFuture<ClassicGroupDescription>> DescribedGroups => _futures;

    /// <summary>
    /// A future that yields every group's description once all of them succeeded; it fails with the
    /// first failure to complete. Each call builds a new future.
    /// </summary>
    public KafkaFuture<IReadOnlyDictionary<string, ClassicGroupDescription>> All()
    {
        IReadOnlyDictionary<string, KafkaFuture<ClassicGroupDescription>> futures = _futures;
        KafkaFuture<ClassicGroupDescription>[] values = new KafkaFuture<ClassicGroupDescription>[futures.Count];
        int next = 0;
        foreach (KafkaFuture<ClassicGroupDescription> future in futures.Values)
        {
            values[next++] = future;
        }

        return KafkaFuture.AllOf(values).ThenApply<IReadOnlyDictionary<string, ClassicGroupDescription>>(() =>
        {
            Dictionary<string, ClassicGroupDescription> descriptions = new Dictionary<string, ClassicGroupDescription>(futures.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, KafkaFuture<ClassicGroupDescription>> entry in futures)
            {
                descriptions.Add(entry.Key, entry.Value.GetNow(null!));
            }

            return descriptions;
        });
    }

    internal static DescribeClassicGroupsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<ClassicGroupDescription>> DescribedGroups, SafeAdminResultHandle Result) native)
    {
        Dictionary<string, KafkaFuture<ClassicGroupDescription>> futures =
            new Dictionary<string, KafkaFuture<ClassicGroupDescription>>(native.DescribedGroups.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, KafkaFutureImpl<ClassicGroupDescription>> entry in native.DescribedGroups)
        {
            futures.Add(entry.Key, new KafkaFuture<ClassicGroupDescription>(entry.Value));
        }

        return new DescribeClassicGroupsResult(futures, native.Result);
    }
}
