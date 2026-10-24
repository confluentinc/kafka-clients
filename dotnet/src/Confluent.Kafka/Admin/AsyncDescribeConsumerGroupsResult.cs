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
/// The result of <see cref="IAsyncAdmin.DescribeConsumerGroups"/>, Java's <c>DescribeConsumerGroupsResult</c>:
/// one future per group id.
/// </summary>
public sealed class AsyncDescribeConsumerGroupsResult
{
    private readonly IReadOnlyDictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>> _futures;
    private readonly SafeAdminResultHandle? _native;

    /// <inheritdoc cref="DescribeConsumerGroupsResult(IReadOnlyDictionary{string, KafkaFuture{ConsumerGroupDescription}})"/>
    public AsyncDescribeConsumerGroupsResult(IReadOnlyDictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>> futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>> copy = new Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>>(futures.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, AsyncKafkaFuture<ConsumerGroupDescription>> entry in futures)
        {
            copy.Add(entry.Key, entry.Value);
        }

        _futures = copy;
    }

    private AsyncDescribeConsumerGroupsResult(IReadOnlyDictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>> futures, SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <inheritdoc cref="DescribeConsumerGroupsResult.DescribedGroups"/>
    public IReadOnlyDictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>> DescribedGroups => _futures;

    /// <inheritdoc cref="DescribeConsumerGroupsResult.All"/>
    public AsyncKafkaFuture<IReadOnlyDictionary<string, ConsumerGroupDescription>> All()
    {
        IReadOnlyDictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>> futures = _futures;
        AsyncKafkaFuture<ConsumerGroupDescription>[] values = new AsyncKafkaFuture<ConsumerGroupDescription>[futures.Count];
        int next = 0;
        foreach (AsyncKafkaFuture<ConsumerGroupDescription> future in futures.Values)
        {
            values[next++] = future;
        }

        return AsyncKafkaFuture.AllOf(values).ThenApply<IReadOnlyDictionary<string, ConsumerGroupDescription>>(() =>
        {
            Dictionary<string, ConsumerGroupDescription> descriptions = new Dictionary<string, ConsumerGroupDescription>(futures.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, AsyncKafkaFuture<ConsumerGroupDescription>> entry in futures)
            {
                descriptions.Add(entry.Key, entry.Value.GetNow(null!));
            }

            return descriptions;
        });
    }

    internal static AsyncDescribeConsumerGroupsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<ConsumerGroupDescription>> DescribedGroups, SafeAdminResultHandle Result) native)
    {
        Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>> futures =
            new Dictionary<string, AsyncKafkaFuture<ConsumerGroupDescription>>(native.DescribedGroups.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, KafkaFutureImpl<ConsumerGroupDescription>> entry in native.DescribedGroups)
        {
            futures.Add(entry.Key, new AsyncKafkaFuture<ConsumerGroupDescription>(entry.Value));
        }

        return new AsyncDescribeConsumerGroupsResult(futures, native.Result);
    }
}
