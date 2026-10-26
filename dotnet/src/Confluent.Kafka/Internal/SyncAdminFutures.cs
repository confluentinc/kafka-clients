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
using System.Linq;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Java's <c>allOf</c> and <c>allOf(...).thenApply(...)</c> over the <see cref="KafkaFuture{T}"/>s a sync admin Result
/// was given, composed over their <see cref="KafkaFutureImpl"/>s.
/// </summary>
internal static class SyncAdminFutures
{
    internal static KafkaFuture AllOf<TValue>(IEnumerable<KafkaFuture<TValue>> futures) =>
        new KafkaFuture(KafkaFutureImpl.AllOf(futures.Select(static future => (KafkaFutureImpl)future.Impl).ToList()));

    internal static KafkaFuture AllOf(IEnumerable<KafkaFuture> futures) =>
        new KafkaFuture(KafkaFutureImpl.AllOf(futures.Select(static future => future.Impl).ToList()));

    internal static KafkaFuture<IReadOnlyDictionary<TKey, TValue>> GatherAll<TKey, TValue>(
        IEnumerable<KeyValuePair<TKey, KafkaFuture<TValue>>> futures, IEqualityComparer<TKey> comparer)
        where TKey : notnull =>
        new KafkaFuture<IReadOnlyDictionary<TKey, TValue>>(KafkaFutureImpl.GatherAll(ImplsOf(futures), comparer));

    private static List<KeyValuePair<TKey, KafkaFutureImpl<TValue>>> ImplsOf<TKey, TValue>(
        IEnumerable<KeyValuePair<TKey, KafkaFuture<TValue>>> futures) =>
        futures
            .Select(static entry => new KeyValuePair<TKey, KafkaFutureImpl<TValue>>(entry.Key, entry.Value.Impl))
            .ToList();
}
