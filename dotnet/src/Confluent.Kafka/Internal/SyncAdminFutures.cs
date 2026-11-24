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
using System.Linq;
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Java's <c>allOf</c> and <c>allOf(...).thenApply(...)</c> over the <see cref="KafkaFuture{T}"/>s a sync admin Result
/// was given, which are their engines; a <see langword="null"/> one is refused at the call, as <c>KafkaFuture.AllOf</c>
/// refuses it. It also turns a native admin future, which is a sync engine, into the public type.
/// </summary>
internal static class SyncAdminFutures
{
    internal static KafkaFuture<T> Of<T>(IKafkaFutureImpl<T> future) => (KafkaFuture<T>)future;

    internal static KafkaFuture AllOf(IEnumerable<KafkaFuture> futures) =>
        FutureState.AllOf(new KafkaFutureImpl<object?>(), futures.Select(static future => EngineOf(future)).ToList());

    internal static KafkaFuture<IReadOnlyDictionary<TKey, TValue>> GatherAll<TKey, TValue>(
        IEnumerable<KeyValuePair<TKey, KafkaFuture<TValue>>> futures, IEqualityComparer<TKey> comparer)
        where TKey : notnull =>
        Of(FutureState.GatherAll(new KafkaFutureImpl<object?>(), EnginesOf(futures), comparer));

    private static IKafkaFutureImpl EngineOf(KafkaFuture? future) =>
        (IKafkaFutureImpl?)future ?? throw new ArgumentNullException("futures", FutureState.ContainsNullMessage);

    private static List<KeyValuePair<TKey, IKafkaFutureImpl<TValue>>> EnginesOf<TKey, TValue>(
        IEnumerable<KeyValuePair<TKey, KafkaFuture<TValue>>> futures) =>
        futures
            .Select(static entry => new KeyValuePair<TKey, IKafkaFutureImpl<TValue>>(
                entry.Key, (IKafkaFutureImpl<TValue>)EngineOf(entry.Value)))
            .ToList();
}
