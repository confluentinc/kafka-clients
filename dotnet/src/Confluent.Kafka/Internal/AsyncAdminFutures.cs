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
/// Java's <c>allOf</c> and <c>allOf(...).thenApply(...)</c> over the <see cref="AsyncKafkaFuture{T}"/>s an async admin
/// Result was given, which are their engines; a <see langword="null"/> one is refused at the call, as
/// <c>AsyncKafkaFuture.AllOf</c> refuses it. It also turns a native admin future, which is an async engine, into the
/// public type, and back.
/// </summary>
internal static class AsyncAdminFutures
{
    internal static AsyncKafkaFuture<T> Of<T>(IKafkaFutureImpl<T> future) => (AsyncKafkaFuture<T>)future;

    internal static IKafkaFutureImpl<T> Engine<T>(AsyncKafkaFuture<T> future) => (IKafkaFutureImpl<T>)future;

    internal static AsyncKafkaFuture AllOf(IEnumerable<AsyncKafkaFuture> futures) =>
        FutureState.AllOf(new AsyncKafkaFutureImpl<object?>(), futures.Select(static future => EngineOf(future)).ToList());

    internal static AsyncKafkaFuture<IReadOnlyDictionary<TKey, TValue>> GatherAll<TKey, TValue>(
        IEnumerable<KeyValuePair<TKey, AsyncKafkaFuture<TValue>>> futures, IEqualityComparer<TKey> comparer)
        where TKey : notnull =>
        Of(FutureState.GatherAll(new AsyncKafkaFutureImpl<object?>(), EnginesOf(futures), comparer));

    private static IKafkaFutureImpl EngineOf(AsyncKafkaFuture? future) =>
        (IKafkaFutureImpl?)future ?? throw new ArgumentNullException("futures", FutureState.ContainsNullMessage);

    private static List<KeyValuePair<TKey, IKafkaFutureImpl<TValue>>> EnginesOf<TKey, TValue>(
        IEnumerable<KeyValuePair<TKey, AsyncKafkaFuture<TValue>>> futures) =>
        futures
            .Select(static entry => new KeyValuePair<TKey, IKafkaFutureImpl<TValue>>(
                entry.Key, (IKafkaFutureImpl<TValue>)EngineOf(entry.Value)))
            .ToList();
}
