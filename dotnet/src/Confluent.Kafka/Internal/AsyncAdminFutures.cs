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

using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The conversions the <c>Async*Result</c> types (M15/P14 S3a) share at their public boundary: a keyed view of the async
/// flavour's tasks presented as <see cref="AsyncKafkaFuture{T}"/>s, and an all-of over such futures.
/// </summary>
/// <remarks>
/// Host scaffolding with no Java counterpart (DoD #7): it only re-types the boundary, so each <c>Async*Result</c> keeps
/// today's <see cref="Task"/> composition underneath.
/// </remarks>
internal static class AsyncAdminFutures
{
    /// <summary>
    /// A read-only view of <paramref name="tasks"/> whose entries are <see cref="AsyncKafkaFuture{T}"/>s over the same
    /// tasks. See <see cref="AsyncFutureView{TKey, TValue}"/>.
    /// </summary>
    internal static IReadOnlyDictionary<TKey, AsyncKafkaFuture<TValue>> Wrap<TKey, TValue>(
        IReadOnlyDictionary<TKey, Task<TValue>> tasks)
        where TKey : notnull =>
        new AsyncFutureView<TKey, TValue>(tasks);

    /// <summary>
    /// <c>Task.WhenAll</c> over the futures' tasks, in their order — so the first failure in that order is the one it
    /// surfaces, exactly as today's <c>Task.WhenAll(values)</c>.
    /// </summary>
    internal static Task WhenAll<TValue>(IEnumerable<AsyncKafkaFuture<TValue>> futures) =>
        Task.WhenAll(futures.Select(static future => future.Get()));

    /// <summary>The void form of <see cref="WhenAll{TValue}"/>.</summary>
    internal static Task WhenAll(IEnumerable<AsyncKafkaFuture> futures) =>
        Task.WhenAll(futures.Select(static future => future.Get()));
}

/// <summary>
/// A read-only dictionary over a dictionary of tasks, whose every entry is an <see cref="AsyncKafkaFuture{T}"/> over the
/// source entry's <b>same</b> task (M15/P14 S3a).
/// </summary>
/// <remarks>
/// <para>
/// A <b>view</b>, not a copy: every member reads the source when called, so the view keeps the source's key order, its
/// key comparer and its membership. That is what lets an <c>Async*Result</c> stand exactly where today's Result handed
/// back its stored dictionary: the same view instance on every access, and an equal future for a key on every read.
/// </para>
/// <para>
/// No second task is created, so none exists whose fault could go unobserved, and a future is a struct, so a read
/// allocates nothing. Host scaffolding with no Java counterpart (DoD #7).
/// </para>
/// </remarks>
internal sealed class AsyncFutureView<TKey, TValue> : IReadOnlyDictionary<TKey, AsyncKafkaFuture<TValue>>
    where TKey : notnull
{
    private readonly IReadOnlyDictionary<TKey, Task<TValue>> _tasks;

    internal AsyncFutureView(IReadOnlyDictionary<TKey, Task<TValue>> tasks) => _tasks = tasks;

    public int Count => _tasks.Count;

    public IEnumerable<TKey> Keys => _tasks.Keys;

    public IEnumerable<AsyncKafkaFuture<TValue>> Values =>
        _tasks.Values.Select(static task => new AsyncKafkaFuture<TValue>(task));

    public AsyncKafkaFuture<TValue> this[TKey key] => new AsyncKafkaFuture<TValue>(_tasks[key]);

    public bool ContainsKey(TKey key) => _tasks.ContainsKey(key);

    public bool TryGetValue(TKey key, out AsyncKafkaFuture<TValue> value)
    {
        if (_tasks.TryGetValue(key, out Task<TValue>? task))
        {
            value = new AsyncKafkaFuture<TValue>(task);
            return true;
        }

        value = default;
        return false;
    }

    public IEnumerator<KeyValuePair<TKey, AsyncKafkaFuture<TValue>>> GetEnumerator()
    {
        foreach (KeyValuePair<TKey, Task<TValue>> entry in _tasks)
        {
            yield return new KeyValuePair<TKey, AsyncKafkaFuture<TValue>>(
                entry.Key, new AsyncKafkaFuture<TValue>(entry.Value));
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
