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

namespace Confluent.Kafka;

/// <summary>
/// The result of a poll — an owned, immutable collection of
/// <see cref="ConsumerRecord{TKey, TValue}"/> — the .NET realization of Java's generic
/// <c>org.apache.kafka.clients.consumer.ConsumerRecords&lt;K, V&gt;</c>. Each record's key /
/// value is the deserialized <typeparamref name="TKey"/> / <typeparamref name="TValue"/>;
/// the topic / headers are copied out of the (now-destroyed) native batch
/// (ffi-marshalling.md §6.4), so this holds only owned managed values: there is no native
/// handle, no <see cref="System.IDisposable"/>, and nothing to keep alive. An empty poll
/// yields a non-null instance with <see cref="Count"/> <c>== 0</c> — success, not failure.
/// </summary>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
public sealed class ConsumerRecords<TKey, TValue> : IReadOnlyCollection<ConsumerRecord<TKey, TValue>>
{
    private readonly IReadOnlyList<ConsumerRecord<TKey, TValue>> _records;

    /// <summary>
    /// Initializes the collection from the owned records produced by the copy-out.
    /// </summary>
    internal ConsumerRecords(IReadOnlyList<ConsumerRecord<TKey, TValue>> records)
    {
        _records = records;
    }

    /// <summary>The number of records in the batch (<c>0</c> for an empty poll).</summary>
    public int Count => _records.Count;

    /// <summary>Enumerates the records in batch insertion order.</summary>
    public IEnumerator<ConsumerRecord<TKey, TValue>> GetEnumerator() => _records.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
