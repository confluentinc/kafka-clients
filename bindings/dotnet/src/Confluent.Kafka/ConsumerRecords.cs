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
/// <see cref="ConsumerRecord"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.consumer.ConsumerRecords</c>. The records are copied out
/// of the (now-destroyed) native batch (ffi-marshalling.md §6.4), so this holds only
/// owned managed values: there is no native handle, no <see cref="System.IDisposable"/>,
/// and nothing to keep alive. An empty poll yields a non-null instance with
/// <see cref="Count"/> <c>== 0</c> — success, not failure.
/// </summary>
public sealed class ConsumerRecords : IReadOnlyCollection<ConsumerRecord>
{
    private readonly IReadOnlyList<ConsumerRecord> _records;

    /// <summary>
    /// Initializes the collection from the owned records produced by the copy-out.
    /// </summary>
    internal ConsumerRecords(IReadOnlyList<ConsumerRecord> records)
    {
        _records = records;
    }

    /// <summary>The number of records in the batch (<c>0</c> for an empty poll).</summary>
    public int Count => _records.Count;

    /// <summary>Enumerates the records in batch insertion order.</summary>
    public IEnumerator<ConsumerRecord> GetEnumerator() => _records.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
