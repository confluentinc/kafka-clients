// Copyright 2026 Confluent Inc.
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
using System.Globalization;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The typed keys and values of the records a mock consumer holds, by the ids that stand for them in the C record's
/// <c>void *</c> key and value slots. The C mock stores a slot value and never reads, frees or compares it, so a
/// key or value crosses the C ABI as an id, not as bytes: <c>AddRecord</c> adds the entries, <c>Poll</c> takes them
/// when it delivers the record, and <c>Close</c> clears what the core dropped without delivering.
/// </summary>
/// <remarks>
/// Java's <c>MockConsumer</c> keeps the record objects themselves; the C ABI carries no managed object, so this
/// table is how the .NET mock keeps them. Ids come from one counter, starting at 1 and never reused; 0 is the
/// slot of a null key or value, which stays <c>NULL</c>. Two dictionaries keep value-type keys and values unboxed.
/// One re-entrant lock guards both and the counter (Java's mock is <c>synchronized</c>, and a poll task may add a
/// record inside <c>Poll</c>); it is never held across an await or a native wait.
/// </remarks>
internal sealed class MockRecordSlots<TKey, TValue>
{
    private readonly object _lock = new object();
    private readonly Dictionary<nint, TKey> _keys = new Dictionary<nint, TKey>();
    private readonly Dictionary<nint, TValue> _values = new Dictionary<nint, TValue>();
    private nint _next;

    /// <summary>The number of keys held, for tests.</summary>
    internal int KeyCount
    {
        get
        {
            lock (_lock)
            {
                return _keys.Count;
            }
        }
    }

    /// <summary>The number of values held, for tests.</summary>
    internal int ValueCount
    {
        get
        {
            lock (_lock)
            {
                return _values.Count;
            }
        }
    }

    /// <summary>Adds a key and a value, returning their slot values: 0 for a null one.</summary>
    internal void Add(TKey? key, TValue? value, out nint keySlot, out nint valueSlot)
    {
        lock (_lock)
        {
            keySlot = 0;
            valueSlot = 0;
            if (key is not null)
            {
                keySlot = ++_next;
                _keys.Add(keySlot, key);
            }

            if (value is not null)
            {
                valueSlot = ++_next;
                _values.Add(valueSlot, value);
            }
        }
    }

    /// <summary>Removes the entries of a record the core did not take.</summary>
    internal void Remove(nint keySlot, nint valueSlot)
    {
        lock (_lock)
        {
            _keys.Remove(keySlot);
            _values.Remove(valueSlot);
        }
    }

    /// <summary>
    /// Takes, and removes, the key and value of a delivered record. A 0 slot is a null key or value.
    /// </summary>
    /// <exception cref="IllegalStateException">A slot has no entry: the binding's bookkeeping is wrong.</exception>
    internal void Take(nint keySlot, nint valueSlot, string topic, int partition, out TKey? key, out TValue? value)
    {
        key = default;
        value = default;
        lock (_lock)
        {
            if (keySlot != 0)
            {
                if (!_keys.TryGetValue(keySlot, out TKey? found))
                {
                    throw Missing("key", keySlot, topic, partition);
                }

                key = found;
            }

            if (valueSlot != 0)
            {
                if (!_values.TryGetValue(valueSlot, out TValue? found))
                {
                    throw Missing("value", valueSlot, topic, partition);
                }

                value = found;
            }

            // Removed together, once both are found, so a failed take leaves the record's entries in place.
            _keys.Remove(keySlot);
            _values.Remove(valueSlot);
        }
    }

    /// <summary>Drops every entry: the mock is closed.</summary>
    internal void Clear()
    {
        lock (_lock)
        {
            _keys.Clear();
            _values.Clear();
        }
    }

    private static IllegalStateException Missing(string slot, nint id, string topic, int partition) =>
        new IllegalStateException(
            "The mock consumer holds no " + slot + " for id " + ((long)id).ToString(CultureInfo.InvariantCulture)
            + " of a record polled from " + topic + "-" + partition.ToString(CultureInfo.InvariantCulture));
}
