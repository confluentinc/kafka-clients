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
/// when it delivers the record, <c>Rebalance</c> and <c>LosePartitions</c> discard those of the records the core
/// discards, as Java's mock drops its record objects with <c>records.clear()</c> and <c>records.remove(tp)</c>, and
/// the native teardown clears the rest.
/// </summary>
/// <remarks>
/// Java's <c>MockConsumer</c> keeps the record objects themselves; the C ABI carries no managed object, so this
/// table is how the .NET mock keeps them. Ids come from one counter, starting at 1 and never reused; 0 is the
/// slot of a null key or value, which stays <c>NULL</c>. Two dictionaries keep value-type keys and values unboxed.
/// One re-entrant lock guards both and the counter (Java's mock is <c>synchronized</c>, and a poll task may add a
/// record inside <c>Poll</c>); it is never held across an await or a native wait.
/// <para>
/// A record is "in the core" from when the core's add has returned: only then can a later discarding call have
/// discarded it. Its entries exist before the add, so a poll on another thread that delivers it finds them, and a
/// discarding call between the two steps never removes them. A discarding call snapshots the records in the core
/// before the native call and discards the snapshot once the core has discarded their records, so a record a
/// listener adds during the call keeps its entries, as Java keeps a record added after its <c>records.clear()</c>.
/// </para>
/// </remarks>
internal sealed class MockRecordSlots<TKey, TValue>
{
    private readonly object _lock = new object();
    private readonly Dictionary<nint, TKey> _keys = new Dictionary<nint, TKey>();
    private readonly Dictionary<nint, TValue> _values = new Dictionary<nint, TValue>();

    // The records in the core, by their first non-zero slot: the key slot, else the value slot.
    private readonly Dictionary<nint, InCore> _inCore = new Dictionary<nint, InCore>();
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
            RemoveLocked(keySlot, valueSlot);
        }
    }

    /// <summary>
    /// Marks a record the core has taken as in the core. A record a poll has already delivered, or one with a null
    /// key and value, has no entry left to mark.
    /// </summary>
    internal void MarkInCore(nint keySlot, nint valueSlot, string topic, int partition)
    {
        lock (_lock)
        {
            if ((keySlot != 0 && _keys.ContainsKey(keySlot)) || (valueSlot != 0 && _values.ContainsKey(valueSlot)))
            {
                _inCore[RecordId(keySlot, valueSlot)] = new InCore(keySlot, valueSlot, topic, partition);
            }
        }
    }

    /// <summary>The records in the core now, for a discarding call to discard once the core has discarded them.</summary>
    internal InCore[] SnapshotInCore()
    {
        lock (_lock)
        {
            InCore[] snapshot = new InCore[_inCore.Count];
            _inCore.Values.CopyTo(snapshot, 0);
            return snapshot;
        }
    }

    /// <summary>
    /// Removes the entries of the snapshot's records, only those of <paramref name="partitions"/> when it is given.
    /// A record a poll has delivered since the snapshot has none left; ids are never reused, so nothing else goes.
    /// </summary>
    internal void Discard(InCore[] snapshot, ICollection<(string Topic, int Partition)>? partitions = null)
    {
        lock (_lock)
        {
            foreach (InCore record in snapshot)
            {
                if (partitions is null || partitions.Contains((record.Topic, record.Partition)))
                {
                    RemoveLocked(record.KeySlot, record.ValueSlot);
                }
            }
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
            RemoveLocked(keySlot, valueSlot);
        }
    }

    /// <summary>Drops every entry: the mock is closed.</summary>
    internal void Clear()
    {
        lock (_lock)
        {
            _keys.Clear();
            _values.Clear();
            _inCore.Clear();
        }
    }

    private static nint RecordId(nint keySlot, nint valueSlot) => keySlot != 0 ? keySlot : valueSlot;

    private void RemoveLocked(nint keySlot, nint valueSlot)
    {
        _keys.Remove(keySlot);
        _values.Remove(valueSlot);
        _inCore.Remove(RecordId(keySlot, valueSlot));
    }

    private static IllegalStateException Missing(string slot, nint id, string topic, int partition) =>
        new IllegalStateException(
            "The mock consumer holds no " + slot + " for id " + ((long)id).ToString(CultureInfo.InvariantCulture)
            + " of a record polled from " + topic + "-" + partition.ToString(CultureInfo.InvariantCulture));

    /// <summary>A record in the core: its slots and its partition.</summary>
    internal readonly struct InCore
    {
        internal InCore(nint keySlot, nint valueSlot, string topic, int partition)
        {
            KeySlot = keySlot;
            ValueSlot = valueSlot;
            Topic = topic;
            Partition = partition;
        }

        internal nint KeySlot { get; }

        internal nint ValueSlot { get; }

        internal string Topic { get; }

        internal int Partition { get; }
    }
}
