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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives production's <b>shape-4</b> per-key completion over an index-addressed synthetic
/// table, for the tests written against the pre-M15/P9 result-root walkers.
/// </summary>
/// <remarks>
/// ⚠ Shape 4 hands the callback an <b>owned</b> error, so <see cref="KeyedResultMarshal"/>
/// destroys it. The callers here own their error handles and destroy them themselves, so
/// each index gets a clone — otherwise the caller's own destroy is a double free.
/// </remarks>
internal static class SyntheticPerKeyWalk
{
    /// <summary>Shape 4a: one value-carrying per-key completion per index.</summary>
    internal static void Run<TKey, TValue>(
        KeyedAdminOperation<TKey, TValue> operation,
        int count,
        Func<int, TKey> readKey,
        Func<int, IntPtr> getError,
        Func<int, TValue> readValue)
        where TKey : notnull
    {
        for (int index = 0; index < count; index++)
        {
            KeyedResultMarshal.CompleteKey(
                operation,
                readKey(index),
                new IntPtr(index + 1),
                CloneError(getError(index)),
                value => readValue((int)value - 1),
                static _ => { });
        }
    }

    /// <summary>Shape 4b: one void per-key completion per index.</summary>
    internal static void RunVoid<TKey>(
        VoidKeyedAdminOperation<TKey> operation,
        int count,
        Func<int, TKey> readKey,
        Func<int, IntPtr> getError)
        where TKey : notnull
    {
        for (int index = 0; index < count; index++)
        {
            KeyedResultMarshal.CompleteKey(operation, readKey(index), CloneError(getError(index)));
        }
    }

    private static IntPtr CloneError(IntPtr borrowed)
    {
        if (borrowed == IntPtr.Zero)
        {
            return IntPtr.Zero;
        }

        string message = Utf8Marshal.PtrToString(NativeMethods.Message(borrowed)) ?? string.Empty;
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        return NativeMethods.KafkaErrorNew(NativeMethods.Code(borrowed), pinned.Pointer);
    }
}
