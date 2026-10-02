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

namespace Confluent.Kafka;

/// <summary>
/// One record header — the .NET realization of Java's
/// <c>org.apache.kafka.common.header.Header</c> — carrying a <see cref="Key"/> and an
/// optional <see cref="Value"/>. Read-only: headers on a
/// <see cref="ConsumerRecord{TKey, TValue}"/> are owned copies produced by the receive-path copy-out
/// (ffi-marshalling.md §B3/§B4), so nothing native-backed escapes.
/// </summary>
/// <remarks>
/// <b>Why <c>byte[]?</c> for <see cref="Value"/> (deviation from the CLAUDE.md §3
/// <see cref="System.ReadOnlyMemory{T}"/> sketch, PLAN micro-decision A).</b> Java's
/// <c>Header.value()</c> is a raw <c>byte[]</c>, and confluent-kafka-dotnet's message
/// bytes are <c>byte[]</c> — so <c>byte[]?</c> is the Java-faithful shape and unifies
/// the raw-byte surface with <see cref="ConsumerRecord{TKey, TValue}.Key"/> / <c>.Value</c>. It adds
/// no cost: the copy-out already allocates an owned array, and returning it directly
/// <em>removes</em> the <see cref="System.ReadOnlyMemory{T}"/> wrap the internal type
/// used — a net simplification, not a new copy.
/// </remarks>
public sealed class Header
{
    /// <summary>
    /// Initializes a new <see cref="Header"/> with the given key and value.
    /// </summary>
    /// <param name="key">The header key.</param>
    /// <param name="value">
    /// The header value, or <see langword="null"/> when the header has a null value.
    /// </param>
    public Header(string key, byte[]? value)
    {
        Key = key;
        Value = value;
    }

    /// <summary>The header key (an owned copy of the length-delimited slice, §B3).</summary>
    public string Key { get; }

    /// <summary>
    /// The header value as an owned <c>byte[]</c>, or <see langword="null"/> when the
    /// header value is null.
    /// </summary>
    public byte[]? Value { get; }
}
