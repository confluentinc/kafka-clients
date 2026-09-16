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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The blittable <see cref="LayoutKind.Sequential"/> mirror of the ABI's
/// <c>kafka_producer_ProducerRecord_t</c> (<c>src/ffi/producer.rs</c>, <c>#[repr(C)]</c>) — the one
/// input struct <c>kafka_producer_Producer_send_batch</c> reads (M11/P3.1 slice S1, PLAN §4.1).
/// Every field is a fixed-width scalar or a raw pointer, so the C# sequential layout is byte-identical
/// to the Rust <c>repr(C)</c> one and the runtime marshals it by <b>blitting</b>, with no per-field
/// conversion (ffi §0.1's <c>ProducerRecord_t</c> row: "Fixed-width ⇒ identical layout").
/// </summary>
/// <remarks>
/// <para>
/// <b>The ABI's field conventions, reproduced exactly</b> (the header's "Field Conventions" block):
/// <list type="bullet">
/// <item><see cref="Partition"/> — <c>-1</c> means "no partition specified" (let the producer choose).</item>
/// <item><see cref="Timestamp"/> — <c>-1</c> means "no timestamp" (let the producer stamp it);
/// <c>&gt;= 0</c> is milliseconds since epoch.</item>
/// <item><see cref="KeyLength"/> / <see cref="ValueLength"/> — <c>-1</c> means <b>absent</b>
/// (no key / a tombstone); <c>&gt;= 0</c> means the matching pointer must be
/// <b>non-null</b> and valid for that many bytes. The core rejects a null pointer with a
/// non-negative length as <c>InvalidRequest</c> (<c>src/ffi/producer.rs</c> — the null-key and
/// null-value guards inside <c>send_batch_inner</c>), which is why an <b>empty</b> (zero-length)
/// key/value must pass a non-null sentinel pointer rather than the null a <c>fixed</c> over an
/// empty span yields (ffi §A4; <see cref="ProducerSendBatchMarshal"/> applies it).</item>
/// </list>
/// </para>
/// <para>
/// <b>No headers field.</b> The ABI struct has none, so the binding's record carries none either
/// (CLAUDE.md §3's "clipped to today's ABI"). This is not an omission to fix here — adding headers
/// would be a Mode-B ABI change.
/// </para>
/// <para>
/// <b>Mutable fields, deliberately.</b> The batch marshaller fills an array of these in place, one
/// element per record, so a <c>readonly struct</c> with a constructor would force a copy per record
/// on the send hot path (DoD §10). The type is <c>internal</c> and confined to
/// <c>Internal/Interop/</c>, so the exposed-field guidance (CA1051, which targets externally-visible
/// types) does not apply.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Sequential)]
internal struct ProducerRecordNative
{
    /// <summary>A pinned, NUL-terminated UTF-8 topic name (<c>const char *</c>).</summary>
    internal IntPtr Topic;

    /// <summary>The target partition, or <c>-1</c> for "let the producer choose".</summary>
    internal int Partition;

    /// <summary>The timestamp in ms since epoch, or <c>-1</c> for "let the producer stamp it".</summary>
    internal long Timestamp;

    /// <summary>A pinned key buffer, or <see cref="IntPtr.Zero"/> when <see cref="KeyLength"/> is <c>-1</c>.</summary>
    internal IntPtr Key;

    /// <summary>The key length in bytes, or <c>-1</c> for an absent key.</summary>
    internal int KeyLength;

    /// <summary>A pinned value buffer, or <see cref="IntPtr.Zero"/> when <see cref="ValueLength"/> is <c>-1</c>.</summary>
    internal IntPtr Value;

    /// <summary>The value length in bytes, or <c>-1</c> for an absent value (a tombstone).</summary>
    internal int ValueLength;
}
