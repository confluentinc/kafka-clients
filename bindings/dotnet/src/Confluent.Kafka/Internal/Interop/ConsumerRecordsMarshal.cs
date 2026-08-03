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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The receive-path <b>copy-out</b> marshaller (ffi-marshalling.md §B3/§B4, §6.4). It
/// turns a borrowed native poll batch (<c>ConsumerRecords_t</c>, a Category-3
/// borrow-root) into an owned managed <see cref="ConsumerRecords"/>, copying every
/// field so <b>nothing native-backed escapes</b>. The caller (the poll callback)
/// invokes this on the core's dispatcher thread and then destroys the batch — safely,
/// because after <see cref="CopyOut"/> returns no borrowed pointer is retained.
/// </summary>
/// <remarks>
/// <para>
/// <b>On the dispatcher thread — the key M3/P3 decision (PLAN §"The key design
/// decision").</b> The whole batch lifetime (create → copy-out → destroy) stays inside
/// the callback, so there is no <c>SafeConsumerRecordsHandle</c>, no native-backed
/// <see cref="ReadOnlyMemory{T}"/>, and no leak-on-abandoned-<c>Task</c>. The copy-out
/// is bounded framework work; the user's continuation runs off-thread via
/// <c>RunContinuationsAsynchronously</c>.
/// </para>
/// <para>
/// <b>Borrow discipline (§B2 Category 4).</b> Each <c>ConsumerRecord_t</c> and every
/// key / value / topic / header slice is <b>borrowed</b> — read only during the copy,
/// never freed here. Strings use the <b>length-delimited</b> form
/// (<see cref="Utf8Marshal.PtrToString(IntPtr, int)"/> with <c>out_len</c>) — NEVER a
/// NUL-scan (§B3), which would over-read past the field into the batch.
/// </para>
/// <para>
/// <b>Allocation budget (§B4 / consumer-threading §27 / DoD §10).</b> The only
/// per-record allocations are the owned copies the user receives — the topic
/// <see cref="string"/>, the key / value <c>byte[]</c>, and the header key strings /
/// value arrays — plus the record objects and the backing list. Batch traversal and
/// the borrowed-pointer reads allocate nothing.
/// </para>
/// </remarks>
internal static class ConsumerRecordsMarshal
{
    private static readonly IReadOnlyList<RecordHeader> s_noHeaders = Array.Empty<RecordHeader>();

    /// <summary>
    /// Copies the borrowed native batch <paramref name="records"/> into an owned
    /// <see cref="ConsumerRecords"/>. The caller retains ownership of
    /// <paramref name="records"/> and must destroy it <b>after</b> this returns (it is
    /// never null on the success path the poll callback uses).
    /// </summary>
    internal static ConsumerRecords CopyOut(IntPtr records)
    {
        int count = NativeMethods.ConsumerRecordsCount(records);
        if (count <= 0)
        {
            // Empty (or defensively, a non-positive count): a valid, non-null result
            // with Count == 0 — success, not failure.
            return new ConsumerRecords(Array.Empty<ConsumerRecord>());
        }

        List<ConsumerRecord> list = new List<ConsumerRecord>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr record = NativeMethods.ConsumerRecordsGet(records, i);
            if (record == IntPtr.Zero)
            {
                // Defensive: get() returns null only out of range, which count guards
                // against — skip rather than deref a null borrowed view.
                continue;
            }

            list.Add(CopyRecord(record));
        }

        return new ConsumerRecords(list);
    }

    /// <summary>
    /// Copies one borrowed <c>ConsumerRecord_t</c> into an owned
    /// <see cref="ConsumerRecord"/>. Reads scalars directly and marshals the
    /// length-delimited topic + copy-out key/value/headers.
    /// </summary>
    private static ConsumerRecord CopyRecord(IntPtr record)
    {
        int partition = NativeMethods.ConsumerRecordPartition(record);
        long offset = NativeMethods.ConsumerRecordOffset(record);
        long timestamp = NativeMethods.ConsumerRecordTimestamp(record);
        int timestampType = NativeMethods.ConsumerRecordTimestampType(record);

        // Topic: length-delimited slice → owned string (§B3, never NUL-scan). A record
        // always has a topic, so a null pointer would be a core contract violation; the
        // length-delimited PtrToString maps (Zero, _) → null, which we normalize to
        // empty to keep Topic non-null.
        IntPtr topicPtr = NativeMethods.ConsumerRecordTopic(record, out int topicLen);
        string topic = Utf8Marshal.PtrToString(topicPtr, topicLen) ?? string.Empty;

        ReadOnlyMemory<byte>? key = CopyBytes(NativeMethods.ConsumerRecordKey(record, out int keyLen), keyLen);
        ReadOnlyMemory<byte>? value = CopyBytes(NativeMethods.ConsumerRecordValue(record, out int valueLen), valueLen);
        IReadOnlyList<RecordHeader> headers = CopyHeaders(record);

        return new ConsumerRecord(topic, partition, offset, timestamp, timestampType, key, value, headers);
    }

    /// <summary>
    /// Copies all headers of a borrowed record into an owned list, each key from the
    /// length-delimited slice (§B3) and each value copied out (or <see langword="null"/>).
    /// Returns a shared empty list when the record has no headers (no allocation).
    /// </summary>
    private static IReadOnlyList<RecordHeader> CopyHeaders(IntPtr record)
    {
        int headerCount = NativeMethods.ConsumerRecordHeaderCount(record);
        if (headerCount <= 0)
        {
            return s_noHeaders;
        }

        List<RecordHeader> headers = new List<RecordHeader>(headerCount);
        for (int i = 0; i < headerCount; i++)
        {
            // Header key: length-delimited slice → owned string (§B3). A missing key
            // (out of range) is guarded by headerCount; normalize a null to empty.
            IntPtr keyPtr = NativeMethods.ConsumerRecordHeaderKey(record, i, out int keyLen);
            string key = Utf8Marshal.PtrToString(keyPtr, keyLen) ?? string.Empty;

            ReadOnlyMemory<byte>? value =
                CopyBytes(NativeMethods.ConsumerRecordHeaderValue(record, i, out int valueLen), valueLen);

            headers.Add(new RecordHeader(key, value));
        }

        return headers;
    }

    /// <summary>
    /// Copies a borrowed <c>(ptr, len)</c> byte slice into an owned array, or returns
    /// <see langword="null"/> when the slice is absent (<c>len &lt; 0</c> or a null
    /// pointer — the ABI's absent-key / tombstone sentinel). A non-null pointer with
    /// <c>len == 0</c> is a genuine empty array (distinct from absent).
    /// </summary>
    private static unsafe ReadOnlyMemory<byte>? CopyBytes(IntPtr ptr, int length)
    {
        if (ptr == IntPtr.Zero || length < 0)
        {
            return null;
        }

        // An owned copy — the borrowed slice is invalidated by ConsumerRecords_destroy
        // right after this callback (§B4 copy-out default). Marshal.Copy pins nothing
        // and does not over-read: exactly `length` bytes.
        byte[] buffer = new byte[length];
        if (length > 0)
        {
            Marshal.Copy(ptr, buffer, 0, length);
        }

        return buffer;
    }
}
