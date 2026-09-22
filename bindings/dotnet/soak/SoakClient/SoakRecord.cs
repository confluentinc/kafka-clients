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

using System;
using System.Globalization;
using System.Text;

namespace Confluent.Kafka.Soak;

/// <summary>
/// A private record type carrying its own metadata in the value payload:
/// <c>{msgid}|{send_time_ms}|{txcnt}|</c> + padding, ASCII decimal, byte-identical to
/// the Python soak's format.
/// <para>
/// The reference librdkafka soak stamps msgid / time / txcnt as record <b>headers</b>.
/// Neither this binding nor the Python one can: <c>ProducerRecord</c> carries only
/// <c>{topic, value, key, partition, timestamp}</c> and the underlying
/// <c>kafka_producer_ProducerRecord_t</c> has no headers field at all. Producing headers
/// is a client-side gap tracked separately; the soak therefore carries the same three
/// fields inside the value, which costs nothing and keeps the end-to-end latency
/// measurement intact.
/// </para>
/// <para>
/// A malformed payload is represented as a <see cref="IsMalformed"/> instance rather
/// than an exception — see <see cref="SoakRecordDeserializer"/> for why that is load-bearing.
/// </para>
/// </summary>
internal sealed class SoakRecord
{
    /// <summary>
    /// The reason recorded for an absent value. The deserializer is never called for one
    /// (the binding returns <c>default(TValue)</c> for an absent value without invoking
    /// it), so the consume loop applies this text itself — the Python soak's
    /// <c>deserialize(None)</c> raises <c>ValueError("empty payload (None)")</c> and is
    /// counted the same way.
    /// </summary>
    internal const string EmptyPayloadReason = "empty payload (null)";

    /// <summary>Creates a well-formed record.</summary>
    internal SoakRecord(long msgId, long sendTimeMs, long txCnt)
    {
        MsgId = msgId;
        SendTimeMs = sendTimeMs;
        TxCnt = txCnt;
    }

    private SoakRecord(string malformedReason)
    {
        IsMalformed = true;
        MalformedReason = malformedReason;
    }

    /// <summary>The monotonically increasing message id the producer assigned.</summary>
    internal long MsgId { get; }

    /// <summary>Wall-clock epoch milliseconds at which the send was attempted.</summary>
    internal long SendTimeMs { get; }

    /// <summary>Send ATTEMPTS for this message id (1 on the first attempt), not a retry count.</summary>
    internal long TxCnt { get; }

    /// <summary>Whether this instance stands in for a payload that could not be parsed.</summary>
    internal bool IsMalformed { get; }

    /// <summary>Why the payload could not be parsed; <see langword="null"/> for a well-formed record.</summary>
    internal string? MalformedReason { get; }

    /// <summary>Builds the marked instance a malformed payload decodes to.</summary>
    internal static SoakRecord Malformed(string reason) => new SoakRecord(reason);

    /// <inheritdoc/>
    public override string ToString() =>
        IsMalformed
            ? string.Format(CultureInfo.InvariantCulture, "SoakRecord(malformed: {0})", MalformedReason)
            : string.Format(
                CultureInfo.InvariantCulture,
                "SoakRecord(msgid={0}, send_time_ms={1}, txcnt={2})",
                MsgId,
                SendTimeMs,
                TxCnt);
}

/// <summary>
/// Encodes a <see cref="SoakRecord"/> to its wire bytes, padded to the profile's target
/// serialized size so <c>848-hi-throughput-*</c> exercises ~10 KB records at the same
/// message rate. Records whose prefix already exceeds the target are emitted unpadded.
/// <para>
/// ⚠ RECORDED DEVIATION from the Python original, which holds the target size as
/// <b>class</b> state (<c>SoakRecord.configure_padding</c>) because it has no serializer
/// object to hang it on. .NET does: <c>ISerializer&lt;T&gt;</c> is exactly where the
/// encoding's configuration belongs, and per-instance state also keeps the unit tests
/// independent without the reset fixture the Python suite needs.
/// </para>
/// </summary>
internal sealed class SoakRecordSerializer : ISerializer<SoakRecord>
{
    private static readonly byte[] s_padUnit = Encoding.ASCII.GetBytes(" SoakRecord nr #0");

    private readonly byte[] _padSource;
    private readonly int _targetSize;

    /// <summary>Creates a serializer padding every record to <paramref name="targetSize"/> bytes.</summary>
    internal SoakRecordSerializer(int targetSize)
    {
        _targetSize = Math.Max(0, targetSize);

        // One pad buffer per serializer, sized once: the send path must not allocate a
        // padding buffer per record (root CLAUDE.md §11 — this is the soak's own hot path
        // even though the binding's allocation budget does not cover it).
        int repetitions = (_targetSize / s_padUnit.Length) + 1;
        var pad = new byte[repetitions * s_padUnit.Length];
        for (int i = 0; i < repetitions; i++)
        {
            Buffer.BlockCopy(s_padUnit, 0, pad, i * s_padUnit.Length, s_padUnit.Length);
        }

        _padSource = pad;
    }

    /// <inheritdoc/>
    public byte[]? Serialize(string topic, SoakRecord data)
    {
        // Java-faithful invoke-on-null (bindings/dotnet/CLAUDE.md §4): the producer calls
        // the serializer even for a null value, and a null return is the tombstone
        // sentinel. The soak never sends one; handling it keeps this total.
        if (data is null)
        {
            return null;
        }

        string prefix = string.Format(
            CultureInfo.InvariantCulture,
            "{0}|{1}|{2}|",
            data.MsgId,
            data.SendTimeMs,
            data.TxCnt);

        int prefixLength = Encoding.ASCII.GetByteCount(prefix);
        int padding = _targetSize - prefixLength;
        if (padding <= 0)
        {
            return Encoding.ASCII.GetBytes(prefix);
        }

        var buffer = new byte[prefixLength + padding];
        Encoding.ASCII.GetBytes(prefix, 0, prefix.Length, buffer, 0);
        Buffer.BlockCopy(_padSource, 0, buffer, prefixLength, padding);
        return buffer;
    }
}

/// <summary>
/// Decodes a <see cref="SoakRecord"/> from the fetch batch. <b>This deserializer is
/// TOTAL: it never throws.</b>
/// <para>
/// ⚠ That is the single highest-risk contract in this client, and it is not an
/// optimization. <c>bindings/dotnet/CLAUDE.md §4</c>: any <c>IDeserializer&lt;T&gt;</c>
/// throw is wrapped in a <c>SerializationException</c> and <b>faults the whole
/// <c>Poll</c></b>. The Python soak catches its <c>ValueError</c> <i>per record</i>,
/// counts <c>consumer.msgerr</c> and continues with the rest of the batch — so a
/// throwing implementation here would convert "one corrupt payload" into "the entire
/// fetch batch is lost and the poll failed", corrupting the very accounting the soak
/// exists to produce. On any malformed input it returns a marked
/// <see cref="SoakRecord"/> instead, which the consume loop counts and — exactly as
/// Python does — keeps out of the high-water-mark / duplicate / gap accounting, because
/// a bad payload is not a gap.
/// </para>
/// <para>
/// REJECTED ALTERNATIVE: deserialize the value as <c>byte[]</c> (<c>Serdes.ByteArray</c>)
/// and parse in the consume loop, which is the most literal port and sidesteps the
/// problem entirely. Rejected because it reintroduces a per-record <c>byte[]</c> copy
/// that the span-based <c>IDeserializer</c> hook exists to avoid, while the total-
/// deserializer shape preserves Python's semantics exactly.
/// </para>
/// </summary>
internal sealed class SoakRecordDeserializer : IDeserializer<SoakRecord>
{
    private const int PreviewBytes = 64;

    /// <inheritdoc/>
    public SoakRecord Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        // Python splits with a maxsplit of 3, so a payload whose PADDING contains '|' is
        // still well-formed: everything after the third separator is one field.
        int first = data.IndexOf((byte)'|');
        int second = first < 0 ? -1 : IndexOfFrom(data, first + 1);
        int third = second < 0 ? -1 : IndexOfFrom(data, second + 1);

        if (third < 0)
        {
            // Python reports the SPLIT COUNT, which is one more than the number of
            // separators found — `b""` is 1 field, `b"1|2"` is 2, `b"1|2|3"` is 3.
            int fields = second >= 0 ? 3 : (first >= 0 ? 2 : 1);
            return SoakRecord.Malformed(string.Format(
                CultureInfo.InvariantCulture,
                "malformed payload: expected 4 '|'-separated fields, got {0} in {1}",
                fields,
                Preview(data)));
        }

        if (!TryParseAsciiInteger(data.Slice(0, first), out long msgId)
            || !TryParseAsciiInteger(data.Slice(first + 1, second - first - 1), out long sendTimeMs)
            || !TryParseAsciiInteger(data.Slice(second + 1, third - second - 1), out long txCnt))
        {
            return SoakRecord.Malformed(string.Format(
                CultureInfo.InvariantCulture,
                "malformed payload: non-numeric header field in {0}",
                Preview(data)));
        }

        return new SoakRecord(msgId, sendTimeMs, txCnt);
    }

    private static int IndexOfFrom(ReadOnlySpan<byte> data, int start)
    {
        if (start >= data.Length)
        {
            return -1;
        }

        int relative = data.Slice(start).IndexOf((byte)'|');
        return relative < 0 ? -1 : start + relative;
    }

    /// <summary>
    /// Parses an ASCII decimal integer with an optional leading sign. Deliberately
    /// stricter than Python's <c>int()</c>, which also tolerates surrounding whitespace:
    /// nothing this soak produces carries any, and a stricter parse can only turn a
    /// payload Python would have accepted into a counted <c>consumer.msgerr</c>, never
    /// the reverse.
    /// </summary>
    private static bool TryParseAsciiInteger(ReadOnlySpan<byte> field, out long value)
    {
        value = 0;
        if (field.Length == 0)
        {
            return false;
        }

        int index = 0;
        bool negative = false;
        if (field[0] == (byte)'-' || field[0] == (byte)'+')
        {
            negative = field[0] == (byte)'-';
            index = 1;
            if (field.Length == 1)
            {
                return false;
            }
        }

        long accumulated = 0;
        for (; index < field.Length; index++)
        {
            byte digit = field[index];
            if (digit < (byte)'0' || digit > (byte)'9')
            {
                return false;
            }

            // A payload long enough to overflow is corrupt by definition; report it the
            // same way as any other unparseable field rather than wrapping silently.
            if (accumulated > (long.MaxValue - (digit - (byte)'0')) / 10)
            {
                return false;
            }

            accumulated = (accumulated * 10) + (digit - (byte)'0');
        }

        value = negative ? -accumulated : accumulated;
        return true;
    }

    /// <summary>
    /// The first <see cref="PreviewBytes"/> bytes rendered for a log line, approximating
    /// Python's <c>{!r}</c> bytes repr: printable ASCII verbatim, everything else as
    /// <c>\xNN</c>.
    /// </summary>
    private static string Preview(ReadOnlySpan<byte> data)
    {
        int length = Math.Min(data.Length, PreviewBytes);
        var sb = new StringBuilder(length + 8);
        sb.Append("b'");
        for (int i = 0; i < length; i++)
        {
            byte b = data[i];
            if (b == (byte)'\\')
            {
                sb.Append("\\\\");
            }
            else if (b == (byte)'\'')
            {
                sb.Append("\\'");
            }
            else if (b >= 0x20 && b < 0x7F)
            {
                sb.Append((char)b);
            }
            else
            {
                sb.Append("\\x").Append(b.ToString("x2", CultureInfo.InvariantCulture));
            }
        }

        sb.Append('\'');
        return sb.ToString();
    }
}
