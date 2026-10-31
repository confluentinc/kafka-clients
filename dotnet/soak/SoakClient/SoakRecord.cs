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
using Confluent.Kafka.Common.Serialization;

namespace Confluent.Kafka.Soak;

/// <summary>
/// A private record type carrying its metadata in the value (<c>{msgid}|{send_time_ms}|{txcnt}|</c>
/// plus padding), byte-identical to the Python soak's so runs stay comparable; a malformed payload
/// decodes to an <see cref="IsMalformed"/> instance.
/// </summary>
internal sealed class SoakRecord
{
    // Applied by the consume loop, as the deserializer is not called for a null value; Python's
    // deserialize(None) raises the same reason.
    internal const string EmptyPayloadReason = "empty payload (null)";

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

    internal long MsgId { get; }

    internal long SendTimeMs { get; }

    internal long TxCnt { get; }

    internal bool IsMalformed { get; }

    internal string? MalformedReason { get; }

    internal static SoakRecord Malformed(string reason) => new SoakRecord(reason);

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
/// Encodes a <see cref="SoakRecord"/>, padded to the profile's target size (a longer record stays
/// unpadded); the target is per-instance state, where Python keeps it on the class.
/// </summary>
internal sealed class SoakRecordSerializer : ISerializer<SoakRecord>
{
    private static readonly byte[] s_padUnit = Encoding.ASCII.GetBytes(" SoakRecord nr #0");

    private readonly byte[] _padSource;
    private readonly int _targetSize;

    internal SoakRecordSerializer(int targetSize)
    {
        _targetSize = Math.Max(0, targetSize);

        // One pad buffer per serializer, so the send path allocates no padding per record.
        int repetitions = (_targetSize / s_padUnit.Length) + 1;
        var pad = new byte[repetitions * s_padUnit.Length];
        for (int i = 0; i < repetitions; i++)
        {
            Buffer.BlockCopy(s_padUnit, 0, pad, i * s_padUnit.Length, s_padUnit.Length);
        }

        _padSource = pad;
    }

    public byte[]? Serialize(string topic, SoakRecord data)
    {
        // The producer calls the serializer for a null value too, as Java; the soak sends none.
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
/// Decodes a <see cref="SoakRecord"/> from the fetch batch and never throws: a malformed payload
/// decodes to a marked record, which the consume loop counts and keeps out of the duplicate and gap
/// accounting, as Python's soak counts its per-record <c>ValueError</c>.
/// </summary>
internal sealed class SoakRecordDeserializer : IDeserializer<SoakRecord>
{
    private const int PreviewBytes = 64;

    // Total, where Python parses a memoryview in the loop: a throw would make Poll seek back to
    // this record and throw on it on every poll, and a byte[] value would copy every record.
    public SoakRecord Deserialize(string topic, ReadOnlySpan<byte> data)
    {
        // As Python's split with maxsplit 3: a '|' in the padding belongs to the fourth field.
        int first = data.IndexOf((byte)'|');
        int second = first < 0 ? -1 : IndexOfFrom(data, first + 1);
        int third = second < 0 ? -1 : IndexOfFrom(data, second + 1);

        if (third < 0)
        {
            // Python's split count: one more than the separators found.
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

    // Stricter than Python's int(), which allows surrounding whitespace: it can only turn a payload
    // Python accepts into a counted consumer.msgerr.
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

            if (accumulated > (long.MaxValue - (digit - (byte)'0')) / 10)
            {
                return false;
            }

            accumulated = (accumulated * 10) + (digit - (byte)'0');
        }

        value = negative ? -accumulated : accumulated;
        return true;
    }

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
