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
using System.Text;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// The payload round-trip, and — the load-bearing half — that a malformed payload is
/// REPORTED rather than THROWN.
/// </summary>
public sealed class SoakRecordTests
{
    private const string Topic = "t";

    private static readonly SoakRecordDeserializer s_deserializer = new SoakRecordDeserializer();

    [Fact]
    public void SerializeDeserializeRoundTrip()
    {
        var serializer = new SoakRecordSerializer(0);
        byte[] payload = serializer.Serialize(Topic, new SoakRecord(42, 1765432100123, 3))!;

        SoakRecord parsed = s_deserializer.Deserialize(Topic, payload);

        Assert.False(parsed.IsMalformed);
        Assert.Equal(42, parsed.MsgId);
        Assert.Equal(1765432100123, parsed.SendTimeMs);
        Assert.Equal(3, parsed.TxCnt);
    }

    [Fact]
    public void RoundTripSurvivesPadding()
    {
        var serializer = new SoakRecordSerializer(10240);
        byte[] payload = serializer.Serialize(Topic, new SoakRecord(7, 1765432100123, 1))!;

        Assert.Equal(10240, payload.Length);

        SoakRecord parsed = s_deserializer.Deserialize(Topic, payload);
        Assert.False(parsed.IsMalformed);
        Assert.Equal(7, parsed.MsgId);
        Assert.Equal(1765432100123, parsed.SendTimeMs);
        Assert.Equal(1, parsed.TxCnt);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(9L)]
    [InlineData(10L)]
    [InlineData(999999L)]
    [InlineData(12345678901234L)]
    public void PaddingTargetIsExactAcrossMsgIdWidths(long msgId)
    {
        var serializer = new SoakRecordSerializer(50);
        byte[] payload = serializer.Serialize(Topic, new SoakRecord(msgId, 1765432100123, 1))!;
        Assert.Equal(50, payload.Length);
    }

    [Fact]
    public void NoPaddingWhenPrefixExceedsTarget()
    {
        var serializer = new SoakRecordSerializer(4);
        byte[] payload = serializer.Serialize(Topic, new SoakRecord(123456, 1765432100123, 1))!;
        Assert.Equal("123456|1765432100123|1|", Encoding.ASCII.GetString(payload));
    }

    [Fact]
    public void PaddingGrowsSourceBufferBeyondDefault()
    {
        // The Python original's pad source is 10880 bytes and grows on demand; the .NET
        // serializer sizes its buffer from the target, so a very large target must still
        // produce an exact payload rather than a short one.
        var serializer = new SoakRecordSerializer(100000);
        byte[] payload = serializer.Serialize(Topic, new SoakRecord(1, 1765432100123, 1))!;
        Assert.Equal(100000, payload.Length);
    }

    [Fact]
    public void PayloadWhosePaddingContainsSeparatorsStillParses()
    {
        // Python splits with maxsplit=3, so everything after the third '|' is one field.
        // The padding contains no '|' today, but the parse must not depend on that.
        byte[] payload = Encoding.ASCII.GetBytes("5|1765432100123|2|a|b|c");
        SoakRecord parsed = s_deserializer.Deserialize(Topic, payload);
        Assert.False(parsed.IsMalformed);
        Assert.Equal(5, parsed.MsgId);
        Assert.Equal(2, parsed.TxCnt);
    }

    /// <summary>The Python suite's malformed table, byte for byte.</summary>
    public static IEnumerable<object[]> MalformedPayloads()
    {
        yield return new object[] { System.Array.Empty<byte>() };                          // empty
        yield return new object[] { Encoding.ASCII.GetBytes("not-a-soak-record") };        // no separators
        yield return new object[] { Encoding.ASCII.GetBytes("1|2") };                      // too few fields
        yield return new object[] { Encoding.ASCII.GetBytes("1|2|3") };                    // missing trailing separator
        yield return new object[] { Encoding.ASCII.GetBytes("x|2|3|pad") };                // non-numeric msgid
        yield return new object[] { Encoding.ASCII.GetBytes("1|y|3|pad") };                // non-numeric send time
        yield return new object[] { Encoding.ASCII.GetBytes("1|2|z|pad") };                // non-numeric txcnt
        yield return new object[] { new byte[] { 0xFF, 0xFE, (byte)'|', (byte)'2', (byte)'|', (byte)'3', (byte)'|', (byte)'p' } };
        yield return new object[] { Encoding.ASCII.GetBytes("|2|3|pad") };                 // empty msgid
    }

    /// <summary>
    /// ⚠ THE D4 GUARD. A throwing implementation would still pass a naive "it is
    /// rejected" test while breaking the whole fetch batch at runtime, so this asserts
    /// BOTH halves: no exception escapes, AND the result is marked malformed with a
    /// reason. <c>Record.Exception</c> is what makes the no-throw half falsifiable —
    /// <c>Assert.True(result.IsMalformed)</c> alone would never reach its assertion if
    /// the deserializer threw.
    /// </summary>
    [Theory]
    [MemberData(nameof(MalformedPayloads))]
    public void MalformedPayloadIsMarkedNotThrown(byte[] payload)
    {
        SoakRecord? parsed = null;
        System.Exception? thrown = Xunit.Record.Exception(() => parsed = s_deserializer.Deserialize(Topic, payload));

        Assert.Null(thrown);
        Assert.NotNull(parsed);
        Assert.True(parsed!.IsMalformed);
        Assert.False(string.IsNullOrEmpty(parsed.MalformedReason));
    }

    [Fact]
    public void MalformedReasonNamesTheFieldCount()
    {
        SoakRecord parsed = s_deserializer.Deserialize(Topic, Encoding.ASCII.GetBytes("1|2"));
        Assert.Contains("expected 4 '|'-separated fields, got 2", parsed.MalformedReason!, System.StringComparison.Ordinal);
    }

    [Fact]
    public void MalformedReasonNamesANonNumericField()
    {
        SoakRecord parsed = s_deserializer.Deserialize(Topic, Encoding.ASCII.GetBytes("1|y|3|pad"));
        Assert.Contains("non-numeric header field", parsed.MalformedReason!, System.StringComparison.Ordinal);
    }

    [Fact]
    public void MalformedReasonPreviewEscapesNonPrintableBytes()
    {
        SoakRecord parsed = s_deserializer.Deserialize(
            Topic, new byte[] { 0xFF, (byte)'|', (byte)'2', (byte)'|', (byte)'3', (byte)'|' });
        Assert.Contains("\\xff", parsed.MalformedReason!, System.StringComparison.Ordinal);
    }

    /// <summary>
    /// The absent-value case never reaches the deserializer at all (the binding returns
    /// <c>default(TValue)</c> without invoking it), so the constant the consume loop
    /// applies must exist and say what happened.
    /// </summary>
    [Fact]
    public void EmptyPayloadReasonIsAvailableToTheConsumeLoop()
    {
        Assert.Equal("empty payload (null)", SoakRecord.EmptyPayloadReason);
    }

    /// <summary>
    /// The producer serializer is invoke-on-null by design (Java-faithful), so it must
    /// tolerate a null value and return the tombstone sentinel rather than throwing on
    /// the send path.
    /// </summary>
    [Fact]
    public void SerializeOfNullReturnsTheTombstoneSentinel()
    {
        var serializer = new SoakRecordSerializer(50);
        Assert.Null(serializer.Serialize(Topic, null!));
    }
}
