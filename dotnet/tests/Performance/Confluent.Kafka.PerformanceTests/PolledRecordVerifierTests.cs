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
using System.Collections.Generic;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// <c>DO_VERIFY</c> for the consumer benchmark: on unless it is <c>False</c>, as the producer harnesses', and the
/// verifier a backend feeds counts the bits set in every key and value byte and the records with a negative
/// partition.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class PolledRecordVerifierTests
{
    [Fact]
    public void DoVerify_Unset_IsOn()
    {
        PerfEngineFixture.Apply(new Dictionary<string, string>(StringComparer.Ordinal));

        Assert.True(ConsumerBenchmarkConfig.FromEnv().DoVerify);
    }

    [Theory]
    [InlineData("True", true)]
    [InlineData("False", false)]
    public void DoVerify_Set_IsRead(string value, bool expected)
    {
        PerfEngineFixture.Apply(new Dictionary<string, string>(StringComparer.Ordinal) { ["DO_VERIFY"] = value });

        Assert.Equal(expected, ConsumerBenchmarkConfig.FromEnv().DoVerify);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(64)]
    [InlineData(1027)]
    public void CountOnes_CountsTheBitsOfEveryByte(int length)
    {
        // Byte i is i % 251, so the whole words and the tail both hold every kind of byte.
        byte[] data = new byte[length];
        long expected = 0;
        for (int i = 0; i < length; i++)
        {
            data[i] = (byte)(i % 251);
            for (int bit = 0; bit < 8; bit++)
            {
                expected += (data[i] >> bit) & 1;
            }
        }

        Assert.Equal(expected, PolledRecordVerifier.CountOnes(data));
    }

    [Fact]
    public void CountOnes_AllBitsSet_IsEightPerByte()
    {
        byte[] data = new byte[19];
        Array.Fill(data, (byte)0xFF);

        Assert.Equal(19 * 8, PolledRecordVerifier.CountOnes(data));
    }

    [Fact]
    public void Verify_AddsTheKeyAndTheValue_AndCountsTheRecord()
    {
        var verifier = new PolledRecordVerifier();

        verifier.Verify(0, new byte[] { 0x01, 0x03 }, new byte[] { 0xFF });
        verifier.Verify(15, default, new byte[] { 0x10 });
        verifier.Verify(3, default, default);

        Assert.Equal(3, verifier.Records);
        Assert.Equal(3 + 8 + 1, verifier.Ones);
        Assert.Equal(0, verifier.Failed);
    }

    [Fact]
    public void Verify_ANegativePartition_IsAFailure_AndItsBytesAreStillRead()
    {
        var verifier = new PolledRecordVerifier();

        verifier.Verify(-1, default, new byte[] { 0x07 });
        verifier.Verify(0, default, new byte[] { 0x01 });

        Assert.Equal(2, verifier.Records);
        Assert.Equal(4, verifier.Ones);
        Assert.Equal(1, verifier.Failed);
    }
}
