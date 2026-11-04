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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Confluent.Kafka.Producer;
using Google.Protobuf;
using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// <see cref="Translate.ToProto"/>'s wire code for each exception type, Python's
/// <c>_kafka_error_to_proto</c> by type (<c>dotnet/CLAUDE.md</c> §7.6), and the record metadata a send returns,
/// as Python's <c>_record_metadata_to_proto</c> sends it, and the record a send builds from the proto, its headers
/// included.
/// </summary>
public sealed class TranslateTests
{
    [Fact]
    public void AKafkaException_KeepsItsCodeAndMessage()
    {
        KafkaException error = TestValues.Coded(27, "rebalance in progress");

        AssertBare(error, 27);
    }

    [Fact]
    public void AKafkaExceptionSubclass_KeepsItsCodeAndMessage()
    {
        SerializationException error = new SerializationException("bad payload");

        AssertBare(error, error.Code);
    }

    [Fact]
    public void AnArgumentException_IsLocalIllegalArgument_WithItsMessage() =>
        AssertBare(new ArgumentException("bad argument"), -3);

    [Fact]
    public void AnArgumentException_WithAParamName_IsSentWithoutDotNetsParameterSuffix() =>
        AssertBare(new ArgumentException("bad argument", "topic"), -3, "bad argument");

    [Fact]
    public void AnArgumentOutOfRangeException_IsLocalIllegalArgument_WithoutDotNetsParameterSuffix() =>
        AssertBare(new ArgumentOutOfRangeException("timeout", "The timeout cannot be negative."), -3, "The timeout cannot be negative.");

    [Fact]
    public void AnArgumentOutOfRangeException_WithAnActualValue_IsSentWithoutDotNetsSuffixes() =>
        AssertBare(
            new ArgumentOutOfRangeException("timeout", TimeSpan.FromMilliseconds(-1), "Invalid negative timeout -1"),
            -3,
            "Invalid negative timeout -1");

    [Fact]
    public void AnArgumentNullException_IsTheServerFallback()
    {
        ArgumentNullException error = new ArgumentNullException("topic");

        Proto.KafkaError proto = Translate.ToProto(error);

        Assert.Equal(-4, proto.Code);
        Assert.Equal("dotnet server: ArgumentNullException: " + error.Message, proto.Message);
    }

    [Fact]
    public void AnInvalidOperationException_IsLocalIllegalState_WithItsMessage() =>
        AssertBare(new InvalidOperationException("KafkaConsumer is not safe for multi-threaded access."), -4);

    [Fact]
    public void AnObjectDisposedException_IsLocalIllegalState_WithoutDotNetsObjectNameLine() =>
        AssertBare(
            new ObjectDisposedException("KafkaConsumer", "This consumer has already been closed."),
            -4,
            "This consumer has already been closed.");

    [Fact]
    public void ATimeoutException_IsLocalTimeout_WithItsMessage() =>
        AssertBare(new System.TimeoutException("Timeout expired after 500ms"), -5);

    [Fact]
    public void ANotSupportedException_IsLocalUnsupportedOperation_WithItsMessage() =>
        AssertBare(new NotSupportedException("Not implemented yet"), -31);

    [Fact]
    public void AnyOtherException_IsTheServerFallback()
    {
        Proto.KafkaError proto = Translate.ToProto(new FormatException("not a number"));

        Assert.Equal(-4, proto.Code);
        Assert.Equal("dotnet server: FormatException: not a number", proto.Message);
    }

    [Fact]
    public void ARecordMetadata_IsSentWithItsSerializedSizes()
    {
        RecordMetadata metadata =
            new RecordMetadata(new TopicPartition("orders", 4), 40L, 2, 1_700_000_000_000L, 3, 5);

        Proto.RecordMetadata proto = Translate.MetadataToProto(metadata);

        Assert.Equal(42L, proto.Offset);
        Assert.Equal(1_700_000_000_000L, proto.Timestamp);
        Assert.Equal(3, proto.SerializedKeySize);
        Assert.Equal(5, proto.SerializedValueSize);
        Assert.Equal("orders", proto.Topic);
        Assert.Equal(4, proto.Partition);
    }

    [Fact]
    public void AProducerRecord_KeepsTheProtosHeaders_InTheirOrder()
    {
        Proto.ProducerRecord proto = new Proto.ProducerRecord { Topic = "orders", Value = ByteString.CopyFrom(7) };
        proto.Headers.Add(new Proto.Header { Key = "b", Value = ByteString.CopyFrom(1, 2) });
        proto.Headers.Add(new Proto.Header { Key = "trace-é", Value = ByteString.CopyFrom(9) });
        proto.Headers.Add(new Proto.Header { Key = "b", Value = ByteString.CopyFrom(3) });

        ProducerRecord<byte[], byte[]> record = Translate.ProducerRecordFromProto(proto);

        Header[] headers = record.Headers.ToArray();
        Assert.Equal(3, headers.Length);
        Assert.Equal("b", headers[0].Key);
        Assert.Equal(new byte[] { 1, 2 }, headers[0].Value);
        Assert.Equal("trace-é", headers[1].Key);
        Assert.Equal(new byte[] { 9 }, headers[1].Value);
        Assert.Equal("b", headers[2].Key);
        Assert.Equal(new byte[] { 3 }, headers[2].Value);
        Assert.Equal(new byte[] { 7 }, record.Value);
    }

    [Fact]
    public void AProducerRecord_KeepsAnEmptyHeaderValueEmpty()
    {
        Proto.ProducerRecord proto = new Proto.ProducerRecord { Topic = "orders" };
        proto.Headers.Add(new Proto.Header { Key = "empty", Value = ByteString.Empty });

        ProducerRecord<byte[], byte[]> record = Translate.ProducerRecordFromProto(proto);

        Header header = Assert.Single(record.Headers);
        Assert.Equal("empty", header.Key);
        byte[]? value = header.Value;
        Assert.NotNull(value);
        Assert.Empty(value);
    }

    [Fact]
    public void AProducerRecord_WithoutProtoHeaders_HasNoHeaders()
    {
        Proto.ProducerRecord proto = new Proto.ProducerRecord { Topic = "orders", Key = ByteString.CopyFrom(1) };

        ProducerRecord<byte[], byte[]> record = Translate.ProducerRecordFromProto(proto);

        Assert.Empty(record.Headers);
        Assert.Equal("orders", record.Topic);
        Assert.Equal(new byte[] { 1 }, record.Key);
        Assert.Null(record.Value);
    }

    private static void AssertBare(Exception error, int code) => AssertBare(error, code, error.Message);

    private static void AssertBare(Exception error, int code, string message)
    {
        Proto.KafkaError proto = Translate.ToProto(error);

        Assert.Equal(code, proto.Code);
        Assert.Equal(message, proto.Message);
        Assert.DoesNotContain("dotnet server", proto.Message, StringComparison.Ordinal);
    }
}
