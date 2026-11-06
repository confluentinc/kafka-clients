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
using Confluent.Kafka.Common.Requests;
using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Confluent.Kafka.Producer;
using Google.Protobuf;
using Xunit;

using FutureTimeoutException = Confluent.Kafka.TimeoutException;
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

        Assert.IsType<RebalanceInProgressException>(error);
        AssertBare(error, 27);
    }

    [Fact]
    public void ABareKafkaException_IsMinusOne() => AssertBare(new KafkaException("bare"), -1);

    [Fact]
    public void AConcurrentModificationException_IsItsId() =>
        AssertBare(new ConcurrentModificationException("KafkaConsumer is not safe for multi-threaded access."), -2);

    [Fact]
    public void AnIllegalArgumentException_IsItsId() =>
        AssertBare(new IllegalArgumentException("Timeout must not be negative"), -3);

    [Fact]
    public void AnIllegalStateException_IsItsId() =>
        AssertBare(new IllegalStateException("This consumer has already been closed."), -4);

    [Fact]
    public void AJavaTimeoutException_IsItsId() =>
        AssertBare(new FutureTimeoutException("Timeout expired after 500ms"), -5);

    [Fact]
    public void ACorrelationIdMismatchException_IsItsId_NotItsParentsId() =>
        AssertBare(new CorrelationIdMismatchException("mismatch", 1, 2), -24);

    [Fact]
    public void AnUnsupportedOperationException_IsItsId() =>
        AssertBare(new UnsupportedOperationException("Not implemented yet"), -31);

    [Fact]
    public void ASubclassOfAJavaNamedClass_IsItsNearestId() =>
        AssertBare(new DerivedIllegalStateException("derived"), -4);

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

    [Theory]
    [MemberData(nameof(ExceptionsWithoutAnId))]
    public void AnExceptionWithoutAnId_IsTheServerFallback(Exception error)
    {
        Proto.KafkaError proto = Translate.ToProto(error);

        Assert.Equal(-4, proto.Code);
        Assert.Equal($"dotnet server: {error.GetType().Name}: {error.Message}", proto.Message);
    }

    [Fact]
    public void AFuturesExecutionException_IsSentAsTheFailureItCarries()
    {
        KafkaException cause = TestValues.Coded(27, "rebalance in progress");

        AssertBare(new ExecutionException(cause), 27, "rebalance in progress");
        AssertBare(new ExecutionException(new IllegalStateException("closed")), -4, "closed");
        Assert.Same(cause, Translate.Unwrapped(new ExecutionException(cause)));
        Assert.Same(cause, Translate.Unwrapped(cause));
    }

    [Fact]
    public void AnExecutionExceptionWithoutACause_IsTheServerFallback()
    {
        ExecutionException error = new ExecutionException((Exception?)null);

        Proto.KafkaError proto = Translate.ToProto(error);

        Assert.Equal(-4, proto.Code);
        Assert.Equal($"dotnet server: ExecutionException: {error.Message}", proto.Message);
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

    public static TheoryData<Exception> ExceptionsWithoutAnId() => new TheoryData<Exception>
    {
        new InvalidOperationException("the admin result produced no value"),
        new ObjectDisposedException("ConsumerHandle"),
        new NotSupportedException("not supported"),
        new System.TimeoutException("Timeout expired after 500ms"),
        new NoSuchElementException("no such element"),
        new IndexOutOfBoundsException("index 3 out of bounds"),
        new FormatException("not a number"),
    };

    private static void AssertBare(Exception error, int code) => AssertBare(error, code, error.Message);

    private sealed class DerivedIllegalStateException : IllegalStateException
    {
        public DerivedIllegalStateException(string message)
            : base(message)
        {
        }
    }

    private static void AssertBare(Exception error, int code, string message)
    {
        Proto.KafkaError proto = Translate.ToProto(error);

        Assert.Equal(code, proto.Code);
        Assert.Equal(message, proto.Message);
        Assert.DoesNotContain("dotnet server", proto.Message, StringComparison.Ordinal);
    }
}
