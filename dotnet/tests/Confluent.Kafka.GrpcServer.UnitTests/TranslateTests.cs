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

using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// <see cref="Translate.ToProto"/>'s wire code for each exception type, Python's
/// <c>_kafka_error_to_proto</c> by type (<c>dotnet/CLAUDE.md</c> §7.6).
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
        AssertBare(new TimeoutException("Timeout expired after 500ms"), -5);

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

    private static void AssertBare(Exception error, int code) => AssertBare(error, code, error.Message);

    private static void AssertBare(Exception error, int code, string message)
    {
        Proto.KafkaError proto = Translate.ToProto(error);

        Assert.Equal(code, proto.Code);
        Assert.Equal(message, proto.Message);
        Assert.DoesNotContain("dotnet server", proto.Message, StringComparison.Ordinal);
    }
}
