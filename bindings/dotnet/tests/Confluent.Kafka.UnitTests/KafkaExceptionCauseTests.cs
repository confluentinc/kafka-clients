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

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="Exception.InnerException"/> is Java's <c>getCause()</c> (M15/P13.3, D11):
/// <c>KafkaException.FromBorrowedHandle</c> reads <c>kafka_common_Error_cause</c> and builds
/// the inner exception from that <b>owned</b> copy with <c>FromHandle</c>, recursing until
/// the core returns null.
/// </summary>
/// <remarks>
/// The broker-free vehicle is admin-client construction: the core wraps any construction
/// failure as Java does — <c>new KafkaException("Failed to create new KafkaAdminClient",
/// exc)</c> (<c>KafkaAdminClient.java:569-573</c>) — keeping the underlying failure as the
/// cause. An unparseable <c>bootstrap.servers</c> entry is the failure the core's own test
/// (<c>construction_failures_are_wrapped_as_a_kafka_error</c>) uses. The no-cause half runs
/// against the real native too, in <c>PublicAdminRemoveMembersFromConsumerGroupTests</c>
/// (the mock's refusal carries no cause).
/// </remarks>
public sealed class KafkaExceptionCauseTests
{
    // The core's bare KafkaError (Error::kafka_message_source) has no protocol code of its
    // own, so the ABI reports UNKNOWN_SERVER_ERROR — the code the header also names for the
    // removeMembersFromConsumerGroup remove-all wrap built the same way.
    private const int UnknownServerErrorCode = -1;

    // kafka_common_ErrorCode_CONFIG in the header: the core's Error::Config, which
    // client_utils.rs raises for the bad URL as Java's parseAndValidateAddresses throws
    // ConfigException.
    private const int ConfigErrorCode = -10;

    private const string BadBootstrap = "not-a-host-port";

    private static KafkaException CreateWithBadBootstrap() =>
        Assert.Throws<KafkaException>(
            () => new KafkaAdminClient(new Dictionary<string, string> { ["bootstrap.servers"] = BadBootstrap }));

    [Fact]
    public void AdminClientCreate_BadBootstrap_CarriesTheCoreCauseAsInnerException()
    {
        KafkaException outer = CreateWithBadBootstrap();

        Assert.Equal(UnknownServerErrorCode, outer.Code);
        Assert.Equal("Failed to create new KafkaAdminClient", outer.Message);
        Assert.False(outer.IsRetriable);

        // Exactly KafkaException — the flat model has no subclass for a cause either.
        KafkaException inner = Assert.IsType<KafkaException>(outer.InnerException);
        Assert.Equal(ConfigErrorCode, inner.Code);
        Assert.Equal("Invalid url in bootstrap.servers: " + BadBootstrap, inner.Message);
        Assert.False(inner.IsRetriable);

        // The chain terminates: the cause has no cause of its own, so the recursion's
        // second kafka_common_Error_cause returned null.
        Assert.Null(inner.InnerException);
    }

    [Fact]
    public void AdminClientCreate_BadBootstrap_RepeatedManyTimes_NoDoubleFreeCorruption()
    {
        // Two handles are freed per failure now — the outer error and its owned cause copy,
        // each by its own FromHandle finally. A double free (the cause destroyed by the
        // parent's free as well, or freed twice) would corrupt the native allocator and
        // crash under repetition; the existing single-handle guard is
        // KafkaExceptionTests.ErrorPath_RepeatedManyTimes_NoDoubleFreeCorruption.
        for (int i = 0; i < 200; i++)
        {
            KafkaException outer = CreateWithBadBootstrap();
            Assert.NotNull(outer.InnerException);
        }
    }
}
