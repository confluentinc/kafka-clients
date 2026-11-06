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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Xunit;

using KafkaTimeoutException = Confluent.Kafka.Common.Errors.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The transaction and idempotence tests of Java's <c>KafkaProducerTest</c> that run without a broker; the ones that
/// need Java's <c>MockClient</c> are listed as skipped, with the reason.
/// </summary>
public sealed class KafkaProducerTransactionTests
{
    private const string NeedsMockClient =
        "needs Java's MockClient to script the coordinator's responses; the binding cannot inject a fake network " +
        "into the core";

    private const string InitTransactionsTimeout =
        "InitTransactions timed out - did not complete coordinator discovery or receive the InitProducerId response " +
        "within max.block.ms.";

    private const string TransactionalIdWithoutIdempotence =
        "Cannot set a transactional.id without also enabling idempotence.";

    private const string AcksNotAll =
        "Must set acks to all in order to use the idempotent producer. Otherwise we cannot guarantee idempotence.";

    private const string RetriesZero = "Must set retries to non-zero when using the idempotent producer.";

    private const string InflightAboveFive =
        "To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. " +
        "Current value is 6.";

    private const int ConfigExceptionCode = -10;

    [Fact]
    public void TestNullGroupMetadataInSendOffsets()
    {
        // Java first runs initTransactions and beginTransaction against a MockClient; the null check comes first in
        // the core, so the call is made at once.
        using KafkaProducer<byte[], byte[]> producer = Producer(new Dictionary<string, object?>
        {
            ["transactional.id"] = "some.id",
            ["max.block.ms"] = 10000,
        });

        IllegalArgumentException exception = Assert.Throws<IllegalArgumentException>(
            () => producer.SendOffsetsToTransaction(new Dictionary<TopicPartition, OffsetAndMetadata>(), null!));
        Assert.Equal("Consumer group metadata could not be null", exception.Message);
        producer.Close(TimeSpan.Zero);
    }

    [Fact]
    public void TestInitTransactionTimeout()
    {
        // Java then scripts the coordinator's responses and retries successfully; only the timeout is portable.
        using KafkaProducer<byte[], byte[]> producer = Producer(new Dictionary<string, object?>
        {
            ["transactional.id"] = "bad-transaction",
            ["max.block.ms"] = 500,
        });

        KafkaTimeoutException exception = Assert.Throws<KafkaTimeoutException>(producer.InitTransactions);
        Assert.Contains(InitTransactionsTimeout, exception.Message);
        producer.Close(TimeSpan.Zero);
    }

    // Java's valid cases also read ProducerConfig's getters; the binding has no config getters, so a valid case
    // asserts only that construction succeeds. The values are Java's strings, which the binding accepts as Java does.
    [Theory]
    [InlineData("0", "false", null, null)]
    [InlineData(null, null, "transactionalId", null)]
    [InlineData("all", "false", null, null)]
    [InlineData("0", null, null, null)]
    [InlineData("1", null, null, null)]
    [InlineData("0", "false", "transactionalId", TransactionalIdWithoutIdempotence)]
    [InlineData("1", "true", null, AcksNotAll)]
    [InlineData("0", null, "transactionalId", TransactionalIdWithoutIdempotence)]
    public void TestAcksAndIdempotenceForIdempotentProducers(
        string? acks, string? idempotence, string? transactionalId, string? expected) =>
        AssertConfig("acks", acks, idempotence, transactionalId, expected);

    [Theory]
    [InlineData("0", "false", null, null)]
    [InlineData("0", null, null, null)]
    [InlineData("0", "false", "transactionalId", TransactionalIdWithoutIdempotence)]
    [InlineData("0", "true", null, RetriesZero)]
    [InlineData("0", null, "transactionalId", TransactionalIdWithoutIdempotence)]
    public void TestRetriesAndIdempotenceForIdempotentProducers(
        string? retries, string? idempotence, string? transactionalId, string? expected) =>
        AssertConfig("retries", retries, idempotence, transactionalId, expected);

    [Theory]
    [InlineData("6", "false", null, null)]
    [InlineData("6", null, null, InflightAboveFive)]
    [InlineData("5", "false", "transactionalId", TransactionalIdWithoutIdempotence)]
    [InlineData("6", "true", null, InflightAboveFive)]
    [InlineData("6", null, "transactionalId", InflightAboveFive)]
    public void TestInflightRequestsAndIdempotenceForIdempotentProducers(
        string? inflight, string? idempotence, string? transactionalId, string? expected) =>
        AssertConfig("max.in.flight.requests.per.connection", inflight, idempotence, transactionalId, expected);

    [Fact(Skip = "needs ProducerConfig's getters (acks, retries), which the binding does not expose")]
    public void TestOverwriteAcksAndRetriesForIdempotentProducers()
    {
    }

    [Fact(Skip = "needs ConsumerGroupMetadata's deprecated public constructor, which the binding does not expose")]
    public void TestInvalidGenerationIdAndMemberIdCombinedInSendOffsets()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestInitTransactionsResponseAfterTimeout()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestInitTransactionWhileThrottled()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestAbortTransaction()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestTransactionV2ProduceWithConcurrentTransactionError()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestMeasureAbortTransactionDuration()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestCommitTransactionWithRecordTooLargeException()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestCommitTransactionWithMetadataTimeoutForMissingTopic()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestCommitTransactionWithMetadataTimeoutForPartitionOutOfRange()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestCommitTransactionWithSendToInvalidTopic()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestSendTxnOffsetsWithGroupId()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestSendTxnOffsetsWithGroupIdTransactionV2()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestSendOffsetsToTransactionNegotiatesV6WhenMetadataKnowsTopicId()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestSendOffsetsToTransactionTriggersMetadataRefreshThenNegotiatesV6()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestTransactionV2Produce()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestMeasureTransactionDurations()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestSendTxnOffsetsWithGroupMetadata()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestTransactionalMethodThrowsWhenSenderClosed()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestPartitionAddedToTransaction()
    {
    }

    [Fact(Skip = NeedsMockClient)]
    public void TestProducerSendOffsetsToTransactionBootstrapResolutionExceptionPropagated()
    {
    }

    private static void AssertConfig(
        string key, string? value, string? idempotence, string? transactionalId, string? expected)
    {
        Dictionary<string, object?> configs = new Dictionary<string, object?>();
        if (value is not null)
        {
            configs[key] = value;
        }

        if (idempotence is not null)
        {
            configs["enable.idempotence"] = idempotence;
        }

        if (transactionalId is not null)
        {
            configs["transactional.id"] = transactionalId;
        }

        if (expected is null)
        {
            using KafkaProducer<byte[], byte[]> producer = Producer(configs);
            producer.Close(TimeSpan.Zero);
            return;
        }

        ConfigException exception = Assert.Throws<ConfigException>(() => Producer(configs).Dispose());
        Assert.Equal(expected, exception.Message);
        Assert.Equal(ConfigExceptionCode, exception.Code);
    }

    private static KafkaProducer<byte[], byte[]> Producer(Dictionary<string, object?> configs)
    {
        configs["bootstrap.servers"] = "localhost:1";
        return new KafkaProducer<byte[], byte[]>(configs, Serdes.ByteArray, Serdes.ByteArray);
    }
}
