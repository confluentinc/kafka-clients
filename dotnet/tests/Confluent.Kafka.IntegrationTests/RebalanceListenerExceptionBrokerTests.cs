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
using System.Diagnostics;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Performance.Tests;

using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.IntegrationTests;

/// <summary>
/// The exception a rebalance listener throws, against a broker: <c>Poll</c> and <c>Unsubscribe</c> throw a
/// <see cref="KafkaException"/> as the same instance and wrap any other exception in
/// <c>KafkaException("User rebalance callback throws an error")</c>, while <c>Close</c> wraps both in
/// <c>KafkaException("Failed to close kafka consumer")</c>.
/// </summary>
[Collection(BrokerCollection.Name)]
public sealed class RebalanceListenerExceptionBrokerTests
{
    private static readonly TimeSpan s_poll = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(60);

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public RebalanceListenerExceptionBrokerTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    [SkippableFact]
    public async Task Poll_ThrowsAListenersKafkaExceptionAsTheSameInstance()
    {
        string topic = await NewTopicAsync();
        MyCommitFailedException raised = new MyCommitFailedException("assign boom");
        using KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        try
        {
            consumer.Subscribe(new[] { topic }, new RaisingListener(onAssigned: raised));

            Assert.Same(raised, PollUntilThrows(consumer));
        }
        finally
        {
            consumer.Close(CloseOptions.Timeout(TimeSpan.Zero));
        }
    }

    [SkippableFact]
    public async Task Poll_ThrowsAListenersKafkaExceptionWithItsPayload()
    {
        string topic = await NewTopicAsync();
        TopicAuthorizationException raised = new TopicAuthorizationException(new[] { "t1" });
        using KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        try
        {
            consumer.Subscribe(new[] { topic }, new RaisingListener(onAssigned: raised));

            Exception thrown = PollUntilThrows(consumer);

            Assert.Same(raised, thrown);
            Assert.Equal(new[] { "t1" }, ((TopicAuthorizationException)thrown).UnauthorizedTopics());
        }
        finally
        {
            consumer.Close(CloseOptions.Timeout(TimeSpan.Zero));
        }
    }

    [SkippableFact]
    public async Task Poll_WrapsAListenersOtherException()
    {
        string topic = await NewTopicAsync();
        InvalidOperationException raised = new InvalidOperationException("listener boom");
        using KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        try
        {
            consumer.Subscribe(new[] { topic }, new RaisingListener(onAssigned: raised));

            Exception thrown = PollUntilThrows(consumer);

            Assert.IsType<KafkaException>(thrown);
            Assert.Equal("User rebalance callback throws an error", thrown.Message);
            Assert.Same(raised, thrown.InnerException);
        }
        finally
        {
            consumer.Close(CloseOptions.Timeout(TimeSpan.Zero));
        }
    }

    [SkippableFact]
    public async Task AsyncPoll_ThrowsAListenersWakeupExceptionAsTheSameInstance()
    {
        string topic = await NewTopicAsync();
        WakeupException raised = new WakeupException();
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            Configs(), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        try
        {
            await consumer.Subscribe(new[] { topic }, new RaisingListener(onAssigned: raised));

            Exception? thrown = null;
            Stopwatch elapsed = Stopwatch.StartNew();
            while (thrown is null && elapsed.Elapsed < s_deadline)
            {
                try
                {
                    _ = await consumer.Poll(s_poll);
                }
                catch (Exception caught)
                {
                    thrown = caught;
                }
            }

            Assert.Same(raised, thrown);
        }
        finally
        {
            await consumer.Close(CloseOptions.Timeout(TimeSpan.Zero));
            await consumer.DisposeAsync();
        }
    }

    [SkippableFact]
    public async Task Unsubscribe_ThrowsAListenersKafkaExceptionAsTheSameInstance()
    {
        string topic = await NewTopicAsync();
        MyCommitFailedException raised = new MyCommitFailedException("revoke boom");
        using KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        try
        {
            consumer.Subscribe(new[] { topic }, new RaisingListener(onRevoked: raised));
            PollUntilAssigned(consumer);

            Exception? thrown = Record.Exception(() => consumer.Unsubscribe());

            Assert.Same(raised, thrown);
        }
        finally
        {
            consumer.Close(CloseOptions.Timeout(TimeSpan.Zero));
        }
    }

    [SkippableFact]
    public async Task Close_WrapsAListenersKafkaException()
    {
        string topic = await NewTopicAsync();
        MyCommitFailedException raised = new MyCommitFailedException("revoke boom");
        using KafkaConsumer<byte[], byte[]> consumer = NewConsumer();
        consumer.Subscribe(new[] { topic }, new RaisingListener(onRevoked: raised));
        PollUntilAssigned(consumer);

        KafkaException thrown =
            Assert.Throws<KafkaException>(() => consumer.Close(CloseOptions.Timeout(TimeSpan.FromSeconds(10))));

        Assert.Equal("Failed to close kafka consumer", thrown.Message);
        Assert.Same(raised, thrown.InnerException);
    }

    private static Exception PollUntilThrows(KafkaConsumer<byte[], byte[]> consumer)
    {
        Exception? thrown = null;
        Stopwatch elapsed = Stopwatch.StartNew();
        while (thrown is null && elapsed.Elapsed < s_deadline)
        {
            try
            {
                _ = consumer.Poll(s_poll);
            }
            catch (Exception caught)
            {
                thrown = caught;
            }
        }

        Assert.True(thrown is not null, "Poll never threw the listener's exception");
        return thrown!;
    }

    private static void PollUntilAssigned(KafkaConsumer<byte[], byte[]> consumer)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (consumer.Assignment().Count == 0)
        {
            Assert.True(elapsed.Elapsed < s_deadline, "the consumer never got its assignment");
            _ = consumer.Poll(s_poll);
        }
    }

    private async Task<string> NewTopicAsync()
    {
        await _broker.EnsureStartedAsync(_output);
        string topic = "dotnet-listener-" + Guid.NewGuid().ToString("N").Substring(0, 12);
        await _broker.CreateTopicAsync(topic, partitions: 1);
        return topic;
    }

    private KafkaConsumer<byte[], byte[]> NewConsumer() =>
        new KafkaConsumer<byte[], byte[]>(
            Configs(), Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());

    private Dictionary<string, object?> Configs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = _broker.ExternalBootstrap!,
            ["group.protocol"] = "consumer",
            ["group.id"] = "dotnet-listener-" + Guid.NewGuid().ToString("N").Substring(0, 12),
            ["auto.offset.reset"] = "earliest",
            ["enable.auto.commit"] = "false",
        };

    /// <summary>A <see cref="CommitFailedException"/> subclass, as a user's own Kafka exception.</summary>
    private sealed class MyCommitFailedException : CommitFailedException
    {
        internal MyCommitFailedException(string message)
            : base(message)
        {
        }
    }

    /// <summary>
    /// A listener that throws its exception once, the first time the callback is given partitions, so the test's
    /// own <c>Close</c> does not throw it again.
    /// </summary>
    private sealed class RaisingListener : IConsumerRebalanceListener
    {
        private Exception? _onAssigned;
        private Exception? _onRevoked;

        internal RaisingListener(Exception? onAssigned = null, Exception? onRevoked = null)
        {
            _onAssigned = onAssigned;
            _onRevoked = onRevoked;
        }

        public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
            ThrowOnce(partitions, ref _onAssigned);

        public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions) =>
            ThrowOnce(partitions, ref _onRevoked);

        private static void ThrowOnce(IReadOnlyCollection<TopicPartition> partitions, ref Exception? raised)
        {
            Exception? once = raised;
            if (partitions.Count > 0 && once is not null)
            {
                raised = null;
                throw once;
            }
        }
    }
}
