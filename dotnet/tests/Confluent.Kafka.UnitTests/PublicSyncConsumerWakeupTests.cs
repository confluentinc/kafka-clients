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
using System.Diagnostics;
using System.Text;
using System.Threading;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync Poll's wakeup: a <see cref="IConsumerCommon.Wakeup"/> from this thread or another makes a Poll throw the
/// Wakeup <see cref="KafkaException"/> once, then the consumer polls again, as Java's WakeupException.
/// </summary>
/// <remarks>
/// The mock's poll never waits, so the cross-thread test polls in a bounded loop until the wakeup lands; it stays set
/// until a poll consumes it.
/// </remarks>
public sealed class PublicSyncConsumerWakeupTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    // Below the TestTimeout deadline, so a missed wakeup fails an assertion rather than the hang guard.
    private static readonly TimeSpan s_loopBudget = TimeSpan.FromSeconds(20);

    private const string Topic = "sync-wakeup-topic";
    private const int Partition = 0;

    private static MockConsumer<byte[], byte[]> ReadyToPoll()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        return consumer;
    }

    [Fact]
    public void Wakeup_ThenPoll_FaultsOnce_ThenReusable()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, Partition, offset: 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

        consumer.Wakeup();

        // Called directly: TestTimeout.Run would wrap the throw in an AggregateException.
        KafkaException ex = Assert.Throws<KafkaException>(() => consumer.Poll(s_pollTimeout));
        Assert.False(string.IsNullOrEmpty(ex.Message));

        ConsumerRecords<byte[], byte[]> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);
        Assert.Single(records);
    }

    [Fact]
    public void Wakeup_FromAnotherThreadWhilePolling_FaultsOnce_ThenReusable()
    {
        MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        KafkaException? observed = null;
        TestTimeout.Run(
            () =>
            {
                using ManualResetEventSlim pollingStarted = new ManualResetEventSlim(false);

                Thread waker = new Thread(() =>
                {
                    pollingStarted.Wait();
                    consumer.Wakeup();
                })
                {
                    IsBackground = true,
                    Name = "sync-wakeup-waker",
                };
                waker.Start();

                pollingStarted.Set();
                Stopwatch stopwatch = Stopwatch.StartNew();
                while (stopwatch.Elapsed < s_loopBudget)
                {
                    try
                    {
                        consumer.Poll(s_pollTimeout);
                    }
                    catch (KafkaException ex)
                    {
                        observed = ex;
                        break;
                    }
                }

                waker.Join();
            },
            s_deadline);

        Assert.NotNull(observed);
        Assert.False(string.IsNullOrEmpty(observed!.Message));

        consumer.AddRecord(Topic, Partition, offset: 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));
        ConsumerRecords<byte[], byte[]> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);
        Assert.Single(records);
    }
}
