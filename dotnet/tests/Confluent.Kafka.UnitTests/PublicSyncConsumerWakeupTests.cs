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
/// M5/P8a — the <b>Wakeup one-shot</b> regression that PROVES the load-bearing blocker: the
/// synchronous <see cref="IConsumer.Poll"/>'s <c>block_on</c> observes
/// <see cref="IConsumerCommon.Wakeup"/>. A <see cref="IConsumerCommon.Wakeup"/> (fired from
/// this thread OR another thread) makes a blocking <see cref="IConsumer.Poll"/> throw a Wakeup
/// <see cref="KafkaException"/> <b>once</b>, then the consumer is reusable — Java's one-shot
/// <c>WakeupException</c> semantics.
/// </summary>
/// <remarks>
/// <para>
/// <b>Blocker mechanism (contract-verified).</b> Sync <c>Consumer_poll</c> does
/// <c>block_on(poll(timeout))</c> — the SAME <c>poll()</c> future the async path awaits —
/// and <c>Consumer_wakeup</c> fires the same rotating wakeup token the async
/// <c>AsyncKafkaConsumer.wakeup</c> uses, so <c>poll()</c> returns <c>Err(Wakeup)</c> when the
/// token cancels, surfaced here as a <see cref="KafkaException"/>.
/// </para>
/// <para>
/// <b>Mock-poll determinism ceiling (documented, source-verified —
/// <c>src/consumer/mock_consumer.rs</c> poll).</b> The mock <c>poll</c> runs to completion
/// <em>synchronously</em> — it records the timeout, drains one poll task, then
/// checks-and-clears the wakeup flag (Step 4), then drains records; it never awaits, so a
/// <c>Poll(30s)</c> does <b>not</b> actually block for 30 s. A genuinely mid-flight interrupt
/// (thread B waking a poll blocked deep inside the fetch) is therefore <b>not reachable</b>
/// broker-free — the same ceiling the async M5/P2–P3 phases recorded. What IS deterministic,
/// and what these tests assert, is that the wakeup flag is <em>sticky</em> until a poll
/// observes-and-clears it: a <see cref="IConsumerCommon.Wakeup"/> from another thread is caught
/// by an actively-polling consumer (a bounded loop), and one-shot then clears. Both tests are
/// bounded by <see cref="TestTimeout"/> so a missed wakeup fails fast, never hangs.
/// </para>
/// </remarks>
public sealed class PublicSyncConsumerWakeupTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    // Cap the cross-thread poll loop below the TestTimeout deadline so a broken wakeup fails
    // via a null assertion (fast) rather than by the outer hang guard.
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
        // Deterministic single-threaded proof: set the one-shot flag (Wakeup), then poll — the
        // mock observes it in Step 4 and faults; the next poll succeeds (flag cleared) and
        // returns the queued record. The canonical broker-free one-shot proof (the async
        // M5/P2–P3 pattern), showing sync Poll's block_on observes the wakeup token.
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();
        consumer.AddRecord(Topic, Partition, offset: 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

        consumer.Wakeup();

        // The faulting poll is called DIRECTLY: the mock poll observes-and-clears the wakeup
        // flag synchronously (Step 4, never blocks), so a broken wakeup fails fast via
        // Assert.Throws (the poll returns empty instead of throwing) rather than hanging — no
        // TestTimeout wrapper needed, and TestTimeout.Run(Action) would surface the fault as an
        // AggregateException. The cross-thread sibling test carries the bounded-loop hang guard.
        KafkaException ex = Assert.Throws<KafkaException>(() => consumer.Poll(s_pollTimeout));
        Assert.False(string.IsNullOrEmpty(ex.Message));

        // One-shot: the flag was cleared by the faulted poll → the next poll returns the record.
        // The success poll keeps the TestTimeout hang guard.
        ConsumerRecords<byte[], byte[]> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);
        Assert.Single(records);
    }

    [Fact]
    public void Wakeup_FromAnotherThreadWhilePolling_FaultsOnce_ThenReusable()
    {
        // The REQUIRED cross-thread proof: thread A actively polls, thread B calls Wakeup() from
        // ANOTHER thread → thread A's poll faults with a Wakeup KafkaException. Because the mock
        // poll does not block (see the type remarks), a single Poll can return before the
        // cross-thread wakeup lands; the flag is STICKY until a poll observes-and-clears it, so a
        // bounded poll loop deterministically catches it. Bounded so a missed wakeup fails fast.
        using MockConsumer<byte[], byte[]> consumer = ReadyToPoll();

        KafkaException? observed = null;
        TestTimeout.Run(
            () =>
            {
                using ManualResetEventSlim pollingStarted = new ManualResetEventSlim(false);

                // Thread B: once thread A signals it is polling, wake it from another thread.
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

                // Thread A (this thread): poll until the cross-thread wakeup is observed.
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

        // Thread A saw the cross-thread Wakeup as a Wakeup KafkaException.
        Assert.NotNull(observed);
        Assert.False(string.IsNullOrEmpty(observed!.Message));

        // One-shot + reusable: the flag was cleared by the faulted poll, so a fresh record polls
        // back (a subsequent poll is not still-woken).
        consumer.AddRecord(Topic, Partition, offset: 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));
        ConsumerRecords<byte[], byte[]> records = default!;
        TestTimeout.Run(() => records = consumer.Poll(s_pollTimeout), s_deadline);
        Assert.Single(records);
    }
}
