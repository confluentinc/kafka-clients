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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Performance.Tests;

using Confluent.Kafka.Producer;
using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.IntegrationTests;

/// <summary>
/// The consumer against a broker: a <c>Dispose</c> made inside the rebalance listener of an
/// <see cref="AsyncKafkaConsumer{TKey, TValue}"/> during a revocation, whose deferred teardown runs while the
/// <c>Poll</c> that ran the listener may still deserialize the records it returns.
/// </summary>
public sealed class KafkaConsumerBrokerTests : IClassFixture<KafkaBrokerFixture>
{
    private const int RecordCount = 2000;

    private const int MaxPollRecords = 5;

    private static readonly TimeSpan s_get = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_poll = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan s_assignmentWait = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan s_deserializeDelay = TimeSpan.FromMilliseconds(100);

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public KafkaConsumerBrokerTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    [SkippableFact]
    public async Task ADisposeInsideTheRebalanceListener_DoesNotDisposeTheDeserializersUnderItsPoll()
    {
        await _broker.EnsureStartedAsync(_output);
        string topic = "dotnet-consumer-" + Guid.NewGuid().ToString("N").Substring(0, 12);
        await _broker.CreateTopicAsync(topic, partitions: 2);
        using (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            new Dictionary<string, object?> { ["bootstrap.servers"] = _broker.ExternalBootstrap! },
            Serdes.ByteArray(),
            Serdes.ByteArray()))
        {
            for (int i = 0; i < RecordCount; i++)
            {
                _ = producer.Send(new ProducerRecord<byte[], byte[]>(topic, Encoding.UTF8.GetBytes("v" + i), partition: i % 2));
            }

            producer.Flush();
        }

        string group = "dotnet-dispose-in-listener-" + Guid.NewGuid().ToString("N").Substring(0, 12);
        SlowDeserializer value = new SlowDeserializer(s_deserializeDelay);
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            GroupConfigs(group), Serdes.ByteArray(), value);
        DisposingListener listener = new DisposingListener(consumer);
        using CancellationTokenSource stopOther = new CancellationTokenSource();
        Task? other = null;
        int returnedWhenDisposed = -1;
        Exception? failureWhenDisposed = null;
        try
        {
            await consumer.Subscribe(new[] { topic }, listener);
            Stopwatch elapsed = Stopwatch.StartNew();
            while (!listener.Disposed && elapsed.Elapsed < s_assignmentWait)
            {
                int returned = -1;
                Exception? failure = null;
                try
                {
                    returned = (await consumer.Poll(s_poll)).Count();
                }
                catch (Exception caught)
                {
                    failure = caught;
                }

                if (listener.Disposed)
                {
                    returnedWhenDisposed = returned;
                    failureWhenDisposed = failure;
                }
                else if (returned > 0 && other is null)
                {
                    // A second member takes a partition away, so the listener runs while records are buffered.
                    other = Task.Run(() => PollAsAnotherMember(topic, group, stopOther.Token));
                }
            }

            Assert.True(listener.Disposed, "the listener never ran a revocation");
            Assert.True(value.WaitDisposed(s_get), "the deferred teardown never disposed the deserializer");
        }
        finally
        {
            stopOther.Cancel();
            if (other is not null)
            {
                await other;
            }

            await consumer.DisposeAsync();
        }

        _output.WriteLine(
            $"the listener's Poll returned {returnedWhenDisposed} record(s)" +
            (failureWhenDisposed is null ? string.Empty : ", failed with " + failureWhenDisposed.GetType().Name) +
            $"; {value.Calls} deserialized in all; disposed while deserializing: {value.DisposedWhileDeserializing}");
        Assert.False(value.DisposedWhileDeserializing, "the deserializer was disposed while a Poll was still deserializing");
        Assert.Equal(1, value.Disposals);
    }

    private void PollAsAnotherMember(string topic, string group, CancellationToken stop)
    {
        using KafkaConsumer<byte[], byte[]> member =
            new KafkaConsumer<byte[], byte[]>(GroupConfigs(group), Serdes.ByteArray(), Serdes.ByteArray());
        member.Subscribe(new[] { topic });
        while (!stop.IsCancellationRequested)
        {
            _ = member.Poll(TimeSpan.FromMilliseconds(200));
        }
    }

    private Dictionary<string, object?> GroupConfigs(string group) =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = _broker.ExternalBootstrap!,
            ["group.protocol"] = "consumer",
            ["group.id"] = group,
            ["auto.offset.reset"] = "earliest",
            ["enable.auto.commit"] = "false",
            ["max.poll.records"] = MaxPollRecords,
        };

    /// <summary>A listener that disposes its consumer on the first revocation that takes a partition away.</summary>
    private sealed class DisposingListener : ConsumerRebalanceListenerBase
    {
        private readonly AsyncKafkaConsumer<byte[], byte[]> _consumer;

        internal DisposingListener(AsyncKafkaConsumer<byte[], byte[]> consumer) => _consumer = consumer;

        internal bool Disposed { get; private set; }

        public override void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
            if (partitions.Count > 0 && !Disposed)
            {
                Disposed = true;
                _consumer.Dispose();
            }
        }

        public override void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
        {
        }
    }

    /// <summary>
    /// A deserializer that takes a while for each record and records whether it was disposed while one was still
    /// being deserialized.
    /// </summary>
    private sealed class SlowDeserializer : IDeserializer<byte[]>, IDisposable
    {
        private readonly TimeSpan _delay;
        private readonly ManualResetEventSlim _disposed = new ManualResetEventSlim(false);
        private int _inside;
        private int _calls;
        private int _disposals;
        private int _disposedWhileDeserializing;

        internal SlowDeserializer(TimeSpan delay) => _delay = delay;

        internal int Calls => Volatile.Read(ref _calls);

        internal int Disposals => Volatile.Read(ref _disposals);

        internal bool DisposedWhileDeserializing => Volatile.Read(ref _disposedWhileDeserializing) != 0;

        internal bool WaitDisposed(TimeSpan timeout) => _disposed.Wait(timeout);

        public byte[] Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            byte[] copy = data.ToArray();
            _ = Interlocked.Increment(ref _inside);
            try
            {
                Thread.Sleep(_delay);
                if (_disposed.IsSet)
                {
                    Volatile.Write(ref _disposedWhileDeserializing, 1);
                }
            }
            finally
            {
                _ = Interlocked.Decrement(ref _inside);
                _ = Interlocked.Increment(ref _calls);
            }

            return copy;
        }

        public void Dispose()
        {
            if (Volatile.Read(ref _inside) > 0)
            {
                Volatile.Write(ref _disposedWhileDeserializing, 1);
            }

            _ = Interlocked.Increment(ref _disposals);
            _disposed.Set();
        }
    }
}
