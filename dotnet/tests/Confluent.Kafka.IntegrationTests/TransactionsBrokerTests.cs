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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Performance.Tests;
using Confluent.Kafka.Producer;

using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.IntegrationTests;

/// <summary>
/// Transactions and idempotence against a broker, sync and async, as Python's
/// <c>test_transactions_broker.py</c>: the consume-transform-produce round trip with
/// <c>SendOffsetsToTransaction</c>, an aborted transaction a <c>read_committed</c> consumer never returns, a
/// producer fenced by a newer one with the same <c>transactional.id</c>, and an idempotent producer's records
/// written once each.
/// </summary>
[Collection(BrokerCollection.Name)]
public sealed class TransactionsBrokerTests
{
    private const string FencedMessage =
        "There is a newer producer with the same transactionalId which fences the current one.";

    private static readonly TimeSpan s_get = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan s_poll = TimeSpan.FromSeconds(0.5);
    private static readonly TimeSpan s_quietWindow = TimeSpan.FromSeconds(1);

    private readonly KafkaBrokerFixture _broker;
    private readonly ITestOutputHelper _output;

    public TransactionsBrokerTests(KafkaBrokerFixture broker, ITestOutputHelper output)
    {
        _broker = broker;
        _output = output;
    }

    [SkippableFact]
    public async Task ExactlyOnceRoundTrip_CommitsTheOutputAndTheOffsets()
    {
        (string source, string sink) = await SeededRoundTripTopicsAsync();
        TopicPartition tp = new TopicPartition(source, 0);
        using KafkaConsumer<string, string> consumer = new KafkaConsumer<string, string>(
            GroupConfigs("read_committed"), Serdes.String, Serdes.String);
        using KafkaProducer<string, string> producer = new KafkaProducer<string, string>(
            TransactionalConfigs("dotnet-eos-txn"), Serdes.String, Serdes.String);

        consumer.Subscribe(new[] { source });
        List<ConsumerRecord<string, string>> records = new List<ConsumerRecord<string, string>>();
        Stopwatch elapsed = Stopwatch.StartNew();
        while (records.Count < 3)
        {
            Assert.True(elapsed.Elapsed < s_deadline, $"polled {records.Count} of 3 records");
            records.AddRange(consumer.Poll(s_poll));
        }

        Assert.Equal(new[] { "a", "b", "c" }, records.Select(record => record.Value));
        producer.InitTransactions();
        producer.BeginTransaction();
        foreach (ConsumerRecord<string, string> record in records)
        {
            producer.Send(new ProducerRecord<string, string>(sink, record.Value.ToUpperInvariant(), record.Value, partition: 0));
        }

        producer.SendOffsetsToTransaction(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(records[^1].Offset + 1) },
            consumer.GroupMetadata());
        producer.CommitTransaction();

        // The offsets are the group's once the transaction committed.
        elapsed.Restart();
        while (consumer.Committed(new[] { tp }, s_get)[tp]?.Offset != 3)
        {
            Assert.True(elapsed.Elapsed < s_deadline, "the committed offset never reached 3");
            await Task.Delay(200);
        }

        Assert.Equal(new[] { "A", "B", "C" }, ReadAll(sink, 3, "read_committed").Select(record => record.Value));
    }

    [SkippableFact]
    public async Task AsyncExactlyOnceRoundTrip_CommitsTheOutputAndTheOffsets()
    {
        (string source, string sink) = await SeededRoundTripTopicsAsync();
        TopicPartition tp = new TopicPartition(source, 0);
        await using AsyncKafkaConsumer<string, string> consumer = new AsyncKafkaConsumer<string, string>(
            GroupConfigs("read_committed"), Serdes.String, Serdes.String);
        await using AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(
            TransactionalConfigs("dotnet-eos-txn"), Serdes.String, Serdes.String);

        await consumer.Subscribe(new[] { source });
        List<ConsumerRecord<string, string>> records = new List<ConsumerRecord<string, string>>();
        Stopwatch elapsed = Stopwatch.StartNew();
        while (records.Count < 3)
        {
            Assert.True(elapsed.Elapsed < s_deadline, $"polled {records.Count} of 3 records");
            records.AddRange(await consumer.Poll(s_poll));
        }

        Assert.Equal(new[] { "a", "b", "c" }, records.Select(record => record.Value));
        await producer.InitTransactions();
        producer.BeginTransaction();
        foreach (ConsumerRecord<string, string> record in records)
        {
            await producer.Send(new ProducerRecord<string, string>(sink, record.Value.ToUpperInvariant(), record.Value, partition: 0));
        }

        await producer.SendOffsetsToTransaction(
            new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(records[^1].Offset + 1) },
            consumer.GroupMetadata());
        await producer.CommitTransaction();

        // The offsets are the group's once the transaction committed.
        elapsed.Restart();
        while ((await consumer.Committed(new[] { tp }, s_get))[tp]?.Offset != 3)
        {
            Assert.True(elapsed.Elapsed < s_deadline, "the committed offset never reached 3");
            await Task.Delay(200);
        }

        Assert.Equal(new[] { "A", "B", "C" }, ReadAll(sink, 3, "read_committed").Select(record => record.Value));
    }

    [SkippableFact]
    public async Task AbortedTransaction_IsHiddenFromReadCommitted()
    {
        string topic = await TopicAsync("dotnet-abort");
        using (KafkaProducer<string, string> producer = new KafkaProducer<string, string>(
            TransactionalConfigs("dotnet-abort-txn"), Serdes.String, Serdes.String))
        {
            producer.InitTransactions();
            producer.BeginTransaction();
            KafkaFuture<RecordMetadata>[] aborted =
            {
                producer.Send(new ProducerRecord<string, string>(topic, "x1", partition: 0)),
                producer.Send(new ProducerRecord<string, string>(topic, "x2", partition: 0)),
            };

            // The records reach the log before the abort marks them aborted.
            producer.Flush();
            Assert.Equal(new[] { 0L, 1L }, aborted.Select(future => future.Get(s_get).Offset));
            producer.AbortTransaction();
            producer.BeginTransaction();
            producer.Send(new ProducerRecord<string, string>(topic, "kept", partition: 0));
            producer.CommitTransaction();
        }

        AssertAbortedHidden(topic);
    }

    [SkippableFact]
    public async Task AsyncAbortedTransaction_IsHiddenFromReadCommitted()
    {
        string topic = await TopicAsync("dotnet-abort");
        await using (AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(
            TransactionalConfigs("dotnet-abort-txn"), Serdes.String, Serdes.String))
        {
            await producer.InitTransactions();
            producer.BeginTransaction();
            AsyncKafkaFuture<RecordMetadata>[] aborted =
            {
                await producer.Send(new ProducerRecord<string, string>(topic, "x1", partition: 0)),
                await producer.Send(new ProducerRecord<string, string>(topic, "x2", partition: 0)),
            };

            // The records reach the log before the abort marks them aborted.
            await producer.Flush();
            Assert.Equal(0L, (await aborted[0].Get(s_get)).Offset);
            Assert.Equal(1L, (await aborted[1].Get(s_get)).Offset);
            await producer.AbortTransaction();
            producer.BeginTransaction();
            await producer.Send(new ProducerRecord<string, string>(topic, "kept", partition: 0));
            await producer.CommitTransaction();
        }

        AssertAbortedHidden(topic);
    }

    [SkippableFact]
    public async Task ANewerProducerWithTheSameTransactionalId_FencesTheOlderOnCommit()
    {
        string topic = await TopicAsync("dotnet-fence");
        Dictionary<string, object?> configs = TransactionalConfigs("dotnet-fence-txn");
        using KafkaProducer<string, string> fenced = new KafkaProducer<string, string>(configs, Serdes.String, Serdes.String);
        using KafkaProducer<string, string> fencing = new KafkaProducer<string, string>(configs, Serdes.String, Serdes.String);

        fenced.InitTransactions();
        fenced.BeginTransaction();
        fenced.Send(new ProducerRecord<string, string>(topic, "fenced", partition: 0));
        fenced.Flush();

        // Initializing the newer producer aborts the older one's open transaction and fences it.
        fencing.InitTransactions();
        fencing.BeginTransaction();
        fencing.Send(new ProducerRecord<string, string>(topic, "kept", partition: 0));

        ProducerFencedException error = Assert.Throws<ProducerFencedException>(() => fenced.CommitTransaction());
        Assert.Equal(FencedMessage, error.Message);
        fencing.CommitTransaction();

        Assert.Equal(new[] { "kept" }, ReadAll(topic, 1, "read_committed").Select(record => record.Value));
    }

    [SkippableFact]
    public async Task AsyncANewerProducerWithTheSameTransactionalId_FencesTheOlderOnCommit()
    {
        string topic = await TopicAsync("dotnet-fence");
        Dictionary<string, object?> configs = TransactionalConfigs("dotnet-fence-txn");
        await using AsyncKafkaProducer<string, string> fenced =
            new AsyncKafkaProducer<string, string>(configs, Serdes.String, Serdes.String);
        await using AsyncKafkaProducer<string, string> fencing =
            new AsyncKafkaProducer<string, string>(configs, Serdes.String, Serdes.String);

        await fenced.InitTransactions();
        fenced.BeginTransaction();
        await fenced.Send(new ProducerRecord<string, string>(topic, "fenced", partition: 0));
        await fenced.Flush();

        // Initializing the newer producer aborts the older one's open transaction and fences it.
        await fencing.InitTransactions();
        fencing.BeginTransaction();
        await fencing.Send(new ProducerRecord<string, string>(topic, "kept", partition: 0));

        ProducerFencedException error = await Assert.ThrowsAsync<ProducerFencedException>(() => fenced.CommitTransaction());
        Assert.Equal(FencedMessage, error.Message);
        await fencing.CommitTransaction();

        Assert.Equal(new[] { "kept" }, ReadAll(topic, 1, "read_committed").Select(record => record.Value));
    }

    [SkippableFact]
    public async Task IdempotentProducer_WritesEachRecordOnce()
    {
        string topic = await TopicAsync("dotnet-idempotent");
        string[] values = Enumerable.Range(0, 100).Select(i => "v" + i).ToArray();
        using (KafkaProducer<string, string> producer = new KafkaProducer<string, string>(
            IdempotentConfigs(), Serdes.String, Serdes.String))
        {
            KafkaFuture<RecordMetadata>[] futures = values
                .Select(value => producer.Send(new ProducerRecord<string, string>(topic, value, partition: 0)))
                .ToArray();
            producer.Flush();
            Assert.Equal(Enumerable.Range(0, 100).Select(i => (long)i), futures.Select(future => future.Get(s_get).Offset));
        }

        AssertWrittenOnce(topic, values);
    }

    [SkippableFact]
    public async Task AsyncIdempotentProducer_WritesEachRecordOnce()
    {
        string topic = await TopicAsync("dotnet-idempotent");
        string[] values = Enumerable.Range(0, 100).Select(i => "v" + i).ToArray();
        await using (AsyncKafkaProducer<string, string> producer = new AsyncKafkaProducer<string, string>(
            IdempotentConfigs(), Serdes.String, Serdes.String))
        {
            List<AsyncKafkaFuture<RecordMetadata>> futures = new List<AsyncKafkaFuture<RecordMetadata>>();
            foreach (string value in values)
            {
                futures.Add(await producer.Send(new ProducerRecord<string, string>(topic, value, partition: 0)));
            }

            await producer.Flush();
            for (int i = 0; i < futures.Count; i++)
            {
                Assert.Equal(i, (await futures[i].Get(s_get)).Offset);
            }
        }

        AssertWrittenOnce(topic, values);
    }

    private void AssertAbortedHidden(string topic)
    {
        // Offset 2 is the abort marker, 3 the committed record, 4 its marker.
        Assert.Equal(
            new (long, string)[] { (0, "x1"), (1, "x2"), (3, "kept") },
            ReadAll(topic, 3, "read_uncommitted").Select(record => (record.Offset, record.Value)));
        Assert.Equal(
            new (long, string)[] { (3, "kept") },
            ReadAll(topic, 1, "read_committed").Select(record => (record.Offset, record.Value)));
    }

    private void AssertWrittenOnce(string topic, string[] values)
    {
        List<ConsumerRecord<string, string>> records = ReadAll(topic, values.Length, "read_uncommitted");
        Assert.Equal(values, records.Select(record => record.Value));
        Assert.Equal(Enumerable.Range(0, values.Length).Select(i => (long)i), records.Select(record => record.Offset));
    }

    private async Task<(string Source, string Sink)> SeededRoundTripTopicsAsync()
    {
        string source = await TopicAsync("dotnet-eos-in");
        string sink = await TopicAsync("dotnet-eos-out");
        using KafkaProducer<string, string> seed = new KafkaProducer<string, string>(Configs(), Serdes.String, Serdes.String);
        foreach (string value in new[] { "a", "b", "c" })
        {
            seed.Send(new ProducerRecord<string, string>(source, value, partition: 0)).Get(s_get);
        }

        return (source, sink);
    }

    private async Task<string> TopicAsync(string prefix)
    {
        await _broker.EnsureStartedAsync(_output);
        string topic = prefix + "-" + Unique();
        await _broker.CreateTopicAsync(topic, partitions: 1);
        return topic;
    }

    private static string Unique() => Guid.NewGuid().ToString("N").Substring(0, 12);

    private Dictionary<string, object?> Configs() => new Dictionary<string, object?>
    {
        ["bootstrap.servers"] = _broker.ExternalBootstrap!,
    };

    private Dictionary<string, object?> TransactionalConfigs(string prefix)
    {
        Dictionary<string, object?> configs = Configs();
        configs["transactional.id"] = prefix + "-" + Unique();
        return configs;
    }

    private Dictionary<string, object?> IdempotentConfigs()
    {
        Dictionary<string, object?> configs = Configs();
        configs["enable.idempotence"] = "true";
        configs["acks"] = "all";
        return configs;
    }

    private Dictionary<string, object?> GroupConfigs(string isolationLevel)
    {
        Dictionary<string, object?> configs = Configs();
        configs["group.protocol"] = "consumer";
        configs["group.id"] = "dotnet-txn-" + Unique();
        configs["enable.auto.commit"] = "false";
        configs["auto.offset.reset"] = "earliest";
        configs["isolation.level"] = isolationLevel;
        return configs;
    }

    /// <summary>
    /// Reads partition 0 from the start until <paramref name="count"/> records arrived, then for a quiet window, so
    /// a record past the expected ones is caught.
    /// </summary>
    private List<ConsumerRecord<string, string>> ReadAll(string topic, int count, string isolationLevel)
    {
        using KafkaConsumer<string, string> consumer =
            new KafkaConsumer<string, string>(GroupConfigs(isolationLevel), Serdes.String, Serdes.String);
        TopicPartition tp = new TopicPartition(topic, 0);
        consumer.Assign(new[] { tp });
        consumer.Seek(tp, 0);
        List<ConsumerRecord<string, string>> records = new List<ConsumerRecord<string, string>>();
        Stopwatch elapsed = Stopwatch.StartNew();
        while (records.Count < count && elapsed.Elapsed < s_deadline)
        {
            records.AddRange(consumer.Poll(s_poll));
        }

        elapsed.Restart();
        while (elapsed.Elapsed < s_quietWindow)
        {
            records.AddRange(consumer.Poll(s_poll));
        }

        return records;
    }
}
