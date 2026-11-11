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
using System.Globalization;
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// This binding's clients for the transactional engine: the blocking <see cref="KafkaProducer{TKey, TValue}"/> or, with
/// <c>ASYNC=True</c>, the <see cref="AsyncKafkaProducer{TKey, TValue}"/> whose transaction calls are awaited.
/// </summary>
internal sealed class V3TransactionalFactory : ITransactionalClientFactory
{
    private const string AllocationStrategyKey = "buffer.memory.allocation.strategy";

    private readonly TransactionalBenchmarkConfig _config;

    internal V3TransactionalFactory(TransactionalBenchmarkConfig config)
    {
        _config = config;
    }

    public string Client => _config.Producer.Async ? "dotnet-rust-async-txn" : "dotnet-rust-txn";

    public string Label => _config.Producer.Async ? "v3 (confluent-kafka-rust), async" : "v3 (confluent-kafka-rust), sync";

    public bool AwaitedControl => _config.Producer.Async;

    // Transactions force idempotence and acks=all. The allocation strategy is left to the core's default, full.
    public IReadOnlyDictionary<string, string> ProducerConfig(string transactionalId)
    {
        Dictionary<string, string> conf = V3Config.BuildProducerConfig();
        conf.Remove(AllocationStrategyKey);
        conf["client.id"] = $"dotnet-rust-txn-{transactionalId}";
        conf["transactional.id"] = transactionalId;
        conf["enable.idempotence"] = "true";
        conf["acks"] = "all";
        return conf;
    }

    public ITransactionalProducer CreateProducer(string transactionalId)
    {
        IReadOnlyDictionary<string, string> conf = ProducerConfig(transactionalId);
        return _config.Producer.Async ? new V3AsyncTransactionalProducer(conf) : new V3SyncTransactionalProducer(conf);
    }

    public ITransactionalConsumer CreateConsumer()
    {
        var conf = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = _config.BootstrapServers,
            ["group.id"] = _config.GroupId,
            ["group.protocol"] = "consumer",
            ["enable.auto.commit"] = "false",
            ["auto.offset.reset"] = "earliest",
            ["isolation.level"] = "read_committed",
            ["client.id"] = $"dotnet-rust-txn-consumer-{_config.GroupId}",
            ["max.poll.records"] = _config.RecordsPerTransaction.ToString(CultureInfo.InvariantCulture),
        };
        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Java))
        {
            conf[kv.Key] = kv.Value;
        }

        return new V3TransactionalConsumer(conf, _config.SourceTopic!, _config.Producer.Async);
    }

    public void CreateTopic() =>
        TopicProvisioning.RecreateTopic(_config.BootstrapServers, _config.Producer.TopicName, _config.Producer.Partitions);

    // Java's fatal transaction errors: the producer must be closed. Retriable ones may still complete; anything else
    // is answered with an abort. Of the authorization errors only these two are fatal: a topic or group one aborts.
    internal static TxnErrorKind Classify(Exception error) => error switch
    {
        ProducerFencedException or InvalidPidMappingException or UnsupportedVersionException
            or TransactionalIdAuthorizationException or ClusterAuthorizationException => TxnErrorKind.Fatal,
        RetriableException => TxnErrorKind.Retriable,
        _ => TxnErrorKind.Abortable,
    };

    // Bounds the producer close, so a broker that stops answering cannot hang the run.
    internal static readonly TimeSpan CloseTimeout = TimeSpan.FromSeconds(30);

    internal static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(IReadOnlyDictionary<(string Topic, int Partition), long> offsets)
    {
        var map = new Dictionary<TopicPartition, OffsetAndMetadata>(offsets.Count);
        foreach (KeyValuePair<(string Topic, int Partition), long> kv in offsets)
        {
            map[new TopicPartition(kv.Key.Topic, kv.Key.Partition)] = new OffsetAndMetadata(kv.Value);
        }

        return map;
    }

    internal static PerfRecordMetadata Metadata(RecordMetadata meta) =>
        new(meta.Topic(), meta.Partition(), meta.Offset(), meta.Timestamp());
}

/// <summary>The blocking transactional producer: every call completes before it returns.</summary>
internal sealed class V3SyncTransactionalProducer : ITransactionalProducer
{
    private readonly KafkaProducer<byte[], byte[]> _producer;

    internal V3SyncTransactionalProducer(IReadOnlyDictionary<string, string> config)
    {
        _producer = new KafkaProducer<byte[], byte[]>(V3Config.ClientConfigs(config), Serdes.ByteArray(), Serdes.ByteArray());
    }

    public ValueTask InitTransactions()
    {
        _producer.InitTransactions();
        return default;
    }

    public ValueTask BeginTransaction()
    {
        _producer.BeginTransaction();
        return default;
    }

    public ValueTask<object?> Send(string topic, byte[]? key, byte[]? value) =>
        new(_producer.Send(new ProducerRecord<byte[], byte[]>(topic, value!, key!)));

    public ValueTask<PerfRecordMetadata> Delivered(object pending) =>
        new(V3TransactionalFactory.Metadata(((KafkaFuture<RecordMetadata>)pending).Get()));

    public ValueTask SendOffsetsToTransaction(IReadOnlyDictionary<(string Topic, int Partition), long> offsets, ITransactionalConsumer consumer)
    {
        _producer.SendOffsetsToTransaction(V3TransactionalFactory.Offsets(offsets), ((V3TransactionalConsumer)consumer).GroupMetadata());
        return default;
    }

    public ValueTask CommitTransaction()
    {
        _producer.CommitTransaction();
        return default;
    }

    public ValueTask AbortTransaction()
    {
        _producer.AbortTransaction();
        return default;
    }

    public TxnErrorKind Classify(Exception error) => V3TransactionalFactory.Classify(error);

    public void PrintCompressionRateAvg() => V3ProducerMetrics.PrintCompressionRateAvg(_producer.Metrics);

    public ValueTask Close()
    {
        _producer.Close(V3TransactionalFactory.CloseTimeout);
        return default;
    }

    public void Dispose() => _producer.Dispose();
}

/// <summary>The awaited transactional producer: the sends and the transaction calls are the client's own tasks.</summary>
internal sealed class V3AsyncTransactionalProducer : ITransactionalProducer
{
    private readonly AsyncKafkaProducer<byte[], byte[]> _producer;

    internal V3AsyncTransactionalProducer(IReadOnlyDictionary<string, string> config)
    {
        _producer = new AsyncKafkaProducer<byte[], byte[]>(V3Config.ClientConfigs(config), Serdes.ByteArray(), Serdes.ByteArray());
    }

    public ValueTask InitTransactions() => new(_producer.InitTransactions());

    public ValueTask BeginTransaction() => new(_producer.BeginTransaction());

    public async ValueTask<object?> Send(string topic, byte[]? key, byte[]? value) =>
        await _producer.Send(new ProducerRecord<byte[], byte[]>(topic, value!, key!)).ConfigureAwait(false);

    public async ValueTask<PerfRecordMetadata> Delivered(object pending) =>
        V3TransactionalFactory.Metadata(await ((AsyncKafkaFuture<RecordMetadata>)pending).Get().ConfigureAwait(false));

    public ValueTask SendOffsetsToTransaction(IReadOnlyDictionary<(string Topic, int Partition), long> offsets, ITransactionalConsumer consumer) =>
        new(_producer.SendOffsetsToTransaction(V3TransactionalFactory.Offsets(offsets), ((V3TransactionalConsumer)consumer).GroupMetadata()));

    public ValueTask CommitTransaction() => new(_producer.CommitTransaction());

    public ValueTask AbortTransaction() => new(_producer.AbortTransaction());

    public TxnErrorKind Classify(Exception error) => V3TransactionalFactory.Classify(error);

    public void PrintCompressionRateAvg() => V3ProducerMetrics.PrintCompressionRateAvg(_producer.Metrics);

    public ValueTask Close() => new(_producer.Close(V3TransactionalFactory.CloseTimeout));

    public void Dispose() => _producer.Dispose();
}

/// <summary>
/// The EOS source consumer: a blocking <see cref="KafkaConsumer{TKey, TValue}"/>, polled off the caller's thread for
/// an awaited producer. Records past <c>maxRecords</c> wait for the next poll.
/// </summary>
internal sealed class V3TransactionalConsumer : ITransactionalConsumer
{
    private readonly KafkaConsumer<byte[], byte[]> _consumer;
    private readonly bool _offThread;
    private readonly Queue<SourceRecord> _leftover = new();

    internal V3TransactionalConsumer(IReadOnlyDictionary<string, string> config, string sourceTopic, bool offThread)
    {
        _consumer = new KafkaConsumer<byte[], byte[]>(V3Config.ClientConfigs(config), Serdes.ByteArray(), Serdes.ByteArray());
        _consumer.Subscribe(new[] { sourceTopic });
        _offThread = offThread;
    }

    internal ConsumerGroupMetadata GroupMetadata() => _consumer.GroupMetadata();

    public ValueTask<IReadOnlyList<SourceRecord>> Poll(int maxRecords, TimeSpan timeout) =>
        _offThread
            ? new ValueTask<IReadOnlyList<SourceRecord>>(Task.Run(() => PollBlocking(maxRecords, timeout)))
            : new ValueTask<IReadOnlyList<SourceRecord>>(PollBlocking(maxRecords, timeout));

    public void Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();

    private IReadOnlyList<SourceRecord> PollBlocking(int maxRecords, TimeSpan timeout)
    {
        if (_leftover.Count == 0)
        {
            foreach (ConsumerRecord<byte[], byte[]> record in _consumer.Poll(timeout))
            {
                _leftover.Enqueue(new SourceRecord(record.Key(), record.Value(), record.Topic(), record.Partition(), record.Offset()));
            }
        }

        var batch = new List<SourceRecord>(Math.Min(maxRecords, _leftover.Count));
        while (batch.Count < maxRecords && _leftover.TryDequeue(out SourceRecord record))
        {
            batch.Add(record);
        }

        return batch;
    }
}
