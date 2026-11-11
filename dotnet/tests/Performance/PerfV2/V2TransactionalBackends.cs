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
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance.V2;

/// <summary>
/// ckd's clients for the transactional engine. ckd's transaction calls only block, so both arms run them on the
/// worker's thread; with <c>ASYNC=True</c> the records go through <c>ProduceAsync</c> instead of <c>Produce</c>.
/// </summary>
internal sealed class V2TransactionalFactory : ITransactionalClientFactory
{
    private readonly TransactionalBenchmarkConfig _config;

    internal V2TransactionalFactory(TransactionalBenchmarkConfig config)
    {
        _config = config;
    }

    public string Client => _config.Producer.Async ? "dotnet-librdkafka-async-txn" : "dotnet-librdkafka-txn";

    public string Label => _config.Producer.Async
        ? "v2 (confluent-kafka-dotnet), async produce, blocking transaction control"
        : "v2 (confluent-kafka-dotnet), sync";

    public bool AwaitedControl => false;

    // Transactions force idempotence and acks=all.
    public IReadOnlyDictionary<string, string> ProducerConfig(string transactionalId)
    {
        Dictionary<string, string> conf = V2Config.BuildProducerConfig();
        conf["client.id"] = $"dotnet-librdkafka-txn-{transactionalId}";
        conf["transactional.id"] = transactionalId;
        conf["enable.idempotence"] = "true";
        conf["acks"] = "all";
        return conf;
    }

    public ITransactionalProducer CreateProducer(string transactionalId) =>
        new V2TransactionalProducer(V2ProducerFactory.Build(ProducerConfig(transactionalId), "txn"), _config.Producer.Async);

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
            ["client.id"] = $"dotnet-librdkafka-txn-consumer-{_config.GroupId}",
        };
        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Librdkafka))
        {
            conf[kv.Key] = kv.Value;
        }

        return new V2TransactionalConsumer(conf, _config.SourceTopic!);
    }

    public void CreateTopic() =>
        TopicProvisioning.RecreateTopic(_config.BootstrapServers, _config.Producer.TopicName, _config.Producer.Partitions);
}

/// <summary>
/// One ckd transactional producer. Every call completes before it returns: a send waits only for the record to be
/// queued (retrying a full queue), and a delivery is read after the commit has flushed it.
/// </summary>
internal sealed class V2TransactionalProducer : ITransactionalProducer
{
    private static readonly TimeSpan s_controlTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan s_deliveryTimeout = TimeSpan.FromSeconds(30);

    private readonly IProducer<byte[], byte[]> _producer;
    private readonly bool _produceAsync;

    internal V2TransactionalProducer(IProducer<byte[], byte[]> producer, bool produceAsync)
    {
        _producer = producer;
        _produceAsync = produceAsync;
    }

    public ValueTask InitTransactions()
    {
        _producer.InitTransactions(s_controlTimeout);
        return default;
    }

    public ValueTask BeginTransaction()
    {
        _producer.BeginTransaction();
        return default;
    }

    // Null once terminating with the local queue still full, as Python's _v2_produce.
    public ValueTask<object?> Send(string topic, byte[]? key, byte[]? value)
    {
        var message = new Message<byte[], byte[]> { Key = key!, Value = value! };
        while (!PerfSignals.Terminating)
        {
            if (_produceAsync)
            {
                Task<DeliveryResult<byte[], byte[]>> delivery = _producer.ProduceAsync(topic, message);
                if (!IsQueueFull(delivery))
                {
                    if (delivery.IsFaulted)
                    {
                        delivery.GetAwaiter().GetResult();
                    }

                    return new ValueTask<object?>(delivery);
                }
            }
            else
            {
                // The report fires on ckd's poll thread, which must not run anything waiting on it.
                var report = new TaskCompletionSource<PerfRecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);
                try
                {
                    _producer.Produce(topic, message, r => Report(report, r));
                    return new ValueTask<object?>(report);
                }
                catch (ProduceException<byte[], byte[]> e) when (e.Error.Code == ErrorCode.Local_QueueFull)
                {
                    // Retried below.
                }
            }

            Thread.Sleep(1);
        }

        return new ValueTask<object?>((object?)null);
    }

    public ValueTask<PerfRecordMetadata> Delivered(object pending)
    {
        if (pending is Task<DeliveryResult<byte[], byte[]>> delivery)
        {
            DeliveryResult<byte[], byte[]> result = Wait(delivery);
            return new ValueTask<PerfRecordMetadata>(new PerfRecordMetadata(
                result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Timestamp.UnixTimestampMs));
        }

        return new ValueTask<PerfRecordMetadata>(Wait(((TaskCompletionSource<PerfRecordMetadata>)pending).Task));
    }

    public ValueTask SendOffsetsToTransaction(IReadOnlyDictionary<(string Topic, int Partition), long> offsets, ITransactionalConsumer consumer)
    {
        var positions = new List<TopicPartitionOffset>(offsets.Count);
        foreach (KeyValuePair<(string Topic, int Partition), long> kv in offsets)
        {
            positions.Add(new TopicPartitionOffset(kv.Key.Topic, kv.Key.Partition, kv.Value));
        }

        // As Python: the batch is flushed before the control calls, so an abort has nothing to purge and the
        // idempotent sequence stays in step with the broker.
        _producer.Flush(s_controlTimeout);
        _producer.SendOffsetsToTransaction(positions, ((V2TransactionalConsumer)consumer).GroupMetadata, s_controlTimeout);
        return default;
    }

    public ValueTask CommitTransaction()
    {
        _producer.CommitTransaction(s_controlTimeout);
        return default;
    }

    public ValueTask AbortTransaction()
    {
        // Flushed first as before the commit, so an abort after an error also has nothing in flight. A flush that
        // fails here is left to the abort to report.
        try
        {
            _producer.Flush(s_controlTimeout);
        }
        catch (KafkaException)
        {
        }

        _producer.AbortTransaction(s_controlTimeout);
        return default;
    }

    public TxnErrorKind Classify(Exception error) => error switch
    {
        KafkaException e when e.Error.IsFatal => TxnErrorKind.Fatal,
        KafkaRetriableException => TxnErrorKind.Retriable,
        _ => TxnErrorKind.Abortable,
    };

    // librdkafka has no compression-rate-avg metric to print.
    public void PrintCompressionRateAvg()
    {
    }

    public ValueTask Close()
    {
        _producer.Flush(s_deliveryTimeout);
        return default;
    }

    public void Dispose() => _producer.Dispose();

    private static bool IsQueueFull(Task<DeliveryResult<byte[], byte[]>> delivery) =>
        delivery.IsFaulted
        && delivery.Exception!.InnerException is ProduceException<byte[], byte[]> e
        && e.Error.Code == ErrorCode.Local_QueueFull;

    private static void Report(TaskCompletionSource<PerfRecordMetadata> report, DeliveryReport<byte[], byte[]> r)
    {
        if (r.Error.IsError)
        {
            report.TrySetException(new KafkaException(r.Error));
        }
        else
        {
            report.TrySetResult(new PerfRecordMetadata(r.Topic, r.Partition.Value, r.Offset.Value, r.Message.Timestamp.UnixTimestampMs));
        }
    }

    private static T Wait<T>(Task<T> delivery)
    {
        if (!delivery.Wait(s_deliveryTimeout))
        {
            throw new TimeoutException($"No delivery report within {s_deliveryTimeout.TotalSeconds} s");
        }

        return delivery.GetAwaiter().GetResult();
    }
}

/// <summary>The EOS source consumer: up to <c>maxRecords</c> records within the timeout, as librdkafka's batch consume.</summary>
internal sealed class V2TransactionalConsumer : ITransactionalConsumer
{
    private readonly IConsumer<byte[], byte[]> _consumer;

    internal V2TransactionalConsumer(IReadOnlyDictionary<string, string> config, string sourceTopic)
    {
        _consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
        _consumer.Subscribe(sourceTopic);
    }

    internal IConsumerGroupMetadata GroupMetadata => _consumer.ConsumerGroupMetadata;

    public ValueTask<IReadOnlyList<SourceRecord>> Poll(int maxRecords, TimeSpan timeout)
    {
        var batch = new List<SourceRecord>();
        long deadline = Stopwatch.GetTimestamp() + (long)(timeout.TotalSeconds * Stopwatch.Frequency);
        while (batch.Count < maxRecords)
        {
            long remaining = deadline - Stopwatch.GetTimestamp();
            if (remaining <= 0)
            {
                break;
            }

            ConsumeResult<byte[], byte[]>? result;
            try
            {
                result = _consumer.Consume(TimeSpan.FromSeconds(remaining / (double)Stopwatch.Frequency));
            }
            catch (ConsumeException)
            {
                // As Python's batch consume: a message carrying an error is skipped.
                continue;
            }

            if (result is null || result.IsPartitionEOF)
            {
                continue;
            }

            batch.Add(new SourceRecord(result.Message.Key, result.Message.Value, result.Topic, result.Partition.Value, result.Offset.Value));
        }

        return new ValueTask<IReadOnlyList<SourceRecord>>(batch);
    }

    public void Close() => _consumer.Close();

    public void Dispose() => _consumer.Dispose();
}
