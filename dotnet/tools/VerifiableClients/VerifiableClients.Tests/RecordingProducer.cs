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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Tools.Tests;

/// <summary>
/// A <see cref="MockProducer{TKey, TValue}"/> that also keeps each record it takes, in order: Java's and Python's
/// <c>MockProducer.history()</c>, which the .NET mock, holding the records serialized, does not give back.
/// </summary>
internal sealed class RecordingProducer : IProducer<string, string>
{
    private readonly List<ProducerRecord<string, string>> _history = new List<ProducerRecord<string, string>>();

    internal RecordingProducer(bool autoComplete = true)
    {
        Mock = new MockProducer<string, string>(new StringSerializer(), new StringSerializer(), autoComplete);
    }

    internal MockProducer<string, string> Mock { get; }

    internal List<ProducerRecord<string, string>> History()
    {
        lock (_history)
        {
            return new List<ProducerRecord<string, string>>(_history);
        }
    }

    public KafkaFuture<RecordMetadata> Send(ProducerRecord<string, string> record) => Recorded(record, Mock.Send(record));

    public KafkaFuture<RecordMetadata> Send(ProducerRecord<string, string> record, ICallback? callback) =>
        Recorded(record, Mock.Send(record, callback));

    public void InitTransactions() => Mock.InitTransactions();

    public void BeginTransaction() => Mock.BeginTransaction();

    public void SendOffsetsToTransaction(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) =>
        Mock.SendOffsetsToTransaction(offsets, groupMetadata);

    public void CommitTransaction() => Mock.CommitTransaction();

    public void AbortTransaction() => Mock.AbortTransaction();

    public void Flush() => Mock.Flush();

    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => Mock.PartitionsFor(topic);

    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => Mock.Metrics();

    public void Close() => Mock.Close();

    public void Close(TimeSpan timeout) => Mock.Close(timeout);

    public void Dispose() => Mock.Dispose();

    private KafkaFuture<RecordMetadata> Recorded(ProducerRecord<string, string> record, KafkaFuture<RecordMetadata> future)
    {
        lock (_history)
        {
            _history.Add(record);
        }

        return future;
    }
}
