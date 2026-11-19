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
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance;

/// <summary>How the transactional engine answers a failed transaction call.</summary>
public enum TxnErrorKind
{
    /// <summary>Retried, up to the engine's attempt limit: the call may still be completing.</summary>
    Retriable,

    /// <summary>Answered by aborting the open transaction.</summary>
    Abortable,

    /// <summary>The producer cannot go on: the worker stops and the run fails.</summary>
    Fatal,
}

/// <summary>One record polled from the EOS source topic, with the position it was read from.</summary>
public readonly struct SourceRecord
{
    public SourceRecord(byte[]? key, byte[]? value, string topic, int partition, long offset)
    {
        Key = key;
        Value = value;
        Topic = topic;
        Partition = partition;
        Offset = offset;
    }

    public byte[]? Key { get; }

    public byte[]? Value { get; }

    public string Topic { get; }

    public int Partition { get; }

    public long Offset { get; }
}

/// <summary>
/// One transactional producer of the engine. A blocking backend returns completed tasks from every call, so a worker
/// running it never leaves its thread; an awaited backend returns the client's own tasks.
/// </summary>
public interface ITransactionalProducer : IDisposable
{
    ValueTask InitTransactions();

    ValueTask BeginTransaction();

    /// <summary>
    /// Hands one record over and returns its pending delivery, which <see cref="Delivered"/> resolves; null when the
    /// run is terminating before the client accepted it. Throws when the client rejects the record.
    /// </summary>
    ValueTask<object?> Send(string topic, byte[]? key, byte[]? value);

    /// <summary>The acknowledged metadata of a record <see cref="Send"/> returned, or its delivery failure.</summary>
    ValueTask<PerfRecordMetadata> Delivered(object pending);

    /// <summary>Sends the next offsets to consume, per source partition, with the consumer's group metadata.</summary>
    ValueTask SendOffsetsToTransaction(IReadOnlyDictionary<(string Topic, int Partition), long> offsets, ITransactionalConsumer consumer);

    ValueTask CommitTransaction();

    ValueTask AbortTransaction();

    TxnErrorKind Classify(Exception error);

    void PrintCompressionRateAvg();

    ValueTask Close();
}

/// <summary>The EOS source consumer of one transactional producer.</summary>
public interface ITransactionalConsumer : IDisposable
{
    /// <summary>Up to <paramref name="maxRecords"/> source records, bounded by <paramref name="timeout"/>.</summary>
    ValueTask<IReadOnlyList<SourceRecord>> Poll(int maxRecords, TimeSpan timeout);

    void Close();
}

/// <summary>The client one perf exe runs the transactional engine over.</summary>
public interface ITransactionalClientFactory
{
    /// <summary>The <c>client</c> value of <c>results.json</c>.</summary>
    string Client { get; }

    /// <summary>The client and arm, as the harness output names them.</summary>
    string Label { get; }

    /// <summary>True when the transaction calls are awaited tasks; false when they block the worker's thread.</summary>
    bool AwaitedControl { get; }

    IReadOnlyDictionary<string, string> ProducerConfig(string transactionalId);

    ITransactionalProducer CreateProducer(string transactionalId);

    ITransactionalConsumer CreateConsumer();

    void CreateTopic();
}
