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

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// The sync flavour's producer behind the factory seam: a <see cref="MockProducer{TKey, TValue}"/>
/// with the <see cref="TestProducerBehaviour"/> applied around it, and every observation
/// reported to a <see cref="ProducerProbe"/>. Every call the servicer makes reaches the real
/// mock (DoD §12): the double only adds the scenario and the counting.
/// </summary>
internal sealed class SyncTestProducer : IProducer<byte[], byte[]>
{
    private readonly MockProducer<byte[], byte[]> _inner;
    private readonly TestProducerBehaviour _behaviour;
    private readonly ProducerProbe _probe;
    private int _sends;

    internal SyncTestProducer(TestProducerBehaviour behaviour, ProducerProbe probe)
    {
        _behaviour = behaviour;
        _probe = probe;
        _inner = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: behaviour.FailEach is null);
    }

    /// <inheritdoc/>
    public KafkaFuture<RecordMetadata> Send(ProducerRecord<byte[], byte[]> record) => _inner.Send(record);

    /// <inheritdoc/>
    public KafkaFuture<RecordMetadata> Send(ProducerRecord<byte[], byte[]> record, IDeliveryCallback callback)
    {
        ulong index = ProducerProbe.IndexOf(record.Key);
        _sends++;
        if (_behaviour.DisposeAfterSends is int disposeAfter && _sends == disposeAfter + 1)
        {
            _inner.Dispose();
        }

        try
        {
            KafkaFuture<RecordMetadata> future = _inner.Send(record, new CountingCallback(callback, index, _probe));
            if (_behaviour.FailEach is (int code, string message))
            {
                _inner.ErrorNext(code, message);
            }

            return future;
        }
        catch (Exception e)
        {
            _probe.SendThrew[index] = e;
            throw;
        }
    }

    /// <inheritdoc/>
    public void Flush() => _inner.Flush();

    /// <inheritdoc/>
    public IReadOnlyList<PartitionInfo> PartitionsFor(string topic) => _inner.PartitionsFor(topic);

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _inner.Metrics();

    /// <inheritdoc/>
    public void Close()
    {
        _probe.CloseEntered.Set();
        _behaviour.HoldClose?.Wait();
        _inner.Close();
        _probe.OnClosed();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _inner.Dispose();
        _probe.OnDisposed();
    }

    /// <summary>Counts each delivery-callback invocation, then forwards it unchanged.</summary>
    private sealed class CountingCallback : IDeliveryCallback
    {
        private readonly IDeliveryCallback _target;
        private readonly ulong _index;
        private readonly ProducerProbe _probe;

        internal CountingCallback(IDeliveryCallback target, ulong index, ProducerProbe probe)
        {
            _target = target;
            _index = index;
            _probe = probe;
        }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            _probe.OnCallback(_index);
            _target.OnCompletion(metadata, exception);
        }
    }
}
