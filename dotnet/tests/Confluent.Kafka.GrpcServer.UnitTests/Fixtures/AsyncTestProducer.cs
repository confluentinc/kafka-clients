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
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// The async flavour's producer behind the factory seam: an <see cref="AsyncMockProducer{TKey, TValue}"/>
/// with the <see cref="TestProducerBehaviour"/> applied around it, every observation reported to a
/// <see cref="ProducerProbe"/> and every token recorded; every call reaches the real mock.
/// </summary>
internal sealed class AsyncTestProducer : IAsyncProducer<byte[], byte[]>
{
    private static readonly TimeSpan s_mockTimeout = TimeSpan.FromSeconds(10);

    private readonly AsyncMockProducer<byte[], byte[]> _inner;
    private readonly TestProducerBehaviour _behaviour;
    private readonly ProducerProbe _probe;
    private int _sends;

    internal AsyncTestProducer(TestProducerBehaviour behaviour, ProducerProbe probe)
    {
        _behaviour = behaviour;
        _probe = probe;
        _inner = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: behaviour.FailEach is null);
    }

    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<byte[], byte[]> record, CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(Send), cancellationToken);
        return _inner.Send(record, cancellationToken);
    }

    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        ProducerRecord<byte[], byte[]> record,
        IDeliveryCallback callback,
        CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(Send), cancellationToken);
        ulong index = ProducerProbe.IndexOf(record.Key);
        _sends++;

        // Both scenario steps wait for the mock with a bounded spin: the disposal for the earlier
        // records' delivery callbacks, which run later on the producer's pump, and ErrorNext, which
        // fails only a record the mock holds.
        if (_behaviour.DisposeAfterSends is int disposeAfter && _sends == disposeAfter + 1)
        {
            if (!SpinWait.SpinUntil(() => _probe.CallbackFires.Count == disposeAfter, s_mockTimeout))
            {
                throw new InvalidOperationException($"the first {disposeAfter} records were not delivered before the disposal");
            }

            _inner.Dispose();
        }

        try
        {
            ValueTask<AsyncKafkaFuture<RecordMetadata>> admission =
                _inner.Send(record, new CountingCallback(callback, index, _probe), cancellationToken);
            if (_behaviour.FailEach is (int code, string message)
                && !SpinWait.SpinUntil(() => _inner.ErrorNext(new KafkaException(code, message, isRetriable: false)), s_mockTimeout))
            {
                throw new InvalidOperationException($"record {index} never reached the mock to be failed");
            }

            return _behaviour.HoldAdmissionOf == index && _behaviour.AdmissionGate is TaskCompletionSource gate
                ? HoldAdmission(admission, gate.Task, cancellationToken)
                : admission;
        }
        catch (Exception e)
        {
            _probe.SendThrew[index] = e;
            throw;
        }
    }

    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        ProducerRecord<byte[], byte[]> record,
        IAsyncDeliveryCallback callback,
        CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(Send), cancellationToken);
        return _inner.Send(record, callback, cancellationToken);
    }

    public Task Flush(CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(Flush), cancellationToken);
        return _inner.Flush(cancellationToken);
    }

    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(PartitionsFor), cancellationToken);
        return _inner.PartitionsFor(topic, cancellationToken);
    }

    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _inner.Metrics();

    public async Task Close(CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(Close), cancellationToken);
        _probe.CloseEntered.Set();

        // Blocks the calling thread rather than awaiting: an async method runs synchronously up to
        // its first await, so if a stop ever resumed the loop inline, the stopping call itself
        // would be held here.
        _behaviour.HoldClose?.Wait();
        await _inner.Close(cancellationToken).ConfigureAwait(false);
        _probe.OnClosed();
    }

    public async Task Close(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        _probe.CloseTimeout = timeout;
        _probe.Tokens.Record(nameof(Close), cancellationToken);
        _probe.CloseEntered.Set();
        _behaviour.HoldClose?.Wait();
        await _inner.Close(timeout, cancellationToken).ConfigureAwait(false);
        _probe.OnClosed();
    }

    public void Dispose()
    {
        _inner.Dispose();
        _probe.OnDisposed();
    }

    public async ValueTask DisposeAsync()
    {
        await _inner.DisposeAsync().ConfigureAwait(false);
        _probe.OnDisposedAsync();
    }

    // The record is already accepted; the stage ends when the gate opens, yielding the mock's own
    // future, or, as the real Send's does, when the caller's token fires.
    private async ValueTask<AsyncKafkaFuture<RecordMetadata>> HoldAdmission(
        ValueTask<AsyncKafkaFuture<RecordMetadata>> admission,
        Task gate,
        CancellationToken cancellationToken)
    {
        AsyncKafkaFuture<RecordMetadata> future = await admission.ConfigureAwait(false);
        _probe.AdmissionHeld.Set();
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        return future;
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

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            _probe.OnCallback(_index);
            _target.OnCompletion(metadata, exception);
        }
    }
}
