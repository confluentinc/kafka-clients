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

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// The async flavour's producer behind the factory seam: an
/// <see cref="AsyncMockProducer{TKey, TValue}"/> with the <see cref="TestProducerBehaviour"/>
/// applied around it, every observation reported to a <see cref="ProducerProbe"/>, and every
/// token the servicer passes recorded (T12). Every call reaches the real mock (DoD §12).
/// </summary>
/// <remarks>
/// The async <c>Send</c> is deferred: it appends to the binding's accumulator and returns, and
/// the binding's send-batch thread hands the record to the native mock later. So two scenario
/// steps that the sync double can take at once wait here for the mock to catch up, each with a
/// bounded spin: driving <c>ErrorNext</c> (it fails only a record the mock already holds, so
/// it is retried until it finds one), and disposing the mock for T11 (the healthy records'
/// outcomes are awaited first, so the disposal cannot fault a record that had not yet reached
/// the mock).
/// </remarks>
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

    /// <inheritdoc/>
    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(ProducerRecord<byte[], byte[]> record, CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(Send), cancellationToken);
        return _inner.Send(record, cancellationToken);
    }

    /// <inheritdoc/>
    public ValueTask<AsyncKafkaFuture<RecordMetadata>> Send(
        ProducerRecord<byte[], byte[]> record,
        IDeliveryCallback callback,
        CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(Send), cancellationToken);
        ulong index = ProducerProbe.IndexOf(record.Key);
        _sends++;
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
                && !SpinWait.SpinUntil(() => _inner.ErrorNext(code, message), s_mockTimeout))
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

    /// <inheritdoc/>
    public Task Flush(CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(Flush), cancellationToken);
        return _inner.Flush(cancellationToken);
    }

    /// <inheritdoc/>
    public Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(PartitionsFor), cancellationToken);
        return _inner.PartitionsFor(topic, cancellationToken);
    }

    /// <inheritdoc/>
    public IReadOnlyDictionary<MetricName, IMetric> Metrics() => _inner.Metrics();

    /// <inheritdoc/>
    public async Task Close(CancellationToken cancellationToken = default)
    {
        _probe.Tokens.Record(nameof(Close), cancellationToken);
        _probe.CloseEntered.Set();

        // Blocks the calling thread rather than awaiting (TestProducerBehaviour.HoldClose): an
        // async method runs synchronously up to its first await, so if a stop ever resumed the
        // loop inline, the stopping call itself would be held here (T6b).
        _behaviour.HoldClose?.Wait();
        await _inner.Close(cancellationToken).ConfigureAwait(false);
        _probe.OnClosed();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _inner.Dispose();
        _probe.OnDisposed();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await _inner.DisposeAsync().ConfigureAwait(false);
        _probe.OnDisposedAsync();
    }

    /// <summary>
    /// The held admission stage (<see cref="TestProducerBehaviour.HoldAdmissionOf"/>): the record
    /// is already accepted, and the stage ends either when the gate opens, yielding the mock's
    /// own future, or — as the real <c>Send</c>'s does — when the caller's token fires.
    /// </summary>
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

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            _probe.OnCallback(_index);
            _target.OnCompletion(metadata, exception);
        }
    }
}
