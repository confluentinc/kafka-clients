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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// What the producer behind the factory seam does, stated once for every flavour (each flavour's
/// double — <see cref="SyncTestProducer"/> and <see cref="AsyncTestProducer"/> — interprets it
/// over its own mock). The default is a plain auto-completing mock.
/// </summary>
internal sealed class TestProducerBehaviour
{
    /// <summary>
    /// When set, every record fails through its delivery callback with this code and message
    /// (<c>ErrorNext</c> after each <c>Send</c> on a non-auto-completing mock, T9).
    /// </summary>
    internal (int Code, string Message)? FailEach { get; init; }

    /// <summary>
    /// When set, <c>Close</c> blocks its <b>calling thread</b> until this is set (T6b): blocking,
    /// not awaiting, so a drain that a stop ran inline would hold the stopping call too.
    /// </summary>
    internal ManualResetEventSlim? HoldClose { get; init; }

    /// <summary>
    /// When set, the mock is disposed just before the send that follows this many, so it and
    /// every later send throw (T11).
    /// </summary>
    internal int? DisposeAfterSends { get; init; }

    /// <summary>
    /// Async flavour only (T12): the send of this record index is accepted by the mock at once,
    /// but its admission stage is held until <see cref="AdmissionGate"/> completes. A token that
    /// fires while it is held ends the stage with an <see cref="OperationCanceledException"/>
    /// although the record is still sent — the real <c>Send</c>'s contract (M11/P3.5 D2 (c)).
    /// </summary>
    internal ulong? HoldAdmissionOf { get; init; }

    /// <summary>Releases the admission stage <see cref="HoldAdmissionOf"/> holds.</summary>
    internal TaskCompletionSource? AdmissionGate { get; init; }
}

/// <summary>What a test producer observed, from whichever thread observed it.</summary>
internal sealed class ProducerProbe
{
    private int _closeCalls;
    private int _disposeCalls;
    private int _disposeAsyncCalls;

    /// <summary>Delivery-callback invocations per record index.</summary>
    internal ConcurrentDictionary<ulong, int> CallbackFires { get; } = new ConcurrentDictionary<ulong, int>();

    /// <summary>The exception each throwing <c>Send</c> threw, per record index.</summary>
    internal ConcurrentDictionary<ulong, Exception> SendThrew { get; } = new ConcurrentDictionary<ulong, Exception>();

    /// <summary>Set when <c>Close</c> is entered (before any <see cref="TestProducerBehaviour.HoldClose"/> wait).</summary>
    internal ManualResetEventSlim CloseEntered { get; } = new ManualResetEventSlim();

    /// <summary>The config the factory was called with.</summary>
    internal IReadOnlyDictionary<string, string>? Config { get; set; }

    /// <summary>How many times <c>Close</c> returned.</summary>
    internal int CloseCalls => Volatile.Read(ref _closeCalls);

    /// <summary>How many times the producer was disposed, by <c>Dispose</c> or <c>DisposeAsync</c>.</summary>
    internal int DisposeCalls => Volatile.Read(ref _disposeCalls);

    /// <summary>How many of <see cref="DisposeCalls"/> were <c>DisposeAsync</c> (the async flavour's).</summary>
    internal int DisposeAsyncCalls => Volatile.Read(ref _disposeAsyncCalls);

    /// <summary>Set when the async double starts holding an admission stage (<see cref="TestProducerBehaviour.HoldAdmissionOf"/>).</summary>
    internal ManualResetEventSlim AdmissionHeld { get; } = new ManualResetEventSlim();

    /// <summary>The tokens the async servicer passed to the producer (T12).</summary>
    internal TokenLog Tokens { get; } = new TokenLog();

    internal void OnClosed() => Interlocked.Increment(ref _closeCalls);

    internal void OnDisposed() => Interlocked.Increment(ref _disposeCalls);

    internal void OnDisposedAsync()
    {
        Interlocked.Increment(ref _disposeAsyncCalls);
        OnDisposed();
    }

    internal void OnCallback(ulong index) => CallbackFires.AddOrUpdate(index, 1, static (_, n) => n + 1);

    /// <summary>The record index a chaos producer encoded in an 8-byte big-endian key.</summary>
    internal static ulong IndexOf(byte[]? key)
    {
        if (key is null || key.Length != 8)
        {
            throw new ArgumentException("not a chaos record key", nameof(key));
        }

        ulong index = 0;
        foreach (byte b in key)
        {
            index = (index << 8) | b;
        }

        return index;
    }
}
