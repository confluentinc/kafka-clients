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
    internal (int Code, string Message)? FailEach { get; init; }

    internal ManualResetEventSlim? HoldClose { get; init; }

    // The mock is disposed just before the send that follows this many, so it and every later send
    // throw.
    internal int? DisposeAfterSends { get; init; }

    // Async flavour only: this record's first stage is held until AdmissionGate completes, the
    // record already accepted.
    internal ulong? HoldAdmissionOf { get; init; }

    internal TaskCompletionSource? AdmissionGate { get; init; }
}

/// <summary>What a test producer observed, from whichever thread observed it.</summary>
internal sealed class ProducerProbe
{
    private int _closeCalls;
    private int _disposeCalls;
    private int _disposeAsyncCalls;

    internal ConcurrentDictionary<ulong, int> CallbackFires { get; } = new ConcurrentDictionary<ulong, int>();

    internal ConcurrentDictionary<ulong, Exception> SendThrew { get; } = new ConcurrentDictionary<ulong, Exception>();

    internal ManualResetEventSlim CloseEntered { get; } = new ManualResetEventSlim();

    internal IReadOnlyDictionary<string, string>? Config { get; set; }

    internal int CloseCalls => Volatile.Read(ref _closeCalls);

    internal int DisposeCalls => Volatile.Read(ref _disposeCalls);

    internal int DisposeAsyncCalls => Volatile.Read(ref _disposeAsyncCalls);

    internal ManualResetEventSlim AdmissionHeld { get; } = new ManualResetEventSlim();

    internal TokenLog Tokens { get; } = new TokenLog();

    internal TimeSpan? CloseTimeout { get; set; }

    internal void OnClosed() => Interlocked.Increment(ref _closeCalls);

    internal void OnDisposed() => Interlocked.Increment(ref _disposeCalls);

    internal void OnDisposedAsync()
    {
        Interlocked.Increment(ref _disposeAsyncCalls);
        OnDisposed();
    }

    internal void OnCallback(ulong index) => CallbackFires.AddOrUpdate(index, 1, static (_, n) => n + 1);

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
