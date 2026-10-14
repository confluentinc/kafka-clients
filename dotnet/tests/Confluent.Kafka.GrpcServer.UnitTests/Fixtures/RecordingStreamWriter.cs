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
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// The response stream of one <c>RunProducer</c> / <c>RunConsumer</c> call (PLAN §7.1): records
/// every batch written and, through <see cref="TestServerCallContext"/>, every response-header
/// write, in one ordered log, so a test can assert both the events and where the headers fell.
/// </summary>
/// <remarks>
/// Writes come from the RPC handler, waits from the test thread; one lock guards the log and
/// <see cref="Monitor.PulseAll"/> wakes a waiter on every append, so <see cref="WaitFor"/> needs
/// no polling or sleeping. gRPC allows one write at a time, which the handler honours; the
/// writer counts any overlap so a test can assert there was none.
/// </remarks>
internal sealed class RecordingStreamWriter : IServerStreamWriter<Proto.WorkloadEventBatch>
{
    /// <summary>The default bound on every wait: generous, because it is a hang guard, not a timing assertion.</summary>
    internal static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    private readonly object _lock = new object();
    private readonly List<Proto.WorkloadEventBatch> _batches = new List<Proto.WorkloadEventBatch>();
    private readonly List<Proto.WorkloadEvent> _events = new List<Proto.WorkloadEvent>();
    private int _headerWrites;
    private int _headerPosition = -1;
    private int _writesInFlight;
    private int _overlappingWrites;

    /// <inheritdoc/>
    public WriteOptions? WriteOptions { get; set; }

    /// <summary>Every batch written so far, in order.</summary>
    internal IReadOnlyList<Proto.WorkloadEventBatch> Batches
    {
        get
        {
            lock (_lock)
            {
                return _batches.ToList();
            }
        }
    }

    /// <summary>Every event written so far, flattened across batches, in order.</summary>
    internal IReadOnlyList<Proto.WorkloadEvent> Events
    {
        get
        {
            lock (_lock)
            {
                return _events.ToList();
            }
        }
    }

    /// <summary>How many times the handler wrote the response headers.</summary>
    internal int HeaderWrites
    {
        get
        {
            lock (_lock)
            {
                return _headerWrites;
            }
        }
    }

    /// <summary>
    /// How many batches had been written when the headers were first written (0 = before any
    /// message), or -1 when they never were.
    /// </summary>
    internal int HeaderPosition
    {
        get
        {
            lock (_lock)
            {
                return _headerPosition;
            }
        }
    }

    /// <summary>How many writes started while another was still in progress (gRPC forbids it).</summary>
    internal int OverlappingWrites => Volatile.Read(ref _overlappingWrites);

    /// <inheritdoc/>
    public Task WriteAsync(Proto.WorkloadEventBatch message)
    {
        if (Interlocked.Increment(ref _writesInFlight) > 1)
        {
            Interlocked.Increment(ref _overlappingWrites);
        }

        try
        {
            lock (_lock)
            {
                _batches.Add(message);
                _events.AddRange(message.Events);
                Monitor.PulseAll(_lock);
            }
        }
        finally
        {
            Interlocked.Decrement(ref _writesInFlight);
        }

        return Task.CompletedTask;
    }

    /// <summary>Called by <see cref="TestServerCallContext"/> when the handler writes the headers.</summary>
    internal void RecordHeaders()
    {
        lock (_lock)
        {
            _headerWrites++;
            if (_headerPosition < 0)
            {
                _headerPosition = _batches.Count;
            }

            Monitor.PulseAll(_lock);
        }
    }

    /// <summary>
    /// Waits until <paramref name="condition"/> holds over the events written so far, and fails
    /// the test with <paramref name="what"/> if it does not within <see cref="DefaultTimeout"/>.
    /// </summary>
    internal IReadOnlyList<Proto.WorkloadEvent> WaitFor(Func<IReadOnlyList<Proto.WorkloadEvent>, bool> condition, string what)
    {
        Stopwatch clock = Stopwatch.StartNew();
        lock (_lock)
        {
            while (!condition(_events))
            {
                TimeSpan remaining = DefaultTimeout - clock.Elapsed;
                if (remaining <= TimeSpan.Zero)
                {
                    throw new TimeoutException(
                        $"timed out after {DefaultTimeout.TotalSeconds} s waiting for {what}; " +
                        $"{_events.Count} event(s) written: {Describe(_events)}");
                }

                Monitor.Wait(_lock, remaining);
            }

            return _events.ToList();
        }
    }

    /// <summary>Waits until at least <paramref name="count"/> events of <paramref name="kind"/> were written.</summary>
    internal IReadOnlyList<Proto.WorkloadEvent> WaitForCount(Proto.WorkloadEvent.EventOneofCase kind, int count) =>
        WaitFor(events => events.Count(e => e.EventCase == kind) >= count, $"{count} {kind} event(s)");

    /// <summary>Waits until the headers were written.</summary>
    internal void WaitForHeaders()
    {
        Stopwatch clock = Stopwatch.StartNew();
        lock (_lock)
        {
            while (_headerWrites == 0)
            {
                TimeSpan remaining = DefaultTimeout - clock.Elapsed;
                if (remaining <= TimeSpan.Zero)
                {
                    throw new TimeoutException($"timed out after {DefaultTimeout.TotalSeconds} s waiting for the response headers");
                }

                Monitor.Wait(_lock, remaining);
            }
        }
    }

    /// <summary>A compact, readable rendering of an event list for failure messages.</summary>
    internal static string Describe(IEnumerable<Proto.WorkloadEvent> events) =>
        string.Join(", ", events.Take(40).Select(e => e.ToString()));
}
