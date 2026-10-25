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
/// The response stream of one <c>Run*</c> call: one ordered log of every batch written and every
/// response-header write, so a test can assert the events and where the headers fell, and a count of
/// overlapping writes, which gRPC forbids.
/// </summary>
internal sealed class RecordingStreamWriter : IServerStreamWriter<Proto.WorkloadEventBatch>
{
    // Generous: a hang guard, not a timing assertion.
    internal static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    private readonly object _lock = new object();
    private readonly List<Proto.WorkloadEventBatch> _batches = new List<Proto.WorkloadEventBatch>();
    private readonly List<Proto.WorkloadEvent> _events = new List<Proto.WorkloadEvent>();
    private int _headerWrites;
    private int _headerPosition = -1;
    private int _writesInFlight;
    private int _overlappingWrites;

    public WriteOptions? WriteOptions { get; set; }

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

    // The batches written before the headers first were (0 = before any message), or -1 when they
    // never were.
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

    internal int OverlappingWrites => Volatile.Read(ref _overlappingWrites);

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

    internal IReadOnlyList<Proto.WorkloadEvent> WaitForCount(Proto.WorkloadEvent.EventOneofCase kind, int count) =>
        WaitFor(events => events.Count(e => e.EventCase == kind) >= count, $"{count} {kind} event(s)");

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

    internal static string Describe(IEnumerable<Proto.WorkloadEvent> events) =>
        string.Join(", ", events.Take(40).Select(e => e.ToString()));
}
