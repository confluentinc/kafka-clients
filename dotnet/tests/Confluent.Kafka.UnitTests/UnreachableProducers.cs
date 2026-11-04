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
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading;

using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Real producers on an unreachable broker (<c>localhost:1</c>), where a record waits for its topic's metadata for
/// <c>max.block.ms</c> and then fails, and the probes the tests built on them share.
/// </summary>
internal static class UnreachableProducers
{
    internal const string SyncFlavor = "sync";

    internal const string AsyncFlavor = "async";

    internal static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    internal static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(300);

    internal static KafkaProducer<byte[], byte[]> Sync(int maxBlockMs) =>
        new KafkaProducer<byte[], byte[]>(Config(maxBlockMs), Serdes.ByteArray, Serdes.ByteArray);

    internal static AsyncKafkaProducer<byte[], byte[]> Async(int maxBlockMs) =>
        new AsyncKafkaProducer<byte[], byte[]>(Config(maxBlockMs), Serdes.ByteArray, Serdes.ByteArray);

    internal static string Topic(string prefix) => prefix + "-" + Guid.NewGuid().ToString("N");

    internal static ProducerRecord<byte[], byte[]> Record(string topic) =>
        new ProducerRecord<byte[], byte[]>(topic, Encoding.UTF8.GetBytes("v"));

    internal static string MetadataTimeout(string topic, int maxBlockMs) =>
        $"Topic {topic} not present in metadata after {maxBlockMs} ms.";

    internal static DestroyLog WatchDestroy(object producer, Action? onDestroy = null)
    {
        NativeProducer native = (NativeProducer)producer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;
        FieldInfo afterDestroy = typeof(SafeProducerHandle)
            .GetField("_afterDestroy", BindingFlags.NonPublic | BindingFlags.Instance)!;
        Action release = (Action)afterDestroy.GetValue(native.Handle)!;
        DestroyLog log = new DestroyLog();
        afterDestroy.SetValue(native.Handle, new Action(() =>
        {
            onDestroy?.Invoke();
            log.Add(Thread.CurrentThread);
            release();
        }));
        return log;
    }

    private static Dictionary<string, string> Config(int maxBlockMs) =>
        new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = maxBlockMs.ToString(CultureInfo.InvariantCulture),
        };

    /// <summary>The threads a producer's native handle was destroyed on, with what had happened before.</summary>
    internal sealed class DestroyLog
    {
        private readonly List<(int ThreadId, string? ThreadName)> _destroys = new List<(int ThreadId, string? ThreadName)>();

        internal IReadOnlyList<(int ThreadId, string? ThreadName)> Destroys()
        {
            lock (_destroys)
            {
                return new List<(int ThreadId, string? ThreadName)>(_destroys);
            }
        }

        internal bool WaitForADestroy(TimeSpan timeout)
        {
            lock (_destroys)
            {
                return _destroys.Count > 0 || (Monitor.Wait(_destroys, timeout) && _destroys.Count > 0);
            }
        }

        internal void Add(Thread thread)
        {
            lock (_destroys)
            {
                _destroys.Add((thread.ManagedThreadId, thread.Name));
                Monitor.PulseAll(_destroys);
            }
        }
    }

    /// <summary>Captures the trace lines that contain a given text, with the thread that wrote each.</summary>
    internal sealed class TraceCapture : TraceListener
    {
        private readonly string _text;
        private readonly List<(int Thread, string Line)> _lines = new List<(int Thread, string Line)>();

        internal TraceCapture(string text)
        {
            _text = text;
            Trace.Listeners.Add(this);
        }

        public override void Write(string? message) => Capture(message);

        public override void WriteLine(string? message) => Capture(message);

        internal IReadOnlyList<string> Lines()
        {
            lock (_lines)
            {
                return _lines.ConvertAll(line => line.Line);
            }
        }

        internal IReadOnlyList<string> LinesFrom(int thread)
        {
            lock (_lines)
            {
                return _lines.FindAll(line => line.Thread == thread).ConvertAll(line => line.Line);
            }
        }

        protected override void Dispose(bool disposing)
        {
            Trace.Listeners.Remove(this);
            base.Dispose(disposing);
        }

        private void Capture(string? message)
        {
            if (message is not null && message.Contains(_text))
            {
                lock (_lines)
                {
                    _lines.Add((Environment.CurrentManagedThreadId, message));
                }
            }
        }
    }

    /// <summary>A delivery callback that runs a test's body on the producer's thread and records its own end.</summary>
    internal sealed class ProbeCallback : IDeliveryCallback
    {
        private readonly Action<RecordMetadata, Exception?> _body;
        private readonly ManualResetEventSlim _done = new ManualResetEventSlim();
        private int _calls;

        internal ProbeCallback(Action<RecordMetadata, Exception?> body) => _body = body;

        internal int Calls => Volatile.Read(ref _calls);

        internal string? ThreadName { get; private set; }

        internal Exception? Delivered { get; private set; }

        internal Exception? BodyFailure { get; private set; }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Interlocked.Increment(ref _calls);
            ThreadName = Thread.CurrentThread.Name;
            Delivered = exception;
            try
            {
                _body(metadata, exception);
            }
            catch (Exception failure)
            {
                BodyFailure = failure;
            }
            finally
            {
                _done.Set();
            }
        }

        internal bool WaitDone(TimeSpan timeout) => _done.Wait(timeout);
    }

    /// <summary>
    /// A delivery callback that holds the producer's completion thread until released, so the completions queued
    /// behind it, the delivery of a later record included, wait.
    /// </summary>
    internal sealed class HoldingCallback : IDeliveryCallback
    {
        private readonly ManualResetEventSlim _entered = new ManualResetEventSlim();
        private readonly ManualResetEventSlim _release = new ManualResetEventSlim();

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            _entered.Set();
            _ = _release.Wait(Deadline);
        }

        internal bool WaitEntered(TimeSpan timeout) => _entered.Wait(timeout);

        internal void Release() => _release.Set();
    }
}
