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
using System.Threading.Tasks;

using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;

using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

namespace Confluent.Kafka.UnitTests.Internal;

/// <summary>
/// Tests <see cref="SerdeDisposal"/>: a serde that is <see cref="IDisposable"/> is disposed, a <c>Dispose</c> that
/// throws is traced and not rethrown, any other serde is left alone, and the pair is disposed once, key then value.
/// Translates Python's <c>close_if_defined</c> tests in <c>test_serde_supply.py</c>.
/// </summary>
public sealed class SerdeDisposalTests
{
    [Fact]
    public void TestLifecycleHelpers_test_close_if_defined_calls_close_DisposesAnIDisposable()
    {
        CountingSerde serde = new CountingSerde();

        SerdeDisposal.DisposeIfDisposable(serde);

        Assert.Equal(1, serde.Disposals);
    }

    [Fact]
    public void TestLifecycleHelpers_test_close_if_defined_logs_not_raises_TracesOneWarningAndDoesNotRethrow()
    {
        string id = Guid.NewGuid().ToString("N");
        ThrowingSerde serde = new ThrowingSerde(id);
        using TraceCapture trace = new TraceCapture(id);

        SerdeDisposal.DisposeIfDisposable(serde);

        Assert.NotNull(serde.Thrown);
        string message = $"Exception while closing serde {serde}: {serde.Thrown}";
        Assert.Equal(message, Assert.Single(trace.Lines()));
        Assert.Equal((TraceEventType.Warning, message), Assert.Single(trace.Events()));
    }

    [Fact]
    public void DisposeIfDisposable_ASerdeWhoseToStringThrows_DoesNotRethrow()
    {
        ThrowingSerde serde = new ThrowingSerde(Guid.NewGuid().ToString("N"), toStringThrows: true);

        SerdeDisposal.DisposeIfDisposable(serde);

        Assert.NotNull(serde.Thrown);
    }

    [Fact]
    public void TestLifecycleHelpers_test_close_if_defined_noop_on_bare_function_LeavesASerdeThatIsNotIDisposableAlone()
    {
        AsyncOnlySerde asyncOnly = new AsyncOnlySerde();

        SerdeDisposal.DisposeIfDisposable(Serdes.String);
        SerdeDisposal.DisposeIfDisposable(asyncOnly);

        Assert.Equal(0, asyncOnly.Disposals);
    }

    [Fact]
    public void DisposeOnce_DisposesTheKeyThenTheValue_Once()
    {
        List<string> log = new List<string>();
        SerdeDisposal pair = new SerdeDisposal(new CountingSerde("key", log), new CountingSerde("value", log));

        pair.DisposeOnce();
        pair.DisposeOnce();

        Assert.Equal(new[] { "key", "value" }, log);
    }

    [Fact]
    public void DisposeOnce_AKeyWhoseDisposeThrows_StillDisposesTheValue()
    {
        CountingSerde value = new CountingSerde();
        SerdeDisposal pair = new SerdeDisposal(new ThrowingSerde(Guid.NewGuid().ToString("N")), value);

        pair.DisposeOnce();

        Assert.Equal(1, value.Disposals);
    }

    /// <summary>A serializer that counts its <c>Dispose</c> calls and adds its name to an optional log.</summary>
    private sealed class CountingSerde : ISerializer<string>, IDisposable
    {
        private readonly string _name;
        private readonly List<string>? _log;

        internal CountingSerde(string name = "serde", List<string>? log = null)
        {
            _name = name;
            _log = log;
        }

        internal int Disposals { get; private set; }

        public byte[]? Serialize(string topic, string data) => null;

        public void Dispose()
        {
            Disposals++;
            _log?.Add(_name);
        }
    }

    /// <summary>A serializer whose <c>Dispose</c> throws, and whose <c>ToString</c> can throw too.</summary>
    private sealed class ThrowingSerde : ISerializer<string>, IDisposable
    {
        private readonly string _id;
        private readonly bool _toStringThrows;

        internal ThrowingSerde(string id, bool toStringThrows = false)
        {
            _id = id;
            _toStringThrows = toStringThrows;
        }

        internal Exception? Thrown { get; private set; }

        public byte[]? Serialize(string topic, string data) => null;

        public void Dispose()
        {
            InvalidOperationException thrown = new InvalidOperationException("dispose failed " + _id);
            Thrown = thrown;
            throw thrown;
        }

        public override string ToString() =>
            _toStringThrows ? throw new InvalidOperationException("ToString failed") : "ThrowingSerde-" + _id;
    }

    /// <summary>
    /// A serializer that is only <see cref="IAsyncDisposable"/>, counting its <c>DisposeAsync</c> calls.
    /// </summary>
    private sealed class AsyncOnlySerde : ISerializer<string>, IAsyncDisposable
    {
        internal int Disposals { get; private set; }

        public byte[]? Serialize(string topic, string data) => null;

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return default;
        }
    }
}
