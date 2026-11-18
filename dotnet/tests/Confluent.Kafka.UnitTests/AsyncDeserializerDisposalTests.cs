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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// How <see cref="AsyncKafkaConsumer{TKey, TValue}"/> disposes its asynchronous deserializers: through
/// <see cref="IAsyncDisposable"/> when a deserializer has it, else through <see cref="IDisposable"/>, once, the key
/// before the value, and it waits for a <c>DisposeAsync()</c> on every path, the synchronous ones included.
/// </summary>
public sealed class AsyncDeserializerDisposalTests
{
    private const string CloseHow = "close";
    private const string DisposeHow = "dispose";
    private const string DisposeAsyncHow = "dispose-async";

    [Theory]
    [InlineData(CloseHow)]
    [InlineData(DisposeHow)]
    [InlineData(DisposeAsyncHow)]
    public async Task ADeserializerWithBothInterfaces_IsDisposedOnce_ThroughIAsyncDisposable(string how)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Both key = new Both("key", log);
        Both value = new Both("value", log);
        AsyncKafkaConsumer<string, string> consumer = new AsyncKafkaConsumer<string, string>(Unreachable(), key, value);

        await CloseBy(consumer, how);

        Assert.Equal(new[] { "key began", "key ended", "value began", "value ended" }, log.ToArray());
        await TestTimeout.DisposeAsync(consumer);
        Assert.Equal(1, key.AsyncDisposals);
        Assert.Equal(1, value.AsyncDisposals);
        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    [Theory]
    [InlineData(CloseHow)]
    [InlineData(DisposeHow)]
    [InlineData(DisposeAsyncHow)]
    public async Task ADeserializerThatIsOnlyIAsyncDisposable_IsDisposedOnce_AndWaitedFor(string how)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        AsyncOnly key = new AsyncOnly("key", log);
        AsyncOnly value = new AsyncOnly("value", log);
        AsyncKafkaConsumer<string, string> consumer = new AsyncKafkaConsumer<string, string>(Unreachable(), key, value);

        await CloseBy(consumer, how);

        Assert.Equal(new[] { "key began", "key ended", "value began", "value ended" }, log.ToArray());
        await TestTimeout.DisposeAsync(consumer);
        Assert.Equal(1, key.AsyncDisposals);
        Assert.Equal(1, value.AsyncDisposals);
    }

    [Theory]
    [InlineData(CloseHow)]
    [InlineData(DisposeHow)]
    [InlineData(DisposeAsyncHow)]
    public async Task ADeserializerThatIsOnlyIDisposable_IsDisposedOnce_ThroughIt(string how)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        SyncOnly key = new SyncOnly("key", log);
        SyncOnly value = new SyncOnly("value", log);
        AsyncKafkaConsumer<string, string> consumer = new AsyncKafkaConsumer<string, string>(Unreachable(), key, value);

        await CloseBy(consumer, how);

        Assert.Equal(new[] { "key disposed", "value disposed" }, log.ToArray());
        await TestTimeout.DisposeAsync(consumer);
        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(CloseHow)]
    [InlineData(DisposeHow)]
    [InlineData(DisposeAsyncHow)]
    public async Task OneOfEachKind_IsDisposedThroughItsOwnInterface_TheKeyFirst(string how)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        SyncOnly key = new SyncOnly("key", log);
        Both value = new Both("value", log);
        AsyncKafkaConsumer<string, string> consumer = new AsyncKafkaConsumer<string, string>(Unreachable(), key, value);

        await CloseBy(consumer, how);

        Assert.Equal(new[] { "key disposed", "value began", "value ended" }, log.ToArray());
        await TestTimeout.DisposeAsync(consumer);
        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.AsyncDisposals);
        Assert.Equal(0, value.Disposals);
    }

    [Fact]
    public void AFailedConstruction_DisposesBoth_ThroughIAsyncDisposable_BeforeItThrows()
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Both key = new Both("key", log);
        AsyncOnly value = new AsyncOnly("value", log);
        Dictionary<string, object?> configs = Unreachable();
        configs["group.id"] = string.Empty;

        KafkaException failure = Assert.ThrowsAny<KafkaException>(
            () => new AsyncKafkaConsumer<string, string>(configs, key, value));

        Assert.Equal("Failed to construct kafka consumer", failure.Message);
        Assert.Equal(
            "The configured group.id should not be an empty string or whitespace.",
            Assert.IsAssignableFrom<KafkaException>(failure.InnerException).Message);
        Assert.Equal(new[] { "key began", "key ended", "value began", "value ended" }, log.ToArray());
        Assert.Equal(1, key.AsyncDisposals);
        Assert.Equal(0, key.Disposals);
        Assert.Equal(1, value.AsyncDisposals);
    }

    private static async Task CloseBy(AsyncKafkaConsumer<string, string> consumer, string how)
    {
        switch (how)
        {
            case CloseHow:
                await TestTimeout.Run(() => consumer.Close(), TestTimeout.TeardownBound);
                break;
            case DisposeHow:
                TestTimeout.Dispose(consumer);
                break;
            default:
                await TestTimeout.DisposeAsync(consumer);
                break;
        }
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "async-deserializer-disposal-group",
            ["enable.auto.commit"] = "false",
        };

    /// <summary>A deserializer of strings that is never called, recording how it is disposed.</summary>
    private abstract class Recording : IAsyncDeserializer<string>
    {
        private readonly string _name;
        private readonly ConcurrentQueue<string> _log;

        private protected Recording(string name, ConcurrentQueue<string> log)
        {
            _name = name;
            _log = log;
        }

        internal int Disposals { get; private set; }

        internal int AsyncDisposals { get; private set; }

        public ValueTask<string?> Deserialize(
            string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) => default;

        private protected void Disposed()
        {
            Disposals++;
            _log.Enqueue(_name + " disposed");
        }

        // Completes on another thread, so a caller that does not wait for it is seen by the log's order.
        private protected async ValueTask DisposedAsync()
        {
            AsyncDisposals++;
            _log.Enqueue(_name + " began");
            await Task.Delay(TimeSpan.FromMilliseconds(50)).ConfigureAwait(false);
            _log.Enqueue(_name + " ended");
        }
    }

    private sealed class Both : Recording, IAsyncDisposable, IDisposable
    {
        internal Both(string name, ConcurrentQueue<string> log)
            : base(name, log)
        {
        }

        public void Dispose() => Disposed();

        public ValueTask DisposeAsync() => DisposedAsync();
    }

    private sealed class AsyncOnly : Recording, IAsyncDisposable
    {
        internal AsyncOnly(string name, ConcurrentQueue<string> log)
            : base(name, log)
        {
        }

        public ValueTask DisposeAsync() => DisposedAsync();
    }

    private sealed class SyncOnly : Recording, IDisposable
    {
        internal SyncOnly(string name, ConcurrentQueue<string> log)
            : base(name, log)
        {
        }

        public void Dispose() => Disposed();
    }
}
