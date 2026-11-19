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

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The construction check of an asynchronous serializer or deserializer: a client that disposes its serdes refuses
/// one that is <see cref="IDisposable"/>, or has a public <c>DisposeAsync()</c>, <c>Dispose()</c> or <c>Close()</c>,
/// without <see cref="IAsyncDisposable"/>, the one interface it disposes through; one that does not dispose them
/// takes every shape, and a <c>Configure</c> without its interface is taken, as on the synchronous interfaces. The asynchronous consumers
/// and producers make the check when they are built: the real ones dispose their serdes, the mocks do not.
/// </summary>
public sealed class AsyncSerdeGuardTests
{
    private const string Prefix = "Confluent.Kafka.UnitTests.AsyncSerdeGuardTests+";

    private const string NeverDisposed =
        " implements IDisposable but does not implement IAsyncDisposable, so the {0} would never dispose it.";

    // The text after the type's name, {0} being the client.
    public static TheoryData<Type, string> Refused => new TheoryData<Type, string>
    {
        { typeof(StrayDisposeAsync), NeverCalled("DisposeAsync()") },
        { typeof(StrayDispose), NeverCalled("Dispose()") },
        { typeof(StrayClose), NeverCalled("Close()") },
        { typeof(DisposableWithStrayDisposeAsync), NeverCalled("DisposeAsync()") },
        { typeof(Disposable), NeverDisposed },
        { typeof(ExplicitlyDisposable), NeverDisposed },
        { typeof(ClosingDisposable), NeverDisposed },
    };

    public static TheoryData<Type> Accepted => new TheoryData<Type>
    {
        typeof(Plain),
        typeof(AsyncDisposable),
        typeof(AsyncDisposableAndDisposable),
        typeof(AsyncDisposableWithStrayDispose),
        typeof(ClosingAsyncDisposable),
        typeof(StrayConfigure),
    };

    [Theory]
    [MemberData(nameof(Refused))]
    public void AClientThatDisposesItsSerdes_RefusesACloseItWouldNeverCall(Type shape, string what)
    {
        Serde serde = (Serde)Activator.CreateInstance(shape)!;

        IllegalArgumentException asSerializer = Assert.Throws<IllegalArgumentException>(
            () => SerdeGuard.Check((IAsyncSerializer<string>)serde, DefaultMethodGuard.Producer, disposes: true));
        IllegalArgumentException asDeserializer = Assert.Throws<IllegalArgumentException>(
            () => SerdeGuard.Check((IAsyncDeserializer<string>)serde, DefaultMethodGuard.Consumer, disposes: true));

        Assert.Equal(Prefix + shape.Name + string.Format(what, "producer"), asSerializer.Message);
        Assert.Equal(Prefix + shape.Name + string.Format(what, "consumer"), asDeserializer.Message);
        Assert.Equal(0, serde.Disposals);
    }

    [Theory]
    [MemberData(nameof(Refused))]
    public void AClientThatDoesNotDisposeItsSerdes_TakesEveryShape(Type shape, string what)
    {
        Assert.NotEmpty(what);
        Serde serde = (Serde)Activator.CreateInstance(shape)!;

        SerdeGuard.Check((IAsyncSerializer<string>)serde, DefaultMethodGuard.Producer, disposes: false);
        SerdeGuard.Check((IAsyncDeserializer<string>)serde, DefaultMethodGuard.Consumer, disposes: false);

        Assert.Equal(0, serde.Disposals);
    }

    [Theory]
    [MemberData(nameof(Accepted))]
    public void AClient_TakesASerdeItDisposesThroughIAsyncDisposable_OrThatDeclaresNoClose(Type shape)
    {
        Serde serde = (Serde)Activator.CreateInstance(shape)!;

        foreach (bool disposes in new[] { true, false })
        {
            SerdeGuard.Check((IAsyncSerializer<string>)serde, DefaultMethodGuard.Producer, disposes);
            SerdeGuard.Check((IAsyncDeserializer<string>)serde, DefaultMethodGuard.Consumer, disposes);
        }

        Assert.Equal(0, serde.Disposals);
    }

    [Fact]
    public void TheBuiltIns_AreTaken()
    {
        SerdeGuard.Check(new AsyncStringSerializer(), DefaultMethodGuard.Producer, disposes: true);
        SerdeGuard.Check(new AsyncStringDeserializer(), DefaultMethodGuard.Consumer, disposes: true);
        SerdeGuard.Check(new AsyncReadOnlyMemorySerializer(), DefaultMethodGuard.Producer, disposes: true);
        SerdeGuard.Check(new AsyncReadOnlyMemoryDeserializer(), DefaultMethodGuard.Consumer, disposes: true);
    }

    [Theory]
    [MemberData(nameof(Refused))]
    public void AnAsyncKafkaConsumer_RefusesACloseItWouldNeverCall_DisposingNothing(Type shape, string what)
    {
        foreach (bool asKey in new[] { true, false })
        {
            Serde serde = (Serde)Activator.CreateInstance(shape)!;
            AsyncDisposable other = new AsyncDisposable();

            IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
                () => new AsyncKafkaConsumer<string, string>(
                    ConsumerConfigs(), asKey ? serde : other, asKey ? other : serde));

            Assert.Equal(Prefix + shape.Name + string.Format(what, "consumer"), e.Message);
            Assert.Equal(0, serde.Disposals);
            Assert.Equal(0, other.Disposals);
        }
    }

    [Theory]
    [MemberData(nameof(Refused))]
    public async Task AnAsyncMockConsumer_TakesEveryShape_SinceItDisposesNothing(Type shape, string what)
    {
        Assert.NotEmpty(what);
        Serde serde = (Serde)Activator.CreateInstance(shape)!;
        Disposable other = new Disposable();

        await TestTimeout.DisposeAsync(new AsyncMockConsumer<string, string>(serde, other));

        Assert.Equal(0, serde.Disposals);
        Assert.Equal(0, other.Disposals);
    }

    [Theory]
    [MemberData(nameof(Accepted))]
    public async Task AnAsyncKafkaConsumer_TakesASerdeItDisposesThroughIAsyncDisposable_AndCallsOnlyItsDisposeAsyncOnce(Type shape)
    {
        foreach (bool asKey in new[] { true, false })
        {
            Serde serde = (Serde)Activator.CreateInstance(shape)!;
            Plain other = new Plain();

            await TestTimeout.DisposeAsync(new AsyncKafkaConsumer<string, string>(
                ConsumerConfigs(), asKey ? serde : other, asKey ? other : serde));

            Assert.Equal(serde is IAsyncDisposable ? 1 : 0, serde.Disposals);
        }
    }

    [Theory]
    [MemberData(nameof(Refused))]
    public void AnAsyncKafkaProducer_RefusesACloseItWouldNeverCall_DisposingNothing(Type shape, string what)
    {
        foreach (bool asKey in new[] { true, false })
        {
            Serde serde = (Serde)Activator.CreateInstance(shape)!;
            AsyncDisposable other = new AsyncDisposable();

            IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
                () => new AsyncKafkaProducer<string, string>(
                    ProducerConfigs(), asKey ? serde : other, asKey ? other : serde));

            Assert.Equal(Prefix + shape.Name + string.Format(what, "producer"), e.Message);
            Assert.Equal(0, serde.Disposals);
            Assert.Equal(0, other.Disposals);
        }
    }

    [Theory]
    [MemberData(nameof(Refused))]
    public async Task AnAsyncMockProducer_TakesEveryShape_SinceItDisposesNothing(Type shape, string what)
    {
        Assert.NotEmpty(what);
        Serde serde = (Serde)Activator.CreateInstance(shape)!;
        Disposable other = new Disposable();

        await TestTimeout.DisposeAsync(new AsyncMockProducer<string, string>(serde, other));

        Assert.Equal(0, serde.Disposals);
        Assert.Equal(0, other.Disposals);
    }

    [Theory]
    [MemberData(nameof(Accepted))]
    public async Task AnAsyncKafkaProducer_TakesASerdeItDisposesThroughIAsyncDisposable_AndCallsOnlyItsDisposeAsyncOnce(Type shape)
    {
        foreach (bool asKey in new[] { true, false })
        {
            Serde serde = (Serde)Activator.CreateInstance(shape)!;
            Plain other = new Plain();

            await TestTimeout.DisposeAsync(new AsyncKafkaProducer<string, string>(
                ProducerConfigs(), asKey ? serde : other, asKey ? other : serde));

            Assert.Equal(serde is IAsyncDisposable ? 1 : 0, serde.Disposals);
        }
    }

    private static string NeverCalled(string method) =>
        " declares " + method + " but does not implement IAsyncDisposable, so the {0} would never call it.";

    private static Dictionary<string, object?> ProducerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
        };

    private static Dictionary<string, object?> ConsumerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "async-serde-guard-group",
            ["enable.auto.commit"] = "false",
        };

    /// <summary>An asynchronous serializer and deserializer of strings that does nothing, counting its disposals.</summary>
    private abstract class Serde : IAsyncSerializer<string>, IAsyncDeserializer<string>
    {
        internal int Disposals { get; private protected set; }

        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, string? data, CancellationToken cancellationToken) => default;

        public ValueTask<string?> Deserialize(
            string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) => default;
    }

    /// <summary>A serde with no close.</summary>
    private sealed class Plain : Serde
    {
    }

    /// <summary>A serde with a public DisposeAsync() but not IAsyncDisposable.</summary>
    private sealed class StrayDisposeAsync : Serde
    {
        public ValueTask DisposeAsync()
        {
            Disposals++;
            return default;
        }
    }

    /// <summary>A serde with a public Dispose() but not IDisposable.</summary>
    private sealed class StrayDispose : Serde
    {
        public void Dispose() => Disposals++;
    }

    /// <summary>A serde with a public Close() and neither disposal interface.</summary>
    private sealed class StrayClose : Serde
    {
        public void Close() => Disposals++;
    }

    /// <summary>An IAsyncDisposable serde with a public Dispose() but not IDisposable, which nothing calls.</summary>
    private sealed class AsyncDisposableWithStrayDispose : Serde, IAsyncDisposable
    {
        public void Dispose() => Disposals += 10;

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return default;
        }
    }

    /// <summary>An IDisposable serde with a public DisposeAsync() but not IAsyncDisposable.</summary>
    private sealed class DisposableWithStrayDisposeAsync : Serde, IDisposable
    {
        public void Dispose() => Disposals++;

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return default;
        }
    }

    /// <summary>An IAsyncDisposable serde.</summary>
    private sealed class AsyncDisposable : Serde, IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            Disposals++;
            return default;
        }
    }

    /// <summary>An IDisposable serde.</summary>
    private sealed class Disposable : Serde, IDisposable
    {
        public void Dispose() => Disposals++;
    }

    /// <summary>An IDisposable serde whose Dispose() is not public.</summary>
    private sealed class ExplicitlyDisposable : Serde, IDisposable
    {
        void IDisposable.Dispose() => Disposals++;
    }

    /// <summary>A serde with both disposal interfaces, whose Dispose() nothing calls.</summary>
    private sealed class AsyncDisposableAndDisposable : Serde, IAsyncDisposable, IDisposable
    {
        public void Dispose() => Disposals += 10;

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return default;
        }
    }

    /// <summary>An IAsyncDisposable serde with a public Close() beside it.</summary>
    private sealed class ClosingAsyncDisposable : Serde, IAsyncDisposable
    {
        public void Close() => Disposals++;

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return default;
        }
    }

    /// <summary>An IDisposable serde with a public Close() beside it.</summary>
    private sealed class ClosingDisposable : Serde, IDisposable
    {
        public void Close() => Disposals++;

        public void Dispose() => Disposals++;
    }

    /// <summary>A serde with a public Configure but not the configure interfaces.</summary>
    private sealed class StrayConfigure : Serde
    {
        public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey) => Disposals++;
    }
}
