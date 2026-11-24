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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The real consumers built over the C mock for the decode tests (<see cref="DecodingConsumers"/>) own their
/// deserializers as the ones <c>Create</c> builds do: a refused strategy disposes only what the call built, and the
/// consumer's close disposes the given ones and the built ones alike.
/// </summary>
public sealed class DecodingConsumersTests
{
    private const string Refused = "Unknown auto offset reset strategy: EARLIEST";

    [Fact]
    public void ARefusedStrategy_DisposesTheBuiltDeserializer_AndNotTheGivenOne()
    {
        CountingDeserializer given = new CountingDeserializer();
        CountingDeserializer? built = null;
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["key.deserializer"] = new Func<CountingDeserializer>(() => built = new CountingDeserializer()),
        };

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => DecodingConsumers.Create<string, string>(configs, null, given, "EARLIEST"));

        Assert.Equal(Refused, e.Message);
        Assert.Equal(1, built!.Disposals);
        Assert.Equal(0, given.Disposals);
    }

    [Fact]
    public async Task ARefusedStrategy_OnTheAsyncConsumer_DisposesNeitherGivenDeserializer()
    {
        CountingDeserializer key = new CountingDeserializer();
        CountingDeserializer value = new CountingDeserializer();

        IllegalArgumentException e = await Assert.ThrowsAsync<IllegalArgumentException>(
            () => AsyncKafkaConsumer<string, string>.CreateOverMock(
                new Dictionary<string, object?>(), key.AsAsync(), value.AsAsync(), "EARLIEST"));

        Assert.Equal(Refused, e.Message);
        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    [Fact]
    public void TheClose_DisposesBothGivenDeserializers_Once()
    {
        CountingDeserializer key = new CountingDeserializer();
        CountingDeserializer value = new CountingDeserializer();
        KafkaConsumer<string, string> consumer = DecodingConsumers.Create<string, string>(key, value);

        TestTimeout.Run(consumer.Close, TimeSpan.FromSeconds(30));
        TestTimeout.Dispose(consumer);

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Fact]
    public async Task TheAsyncClose_DisposesBothGivenDeserializers_Once()
    {
        CountingDeserializer key = new CountingDeserializer();
        CountingDeserializer value = new CountingDeserializer();
        AsyncKafkaConsumer<string, string> consumer = DecodingConsumers.CreateAsync<string, string>(key.AsAsync(), value.AsAsync());

        await TestTimeout.Run(() => consumer.Close(), TimeSpan.FromSeconds(30));
        await TestTimeout.DisposeAsync(consumer);

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    private sealed class CountingDeserializer : IDeserializer<string>, IDisposable
    {
        internal int Disposals { get; private set; }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;

        public void Dispose() => Disposals++;
    }
}
