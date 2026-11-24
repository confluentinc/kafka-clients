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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A topic holding a NUL character makes <c>PartitionsFor</c> throw <see cref="ArgumentException"/> at the call on
/// <c>KafkaProducer</c> and <c>AsyncKafkaProducer</c>, as <c>Send</c> refuses such a topic: the C ABI would cut it at
/// the NUL and answer for another topic. The mocks answer as Java's <c>MockProducer</c>: an empty list, or the
/// injected exception.
/// </summary>
public sealed class PublicProducerPartitionsForStringTests
{
    private const string TopicMessage =
        "A topic must not contain a NUL character: such a string cannot be passed to the native client unchanged.";

    private const string NulTopic = "t\0u";

    [Fact]
    public void KafkaProducer_PartitionsFor_ATopicWithANul_ThrowsArgumentException()
    {
        using KafkaProducer<string, string> producer =
            KafkaProducer<string, string>.Create(Unreachable(), Serdes.String().Serializer(), Serdes.String().Serializer());

        AssertRejected(Assert.Throws<ArgumentException>(() => producer.PartitionsFor(NulTopic)));
    }

    [Fact]
    public async Task AsyncKafkaProducer_PartitionsFor_ATopicWithANul_ThrowsArgumentExceptionAtTheCall()
    {
        await using AsyncKafkaProducer<string, string> producer =
            await AsyncKafkaProducer<string, string>.Create(
                Unreachable(), new AsyncStringSerializer(), new AsyncStringSerializer());

        AssertRejected(Assert.Throws<ArgumentException>(() => { _ = producer.PartitionsFor(NulTopic); }));
    }

    [Fact]
    public void MockProducer_PartitionsFor_ATopicWithANul_ReturnsAnEmptyList()
    {
        using MockProducer<string, string> producer =
            new MockProducer<string, string>(Serdes.String().Serializer(), Serdes.String().Serializer());

        Assert.Empty(producer.PartitionsFor(NulTopic));
    }

    [Fact]
    public async Task AsyncMockProducer_PartitionsFor_ATopicWithANul_ReturnsAnEmptyList()
    {
        await using AsyncMockProducer<string, string> producer =
            new AsyncMockProducer<string, string>(new AsyncStringSerializer(), new AsyncStringSerializer());

        Assert.Empty(await producer.PartitionsFor(NulTopic));
    }

    [Fact]
    public void MockProducer_PartitionsFor_ATopicWithANul_ThrowsTheInjectedException()
    {
        using MockProducer<string, string> producer =
            new MockProducer<string, string>(Serdes.String().Serializer(), Serdes.String().Serializer());
        KafkaException injected = new KafkaException("partitionsFor");
        producer.SetPartitionsForException(injected);

        Assert.Same(injected, Assert.Throws<KafkaException>(() => producer.PartitionsFor(NulTopic)));
    }

    [Fact]
    public async Task AsyncMockProducer_PartitionsFor_ATopicWithANul_ThrowsTheInjectedException()
    {
        await using AsyncMockProducer<string, string> producer =
            new AsyncMockProducer<string, string>(new AsyncStringSerializer(), new AsyncStringSerializer());
        KafkaException injected = new KafkaException("partitionsFor");
        producer.SetPartitionsForException(injected);

        Assert.Same(injected, await Assert.ThrowsAsync<KafkaException>(() => producer.PartitionsFor(NulTopic)));
    }

    private static void AssertRejected(ArgumentException rejected)
    {
        Assert.Equal("topic", rejected.ParamName);
        Assert.Equal(new ArgumentException(TopicMessage, "topic").Message, rejected.Message);
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "500",
        };
}
