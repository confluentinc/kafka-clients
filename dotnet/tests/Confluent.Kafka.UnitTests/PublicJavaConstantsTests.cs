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

using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>Java's public constants on the consumer record and the producers, with Java's values.</summary>
public sealed class PublicJavaConstantsTests
{
    [Fact]
    public void ConsumerRecord_NullSize_IsJavasMinusOne()
    {
        Assert.Equal(-1, ConsumerRecord<string, string>.NULL_SIZE);
    }

    [Fact]
    public void Producers_CarryJavasThreadPrefixAndMetricGroup()
    {
        Assert.Equal("kafka-producer-network-thread", KafkaProducer<string, string>.NETWORK_THREAD_PREFIX);
        Assert.Equal("producer-metrics", KafkaProducer<string, string>.PRODUCER_METRIC_GROUP_NAME);
        Assert.Equal("kafka-producer-network-thread", AsyncKafkaProducer<string, string>.NETWORK_THREAD_PREFIX);
        Assert.Equal("producer-metrics", AsyncKafkaProducer<string, string>.PRODUCER_METRIC_GROUP_NAME);
    }
}
