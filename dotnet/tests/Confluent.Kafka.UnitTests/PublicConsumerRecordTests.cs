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
using System.Reflection;
using System.Text;

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="ConsumerRecord{TKey, TValue}"/>'s three constructors (Java's forms), their null checks with Java's
/// messages, <see cref="ConsumerRecord{TKey, TValue}.DeliveryCount()"/> and the string form of a given delivery count:
/// Java's <c>ConsumerRecordTest</c>.
/// </summary>
public sealed class PublicConsumerRecordTests
{
    [Fact]
    public void TestShortConstructor()
    {
        string topic = "topic";
        int partition = 0;
        long offset = 23;
        string key = "key";
        string value = "value";

        ConsumerRecord<string, string> record = new ConsumerRecord<string, string>(topic, partition, offset, key, value);
        Assert.Equal(topic, record.Topic());
        Assert.Equal(partition, record.Partition());
        Assert.Equal(offset, record.Offset());
        Assert.Equal(key, record.Key());
        Assert.Equal(value, record.Value());
        Assert.Equal(TimestampType.NO_TIMESTAMP_TYPE, record.TimestampType());
        Assert.Equal(ConsumerRecord<string, string>.NO_TIMESTAMP, record.Timestamp());
        Assert.Equal(ConsumerRecord<string, string>.NULL_SIZE, record.SerializedKeySize());
        Assert.Equal(ConsumerRecord<string, string>.NULL_SIZE, record.SerializedValueSize());
        Assert.Null(record.LeaderEpoch());
        Assert.Null(record.DeliveryCount());
        Assert.True(new Headers().Equals(record.Headers()));
    }

    [Fact]
    public void TestLongConstructor()
    {
        string topic = "topic";
        int partition = 0;
        long offset = 23;
        long timestamp = 23434217432432L;
        TimestampType timestampType = TimestampType.CREATE_TIME;
        string key = "key";
        string value = "value";
        int serializedKeySize = 100;
        int serializedValueSize = 1142;

        Headers headers = new Headers();
        headers.Add(new Header("header key", Encoding.UTF8.GetBytes("header value")));
        ConsumerRecord<string, string> record = new ConsumerRecord<string, string>(
            topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize, key, value,
            headers, null);
        Assert.Equal(topic, record.Topic());
        Assert.Equal(partition, record.Partition());
        Assert.Equal(offset, record.Offset());
        Assert.Equal(key, record.Key());
        Assert.Equal(value, record.Value());
        Assert.Equal(timestampType, record.TimestampType());
        Assert.Equal(timestamp, record.Timestamp());
        Assert.Equal(serializedKeySize, record.SerializedKeySize());
        Assert.Equal(serializedValueSize, record.SerializedValueSize());
        Assert.Null(record.LeaderEpoch());
        Assert.Null(record.DeliveryCount());
        Assert.True(headers.Equals(record.Headers()));

        int? leaderEpoch = 10;
        short? deliveryCount = 1;
        record = new ConsumerRecord<string, string>(
            topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize, key, value,
            headers, leaderEpoch, deliveryCount);
        Assert.Equal(topic, record.Topic());
        Assert.Equal(partition, record.Partition());
        Assert.Equal(offset, record.Offset());
        Assert.Equal(key, record.Key());
        Assert.Equal(value, record.Value());
        Assert.Equal(timestampType, record.TimestampType());
        Assert.Equal(timestamp, record.Timestamp());
        Assert.Equal(serializedKeySize, record.SerializedKeySize());
        Assert.Equal(serializedValueSize, record.SerializedValueSize());
        Assert.Equal(leaderEpoch, record.LeaderEpoch());
        Assert.Equal(deliveryCount, record.DeliveryCount());
        Assert.True(headers.Equals(record.Headers()));
    }

    [Fact]
    public void Ctor_KeepsTheGivenHeadersObject()
    {
        Headers headers = new Headers();

        ConsumerRecord<string, string> eleven = new ConsumerRecord<string, string>(
            "t", 0, 0L, 0L, TimestampType.CREATE_TIME, 1, 1, "k", "v", headers, null);
        ConsumerRecord<string, string> twelve = new ConsumerRecord<string, string>(
            "t", 0, 0L, 0L, TimestampType.CREATE_TIME, 1, 1, "k", "v", headers, null, null);

        Assert.Same(headers, eleven.Headers());
        Assert.Same(headers, twelve.Headers());
    }

    [Fact]
    public void Ctor_NullTopic_ThrowsIllegalArgumentWithJavasMessage()
    {
        IllegalArgumentException shortForm = Assert.Throws<IllegalArgumentException>(
            () => new ConsumerRecord<string, string>(null!, 0, 0L, "k", "v"));
        Assert.Equal("Topic cannot be null", shortForm.Message);

        IllegalArgumentException eleven = Assert.Throws<IllegalArgumentException>(
            () => new ConsumerRecord<string, string>(
                null!, 0, 0L, 0L, TimestampType.CREATE_TIME, 1, 1, "k", "v", new Headers(), null));
        Assert.Equal("Topic cannot be null", eleven.Message);

        IllegalArgumentException twelve = Assert.Throws<IllegalArgumentException>(
            () => new ConsumerRecord<string, string>(
                null!, 0, 0L, 0L, TimestampType.CREATE_TIME, 1, 1, "k", "v", new Headers(), null, null));
        Assert.Equal("Topic cannot be null", twelve.Message);
    }

    [Fact]
    public void Ctor_NullHeaders_ThrowsIllegalArgumentWithJavasMessage()
    {
        IllegalArgumentException eleven = Assert.Throws<IllegalArgumentException>(
            () => new ConsumerRecord<string, string>(
                "t", 0, 0L, 0L, TimestampType.CREATE_TIME, 1, 1, "k", "v", null!, null));
        Assert.Equal("Headers cannot be null", eleven.Message);

        IllegalArgumentException twelve = Assert.Throws<IllegalArgumentException>(
            () => new ConsumerRecord<string, string>(
                "t", 0, 0L, 0L, TimestampType.CREATE_TIME, 1, 1, "k", "v", null!, null, (short)1));
        Assert.Equal("Headers cannot be null", twelve.Message);
    }

    [Fact]
    public void Ctor_NullTopicAndHeaders_ReportsTheTopic()
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new ConsumerRecord<string, string>(
                null!, 0, 0L, 0L, TimestampType.CREATE_TIME, 1, 1, "k", "v", null!, null, null));
        Assert.Equal("Topic cannot be null", ex.Message);
    }

    [Fact]
    public void DeliveryCount_IsTheGivenCount()
    {
        ConsumerRecord<string, string> record = new ConsumerRecord<string, string>(
            "orders", 3, 42L, 1700L, TimestampType.CREATE_TIME, 1, 1, "k", "v", new Headers(), 7, (short)5);

        Assert.Equal((short)5, record.DeliveryCount());
        Assert.Equal(7, record.LeaderEpoch());
    }

    [Fact]
    public void ToString_PrintsTheGivenDeliveryCount()
    {
        ConsumerRecord<string, string> record = new ConsumerRecord<string, string>(
            "orders", 3, 42L, 1700L, TimestampType.CREATE_TIME, 1, 1, "k", "v", new Headers(), 7, (short)5);

        Assert.Equal(
            "ConsumerRecord(topic = orders, partition = 3, leaderEpoch = 7, offset = 42, "
            + "CreateTime = 1700, deliveryCount = 5, serialized key size = 1, "
            + "serialized value size = 1, headers = RecordHeaders(headers = [], isReadOnly = false), "
            + "key = k, value = v)",
            record.ToString());
    }

    [Fact]
    public void Constructors_AreJavasThreeForms()
    {
        Type type = typeof(ConsumerRecord<,>);
        Type[] arguments = type.GetGenericArguments();
        (Type Type, string Name) topic = (typeof(string), "topic");
        (Type Type, string Name) partition = (typeof(int), "partition");
        (Type Type, string Name) offset = (typeof(long), "offset");
        (Type Type, string Name) timestamp = (typeof(long), "timestamp");
        (Type Type, string Name) timestampType = (typeof(TimestampType), "timestampType");
        (Type Type, string Name) serializedKeySize = (typeof(int), "serializedKeySize");
        (Type Type, string Name) serializedValueSize = (typeof(int), "serializedValueSize");
        (Type Type, string Name) key = (arguments[0], "key");
        (Type Type, string Name) value = (arguments[1], "value");
        (Type Type, string Name) headers = (typeof(Headers), "headers");
        (Type Type, string Name) leaderEpoch = (typeof(int?), "leaderEpoch");
        (Type Type, string Name) deliveryCount = (typeof(short?), "deliveryCount");
        List<(Type Type, string Name)[]> expected = new List<(Type Type, string Name)[]>
        {
            new[] { topic, partition, offset, key, value },
            new[]
            {
                topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize, key, value,
                headers, leaderEpoch,
            },
            new[]
            {
                topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize, key, value,
                headers, leaderEpoch, deliveryCount,
            },
        };

        ConstructorInfo[] constructors = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
        Array.Sort(constructors, (left, right) => left.MetadataToken.CompareTo(right.MetadataToken));

        Assert.Equal(expected.Count, constructors.Length);
        Assert.Empty(type.GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));
        for (int i = 0; i < expected.Count; i++)
        {
            ParameterInfo[] parameters = constructors[i].GetParameters();
            Assert.Equal(
                expected[i],
                Array.ConvertAll(parameters, parameter => (parameter.ParameterType, parameter.Name!)));
            Assert.All(parameters, parameter => Assert.False(parameter.IsOptional, parameter.Name));
            Assert.All(parameters, parameter => Assert.False(parameter.HasDefaultValue, parameter.Name));
        }
    }
}
