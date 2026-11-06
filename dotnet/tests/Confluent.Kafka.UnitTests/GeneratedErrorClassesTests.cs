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
using System.Linq;
using System.Reflection;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Common.Requests;
using Confluent.Kafka.Internal;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the error classes the generator writes from the Java sources: a message Java builds from its arguments
/// prints them as Java's string concatenation does, and the registry's two tables agree.
/// </summary>
public sealed class GeneratedErrorClassesTests
{
    [Fact]
    public void ForGroupId_PrintsANullGroupAsJavaDoes()
    {
        GroupAuthorizationException error = GroupAuthorizationException.ForGroupId(null);

        Assert.Equal("Not authorized to access group: null", error.Message);
        Assert.Null(error.GroupId());
    }

    [Fact]
    public void ConfigException_PrintsANullNameAsJavaDoes()
    {
        var error = new ConfigException(null, "v", "msg");

        Assert.Equal("Invalid value v for configuration null: msg", error.Message);
    }

    [Fact]
    public void ConfigException_WithoutAMessage_EndsAtTheName()
    {
        var error = new ConfigException("a.b", 5);

        Assert.Equal("Invalid value 5 for configuration a.b", error.Message);
    }

    [Fact]
    public void InvalidTopicException_PrintsANullElementAsJavaDoes()
    {
        var topics = new List<string> { "a", null! };

        var error = new InvalidTopicException(topics);

        Assert.Equal("Invalid topics: [a, null]", error.Message);
        Assert.Same(topics, error.InvalidTopics());
    }

    [Theory]
    [InlineData(typeof(DuplicateResourceException), "Resource")]
    [InlineData(typeof(GroupAuthorizationException), "GroupId")]
    [InlineData(typeof(InvalidTopicException), "InvalidTopics")]
    [InlineData(
        typeof(RecordDeserializationException),
        "Headers,KeyBuffer,Offset,Origin,Timestamp,TimestampType,TopicPartition,ValueBuffer")]
    [InlineData(typeof(RecordTooLargeException), "RecordTooLargePartitions")]
    [InlineData(typeof(ResourceNotFoundException), "Resource")]
    [InlineData(typeof(ThrottlingQuotaExceededException), "ThrottleTimeMs")]
    [InlineData(typeof(TopicAuthorizationException), "UnauthorizedTopics")]
    [InlineData(typeof(QuotaViolationException), "Bound,Metric,Value")]
    [InlineData(typeof(CorrelationIdMismatchException), "RequestCorrelationId,ResponseCorrelationId")]
    [InlineData(typeof(Consumer.InvalidOffsetException), "Partitions")]
    [InlineData(typeof(Consumer.LogTruncationException), "DivergentOffsets")]
    [InlineData(typeof(Consumer.NoOffsetForPartitionException), "Partitions")]
    [InlineData(typeof(Consumer.OffsetOutOfRangeException), "OffsetOutOfRangePartitions,Partitions")]
    public void PayloadGetters_AreJavasMethods_AndNotProperties(Type type, string getters)
    {
        foreach (string name in getters.Split(','))
        {
            MethodInfo? getter = type.GetMethod(name, Type.EmptyTypes);
            Assert.True(getter is { IsSpecialName: false, IsPublic: true }, $"{type.Name}.{name}() is not a method.");
        }

        Assert.Empty(type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly));
    }

    [Fact]
    public void NoPublicErrorClass_DeclaresAPublicProperty()
    {
        IEnumerable<string> withProperties = typeof(KafkaException).Assembly.GetExportedTypes()
            .Where(type => typeof(Exception).IsAssignableFrom(type))
            .Where(type => type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static
                | BindingFlags.DeclaredOnly).Length > 0)
            .Select(type => type.FullName!);

        Assert.Empty(withProperties);
    }

    [Fact]
    public void EveryFactory_BuildsTheClassOfItsId()
    {
        foreach (KeyValuePair<int, Func<string?, Exception?, IntPtr, Exception>> entry in ErrorClasses.Factories)
        {
            Exception error = entry.Value("m", null, IntPtr.Zero);

            Assert.True(
                ErrorClasses.Ids.TryGetValue(error.GetType(), out int id) && id == entry.Key,
                $"The factory of {entry.Key} builds {error.GetType().FullName}, whose id is not {entry.Key}.");
            if (error is KafkaException kafka)
            {
                Assert.Equal(entry.Key, kafka.FfiId);
            }
        }
    }

    [Fact]
    public void TheFactories_SkipTheBaseAndTheHandWrittenRecordError()
    {
        Assert.False(ErrorClasses.Factories.ContainsKey(-1));
        Assert.False(ErrorClasses.Factories.ContainsKey(-27));
        Assert.Equal(-27, ErrorClasses.Ids[typeof(RecordDeserializationException)]);
        Assert.DoesNotContain(ErrorClasses.Ids.Keys, type => type.IsAbstract);
    }
}
