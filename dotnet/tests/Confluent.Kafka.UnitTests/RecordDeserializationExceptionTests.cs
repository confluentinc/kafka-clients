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
using System.Linq;
using System.Reflection;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Record;
using Xunit;

using Origin = Confluent.Kafka.Common.Errors.RecordDeserializationException.DeserializationExceptionOrigin;

namespace Confluent.Kafka.UnitTests;

/// <summary><see cref="RecordDeserializationException"/>'s constructor, accessors and shape.</summary>
public sealed class RecordDeserializationExceptionTests
{
    private static readonly TopicPartition s_tp = new TopicPartition("topic", 3);

    [Fact]
    public void TheConstructor_KeepsEveryValue()
    {
        InvalidOperationException cause = new InvalidOperationException("bad");
        RecordDeserializationException e = new RecordDeserializationException(
            Origin.Value,
            s_tp,
            offset: 7,
            timestamp: 10,
            TimestampType.CreateTime,
            new byte[] { (byte)'k' },
            new byte[] { (byte)'v' },
            new Headers(new[] { new Header("h", new byte[] { (byte)'1' }) }),
            "m",
            cause);

        Assert.Equal(Origin.Value, e.Origin);
        Assert.Equal(s_tp, e.TopicPartition);
        Assert.Equal(7L, e.Offset);
        Assert.Equal(10L, e.Timestamp);
        Assert.Equal(TimestampType.CreateTime, e.TimestampType);
        Assert.Equal(new byte[] { (byte)'k' }, e.KeyBuffer);
        Assert.Equal(new byte[] { (byte)'v' }, e.ValueBuffer);
        Header header = Assert.Single(e.Headers!);
        Assert.Equal("h", header.Key);
        Assert.Equal(new byte[] { (byte)'1' }, header.Value);
        Assert.Equal("m", e.Message);
        Assert.Same(cause, e.InnerException);
        Assert.Equal(0, e.Code);
    }

    [Fact]
    public void TheConstructor_TakesJavasNulls()
    {
        RecordDeserializationException e = new RecordDeserializationException(
            origin: null,
            s_tp,
            offset: 2,
            timestamp: -1,
            TimestampType.NoTimestampType,
            keyBuffer: null,
            valueBuffer: null,
            headers: null,
            "m",
            cause: null);

        Assert.Null(e.Origin);
        Assert.Null(e.KeyBuffer);
        Assert.Null(e.ValueBuffer);
        Assert.Null(e.Headers);
        Assert.Null(e.InnerException);
    }

    [Fact]
    public void ARecordWithoutAKey_KeepsTheValueBuffer()
    {
        RecordDeserializationException e = new RecordDeserializationException(
            Origin.Key, s_tp, 3, 1, TimestampType.CreateTime, null, new byte[] { (byte)'v' }, Headers.Empty, "m", null);

        Assert.Null(e.KeyBuffer);
        Assert.Equal(new byte[] { (byte)'v' }, e.ValueBuffer);
    }

    [Fact]
    public void Headers_KeepsTheHeadersInstance()
    {
        Headers headers = new Headers(new[] { new Header("a", null) });
        RecordDeserializationException e = new RecordDeserializationException(
            Origin.Key, s_tp, 0, 0, TimestampType.CreateTime, null, null, headers, "m", null);

        Assert.Same(headers, e.Headers);
    }

    [Fact]
    public void ItIsASerializationException()
    {
        RecordDeserializationException e = new RecordDeserializationException(
            Origin.Key, s_tp, 0, 0, TimestampType.CreateTime, null, null, null, "m", null);

        Assert.IsAssignableFrom<SerializationException>(e);
        Assert.IsAssignableFrom<KafkaException>(e);
        Assert.False(e.IsRetriable);
    }

    [Fact]
    public void TheOrigin_IsNestedWithJavasMembersInOrder()
    {
        Assert.Equal(typeof(RecordDeserializationException), typeof(Origin).DeclaringType);
        Assert.Equal(new[] { "Key", "Value" }, Enum.GetNames(typeof(Origin)));
        Assert.Equal(new[] { 0, 1 }, Enum.GetValues(typeof(Origin)).Cast<int>().ToArray());
    }

    [Fact]
    public void ItHasOnlyTheFullConstructor()
    {
        ConstructorInfo constructor = Assert.Single(typeof(RecordDeserializationException).GetConstructors());
        Assert.Equal(
            new[]
            {
                "origin", "partition", "offset", "timestamp", "timestampType", "keyBuffer", "valueBuffer", "headers",
                "message", "cause",
            },
            constructor.GetParameters().Select(p => p.Name).ToArray());
    }
}
