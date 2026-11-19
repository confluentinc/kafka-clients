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
using System.Text;

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>Deserialize(topic, headers, data)</c> on any <see cref="IDeserializer{T}"/> has one extension, over a span, so a
/// call with an array or an array segment has one candidate and compiles on every language version; a caller that
/// holds a memory passes its span.
/// </summary>
public sealed class DeserializerHeadersExtensionArgumentTests
{
    private const string Topic = "testTopic";

    private static readonly byte[] s_value = Encoding.UTF8.GetBytes("value");

    private static readonly byte[] s_batch = Encoding.UTF8.GetBytes("--value-");

    [Fact]
    public void AnArray_IsDecoded()
    {
        IDeserializer<string> deserializer = new StringDeserializer();

        Assert.Equal("value", deserializer.Deserialize(Topic, new Headers(), s_value));
        Assert.Equal("value", new StringDeserializer().Deserialize(Topic, new Headers(), s_value));
    }

    [Fact]
    public void AnArraySegment_IsDecoded()
    {
        IDeserializer<string> deserializer = new StringDeserializer();
        ArraySegment<byte> segment = new ArraySegment<byte>(s_batch, 2, 5);

        Assert.Equal("value", deserializer.Deserialize(Topic, new Headers(), segment));
        Assert.Equal("value", new StringDeserializer().Deserialize(Topic, new Headers(), segment));
    }

    [Fact]
    public void AMemorysSpan_ReachesTheHeadersForm_AlsoOfADeserializerWithTheMemoryForm()
    {
        MemoryDeserializer deserializer = new MemoryDeserializer();
        IDeserializer<string> asInterface = deserializer;
        ReadOnlyMemory<byte> data = new ReadOnlyMemory<byte>(s_batch, 2, 5);

        Assert.Equal("span:value", asInterface.Deserialize(Topic, new Headers(), data.Span));
        Assert.Equal(0, deserializer.MemoryCalls);
    }

    /// <summary>A deserializer with the memory form, recording whether that form is called.</summary>
    private sealed class MemoryDeserializer : IDeserializerWithReadOnlyMemory<string>
    {
        internal int MemoryCalls { get; private set; }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data) =>
            "span:" + Encoding.UTF8.GetString(data.ToArray());

        public string? Deserialize(string topic, Headers headers, ReadOnlyMemory<byte> data)
        {
            MemoryCalls++;
            return "memory:" + Encoding.UTF8.GetString(data.ToArray());
        }
    }
}
