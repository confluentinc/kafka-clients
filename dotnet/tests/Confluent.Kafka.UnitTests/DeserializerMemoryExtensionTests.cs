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
using System.Runtime.InteropServices;
using System.Text;

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>Deserialize(topic, headers, memory)</c> on any <see cref="IDeserializer{T}"/>, Java's default
/// <c>deserialize(String, Headers, ByteBuffer)</c>: the memory form of a deserializer that has one, and otherwise the
/// headers form over the memory's span, which is in turn the plain form unless the deserializer has a headers form.
/// </summary>
public sealed class DeserializerMemoryExtensionTests
{
    private const string Topic = "testTopic";

    private static readonly byte[] s_batch = Encoding.UTF8.GetBytes("--value-");

    private static ReadOnlyMemory<byte> Data => new ReadOnlyMemory<byte>(s_batch, 2, 5);

    [Fact]
    public void ADeserializerWithTheMemoryForm_IsGivenTheMemoryItself()
    {
        MemoryDeserializer deserializer = new MemoryDeserializer();
        IDeserializer<string> asInterface = deserializer;
        Headers headers = new Headers();

        string? value = asInterface.Deserialize(Topic, headers, Data);

        Assert.Equal("memory:value", value);
        Assert.Equal(1, deserializer.MemoryCalls);
        Assert.Equal(0, deserializer.SpanCalls);
        Assert.Same(headers, deserializer.Headers);
        Assert.True(deserializer.Memory.Equals(Data));
    }

    [Fact]
    public void TheReadOnlyMemoryDeserializer_ReturnsTheMemoryItself_ThroughTheExtension()
    {
        IDeserializer<ReadOnlyMemory<byte>?> deserializer = new ReadOnlyMemoryDeserializer();

        ReadOnlyMemory<byte> returned = deserializer.Deserialize(Topic, new Headers(), Data)!.Value;

        Assert.True(returned.Equals(Data));
        Assert.True(MemoryMarshal.TryGetArray(returned, out ArraySegment<byte> segment));
        Assert.Same(s_batch, segment.Array);
    }

    [Fact]
    public void ADeserializerWithTheHeadersFormOnly_IsGivenTheMemorysSpan_AsJavasDefault()
    {
        HeadersDeserializer deserializer = new HeadersDeserializer();
        IDeserializer<string> asInterface = deserializer;
        Headers headers = new Headers();

        string? value = asInterface.Deserialize(Topic, headers, Data);

        Assert.Equal("headers:value", value);
        Assert.Equal(1, deserializer.HeadersCalls);
        Assert.Equal(0, deserializer.PlainCalls);
        Assert.Same(headers, deserializer.Headers);
    }

    [Fact]
    public void APlainDeserializer_IsGivenTheMemorysSpan_AsJavasDefaults()
    {
        IDeserializer<string> deserializer = new StringDeserializer();

        Assert.Equal("value", deserializer.Deserialize(Topic, new Headers(), Data));
        Assert.Equal(string.Empty, deserializer.Deserialize(Topic, new Headers(), ReadOnlyMemory<byte>.Empty));
    }

    [Fact]
    public void ANullDeserializer_Throws()
    {
        ArgumentNullException e = Assert.Throws<ArgumentNullException>(
            () => ((IDeserializer<string>)null!).Deserialize(Topic, new Headers(), Data));

        Assert.Equal("deserializer", e.ParamName);
    }

    /// <summary>A deserializer with the memory form, recording which form is called and what it is given.</summary>
    private sealed class MemoryDeserializer : IDeserializerWithReadOnlyMemory<string>
    {
        internal int SpanCalls { get; private set; }

        internal int MemoryCalls { get; private set; }

        internal Headers? Headers { get; private set; }

        internal ReadOnlyMemory<byte> Memory { get; private set; }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            SpanCalls++;
            return "span:" + Text(data);
        }

        public string? Deserialize(string topic, Headers headers, ReadOnlyMemory<byte> data)
        {
            MemoryCalls++;
            Headers = headers;
            Memory = data;
            return "memory:" + Text(data.Span);
        }
    }

    /// <summary>A deserializer with the headers form, recording which form is called.</summary>
    private sealed class HeadersDeserializer : IDeserializerWithHeaders<string>
    {
        internal int PlainCalls { get; private set; }

        internal int HeadersCalls { get; private set; }

        internal Headers? Headers { get; private set; }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            PlainCalls++;
            return "plain:" + Text(data);
        }

        public string? Deserialize(string topic, Headers headers, ReadOnlySpan<byte> data)
        {
            HeadersCalls++;
            Headers = headers;
            return "headers:" + Text(data);
        }
    }

    private static string Text(ReadOnlySpan<byte> data) => Encoding.UTF8.GetString(data.ToArray());
}
