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

using Confluent.Kafka.Common.Header;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary><see cref="Headers"/>'s public constructors, Java's <c>RecordHeaders</c> constructors.</summary>
public sealed class PublicHeadersConstructorTests
{
    [Fact]
    public void TheConstructors_CopyTheirInput()
    {
        Header header = new Header("key", new byte[] { (byte)'v' });
        List<Header> source = new List<Header> { header };
        Headers headers = new Headers(source);
        source.Add(new Header("key", new byte[] { (byte)'w' }));

        Assert.Same(header, Assert.Single(headers));

        List<Header> backing = new List<Header> { header };
        Headers original = Headers.Wrap(backing);
        Headers copy = new Headers(original);
        backing.Add(new Header("key", new byte[] { (byte)'w' }));

        Assert.NotSame(original, copy);
        Assert.Equal(2, original.Count);
        Assert.Same(header, Assert.Single(copy));
    }

    [Fact]
    public void ANullHeader_ThrowsWithJavasMessage()
    {
        AssertJavasNullHeader(Assert.Throws<ArgumentNullException>(() => new Headers(new Header[1])));
        AssertJavasNullHeader(Assert.Throws<ArgumentNullException>(() => new Headers(new List<Header> { null! })));
    }

    [Fact]
    public void NoArgumentOrNull_GivesEmptyHeaders()
    {
        Assert.Empty(new Headers());
        Assert.Empty(new Headers(null));
        Assert.Empty(new Headers((IEnumerable<Header>?)null));
    }

    private static void AssertJavasNullHeader(ArgumentNullException exception)
    {
        Assert.Equal("headers", exception.ParamName);
        Assert.Equal(new ArgumentNullException("headers", "Header cannot be null.").Message, exception.Message);
    }
}
