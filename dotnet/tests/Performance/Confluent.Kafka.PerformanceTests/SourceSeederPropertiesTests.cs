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

using System.Collections.Generic;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>Checks that the seed producer's <c>.properties</c> text escapes values as <c>Properties.load</c> reads them.</summary>
public sealed class SourceSeederPropertiesTests
{
    [Fact]
    public void PropertiesText_KeepsAPlainValue()
    {
        string text = SourceSeeder.PropertiesText(new[] { Pair("bootstrap.servers", "host:9092,other:9092") });

        Assert.Equal("bootstrap.servers=host:9092,other:9092\n", text);
    }

    [Fact]
    public void PropertiesText_EscapesBackslashesAndLineBreaks()
    {
        string jaas = "PlainLoginModule required\n  username=\"a\\b\"\tpassword=\"p=w:d#!\";\r";

        string text = SourceSeeder.PropertiesText(new[] { Pair("sasl.jaas.config", jaas) });

        Assert.Equal(
            "sasl.jaas.config=PlainLoginModule required\\n  username=\"a\\\\b\"\\tpassword=\"p=w:d#!\";\\r\n",
            text);
    }

    [Fact]
    public void PropertiesText_EscapesALeadingSpaceOnly()
    {
        string text = SourceSeeder.PropertiesText(new[] { Pair("k", " a b") });

        Assert.Equal("k=\\ a b\n", text);
    }

    [Fact]
    public void PropertiesText_EscapesNonAsciiAndControlCharacters()
    {
        string text = SourceSeeder.PropertiesText(new[] { Pair("k", "pä\u0001ß\u20ac") });

        Assert.Equal("k=p\\u00E4\\u0001\\u00DF\\u20AC\n", text);
    }

    private static KeyValuePair<string, string> Pair(string key, string value) => new(key, value);
}
