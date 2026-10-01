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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public <see cref="MetricName"/> value-type contract (M9/P2) — pure managed, no
/// consumer needed. Mirrors Java <c>MetricName.equals</c> / <c>hashCode</c>: value identity
/// over <c>(name, group, tags)</c> with <b>description excluded</b> and <b>tag order
/// irrelevant</b>, hand-implemented (not <see cref="IReadOnlyDictionary{TKey, TValue}"/>
/// reference equality), with a consistent <see cref="MetricName.GetHashCode"/>.
/// </summary>
public sealed class PublicMetricNameTests
{
    private static Dictionary<string, string> Tags(params (string Key, string Value)[] pairs)
    {
        Dictionary<string, string> map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach ((string key, string value) in pairs)
        {
            map[key] = value;
        }

        return map;
    }

    [Fact]
    public void Constructor_NullArgs_Throw()
    {
        Dictionary<string, string> tags = Tags();
        Assert.Throws<ArgumentNullException>(() => new MetricName(null!, "g", "d", tags));
        Assert.Throws<ArgumentNullException>(() => new MetricName("n", null!, "d", tags));
        // Java requires a non-null description too (Objects.requireNonNull).
        Assert.Throws<ArgumentNullException>(() => new MetricName("n", "g", null!, tags));
        Assert.Throws<ArgumentNullException>(() => new MetricName("n", "g", "d", null!));
    }

    [Fact]
    public void Properties_RoundTrip()
    {
        MetricName name = new MetricName("records-lag", "consumer-fetch", "the lag", Tags(("topic", "t"), ("partition", "0")));
        Assert.Equal("records-lag", name.Name);
        Assert.Equal("consumer-fetch", name.Group);
        Assert.Equal("the lag", name.Description);
        Assert.Equal(2, name.Tags.Count);
        Assert.Equal("t", name.Tags["topic"]);
        Assert.Equal("0", name.Tags["partition"]);
    }

    [Fact]
    public void Equality_SameNameGroupTags_AreEqual()
    {
        MetricName a = new MetricName("n", "g", "desc-a", Tags(("k", "v")));
        MetricName b = new MetricName("n", "g", "desc-a", Tags(("k", "v")));

        Assert.True(a.Equals(b));
        Assert.True(a == b);
        Assert.False(a != b);
        Assert.True(a.Equals((object)b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Equality_DifferentDescription_StillEqual()
    {
        // Description is EXCLUDED from equality and the hash (Java parity) — this is what lets
        // a caller re-key a metrics dictionary with a MetricName carrying a different description.
        MetricName a = new MetricName("n", "g", "description one", Tags(("k", "v")));
        MetricName b = new MetricName("n", "g", "a totally different description", Tags(("k", "v")));

        Assert.True(a.Equals(b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Equality_TagOrderIndependent()
    {
        // Tag enumeration order must not affect equality or the hash (Java Map.equals /
        // Map.hashCode are order-independent). Build the two tag sets in opposite insertion order.
        MetricName a = new MetricName("n", "g", "d", Tags(("client-id", "c1"), ("topic", "t"), ("partition", "3")));
        MetricName b = new MetricName("n", "g", "d", Tags(("partition", "3"), ("topic", "t"), ("client-id", "c1")));

        Assert.True(a.Equals(b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void Equality_DifferentName_NotEqual()
    {
        MetricName a = new MetricName("n1", "g", "d", Tags(("k", "v")));
        MetricName b = new MetricName("n2", "g", "d", Tags(("k", "v")));
        Assert.False(a.Equals(b));
        Assert.True(a != b);
    }

    [Fact]
    public void Equality_DifferentGroup_NotEqual()
    {
        MetricName a = new MetricName("n", "g1", "d", Tags(("k", "v")));
        MetricName b = new MetricName("n", "g2", "d", Tags(("k", "v")));
        Assert.False(a.Equals(b));
    }

    [Fact]
    public void Equality_DifferentTagValue_NotEqual()
    {
        MetricName a = new MetricName("n", "g", "d", Tags(("k", "v1")));
        MetricName b = new MetricName("n", "g", "d", Tags(("k", "v2")));
        Assert.False(a.Equals(b));
    }

    [Fact]
    public void Equality_DifferentTagCount_NotEqual()
    {
        MetricName a = new MetricName("n", "g", "d", Tags(("k", "v")));
        MetricName b = new MetricName("n", "g", "d", Tags(("k", "v"), ("k2", "v2")));
        Assert.False(a.Equals(b));
    }

    [Fact]
    public void Equality_IsOrdinal_CaseSensitive()
    {
        MetricName a = new MetricName("Name", "Group", "d", Tags(("K", "V")));
        MetricName lowerName = new MetricName("name", "Group", "d", Tags(("K", "V")));
        MetricName lowerTagValue = new MetricName("Name", "Group", "d", Tags(("K", "v")));
        Assert.False(a.Equals(lowerName));
        Assert.False(a.Equals(lowerTagValue));
    }

    [Fact]
    public void Equality_NullAndOtherType_False()
    {
        MetricName a = new MetricName("n", "g", "d", Tags(("k", "v")));
        Assert.False(a.Equals(null));
        Assert.False(a.Equals((object?)null));
        Assert.False(a.Equals("not a metric name"));
    }

    [Fact]
    public void Operators_NullHandling()
    {
        MetricName a = new MetricName("n", "g", "d", Tags());
        Assert.False(a == null);
        Assert.False(null == a);
        Assert.True(a != null);
        Assert.True((MetricName?)null == (MetricName?)null);
    }

    [Fact]
    public void UsableAsDictionaryKey_ByValueIdentity()
    {
        // The load-bearing property for Metrics(): value identity makes a re-constructed
        // MetricName (same name/group/tags, DIFFERENT description, DIFFERENT tag order) index
        // the same entry.
        Dictionary<MetricName, string> map = new Dictionary<MetricName, string>
        {
            [new MetricName("n", "g", "original", Tags(("a", "1"), ("b", "2")))] = "value",
        };

        MetricName lookup = new MetricName("n", "g", "different-desc", Tags(("b", "2"), ("a", "1")));
        Assert.True(map.ContainsKey(lookup));
        Assert.Equal("value", map[lookup]);
    }

    [Fact]
    public void NonAscii_StringFields_RoundTripAndEquate()
    {
        // Non-ASCII round-trip through a MetricName string field (name/group/tag) — the managed
        // side of the §B3 UTF-8 contract (the native marshalling of an owned string is exercised
        // by the ClientId non-ASCII round-trip, which shares Utf8Marshal.PtrToString).
        const string name = "café-Ω-日本語-😀";
        MetricName a = new MetricName(name, "gruppe-Ω", "描述", Tags(("clé-Ω", "valeur-😀")));
        MetricName b = new MetricName(name, "gruppe-Ω", "another", Tags(("clé-Ω", "valeur-😀")));

        Assert.Equal(name, a.Name);
        Assert.Equal("gruppe-Ω", a.Group);
        Assert.Equal("valeur-😀", a.Tags["clé-Ω"]);
        Assert.True(a.Equals(b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.Contains(name, a.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Tags_AreDefensivelyCopied()
    {
        // Mutating the caller's dictionary after construction must not change the MetricName.
        Dictionary<string, string> source = Tags(("k", "v"));
        MetricName name = new MetricName("n", "g", "d", source);
        source["k"] = "mutated";
        source["added"] = "x";

        Assert.Equal("v", name.Tags["k"]);
        Assert.Single(name.Tags);
    }
}
