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

using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the public <see cref="SubscriptionPattern"/>, which stores its pattern as given and compares it ordinally;
/// a <see langword="null"/> pattern prints as <c>"null"</c>, equals another, and has no hash.
/// </summary>
public sealed class PublicSubscriptionPatternTests
{
    [Fact]
    public void Pattern_AndToString_ReturnTheGivenString()
    {
        SubscriptionPattern pattern = new SubscriptionPattern("t.*");

        Assert.Equal("t.*", pattern.Pattern());
        Assert.Equal("t.*", pattern.ToString());
    }

    [Fact]
    public void Equality_ComparesThePattern()
    {
        SubscriptionPattern pattern = new SubscriptionPattern("t.*");
        SubscriptionPattern same = new SubscriptionPattern(new string("t.*".ToCharArray()));

        Assert.NotSame(pattern.Pattern(), same.Pattern());
        Assert.True(pattern.Equals(same));
        Assert.True(same.Equals(pattern));
        Assert.Equal(pattern.GetHashCode(), same.GetHashCode());

        Assert.False(pattern.Equals(new SubscriptionPattern("u.*")));
        Assert.False(pattern.Equals(null));
        Assert.False(pattern.Equals("t.*"));
    }

    [Fact]
    public void Equality_IsOrdinal()
    {
        Assert.False(new SubscriptionPattern("T.*").Equals(new SubscriptionPattern("t.*")));
        Assert.False(new SubscriptionPattern("t.*").Equals(new SubscriptionPattern("T.*")));
    }

    [Fact]
    public void NullPattern_IsStored_PrintsAsNull_EqualsAnother_AndHasNoHash()
    {
        SubscriptionPattern pattern = new SubscriptionPattern(null!);

        Assert.Null(pattern.Pattern());
        Assert.Equal("null", pattern.ToString());
        Assert.True(pattern.Equals(new SubscriptionPattern(null!)));
        Assert.False(pattern.Equals(new SubscriptionPattern("null")));
        Assert.False(new SubscriptionPattern("null").Equals(pattern));

        ArgumentNullException e = Assert.Throws<ArgumentNullException>(() => pattern.GetHashCode());
        Assert.Equal("pattern", e.ParamName);
    }

    [Fact]
    public void EmptyPattern_IsStoredAsGiven()
    {
        SubscriptionPattern pattern = new SubscriptionPattern(string.Empty);

        Assert.Equal(string.Empty, pattern.Pattern());
        Assert.Equal(string.Empty, pattern.ToString());
        Assert.True(pattern.Equals(new SubscriptionPattern(string.Empty)));
        Assert.Equal(pattern.GetHashCode(), new SubscriptionPattern(string.Empty).GetHashCode());
    }
}
