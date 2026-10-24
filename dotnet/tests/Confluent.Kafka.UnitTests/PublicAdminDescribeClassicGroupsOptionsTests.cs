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
using System.Linq;
using System.Reflection;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="DescribeClassicGroupsOptions"/> against Java's
/// <c>org.apache.kafka.clients.admin.DescribeClassicGroupsOptions</c>: its defaults, its shape, and that nothing is
/// deprecated, as in Java. The shape test repeats its <see cref="DescribeConsumerGroupsOptions"/> sibling's, so each
/// type matches its own Java class.
/// </summary>
public sealed class PublicAdminDescribeClassicGroupsOptionsTests
{
    [Fact]
    public void Defaults_MatchJavasFieldDefaults()
    {
        DescribeClassicGroupsOptions options = new DescribeClassicGroupsOptions();

        Assert.Null(options.TimeoutMs);
        Assert.False(options.IncludeAuthorizedOperations);
    }

    [Fact]
    public void PublicShape_IsTheTimeoutPlusJavasOneAccessor()
    {
        Assert.Equal(
            new[] { "IncludeAuthorizedOperations", "TimeoutMs" },
            typeof(DescribeClassicGroupsOptions)
                .GetProperties()
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        Assert.Empty(
            typeof(DescribeClassicGroupsOptions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        ConstructorInfo constructor = Assert.Single(typeof(DescribeClassicGroupsOptions).GetConstructors());
        Assert.Empty(constructor.GetParameters());

        PropertyInfo timeout = typeof(DescribeClassicGroupsOptions).GetProperty(
            nameof(DescribeClassicGroupsOptions.TimeoutMs))!;
        Assert.Equal(typeof(int?), timeout.PropertyType);
        Assert.NotNull(timeout.SetMethod);

        PropertyInfo include = typeof(DescribeClassicGroupsOptions).GetProperty(
            nameof(DescribeClassicGroupsOptions.IncludeAuthorizedOperations))!;
        Assert.Equal(typeof(bool), include.PropertyType);
        Assert.NotNull(include.SetMethod);
    }

    [Fact]
    public void IsADistinctTypeFromTheConsumerGroupOptions()
    {
        Assert.NotEqual(typeof(DescribeConsumerGroupsOptions), typeof(DescribeClassicGroupsOptions));
        Assert.False(
            typeof(DescribeClassicGroupsOptions).IsAssignableFrom(typeof(DescribeConsumerGroupsOptions)));
        Assert.False(
            typeof(DescribeConsumerGroupsOptions).IsAssignableFrom(typeof(DescribeClassicGroupsOptions)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IncludeAuthorizedOperations_RoundTrips(bool value)
    {
        DescribeClassicGroupsOptions options = new DescribeClassicGroupsOptions
        {
            IncludeAuthorizedOperations = value,
        };

        Assert.Equal(value, options.IncludeAuthorizedOperations);

        options.IncludeAuthorizedOperations = !value;
        Assert.Equal(!value, options.IncludeAuthorizedOperations);
    }

    [Fact]
    public void TimeoutMs_RoundTripsIncludingBackToNull()
    {
        DescribeClassicGroupsOptions options = new DescribeClassicGroupsOptions { TimeoutMs = 0 };
        Assert.Equal(0, options.TimeoutMs);

        options.TimeoutMs = 30_000;
        Assert.Equal(30_000, options.TimeoutMs);

        options.TimeoutMs = null;
        Assert.Null(options.TimeoutMs);
    }

    [Fact]
    public void NothingIsDeprecated_BecauseJavaDeprecatesNothingHere()
    {
        Assert.Null(typeof(DescribeClassicGroupsOptions).GetCustomAttribute<ObsoleteAttribute>());

        foreach (PropertyInfo property in typeof(DescribeClassicGroupsOptions).GetProperties())
        {
            Assert.Null(property.GetCustomAttribute<ObsoleteAttribute>());
        }
    }
}
