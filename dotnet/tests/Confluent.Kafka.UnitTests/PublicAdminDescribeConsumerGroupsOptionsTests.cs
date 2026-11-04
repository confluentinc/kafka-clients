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
/// <see cref="DescribeConsumerGroupsOptions"/> against Java's
/// <c>org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions</c>: its defaults, its shape, and that nothing is
/// deprecated, as in Java.
/// </summary>
public sealed class PublicAdminDescribeConsumerGroupsOptionsTests
{
    [Fact]
    public void Defaults_MatchJavasFieldDefaults()
    {
        DescribeConsumerGroupsOptions options = new DescribeConsumerGroupsOptions();

        Assert.Null(options.TimeoutMs);
        Assert.False(options.IncludeAuthorizedOperations);
    }

    [Fact]
    public void PublicShape_IsTheTimeoutPlusJavasOneAccessor()
    {
        Assert.Equal(
            new[] { "IncludeAuthorizedOperations", "TimeoutMs" },
            typeof(DescribeConsumerGroupsOptions)
                .GetProperties()
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        Assert.Empty(
            typeof(DescribeConsumerGroupsOptions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        ConstructorInfo constructor = Assert.Single(typeof(DescribeConsumerGroupsOptions).GetConstructors());
        Assert.Empty(constructor.GetParameters());

        PropertyInfo timeout = typeof(DescribeConsumerGroupsOptions).GetProperty(
            nameof(DescribeConsumerGroupsOptions.TimeoutMs))!;
        Assert.Equal(typeof(int?), timeout.PropertyType);
        Assert.NotNull(timeout.SetMethod);

        PropertyInfo include = typeof(DescribeConsumerGroupsOptions).GetProperty(
            nameof(DescribeConsumerGroupsOptions.IncludeAuthorizedOperations))!;
        Assert.Equal(typeof(bool), include.PropertyType);
        Assert.NotNull(include.SetMethod);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void IncludeAuthorizedOperations_RoundTrips(bool value)
    {
        DescribeConsumerGroupsOptions options = new DescribeConsumerGroupsOptions
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
        DescribeConsumerGroupsOptions options = new DescribeConsumerGroupsOptions { TimeoutMs = 0 };
        Assert.Equal(0, options.TimeoutMs);

        options.TimeoutMs = 30_000;
        Assert.Equal(30_000, options.TimeoutMs);

        options.TimeoutMs = null;
        Assert.Null(options.TimeoutMs);
    }

    [Fact]
    public void NothingIsDeprecated_BecauseJavaDeprecatesNothingHere()
    {
        Assert.Null(typeof(DescribeConsumerGroupsOptions).GetCustomAttribute<ObsoleteAttribute>());

        foreach (PropertyInfo property in typeof(DescribeConsumerGroupsOptions).GetProperties())
        {
            Assert.Null(property.GetCustomAttribute<ObsoleteAttribute>());
        }
    }
}
