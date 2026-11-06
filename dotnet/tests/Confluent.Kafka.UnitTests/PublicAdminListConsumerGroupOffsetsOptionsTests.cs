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

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="ListConsumerGroupOffsetsOptions"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions</c>: its defaults, its shape, that it carries no
/// partition selection (that is <see cref="ListConsumerGroupOffsetsSpec"/>'s job), and that nothing is deprecated, as
/// in Java.
/// </summary>
public sealed class PublicAdminListConsumerGroupOffsetsOptionsTests
{
    [Fact]
    public void Defaults_MatchJavasFieldDefaults()
    {
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions();

        Assert.Null(options.TimeoutMs());
        Assert.False(options.RequireStable());
    }

    [Fact]
    public void PublicShape_IsTheTimeoutPlusJavasOneAccessor()
    {
        Assert.Empty(typeof(ListConsumerGroupOffsetsOptions).GetProperties());
        Assert.Equal(new[] { "RequireStable", "TimeoutMs" }, Getters.NamesOf(typeof(ListConsumerGroupOffsetsOptions)));
        Assert.Equal(new[] { "RequireStable", "TimeoutMs" }, SetterMethods.NamesOf(typeof(ListConsumerGroupOffsetsOptions)));

        Assert.Empty(
            typeof(ListConsumerGroupOffsetsOptions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        ConstructorInfo constructor = Assert.Single(typeof(ListConsumerGroupOffsetsOptions).GetConstructors());
        Assert.Empty(constructor.GetParameters());

        Assert.Equal(typeof(int?), Getters.Get(typeof(ListConsumerGroupOffsetsOptions), nameof(ListConsumerGroupOffsetsOptions.TimeoutMs)).ReturnType);
        Assert.Equal(typeof(int?), SetterMethods.ParameterTypeOf(typeof(ListConsumerGroupOffsetsOptions), nameof(ListConsumerGroupOffsetsOptions.TimeoutMs)));

        Assert.Equal(typeof(bool), Getters.Get(typeof(ListConsumerGroupOffsetsOptions), nameof(ListConsumerGroupOffsetsOptions.RequireStable)).ReturnType);
        Assert.Equal(typeof(bool), SetterMethods.ParameterTypeOf(typeof(ListConsumerGroupOffsetsOptions), nameof(ListConsumerGroupOffsetsOptions.RequireStable)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void RequireStable_RoundTrips(bool value)
    {
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions()
            .RequireStable(value);

        Assert.Equal(value, options.RequireStable());

        options.RequireStable(!value);
        Assert.Equal(!value, options.RequireStable());
    }

    [Fact]
    public void TimeoutMs_RoundTripsIncludingBackToNull()
    {
        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions().TimeoutMs(0);
        Assert.Equal(0, options.TimeoutMs());

        options.TimeoutMs(30_000);
        Assert.Equal(30_000, options.TimeoutMs());

        options.TimeoutMs(null);
        Assert.Null(options.TimeoutMs());
    }

    [Fact]
    public void CarriesNoPartitionSelection_ThatIsTheSpecsJob()
    {
        Assert.DoesNotContain(
            typeof(ListConsumerGroupOffsetsOptions).GetProperties(),
            property => property.PropertyType == typeof(System.Collections.Generic.IReadOnlyCollection<TopicPartition>));

        Assert.NotEqual(typeof(ListConsumerGroupOffsetsSpec), typeof(ListConsumerGroupOffsetsOptions));
        Assert.False(
            typeof(ListConsumerGroupOffsetsSpec).IsAssignableFrom(typeof(ListConsumerGroupOffsetsOptions)));
    }

    [Fact]
    public void NothingIsDeprecated_BecauseJavaDeprecatesNothingHere()
    {
        Assert.Null(typeof(ListConsumerGroupOffsetsOptions).GetCustomAttribute<ObsoleteAttribute>());

        foreach (MemberInfo member in typeof(ListConsumerGroupOffsetsOptions).GetMembers(
                     BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly))
        {
            Assert.Null(member.GetCustomAttribute<ObsoleteAttribute>());
        }
    }
}
