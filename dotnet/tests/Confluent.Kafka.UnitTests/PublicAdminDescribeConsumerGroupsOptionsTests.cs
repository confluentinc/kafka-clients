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
/// Pins <see cref="DescribeConsumerGroupsOptions"/> against Java's
/// <c>org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>This class is <em>not</em> deprecated in Java</b>, unlike its neighbour
/// <see cref="ListConsumerGroupsOptions"/> — <c>describeConsumerGroups</c> has no
/// generation-newer replacement. The absence of <see cref="ObsoleteAttribute"/> is
/// asserted rather than left implicit, because an attribute nothing asserts is one a later
/// slice can add or drop without anything going red.
/// </para>
/// <para>
/// <b>This is a pure value type</b> — the ABI wiring, the result type and the client method
/// arrive in later slices — so everything here is managed and broker-free.
/// </para>
/// </remarks>
public sealed class PublicAdminDescribeConsumerGroupsOptionsTests
{
    /// <summary>
    /// A freshly constructed instance matches Java's field defaults — an unset timeout and
    /// an uninitialized <c>boolean</c> (<c>:27</c>) — so <c>options: null</c> at a call site
    /// will behave like one.
    /// </summary>
    [Fact]
    public void Defaults_MatchJavasFieldDefaults()
    {
        DescribeConsumerGroupsOptions options = new DescribeConsumerGroupsOptions();

        Assert.Null(options.TimeoutMs);
        Assert.False(options.IncludeAuthorizedOperations);
    }

    /// <summary>
    /// The public property set is exactly the inherited timeout plus Java's one accessor —
    /// no member dropped, none invented, and no factory Java does not have.
    /// </summary>
    [Fact]
    public void PublicShape_IsTheTimeoutPlusJavasOneAccessor()
    {
        Assert.Equal(
            new[] { "IncludeAuthorizedOperations", "TimeoutMs" },
            typeof(DescribeConsumerGroupsOptions)
                .GetProperties()
                .Select(property => property.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        // Java declares no static factories on this class, so nothing static may appear.
        Assert.Empty(
            typeof(DescribeConsumerGroupsOptions)
                .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly));

        // Java declares no constructor either, so the only one is the implicit default.
        ConstructorInfo constructor = Assert.Single(typeof(DescribeConsumerGroupsOptions).GetConstructors());
        Assert.Empty(constructor.GetParameters());

        PropertyInfo timeout = typeof(DescribeConsumerGroupsOptions).GetProperty(
            nameof(DescribeConsumerGroupsOptions.TimeoutMs))!;
        Assert.Equal(typeof(int?), timeout.PropertyType);
        Assert.NotNull(timeout.SetMethod);

        // Java's fluent setter is a setter, so the property is settable rather than
        // read-only — the object initializer reads the way Java's chain does.
        PropertyInfo include = typeof(DescribeConsumerGroupsOptions).GetProperty(
            nameof(DescribeConsumerGroupsOptions.IncludeAuthorizedOperations))!;
        Assert.Equal(typeof(bool), include.PropertyType);
        Assert.NotNull(include.SetMethod);
    }

    /// <summary>
    /// Java's fluent setter stores the value it is handed and its getter reports it back,
    /// in both directions — including back to the default, which a write-once field would
    /// fail.
    /// </summary>
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

    /// <summary>
    /// The timeout round-trips and can be returned to <see langword="null"/>, which is how
    /// a caller asks for the client's <c>default.api.timeout.ms</c> — distinct from any
    /// numeric value, including zero.
    /// </summary>
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

    /// <summary>
    /// Java carries no <c>@Deprecated</c> on this class or on its accessor, so nothing here
    /// carries <see cref="ObsoleteAttribute"/> — the mirror image of the deprecation
    /// <see cref="ListConsumerGroupsOptions"/> does carry.
    /// </summary>
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
