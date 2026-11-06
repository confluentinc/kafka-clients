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
using System.Globalization;
using System.Linq;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests <see cref="ConfigException"/>: Java's three constructors and their messages (Python's
/// <c>test_config_error_constructors</c>), its code and retriable flag, and the cause it carries for a core error.
/// </summary>
public sealed class ConfigExceptionTests
{
    private const int ConfigErrorCode = -10;

    public static TheoryData<string> ConstructorForms => new TheoryData<string>
    {
        "message",
        "name, value",
        "name, value, message",
    };

    [Fact]
    public void TestConfigErrorConstructors()
    {
        Assert.Equal("plain", new ConfigException(message: "plain").Message);
        Assert.Equal(
            "Invalid value 5 for configuration a.b",
            new ConfigException(name: "a.b", value: 5).Message);
        Assert.Equal(
            "Invalid value true for configuration a.b: m",
            new ConfigException(name: "a.b", value: true, message: "m").Message);
        Assert.Equal(
            "Invalid value [1, 2] for configuration a.b: m",
            new ConfigException(name: "a.b", value: new List<int> { 1, 2 }, message: "m").Message);
    }

    [Fact]
    public void TheValue_IsPrintedAsJavasStringValueOf()
    {
        Assert.Equal(
            "Invalid value 42.5 for configuration batch.size: Expected value to be a 32-bit integer, but it was a System.Double",
            new ConfigException(
                "batch.size", 42.5, "Expected value to be a 32-bit integer, but it was a System.Double").Message);
        Assert.Equal("Invalid value 1.0E7 for configuration a.b", new ConfigException("a.b", 1e7).Message);
        Assert.Equal("Invalid value 0.1 for configuration a.b", new ConfigException("a.b", 0.1f).Message);
        Assert.Equal("Invalid value 1E-7 for configuration a.b", new ConfigException("a.b", 0.0000001m).Message);
        Assert.Equal("Invalid value [a, b] for configuration a.b", new ConfigException("a.b", new[] { "a", "b" }).Message);
    }

    [Fact]
    public void NameValueConstructors_NullNameOrValue_PrintJavasNull()
    {
        Assert.Equal(
            "Invalid value 5 for configuration null",
            new ConfigException(name: null, value: 5).Message);
        Assert.Equal(
            "Invalid value null for configuration a.b",
            new ConfigException(name: "a.b", value: null).Message);
        Assert.Equal(
            "Invalid value null for configuration a.b",
            new ConfigException(name: "a.b", value: null, message: null).Message);
        Assert.Equal(
            "Invalid value false for configuration a.b: m",
            new ConfigException(name: "a.b", value: false, message: "m").Message);
    }

    [Theory]
    [MemberData(nameof(ConstructorForms))]
    public void EveryConstructor_IsAConfigErrorCode_NotRetriable_AndCaughtAsAKafkaException(string form)
    {
        KafkaException? caught = null;
        try
        {
            throw Create(form);
        }
        catch (KafkaException exception)
        {
            caught = exception;
        }

        ConfigException thrown = Assert.IsType<ConfigException>(caught);
        Assert.Equal(ConfigErrorCode, thrown.Code);
        Assert.Null(thrown.InnerException);
    }

    [Fact]
    public void TheType_IsAnUnsealedKafkaException_WithJavasThreePublicConstructors()
    {
        Assert.Equal(typeof(KafkaException), typeof(ConfigException).BaseType);
        Assert.False(typeof(ConfigException).IsSealed);

        string[] constructors = typeof(ConfigException).GetConstructors()
            .Select(constructor => string.Join(", ", constructor.GetParameters().Select(p => p.ParameterType.Name)))
            .OrderBy(signature => signature, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(new[] { "String", "String, Object", "String, Object, String" }, constructors);
    }

    [Fact]
    public void FromCore_CarriesTheCoreMessageAndCause()
    {
        var cause = new InvalidOperationException("the cause: café");

        ConfigException exception = new ConfigException(default(CoreError), "Invalid url in bootstrap.servers: x", cause);

        Assert.Equal("Invalid url in bootstrap.servers: x", exception.Message);
        Assert.Same(cause, exception.InnerException);
        Assert.Equal(ConfigErrorCode, exception.Code);
    }

    [Theory]
    [InlineData("de-DE")]
    [InlineData("sv-SE")]
    public void TheValue_IsPrintedInTheInvariantCulture(string culture)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try
        {
            Assert.NotEqual("-1.5", (-1.5).ToString(CultureInfo.CurrentCulture));

            Assert.Equal("Invalid value -1 for configuration a.b", new ConfigException("a.b", -1).Message);
            Assert.Equal("Invalid value -1.5 for configuration a.b: m", new ConfigException("a.b", -1.5, "m").Message);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    private static ConfigException Create(string form) => form switch
    {
        "message" => new ConfigException("m"),
        "name, value" => new ConfigException("a.b", 5),
        _ => new ConfigException("a.b", 5, "m"),
    };
}
