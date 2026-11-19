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

using Confluent.Kafka.Consumer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>OffsetAndMetadataTest</c> run against <see cref="OffsetAndMetadata"/>, plus the shape of its three
/// public constructors.
/// </summary>
public sealed class OffsetAndMetadataTests
{
    private const string TestsJavaSerialization =
        "tests java.io.Serializable, which the .NET OffsetAndMetadata does not implement";

    [Fact]
    public void TestInvalidNegativeOffset()
    {
        Assert.Equal(
            "Invalid negative offset",
            Assert.Throws<IllegalArgumentException>(() => new OffsetAndMetadata(-239L, 15, "")).Message);
        Assert.Equal(
            "Invalid negative offset",
            Assert.Throws<IllegalArgumentException>(() => new OffsetAndMetadata(-239L, "")).Message);
        Assert.Equal(
            "Invalid negative offset",
            Assert.Throws<IllegalArgumentException>(() => new OffsetAndMetadata(-239L)).Message);
    }

    [Fact(Skip = TestsJavaSerialization)]
    public void TestSerializationRoundtrip()
    {
    }

    [Fact(Skip = TestsJavaSerialization)]
    public void TestDeserializationCompatibilityBeforeLeaderEpoch()
    {
    }

    [Fact(Skip = TestsJavaSerialization)]
    public void TestDeserializationCompatibilityWithLeaderEpoch()
    {
    }

    [Fact]
    public void TestEqualsWithNullAndNegativeLeaderEpoch()
    {
        OffsetAndMetadata metadataWithNullEpoch = new OffsetAndMetadata(100L, null, "metadata");
        OffsetAndMetadata metadataWithNegativeEpoch = new OffsetAndMetadata(100L, -1, "metadata");
        Assert.Equal(metadataWithNullEpoch, metadataWithNegativeEpoch);
        Assert.Equal(metadataWithNullEpoch.GetHashCode(), metadataWithNegativeEpoch.GetHashCode());
    }

    [Fact]
    public void TestEqualsWithNullAndEmptyMetadata()
    {
        OffsetAndMetadata metadataWithNullMetadata = new OffsetAndMetadata(100L, 1, null);
        OffsetAndMetadata metadataWithEmptyMetadata = new OffsetAndMetadata(100L, 1, "");
        Assert.Equal(metadataWithNullMetadata, metadataWithEmptyMetadata);
        Assert.Equal(metadataWithNullMetadata.GetHashCode(), metadataWithEmptyMetadata.GetHashCode());
    }

    [Fact]
    public void ShorterConstructors_PassJavasValues()
    {
        Assert.Equal(new OffsetAndMetadata(239L, null, "blah"), new OffsetAndMetadata(239L, "blah"));
        Assert.Null(new OffsetAndMetadata(239L, "blah").LeaderEpoch());
        Assert.Equal(string.Empty, new OffsetAndMetadata(239L, null).Metadata());
        Assert.Equal(new OffsetAndMetadata(239L, null, string.Empty), new OffsetAndMetadata(239L));
    }

    [Fact]
    public void PublicConstructors_AreJavasThree()
    {
        ConstructorInfo[] constructors =
            typeof(OffsetAndMetadata).GetConstructors(BindingFlags.Public | BindingFlags.Instance);

        Assert.Equal(
            new[]
            {
                "Int64 offset, Int32? leaderEpoch, String metadata",
                "Int64 offset, String metadata",
                "Int64 offset",
            },
            constructors
                .OrderByDescending(constructor => constructor.GetParameters().Length)
                .Select(constructor => string.Join(", ", constructor.GetParameters().Select(Describe)))
                .ToArray());
        Assert.All(
            constructors,
            constructor => Assert.All(constructor.GetParameters(), parameter => Assert.False(parameter.IsOptional)));
    }

    private static string Describe(ParameterInfo parameter)
    {
        Type? underlying = Nullable.GetUnderlyingType(parameter.ParameterType);
        string type = underlying is null ? parameter.ParameterType.Name : underlying.Name + "?";
        return type + " " + parameter.Name;
    }
}
