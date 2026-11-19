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

using Confluent.Kafka.Admin;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>ScramMechanismTest</c> on <see cref="ScramMechanismMethods"/>, under Java's names (Python's
/// <c>test_from_mechanism_name</c> asserts the same five names); the <see cref="ScramMechanism"/> part of Python's
/// <c>test_enums</c>; then every constant's type and name, every byte type, the type an instance member reads from an
/// undefined value, and the form older C# calls.
/// </summary>
public sealed class PublicScramMechanismTests
{
    [Fact]
    public void TestFromMechanismName()
    {
        Assert.Equal(ScramMechanism.UNKNOWN, ScramMechanism.FromMechanismName("UNKNOWN"));
        Assert.Equal(ScramMechanism.SCRAM_SHA_256, ScramMechanism.FromMechanismName("SCRAM-SHA-256"));
        Assert.Equal(ScramMechanism.SCRAM_SHA_512, ScramMechanism.FromMechanismName("SCRAM-SHA-512"));
        Assert.Equal(ScramMechanism.UNKNOWN, ScramMechanism.FromMechanismName("some string"));
        Assert.Equal(ScramMechanism.UNKNOWN, ScramMechanism.FromMechanismName("scram-sha-256"));
    }

    [Fact]
    public void AdminValueTypes_test_enums_ScramMechanismsTypesAndLookups()
    {
        Assert.Equal(
            new[] { 0, 1, 2 }, ((ScramMechanism[])Enum.GetValues(typeof(ScramMechanism))).Select(m => (int)m));
        Assert.Equal(ScramMechanism.SCRAM_SHA_512, ScramMechanism.FromType(2));
        Assert.Equal(ScramMechanism.UNKNOWN, ScramMechanism.FromType(9));
        Assert.Equal(1, ScramMechanism.SCRAM_SHA_256.Type());
        Assert.Equal("SCRAM_SHA_256", ScramMechanism.SCRAM_SHA_256.ToString());
    }

    [Fact]
    public void Type_IsJavasTypeForEveryConstant()
    {
        Assert.Equal(
            new byte[] { 0, 1, 2 },
            new[] { ScramMechanism.UNKNOWN, ScramMechanism.SCRAM_SHA_256, ScramMechanism.SCRAM_SHA_512 }
                .Select(m => m.Type()));
    }

    [Theory]
    [InlineData(ScramMechanism.UNKNOWN, "UNKNOWN")]
    [InlineData(ScramMechanism.SCRAM_SHA_256, "SCRAM-SHA-256")]
    [InlineData(ScramMechanism.SCRAM_SHA_512, "SCRAM-SHA-512")]
    public void MechanismName_IsTheConstantWithDashes_AndFindsTheConstantAgain(
        ScramMechanism mechanism, string mechanismName)
    {
        Assert.Equal(mechanismName, mechanism.MechanismName());
        Assert.Equal(mechanism, ScramMechanism.FromMechanismName(mechanismName));
    }

    [Fact]
    public void FromMechanismName_IsUnknownForAnotherScramName_OrNull()
    {
        Assert.Equal(ScramMechanism.UNKNOWN, ScramMechanism.FromMechanismName("SCRAM-SHA-1"));
        Assert.Equal(ScramMechanism.UNKNOWN, ScramMechanism.FromMechanismName(null!));
    }

    [Fact]
    public void FromType_RoundTripsEveryConstant()
    {
        foreach (ScramMechanism mechanism in (ScramMechanism[])Enum.GetValues(typeof(ScramMechanism)))
        {
            Assert.True(mechanism == ScramMechanism.FromType(mechanism.Type()), mechanism.ToString());
        }
    }

    [Fact]
    public void FromType_EveryByte_IsTheMechanismWithThatTypeOrUnknown()
    {
        for (int type = 0; type <= byte.MaxValue; type++)
        {
            ScramMechanism expected = type switch
            {
                1 => ScramMechanism.SCRAM_SHA_256,
                2 => ScramMechanism.SCRAM_SHA_512,
                _ => ScramMechanism.UNKNOWN,
            };

            Assert.True(expected == ScramMechanism.FromType((byte)type), "type " + type);
        }
    }

    [Theory]
    [InlineData(3)]
    [InlineData(99)]
    [InlineData(255)]
    public void Type_ReadsAnUndefinedValueAsItIs(byte value) =>
        Assert.Equal(value, ((ScramMechanism)value).Type());

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(ScramMechanism.SCRAM_SHA_256, ScramMechanismMethods.FromMechanismName("SCRAM-SHA-256"));
        Assert.Equal(ScramMechanism.SCRAM_SHA_512, ScramMechanismMethods.FromType(2));
        Assert.Equal("SCRAM-SHA-512", ScramMechanismMethods.MechanismName(ScramMechanism.SCRAM_SHA_512));
        Assert.Equal(2, ScramMechanismMethods.Type(ScramMechanism.SCRAM_SHA_512));
    }
}
