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
/// The <see cref="UpgradeType"/> part of Python's <c>test_enums</c> on <see cref="UpgradeTypeMethods"/>, under Java's
/// names; then every constant's code and round trip, the code an instance member reads from an undefined value, and
/// the form older C# calls.
/// </summary>
public sealed class PublicUpgradeTypeTests
{
    [Fact]
    public void AdminValueTypes_test_enums_UpgradeTypesCodesAndLookups()
    {
        Assert.Equal(
            new[] { 0, 1, 2, 3 }, ((UpgradeType[])Enum.GetValues(typeof(UpgradeType))).Select(t => (int)t));
        Assert.Equal(UpgradeType.SAFE_DOWNGRADE, UpgradeType.FromCode(2));
        Assert.Equal(UpgradeType.UNKNOWN, UpgradeType.FromCode(0));
        Assert.Equal(1, UpgradeType.UPGRADE.Code());
    }

    [Fact]
    public void Code_IsJavasCodeForEveryConstant()
    {
        Assert.Equal(
            new byte[] { 0, 1, 2, 3 },
            new[] { UpgradeType.UNKNOWN, UpgradeType.UPGRADE, UpgradeType.SAFE_DOWNGRADE, UpgradeType.UNSAFE_DOWNGRADE }
                .Select(t => t.Code()));
    }

    [Fact]
    public void FromCode_RoundTripsEveryConstant()
    {
        foreach (UpgradeType upgradeType in (UpgradeType[])Enum.GetValues(typeof(UpgradeType)))
        {
            Assert.True(upgradeType == UpgradeType.FromCode(upgradeType.Code()), upgradeType.ToString());
        }
    }

    [Theory]
    [InlineData(4)]
    [InlineData(99)]
    [InlineData(255)]
    public void Code_ReadsAnUndefinedValueAsItIs(byte value) =>
        Assert.Equal(value, ((UpgradeType)value).Code());

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(UpgradeType.UNSAFE_DOWNGRADE, UpgradeTypeMethods.FromCode(3));
        Assert.Equal(2, UpgradeTypeMethods.Code(UpgradeType.SAFE_DOWNGRADE));
    }
}
