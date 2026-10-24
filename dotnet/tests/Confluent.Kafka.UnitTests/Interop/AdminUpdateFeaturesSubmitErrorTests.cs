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
using System.Runtime.CompilerServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The <c>UpdateFeatures</c> calls that fail before any result is handed out: the core's refusal of an empty
/// map or a blank name on the real client, and the binding's refusal of a name the caller's comparer lets
/// appear twice, which a C map cannot hold.
/// </summary>
public sealed class AdminUpdateFeaturesSubmitErrorTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly IReadOnlyDictionary<string, string> s_unreachable =
        new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AnEmptyMap_OnTheRealClient_IsRefusedByTheCore(bool asyncFlavour)
    {
        NativeAdminClient admin = NativeAdminClient.Create(s_unreachable, asyncFlavour ? AdminFlavour.Async : AdminFlavour.Sync);
        try
        {
            ArgumentException thrown = Assert.Throws<ArgumentException>(
                () => admin.UpdateFeatures(new Dictionary<string, FeatureUpdate>(), options: null));

            Assert.Equal("Feature updates can not be null or empty.", thrown.Message);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [Theory]
    [InlineData(false, "")]
    [InlineData(false, " \t")]
    [InlineData(true, "")]
    [InlineData(true, " \t")]
    public void ABlankName_OnTheRealClient_IsRefusedByTheCore(bool asyncFlavour, string name)
    {
        NativeAdminClient admin = NativeAdminClient.Create(s_unreachable, asyncFlavour ? AdminFlavour.Async : AdminFlavour.Sync);
        try
        {
            ArgumentException thrown = Assert.Throws<ArgumentException>(() => admin.UpdateFeatures(
                new Dictionary<string, FeatureUpdate>
                {
                    ["metadata.version"] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
                    [name] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
                },
                options: null));

            Assert.Equal("Provided feature can not be empty.", thrown.Message);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ARepeatedFeatureUnderTheCallersComparer_IsRefusedBeforeTheCall(bool real)
    {
        string first = Copy("f");
        string second = Copy("f");
        Assert.NotSame(first, second);
        Dictionary<string, FeatureUpdate> updates =
            new Dictionary<string, FeatureUpdate>(ReferenceComparer.Instance)
            {
                [first] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
                [second] = new FeatureUpdate(2, FeatureUpdate.UpgradeType.Upgrade),
            };
        Assert.Equal(2, updates.Count);

        NativeAdminClient admin = real ? NativeAdminClient.Create(s_unreachable) : NativeAdminClient.CreateMock(1);
        try
        {
            ArgumentException thrown =
                Assert.Throws<ArgumentException>(() => admin.UpdateFeatures(updates, options: null));

            Assert.Equal("featureUpdates", thrown.ParamName);
            Assert.StartsWith("The feature updates contain the feature 'f' more than once.", thrown.Message);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string Copy(string value) => new string(value.ToCharArray());

    /// <summary>
    /// Reference equality over strings, hand-written because this project also compiles for
    /// net462, where <c>System.Collections.Generic.ReferenceEqualityComparer</c> (.NET 5+) does
    /// not exist.
    /// </summary>
    private sealed class ReferenceComparer : IEqualityComparer<string>
    {
        public static readonly ReferenceComparer Instance = new ReferenceComparer();

        public bool Equals(string? x, string? y) => ReferenceEquals(x, y);

        public int GetHashCode(string obj) => RuntimeHelpers.GetHashCode(obj);
    }
}
