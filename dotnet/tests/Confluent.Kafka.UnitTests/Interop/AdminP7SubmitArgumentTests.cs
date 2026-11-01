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
using System.Linq;
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What the feature, delegation-token and SCRAM options, the feature updates, the alterations and the requests become
/// in the C handles the binding builds, and how a MAC crosses.
/// </summary>
public sealed class AdminP7SubmitArgumentTests
{
    private const int AlterationUpsertion = 0;

    private const int AlterationDeletion = 1;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Theory]
    [InlineData(ScramMechanism.ScramSha256)]
    [InlineData(ScramMechanism.ScramSha512)]
    public void Upsertion_CarriesTheUserTheCredentialInfoAndThePasswordVerbatim(ScramMechanism mechanism) =>
        ReadUpsertion(
            new UserScramCredentialUpsertion(
                "ålice", new ScramCredentialInfo(mechanism, 8192), new byte[] { 0x61, 0x00, 0x62 }, salt: null),
            native =>
            {
                Assert.Equal("ålice", Utf8Marshal.PtrToString(ReadPointer(native, Native.UpsertionUser)));
                IntPtr info = ReadPointer(native, Native.UpsertionCredentialInfo);
                Assert.Equal((sbyte)mechanism, MechanismType(ReadPointer(info, NativeMethods.ScramCredentialInfoMechanism)));
                Assert.Equal(8192, ReadInt32(info, NativeMethods.ScramCredentialInfoIterations));
                Assert.Equal(new byte[] { 0x61, 0x00, 0x62 }, ReadBytes(native, Native.UpsertionPassword));
            });

    [Fact]
    public void Upsertion_ANullSalt_IsGeneratedByTheCore() =>
        ReadUpsertion(Upsert("u", salt: null), native => Assert.NotEmpty(ReadBytes(native, Native.UpsertionSalt)));

    [Fact]
    public void Upsertion_AnEmptySalt_IsKeptEmpty() =>
        ReadUpsertion(
            Upsert("u", salt: Array.Empty<byte>()), native => Assert.Empty(ReadBytes(native, Native.UpsertionSalt)));

    [Fact]
    public void Upsertion_ASalt_IsForwardedVerbatim() =>
        ReadUpsertion(
            Upsert("u", salt: new byte[] { 0x01, 0x00, 0xFF }),
            native => Assert.Equal(new byte[] { 0x01, 0x00, 0xFF }, ReadBytes(native, Native.UpsertionSalt)));

    [Fact]
    public void Upsertion_AnEmptyPassword_IsKeptEmpty() =>
        ReadUpsertion(
            Upsert("u", password: Array.Empty<byte>(), salt: new byte[] { 7 }),
            native => Assert.Empty(ReadBytes(native, Native.UpsertionPassword)));

    [Theory]
    [InlineData(ScramMechanism.ScramSha256)]
    [InlineData(ScramMechanism.ScramSha512)]
    public void Deletion_CarriesTheUserAndTheMechanism(ScramMechanism mechanism)
    {
        IntPtr native = UserScramCredentialMarshal.NewDeletion(new UserScramCredentialDeletion("ålice", mechanism));
        try
        {
            Assert.Equal("ålice", Utf8Marshal.PtrToString(ReadPointer(native, Native.DeletionUser)));
            Assert.Equal((sbyte)mechanism, MechanismType(ReadPointer(native, Native.DeletionMechanism)));
        }
        finally
        {
            UserScramCredentialMarshal.DestroyDeletion(native);
        }
    }

    [Fact]
    public void Alteration_IsTheSubclassItWasBuiltFrom_WithItsUser()
    {
        AssertAlteration(Upsert("upserted"), AlterationUpsertion, "upserted");
        AssertAlteration(
            new UserScramCredentialDeletion("deleted", ScramMechanism.ScramSha512), AlterationDeletion, "deleted");
    }

    [Fact]
    public void AlterUserScramCredentials_ARepeatedUser_IsOneKey_AndTheMockFailsEveryKey()
    {
        using MockAdminClient admin = MockAdminClient.Create().Build();

        AlterUserScramCredentialsResult result = admin.AlterUserScramCredentials(
            new UserScramCredentialAlteration[]
            {
                Upsert("dup"),
                Upsert("dup"),
                new UserScramCredentialDeletion("other", ScramMechanism.ScramSha256),
            });

        Assert.Equal(new[] { "dup", "other" }, result.Values.Keys.OrderBy(key => key, StringComparer.Ordinal));
        foreach (KafkaFuture future in result.Values.Values)
        {
            AssertNotImplemented(Record.Exception(() => future.Get(s_deadline)));
        }

        AssertNotImplemented(Record.Exception(() => result.All().Get(s_deadline)));
    }

    [Fact]
    public void AlterUserScramCredentials_ANullElement_ThrowsBeforeTheCall()
    {
        using MockAdminClient admin = MockAdminClient.Create().Build();

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => admin.AlterUserScramCredentials(new UserScramCredentialAlteration[] { Upsert("u"), null! }));

        Assert.Equal("alterations", thrown.ParamName);
        Assert.Equal(
            new ArgumentException(
                "The SCRAM credential alterations must not contain a null element.", "alterations").Message,
            thrown.Message);
    }

    [Fact]
    public void DescribeUsers_NullRequestsEveryUser_AndEmptyIsAnEmptyRequest()
    {
        Assert.Null(NativeAdminClient.RequestedScramUsers(null));
        Assert.Empty(NativeAdminClient.RequestedScramUsers(Array.Empty<string>())!);
        Assert.Equal(new[] { "alice", "bob" }, NativeAdminClient.RequestedScramUsers(new[] { "alice", "bob" }));
    }

    [Fact]
    public void DescribeUsers_ARepeatedUser_IsPassedThroughInOrder()
    {
        Assert.Equal(new[] { "alice", "alice" }, NativeAdminClient.RequestedScramUsers(new[] { "alice", "alice" }));
        Assert.Equal(
            new[] { "bob", "alice", "bob" }, NativeAdminClient.RequestedScramUsers(new[] { "bob", "alice", "bob" }));
    }

    [Fact]
    public void DescribeUsers_ANullElement_IsSkipped()
    {
        Assert.Equal(new[] { "alice" }, NativeAdminClient.RequestedScramUsers(new[] { "alice", null! }));
        Assert.Empty(NativeAdminClient.RequestedScramUsers(new string[] { null! })!);

        using MockAdminClient admin = MockAdminClient.Create().Build();
        DescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials(new[] { "alice", null! });
        AssertNotImplemented(Record.Exception(() => result.All().Get(s_deadline)));
    }

    [Fact]
    public void DescribeUsers_AnInvalidUser_ThrowsBeforeTheCall()
    {
        using MockAdminClient admin = MockAdminClient.Create().Build();

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => admin.DescribeUserScramCredentials(new[] { "al\0ice" }));

        Assert.Equal("users", thrown.ParamName);
        Assert.Equal(new ArgumentException(AdminStrings.InvalidStringMessage, "users").Message, thrown.Message);
    }

    [Fact]
    public void ScramOptions_NoSetterRan_TheCOptionsCarryTheDefaultTimeout()
    {
        Assert.Equal(-1, DescribeTimeoutMs(null));
        Assert.Equal(-1, DescribeTimeoutMs(new DescribeUserScramCredentialsOptions()));
        Assert.Empty(new DescribeUserScramCredentialsOptions().Setters.InOrder);
        Assert.Equal(-1, AlterTimeoutMs(null));
        Assert.Equal(-1, AlterTimeoutMs(new AlterUserScramCredentialsOptions()));
        Assert.Empty(new AlterUserScramCredentialsOptions().Setters.InOrder);
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void ScramOptions_TimeoutMs_ReachesTheCOptions_NullAsTheDefaultAndANegativeAsZero(int? timeoutMs, int expected)
    {
        Assert.Equal(expected, DescribeTimeoutMs(new DescribeUserScramCredentialsOptions { TimeoutMs = timeoutMs }));
        Assert.Equal(expected, AlterTimeoutMs(new AlterUserScramCredentialsOptions { TimeoutMs = timeoutMs }));
    }

    [Fact]
    public void ScramOptions_ASetterRunTwice_SendsItsLastValue()
    {
        DescribeUserScramCredentialsOptions describe = new DescribeUserScramCredentialsOptions { TimeoutMs = 10 };
        describe.TimeoutMs = 20;
        Assert.Equal(20, DescribeTimeoutMs(describe));
        Assert.Equal(new[] { nameof(DescribeUserScramCredentialsOptions.TimeoutMs) }, describe.Setters.InOrder);

        AlterUserScramCredentialsOptions alter = new AlterUserScramCredentialsOptions { TimeoutMs = 10 };
        alter.TimeoutMs = null;
        Assert.Equal(-1, AlterTimeoutMs(alter));
        Assert.Equal(new[] { nameof(AlterUserScramCredentialsOptions.TimeoutMs) }, alter.Setters.InOrder);
    }

    [Fact]
    public void TokenOptions_NoSetterRan_TheCOptionsCarryJavasDefaults()
    {
        foreach (CreateDelegationTokenOptions? create in new[] { null, new CreateDelegationTokenOptions() })
        {
            Assert.Empty(ReadCreate(create, native => Principals(ReadPointer(native, Native.CreateDelegationTokenOptionsRenewers))));
            Assert.Equal(IntPtr.Zero, ReadCreate(create, native => ReadPointer(native, Native.CreateDelegationTokenOptionsOwner)));
            Assert.Equal(-1L, ReadCreate(create, native => ReadInt64(native, Native.CreateDelegationTokenOptionsMaxLifetimeMs)));
            Assert.Equal(-1, ReadCreate(create, native => ReadInt32(native, Native.CreateDelegationTokenOptionsTimeoutMs)));
        }

        foreach (RenewDelegationTokenOptions? renew in new[] { null, new RenewDelegationTokenOptions() })
        {
            Assert.Equal(-1L, ReadRenew(renew, native => ReadInt64(native, Native.RenewDelegationTokenOptionsRenewTimePeriodMs)));
            Assert.Equal(-1, ReadRenew(renew, native => ReadInt32(native, Native.RenewDelegationTokenOptionsTimeoutMs)));
        }

        foreach (ExpireDelegationTokenOptions? expire in new[] { null, new ExpireDelegationTokenOptions() })
        {
            Assert.Equal(-1L, ReadExpire(expire, native => ReadInt64(native, Native.ExpireDelegationTokenOptionsExpiryTimePeriodMs)));
            Assert.Equal(-1, ReadExpire(expire, native => ReadInt32(native, Native.ExpireDelegationTokenOptionsTimeoutMs)));
        }

        foreach (DescribeDelegationTokenOptions? describe in new[] { null, new DescribeDelegationTokenOptions() })
        {
            Assert.Null(DescribeOwners(describe));
            Assert.Equal(-1, ReadDescribe(describe, native => ReadInt32(native, Native.DescribeDelegationTokenOptionsTimeoutMs)));
        }

        Assert.Empty(new CreateDelegationTokenOptions().Setters.InOrder);
        Assert.Empty(new RenewDelegationTokenOptions().Setters.InOrder);
        Assert.Empty(new ExpireDelegationTokenOptions().Setters.InOrder);
        Assert.Empty(new DescribeDelegationTokenOptions().Setters.InOrder);
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void TokenOptions_TimeoutMs_ReachesTheCOptions_NullAsTheDefaultAndANegativeAsZero(int? timeoutMs, int expected)
    {
        Assert.Equal(
            expected,
            ReadCreate(
                new CreateDelegationTokenOptions { TimeoutMs = timeoutMs },
                native => ReadInt32(native, Native.CreateDelegationTokenOptionsTimeoutMs)));
        Assert.Equal(
            expected,
            ReadRenew(
                new RenewDelegationTokenOptions { TimeoutMs = timeoutMs },
                native => ReadInt32(native, Native.RenewDelegationTokenOptionsTimeoutMs)));
        Assert.Equal(
            expected,
            ReadExpire(
                new ExpireDelegationTokenOptions { TimeoutMs = timeoutMs },
                native => ReadInt32(native, Native.ExpireDelegationTokenOptionsTimeoutMs)));
        Assert.Equal(
            expected,
            ReadDescribe(
                new DescribeDelegationTokenOptions { TimeoutMs = timeoutMs },
                native => ReadInt32(native, Native.DescribeDelegationTokenOptionsTimeoutMs)));
    }

    [Fact]
    public void CreateOptions_EachSetter_ReachesTheCOptions()
    {
        CreateDelegationTokenOptions options = new CreateDelegationTokenOptions
        {
            Renewers = new[] { new KafkaPrincipal("User", "first"), new KafkaPrincipal("Group", "second") },
            Owner = new KafkaPrincipal("User", "owner"),
            MaxLifetimeMs = 86_400_000L,
        };

        Assert.Equal(
            new[] { new KafkaPrincipal("User", "first"), new KafkaPrincipal("Group", "second") },
            ReadCreate(options, native => Principals(ReadPointer(native, Native.CreateDelegationTokenOptionsRenewers))));
        Assert.Equal(
            new KafkaPrincipal("User", "owner"),
            ReadCreate(options, native => DelegationTokenMarshal.CopyOutPrincipal(ReadPointer(native, Native.CreateDelegationTokenOptionsOwner))));
        Assert.Equal(86_400_000L, ReadCreate(options, native => ReadInt64(native, Native.CreateDelegationTokenOptionsMaxLifetimeMs)));
    }

    [Fact]
    public void CreateOptions_ANullOwner_LeavesTheOwnerUnset()
    {
        CreateDelegationTokenOptions options = new CreateDelegationTokenOptions { Owner = new KafkaPrincipal("User", "owner") };
        options.Owner = null;

        Assert.Equal(IntPtr.Zero, ReadCreate(options, native => ReadPointer(native, Native.CreateDelegationTokenOptionsOwner)));
        Assert.Equal(new[] { nameof(CreateDelegationTokenOptions.Owner) }, options.Setters.InOrder);
    }

    [Fact]
    public void RenewAndExpireOptions_ThePeriodReachesTheCOptions()
    {
        Assert.Equal(
            3_600_000L,
            ReadRenew(
                new RenewDelegationTokenOptions { RenewTimePeriodMs = 3_600_000L },
                native => ReadInt64(native, Native.RenewDelegationTokenOptionsRenewTimePeriodMs)));
        Assert.Equal(
            5_000L,
            ReadExpire(
                new ExpireDelegationTokenOptions { ExpiryTimePeriodMs = 5_000L },
                native => ReadInt64(native, Native.ExpireDelegationTokenOptionsExpiryTimePeriodMs)));
    }

    [Fact]
    public void DescribeOptions_Owners_NullAndEmptyStayApart_AndKeepRequestOrder()
    {
        Assert.Null(DescribeOwners(new DescribeDelegationTokenOptions { Owners = null }));
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyList<KafkaPrincipal>>(
            DescribeOwners(new DescribeDelegationTokenOptions { Owners = Array.Empty<KafkaPrincipal>() })));
        Assert.Equal(
            new[] { new KafkaPrincipal("User", "alice"), new KafkaPrincipal("Group", "ops") },
            DescribeOwners(new DescribeDelegationTokenOptions
            {
                Owners = new[] { new KafkaPrincipal("User", "alice"), new KafkaPrincipal("Group", "ops") },
            }));
    }

    [Fact]
    public void TokenOptions_ASetterRunTwice_SendsItsLastValue()
    {
        CreateDelegationTokenOptions create = new CreateDelegationTokenOptions
        {
            Renewers = new[] { new KafkaPrincipal("User", "first") },
            Owner = new KafkaPrincipal("User", "first-owner"),
        };
        create.Renewers = new[] { new KafkaPrincipal("User", "second") };
        create.Owner = new KafkaPrincipal("User", "second-owner");
        Assert.Equal(
            new[] { new KafkaPrincipal("User", "second") },
            ReadCreate(create, native => Principals(ReadPointer(native, Native.CreateDelegationTokenOptionsRenewers))));
        Assert.Equal(
            new KafkaPrincipal("User", "second-owner"),
            ReadCreate(create, native => DelegationTokenMarshal.CopyOutPrincipal(ReadPointer(native, Native.CreateDelegationTokenOptionsOwner))));
        Assert.Equal(
            new[] { nameof(CreateDelegationTokenOptions.Renewers), nameof(CreateDelegationTokenOptions.Owner) },
            create.Setters.InOrder);

        RenewDelegationTokenOptions renew = new RenewDelegationTokenOptions { RenewTimePeriodMs = 10 };
        renew.RenewTimePeriodMs = 20;
        Assert.Equal(20L, ReadRenew(renew, native => ReadInt64(native, Native.RenewDelegationTokenOptionsRenewTimePeriodMs)));

        ExpireDelegationTokenOptions expire = new ExpireDelegationTokenOptions { ExpiryTimePeriodMs = 10 };
        expire.ExpiryTimePeriodMs = 20;
        Assert.Equal(20L, ReadExpire(expire, native => ReadInt64(native, Native.ExpireDelegationTokenOptionsExpiryTimePeriodMs)));

        DescribeDelegationTokenOptions describe = new DescribeDelegationTokenOptions
        {
            Owners = new[] { new KafkaPrincipal("User", "alice") },
        };
        describe.Owners = null;
        Assert.Null(DescribeOwners(describe));
        Assert.Equal(new[] { nameof(DescribeDelegationTokenOptions.Owners) }, describe.Setters.InOrder);
    }

    [Fact]
    public void WithHmac_PassesTheWholeMac_AnInteriorZeroIncluded()
    {
        byte[] hmac = { 0x9A, 0x4F, 0x00, 0x00, 0xC3, 0x01 };

        byte[] crossed = DelegationTokenMarshal.WithHmac(hmac, bytes => CopyRow(bytes.Data, bytes.Length));

        Assert.Equal(hmac, crossed);
    }

    [Fact]
    public void WithHmac_NullIsANullPointer_EmptyIsANonNullPointerOfLengthZero()
    {
        KafkaBytes absent = DelegationTokenMarshal.WithHmac(null, bytes => bytes);
        Assert.Equal(IntPtr.Zero, absent.Data);
        Assert.Equal(0, absent.Length);

        (bool NonNull, int Length) empty = DelegationTokenMarshal.WithHmac(
            Array.Empty<byte>(), bytes => (bytes.Data != IntPtr.Zero, bytes.Length));
        Assert.True(empty.NonNull);
        Assert.Equal(0, empty.Length);
    }

    [Fact]
    public void FeatureOptions_NoSetterRan_TheCOptionsCarryJavasDefaults()
    {
        Assert.Equal(-1, ReadDescribeFeatures(null, native => ReadInt32(native, Native.DescribeFeaturesOptionsTimeoutMs)));
        Assert.Equal(-1, NodeIdOf(null));
        Assert.Equal(-1, NodeIdOf(new DescribeFeaturesOptions()));
        Assert.Empty(new DescribeFeaturesOptions().Setters.InOrder);
        Assert.Equal(-1, ReadUpdateFeatures(null, native => ReadInt32(native, Native.UpdateFeaturesOptionsTimeoutMs)));
        Assert.Equal(0, ValidateOnlyOf(null));
        Assert.Equal(0, ValidateOnlyOf(new UpdateFeaturesOptions()));
        Assert.Empty(new UpdateFeaturesOptions().Setters.InOrder);
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void FeatureOptions_TimeoutMs_ReachesTheCOptions_NullAsTheDefaultAndANegativeAsZero(int? timeoutMs, int expected)
    {
        Assert.Equal(
            expected,
            ReadDescribeFeatures(
                new DescribeFeaturesOptions { TimeoutMs = timeoutMs },
                native => ReadInt32(native, Native.DescribeFeaturesOptionsTimeoutMs)));
        Assert.Equal(
            expected,
            ReadUpdateFeatures(
                new UpdateFeaturesOptions { TimeoutMs = timeoutMs },
                native => ReadInt32(native, Native.UpdateFeaturesOptionsTimeoutMs)));
    }

    [Fact]
    public void NodeId_UnsetAndZero_StayApart_AndASetIdIsForwardedVerbatim()
    {
        Assert.Equal(-1, NodeIdOf(new DescribeFeaturesOptions()));
        Assert.Equal(0, NodeIdOf(new DescribeFeaturesOptions { NodeId = 0 }));
        Assert.Equal(7, NodeIdOf(new DescribeFeaturesOptions { NodeId = 7 }));
    }

    [Fact]
    public void NodeId_Null_ThrowsAndLeavesTheFieldAsItWas()
    {
        DescribeFeaturesOptions options = new DescribeFeaturesOptions();
        Assert.Equal("value", Assert.Throws<ArgumentNullException>(() => options.NodeId = null).ParamName);
        Assert.Null(options.NodeId);
        Assert.Empty(options.Setters.InOrder);
        Assert.Equal(-1, NodeIdOf(options));

        options.NodeId = 3;
        Assert.Throws<ArgumentNullException>(() => options.NodeId = null);
        Assert.Equal(3, options.NodeId);
        Assert.Equal(new[] { nameof(DescribeFeaturesOptions.NodeId) }, options.Setters.InOrder);
        Assert.Equal(3, NodeIdOf(options));
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(true, 1)]
    public void UpdateFeaturesOptions_ValidateOnly_ReachesTheCOptions(bool validateOnly, int expected) =>
        Assert.Equal(expected, ValidateOnlyOf(new UpdateFeaturesOptions { ValidateOnly = validateOnly }));

    [Fact]
    public void FeatureOptions_ASetterRunTwice_SendsItsLastValue()
    {
        DescribeFeaturesOptions describe = new DescribeFeaturesOptions { NodeId = 1, TimeoutMs = 10 };
        describe.NodeId = 2;
        Assert.Equal(2, NodeIdOf(describe));
        Assert.Equal(
            new[] { nameof(DescribeFeaturesOptions.TimeoutMs), nameof(DescribeFeaturesOptions.NodeId) },
            describe.Setters.InOrder);

        UpdateFeaturesOptions update = new UpdateFeaturesOptions { ValidateOnly = true, TimeoutMs = 10 };
        update.ValidateOnly = false;
        update.TimeoutMs = null;
        Assert.Equal(0, ValidateOnlyOf(update));
        Assert.Equal(-1, ReadUpdateFeatures(update, native => ReadInt32(native, Native.UpdateFeaturesOptionsTimeoutMs)));
        Assert.Equal(
            new[] { nameof(UpdateFeaturesOptions.ValidateOnly), nameof(UpdateFeaturesOptions.TimeoutMs) },
            update.Setters.InOrder);
    }

    [Theory]
    [InlineData((short)3, FeatureUpdate.UpgradeType.Upgrade, 1)]
    [InlineData((short)2, FeatureUpdate.UpgradeType.SafeDowngrade, 2)]
    [InlineData((short)0, FeatureUpdate.UpgradeType.UnsafeDowngrade, 3)]
    [InlineData(short.MaxValue, FeatureUpdate.UpgradeType.Upgrade, 1)]
    [InlineData((short)1, FeatureUpdate.UpgradeType.Unknown, 0)]
    public void FeatureUpdate_CrossesItsLevelAndItsUpgradeTypeCode(
        short maxVersionLevel, FeatureUpdate.UpgradeType upgradeType, int expectedCode) =>
        Assert.Equal(
            (maxVersionLevel, (sbyte)expectedCode),
            ReadFeatureUpdate(new FeatureUpdate(maxVersionLevel, upgradeType)));

    [Fact]
    public void FeatureUpdate_AnUndefinedUpgradeType_CrossesAsUnknown() =>
        Assert.Equal(
            ((short)1, (sbyte)0),
            ReadFeatureUpdate(new FeatureUpdate(1, (FeatureUpdate.UpgradeType)9)));

    [Fact]
    public void UnregisterControllerOptions_NoSetterRan_TheCOptionsCarryJavasDefaults()
    {
        Assert.Equal(-1, UnregisterControllerTimeoutOf(null));
        Assert.Equal(-1, UnregisterControllerTimeoutOf(new UnregisterControllerOptions()));
        Assert.Empty(new UnregisterControllerOptions().Setters.InOrder);
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    [InlineData(int.MinValue, 0)]
    public void UnregisterControllerOptions_TimeoutMs_ReachesTheCOptions_NullAsTheDefaultAndANegativeAsZero(
        int? timeoutMs, int expected) =>
        Assert.Equal(expected, UnregisterControllerTimeoutOf(new UnregisterControllerOptions { TimeoutMs = timeoutMs }));

    [Fact]
    public void UnregisterControllerOptions_ASetterRunTwice_SendsItsLastValue()
    {
        UnregisterControllerOptions options = new UnregisterControllerOptions { TimeoutMs = 10 };
        options.TimeoutMs = 20;
        Assert.Equal(20, UnregisterControllerTimeoutOf(options));

        options.TimeoutMs = null;
        Assert.Equal(-1, UnregisterControllerTimeoutOf(options));
        Assert.Equal(new[] { nameof(UnregisterControllerOptions.TimeoutMs) }, options.Setters.InOrder);
    }

    private static UserScramCredentialUpsertion Upsert(string user, byte[]? password = null, byte[]? salt = null) =>
        new UserScramCredentialUpsertion(
            user,
            new ScramCredentialInfo(ScramMechanism.ScramSha256, 4096),
            password ?? new byte[] { 1, 2, 3 },
            salt);

    private static void ReadUpsertion(UserScramCredentialUpsertion upsertion, Action<IntPtr> read)
    {
        IntPtr native = UserScramCredentialMarshal.NewUpsertion(upsertion);
        try
        {
            read(native);
        }
        finally
        {
            UserScramCredentialMarshal.DestroyUpsertion(native);
        }
    }

    private static void AssertAlteration(UserScramCredentialAlteration alteration, int expectedKind, string expectedUser)
    {
        IntPtr native = UserScramCredentialMarshal.NewAlteration(alteration);
        try
        {
            Assert.Equal(expectedKind, ReadInt32(native, Native.AlterationEnum));
            Assert.Equal(expectedUser, Utf8Marshal.PtrToString(ReadPointer(native, Native.AlterationUser)));
        }
        finally
        {
            UserScramCredentialMarshal.DestroyAlteration(native);
        }
    }

    private static void AssertNotImplemented(Exception? outcome)
    {
        NotSupportedException failure = Assert.IsType<NotSupportedException>(outcome);
        Assert.Equal("Not implemented yet", failure.Message);
    }

    private static int DescribeTimeoutMs(DescribeUserScramCredentialsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeUserScramCredentialsOptions(options);
        try
        {
            return ReadInt32(native, Native.DescribeUserScramCredentialsOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeDescribeUserScramCredentialsOptions(native);
        }
    }

    private static int AlterTimeoutMs(AlterUserScramCredentialsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewAlterUserScramCredentialsOptions(options);
        try
        {
            return ReadInt32(native, Native.AlterUserScramCredentialsOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeAlterUserScramCredentialsOptions(native);
        }
    }

    private static sbyte MechanismType(IntPtr mechanism)
    {
        IntPtr outErr = IntPtr.Zero;
        sbyte type = NativeMethods.ScramMechanismType(mechanism, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return type;
    }

    private static IntPtr ReadPointer(IntPtr self, Native.PointerGetter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr value = getter(self, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static int ReadInt32(IntPtr self, Native.Int32Getter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        int value = getter(self, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static byte[] ReadBytes(IntPtr self, Native.BytesGetter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        KafkaBytes bytes = getter(self, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return CopyRow(bytes.Data, bytes.Length);
    }

    private static T ReadCreate<T>(CreateDelegationTokenOptions? options, Func<IntPtr, T> read)
    {
        IntPtr native = NativeAdminClient.NewCreateDelegationTokenOptions(options);
        try
        {
            return read(native);
        }
        finally
        {
            NativeAdminClient.FreeCreateDelegationTokenOptions(native);
        }
    }

    private static T ReadRenew<T>(RenewDelegationTokenOptions? options, Func<IntPtr, T> read)
    {
        IntPtr native = NativeAdminClient.NewRenewDelegationTokenOptions(options);
        try
        {
            return read(native);
        }
        finally
        {
            NativeAdminClient.FreeRenewDelegationTokenOptions(native);
        }
    }

    private static T ReadExpire<T>(ExpireDelegationTokenOptions? options, Func<IntPtr, T> read)
    {
        IntPtr native = NativeAdminClient.NewExpireDelegationTokenOptions(options);
        try
        {
            return read(native);
        }
        finally
        {
            NativeAdminClient.FreeExpireDelegationTokenOptions(native);
        }
    }

    private static T ReadDescribe<T>(DescribeDelegationTokenOptions? options, Func<IntPtr, T> read)
    {
        IntPtr native = NativeAdminClient.NewDescribeDelegationTokenOptions(options);
        try
        {
            return read(native);
        }
        finally
        {
            NativeAdminClient.FreeDescribeDelegationTokenOptions(native);
        }
    }

    private static IReadOnlyList<KafkaPrincipal>? DescribeOwners(DescribeDelegationTokenOptions? options) =>
        ReadDescribe(options, native =>
        {
            IntPtr owners = ReadPointer(native, Native.DescribeDelegationTokenOptionsOwners);
            return owners == IntPtr.Zero ? null : Principals(owners);
        });

    private static IReadOnlyList<KafkaPrincipal> Principals(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        try
        {
            int count = NativeMethods.ListSize(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            List<KafkaPrincipal> principals = new List<KafkaPrincipal>(count);
            for (int i = 0; i < count; i++)
            {
                IntPtr principal = NativeMethods.ListGet(list, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                principals.Add(DelegationTokenMarshal.CopyOutPrincipal(principal));
            }

            return principals;
        }
        finally
        {
            NativeMethods.ListDestroy(list, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }
    }

    private static long ReadInt64(IntPtr self, Native.Int64Getter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        long value = getter(self, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static T ReadDescribeFeatures<T>(DescribeFeaturesOptions? options, Func<IntPtr, T> read)
    {
        IntPtr native = NativeAdminClient.NewDescribeFeaturesOptions(options);
        try
        {
            return read(native);
        }
        finally
        {
            NativeAdminClient.FreeDescribeFeaturesOptions(native);
        }
    }

    private static T ReadUpdateFeatures<T>(UpdateFeaturesOptions? options, Func<IntPtr, T> read)
    {
        IntPtr native = NativeAdminClient.NewUpdateFeaturesOptions(options);
        try
        {
            return read(native);
        }
        finally
        {
            NativeAdminClient.FreeUpdateFeaturesOptions(native);
        }
    }

    private static int NodeIdOf(DescribeFeaturesOptions? options) =>
        ReadDescribeFeatures(options, native => ReadInt32(native, Native.DescribeFeaturesOptionsNodeId));

    private static sbyte ValidateOnlyOf(UpdateFeaturesOptions? options) =>
        ReadUpdateFeatures(options, native =>
        {
            IntPtr outErr = IntPtr.Zero;
            sbyte value = Native.UpdateFeaturesOptionsValidateOnly(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return value;
        });

    private static int UnregisterControllerTimeoutOf(UnregisterControllerOptions? options)
    {
        IntPtr native = NativeAdminClient.NewUnregisterControllerOptions(options);
        try
        {
            return ReadInt32(native, Native.UnregisterControllerOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeUnregisterControllerOptions(native);
        }
    }

    private static (short MaxVersionLevel, sbyte Code) ReadFeatureUpdate(FeatureUpdate update)
    {
        IntPtr native = NativeAdminClient.NewFeatureUpdate(update);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            short level = Native.FeatureUpdateMaxVersionLevel(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr upgradeType = Native.FeatureUpdateUpgradeType(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte code = Native.UpgradeTypeCode(upgradeType, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (level, code);
        }
        finally
        {
            NativeAdminClient.DestroyFeatureUpdate(native);
        }
    }

    private static byte[] CopyRow(IntPtr pointer, int length)
    {
        if (pointer == IntPtr.Zero || length <= 0)
        {
            return Array.Empty<byte>();
        }

        byte[] bytes = new byte[length];
        Marshal.Copy(pointer, bytes, 0, length);
        return bytes;
    }

    /// <summary>The C getters only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        internal delegate IntPtr PointerGetter(IntPtr self, ref IntPtr outErr);

        internal delegate int Int32Getter(IntPtr self, ref IntPtr outErr);

        internal delegate KafkaBytes BytesGetter(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialUpsertion_user_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr UpsertionUser(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialUpsertion_credential_info_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr UpsertionCredentialInfo(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialUpsertion_salt_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern KafkaBytes UpsertionSalt(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialUpsertion_password_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern KafkaBytes UpsertionPassword(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialDeletion_user_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr DeletionUser(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialDeletion_mechanism_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr DeletionMechanism(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialAlteration__enum_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AlterationEnum(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialAlteration_user_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr AlterationUser(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeUserScramCredentialsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeUserScramCredentialsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterUserScramCredentialsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AlterUserScramCredentialsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        internal delegate long Int64Getter(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_renewers_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr CreateDelegationTokenOptionsRenewers(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_owner_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr CreateDelegationTokenOptionsOwner(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_max_lifetime_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern long CreateDelegationTokenOptionsMaxLifetimeMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreateDelegationTokenOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int CreateDelegationTokenOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_RenewDelegationTokenOptions_renew_time_period_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern long RenewDelegationTokenOptionsRenewTimePeriodMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_RenewDelegationTokenOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int RenewDelegationTokenOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ExpireDelegationTokenOptions_expiry_time_period_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern long ExpireDelegationTokenOptionsExpiryTimePeriodMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ExpireDelegationTokenOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ExpireDelegationTokenOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeDelegationTokenOptions_owners_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr DescribeDelegationTokenOptionsOwners(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeDelegationTokenOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeDelegationTokenOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeFeaturesOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeFeaturesOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeFeaturesOptions_node_id_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeFeaturesOptionsNodeId(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UpdateFeaturesOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int UpdateFeaturesOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UpdateFeaturesOptions_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte UpdateFeaturesOptionsValidateOnly(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_FeatureUpdate_max_version_level_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern short FeatureUpdateMaxVersionLevel(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_FeatureUpdate_upgrade_type_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr FeatureUpdateUpgradeType(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_FeatureUpdate_UpgradeType_code_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte UpgradeTypeCode(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UnregisterControllerOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int UnregisterControllerOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);
    }
}
