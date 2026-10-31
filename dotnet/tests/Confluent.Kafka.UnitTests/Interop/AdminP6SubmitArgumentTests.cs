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
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What the admin inputs become in C: the ACL bindings and filters, the client-quota filters and alterations, and
/// their options, read back from the C handles the client builds.
/// </summary>
public sealed class AdminP6SubmitArgumentTests
{
    public static TheoryData<ResourceType> ResourceTypes => EnumValues<ResourceType>();

    public static TheoryData<PatternType> PatternTypes => EnumValues<PatternType>();

    public static TheoryData<AclOperation> Operations => EnumValues<AclOperation>();

    public static TheoryData<AclPermissionType> PermissionTypes => EnumValues<AclPermissionType>();

    [Fact]
    public void Filter_NullIsANullPointer_AndEmptyIsAPointerToANul()
    {
        AclBindingFilter nulls = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Any),
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any));
        WithFilter(nulls, (pattern, entry) =>
        {
            Assert.Equal(IntPtr.Zero, Read(pattern, NativeMethods.ResourcePatternFilterName));
            Assert.Equal(IntPtr.Zero, Read(entry, NativeMethods.AccessControlEntryFilterPrincipal));
            Assert.Equal(IntPtr.Zero, Read(entry, NativeMethods.AccessControlEntryFilterHost));
        });

        AclBindingFilter empties = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Any),
            new AccessControlEntryFilter(string.Empty, string.Empty, AclOperation.Any, AclPermissionType.Any));
        WithFilter(empties, (pattern, entry) =>
        {
            AssertEmptyString(Read(pattern, NativeMethods.ResourcePatternFilterName));
            AssertEmptyString(Read(entry, NativeMethods.AccessControlEntryFilterPrincipal));
            AssertEmptyString(Read(entry, NativeMethods.AccessControlEntryFilterHost));
        });
    }

    [Fact]
    public void Filter_CarriesEveryColumn()
    {
        AclBindingFilter filter = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Group, "grüppe-ü", PatternType.Prefixed),
            new AccessControlEntryFilter("User:bjørn", "10.0.0.1", AclOperation.Describe, AclPermissionType.Deny));
        WithFilter(filter, (pattern, entry) =>
        {
            Assert.Equal((sbyte)ResourceType.Group, ResourceTypeCode(pattern, NativeMethods.ResourcePatternFilterResourceType));
            Assert.Equal("grüppe-ü", Utf8Marshal.PtrToString(Read(pattern, NativeMethods.ResourcePatternFilterName)));
            Assert.Equal((sbyte)PatternType.Prefixed, PatternTypeCode(pattern, NativeMethods.ResourcePatternFilterPatternType));
            Assert.Equal("User:bjørn", Utf8Marshal.PtrToString(Read(entry, NativeMethods.AccessControlEntryFilterPrincipal)));
            Assert.Equal("10.0.0.1", Utf8Marshal.PtrToString(Read(entry, NativeMethods.AccessControlEntryFilterHost)));
            Assert.Equal((sbyte)AclOperation.Describe, OperationCode(entry, NativeMethods.AccessControlEntryFilterOperation));
            Assert.Equal(
                (sbyte)AclPermissionType.Deny,
                PermissionTypeCode(entry, NativeMethods.AccessControlEntryFilterPermissionType));
        });
    }

    [Theory]
    [MemberData(nameof(ResourceTypes))]
    public void Filter_EveryResourceType_CrossesAsItsCode(ResourceType resourceType) =>
        WithFilter(
            Filter(resourceType, PatternType.Any, AclOperation.Any, AclPermissionType.Any),
            (pattern, _) => Assert.Equal(
                (sbyte)resourceType, ResourceTypeCode(pattern, NativeMethods.ResourcePatternFilterResourceType)));

    [Theory]
    [MemberData(nameof(PatternTypes))]
    public void Filter_EveryPatternType_CrossesAsItsCode(PatternType patternType) =>
        WithFilter(
            Filter(ResourceType.Any, patternType, AclOperation.Any, AclPermissionType.Any),
            (pattern, _) => Assert.Equal(
                (sbyte)patternType, PatternTypeCode(pattern, NativeMethods.ResourcePatternFilterPatternType)));

    [Theory]
    [MemberData(nameof(Operations))]
    public void Filter_EveryOperation_CrossesAsItsCode(AclOperation operation) =>
        WithFilter(
            Filter(ResourceType.Any, PatternType.Any, operation, AclPermissionType.Any),
            (_, entry) => Assert.Equal(
                (sbyte)operation, OperationCode(entry, NativeMethods.AccessControlEntryFilterOperation)));

    [Theory]
    [MemberData(nameof(PermissionTypes))]
    public void Filter_EveryPermissionType_CrossesAsItsCode(AclPermissionType permissionType) =>
        WithFilter(
            Filter(ResourceType.Any, PatternType.Any, AclOperation.Any, permissionType),
            (_, entry) => Assert.Equal(
                (sbyte)permissionType, PermissionTypeCode(entry, NativeMethods.AccessControlEntryFilterPermissionType)));

    [Fact]
    public void Binding_CarriesEveryColumn()
    {
        AclBinding binding = new AclBinding(
            new ResourcePattern(ResourceType.TransactionalId, "tx-ünï", PatternType.Literal),
            new AccessControlEntry("User:ålice", "*", AclOperation.Write, AclPermissionType.Allow));
        WithBinding(binding, (pattern, entry) =>
        {
            Assert.Equal((sbyte)ResourceType.TransactionalId, ResourceTypeCode(pattern, NativeMethods.ResourcePatternResourceType));
            Assert.Equal("tx-ünï", Utf8Marshal.PtrToString(Read(pattern, NativeMethods.ResourcePatternName)));
            Assert.Equal((sbyte)PatternType.Literal, PatternTypeCode(pattern, NativeMethods.ResourcePatternPatternType));
            Assert.Equal("User:ålice", Utf8Marshal.PtrToString(Read(entry, NativeMethods.AccessControlEntryPrincipal)));
            Assert.Equal("*", Utf8Marshal.PtrToString(Read(entry, NativeMethods.AccessControlEntryHost)));
            Assert.Equal((sbyte)AclOperation.Write, OperationCode(entry, NativeMethods.AccessControlEntryOperation));
            Assert.Equal((sbyte)AclPermissionType.Allow, PermissionTypeCode(entry, NativeMethods.AccessControlEntryPermissionType));
        });
    }

    [Fact]
    public void Binding_AnEmptyString_IsAPointerToANul()
    {
        AclBinding binding = new AclBinding(
            new ResourcePattern(ResourceType.Topic, string.Empty, PatternType.Literal),
            new AccessControlEntry(string.Empty, string.Empty, AclOperation.Read, AclPermissionType.Allow));
        WithBinding(binding, (pattern, entry) =>
        {
            AssertEmptyString(Read(pattern, NativeMethods.ResourcePatternName));
            AssertEmptyString(Read(entry, NativeMethods.AccessControlEntryPrincipal));
            AssertEmptyString(Read(entry, NativeMethods.AccessControlEntryHost));
        });
    }

    [Fact]
    public void AclOptions_NoSetterRan_TheCOptionsCarryTheDefaultTimeout()
    {
        Assert.Equal(-1, CreateAclsTimeoutMs(null));
        Assert.Equal(-1, CreateAclsTimeoutMs(new CreateAclsOptions()));
        Assert.Empty(new CreateAclsOptions().Setters.InOrder);
        Assert.Equal(-1, DescribeAclsTimeoutMs(null));
        Assert.Equal(-1, DescribeAclsTimeoutMs(new DescribeAclsOptions()));
        Assert.Empty(new DescribeAclsOptions().Setters.InOrder);
        Assert.Equal(-1, DeleteAclsTimeoutMs(null));
        Assert.Equal(-1, DeleteAclsTimeoutMs(new DeleteAclsOptions()));
        Assert.Empty(new DeleteAclsOptions().Setters.InOrder);
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void AclOptions_TimeoutMs_ReachesTheCOptions_NullAsTheDefaultAndANegativeAsZero(int? timeoutMs, int expected)
    {
        Assert.Equal(expected, CreateAclsTimeoutMs(new CreateAclsOptions { TimeoutMs = timeoutMs }));
        Assert.Equal(expected, DescribeAclsTimeoutMs(new DescribeAclsOptions { TimeoutMs = timeoutMs }));
        Assert.Equal(expected, DeleteAclsTimeoutMs(new DeleteAclsOptions { TimeoutMs = timeoutMs }));
    }

    [Fact]
    public void AclOptions_ASetterRunTwice_SendsItsLastValue()
    {
        CreateAclsOptions create = new CreateAclsOptions { TimeoutMs = 10 };
        create.TimeoutMs = 20;
        Assert.Equal(20, CreateAclsTimeoutMs(create));
        Assert.Equal(new[] { nameof(CreateAclsOptions.TimeoutMs) }, create.Setters.InOrder);

        DescribeAclsOptions describe = new DescribeAclsOptions { TimeoutMs = 10 };
        describe.TimeoutMs = null;
        Assert.Equal(-1, DescribeAclsTimeoutMs(describe));

        DeleteAclsOptions delete = new DeleteAclsOptions { TimeoutMs = 10 };
        delete.TimeoutMs = 30;
        Assert.Equal(30, DeleteAclsTimeoutMs(delete));
    }

    [Fact]
    public void QuotaFilter_TheThreeFactories_CrossAsThreeDistinctMatches()
    {
        ClientQuotaFilter filter = ClientQuotaFilter.Contains(
            new[]
            {
                ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "alice"),
                ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.ClientId),
                ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.Ip),
            });
        WithQuotaFilter(filter, (components, strict) =>
        {
            Assert.False(strict);
            Assert.Equal(new[] { "user", "client-id", "ip" }, components.Select(ComponentEntityType));
            Assert.Equal(new[] { MatchExact, MatchDefault, MatchAny }, components.Select(MatchCode));

            Assert.Equal(Singleton(Native.ClientQuotaMatchDefault), Read(components[1], Native.ClientQuotaFilterComponentMatch));
            Assert.Equal(Singleton(Native.ClientQuotaMatchAny), Read(components[2], Native.ClientQuotaFilterComponentMatch));
            Assert.Equal(
                "ClientQuotaFilterComponent(entityType=user, match=Optional[alice])",
                OwnedString(components[0], Native.ClientQuotaFilterComponentToString));
        });
    }

    [Fact]
    public void QuotaFilter_AnExactEmptyName_IsAnExactMatchOfTheEmptyString()
    {
        ClientQuotaFilter filter = ClientQuotaFilter.Contains(
            new[]
            {
                ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, string.Empty),
                ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User),
            });
        WithQuotaFilter(filter, (components, _) =>
        {
            Assert.Equal(MatchExact, MatchCode(components[0]));
            Assert.Equal(
                "ClientQuotaFilterComponent(entityType=user, match=Optional[])",
                OwnedString(components[0], Native.ClientQuotaFilterComponentToString));
            Assert.Equal(MatchAny, MatchCode(components[1]));
        });
    }

    [Fact]
    public void QuotaFilter_All_IsZeroComponentsAndNotStrict() =>
        WithQuotaFilter(ClientQuotaFilter.All(), (components, strict) =>
        {
            Assert.Empty(components);
            Assert.False(strict);
        });

    [Fact]
    public void QuotaFilter_StrictIsForwardedBothWays()
    {
        ClientQuotaFilterComponent[] components =
            { ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User) };

        WithQuotaFilter(ClientQuotaFilter.ContainsOnly(components), (native, strict) =>
        {
            Assert.True(strict);
            Assert.Single(native);
        });
        WithQuotaFilter(ClientQuotaFilter.Contains(components), (native, strict) =>
        {
            Assert.False(strict);
            Assert.Single(native);
        });
    }

    [Fact]
    public void QuotaFilter_NonAsciiNameRoundTrips() =>
        WithQuotaFilter(
            ClientQuotaFilter.Contains(
                new[] { ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "用户-ü") }),
            (components, _) => Assert.Equal(
                "ClientQuotaFilterComponent(entityType=user, match=Optional[用户-ü])",
                OwnedString(components[0], Native.ClientQuotaFilterComponentToString)));

    [Fact]
    public void Alteration_ANullOpValueIsNaN_AndZeroIsZero() =>
        WithAlteration(
            new ClientQuotaAlteration(
                Entity(ClientQuotaEntity.User, "ternary"),
                new[]
                {
                    new ClientQuotaAlteration.Op("producer_byte_rate", null),
                    new ClientQuotaAlteration.Op("consumer_byte_rate", 0d),
                }),
            (_, ops) =>
            {
                Assert.Equal(new[] { "producer_byte_rate", "consumer_byte_rate" }, ops.Select(OpKey));
                Assert.True(double.IsNaN(OpValue(ops[0])));
                Assert.Equal(0d, OpValue(ops[1]));
            });

    [Fact]
    public void Alteration_TheEntityNameTernary_IsANullPointer_AnEmptyString_OrTheName()
    {
        WithAlteration(
            new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, null), s_noOps),
            (entity, _) =>
            {
                EntityEntry entry = Assert.Single(EntityEntries(entity));
                Assert.Equal("user", entry.Type);
                Assert.True(entry.NameIsNull);
                Assert.Null(entry.Name);
            });
        WithAlteration(
            new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, string.Empty), s_noOps),
            (entity, _) =>
            {
                EntityEntry entry = Assert.Single(EntityEntries(entity));
                Assert.False(entry.NameIsNull);
                Assert.Equal(string.Empty, entry.Name);
            });
        WithAlteration(
            new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, "x"), s_noOps),
            (entity, _) =>
            {
                EntityEntry entry = Assert.Single(EntityEntries(entity));
                Assert.False(entry.NameIsNull);
                Assert.Equal("x", entry.Name);
            });
    }

    [Fact]
    public void Alteration_CarriesEveryEntityPart_AndTheOpsInOrder()
    {
        ClientQuotaAlteration alteration = new ClientQuotaAlteration(
            new ClientQuotaEntity(
                new Dictionary<string, string?>(StringComparer.Ordinal)
                {
                    [ClientQuotaEntity.User] = "u",
                    [ClientQuotaEntity.ClientId] = "c",
                }),
            new[]
            {
                new ClientQuotaAlteration.Op("first", 1d),
                new ClientQuotaAlteration.Op("second", null),
                new ClientQuotaAlteration.Op("third", 3d),
            });
        WithAlteration(alteration, (entity, ops) =>
        {
            Assert.Equal(
                new[] { "client-id=c", "user=u" },
                EntityEntries(entity).Select(entry => entry.Type + "=" + entry.Name).OrderBy(pair => pair, StringComparer.Ordinal));
            Assert.Equal(alteration.Entity, ClientQuotaMarshal.CopyOutEntity(entity));
            Assert.Equal(new[] { "first", "second", "third" }, ops.Select(OpKey));
            Assert.Equal(1d, OpValue(ops[0]));
            Assert.True(double.IsNaN(OpValue(ops[1])));
            Assert.Equal(3d, OpValue(ops[2]));
        });
    }

    [Fact]
    public void Alteration_AnEmptyEntity_CrossesAsAnEntityWithNoParts() =>
        WithAlteration(
            new ClientQuotaAlteration(EmptyEntity(), new[] { new ClientQuotaAlteration.Op("producer_byte_rate", 1d) }),
            (entity, ops) =>
            {
                Assert.Empty(EntityEntries(entity));
                Assert.Equal("producer_byte_rate", OpKey(Assert.Single(ops)));
            });

    [Fact]
    public void Alteration_WithNoOps_CrossesAsAnEmptyOpList() =>
        WithAlteration(
            new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, "no-ops"), s_noOps),
            (entity, ops) =>
            {
                Assert.Empty(ops);
                Assert.Equal("no-ops", Assert.Single(EntityEntries(entity)).Name);
            });

    [Fact]
    public void Alteration_NonAsciiNameAndKeyRoundTrip() =>
        WithAlteration(
            new ClientQuotaAlteration(
                Entity(ClientQuotaEntity.User, "用户-ü"),
                new[] { new ClientQuotaAlteration.Op("配额-café", 7d) }),
            (entity, ops) =>
            {
                Assert.Equal("用户-ü", Assert.Single(EntityEntries(entity)).Name);
                Assert.Equal("配额-café", OpKey(Assert.Single(ops)));
                Assert.Equal(7d, OpValue(ops[0]));
            });

    [Fact]
    public void QuotaOptions_NoSetterRan_TheCOptionsCarryTheDefaults()
    {
        Assert.Equal(-1, DescribeClientQuotasTimeoutMs(null));
        Assert.Equal(-1, DescribeClientQuotasTimeoutMs(new DescribeClientQuotasOptions()));
        Assert.Empty(new DescribeClientQuotasOptions().Setters.InOrder);
        Assert.Equal(-1, AlterClientQuotasTimeoutMs(null));
        Assert.Equal(-1, AlterClientQuotasTimeoutMs(new AlterClientQuotasOptions()));
        Assert.Equal(0, AlterClientQuotasValidateOnly(null));
        Assert.Equal(0, AlterClientQuotasValidateOnly(new AlterClientQuotasOptions()));
        Assert.Empty(new AlterClientQuotasOptions().Setters.InOrder);
    }

    [Theory]
    [InlineData(4321, 4321)]
    [InlineData(0, 0)]
    [InlineData(null, -1)]
    [InlineData(-5, 0)]
    public void QuotaOptions_TimeoutMs_ReachesTheCOptions_NullAsTheDefaultAndANegativeAsZero(int? timeoutMs, int expected)
    {
        Assert.Equal(expected, DescribeClientQuotasTimeoutMs(new DescribeClientQuotasOptions { TimeoutMs = timeoutMs }));
        Assert.Equal(expected, AlterClientQuotasTimeoutMs(new AlterClientQuotasOptions { TimeoutMs = timeoutMs }));
    }

    [Fact]
    public void AlterClientQuotasOptions_ValidateOnly_ReachesTheCOptionsBothWays()
    {
        Assert.Equal(1, AlterClientQuotasValidateOnly(new AlterClientQuotasOptions { ValidateOnly = true }));

        AlterClientQuotasOptions cleared = new AlterClientQuotasOptions { ValidateOnly = true };
        cleared.ValidateOnly = false;
        Assert.Equal(0, AlterClientQuotasValidateOnly(cleared));
        Assert.Equal(new[] { nameof(AlterClientQuotasOptions.ValidateOnly) }, cleared.Setters.InOrder);
    }

    [Fact]
    public void QuotaOptions_ASetterRunTwice_SendsItsLastValue()
    {
        DescribeClientQuotasOptions describe = new DescribeClientQuotasOptions { TimeoutMs = 10 };
        describe.TimeoutMs = 20;
        Assert.Equal(20, DescribeClientQuotasTimeoutMs(describe));
        Assert.Equal(new[] { nameof(DescribeClientQuotasOptions.TimeoutMs) }, describe.Setters.InOrder);

        AlterClientQuotasOptions alter = new AlterClientQuotasOptions { TimeoutMs = 10, ValidateOnly = true };
        alter.TimeoutMs = null;
        Assert.Equal(-1, AlterClientQuotasTimeoutMs(alter));
        Assert.Equal(1, AlterClientQuotasValidateOnly(alter));
        Assert.Equal(
            new[] { nameof(AlterClientQuotasOptions.ValidateOnly), nameof(AlterClientQuotasOptions.TimeoutMs) },
            alter.Setters.InOrder);
    }

    private delegate IntPtr Getter(IntPtr self, ref IntPtr outErr);

    private delegate sbyte CodeGetter(IntPtr self, ref IntPtr outErr);

    private static TheoryData<T> EnumValues<T>()
        where T : struct, Enum
    {
        TheoryData<T> data = new TheoryData<T>();
        foreach (T value in Enum.GetValues(typeof(T)).Cast<T>())
        {
            data.Add(value);
        }

        return data;
    }

    private static AclBindingFilter Filter(
        ResourceType resourceType, PatternType patternType, AclOperation operation, AclPermissionType permissionType) =>
        new AclBindingFilter(
            new ResourcePatternFilter(resourceType, "n", patternType),
            new AccessControlEntryFilter("User:alice", "*", operation, permissionType));

    private static void WithFilter(AclBindingFilter filter, Action<IntPtr, IntPtr> read)
    {
        IntPtr native = AclMarshal.NewFilter(filter);
        try
        {
            read(
                Read(native, NativeMethods.AclBindingFilterPatternFilter),
                Read(native, NativeMethods.AclBindingFilterEntryFilter));
        }
        finally
        {
            AclMarshal.DestroyFilter(native);
        }
    }

    private static void WithBinding(AclBinding binding, Action<IntPtr, IntPtr> read)
    {
        IntPtr native = AclMarshal.NewBinding(binding);
        try
        {
            read(Read(native, NativeMethods.AclBindingPattern), Read(native, NativeMethods.AclBindingEntry));
        }
        finally
        {
            AclMarshal.DestroyBinding(native);
        }
    }

    private static IntPtr Read(IntPtr self, Getter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr value = getter(self, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static sbyte Code(IntPtr singleton, CodeGetter code)
    {
        IntPtr outErr = IntPtr.Zero;
        sbyte value = code(singleton, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static sbyte ResourceTypeCode(IntPtr self, Getter singleton) =>
        Code(Read(self, singleton), NativeMethods.ResourceTypeCode);

    private static sbyte PatternTypeCode(IntPtr self, Getter singleton) =>
        Code(Read(self, singleton), NativeMethods.PatternTypeCode);

    private static sbyte OperationCode(IntPtr self, Getter singleton) =>
        Code(Read(self, singleton), NativeMethods.AclOperationCode);

    private static sbyte PermissionTypeCode(IntPtr self, Getter singleton) =>
        Code(Read(self, singleton), NativeMethods.AclPermissionTypeCode);

    private static void AssertEmptyString(IntPtr value)
    {
        Assert.NotEqual(IntPtr.Zero, value);
        Assert.Equal(0, Marshal.ReadByte(value));
    }

    private static int CreateAclsTimeoutMs(CreateAclsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewCreateAclsOptions(options);
        try
        {
            return TimeoutMs(native, Native.CreateAclsOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeCreateAclsOptions(native);
        }
    }

    private static int DescribeAclsTimeoutMs(DescribeAclsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeAclsOptions(options);
        try
        {
            return TimeoutMs(native, Native.DescribeAclsOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeDescribeAclsOptions(native);
        }
    }

    private static int DeleteAclsTimeoutMs(DeleteAclsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDeleteAclsOptions(options);
        try
        {
            return TimeoutMs(native, Native.DeleteAclsOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeDeleteAclsOptions(native);
        }
    }

    private static int TimeoutMs(IntPtr native, Native.Int32Getter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        int timeoutMs = getter(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return timeoutMs;
    }

    private const int MatchDefault = 0;

    private const int MatchAny = 1;

    private const int MatchExact = 2;

    private static readonly ClientQuotaAlteration.Op[] s_noOps = Array.Empty<ClientQuotaAlteration.Op>();

    private static ClientQuotaEntity Entity(string type, string? name) =>
        new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal) { [type] = name });

    private static ClientQuotaEntity EmptyEntity() =>
        new ClientQuotaEntity(new Dictionary<string, string?>(StringComparer.Ordinal));

    private static void WithQuotaFilter(ClientQuotaFilter filter, Action<IReadOnlyList<IntPtr>, bool> read)
    {
        IntPtr native = ClientQuotaMarshal.NewFilter(filter);
        try
        {
            IntPtr components = Read(native, Native.ClientQuotaFilterComponents);
            try
            {
                read(Elements(components), Code(native, Native.ClientQuotaFilterStrict) != 0);
            }
            finally
            {
                DestroyList(components);
            }
        }
        finally
        {
            ClientQuotaMarshal.DestroyFilter(native);
        }
    }

    private static void WithAlteration(ClientQuotaAlteration alteration, Action<IntPtr, IReadOnlyList<IntPtr>> read)
    {
        IntPtr native = ClientQuotaMarshal.NewAlteration(alteration);
        try
        {
            IntPtr ops = Read(native, Native.ClientQuotaAlterationOps);
            try
            {
                read(Read(native, Native.ClientQuotaAlterationEntity), Elements(ops));
            }
            finally
            {
                DestroyList(ops);
            }
        }
        finally
        {
            ClientQuotaMarshal.DestroyAlteration(native);
        }
    }

    private static IReadOnlyList<IntPtr> Elements(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr[] elements = new IntPtr[count];
        for (int i = 0; i < count; i++)
        {
            elements[i] = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return elements;
    }

    private static void DestroyList(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ListDestroy(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private static string? ComponentEntityType(IntPtr component) =>
        Utf8Marshal.PtrToString(Read(component, Native.ClientQuotaFilterComponentEntityType));

    private static int MatchCode(IntPtr component)
    {
        IntPtr outErr = IntPtr.Zero;
        int code = Native.ClientQuotaMatchEnum(Read(component, Native.ClientQuotaFilterComponentMatch), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return code;
    }

    private static IntPtr Singleton(Native.SingletonGetter getter)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = getter(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Assert.NotEqual(IntPtr.Zero, singleton);
        return singleton;
    }

    private static string? OwnedString(IntPtr self, Getter getter)
    {
        IntPtr owned = Read(self, getter);
        try
        {
            return Utf8Marshal.PtrToString(owned);
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.StringDestroy(owned, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    private static string? OpKey(IntPtr op) => Utf8Marshal.PtrToString(Read(op, Native.ClientQuotaAlterationOpKey));

    private static double OpValue(IntPtr op)
    {
        IntPtr outErr = IntPtr.Zero;
        double value = Native.ClientQuotaAlterationOpValue(op, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return value;
    }

    private static IReadOnlyList<EntityEntry> EntityEntries(IntPtr entity)
    {
        IntPtr entries = Read(entity, NativeMethods.ClientQuotaEntityEntries);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int count = NativeMethods.MapSize(entries, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            EntityEntry[] read = new EntityEntry[count];
            for (int i = 0; i < count; i++)
            {
                IntPtr type = NativeMethods.MapKey(entries, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr name = NativeMethods.MapValue(entries, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                read[i] = new EntityEntry(Utf8Marshal.PtrToString(type), name == IntPtr.Zero, Utf8Marshal.PtrToString(name));
            }

            return read;
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MapDestroy(entries, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    private static int DescribeClientQuotasTimeoutMs(DescribeClientQuotasOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeClientQuotasOptions(options);
        try
        {
            return TimeoutMs(native, Native.DescribeClientQuotasOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeDescribeClientQuotasOptions(native);
        }
    }

    private static int AlterClientQuotasTimeoutMs(AlterClientQuotasOptions? options)
    {
        IntPtr native = NativeAdminClient.NewAlterClientQuotasOptions(options);
        try
        {
            return TimeoutMs(native, Native.AlterClientQuotasOptionsTimeoutMs);
        }
        finally
        {
            NativeAdminClient.FreeAlterClientQuotasOptions(native);
        }
    }

    private static sbyte AlterClientQuotasValidateOnly(AlterClientQuotasOptions? options)
    {
        IntPtr native = NativeAdminClient.NewAlterClientQuotasOptions(options);
        try
        {
            return Code(native, Native.AlterClientQuotasOptionsValidateOnly);
        }
        finally
        {
            NativeAdminClient.FreeAlterClientQuotasOptions(native);
        }
    }

    /// <summary>One (type, name) part of a C client-quota entity, read before its map is freed.</summary>
    private sealed class EntityEntry
    {
        internal EntityEntry(string? type, bool nameIsNull, string? name)
        {
            Type = type;
            NameIsNull = nameIsNull;
            Name = name;
        }

        internal string? Type { get; }

        internal bool NameIsNull { get; }

        internal string? Name { get; }
    }

    /// <summary>The C getters only these tests call, to read the C handles back.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        internal delegate int Int32Getter(IntPtr self, ref IntPtr outErr);

        internal delegate IntPtr SingletonGetter(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreateAclsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int CreateAclsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeAclsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeAclsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DeleteAclsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DeleteAclsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClientQuotasOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeClientQuotasOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterClientQuotasOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AlterClientQuotasOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterClientQuotasOptions_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte AlterClientQuotasOptionsValidateOnly(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilter_components_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClientQuotaFilterComponents(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilter_strict_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte ClientQuotaFilterStrict(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilterComponent_entity_type_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClientQuotaFilterComponentEntityType(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilterComponent_match_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClientQuotaFilterComponentMatch(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaFilterComponent_to_string_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClientQuotaFilterComponentToString(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaMatch__enum_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ClientQuotaMatchEnum(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaMatch_default_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClientQuotaMatchDefault(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaMatch_any_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClientQuotaMatchAny(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaAlteration_entity_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClientQuotaAlterationEntity(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaAlteration_ops_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClientQuotaAlterationOps(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaAlteration_Op_key_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ClientQuotaAlterationOpKey(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_quota_ClientQuotaAlteration_Op_value_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern double ClientQuotaAlterationOpValue(IntPtr self, ref IntPtr outErr);
    }
}
