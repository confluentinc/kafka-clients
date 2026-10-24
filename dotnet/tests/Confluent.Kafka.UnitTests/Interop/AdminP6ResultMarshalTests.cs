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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Copies ACL bindings and filters, and client-quota entities and their quota maps, out of real C handles through
/// <see cref="AclMarshal"/> and <see cref="ClientQuotaMarshal"/>.
/// </summary>
public sealed class AdminP6ResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    public static TheoryData<ResourceType> ResourceTypes => Values<ResourceType>();

    public static TheoryData<PatternType> PatternTypes => Values<PatternType>();

    public static TheoryData<AclOperation> Operations => Values<AclOperation>();

    public static TheoryData<AclPermissionType> PermissionTypes => Values<AclPermissionType>();

    [Fact]
    public void CopyOutFilter_KeepsNullApartFromEmpty()
    {
        AclBindingFilter nulls = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Literal),
            new AccessControlEntryFilter(null, null, AclOperation.Read, AclPermissionType.Allow));
        AclBindingFilter empties = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Literal),
            new AccessControlEntryFilter(string.Empty, string.Empty, AclOperation.Read, AclPermissionType.Allow));

        AclBindingFilter fromNulls = RoundTrip(nulls);
        AclBindingFilter fromEmpties = RoundTrip(empties);

        Assert.Null(fromNulls.PatternFilter.Name);
        Assert.Null(fromNulls.EntryFilter.Principal);
        Assert.Null(fromNulls.EntryFilter.Host);
        Assert.Equal(string.Empty, fromEmpties.PatternFilter.Name);
        Assert.Equal(string.Empty, fromEmpties.EntryFilter.Principal);
        Assert.Equal(string.Empty, fromEmpties.EntryFilter.Host);
        Assert.Equal(nulls, fromNulls);
        Assert.Equal(empties, fromEmpties);
        Assert.NotEqual(fromNulls, fromEmpties);
    }

    [Fact]
    public void CopyOutFilter_RestoresEverySlot()
    {
        AclBindingFilter filter = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Group, "grüppe-ü", PatternType.Prefixed),
            new AccessControlEntryFilter("User:bjørn", "10.0.0.1", AclOperation.Describe, AclPermissionType.Deny));

        AclBindingFilter copied = RoundTrip(filter);

        Assert.Equal(ResourceType.Group, copied.PatternFilter.ResourceType);
        Assert.Equal("grüppe-ü", copied.PatternFilter.Name);
        Assert.Equal(PatternType.Prefixed, copied.PatternFilter.PatternType);
        Assert.Equal("User:bjørn", copied.EntryFilter.Principal);
        Assert.Equal("10.0.0.1", copied.EntryFilter.Host);
        Assert.Equal(AclOperation.Describe, copied.EntryFilter.Operation);
        Assert.Equal(AclPermissionType.Deny, copied.EntryFilter.PermissionType);
        Assert.Equal(filter, copied);
    }

    [Theory]
    [MemberData(nameof(ResourceTypes))]
    public void CopyOutFilter_EveryResourceType_CrossesAsItself(ResourceType resourceType) =>
        Assert.Equal(
            resourceType,
            RoundTrip(Filter(resourceType, PatternType.Any, AclOperation.Any, AclPermissionType.Any))
                .PatternFilter.ResourceType);

    [Theory]
    [MemberData(nameof(PatternTypes))]
    public void CopyOutFilter_EveryPatternType_CrossesAsItself(PatternType patternType) =>
        Assert.Equal(
            patternType,
            RoundTrip(Filter(ResourceType.Any, patternType, AclOperation.Any, AclPermissionType.Any))
                .PatternFilter.PatternType);

    [Theory]
    [MemberData(nameof(Operations))]
    public void CopyOutFilter_EveryOperation_CrossesAsItself(AclOperation operation) =>
        Assert.Equal(
            operation,
            RoundTrip(Filter(ResourceType.Any, PatternType.Any, operation, AclPermissionType.Any))
                .EntryFilter.Operation);

    [Theory]
    [MemberData(nameof(PermissionTypes))]
    public void CopyOutFilter_EveryPermissionType_CrossesAsItself(AclPermissionType permissionType) =>
        Assert.Equal(
            permissionType,
            RoundTrip(Filter(ResourceType.Any, PatternType.Any, AclOperation.Any, permissionType))
                .EntryFilter.PermissionType);

    [Fact]
    public void CopyOutBinding_RestoresEverySlot()
    {
        AclBinding binding = new AclBinding(
            new ResourcePattern(ResourceType.TransactionalId, "tx-ünï", PatternType.Literal),
            new AccessControlEntry("User:ålice", "*", AclOperation.Write, AclPermissionType.Allow));

        AclBinding copied = RoundTrip(binding);

        Assert.Equal(ResourceType.TransactionalId, copied.Pattern.ResourceType);
        Assert.Equal("tx-ünï", copied.Pattern.Name);
        Assert.Equal(PatternType.Literal, copied.Pattern.PatternType);
        Assert.Equal("User:ålice", copied.Entry.Principal);
        Assert.Equal("*", copied.Entry.Host);
        Assert.Equal(AclOperation.Write, copied.Entry.Operation);
        Assert.Equal(AclPermissionType.Allow, copied.Entry.PermissionType);
        Assert.Equal(binding, copied);
    }

    [Fact]
    public void CopyOutBindings_KeepsTheListOrder_AndOutlivesTheList()
    {
        AclBinding[] bindings = { Binding("charlie"), Binding("alpha"), Binding("bravo") };

        IReadOnlyCollection<AclBinding> copied = CopyOutList(bindings);

        Assert.Equal(bindings, copied);
    }

    [Fact]
    public void CopyOutBindings_AnEmptyList_IsAnEmptyCollection() => Assert.Empty(CopyOutList());

    [Fact]
    public void CopyOutEntity_KeepsNullApartFromEmptyAndFromAName()
    {
        ClientQuotaEntity fromNull = RoundTrip(Entity(("user", null)));
        ClientQuotaEntity fromEmpty = RoundTrip(Entity(("user", string.Empty)));
        ClientQuotaEntity fromReal = RoundTrip(Entity(("user", "x")));

        Assert.Null(Assert.Single(fromNull.Entries).Value);
        Assert.Equal(string.Empty, Assert.Single(fromEmpty.Entries).Value);
        Assert.Equal("x", Assert.Single(fromReal.Entries).Value);
        Assert.Equal(3, new HashSet<ClientQuotaEntity> { fromNull, fromEmpty, fromReal }.Count);
    }

    [Fact]
    public void CopyOutEntity_RestoresEveryEntry()
    {
        ClientQuotaEntity entity = Entity(("client-id", "svc-ü"), ("user", null), ("ip", "10.0.0.1"));

        ClientQuotaEntity copied = RoundTrip(entity);

        Assert.Equal(3, copied.Entries.Count);
        Assert.Equal("svc-ü", copied.Entries["client-id"]);
        Assert.Null(copied.Entries["user"]);
        Assert.Equal("10.0.0.1", copied.Entries["ip"]);
        Assert.Equal(entity, copied);
    }

    [Fact]
    public void CopyOutEntity_AnEmptyEntity_HasNoEntries() => Assert.Empty(RoundTrip(Entity()).Entries);

    [Fact]
    public void CopyOutEntities_ZeroAndNegativeValues_AreValues()
    {
        using QuotaMapFixture fixture = new QuotaMapFixture();
        ClientQuotaEntity key = fixture.Add(
            Entity(("user", "alice")),
            ("producer_byte_rate", 0.0),
            ("consumer_byte_rate", -1.0),
            ("request_percentage", double.MaxValue));

        IReadOnlyDictionary<string, double> quotas = ClientQuotaMarshal.CopyOutEntities(fixture.Map)[key];

        Assert.Equal(3, quotas.Count);
        Assert.Equal(0.0, quotas["producer_byte_rate"]);
        Assert.Equal(-1.0, quotas["consumer_byte_rate"]);
        Assert.Equal(double.MaxValue, quotas["request_percentage"]);
    }

    [Fact]
    public void CopyOutEntities_AnEntityWithNoQuotas_IsAnEmptyMap()
    {
        using QuotaMapFixture fixture = new QuotaMapFixture();
        ClientQuotaEntity key = fixture.Add(Entity(("ip", "10.0.0.1")));

        Assert.Empty(ClientQuotaMarshal.CopyOutEntities(fixture.Map)[key]);
    }

    [Fact]
    public void CopyOutEntities_EntitiesAreKeyedByValue_NotByReference()
    {
        using QuotaMapFixture fixture = new QuotaMapFixture();
        ClientQuotaEntity added = fixture.Add(Entity(("user", null), ("client-id", "svc")), ("producer_byte_rate", 1024.0));

        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
            ClientQuotaMarshal.CopyOutEntities(fixture.Map);

        ClientQuotaEntity lookup = Entity(("client-id", "svc"), ("user", null));
        Assert.NotSame(lookup, added);
        Assert.True(entities.ContainsKey(lookup));
        Assert.Equal(1024.0, entities[lookup]["producer_byte_rate"]);
    }

    [Fact]
    public void CopyOutEntities_BuildsOneInnerMapPerEntity()
    {
        using QuotaMapFixture fixture = new QuotaMapFixture();
        ClientQuotaEntity alice = fixture.Add(Entity(("user", "alice")), ("producer_byte_rate", 1.0));
        ClientQuotaEntity theDefault = fixture.Add(Entity(("user", null)), ("producer_byte_rate", 2.0));

        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
            ClientQuotaMarshal.CopyOutEntities(fixture.Map);

        Assert.Equal(2, entities.Count);
        Assert.Equal(1.0, entities[alice]["producer_byte_rate"]);
        Assert.Equal(2.0, entities[theDefault]["producer_byte_rate"]);
    }

    [Fact]
    public void CopyOutEntities_ANullMap_IsEmpty() => Assert.Empty(ClientQuotaMarshal.CopyOutEntities(IntPtr.Zero));

    [Fact]
    public async Task DescribeClientQuotas_OnTheCoreMock_FailsTheOneFuture()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeClientQuotasResult result =
            AsyncDescribeClientQuotasResult.OfClient(admin.DescribeClientQuotas(ClientQuotaFilter.All(), options: null));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Entities().Get(), s_deadline));
        Assert.Equal(35, failure.Code);
        Assert.Equal("Not implement yet", failure.Message);
    }

    private static ClientQuotaEntity Entity(params (string Type, string? Name)[] entries)
    {
        Dictionary<string, string?> map = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach ((string type, string? name) in entries)
        {
            map[type] = name;
        }

        return new ClientQuotaEntity(map);
    }

    private static ClientQuotaEntity RoundTrip(ClientQuotaEntity entity)
    {
        IntPtr native = ClientQuotaMarshal.NewEntity(entity);
        try
        {
            return ClientQuotaMarshal.CopyOutEntity(native);
        }
        finally
        {
            ClientQuotaMarshal.DestroyEntity(native);
        }
    }

    /// <summary>
    /// A <c>kafka_Map_t</c> of C entities to <c>kafka_Map_t</c>s of quota names to <c>double*</c>, the shape the
    /// <c>describeClientQuotas</c> future delivers; it owns and frees everything it built.
    /// </summary>
    private sealed class QuotaMapFixture : IDisposable
    {
        private readonly List<IntPtr> _entities = new List<IntPtr>();
        private readonly List<IntPtr> _innerMaps = new List<IntPtr>();
        private readonly List<IntPtr> _values = new List<IntPtr>();
        private readonly List<Utf8Marshal.PinnedUtf8String> _pins = new List<Utf8Marshal.PinnedUtf8String>();

        internal QuotaMapFixture()
        {
            Map = NewMap();
        }

        internal IntPtr Map { get; }

        internal ClientQuotaEntity Add(ClientQuotaEntity entity, params (string Key, double Value)[] quotas)
        {
            IntPtr native = ClientQuotaMarshal.NewEntity(entity);
            _entities.Add(native);
            IntPtr inner = NewMap();
            _innerMaps.Add(inner);
            foreach ((string key, double value) in quotas)
            {
                Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(key);
                _pins.Add(pinned);
                IntPtr boxed = Marshal.AllocHGlobal(sizeof(double));
                _values.Add(boxed);
                Marshal.WriteInt64(boxed, BitConverter.DoubleToInt64Bits(value));
                Put(inner, pinned.Pointer, boxed);
            }

            Put(Map, native, inner);
            return entity;
        }

        public void Dispose()
        {
            Destroy(Map);
            foreach (IntPtr inner in _innerMaps)
            {
                Destroy(inner);
            }

            foreach (IntPtr entity in _entities)
            {
                ClientQuotaMarshal.DestroyEntity(entity);
            }

            foreach (IntPtr value in _values)
            {
                Marshal.FreeHGlobal(value);
            }

            foreach (Utf8Marshal.PinnedUtf8String pin in _pins)
            {
                pin.Dispose();
            }
        }

        private static IntPtr NewMap()
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return map;
        }

        private static void Put(IntPtr map, IntPtr key, IntPtr value)
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MapPut(map, key, value, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        private static void Destroy(IntPtr map)
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MapDestroy(map, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }
    }

    private static TheoryData<T> Values<T>()
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
            new ResourcePatternFilter(resourceType, "name", patternType),
            new AccessControlEntryFilter("User:alice", "*", operation, permissionType));

    private static AclBinding Binding(string name) =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

    private static AclBindingFilter RoundTrip(AclBindingFilter filter)
    {
        IntPtr native = AclMarshal.NewFilter(filter);
        try
        {
            return AclMarshal.CopyOutFilter(native);
        }
        finally
        {
            AclMarshal.DestroyFilter(native);
        }
    }

    private static AclBinding RoundTrip(AclBinding binding)
    {
        IntPtr native = AclMarshal.NewBinding(binding);
        try
        {
            return AclMarshal.CopyOutBinding(native);
        }
        finally
        {
            AclMarshal.DestroyBinding(native);
        }
    }

    private static IReadOnlyCollection<AclBinding> CopyOutList(params AclBinding[] bindings)
    {
        IntPtr[] natives = new IntPtr[bindings.Length];
        IntPtr list = IntPtr.Zero;
        IntPtr outErr = IntPtr.Zero;
        try
        {
            for (int i = 0; i < natives.Length; i++)
            {
                natives[i] = AclMarshal.NewBinding(bindings[i]);
            }

            list = NativeMethods.ListNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (IntPtr native in natives)
            {
                NativeMethods.ListAdd(list, native, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return AclMarshal.CopyOutBindings(list);
        }
        finally
        {
            NativeMethods.ListDestroy(list, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
            foreach (IntPtr native in natives)
            {
                AclMarshal.DestroyBinding(native);
            }
        }
    }
}
