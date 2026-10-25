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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The SCRAM description readers over real C handles, and the token MAC and feature readers over injected accessors,
/// for the shapes the mock admin client cannot produce: a MAC carrying an interior zero byte, and two feature tables
/// that differ in size and key set.
/// </summary>
public sealed class AdminP7ResultMarshalTests
{
    private const sbyte ScramSha256 = 1;

    private const sbyte ScramSha512 = 2;

    [Fact]
    public void CopyOutDescription_WalksEachDescriptionsOwnCredentials()
    {
        using NativeScramDescriptions native = new NativeScramDescriptions();
        IntPtr empty = native.Description("empty");
        IntPtr alice = native.Description("alice", (ScramSha256, 4096), (ScramSha512, 8192), (ScramSha256, 16384));

        UserScramCredentialsDescription emptyCopy = UserScramCredentialMarshal.CopyOutDescription(empty);
        UserScramCredentialsDescription aliceCopy = UserScramCredentialMarshal.CopyOutDescription(alice);

        Assert.Equal("empty", emptyCopy.Name);
        Assert.Empty(emptyCopy.CredentialInfos);
        Assert.Equal("alice", aliceCopy.Name);
        Assert.Equal(
            new[] { ScramMechanism.ScramSha256, ScramMechanism.ScramSha512, ScramMechanism.ScramSha256 },
            aliceCopy.CredentialInfos.Select(info => info.Mechanism));
        Assert.Equal(new[] { 4096, 8192, 16384 }, aliceCopy.CredentialInfos.Select(info => info.Iterations));
    }

    [Fact]
    public void CopyOutDescription_LeavesTheDescriptionItReads()
    {
        using NativeScramDescriptions native = new NativeScramDescriptions();
        IntPtr alice = native.Description("alice", (ScramSha512, 8192));

        UserScramCredentialsDescription first = UserScramCredentialMarshal.CopyOutDescription(alice);
        UserScramCredentialsDescription second = UserScramCredentialMarshal.CopyOutDescription(alice);

        Assert.Equal(first, second);
        Assert.Equal(ScramMechanism.ScramSha512, second.CredentialInfos.Single().Mechanism);
    }

    [Fact]
    public void CopyOutDescription_AMechanismJavaDoesNotKnow_IsUnknown()
    {
        using NativeScramDescriptions native = new NativeScramDescriptions();
        IntPtr user = native.Description("u", (99, 4096));

        ScramCredentialInfo info = UserScramCredentialMarshal.CopyOutDescription(user).CredentialInfos.Single();

        Assert.Equal(ScramMechanism.Unknown, info.Mechanism);
        Assert.Equal(4096, info.Iterations);
    }

    [Fact]
    public void CopyOutDescription_ANonAsciiName_RoundTrips()
    {
        using NativeScramDescriptions native = new NativeScramDescriptions();

        Assert.Equal(
            "ålice-ü-世",
            UserScramCredentialMarshal.CopyOutDescription(native.Description("ålice-ü-世")).Name);
    }

    [Fact]
    public void CopyOutDescriptions_KeysEachDescriptionByItsUser()
    {
        using NativeScramDescriptions native = new NativeScramDescriptions();
        IntPtr map = native.Map(
            ("alice", native.Description("alice", (ScramSha256, 4096))),
            ("bob", native.Description("bob", (ScramSha512, 8192), (ScramSha256, 4096))));

        IReadOnlyDictionary<string, UserScramCredentialsDescription> copied =
            UserScramCredentialMarshal.CopyOutDescriptions(map);

        Assert.Equal(new[] { "alice", "bob" }, copied.Keys.OrderBy(key => key, StringComparer.Ordinal));
        Assert.Equal("alice", copied["alice"].Name);
        Assert.Equal(4096, copied["alice"].CredentialInfos.Single().Iterations);
        Assert.Equal("bob", copied["bob"].Name);
        Assert.Equal(
            new[] { ScramMechanism.ScramSha512, ScramMechanism.ScramSha256 },
            copied["bob"].CredentialInfos.Select(info => info.Mechanism));
        Assert.False(copied.ContainsKey("Alice"));
    }

    [Fact]
    public void CopyOutDescriptions_AnEmptyMap_IsEmpty()
    {
        using NativeScramDescriptions native = new NativeScramDescriptions();

        Assert.Empty(UserScramCredentialMarshal.CopyOutDescriptions(native.Map()));
    }

    [Fact]
    public void CopyOutDescriptions_ANullMap_IsEmpty() =>
        Assert.Empty(UserScramCredentialMarshal.CopyOutDescriptions(IntPtr.Zero));

    [Fact]
    public void CopyOutUsers_KeepsTheListOrder()
    {
        using NativeScramDescriptions native = new NativeScramDescriptions();

        Assert.Equal(
            new[] { "bob", "alice", "ålice" },
            UserScramCredentialMarshal.CopyOutUsers(native.Users("bob", "alice", "ålice")));
    }

    [Fact]
    public void CopyOutUsers_AnEmptyList_IsEmpty()
    {
        using NativeScramDescriptions native = new NativeScramDescriptions();

        Assert.Empty(UserScramCredentialMarshal.CopyOutUsers(native.Users()));
    }

    [Fact]
    public void CopyOutUsers_ANullList_IsEmpty() => Assert.Empty(UserScramCredentialMarshal.CopyOutUsers(IntPtr.Zero));

    [Fact]
    public void CopyOutToken_KeepsAMacWithAnInteriorZeroByte()
    {
        byte[] mac = { 0x7F, 0x00, 0x00, 0xA1, 0x00, 0xFE };
        using NativeTokens natives = new NativeTokens();

        DelegationToken token = DelegationTokenMarshal.CopyOutToken(natives.Token("token-1", mac));

        Assert.Equal(mac, token.Hmac);
    }

    [Fact]
    public void CopyOutToken_KeepsAnAllZeroMacAtFullLength()
    {
        using NativeTokens natives = new NativeTokens();

        byte[] hmac = DelegationTokenMarshal.CopyOutToken(natives.Token("token-1", new byte[6])).Hmac;

        Assert.Equal(6, hmac.Length);
        Assert.All(hmac, value => Assert.Equal(0, value));
    }

    [Theory]
    [InlineData(true, 8)]
    [InlineData(false, 0)]
    [InlineData(false, -1)]
    public void CopyBytes_AbsentIsEmpty_NotNull(bool nullPointer, int length)
    {
        GCHandle pin = GCHandle.Alloc(new byte[] { 1, 2, 3, 4 }, GCHandleType.Pinned);
        try
        {
            byte[] copied = DelegationTokenMarshal.CopyBytes(
                new KafkaBytes(nullPointer ? IntPtr.Zero : pin.AddrOfPinnedObject(), length));

            Assert.NotNull(copied);
            Assert.Empty(copied);
        }
        finally
        {
            pin.Free();
        }
    }

    [Fact]
    public void CopyOutToken_CopiesEveryField()
    {
        using NativeTokens natives = new NativeTokens();
        IntPtr native = natives.Token(
            "token-ü",
            new byte[] { 1, 2, 3 },
            new KafkaPrincipal("User", "ownér"),
            requester: null,
            renewers: new[] { new KafkaPrincipal("User", "bob"), new KafkaPrincipal("Group", "ops") },
            issueTimestamp: 11,
            maxTimestamp: 33,
            expiryTimestamp: 22);

        DelegationToken token = DelegationTokenMarshal.CopyOutToken(native);

        TokenInformation info = token.TokenInfo;
        Assert.Equal("token-ü", info.TokenId);
        Assert.Equal(new KafkaPrincipal("User", "ownér"), info.Owner);
        Assert.Equal(new KafkaPrincipal("User", "ownér"), info.TokenRequester);
        Assert.Equal(
            new[] { new KafkaPrincipal("User", "bob"), new KafkaPrincipal("Group", "ops") },
            info.Renewers);
        Assert.Equal(11, info.IssueTimestamp);
        Assert.Equal(33, info.MaxTimestamp);
        Assert.Equal(22, info.ExpiryTimestamp);
        Assert.Equal(new byte[] { 1, 2, 3 }, token.Hmac);
    }

    [Fact]
    public void CopyOutToken_KeepsARequesterThatIsNotTheOwner()
    {
        using NativeTokens natives = new NativeTokens();
        IntPtr native = natives.Token(
            "token-1",
            new byte[] { 9 },
            new KafkaPrincipal("User", "alice"),
            requester: new KafkaPrincipal("User", "admin"),
            renewers: Array.Empty<KafkaPrincipal>(),
            issueTimestamp: 1,
            maxTimestamp: 3,
            expiryTimestamp: 2);

        TokenInformation info = DelegationTokenMarshal.CopyOutToken(native).TokenInfo;

        Assert.Equal(new KafkaPrincipal("User", "alice"), info.Owner);
        Assert.Equal(new KafkaPrincipal("User", "admin"), info.TokenRequester);
        Assert.Empty(info.Renewers);
    }

    [Fact]
    public void CopyOutTokens_KeepsTheListOrder()
    {
        using NativeTokens natives = new NativeTokens();
        IntPtr list = natives.List(natives.Token("b", new byte[] { 2 }), natives.Token("a", new byte[] { 1 }));

        IReadOnlyList<DelegationToken> tokens = DelegationTokenMarshal.CopyOutTokens(list);

        Assert.Equal(new[] { "b", "a" }, tokens.Select(token => token.TokenInfo.TokenId));
    }

    [Fact]
    public void CopyOutTokens_ANullList_IsEmpty() => Assert.Empty(DelegationTokenMarshal.CopyOutTokens(IntPtr.Zero));

    [Fact]
    public void CopyOutToken_NoToken_ThrowsKafkaException()
    {
        KafkaException exception = Assert.Throws<KafkaException>(() => DelegationTokenMarshal.CopyOutToken(IntPtr.Zero));

        Assert.Equal("The admin result produced no delegation token.", exception.Message);
    }

    [Fact]
    public void CopyOutFinalized_CopiesEachRangeUnderItsName()
    {
        using NativeFeatureRanges ranges = new NativeFeatureRanges();
        IntPtr map = ranges.Finalized(("metadata.version", 3, 7), ("group.version", 0, 1));

        Dictionary<string, FinalizedVersionRange> copied = FeatureMetadataMarshal.CopyOutFinalized(map);

        Assert.Equal(new[] { "group.version", "metadata.version" }, copied.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(3, copied["metadata.version"].MinVersionLevel);
        Assert.Equal(7, copied["metadata.version"].MaxVersionLevel);
        Assert.Equal(0, copied["group.version"].MinVersionLevel);
        Assert.Equal(1, copied["group.version"].MaxVersionLevel);
    }

    [Fact]
    public void CopyOutSupported_CopiesEachRangeUnderItsName()
    {
        using NativeFeatureRanges ranges = new NativeFeatureRanges();
        IntPtr map = ranges.Supported(("metadata.version", 1, 9), ("group.version", 0, 2), ("transaction.version", 0, 1));

        Dictionary<string, SupportedVersionRange> copied = FeatureMetadataMarshal.CopyOutSupported(map);

        Assert.Equal(
            new[] { "group.version", "metadata.version", "transaction.version" },
            copied.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(1, copied["metadata.version"].MinVersion);
        Assert.Equal(9, copied["metadata.version"].MaxVersion);
        Assert.Equal(2, copied["group.version"].MaxVersion);
    }

    [Fact]
    public void CopyOutFeatures_ANonAsciiName_RoundTrips()
    {
        using NativeFeatureRanges ranges = new NativeFeatureRanges();

        Assert.Equal(
            new[] { "fé✓ture" },
            FeatureMetadataMarshal.CopyOutFinalized(ranges.Finalized(("fé✓ture", 1, 2))).Keys);
        Assert.Equal(
            new[] { "fé✓ture" },
            FeatureMetadataMarshal.CopyOutSupported(ranges.Supported(("fé✓ture", 1, 2))).Keys);
    }

    [Fact]
    public void CopyOutFeatures_AnEmptyMap_IsEmpty()
    {
        using NativeFeatureRanges ranges = new NativeFeatureRanges();

        Assert.Empty(FeatureMetadataMarshal.CopyOutFinalized(ranges.Finalized()));
        Assert.Empty(FeatureMetadataMarshal.CopyOutSupported(ranges.Supported()));
    }

    [Fact]
    public void CopyOut_NoMetadata_ThrowsKafkaException()
    {
        KafkaException thrown = Assert.Throws<KafkaException>(() => FeatureMetadataMarshal.CopyOut(IntPtr.Zero));

        Assert.Equal("The describeFeatures result produced no feature metadata.", thrown.Message);
    }

    [Theory]
    [InlineData(0L, 0L)]
    [InlineData(4242L, 4242L)]
    [InlineData(long.MaxValue, long.MaxValue)]
    [InlineData(-1L, null)]
    [InlineData(-2L, null)]
    [InlineData(long.MinValue, null)]
    public void Epoch_ANegativeValueIsAbsent(long native, long? expected) =>
        Assert.Equal(expected, FeatureMetadataMarshal.ToEpoch(native));

    /// <summary>
    /// Real <c>UserScramCredentialsDescription_t</c>s, and the maps and lists of borrowed pointers the readers walk,
    /// each freed once at <see cref="Dispose"/>.
    /// </summary>
    private sealed class NativeScramDescriptions : IDisposable
    {
        private readonly List<IntPtr> _descriptions = new List<IntPtr>();
        private readonly List<IntPtr> _maps = new List<IntPtr>();
        private readonly List<IntPtr> _lists = new List<IntPtr>();
        private readonly List<Utf8Marshal.PinnedUtf8String> _pins = new List<Utf8Marshal.PinnedUtf8String>();

        internal IntPtr Description(string name, params (sbyte Type, int Iterations)[] credentials)
        {
            IntPtr[] infos = new IntPtr[credentials.Length];
            IntPtr list = IntPtr.Zero;
            IntPtr outErr = IntPtr.Zero;
            try
            {
                for (int i = 0; i < infos.Length; i++)
                {
                    IntPtr mechanism = NativeMethods.ScramMechanismFromType(credentials[i].Type, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    infos[i] = NativeMethods.ScramCredentialInfoNew(mechanism, credentials[i].Iterations, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                }

                list = NewList(infos);
                using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(name);
                IntPtr description = Native.UserScramCredentialsDescriptionNew(pinned.Pointer, list, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                _descriptions.Add(description);
                return description;
            }
            finally
            {
                NativeMethods.ListDestroy(list, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
                foreach (IntPtr info in infos)
                {
                    NativeMethods.ScramCredentialInfoDestroy(info, ref outErr);
                    Assert.Equal(IntPtr.Zero, outErr);
                }
            }
        }

        internal IntPtr Map(params (string User, IntPtr Description)[] entries)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            _maps.Add(map);
            foreach ((string user, IntPtr description) in entries)
            {
                NativeMethods.MapPut(map, Pin(user), description, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return map;
        }

        internal IntPtr Users(params string[] users)
        {
            IntPtr list = NewList(users.Select(Pin).ToArray());
            _lists.Add(list);
            return list;
        }

        public void Dispose()
        {
            IntPtr outErr = IntPtr.Zero;
            foreach (IntPtr map in _maps)
            {
                NativeMethods.MapDestroy(map, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr list in _lists)
            {
                NativeMethods.ListDestroy(list, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr description in _descriptions)
            {
                Native.UserScramCredentialsDescriptionDestroy(description, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (Utf8Marshal.PinnedUtf8String pin in _pins)
            {
                pin.Dispose();
            }
        }

        private static IntPtr NewList(IntPtr[] elements)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr list = NativeMethods.ListNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (IntPtr element in elements)
            {
                NativeMethods.ListAdd(list, element, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return list;
        }

        private IntPtr Pin(string value)
        {
            Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(value);
            _pins.Add(pinned);
            return pinned.Pointer;
        }
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialsDescription_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr UserScramCredentialsDescriptionNew(IntPtr name, IntPtr credentialInfos, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_UserScramCredentialsDescription_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void UserScramCredentialsDescriptionDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TokenInformationNew(
            IntPtr tokenId, IntPtr owner, IntPtr renewers, long issueTimestamp, long maxTimestamp, long expiryTimestamp, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_with_token_requester_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TokenInformationWithTokenRequester(
            IntPtr tokenId,
            IntPtr owner,
            IntPtr tokenRequester,
            IntPtr renewers,
            long issueTimestamp,
            long maxTimestamp,
            long expiryTimestamp,
            ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_TokenInformation_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void TokenInformationDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_DelegationToken_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr DelegationTokenNew(IntPtr tokenInformation, KafkaBytes hmac, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_security_token_delegation_DelegationToken_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void DelegationTokenDestroy(IntPtr self, ref IntPtr outErr);

        internal delegate IntPtr RangeNew(short min, short max, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_FinalizedVersionRange_new", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr FinalizedVersionRangeNew(short minVersionLevel, short maxVersionLevel, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_FinalizedVersionRange_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void FinalizedVersionRangeDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_SupportedVersionRange_new", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SupportedVersionRangeNew(short minVersion, short maxVersion, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_SupportedVersionRange_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void SupportedVersionRangeDestroy(IntPtr self, ref IntPtr outErr);
    }

    /// <summary>
    /// Real <c>DelegationToken_t</c>s and lists of them, built through the core's constructors, each freed once at
    /// <see cref="Dispose"/>.
    /// </summary>
    private sealed class NativeTokens : IDisposable
    {
        private readonly List<IntPtr> _tokens = new List<IntPtr>();
        private readonly List<IntPtr> _lists = new List<IntPtr>();

        internal IntPtr Token(string tokenId, byte[] hmac) =>
            Token(tokenId, hmac, new KafkaPrincipal("User", "alice"), null, Array.Empty<KafkaPrincipal>(), 1, 3, 2);

        internal IntPtr Token(
            string tokenId,
            byte[] hmac,
            KafkaPrincipal owner,
            KafkaPrincipal? requester,
            KafkaPrincipal[] renewers,
            long issueTimestamp,
            long maxTimestamp,
            long expiryTimestamp)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr ownerHandle = DelegationTokenMarshal.NewPrincipal(owner);
            IntPtr requesterHandle = requester is null ? IntPtr.Zero : DelegationTokenMarshal.NewPrincipal(requester);
            IntPtr[] renewerHandles = DelegationTokenMarshal.NewPrincipals(renewers);
            IntPtr renewerList = NewList(renewerHandles);
            IntPtr info = IntPtr.Zero;
            try
            {
                using Utf8Marshal.PinnedUtf8String id = Utf8Marshal.Pin(tokenId);
                info = requester is null
                    ? Native.TokenInformationNew(
                        id.Pointer, ownerHandle, renewerList, issueTimestamp, maxTimestamp, expiryTimestamp, ref outErr)
                    : Native.TokenInformationWithTokenRequester(
                        id.Pointer,
                        ownerHandle,
                        requesterHandle,
                        renewerList,
                        issueTimestamp,
                        maxTimestamp,
                        expiryTimestamp,
                        ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr token = DelegationTokenMarshal.WithHmac(
                    hmac,
                    bytes =>
                    {
                        IntPtr err = IntPtr.Zero;
                        IntPtr created = Native.DelegationTokenNew(info, bytes, ref err);
                        KafkaException.ThrowIfFfiFailed(ref err);
                        return created;
                    });
                _tokens.Add(token);
                return token;
            }
            finally
            {
                if (info != IntPtr.Zero)
                {
                    Native.TokenInformationDestroy(info, ref outErr);
                    Assert.Equal(IntPtr.Zero, outErr);
                }

                NativeMethods.ListDestroy(renewerList, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
                DelegationTokenMarshal.DestroyPrincipals(renewerHandles);
                DelegationTokenMarshal.DestroyPrincipal(requesterHandle);
                DelegationTokenMarshal.DestroyPrincipal(ownerHandle);
            }
        }

        internal IntPtr List(params IntPtr[] tokens)
        {
            IntPtr list = NewList(tokens);
            _lists.Add(list);
            return list;
        }

        public void Dispose()
        {
            IntPtr outErr = IntPtr.Zero;
            foreach (IntPtr list in _lists)
            {
                NativeMethods.ListDestroy(list, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr token in _tokens)
            {
                Native.DelegationTokenDestroy(token, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }
        }

        private static IntPtr NewList(IntPtr[] elements)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr list = NativeMethods.ListNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (IntPtr element in elements)
            {
                NativeMethods.ListAdd(list, element, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return list;
        }
    }

    /// <summary>
    /// Maps of feature names to real <c>FinalizedVersionRange_t</c> / <c>SupportedVersionRange_t</c>s, built
    /// through the core's constructors. A map the binding builds does not own what it holds, so each range,
    /// map and name is freed once at <see cref="Dispose"/>.
    /// </summary>
    private sealed class NativeFeatureRanges : IDisposable
    {
        private readonly List<IntPtr> _finalized = new List<IntPtr>();
        private readonly List<IntPtr> _supported = new List<IntPtr>();
        private readonly List<IntPtr> _maps = new List<IntPtr>();
        private readonly List<Utf8Marshal.PinnedUtf8String> _names = new List<Utf8Marshal.PinnedUtf8String>();

        internal IntPtr Finalized(params (string Name, short Min, short Max)[] features) =>
            Map(features, _finalized, Native.FinalizedVersionRangeNew);

        internal IntPtr Supported(params (string Name, short Min, short Max)[] features) =>
            Map(features, _supported, Native.SupportedVersionRangeNew);

        public void Dispose()
        {
            IntPtr outErr = IntPtr.Zero;
            foreach (IntPtr map in _maps)
            {
                NativeMethods.MapDestroy(map, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr range in _finalized)
            {
                Native.FinalizedVersionRangeDestroy(range, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (IntPtr range in _supported)
            {
                Native.SupportedVersionRangeDestroy(range, ref outErr);
                Assert.Equal(IntPtr.Zero, outErr);
            }

            foreach (Utf8Marshal.PinnedUtf8String name in _names)
            {
                name.Dispose();
            }
        }

        private IntPtr Map(
            (string Name, short Min, short Max)[] features, List<IntPtr> ranges, Native.RangeNew newRange)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            _maps.Add(map);
            foreach ((string name, short min, short max) in features)
            {
                Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(name);
                _names.Add(pinned);
                IntPtr range = newRange(min, max, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                ranges.Add(range);
                NativeMethods.MapPut(map, pinned.Pointer, range, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return map;
        }
    }
}
