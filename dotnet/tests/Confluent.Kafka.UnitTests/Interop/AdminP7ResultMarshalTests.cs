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
/// M15/P7's three readers, driven over shapes the ABI cannot produce: the nested
/// <c>(user, credential)</c> walk, a MAC carrying an interior zero byte, and two feature
/// tables that differ in size and key set.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>Every shape here is unreachable end to end</b>, which is why the accessors are
/// injected. The two SCRAM RPCs mirror Java's own <c>UnsupportedOperationException</c>, so no
/// populated <c>DescribeUserScramCredentialsResult_t</c> exists without a broker; the mock's
/// MAC is a Uuid's UTF-8 and never carries an interior zero; and the mock derives both feature
/// tables from one seeded key set, so they always agree.
/// </para>
/// <para>
/// ⚠ The <b>wiring</b> — that each reader is built on its own ABI symbols — is
/// <see cref="AdminP4ReaderWiringTests"/>'s job. Injected accessors prove the walk, not the
/// binding.
/// </para>
/// </remarks>
public sealed class AdminP7ResultMarshalTests
{
    // ------------------------------------------------------------------------------------
    // The per-description credential walk, and the owned/borrowed reader pair (M15/P10).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>THE test for this slice.</b> Each description's credentials are walked by
    /// <b>its own</b> <c>credential_count()</c>, never by any outer count (the <c>all()</c>
    /// row count, the <c>users()</c> count).
    /// </summary>
    /// <remarks>
    /// The two descriptions are deliberately unequal in <em>both</em> directions — inner
    /// counts of 0 and 3 against an outer count of 2 — so a reader driven from an outer count
    /// over-reads on one and truncates on the other.
    /// </remarks>
    [Fact]
    public void CredentialWalk_IsBoundedByTheDescriptionsOwnCredentialCount()
    {
        using StubDescriptions stub = new StubDescriptions(
            new StubDescription("empty", Array.Empty<(int, int)>()),
            new StubDescription("alice", new[] { (1, 4096), (2, 8192), (1, 16384) }));

        UserScramCredentialsDescription empty =
            UserScramCredentialMarshal.Read(stub.Pointer(0), stub.Accessors);
        UserScramCredentialsDescription alice =
            UserScramCredentialMarshal.Read(stub.Pointer(1), stub.Accessors);

        Assert.Equal("empty", empty.Name);
        Assert.Empty(empty.CredentialInfos);

        Assert.Equal("alice", alice.Name);
        Assert.Equal(3, alice.CredentialInfos.Count);
        Assert.Equal(
            new[] { ScramMechanism.ScramSha256, ScramMechanism.ScramSha512, ScramMechanism.ScramSha256 },
            alice.CredentialInfos.Select(info => info.Mechanism));
        Assert.Equal(
            new[] { 4096, 8192, 16384 },
            alice.CredentialInfos.Select(info => info.Iterations));
    }

    /// <summary>
    /// ⚠⚠ <b>The <c>all_get_description</c> pointer is BORROWED — the plain reader must never
    /// destroy it</b> (ffi §B2 Category 4): the result root owns it and frees it on its own
    /// destroy, so a destroy here is a double free with no managed symptom.
    /// </summary>
    [Fact]
    public void Read_NeverDestroysTheBorrowedDescription()
    {
        using StubDescriptions stub = new StubDescriptions(
            new StubDescription("alice", new[] { (1, 4096) }));

        UserScramCredentialMarshal.Read(stub.Pointer(0), stub.Accessors);
        UserScramCredentialMarshal.Read(stub.Pointer(0), stub.Accessors);

        Assert.Equal(0, stub.DestroyCount(0));
    }

    /// <summary>
    /// ⚠⚠ The owned twin destroys its description <b>exactly once</b> — the
    /// <c>description(user)</c> side of the same type. Not destroying it leaks one per call.
    /// </summary>
    [Fact]
    public void ReadAndDestroy_DestroysTheOwnedDescriptionExactlyOnce()
    {
        using StubDescriptions stub = new StubDescriptions(
            new StubDescription("alice", new[] { (2, 8192) }));

        UserScramCredentialsDescription alice =
            UserScramCredentialMarshal.ReadAndDestroy(stub.Pointer(0), stub.Accessors);

        Assert.Equal("alice", alice.Name);
        Assert.Equal(ScramMechanism.ScramSha512, alice.CredentialInfos.Single().Mechanism);
        Assert.Equal(1, stub.DestroyCount(0));
    }

    /// <summary>
    /// ⚠⚠ …and <b>on the throwing path too</b> — the destroy is in a <c>finally</c>, so a read
    /// that faults mid-walk still releases the handle exactly once rather than leaking it.
    /// </summary>
    /// <remarks>
    /// The probe is a null <c>name()</c>, which is a real ABI-reachable shape and makes
    /// <c>ReadStringKey</c> throw — deliberately not an exception the reader itself could
    /// produce and swallow, so a green result cannot come from the throw being normalized.
    /// </remarks>
    [Fact]
    public void ReadAndDestroy_DestroysExactlyOnce_WhenTheReadThrows()
    {
        using StubDescriptions stub = new StubDescriptions(
            new StubDescription(name: null, new[] { (1, 4096) }));

        Assert.Throws<KafkaException>(
            () => UserScramCredentialMarshal.ReadAndDestroy(stub.Pointer(0), stub.Accessors));

        Assert.Equal(1, stub.DestroyCount(0));
    }

    /// <summary>
    /// A mechanism type the ABI reports outside the two known codes decodes to
    /// <see cref="ScramMechanism.Unknown"/> — Java's <c>fromType</c> fallback — rather than
    /// throwing on a broker that learned a third mechanism.
    /// </summary>
    [Fact]
    public void UnknownMechanismCode_DecodesToUnknown()
    {
        using StubDescriptions stub = new StubDescriptions(
            new StubDescription("u", new[] { (99, 4096) }));

        Assert.Equal(
            ScramMechanism.Unknown,
            UserScramCredentialMarshal.Read(stub.Pointer(0), stub.Accessors)
                .CredentialInfos.Single().Mechanism);
    }

    // ------------------------------------------------------------------------------------
    // The MAC — length-delimited, never NUL-scanned (T-N2, read half).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>THE test for the MAC.</b> A MAC whose second byte is <c>0x00</c> is copied out
    /// <b>whole</b>: its full length, and every byte past the zero. A NUL-scan returns one
    /// byte here and the truncated MAC round-trips into a broker-side rejection.
    /// </summary>
    [Fact]
    public void ReadHmac_SurvivesAnInteriorZeroByte()
    {
        byte[] native = { 0x7F, 0x00, 0x00, 0xA1, 0x00, 0xFE };

        using PinnedBytes pinned = new PinnedBytes(native);

        Assert.Equal(native, DelegationTokenMarshal.ReadHmac(IntPtr.Zero, pinned.Accessor));
    }

    /// <summary>
    /// A MAC that is entirely zero bytes still comes back at full length — the degenerate case
    /// a NUL-scan reports as empty rather than as six zeros.
    /// </summary>
    [Fact]
    public void ReadHmac_SurvivesAnAllZeroMac()
    {
        byte[] native = new byte[6];

        using PinnedBytes pinned = new PinnedBytes(native);

        byte[] hmac = DelegationTokenMarshal.ReadHmac(IntPtr.Zero, pinned.Accessor);
        Assert.Equal(6, hmac.Length);
        Assert.All(hmac, value => Assert.Equal(0, value));
    }

    /// <summary>
    /// An absent MAC — a null pointer, or a non-positive length — is the empty array, never a
    /// null the public <see cref="DelegationToken.Hmac"/> would have to admit.
    /// </summary>
    [Theory]
    [InlineData(true, 8)]
    [InlineData(false, 0)]
    [InlineData(false, -1)]
    public void ReadHmac_AbsentIsEmpty_NotNull(bool nullPointer, int length)
    {
        using PinnedBytes pinned = new PinnedBytes(new byte[] { 1, 2, 3, 4 });

        byte[] hmac = DelegationTokenMarshal.ReadHmac(
            IntPtr.Zero,
            (IntPtr token, out int outLength) =>
            {
                outLength = length;
                return nullPointer ? IntPtr.Zero : pinned.Pointer;
            });

        Assert.NotNull(hmac);
        Assert.Empty(hmac);
    }

    // ------------------------------------------------------------------------------------
    // The two feature tables (T-N6).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>THE test for this slice.</b> The finalized and supported tables are walked by
    /// <b>their own</b> counts, over their own key sets — here 1 finalized against 3
    /// supported, with only one key in common.
    /// </summary>
    /// <remarks>
    /// Driving the supported walk from <c>finalized_count</c> yields a one-entry supported
    /// table, so both the count and the key set are asserted.
    /// </remarks>
    [Fact]
    public void EachFeatureTable_IsWalkedByItsOwnCount()
    {
        using StubFeatures stub = new StubFeatures(
            finalized: new[] { ("metadata.version", (short)3, (short)7) },
            supported: new[]
            {
                ("metadata.version", (short)1, (short)9),
                ("group.version", (short)0, (short)2),
                ("transaction.version", (short)0, (short)1),
            },
            epoch: 4242L);

        FeatureMetadata metadata = FeatureMetadataMarshal.CopyOut(IntPtr.Zero, stub.Accessors);

        Assert.Single(metadata.FinalizedFeatures);
        Assert.Equal(3, metadata.SupportedFeatures.Count);
        Assert.Equal(
            new[] { "group.version", "metadata.version", "transaction.version" },
            metadata.SupportedFeatures.Keys.OrderBy(key => key, StringComparer.Ordinal));

        Assert.Equal(3, metadata.FinalizedFeatures["metadata.version"].MinVersionLevel);
        Assert.Equal(7, metadata.FinalizedFeatures["metadata.version"].MaxVersionLevel);
        Assert.Equal(1, metadata.SupportedFeatures["metadata.version"].MinVersion);
        Assert.Equal(9, metadata.SupportedFeatures["metadata.version"].MaxVersion);
    }

    /// <summary>
    /// The reverse skew — a longer finalized table than supported — so a walk driven from the
    /// wrong count fails whichever way the tables are sized.
    /// </summary>
    [Fact]
    public void EachFeatureTable_IsWalkedByItsOwnCount_WhenFinalizedIsLonger()
    {
        using StubFeatures stub = new StubFeatures(
            finalized: new[] { ("a", (short)1, (short)2), ("b", (short)1, (short)3) },
            supported: new[] { ("c", (short)0, (short)4) },
            epoch: 1L);

        FeatureMetadata metadata = FeatureMetadataMarshal.CopyOut(IntPtr.Zero, stub.Accessors);

        Assert.Equal(new[] { "a", "b" }, metadata.FinalizedFeatures.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(new[] { "c" }, metadata.SupportedFeatures.Keys);
    }

    /// <summary>
    /// ⚠⚠ <b>The epoch's presence is the accessor's RETURN, never a sentinel</b> — every
    /// <see cref="long"/> is a legal epoch, so an absent epoch is <see langword="null"/> and a
    /// present one of <c>0</c> or <c>-1</c> is kept.
    /// </summary>
    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(long.MinValue)]
    public void PresentEpoch_IsKept_WhateverItsValue(long epoch)
    {
        using StubFeatures stub = new StubFeatures(
            Array.Empty<(string, short, short)>(), Array.Empty<(string, short, short)>(), epoch);

        Assert.Equal(epoch, FeatureMetadataMarshal.CopyOut(IntPtr.Zero, stub.Accessors).FinalizedFeaturesEpoch);
    }

    /// <summary>An absent epoch is <see langword="null"/>, not <c>0</c>.</summary>
    [Fact]
    public void AbsentEpoch_IsNull()
    {
        using StubFeatures stub = new StubFeatures(
            Array.Empty<(string, short, short)>(), Array.Empty<(string, short, short)>(), epoch: null);

        Assert.Null(FeatureMetadataMarshal.CopyOut(IntPtr.Zero, stub.Accessors).FinalizedFeaturesEpoch);
    }

    // ------------------------------------------------------------------------------------
    // Stand-ins.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// One row of the <c>describeUserScramCredentials</c> table. A plain class rather than a
    /// positional <c>record</c>: the latter needs <c>IsExternalInit</c>, which net462 lacks.
    /// </summary>
    private sealed class StubDescription
    {
        internal StubDescription(string? name, (int Mechanism, int Iterations)[] credentials)
        {
            Name = name;
            Credentials = credentials;
        }

        /// <summary>The user name, or null to drive the reader's throwing path.</summary>
        internal string? Name { get; }

        internal (int Mechanism, int Iterations)[] Credentials { get; }
    }

    /// <summary>
    /// Stand-in <c>UserScramCredentialsDescription_t</c>s: the pinned names, the four read
    /// accessors over them, and a <b>counting</b> destroy so each handle's release count is
    /// assertable — the only way to see the owned/borrowed distinction, which has no other
    /// managed symptom.
    /// </summary>
    /// <remarks>
    /// The stand-in pointer is the 1-based index, so <c>IntPtr.Zero</c> stays distinguishable
    /// from a valid handle and the destroy counter can attribute a release to one description.
    /// </remarks>
    private sealed class StubDescriptions : IDisposable
    {
        private readonly StubDescription[] _descriptions;
        private readonly List<Utf8Marshal.PinnedUtf8String> _names = new List<Utf8Marshal.PinnedUtf8String>();
        private readonly int[] _destroyCounts;

        internal StubDescriptions(params StubDescription[] descriptions)
        {
            _descriptions = descriptions;
            _destroyCounts = new int[descriptions.Length];

            foreach (StubDescription description in descriptions)
            {
                _names.Add(description.Name is null ? null! : Utf8Marshal.Pin(description.Name));
            }

            Accessors = new UserScramCredentialMarshal.Accessors(
                description => _names[Index(description)]?.Pointer ?? IntPtr.Zero,
                description => _descriptions[Index(description)].Credentials.Length,
                (description, credential) =>
                    _descriptions[Index(description)].Credentials[credential].Mechanism,
                (description, credential) =>
                    _descriptions[Index(description)].Credentials[credential].Iterations,
                description => _destroyCounts[Index(description)]++);
        }

        internal UserScramCredentialMarshal.Accessors Accessors { get; }

        /// <summary>The stand-in handle for description <paramref name="index"/>.</summary>
        internal IntPtr Pointer(int index) => new IntPtr(index + 1);

        /// <summary>How many times description <paramref name="index"/> was destroyed.</summary>
        internal int DestroyCount(int index) => _destroyCounts[index];

        public void Dispose()
        {
            foreach (Utf8Marshal.PinnedUtf8String name in _names)
            {
                name?.Dispose();
            }
        }

        private static int Index(IntPtr description) => description.ToInt32() - 1;
    }

    /// <summary>A pinned byte buffer plus a MAC accessor that hands it back with its length.</summary>
    private sealed class PinnedBytes : IDisposable
    {
        private GCHandle _pin;

        internal PinnedBytes(byte[] bytes)
        {
            _pin = GCHandle.Alloc(bytes, GCHandleType.Pinned);
            Length = bytes.Length;
        }

        internal IntPtr Pointer => _pin.AddrOfPinnedObject();

        internal int Length { get; }

        internal DelegationTokenMarshal.HmacAccessor Accessor =>
            (IntPtr token, out int outLength) =>
            {
                outLength = Length;
                return Pointer;
            };

        public void Dispose()
        {
            if (_pin.IsAllocated)
            {
                _pin.Free();
            }
        }
    }

    /// <summary>
    /// A stand-in <c>DescribeFeaturesResult_t</c>: two independently-sized tables, their
    /// pinned names, and an epoch whose presence is the accessor's return.
    /// </summary>
    private sealed class StubFeatures : IDisposable
    {
        private readonly (string Name, short Min, short Max)[] _finalized;
        private readonly (string Name, short Min, short Max)[] _supported;
        private readonly List<Utf8Marshal.PinnedUtf8String> _names = new List<Utf8Marshal.PinnedUtf8String>();

        internal StubFeatures(
            (string Name, short Min, short Max)[] finalized,
            (string Name, short Min, short Max)[] supported,
            long? epoch)
        {
            _finalized = finalized;
            _supported = supported;

            IntPtr[] finalizedNames = finalized.Select(Pin).ToArray();
            IntPtr[] supportedNames = supported.Select(Pin).ToArray();

            Accessors = new FeatureMetadataMarshal.Accessors(
                result => _finalized.Length,
                (result, index) => finalizedNames[index],
                (result, index) => _finalized[index].Min,
                (result, index) => _finalized[index].Max,
                result => _supported.Length,
                (result, index) => supportedNames[index],
                (result, index) => _supported[index].Min,
                (result, index) => _supported[index].Max,
                (IntPtr result, out long value) =>
                {
                    value = epoch ?? 0L;
                    return epoch is not null;
                });
        }

        internal FeatureMetadataMarshal.Accessors Accessors { get; }

        public void Dispose()
        {
            foreach (Utf8Marshal.PinnedUtf8String name in _names)
            {
                name.Dispose();
            }
        }

        private IntPtr Pin((string Name, short Min, short Max) feature)
        {
            Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(feature.Name);
            _names.Add(pinned);
            return pinned.Pointer;
        }
    }
}
