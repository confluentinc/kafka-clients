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
/// What M15/P7's inputs become on the wire: the four explicit discriminants, the
/// length-delimited MAC, and <c>alter_user_scram_credentials_async</c>'s ten parallel arrays —
/// which have <b>no mock happy path</b> (both mocks throw Java's own "Not implemented yet"),
/// so the rows are asserted here rather than end to end.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>Each of the four discriminants is asserted in BOTH directions.</b> A discriminant
/// collapsed into a sentinel still submits successfully — <c>has_salts[i] = 0</c> means "let
/// the core generate a salt" while <c>1</c> with a zero-length salt means "use this empty
/// salt", and only the submitted pair tells them apart.
/// </para>
/// <para>
/// ⚠ Every pointer is decoded <b>inside</b> the stand-in: production unpins in its
/// <c>finally</c> (ffi §A4), so a read after the submit returns is a use-after-unpin.
/// </para>
/// </remarks>
public sealed class AdminP7SubmitArgumentTests
{
    // ------------------------------------------------------------------------------------
    // Discriminant 1 — has_salts (T-N3).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>THE test for this slice.</b> A <see langword="null"/> salt clears
    /// <c>has_salts[i]</c> (the core generates one); an <b>empty</b> salt sets it. Both
    /// directions, because either collapse submits cleanly.
    /// </summary>
    [Fact]
    public void Salt_NullAndEmpty_AreADiscriminantApart()
    {
        Assert.Equal(new byte[] { 0 }, CaptureAlter(Upsert("u", salt: null)).HasSalts);
        Assert.Equal(new byte[] { 1 }, CaptureAlter(Upsert("u", salt: Array.Empty<byte>())).HasSalts);
        Assert.Equal(new byte[] { 1 }, CaptureAlter(Upsert("u", salt: new byte[] { 7, 8 })).HasSalts);
    }

    /// <summary>
    /// A null salt still sends a length of <c>0</c> and a null pointer — the core reads the
    /// discriminant, not the pair, but a stale length would be read as a salt.
    /// </summary>
    [Fact]
    public void NullSalt_SendsNoBytes()
    {
        Captured captured = CaptureAlter(Upsert("u", salt: null));

        Assert.Equal(new[] { 0 }, captured.SaltLens);
        Assert.Equal(IntPtr.Zero, captured.SaltPointers[0]);
    }

    /// <summary>The salt's bytes and length reach the submit verbatim.</summary>
    [Fact]
    public void Salt_BytesAndLength_AreForwardedVerbatim()
    {
        Captured captured = CaptureAlter(Upsert("u", salt: new byte[] { 0x01, 0x00, 0xFF }));

        Assert.Equal(new[] { 3 }, captured.SaltLens);
        Assert.Equal(new byte[] { 0x01, 0x00, 0xFF }, captured.Salts[0]);
    }

    // ------------------------------------------------------------------------------------
    // is_deletions, and the ten arrays.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// An upsertion clears <c>is_deletions[i]</c> and a deletion sets it, per row — a batch
    /// mixing both keeps each row's own flag.
    /// </summary>
    [Fact]
    public void IsDeletions_IsPerRow()
    {
        Captured captured = CaptureAlter(
            Upsert("upserted"),
            new UserScramCredentialDeletion("deleted", ScramMechanism.ScramSha512));

        Assert.Equal(new byte[] { 0, 1 }, captured.IsDeletions);
        Assert.Equal(new[] { "upserted", "deleted" }, captured.Users);
        Assert.Equal(
            new[] { (int)ScramMechanism.ScramSha256, (int)ScramMechanism.ScramSha512 },
            captured.Mechanisms);
    }

    /// <summary>
    /// A deletion carries no password, salt or iteration count, so its four value columns are
    /// the neutral pair — a deletion that leaked the previous row's password would be caught
    /// here.
    /// </summary>
    [Fact]
    public void Deletion_CarriesNoSecret()
    {
        Captured captured = CaptureAlter(
            Upsert("upserted", password: new byte[] { 9, 9, 9 }),
            new UserScramCredentialDeletion("deleted", ScramMechanism.ScramSha256));

        Assert.Equal(IntPtr.Zero, captured.PasswordPointers[1]);
        Assert.Equal(0, captured.PasswordLens[1]);
        Assert.Equal(IntPtr.Zero, captured.SaltPointers[1]);
        Assert.Equal((byte)0, captured.HasSalts[1]);
    }

    /// <summary>The upsertion's password bytes and iteration count are forwarded verbatim.</summary>
    [Fact]
    public void Upsertion_PasswordAndIterations_AreForwardedVerbatim()
    {
        Captured captured = CaptureAlter(
            new UserScramCredentialUpsertion(
                "u",
                new ScramCredentialInfo(ScramMechanism.ScramSha512, 8192),
                new byte[] { 0x61, 0x00, 0x62 },
                salt: null));

        Assert.Equal(new[] { 8192 }, captured.Iterations);
        Assert.Equal(new[] { 3 }, captured.PasswordLens);
        Assert.Equal(new byte[] { 0x61, 0x00, 0x62 }, captured.Passwords[0]);
    }

    /// <summary>
    /// ⚠ An <b>empty</b> request still sends a non-null <c>is_deletions</c>: the core rejects a
    /// NULL one, so the projector pins a one-byte stand-in with a count of <c>0</c>.
    /// </summary>
    [Fact]
    public void EmptyRequest_StillSendsANonNullIsDeletions()
    {
        using AlterUserScramCredentialsMarshal.Rows rows =
            AlterUserScramCredentialsMarshal.Pin(Array.Empty<UserScramCredentialAlteration>());

        Assert.Equal(0, rows.Count);
        Assert.NotEqual(IntPtr.Zero, rows.IsDeletions);
        Assert.NotEqual(IntPtr.Zero, rows.HasSalts);
    }

    /// <summary>
    /// ⚠ Duplicate users are <b>passed through</b> as rows (<c>h:8875-8886</c>) while the
    /// awaitable key set collapses — Java keys one future per user.
    /// </summary>
    [Fact]
    public void DuplicateUsers_SendTwoRows_ButOneAwaitable()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        AlterUserScramCredentialsResult result = admin.AlterUserScramCredentials(
            new UserScramCredentialAlteration[] { Upsert("dup"), Upsert("dup") },
            options: null,
            (handle, users, isDeletions, mechanisms, iterations, passwords, passwordLens,
             salts, saltLens, hasSalts, count, timeoutMs, callback, userData) =>
            {
                captured.Count = count;
                captured.Users = Decode(users);
                captured.UserData = userData;
            });

        AdminCallbacks.AlterUserScramCredentials(IntPtr.Zero, CapturedError(), captured.UserData);

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { "dup", "dup" }, captured.Users);
        Assert.Single(result.Values);
    }

    // ------------------------------------------------------------------------------------
    // Discriminant 2 — has_owners_filter (T-N5).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ A <see langword="null"/> owners filter ("describe every token") and an
    /// <b>empty</b> one ("describe tokens owned by nobody") are a discriminant apart, not a
    /// count apart — both send <c>owner_count == 0</c>.
    /// </summary>
    [Fact]
    public void Owners_NullAndEmpty_AreADiscriminantApart()
    {
        CapturedDescribeTokens unset = CaptureDescribeTokens(new DescribeDelegationTokenOptions());
        CapturedDescribeTokens empty = CaptureDescribeTokens(
            new DescribeDelegationTokenOptions { Owners = Array.Empty<KafkaPrincipal>() });

        Assert.False(unset.HasOwnersFilter);
        Assert.True(empty.HasOwnersFilter);
        Assert.Equal(0, unset.OwnerCount);
        Assert.Equal(0, empty.OwnerCount);
    }

    /// <summary>A populated owners filter sets the flag and forwards both columns in order.</summary>
    [Fact]
    public void Owners_AreForwardedAsTwoParallelColumns()
    {
        CapturedDescribeTokens captured = CaptureDescribeTokens(
            new DescribeDelegationTokenOptions
            {
                Owners = new[] { new KafkaPrincipal("User", "alice"), new KafkaPrincipal("Group", "ops") },
            });

        Assert.True(captured.HasOwnersFilter);
        Assert.Equal(2, captured.OwnerCount);
        Assert.Equal(new[] { "User", "Group" }, captured.OwnerPrincipalTypes);
        Assert.Equal(new[] { "alice", "ops" }, captured.OwnerNames);
    }

    // ------------------------------------------------------------------------------------
    // Discriminant 3 — has_node_id (T-N5).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ An unset node id and node <c>0</c> are a discriminant apart — <c>0</c> is a real
    /// broker id, so a sentinel would silently retarget the describe.
    /// </summary>
    [Fact]
    public void NodeId_UnsetAndZero_AreADiscriminantApart()
    {
        (bool HasNodeId, int NodeId) unset = CaptureDescribeFeatures(new DescribeFeaturesOptions());
        (bool HasNodeId, int NodeId) zero =
            CaptureDescribeFeatures(new DescribeFeaturesOptions { NodeId = 0 });

        Assert.False(unset.HasNodeId);
        Assert.True(zero.HasNodeId);
        Assert.Equal(0, zero.NodeId);
    }

    /// <summary>An explicit node id is forwarded verbatim.</summary>
    [Fact]
    public void NodeId_IsForwardedVerbatim() =>
        Assert.Equal(7, CaptureDescribeFeatures(new DescribeFeaturesOptions { NodeId = 7 }).NodeId);

    // ------------------------------------------------------------------------------------
    // The MAC — length-delimited, never NUL-scanned (T-N2, submit half).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ A MAC carrying an <b>interior zero byte</b> reaches <c>renew</c> whole: its full
    /// length, and every byte past the zero. A NUL-scan truncates at index 2 here and the
    /// broker rejects the shortened MAC rather than the binding failing locally.
    /// </summary>
    [Fact]
    public void RenewHmac_SurvivesAnInteriorZeroByte()
    {
        byte[] hmac = { 0x9A, 0x4F, 0x00, 0x00, 0xC3, 0x01 };

        (int Length, byte[] Bytes, long Period) captured = CaptureRenew(hmac, periodMs: 3_600_000L);

        Assert.Equal(hmac.Length, captured.Length);
        Assert.Equal(hmac, captured.Bytes);
        Assert.Equal(3_600_000L, captured.Period);
    }

    /// <summary>The same, through <c>expire</c> — the byte-identical twin submit.</summary>
    [Fact]
    public void ExpireHmac_SurvivesAnInteriorZeroByte()
    {
        byte[] hmac = { 0x00, 0x11, 0x00, 0x22 };

        (int Length, byte[] Bytes, long Period) captured = CaptureExpire(hmac, periodMs: -1L);

        Assert.Equal(hmac.Length, captured.Length);
        Assert.Equal(hmac, captured.Bytes);
        Assert.Equal(-1L, captured.Period);
    }

    // ------------------------------------------------------------------------------------
    // createDelegationToken — the optional owner, and the renewer columns.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// An unset owner sends two null pointers (the core uses the caller's own principal); a
    /// set one sends both strings.
    /// </summary>
    [Fact]
    public void Owner_UnsetSendsNullPointers_SetSendsBoth()
    {
        CapturedCreateToken unset = CaptureCreateToken(new CreateDelegationTokenOptions());
        Assert.Equal(IntPtr.Zero, unset.OwnerPrincipalTypePointer);
        Assert.Equal(IntPtr.Zero, unset.OwnerNamePointer);

        CapturedCreateToken set = CaptureCreateToken(
            new CreateDelegationTokenOptions { Owner = new KafkaPrincipal("User", "owner") });
        Assert.Equal("User", set.OwnerPrincipalType);
        Assert.Equal("owner", set.OwnerName);
    }

    /// <summary>The renewer columns keep request order, and the lifetime is forwarded verbatim.</summary>
    [Fact]
    public void Renewers_KeepRequestOrder()
    {
        CapturedCreateToken captured = CaptureCreateToken(
            new CreateDelegationTokenOptions
            {
                Renewers = new[]
                {
                    new KafkaPrincipal("User", "first"),
                    new KafkaPrincipal("User", "second"),
                },
                MaxLifetimeMs = 86_400_000L,
            });

        Assert.Equal(2, captured.RenewerCount);
        Assert.Equal(new[] { "first", "second" }, captured.RenewerNames);
        Assert.Equal(86_400_000L, captured.MaxLifetimeMs);
    }

    // ------------------------------------------------------------------------------------
    // updateFeatures — short, not int (T-N6's submit half).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠ The version levels cross as <c>short</c>. A value above <see cref="short.MaxValue"/>
    /// cannot even be built (<see cref="FeatureUpdate"/> takes a <c>short</c>), so the column's
    /// element type is what the assertion pins.
    /// </summary>
    [Fact]
    public void UpdateFeatures_SendsShortVersionLevels()
    {
        CapturedUpdateFeatures captured = CaptureUpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                ["metadata.version"] = new FeatureUpdate(short.MaxValue, FeatureUpdate.UpgradeType.Upgrade),
            },
            new UpdateFeaturesOptions());

        Assert.IsType<short[]>(captured.MaxVersionLevels);
        Assert.Equal(new short[] { short.MaxValue }, captured.MaxVersionLevels);
    }

    /// <summary>Each update's upgrade type travels as its own Java ordinal, per feature.</summary>
    [Fact]
    public void UpdateFeatures_SendsPerFeatureUpgradeTypes()
    {
        CapturedUpdateFeatures captured = CaptureUpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                ["a"] = new FeatureUpdate(3, FeatureUpdate.UpgradeType.Upgrade),
                ["b"] = new FeatureUpdate(0, FeatureUpdate.UpgradeType.UnsafeDowngrade),
            },
            new UpdateFeaturesOptions());

        Dictionary<string, int> byFeature = captured.Features
            .Select((feature, index) => (feature, type: captured.UpgradeTypes[index]))
            .ToDictionary(pair => pair.feature!, pair => pair.type, StringComparer.Ordinal);

        Assert.Equal((int)FeatureUpdate.UpgradeType.Upgrade, byFeature["a"]);
        Assert.Equal((int)FeatureUpdate.UpgradeType.UnsafeDowngrade, byFeature["b"]);
    }

    /// <summary><c>ValidateOnly</c> reaches the submit in both states.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void UpdateFeatures_ForwardsValidateOnly(bool validateOnly) =>
        Assert.Equal(
            validateOnly,
            CaptureUpdateFeatures(
                new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
                {
                    ["f"] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
                },
                new UpdateFeaturesOptions { ValidateOnly = validateOnly }).ValidateOnly);

    // ------------------------------------------------------------------------------------
    // describeUserScramCredentials — the "every user" request.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// A null or empty user list sends <c>count == 0</c>, which the ABI reads as "describe
    /// every user" (<c>h:8784-8785</c>); a populated one sends the names in order.
    /// </summary>
    [Fact]
    public void DescribeUsers_NullAndEmpty_BothRequestEveryUser()
    {
        Assert.Equal(0, CaptureDescribeUsers(null).Count);
        Assert.Equal(0, CaptureDescribeUsers(Array.Empty<string>()).Count);

        (int Count, IReadOnlyList<string?> Users, int TimeoutMs) captured =
            CaptureDescribeUsers(new[] { "alice", "bob" });
        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { "alice", "bob" }, captured.Users);
    }

    // ------------------------------------------------------------------------------------
    // Timeouts — the shared mapping, sampled on one RPC per submit shape.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// A null timeout maps to a <b>negative</b> <c>timeout_ms</c> ("use the client default"),
    /// never <c>0</c> ("time out immediately"); an explicit one is forwarded verbatim.
    /// </summary>
    [Fact]
    public void NullTimeout_MapsToANegative_NotZero()
    {
        Assert.True(CaptureDescribeUsers(null, options: null).TimeoutMs < 0);
        Assert.True(CaptureDescribeUsers(null, new DescribeUserScramCredentialsOptions()).TimeoutMs < 0);
        Assert.Equal(
            0,
            CaptureDescribeUsers(null, new DescribeUserScramCredentialsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            9_876,
            CaptureDescribeUsers(null, new DescribeUserScramCredentialsOptions { TimeoutMs = 9_876 })
                .TimeoutMs);
    }

    // ------------------------------------------------------------------------------------
    // Capture helpers.
    // ------------------------------------------------------------------------------------

    private static UserScramCredentialUpsertion Upsert(
        string user, byte[]? password = null, byte[]? salt = null) =>
        new UserScramCredentialUpsertion(
            user,
            new ScramCredentialInfo(ScramMechanism.ScramSha256, 4096),
            password ?? new byte[] { 1, 2, 3 },
            salt);

    private static Captured CaptureAlter(params UserScramCredentialAlteration[] alterations)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        admin.AlterUserScramCredentials(
            alterations,
            options: null,
            (handle, users, isDeletions, mechanisms, iterations, passwords, passwordLens,
             salts, saltLens, hasSalts, count, timeoutMs, callback, userData) =>
            {
                captured.Count = count;
                captured.Users = Decode(users);
                captured.Mechanisms = mechanisms;
                captured.Iterations = iterations;
                captured.PasswordLens = passwordLens;
                captured.SaltLens = saltLens;
                captured.PasswordPointers = passwords;
                captured.SaltPointers = salts;
                captured.Passwords = CopyRows(passwords, passwordLens);
                captured.Salts = CopyRows(salts, saltLens);
                captured.IsDeletions = CopyFlags(isDeletions, count);
                captured.HasSalts = CopyFlags(hasSalts, count);
                captured.UserData = userData;
            });

        AdminCallbacks.AlterUserScramCredentials(IntPtr.Zero, CapturedError(), captured.UserData);
        return captured;
    }

    private static (int Count, IReadOnlyList<string?> Users, int TimeoutMs) CaptureDescribeUsers(
        IReadOnlyCollection<string>? users, DescribeUserScramCredentialsOptions? options = null)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        int count = 0;
        int timeout = 0;
        IReadOnlyList<string?> names = Array.Empty<string?>();
        IntPtr userData = IntPtr.Zero;

        admin.DescribeUserScramCredentials(
            users,
            options,
            (handle, pinned, pinnedCount, timeoutMs, callback, data) =>
            {
                count = pinnedCount;
                timeout = timeoutMs;
                names = Decode(pinned);
                userData = data;
            });

        AdminCallbacks.DescribeUserScramCredentials(IntPtr.Zero, CapturedError(), userData);
        return (count, names, timeout);
    }

    private static CapturedCreateToken CaptureCreateToken(CreateDelegationTokenOptions options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        CapturedCreateToken captured = new CapturedCreateToken();
        admin.CreateDelegationToken(
            options,
            (handle, renewerTypes, renewerNames, renewerCount, ownerType, ownerName,
             maxLifetimeMs, timeoutMs, callback, userData) =>
            {
                captured.RenewerCount = renewerCount;
                captured.RenewerPrincipalTypes = Decode(renewerTypes);
                captured.RenewerNames = Decode(renewerNames);
                captured.OwnerPrincipalTypePointer = ownerType;
                captured.OwnerNamePointer = ownerName;
                captured.OwnerPrincipalType = Utf8Marshal.PtrToString(ownerType);
                captured.OwnerName = Utf8Marshal.PtrToString(ownerName);
                captured.MaxLifetimeMs = maxLifetimeMs;
                captured.UserData = userData;
            });

        AdminCallbacks.CreateDelegationToken(IntPtr.Zero, CapturedError(), captured.UserData);
        return captured;
    }

    private static (int Length, byte[] Bytes, long Period) CaptureRenew(byte[] hmac, long periodMs)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        int length = -1;
        byte[] bytes = Array.Empty<byte>();
        long period = 0;
        IntPtr userData = IntPtr.Zero;

        admin.RenewDelegationToken(
            hmac,
            new RenewDelegationTokenOptions { RenewTimePeriodMs = periodMs },
            (handle, pointer, hmacLength, renewMs, timeoutMs, callback, data) =>
            {
                length = hmacLength;
                bytes = CopyRow(pointer, hmacLength);
                period = renewMs;
                userData = data;
            });

        AdminCallbacks.RenewDelegationToken(IntPtr.Zero, CapturedError(), userData);
        return (length, bytes, period);
    }

    private static (int Length, byte[] Bytes, long Period) CaptureExpire(byte[] hmac, long periodMs)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        int length = -1;
        byte[] bytes = Array.Empty<byte>();
        long period = 0;
        IntPtr userData = IntPtr.Zero;

        admin.ExpireDelegationToken(
            hmac,
            new ExpireDelegationTokenOptions { ExpiryTimePeriodMs = periodMs },
            (handle, pointer, hmacLength, expiryMs, timeoutMs, callback, data) =>
            {
                length = hmacLength;
                bytes = CopyRow(pointer, hmacLength);
                period = expiryMs;
                userData = data;
            });

        AdminCallbacks.ExpireDelegationToken(IntPtr.Zero, CapturedError(), userData);
        return (length, bytes, period);
    }

    private static CapturedDescribeTokens CaptureDescribeTokens(DescribeDelegationTokenOptions options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        CapturedDescribeTokens captured = new CapturedDescribeTokens();
        admin.DescribeDelegationToken(
            options,
            (handle, hasOwnersFilter, ownerTypes, ownerNames, ownerCount, timeoutMs, callback, userData) =>
            {
                captured.HasOwnersFilter = hasOwnersFilter;
                captured.OwnerCount = ownerCount;
                captured.OwnerPrincipalTypes = Decode(ownerTypes);
                captured.OwnerNames = Decode(ownerNames);
                captured.UserData = userData;
            });

        AdminCallbacks.DescribeDelegationToken(IntPtr.Zero, CapturedError(), captured.UserData);
        return captured;
    }

    private static (bool HasNodeId, int NodeId) CaptureDescribeFeatures(DescribeFeaturesOptions options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool hasNodeId = false;
        int node = -1;
        IntPtr userData = IntPtr.Zero;

        admin.DescribeFeatures(
            options,
            (handle, has, nodeId, timeoutMs, callback, data) =>
            {
                hasNodeId = has;
                node = nodeId;
                userData = data;
            });

        AdminCallbacks.DescribeFeatures(IntPtr.Zero, CapturedError(), userData);
        return (hasNodeId, node);
    }

    private static CapturedUpdateFeatures CaptureUpdateFeatures(
        IReadOnlyDictionary<string, FeatureUpdate> updates, UpdateFeaturesOptions options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        CapturedUpdateFeatures captured = new CapturedUpdateFeatures();
        admin.UpdateFeatures(
            updates,
            options,
            (handle, features, maxVersionLevels, upgradeTypes, count, timeoutMs, validateOnly,
             callback, userData) =>
            {
                captured.Count = count;
                captured.Features = Decode(features);
                captured.MaxVersionLevels = maxVersionLevels;
                captured.UpgradeTypes = upgradeTypes;
                captured.ValidateOnly = validateOnly;
                captured.UserData = userData;
            });

        AdminCallbacks.UpdateFeatures(IntPtr.Zero, CapturedError(), captured.UserData);
        return captured;
    }

    /// <summary>
    /// Decodes one pointer array <b>by its own length</b>, so a mismatch against the count is
    /// an assertion failure rather than an index-out-of-range inside the stand-in.
    /// </summary>
    private static IReadOnlyList<string?> Decode(IntPtr[] values) =>
        values.Select(value => Utf8Marshal.PtrToString(value)).ToArray();

    private static byte[][] CopyRows(IntPtr[] pointers, int[] lengths) =>
        pointers.Select((pointer, index) => CopyRow(pointer, lengths[index])).ToArray();

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

    /// <summary>Reads one pinned <c>const bool *</c> column back out as its 0/1 bytes.</summary>
    private static byte[] CopyFlags(IntPtr column, int count) => CopyRow(column, count);

    /// <summary>
    /// An <b>owned</b> error for the trampoline to consume, standing in for the one native
    /// would hand the callback. The trampoline frees it, so it must never be freed here.
    /// </summary>
    private static IntPtr CapturedError()
    {
        using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("captured");
        IntPtr error = NativeMethods.KafkaErrorNew(1, message.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    private sealed class Captured
    {
        internal int Count { get; set; }

        internal IReadOnlyList<string?> Users { get; set; } = Array.Empty<string?>();

        internal int[] Mechanisms { get; set; } = Array.Empty<int>();

        internal int[] Iterations { get; set; } = Array.Empty<int>();

        internal int[] PasswordLens { get; set; } = Array.Empty<int>();

        internal int[] SaltLens { get; set; } = Array.Empty<int>();

        internal IntPtr[] PasswordPointers { get; set; } = Array.Empty<IntPtr>();

        internal IntPtr[] SaltPointers { get; set; } = Array.Empty<IntPtr>();

        internal byte[][] Passwords { get; set; } = Array.Empty<byte[]>();

        internal byte[][] Salts { get; set; } = Array.Empty<byte[]>();

        internal byte[] IsDeletions { get; set; } = Array.Empty<byte>();

        internal byte[] HasSalts { get; set; } = Array.Empty<byte>();

        internal IntPtr UserData { get; set; }
    }

    private sealed class CapturedCreateToken
    {
        internal int RenewerCount { get; set; }

        internal IReadOnlyList<string?> RenewerPrincipalTypes { get; set; } = Array.Empty<string?>();

        internal IReadOnlyList<string?> RenewerNames { get; set; } = Array.Empty<string?>();

        internal IntPtr OwnerPrincipalTypePointer { get; set; }

        internal IntPtr OwnerNamePointer { get; set; }

        internal string? OwnerPrincipalType { get; set; }

        internal string? OwnerName { get; set; }

        internal long MaxLifetimeMs { get; set; }

        internal IntPtr UserData { get; set; }
    }

    private sealed class CapturedDescribeTokens
    {
        internal bool HasOwnersFilter { get; set; }

        internal int OwnerCount { get; set; }

        internal IReadOnlyList<string?> OwnerPrincipalTypes { get; set; } = Array.Empty<string?>();

        internal IReadOnlyList<string?> OwnerNames { get; set; } = Array.Empty<string?>();

        internal IntPtr UserData { get; set; }
    }

    private sealed class CapturedUpdateFeatures
    {
        internal int Count { get; set; }

        internal IReadOnlyList<string?> Features { get; set; } = Array.Empty<string?>();

        internal short[] MaxVersionLevels { get; set; } = Array.Empty<short>();

        internal int[] UpgradeTypes { get; set; } = Array.Empty<int>();

        internal bool ValidateOnly { get; set; }

        internal IntPtr UserData { get; set; }
    }
}
