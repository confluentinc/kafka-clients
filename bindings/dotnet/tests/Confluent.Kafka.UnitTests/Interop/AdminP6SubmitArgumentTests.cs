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
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What P6's inputs actually become on the wire: <c>AclRowMarshal</c>'s seven-array
/// projection behind <c>create_acls_async</c> / <c>delete_acls_async</c> /
/// <c>describe_acls_async</c>, and <c>ClientQuotaMarshal</c>'s two projections behind
/// <c>describe_client_quotas_async</c> and the ragged <c>alter_client_quotas_async</c>
/// (M15/P6).
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The defect this file exists to catch is <see langword="null"/> collapsing into
/// <c>""</c>.</b> On a filter row a null name means "match any resource name" and must
/// travel as <see cref="IntPtr.Zero"/>; <c>""</c> is a filter on the literal empty name and
/// must travel as a <b>non-null</b> pointer to a NUL byte
/// (<c>confluent_kafka.h:7528-7530</c>). A helper mapping both to
/// <see cref="IntPtr.Zero"/> turns a filter on <c>""</c> into match-everything, which on
/// <c>delete_acls</c> deletes ACLs the caller never named. It is asserted here, on the
/// <b>pointer</b>, where it is a fact rather than an inference — twice: once on the projector
/// itself and once through the <c>delete_acls</c> submit, the RPC that can act on it.
/// </para>
/// <para>
/// <b>The projector is also driven directly for the filter shape.</b> The two overloads share
/// the one rule by construction, so exercising it at both levels is what keeps a change to
/// either from silently diverging.
/// </para>
/// <para>
/// ⚠⚠ <b>The quota family carries the same class of defect on two more axes.</b> An entity
/// name of <see langword="null"/> is the built-in <em>default</em> entity rather than the name
/// <c>""</c> (<c>confluent_kafka.h:8287-8290</c>), and a cleared
/// <c>op_has_values[i][j]</c> is Java's <c>Op(key, null)</c> — <em>remove</em> the quota
/// rather than set it to <c>0</c> (<c>h:8291-8295</c>). Both are asserted on the submitted
/// bytes for the same reason the ACL rule is: neither is visible end to end.
/// </para>
/// <para>
/// ⚠ <b>Everything is decoded inside the submit, never after it.</b> Production unpins every
/// string in its <c>finally</c> (ffi §A4), so a pointer read after <c>CreateAcls</c> returns
/// is a use-after-unpin. Reading them in the stand-in is also the only way to observe that
/// the pins are live for the whole call.
/// </para>
/// </remarks>
public sealed class AdminP6SubmitArgumentTests
{
    // ------------------------------------------------------------------------------------
    // AclRowMarshal — the projector itself.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>THE test for this slice.</b> A null filter name, principal and host each become
    /// <see cref="IntPtr.Zero"/>; an <b>empty</b> one becomes a non-null pointer to a NUL
    /// byte, and decodes back to <c>""</c>.
    /// </summary>
    [Fact]
    public void FilterRow_NullIsAPointerApart_FromEmpty()
    {
        AclBindingFilter wildcard = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Any),
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any));

        AclBindingFilter literalEmpty = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Any),
            new AccessControlEntryFilter(
                string.Empty, string.Empty, AclOperation.Any, AclPermissionType.Any));

        using AclRowMarshal.Rows rows =
            AclRowMarshal.Pin(new List<AclBindingFilter> { wildcard, literalEmpty });

        Assert.Equal(IntPtr.Zero, rows.ResourceNames[0]);
        Assert.Equal(IntPtr.Zero, rows.Principals[0]);
        Assert.Equal(IntPtr.Zero, rows.Hosts[0]);

        // Non-null, and decoding to the empty string — not to null, and not to a stray byte.
        Assert.NotEqual(IntPtr.Zero, rows.ResourceNames[1]);
        Assert.NotEqual(IntPtr.Zero, rows.Principals[1]);
        Assert.NotEqual(IntPtr.Zero, rows.Hosts[1]);
        Assert.Equal(string.Empty, Utf8Marshal.PtrToString(rows.ResourceNames[1]));
        Assert.Equal(string.Empty, Utf8Marshal.PtrToString(rows.Principals[1]));
        Assert.Equal(string.Empty, Utf8Marshal.PtrToString(rows.Hosts[1]));
    }

    /// <summary>
    /// Every column of a filter row lands in its own array, at its own index — the
    /// seven-way destructuring, asserted with seven <em>distinct</em> values so a swapped
    /// pair cannot pass.
    /// </summary>
    [Fact]
    public void FilterRow_ProjectsEveryColumn()
    {
        AclBindingFilter filter = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.TransactionalId, "txn-1", PatternType.Prefixed),
            new AccessControlEntryFilter(
                "User:alice", "10.0.0.1", AclOperation.DescribeConfigs, AclPermissionType.Deny));

        using AclRowMarshal.Rows rows = AclRowMarshal.Pin(new List<AclBindingFilter> { filter });

        Assert.Equal(1, rows.Count);
        Assert.Equal((int)ResourceType.TransactionalId, rows.ResourceTypes[0]);
        Assert.Equal("txn-1", Utf8Marshal.PtrToString(rows.ResourceNames[0]));
        Assert.Equal((int)PatternType.Prefixed, rows.PatternTypes[0]);
        Assert.Equal("User:alice", Utf8Marshal.PtrToString(rows.Principals[0]));
        Assert.Equal("10.0.0.1", Utf8Marshal.PtrToString(rows.Hosts[0]));
        Assert.Equal((int)AclOperation.DescribeConfigs, rows.Operations[0]);
        Assert.Equal((int)AclPermissionType.Deny, rows.PermissionTypes[0]);
    }

    /// <summary>
    /// The concrete overload projects the same seven columns out of the <b>nested</b>
    /// managed shape — <c>Pattern.*</c> into 0-2 and <c>Entry.*</c> into 3-6.
    /// </summary>
    [Fact]
    public void ConcreteRow_ProjectsEveryColumn()
    {
        using AclRowMarshal.Rows rows = AclRowMarshal.Pin(new List<AclBinding> { Binding("topic-a") });

        Assert.Equal(1, rows.Count);
        Assert.Equal((int)ResourceType.Topic, rows.ResourceTypes[0]);
        Assert.Equal("topic-a", Utf8Marshal.PtrToString(rows.ResourceNames[0]));
        Assert.Equal((int)PatternType.Literal, rows.PatternTypes[0]);
        Assert.Equal("User:alice", Utf8Marshal.PtrToString(rows.Principals[0]));
        Assert.Equal("*", Utf8Marshal.PtrToString(rows.Hosts[0]));
        Assert.Equal((int)AclOperation.Read, rows.Operations[0]);
        Assert.Equal((int)AclPermissionType.Allow, rows.PermissionTypes[0]);
    }

    /// <summary>
    /// A concrete row's strings are never null, so its three pointer columns are never
    /// <see cref="IntPtr.Zero"/> — including an <b>empty</b> resource name, which the value
    /// types accept.
    /// </summary>
    [Fact]
    public void ConcreteRow_NeverSendsANullPointer()
    {
        AclBinding empty = new AclBinding(
            new ResourcePattern(ResourceType.Topic, string.Empty, PatternType.Literal),
            new AccessControlEntry(string.Empty, string.Empty, AclOperation.Read, AclPermissionType.Allow));

        using AclRowMarshal.Rows rows = AclRowMarshal.Pin(new List<AclBinding> { empty });

        Assert.NotEqual(IntPtr.Zero, rows.ResourceNames[0]);
        Assert.NotEqual(IntPtr.Zero, rows.Principals[0]);
        Assert.NotEqual(IntPtr.Zero, rows.Hosts[0]);
    }

    /// <summary>
    /// Rows keep their request order and their own index across a batch — a projector that
    /// shared one buffer, or reversed, is caught here.
    /// </summary>
    [Fact]
    public void Rows_KeepRequestOrder()
    {
        using AclRowMarshal.Rows rows = AclRowMarshal.Pin(
            new List<AclBinding> { Binding("first"), Binding("second"), Binding("third") });

        Assert.Equal(3, rows.Count);
        Assert.Equal(
            new[] { "first", "second", "third" },
            new[] { rows.ResourceNames[0], rows.ResourceNames[1], rows.ResourceNames[2] }
                .Select(name => Utf8Marshal.PtrToString(name)));
    }

    /// <summary>
    /// An empty request produces seven zero-length arrays and a count of <c>0</c> — never a
    /// null array, which the ABI would read as "absent" rather than "nothing to do".
    /// </summary>
    [Fact]
    public void EmptyRequest_ProducesSevenEmptyArrays()
    {
        using AclRowMarshal.Rows rows = AclRowMarshal.Pin(new List<AclBinding>());

        Assert.Equal(0, rows.Count);
        Assert.Empty(rows.ResourceTypes);
        Assert.Empty(rows.ResourceNames);
        Assert.Empty(rows.PatternTypes);
        Assert.Empty(rows.Principals);
        Assert.Empty(rows.Hosts);
        Assert.Empty(rows.Operations);
        Assert.Empty(rows.PermissionTypes);
    }

    /// <summary>
    /// ⚠ <b>Every pin is released, and the assertion is on the pins themselves.</b> A pinned
    /// <c>GCHandle</c> is a strong root, so a leaked one is a permanent pin that nothing
    /// downstream can observe — no exception, no failing assertion, just a fragmenting heap
    /// (ffi §A4).
    /// </summary>
    /// <remarks>
    /// The pins are reached by reflection because <c>Rows</c> deliberately exposes only the
    /// seven arrays: an accessor added for the test would be production surface that only the
    /// test uses. A freed <c>GCHandle</c> makes <c>Pointer</c> throw, which is the
    /// discriminator — a <c>Dispose</c> that cleared the list without freeing would leave
    /// every pointer readable.
    /// </remarks>
    [Fact]
    public void Dispose_ReleasesEveryPin()
    {
        AclRowMarshal.Rows rows = AclRowMarshal.Pin(new List<AclBinding> { Binding("pinned") });

        List<Utf8Marshal.PinnedUtf8String> pins = (List<Utf8Marshal.PinnedUtf8String>)
            typeof(AclRowMarshal.Rows)
                .GetField("_pinned", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(rows)!;

        // Control-positive: three strings per concrete row, all live before the dispose.
        Assert.Equal(3, pins.Count);
        Assert.All(pins, pin => Assert.NotEqual(IntPtr.Zero, pin.Pointer));

        rows.Dispose();

        Assert.All(pins, pin => Assert.Throws<InvalidOperationException>(() => pin.Pointer));

        // The submit's finally runs once, but an inner failure path can reach it twice.
        rows.Dispose();
    }

    // ------------------------------------------------------------------------------------
    // create_acls_async — the submit seam.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// A <see langword="null"/> timeout must become a <b>negative</b> <c>timeout_ms</c>,
    /// which the ABI reads as "unset, use the client default" — <b>not</b> <c>0</c>, which
    /// would mean "time out immediately".
    /// </summary>
    [Fact]
    public void NullTimeout_MapsToANegative_NotZero()
    {
        Assert.True(
            Capture(new[] { Binding("t") }, options: null).TimeoutMs < 0,
            "a null timeout must map to a NEGATIVE timeout_ms (unset), not 0");

        Assert.True(
            Capture(new[] { Binding("t") }, new CreateAclsOptions()).TimeoutMs < 0,
            "an explicit options object with a null timeout must map the same way");
    }

    /// <summary>
    /// An explicit timeout is forwarded verbatim, and <c>0</c> stays <c>0</c> — a real
    /// request ("do not wait"), distinct from <see langword="null"/>.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(45_678)]
    public void ExplicitTimeout_IsForwardedVerbatim(int timeoutMs) =>
        Assert.Equal(
            timeoutMs,
            Capture(new[] { Binding("t") }, new CreateAclsOptions { TimeoutMs = timeoutMs }).TimeoutMs);

    /// <summary>
    /// The seven arrays reach the P/Invoke with the batch's own count and one row per
    /// requested binding, in request order.
    /// </summary>
    [Fact]
    public void Submit_SendsSevenParallelArrays()
    {
        Captured captured = Capture(
            new[] { Binding("alpha"), Binding("beta") }, options: null);

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { (int)ResourceType.Topic, (int)ResourceType.Topic }, captured.ResourceTypes);
        Assert.Equal(new[] { "alpha", "beta" }, captured.ResourceNames);
        Assert.Equal(new[] { (int)PatternType.Literal, (int)PatternType.Literal }, captured.PatternTypes);
        Assert.Equal(new[] { "User:alice", "User:alice" }, captured.Principals);
        Assert.Equal(new[] { "*", "*" }, captured.Hosts);
        Assert.Equal(new[] { (int)AclOperation.Read, (int)AclOperation.Read }, captured.Operations);
        Assert.Equal(
            new[] { (int)AclPermissionType.Allow, (int)AclPermissionType.Allow },
            captured.PermissionTypes);
    }

    /// <summary>
    /// A repeated binding collapses to one row and one awaitable — Java keys its result on a
    /// <c>Map</c>, so two <em>value-equal</em> bindings are one entry.
    /// </summary>
    [Fact]
    public void DuplicateBindings_CollapseToOneRow()
    {
        // Distinct instances, equal by value — reference de-duplication would send two rows.
        Captured captured = Capture(new[] { Binding("dup"), Binding("dup") }, options: null);

        Assert.Equal(1, captured.Count);
        Assert.Equal(new[] { "dup" }, captured.ResourceNames);
    }

    /// <summary>
    /// A concrete binding never sends a null pointer through the submit either — the
    /// seam-level twin of <see cref="ConcreteRow_NeverSendsANullPointer"/>.
    /// </summary>
    [Fact]
    public void Submit_NeverSendsANullPointerForAConcreteRow()
    {
        Captured captured = Capture(
            new[]
            {
                new AclBinding(
                    new ResourcePattern(ResourceType.Topic, string.Empty, PatternType.Literal),
                    new AccessControlEntry(
                        string.Empty, string.Empty, AclOperation.Read, AclPermissionType.Allow)),
            },
            options: null);

        Assert.All(captured.ResourceNamePointers, pointer => Assert.NotEqual(IntPtr.Zero, pointer));
        Assert.All(captured.PrincipalPointers, pointer => Assert.NotEqual(IntPtr.Zero, pointer));
        Assert.All(captured.HostPointers, pointer => Assert.NotEqual(IntPtr.Zero, pointer));
    }

    /// <summary>
    /// A non-ASCII resource name survives the hand-rolled UTF-8 round trip (ffi §A3) — the
    /// guard against an <c>LPStr</c> slipping into the row projector.
    /// </summary>
    [Fact]
    public void NonAsciiName_RoundTripsThroughTheSubmit() =>
        Assert.Equal(
            new[] { "tópico-café-日本" },
            Capture(new[] { Binding("tópico-café-日本") }, options: null).ResourceNames);

    /// <summary>
    /// ⚠ <b>The inline-callback path (PLAN §4.5 case 2).</b> The ABI fires this RPC's
    /// callback <b>synchronously on the calling thread, before the submit returns</b> when
    /// the request cannot be submitted at all. Driving the production trampoline from inside
    /// the stand-in reproduces that exactly: it must not deadlock, every binding's awaitable
    /// must fault with the submit error, and the <c>GCHandle</c> must be freed once — which
    /// the client's <see cref="NativeAdminClient.Dispose"/> below would hang on otherwise.
    /// </summary>
    [Fact]
    public void InlineCallback_FaultsEveryKey_AndReleasesTheRegistration()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        AclBinding alpha = Binding("inline-alpha");
        AclBinding beta = Binding("inline-beta");

        CreateAclsResult result = admin.CreateAcls(
            new[] { alpha, beta },
            options: null,
            (handle, resourceTypes, resourceNames, patternTypes, principals, hosts,
             operations, permissionTypes, count, timeoutMs, callback, userData) =>
                // Synchronously, on this thread, before the submit returns — as the ABI does.
                callback(IntPtr.Zero, CapturedError(), userData));

        Assert.True(result.Values[alpha].IsFaulted);
        Assert.True(result.Values[beta].IsFaulted);

        KafkaException alphaFailure = Assert.IsType<KafkaException>(
            result.Values[alpha].Exception!.InnerException);
        Assert.Equal("captured", alphaFailure.Message);
        Assert.Equal(1, alphaFailure.Code);
    }

    /// <summary>
    /// Whatever the <c>GCHandle</c> carries, the submit publishes it <b>before</b> the
    /// P/Invoke — the ABI requires everything the callback needs to be published before the
    /// call, because the callback can fire inside it.
    /// </summary>
    [Fact]
    public void UserData_IsPublishedBeforeTheCall() =>
        Assert.NotEqual(IntPtr.Zero, Capture(new[] { Binding("t") }, options: null).UserData);

    // ------------------------------------------------------------------------------------
    // delete_acls_async — the same seven arrays, but the NULLABLE half of the rule.
    //
    // ⚠ Byte-identical signature to create_acls_async, opposite contract: here a NULL string
    // entry means "match any" and no enum combination is rejected (confluent_kafka.h:8121-8125).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>T-N1 at the RPC that can act on it.</b> A wildcard filter's three strings reach
    /// <c>delete_acls</c> as <see cref="IntPtr.Zero"/>, and a literal-empty filter's as
    /// non-null pointers to a NUL byte. Collapsing them here deletes ACLs the caller never
    /// named.
    /// </summary>
    [Fact]
    public void DeleteSubmit_KeepsNullApartFromEmpty()
    {
        Captured captured = CaptureDelete(
            new[]
            {
                new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Any),
                    new AccessControlEntryFilter(
                        null, null, AclOperation.Any, AclPermissionType.Any)),
                new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Any),
                    new AccessControlEntryFilter(
                        string.Empty, string.Empty, AclOperation.Any, AclPermissionType.Any)),
            },
            options: null);

        Assert.Equal(2, captured.Count);

        Assert.Equal(IntPtr.Zero, captured.ResourceNamePointers[0]);
        Assert.Equal(IntPtr.Zero, captured.PrincipalPointers[0]);
        Assert.Equal(IntPtr.Zero, captured.HostPointers[0]);

        Assert.NotEqual(IntPtr.Zero, captured.ResourceNamePointers[1]);
        Assert.NotEqual(IntPtr.Zero, captured.PrincipalPointers[1]);
        Assert.NotEqual(IntPtr.Zero, captured.HostPointers[1]);

        Assert.Equal(new string?[] { null, string.Empty }, captured.ResourceNames);
        Assert.Equal(new string?[] { null, string.Empty }, captured.Principals);
        Assert.Equal(new string?[] { null, string.Empty }, captured.Hosts);
    }

    /// <summary>
    /// The seven arrays reach the P/Invoke with the batch's own count, one row per filter, in
    /// request order — and the ANY sentinels are forwarded rather than screened out.
    /// </summary>
    [Fact]
    public void DeleteSubmit_SendsSevenParallelArrays()
    {
        Captured captured = CaptureDelete(
            new[] { DeleteFilter("alpha"), DeleteFilter("beta") }, options: null);

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { (int)ResourceType.Topic, (int)ResourceType.Topic }, captured.ResourceTypes);
        Assert.Equal(new[] { "alpha", "beta" }, captured.ResourceNames);
        Assert.Equal(new[] { (int)PatternType.Any, (int)PatternType.Any }, captured.PatternTypes);
        Assert.Equal(new[] { "User:alice", "User:alice" }, captured.Principals);
        Assert.Equal(new[] { "*", "*" }, captured.Hosts);
        Assert.Equal(new[] { (int)AclOperation.Any, (int)AclOperation.Any }, captured.Operations);
        Assert.Equal(
            new[] { (int)AclPermissionType.Any, (int)AclPermissionType.Any },
            captured.PermissionTypes);
    }

    /// <summary>
    /// A <see langword="null"/> timeout becomes a <b>negative</b> <c>timeout_ms</c>, and an
    /// explicit one is forwarded verbatim — <c>0</c> included.
    /// </summary>
    [Fact]
    public void DeleteSubmit_MapsTheTimeout()
    {
        Assert.True(
            CaptureDelete(new[] { DeleteFilter("t") }, options: null).TimeoutMs < 0,
            "a null timeout must map to a NEGATIVE timeout_ms (unset), not 0");
        Assert.True(
            CaptureDelete(new[] { DeleteFilter("t") }, new DeleteAclsOptions()).TimeoutMs < 0,
            "an explicit options object with a null timeout must map the same way");

        Assert.Equal(
            0,
            CaptureDelete(
                new[] { DeleteFilter("t") }, new DeleteAclsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            45_678,
            CaptureDelete(
                new[] { DeleteFilter("t") }, new DeleteAclsOptions { TimeoutMs = 45_678 }).TimeoutMs);
    }

    /// <summary>
    /// A repeated filter collapses to one row — Java keys its result on a map, so two
    /// value-equal filters are one entry.
    /// </summary>
    [Fact]
    public void DeleteSubmit_DuplicateFiltersCollapseToOneRow()
    {
        Captured captured = CaptureDelete(
            new[] { DeleteFilter("dup"), DeleteFilter("dup") }, options: null);

        Assert.Equal(1, captured.Count);
        Assert.Equal(new[] { "dup" }, captured.ResourceNames);
    }

    /// <summary>
    /// ⚠ <b>The inline-callback path.</b> Driving the production trampoline from inside the
    /// stand-in must not deadlock, must fault every filter's awaitable with the submit error,
    /// and must free the <c>GCHandle</c> once — which the client's <c>Dispose</c> would hang
    /// on otherwise.
    /// </summary>
    [Fact]
    public async Task DeleteInlineCallback_FaultsEveryKey_AndReleasesTheRegistration()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        AclBindingFilter alpha = DeleteFilter("inline-alpha");
        AclBindingFilter beta = DeleteFilter("inline-beta");

        DeleteAclsResult result = admin.DeleteAcls(
            new[] { alpha, beta },
            options: null,
            (handle, resourceTypes, resourceNames, patternTypes, principals, hosts,
             operations, permissionTypes, count, timeoutMs, callback, userData) =>
                // Synchronously, on this thread, before the submit returns — as the ABI does.
                callback(IntPtr.Zero, CapturedError(), userData));

        Assert.True(result.Values[alpha].IsFaulted);
        Assert.True(result.Values[beta].IsFaulted);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => result.All());
        Assert.Equal("captured", failure.Message);
        Assert.Equal(1, failure.Code);
    }

    /// <summary>
    /// The <c>GCHandle</c> is published before the P/Invoke, because the callback can fire
    /// inside it.
    /// </summary>
    [Fact]
    public void DeleteUserData_IsPublishedBeforeTheCall() =>
        Assert.NotEqual(
            IntPtr.Zero, CaptureDelete(new[] { DeleteFilter("t") }, options: null).UserData);

    // ------------------------------------------------------------------------------------
    // describe_acls — a SINGLE filter, so seven SCALARS rather than seven arrays.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>T-N1 on the scalar path.</b> The same null-versus-empty rule as the array RPCs,
    /// applied to one row's worth of scalars: a wildcard field crosses as
    /// <see cref="IntPtr.Zero"/>, a literal-empty field as a non-null pointer to a NUL byte.
    /// </summary>
    [Fact]
    public void DescribeSubmit_KeepsNullApartFromEmpty()
    {
        DescribeCaptured wildcard = CaptureDescribe(
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Any),
                new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any)),
            options: null);

        Assert.Equal(IntPtr.Zero, wildcard.ResourceNamePointer);
        Assert.Equal(IntPtr.Zero, wildcard.PrincipalPointer);
        Assert.Equal(IntPtr.Zero, wildcard.HostPointer);
        Assert.Null(wildcard.ResourceName);
        Assert.Null(wildcard.Principal);
        Assert.Null(wildcard.Host);

        DescribeCaptured empty = CaptureDescribe(
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Any),
                new AccessControlEntryFilter(
                    string.Empty, string.Empty, AclOperation.Any, AclPermissionType.Any)),
            options: null);

        Assert.NotEqual(IntPtr.Zero, empty.ResourceNamePointer);
        Assert.NotEqual(IntPtr.Zero, empty.PrincipalPointer);
        Assert.NotEqual(IntPtr.Zero, empty.HostPointer);
        Assert.Equal(string.Empty, empty.ResourceName);
        Assert.Equal(string.Empty, empty.Principal);
        Assert.Equal(string.Empty, empty.Host);
    }

    /// <summary>
    /// All seven fields reach the P/Invoke as scalars, in the ABI's own order, with the ANY
    /// sentinels forwarded rather than screened out.
    /// </summary>
    [Fact]
    public void DescribeSubmit_SendsEveryScalar()
    {
        DescribeCaptured captured = CaptureDescribe(
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Group, "alpha", PatternType.Prefixed),
                new AccessControlEntryFilter(
                    "User:bob", "10.0.0.1", AclOperation.Any, AclPermissionType.Any)),
            options: null);

        Assert.Equal((int)ResourceType.Group, captured.ResourceType);
        Assert.Equal("alpha", captured.ResourceName);
        Assert.Equal((int)PatternType.Prefixed, captured.PatternType);
        Assert.Equal("User:bob", captured.Principal);
        Assert.Equal("10.0.0.1", captured.Host);
        Assert.Equal((int)AclOperation.Any, captured.Operation);
        Assert.Equal((int)AclPermissionType.Any, captured.PermissionType);
    }

    /// <summary>A non-ASCII name survives the UTF-8 round trip through the seam.</summary>
    [Fact]
    public void DescribeSubmit_NonAsciiNameRoundTrips() =>
        Assert.Equal(
            "主题-ü",
            CaptureDescribe(DescribeFilter("主题-ü"), options: null).ResourceName);

    /// <summary>
    /// A <see langword="null"/> timeout becomes a <b>negative</b> <c>timeout_ms</c>, and an
    /// explicit one is forwarded verbatim — <c>0</c> included.
    /// </summary>
    [Fact]
    public void DescribeSubmit_MapsTheTimeout()
    {
        Assert.True(
            CaptureDescribe(DescribeFilter("t"), options: null).TimeoutMs < 0,
            "a null timeout must map to a NEGATIVE timeout_ms (unset), not 0");
        Assert.True(
            CaptureDescribe(DescribeFilter("t"), new DescribeAclsOptions()).TimeoutMs < 0,
            "an explicit options object with a null timeout must map the same way");

        Assert.Equal(
            0,
            CaptureDescribe(DescribeFilter("t"), new DescribeAclsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            45_678,
            CaptureDescribe(
                DescribeFilter("t"), new DescribeAclsOptions { TimeoutMs = 45_678 }).TimeoutMs);
    }

    /// <summary>
    /// ⚠ <b>The inline-callback path.</b> Driving the production trampoline from inside the
    /// stand-in must not deadlock, must fault the single awaitable with the submit error, and
    /// must free the <c>GCHandle</c> once — which the client's <c>Dispose</c> would hang on
    /// otherwise.
    /// </summary>
    [Fact]
    public async Task DescribeInlineCallback_FaultsTheTask_AndReleasesTheRegistration()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        DescribeAclsResult result = admin.DescribeAcls(
            DescribeFilter("inline"),
            options: null,
            (handle, resourceType, resourceName, patternType, principal, host,
             operation, permissionType, timeoutMs, callback, userData) =>
                // Synchronously, on this thread, before the submit returns — as the ABI does.
                callback(IntPtr.Zero, CapturedError(), userData));

        Assert.True(result.Values().IsFaulted);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => result.Values());
        Assert.Equal("captured", failure.Message);
        Assert.Equal(1, failure.Code);
    }

    /// <summary>
    /// The <c>GCHandle</c> is published before the P/Invoke, because the callback can fire
    /// inside it.
    /// </summary>
    [Fact]
    public void DescribeUserData_IsPublishedBeforeTheCall() =>
        Assert.NotEqual(IntPtr.Zero, CaptureDescribe(DescribeFilter("t"), options: null).UserData);

    // ------------------------------------------------------------------------------------
    // describe_client_quotas — three parallel arrays plus the `strict` bool.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>T-N3.</b> The three <see cref="ClientQuotaFilterComponent"/> factories submit
    /// <b>three distinct</b> match types — 0 EXACT / 1 DEFAULT / 2 SPECIFIED. A
    /// <c>string?</c>-based model has only two states and collapses DEFAULT into SPECIFIED,
    /// silently turning "the default user's quota" into "every named user's quota".
    /// </summary>
    [Fact]
    public void QuotaSubmit_TheThreeFactories_ProduceThreeDistinctMatchTypes()
    {
        QuotaCaptured captured = CaptureQuotas(
            ClientQuotaFilter.Contains(
                new[]
                {
                    ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "alice"),
                    ClientQuotaFilterComponent.OfDefaultEntity(ClientQuotaEntity.ClientId),
                    ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.Ip),
                }),
            options: null);

        Assert.Equal(3, captured.Count);
        Assert.Equal(
            new[]
            {
                (int)ClientQuotaMatchType.Exact,
                (int)ClientQuotaMatchType.Default,
                (int)ClientQuotaMatchType.Specified,
            },
            captured.MatchTypes);

        // The three values really are distinct — a collapsed model fails here first.
        Assert.Equal(3, captured.MatchTypes.Distinct().Count());

        Assert.Equal(new[] { "user", "client-id", "ip" }, captured.EntityTypes);

        // Only EXACT carries a name; the other two must submit a NULL, not "".
        Assert.Equal("alice", captured.MatchNames[0]);
        Assert.Null(captured.MatchNames[1]);
        Assert.Null(captured.MatchNames[2]);
        Assert.Equal(IntPtr.Zero, captured.MatchNamePointers[1]);
        Assert.Equal(IntPtr.Zero, captured.MatchNamePointers[2]);
    }

    /// <summary>
    /// An EXACT component naming <c>""</c> submits a <b>non-null</b> pointer to a NUL byte,
    /// distinct from the null a nameless component submits.
    /// </summary>
    [Fact]
    public void QuotaSubmit_AnExactEmptyName_IsAPointerApartFromNoName()
    {
        QuotaCaptured captured = CaptureQuotas(
            ClientQuotaFilter.Contains(
                new[]
                {
                    ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, string.Empty),
                    ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User),
                }),
            options: null);

        Assert.NotEqual(IntPtr.Zero, captured.MatchNamePointers[0]);
        Assert.Equal(string.Empty, captured.MatchNames[0]);

        Assert.Equal(IntPtr.Zero, captured.MatchNamePointers[1]);
        Assert.Null(captured.MatchNames[1]);
    }

    /// <summary>
    /// ⚠ <c>count == 0</c> with <c>strict == false</c> is Java's
    /// <see cref="ClientQuotaFilter.All"/> — the most common call — and must reach the ABI
    /// as exactly that rather than being rejected as empty.
    /// </summary>
    [Fact]
    public void QuotaSubmit_AllFilter_IsZeroComponentsAndNotStrict()
    {
        QuotaCaptured captured = CaptureQuotas(ClientQuotaFilter.All(), options: null);

        Assert.Equal(0, captured.Count);
        Assert.False(captured.Strict);
        Assert.Empty(captured.EntityTypes);
        Assert.Empty(captured.MatchTypes);
    }

    /// <summary>
    /// <c>containsOnly</c> submits <c>strict == true</c> and <c>contains</c> <c>false</c> —
    /// the two are not merged, and the one-byte marshalling is exercised both ways.
    /// </summary>
    [Fact]
    public void QuotaSubmit_StrictIsForwardedBothWays()
    {
        ClientQuotaFilterComponent[] components =
            { ClientQuotaFilterComponent.OfEntityType(ClientQuotaEntity.User) };

        Assert.True(CaptureQuotas(ClientQuotaFilter.ContainsOnly(components), options: null).Strict);
        Assert.False(CaptureQuotas(ClientQuotaFilter.Contains(components), options: null).Strict);
    }

    /// <summary>
    /// A <see langword="null"/> timeout becomes a <b>negative</b> <c>timeout_ms</c>, and an
    /// explicit one is forwarded verbatim — <c>0</c> included.
    /// </summary>
    [Fact]
    public void QuotaSubmit_MapsTheTimeout()
    {
        Assert.True(
            CaptureQuotas(ClientQuotaFilter.All(), options: null).TimeoutMs < 0,
            "a null timeout must map to a NEGATIVE timeout_ms (unset), not 0");
        Assert.True(
            CaptureQuotas(ClientQuotaFilter.All(), new DescribeClientQuotasOptions()).TimeoutMs < 0,
            "an explicit options object with a null timeout must map the same way");

        Assert.Equal(
            0,
            CaptureQuotas(
                ClientQuotaFilter.All(), new DescribeClientQuotasOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            45_678,
            CaptureQuotas(
                ClientQuotaFilter.All(),
                new DescribeClientQuotasOptions { TimeoutMs = 45_678 }).TimeoutMs);
    }

    /// <summary>A non-ASCII entity name survives the UTF-8 round trip through the seam.</summary>
    [Fact]
    public void QuotaSubmit_NonAsciiNameRoundTrips() =>
        Assert.Equal(
            "用户-ü",
            CaptureQuotas(
                ClientQuotaFilter.Contains(
                    new[] { ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, "用户-ü") }),
                options: null).MatchNames[0]);

    /// <summary>
    /// ⚠ <b>The inline-callback path.</b> Driving the production trampoline from inside the
    /// stand-in must not deadlock, must fault the single awaitable, and must free the
    /// <c>GCHandle</c> once.
    /// </summary>
    [Fact]
    public async Task QuotaInlineCallback_FaultsTheTask_AndReleasesTheRegistration()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        DescribeClientQuotasResult result = admin.DescribeClientQuotas(
            ClientQuotaFilter.All(),
            options: null,
            (handle, entityTypes, matchTypes, matchNames, count, strict, timeoutMs, callback, userData) =>
                // Synchronously, on this thread, before the submit returns — as the ABI does.
                callback(IntPtr.Zero, CapturedError(), userData));

        Assert.True(result.Entities().IsFaulted);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => result.Entities());
        Assert.Equal("captured", failure.Message);
        Assert.Equal(1, failure.Code);
    }

    /// <summary>
    /// The <c>GCHandle</c> is published before the P/Invoke, because the callback can fire
    /// inside it.
    /// </summary>
    [Fact]
    public void QuotaUserData_IsPublishedBeforeTheCall() =>
        Assert.NotEqual(IntPtr.Zero, CaptureQuotas(ClientQuotaFilter.All(), options: null).UserData);

    // ------------------------------------------------------------------------------------
    // alter_client_quotas — the phase's only RAGGED 2-level input.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>T-N4.</b> <c>Op(key, null)</c> submits <c>op_has_values == 0</c> — Java's
    /// <em>remove this quota</em> — while <c>Op(key, 0.0)</c> submits <c>1</c> with the value
    /// <c>0.0</c>. Two <b>different</b> requests, not one.
    /// </summary>
    /// <remarks>
    /// Every <c>double</c>, <c>0</c> included, is a legal quota value, so no sentinel could
    /// carry the distinction (<c>confluent_kafka.h:8291-8295</c>) — a sentinel-based encoding
    /// silently turns "set this quota to zero" into "delete it".
    /// </remarks>
    [Fact]
    public void AlterSubmit_ANullOpValueRemoves_AndZeroSets()
    {
        AlterCaptured captured = CaptureAlter(
            new[]
            {
                new ClientQuotaAlteration(
                    Entity(ClientQuotaEntity.User, "ternary"),
                    new[]
                    {
                        new ClientQuotaAlteration.Op("producer_byte_rate", null),
                        new ClientQuotaAlteration.Op("consumer_byte_rate", 0d),
                    }),
            },
            options: null);

        Assert.Equal(new[] { 2 }, captured.OpCounts);
        Assert.Equal(new[] { "producer_byte_rate", "consumer_byte_rate" }, captured.OpKeys[0]);

        // The flag is the only channel, and the two ops disagree on it.
        Assert.Equal(new byte[] { 0, 1 }, captured.OpHasValues[0]);
        Assert.NotEqual(captured.OpHasValues[0][0], captured.OpHasValues[0][1]);

        // The set op carries 0.0 — the value a sentinel encoding would have to reuse.
        Assert.Equal(0d, captured.OpValues[0][1]);
    }

    /// <summary>
    /// ⚠ <b>The <c>bool**</c> width.</b> The presence flags are one byte each, so an
    /// alternating pattern reads back at consecutive byte offsets. A 4-byte Win32 <c>BOOL</c>
    /// encoding would put zeroes at offsets 1..3 and fail here.
    /// </summary>
    [Fact]
    public void AlterSubmit_OpHasValues_AreOneBytePerOp()
    {
        AlterCaptured captured = CaptureAlter(
            new[]
            {
                new ClientQuotaAlteration(
                    Entity(ClientQuotaEntity.User, "width"),
                    new[]
                    {
                        new ClientQuotaAlteration.Op("a", null),
                        new ClientQuotaAlteration.Op("b", 1d),
                        new ClientQuotaAlteration.Op("c", null),
                        new ClientQuotaAlteration.Op("d", 2d),
                    }),
            },
            options: null);

        Assert.Equal(new byte[] { 0, 1, 0, 1 }, captured.OpHasValues[0]);
        Assert.Equal(new[] { 0d, 1d, 0d, 2d }, captured.OpValues[0]);
    }

    /// <summary>
    /// ⚠⚠ <b>T-N5.</b> An entity name of <see langword="null"/>, <c>""</c> and <c>"x"</c>
    /// produce <b>three distinct</b> submitted rows: a NULL pointer (the built-in default
    /// entity), a non-null pointer to a NUL byte, and a non-null pointer to <c>"x"</c>.
    /// </summary>
    /// <remarks>
    /// A null name is Java's null map value, which is neither omitting the type nor the name
    /// <c>""</c> (<c>confluent_kafka.h:8287-8290</c>).
    /// </remarks>
    [Fact]
    public void AlterSubmit_TheEntityNameTernary_ProducesThreeDistinctRows()
    {
        AlterCaptured captured = CaptureAlter(
            new[]
            {
                new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, null), s_noOps),
                new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, string.Empty), s_noOps),
                new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, "x"), s_noOps),
            },
            options: null);

        Assert.Equal(3, captured.Count);
        Assert.Equal(new[] { 1, 1, 1 }, captured.EntityCounts);

        Assert.Equal(IntPtr.Zero, captured.EntityNamePointers[0][0]);
        Assert.Null(captured.EntityNames[0][0]);

        Assert.NotEqual(IntPtr.Zero, captured.EntityNamePointers[1][0]);
        Assert.Equal(string.Empty, captured.EntityNames[1][0]);

        Assert.NotEqual(IntPtr.Zero, captured.EntityNamePointers[2][0]);
        Assert.Equal("x", captured.EntityNames[2][0]);
    }

    /// <summary>
    /// ⚠ <b>The ragged shape.</b> Two alterations with <b>different</b> entity counts and
    /// <b>different</b> op counts each land against their own row's counts — a test with
    /// uniform lengths would pass even if the inner arrays were indexed with the wrong row's
    /// count.
    /// </summary>
    [Fact]
    public void AlterSubmit_BothLevelsAreRagged_AndIndependentOfEachOther()
    {
        AlterCaptured captured = CaptureAlter(
            new[]
            {
                // Two entity types, one op.
                new ClientQuotaAlteration(
                    new ClientQuotaEntity(
                        new Dictionary<string, string?>(StringComparer.Ordinal)
                        {
                            [ClientQuotaEntity.User] = "u",
                            [ClientQuotaEntity.ClientId] = "c",
                        }),
                    new[] { new ClientQuotaAlteration.Op("only", 1d) }),

                // One entity type, three ops — the opposite raggedness.
                new ClientQuotaAlteration(
                    Entity(ClientQuotaEntity.Ip, "10.0.0.1"),
                    new[]
                    {
                        new ClientQuotaAlteration.Op("first", 1d),
                        new ClientQuotaAlteration.Op("second", null),
                        new ClientQuotaAlteration.Op("third", 3d),
                    }),
            },
            options: null);

        Assert.Equal(2, captured.Count);
        Assert.Equal(new[] { 2, 1 }, captured.EntityCounts);
        Assert.Equal(new[] { 1, 3 }, captured.OpCounts);

        // Row 0's (type, name) pairs, as a set — the entity's own map order is not a contract.
        Assert.Equal(
            new[] { "client-id=c", "user=u" },
            Pairs(captured.EntityTypes[0], captured.EntityNames[0]));
        Assert.Equal(new[] { "ip=10.0.0.1" }, Pairs(captured.EntityTypes[1], captured.EntityNames[1]));

        Assert.Equal(new[] { "only" }, captured.OpKeys[0]);
        Assert.Equal(new[] { "first", "second", "third" }, captured.OpKeys[1]);
        Assert.Equal(new byte[] { 1 }, captured.OpHasValues[0]);
        Assert.Equal(new byte[] { 1, 0, 1 }, captured.OpHasValues[1]);
    }

    /// <summary>
    /// An alteration with no ops submits a count of 0 and a <b>NULL</b> inner pointer, which
    /// the core null-checks before reading — never a pinned empty array, whose address is
    /// undocumented (ffi §A4).
    /// </summary>
    [Fact]
    public void AlterSubmit_AnAlterationWiths_noOps_SubmitsANullInnerPointer()
    {
        AlterCaptured captured = CaptureAlter(
            new[] { new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, "no-ops"), s_noOps) },
            options: null);

        Assert.Equal(new[] { 0 }, captured.OpCounts);
        Assert.Equal(IntPtr.Zero, captured.OpKeyRows[0]);
        Assert.Equal(IntPtr.Zero, captured.OpValueRows[0]);
        Assert.Equal(IntPtr.Zero, captured.OpHasValueRows[0]);

        // The entity level is still populated — the two levels are independent.
        Assert.Equal(new[] { 1 }, captured.EntityCounts);
        Assert.NotEqual(IntPtr.Zero, captured.EntityTypeRows[0]);
    }

    /// <summary>
    /// <c>validateOnly</c> reaches the seam both ways — Java's
    /// <c>AlterClientQuotasOptions.validateOnly</c>, which validates without applying.
    /// </summary>
    [Fact]
    public void AlterSubmit_ValidateOnlyIsForwardedBothWays()
    {
        ClientQuotaAlteration[] alterations =
            { new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, "vo"), s_noOps) };

        Assert.False(CaptureAlter(alterations, options: null).ValidateOnly);
        Assert.False(
            CaptureAlter(alterations, new AlterClientQuotasOptions()).ValidateOnly,
            "Java's default is false (AlterClientQuotasOptions.java:27)");
        Assert.True(
            CaptureAlter(alterations, new AlterClientQuotasOptions { ValidateOnly = true })
                .ValidateOnly);
    }

    /// <summary>
    /// A <see langword="null"/> timeout becomes a <b>negative</b> <c>timeout_ms</c>, and an
    /// explicit one is forwarded verbatim — <c>0</c> included.
    /// </summary>
    [Fact]
    public void AlterSubmit_MapsTheTimeout()
    {
        ClientQuotaAlteration[] alterations =
            { new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, "t"), s_noOps) };

        Assert.True(
            CaptureAlter(alterations, options: null).TimeoutMs < 0,
            "a null timeout must map to a NEGATIVE timeout_ms (unset), not 0");
        Assert.True(
            CaptureAlter(alterations, new AlterClientQuotasOptions()).TimeoutMs < 0,
            "an explicit options object with a null timeout must map the same way");

        Assert.Equal(
            0, CaptureAlter(alterations, new AlterClientQuotasOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            45_678,
            CaptureAlter(alterations, new AlterClientQuotasOptions { TimeoutMs = 45_678 }).TimeoutMs);
    }

    /// <summary>A non-ASCII entity name and op key survive the UTF-8 round trip at the seam.</summary>
    [Fact]
    public void AlterSubmit_NonAsciiRoundTrips()
    {
        AlterCaptured captured = CaptureAlter(
            new[]
            {
                new ClientQuotaAlteration(
                    Entity(ClientQuotaEntity.User, "用户-ü"),
                    new[] { new ClientQuotaAlteration.Op("配额-café", 7d) }),
            },
            options: null);

        Assert.Equal("用户-ü", captured.EntityNames[0][0]);
        Assert.Equal("配额-café", captured.OpKeys[0][0]);
    }

    /// <summary>
    /// ⚠ <b>The inline-callback path.</b> Driving the production trampoline from inside the
    /// stand-in must not deadlock, must fault <b>every</b> per-entity awaitable with the
    /// submit error, and must free the <c>GCHandle</c> once.
    /// </summary>
    [Fact]
    public async Task AlterInlineCallback_FaultsEveryTask_AndReleasesTheRegistration()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        AlterClientQuotasResult result = admin.AlterClientQuotas(
            new[]
            {
                new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, "inline-a"), s_noOps),
                new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, "inline-b"), s_noOps),
            },
            options: null,
            (handle, entityTypes, entityNames, entityCounts, opKeys, opValues, opHasValues,
             opCounts, count, timeoutMs, validateOnly, callback, userData) =>
                // Synchronously, on this thread, before the submit returns — as the ABI does.
                callback(IntPtr.Zero, CapturedError(), userData));

        Assert.Equal(2, result.Values.Count);

        // A top-level submit failure faults EVERY per-key task: none is left hanging.
        foreach (Task task in result.Values.Values)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => task);
            Assert.Equal("captured", failure.Message);
            Assert.Equal(1, failure.Code);
        }
    }

    /// <summary>
    /// The <c>GCHandle</c> is published before the P/Invoke, because the callback can fire
    /// inside it.
    /// </summary>
    [Fact]
    public void AlterUserData_IsPublishedBeforeTheCall() =>
        Assert.NotEqual(
            IntPtr.Zero,
            CaptureAlter(
                new[] { new ClientQuotaAlteration(Entity(ClientQuotaEntity.User, "ud"), s_noOps) },
                options: null).UserData);

    // ------------------------------------------------------------------------------------
    // Harness.
    // ------------------------------------------------------------------------------------

    /// <summary>A binding differing only in resource name, so batches stay distinguishable.</summary>
    private static AclBinding Binding(string name) =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

    /// <summary>
    /// Runs the production submit with a stand-in that records the seven arrays instead of
    /// calling native, then completes the operation through the <b>production</b> trampoline
    /// so the <c>GCHandle</c> and the span-the-op reference are released before the client is
    /// disposed.
    /// </summary>
    private static Captured Capture(IEnumerable<AclBinding> acls, CreateAclsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        CreateAclsResult result = admin.CreateAcls(
            acls,
            options,
            (handle, resourceTypes, resourceNames, patternTypes, principals, hosts,
             operations, permissionTypes, count, timeoutMs, callback, userData) =>
            {
                captured.Count = count;
                captured.TimeoutMs = timeoutMs;
                captured.ResourceTypes = resourceTypes;
                captured.PatternTypes = patternTypes;
                captured.Operations = operations;
                captured.PermissionTypes = permissionTypes;

                captured.ResourceNamePointers = resourceNames;
                captured.PrincipalPointers = principals;
                captured.HostPointers = hosts;

                // ⚠ Decoded HERE: production unpins every string the moment this returns.
                captured.ResourceNames = Decode(resourceNames);
                captured.Principals = Decode(principals);
                captured.Hosts = Decode(hosts);
                captured.UserData = userData;
            });

        AdminCallbacks.CreateAcls(IntPtr.Zero, CapturedError(), captured.UserData);

        // Observe the fault the trampoline just delivered. All() awaits already-faulted
        // sources, so it completes synchronously and this read is race-free.
        Assert.NotNull(result.All().Exception);
        return captured;
    }

    /// <summary>
    /// Decodes one pointer array in full — <b>by the array's own length</b>, never by a
    /// count, so a mismatch between the two is reported by an assertion rather than by an
    /// index-out-of-range inside the stand-in.
    /// </summary>
    private static IReadOnlyList<string?> Decode(IntPtr[] values) =>
        values.Select(value => Utf8Marshal.PtrToString(value)).ToArray();

    /// <summary>
    /// An <b>owned</b> error for the trampoline to consume, standing in for the one native
    /// would hand the callback. The trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/>, so it must never be freed here.
    /// </summary>
    private static IntPtr CapturedError()
    {
        using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("captured");
        IntPtr error = NativeMethods.KafkaErrorNew(1, message.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    /// <summary>
    /// A filter differing only in resource name, with ANY everywhere else — the combination
    /// <c>create_acls</c> rejects and <c>delete_acls</c> accepts.
    /// </summary>
    private static AclBindingFilter DeleteFilter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Any),
            new AccessControlEntryFilter(
                "User:alice", "*", AclOperation.Any, AclPermissionType.Any));

    /// <summary>
    /// The <c>delete_acls</c> twin of <see cref="Capture"/>: records the seven arrays, then
    /// completes the operation through the <b>production</b> trampoline.
    /// </summary>
    private static Captured CaptureDelete(
        IEnumerable<AclBindingFilter> filters, DeleteAclsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        DeleteAclsResult result = admin.DeleteAcls(
            filters,
            options,
            (handle, resourceTypes, resourceNames, patternTypes, principals, hosts,
             operations, permissionTypes, count, timeoutMs, callback, userData) =>
            {
                captured.Count = count;
                captured.TimeoutMs = timeoutMs;
                captured.ResourceTypes = resourceTypes;
                captured.PatternTypes = patternTypes;
                captured.Operations = operations;
                captured.PermissionTypes = permissionTypes;

                captured.ResourceNamePointers = resourceNames;
                captured.PrincipalPointers = principals;
                captured.HostPointers = hosts;

                // ⚠ Decoded HERE: production unpins every string the moment this returns.
                captured.ResourceNames = Decode(resourceNames);
                captured.Principals = Decode(principals);
                captured.Hosts = Decode(hosts);
                captured.UserData = userData;
            });

        AdminCallbacks.DeleteAcls(IntPtr.Zero, CapturedError(), captured.UserData);

        // Observe the fault the trampoline just delivered, so nothing is left unobserved.
        Assert.NotNull(result.All().Exception);
        return captured;
    }

    /// <summary>
    /// A single-filter stand-in differing only in resource name, with ANY everywhere else.
    /// </summary>
    private static AclBindingFilter DescribeFilter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Any),
            new AccessControlEntryFilter(
                "User:alice", "*", AclOperation.Any, AclPermissionType.Any));

    /// <summary>
    /// The <c>describe_acls</c> twin of <see cref="Capture"/>: records the seven
    /// <b>scalars</b>, then completes the operation through the <b>production</b> trampoline
    /// so the <c>GCHandle</c> and the span-the-op reference are released before the client is
    /// disposed.
    /// </summary>
    private static DescribeCaptured CaptureDescribe(
        AclBindingFilter filter, DescribeAclsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        DescribeCaptured captured = new DescribeCaptured();
        DescribeAclsResult result = admin.DescribeAcls(
            filter,
            options,
            (handle, resourceType, resourceName, patternType, principal, host,
             operation, permissionType, timeoutMs, callback, userData) =>
            {
                captured.ResourceType = resourceType;
                captured.PatternType = patternType;
                captured.Operation = operation;
                captured.PermissionType = permissionType;
                captured.TimeoutMs = timeoutMs;

                captured.ResourceNamePointer = resourceName;
                captured.PrincipalPointer = principal;
                captured.HostPointer = host;

                // ⚠ Decoded HERE: production unpins every string the moment this returns.
                captured.ResourceName = Utf8Marshal.PtrToString(resourceName);
                captured.Principal = Utf8Marshal.PtrToString(principal);
                captured.Host = Utf8Marshal.PtrToString(host);
                captured.UserData = userData;
            });

        AdminCallbacks.DescribeAcls(IntPtr.Zero, CapturedError(), captured.UserData);

        // Observe the fault the trampoline just delivered, so nothing is left unobserved.
        Assert.NotNull(result.Values().Exception);
        return captured;
    }

    /// <summary>One <c>describe_acls</c> submit, exactly as it crossed the seam.</summary>
    private sealed class DescribeCaptured
    {
        internal int ResourceType { get; set; } = int.MinValue;

        internal int PatternType { get; set; } = int.MinValue;

        internal int Operation { get; set; } = int.MinValue;

        internal int PermissionType { get; set; } = int.MinValue;

        internal int TimeoutMs { get; set; }

        internal IntPtr ResourceNamePointer { get; set; } = new IntPtr(-1);

        internal IntPtr PrincipalPointer { get; set; } = new IntPtr(-1);

        internal IntPtr HostPointer { get; set; } = new IntPtr(-1);

        internal string? ResourceName { get; set; }

        internal string? Principal { get; set; }

        internal string? Host { get; set; }

        internal IntPtr UserData { get; set; }
    }

    /// <summary>
    /// The <c>describe_client_quotas</c> twin of <see cref="Capture"/>: records the three
    /// arrays plus <c>strict</c>, then completes the operation through the <b>production</b>
    /// trampoline.
    /// </summary>
    private static QuotaCaptured CaptureQuotas(
        ClientQuotaFilter filter, DescribeClientQuotasOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        QuotaCaptured captured = new QuotaCaptured();
        DescribeClientQuotasResult result = admin.DescribeClientQuotas(
            filter,
            options,
            (handle, entityTypes, matchTypes, matchNames, count, strict, timeoutMs, callback, userData) =>
            {
                captured.Count = count;
                captured.Strict = strict;
                captured.TimeoutMs = timeoutMs;
                captured.MatchTypes = matchTypes;
                captured.MatchNamePointers = matchNames;

                // ⚠ Decoded HERE: production unpins every string the moment this returns.
                captured.EntityTypes = Decode(entityTypes);
                captured.MatchNames = Decode(matchNames);
                captured.UserData = userData;
            });

        AdminCallbacks.DescribeClientQuotas(IntPtr.Zero, CapturedError(), captured.UserData);

        // Observe the fault the trampoline just delivered, so nothing is left unobserved.
        Assert.NotNull(result.Entities().Exception);
        return captured;
    }

    /// <summary>An alteration with no ops — the entity level exercised on its own.</summary>
    private static readonly ClientQuotaAlteration.Op[] s_noOps = Array.Empty<ClientQuotaAlteration.Op>();

    /// <summary>A single-entry entity; a null <paramref name="name"/> is the default entity.</summary>
    private static ClientQuotaEntity Entity(string type, string? name) =>
        new ClientQuotaEntity(
            new Dictionary<string, string?>(StringComparer.Ordinal) { [type] = name });

    /// <summary>
    /// Row <paramref name="types"/>/<paramref name="names"/> rendered as sorted
    /// <c>type=name</c> pairs, so the assertion does not depend on the entity map's order.
    /// </summary>
    private static IReadOnlyList<string> Pairs(
        IReadOnlyList<string?> types, IReadOnlyList<string?> names) =>
        types
            .Select((type, index) => string.Concat(type, "=", names[index] ?? "<null>"))
            .OrderBy(pair => pair, StringComparer.Ordinal)
            .ToArray();

    /// <summary>
    /// The <c>alter_client_quotas</c> twin of <see cref="Capture"/>: records the seven arrays
    /// — decoding <b>both</b> ragged levels against their own row's count — then completes the
    /// operation through the <b>production</b> trampoline.
    /// </summary>
    private static AlterCaptured CaptureAlter(
        IEnumerable<ClientQuotaAlteration> entries, AlterClientQuotasOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        AlterCaptured captured = new AlterCaptured();
        AlterClientQuotasResult result = admin.AlterClientQuotas(
            entries,
            options,
            (handle, entityTypes, entityNames, entityCounts, opKeys, opValues, opHasValues,
             opCounts, count, timeoutMs, validateOnly, callback, userData) =>
            {
                captured.Count = count;
                captured.TimeoutMs = timeoutMs;
                captured.ValidateOnly = validateOnly;
                captured.EntityCounts = (int[])entityCounts.Clone();
                captured.OpCounts = (int[])opCounts.Clone();
                captured.EntityTypeRows = (IntPtr[])entityTypes.Clone();
                captured.OpKeyRows = (IntPtr[])opKeys.Clone();
                captured.OpValueRows = (IntPtr[])opValues.Clone();
                captured.OpHasValueRows = (IntPtr[])opHasValues.Clone();

                // ⚠ Decoded HERE: production unpins every string and every inner buffer the
                // moment this returns. Each row is read with ITS OWN count, which is what
                // makes the raggedness observable.
                for (int row = 0; row < count; row++)
                {
                    captured.EntityTypes.Add(ReadStrings(entityTypes[row], entityCounts[row]));
                    captured.EntityNames.Add(ReadStrings(entityNames[row], entityCounts[row]));
                    captured.EntityNamePointers.Add(ReadPointers(entityNames[row], entityCounts[row]));
                    captured.OpKeys.Add(ReadStrings(opKeys[row], opCounts[row]));
                    captured.OpValues.Add(ReadDoubles(opValues[row], opCounts[row]));
                    captured.OpHasValues.Add(ReadBytes(opHasValues[row], opCounts[row]));
                }

                captured.UserData = userData;
            });

        AdminCallbacks.AlterClientQuotas(IntPtr.Zero, CapturedError(), captured.UserData);

        // Observe the faults the trampoline just delivered, so nothing is left unobserved.
        foreach (Task task in result.Values.Values)
        {
            Assert.NotNull(task.Exception);
        }

        return captured;
    }

    /// <summary>Reads <paramref name="count"/> UTF-8 pointers out of an inner string row.</summary>
    private static IReadOnlyList<string?> ReadStrings(IntPtr row, int count)
    {
        string?[] values = new string?[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = Utf8Marshal.PtrToString(Marshal.ReadIntPtr(row, i * IntPtr.Size));
        }

        return values;
    }

    /// <summary>Reads the raw pointers out of an inner string row — null versus non-null.</summary>
    private static IReadOnlyList<IntPtr> ReadPointers(IntPtr row, int count)
    {
        IntPtr[] values = new IntPtr[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = Marshal.ReadIntPtr(row, i * IntPtr.Size);
        }

        return values;
    }

    /// <summary>Reads <paramref name="count"/> doubles out of an inner <c>double*</c> row.</summary>
    private static IReadOnlyList<double> ReadDoubles(IntPtr row, int count)
    {
        if (count == 0)
        {
            return Array.Empty<double>();
        }

        double[] values = new double[count];
        Marshal.Copy(row, values, 0, count);
        return values;
    }

    /// <summary>
    /// Reads <paramref name="count"/> presence flags at a <b>one-byte</b> stride — the stride
    /// C's <c>bool</c> actually has.
    /// </summary>
    private static IReadOnlyList<byte> ReadBytes(IntPtr row, int count)
    {
        byte[] values = new byte[count];
        for (int i = 0; i < count; i++)
        {
            values[i] = Marshal.ReadByte(row, i);
        }

        return values;
    }

    /// <summary>One <c>alter_client_quotas</c> submit, exactly as it crossed the seam.</summary>
    private sealed class AlterCaptured
    {
        internal int Count { get; set; } = int.MinValue;

        internal int TimeoutMs { get; set; }

        internal bool ValidateOnly { get; set; }

        internal int[] EntityCounts { get; set; } = Array.Empty<int>();

        internal int[] OpCounts { get; set; } = Array.Empty<int>();

        internal IntPtr[] EntityTypeRows { get; set; } = Array.Empty<IntPtr>();

        internal IntPtr[] OpKeyRows { get; set; } = Array.Empty<IntPtr>();

        internal IntPtr[] OpValueRows { get; set; } = Array.Empty<IntPtr>();

        internal IntPtr[] OpHasValueRows { get; set; } = Array.Empty<IntPtr>();

        internal List<IReadOnlyList<string?>> EntityTypes { get; } = new List<IReadOnlyList<string?>>();

        internal List<IReadOnlyList<string?>> EntityNames { get; } = new List<IReadOnlyList<string?>>();

        internal List<IReadOnlyList<IntPtr>> EntityNamePointers { get; } =
            new List<IReadOnlyList<IntPtr>>();

        internal List<IReadOnlyList<string?>> OpKeys { get; } = new List<IReadOnlyList<string?>>();

        internal List<IReadOnlyList<double>> OpValues { get; } = new List<IReadOnlyList<double>>();

        internal List<IReadOnlyList<byte>> OpHasValues { get; } = new List<IReadOnlyList<byte>>();

        internal IntPtr UserData { get; set; }
    }

    /// <summary>One <c>describe_client_quotas</c> submit, exactly as it crossed the seam.</summary>
    private sealed class QuotaCaptured
    {
        internal int Count { get; set; } = int.MinValue;

        internal bool Strict { get; set; }

        internal int TimeoutMs { get; set; }

        internal int[] MatchTypes { get; set; } = Array.Empty<int>();

        internal IntPtr[] MatchNamePointers { get; set; } = Array.Empty<IntPtr>();

        internal IReadOnlyList<string?> EntityTypes { get; set; } = Array.Empty<string?>();

        internal IReadOnlyList<string?> MatchNames { get; set; } = Array.Empty<string?>();

        internal IntPtr UserData { get; set; }
    }

    /// <summary>One submit, exactly as it crossed the seam.</summary>
    private sealed class Captured
    {
        internal int Count { get; set; } = int.MinValue;

        internal int TimeoutMs { get; set; }

        internal int[] ResourceTypes { get; set; } = Array.Empty<int>();

        internal int[] PatternTypes { get; set; } = Array.Empty<int>();

        internal int[] Operations { get; set; } = Array.Empty<int>();

        internal int[] PermissionTypes { get; set; } = Array.Empty<int>();

        internal IntPtr[] ResourceNamePointers { get; set; } = Array.Empty<IntPtr>();

        internal IntPtr[] PrincipalPointers { get; set; } = Array.Empty<IntPtr>();

        internal IntPtr[] HostPointers { get; set; } = Array.Empty<IntPtr>();

        internal IReadOnlyList<string?> ResourceNames { get; set; } = Array.Empty<string?>();

        internal IReadOnlyList<string?> Principals { get; set; } = Array.Empty<string?>();

        internal IReadOnlyList<string?> Hosts { get; set; } = Array.Empty<string?>();

        internal IntPtr UserData { get; set; }
    }
}
