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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives M15/P3 Stage 1's marshallers over <b>real</b> native result roots — the
/// <c>DescribeClusterResult_t</c> (result shape 5) and the two sub-shape-3b list results.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these tests own the result handle instead of going through the client's success
/// path.</b> The production trampoline destroys the result root in its <c>finally</c> —
/// correctly — so it never lets a caller inspect the borrowed pointers afterwards, and
/// "everything was copied out before the root died" is exactly what has to be proven. So
/// each test submits the <c>_async</c> entry point directly with its own capturing
/// callback, keeps the root alive, walks it with <em>production's</em> marshallers and
/// readers (<c>definition-of-done.md</c> §12), and destroys it exactly once.
/// </para>
/// <para>
/// ⚠ <b>The authorized-operations gate is the phase's central correctness claim, and it is
/// tested as an A/B over one root.</b> The header says a count of <c>0</c> covers both "the
/// broker did not report them" (Java yields <see langword="null"/>) and "reported, but none
/// authorized", so only <c>has_authorized_operations</c> separates them. The Rust mock
/// hands us the <em>ambiguous</em> input — gate <c>true</c>, count <c>0</c> — so walking it
/// twice, once with production's real gate and once with the gate stubbed to <c>false</c>
/// and <b>everything else identical</b>, isolates the signal under test. An implementation
/// that read the count alone would return an empty collection in both walks.
/// </para>
/// <para>
/// ⚠ <b>The error ownership here is the INVERSE of every earlier admin phase.</b> None of
/// these three result types declares a <c>get_error</c> of any kind, so there is no
/// <em>borrowed</em> error anywhere on these paths: the only error they can ever see is the
/// callback's own <c>error</c> parameter, which is <b>owned</b> and freed by
/// <see cref="KafkaException.FromHandle"/>. Reaching for
/// <see cref="KafkaException.FromBorrowedHandle"/> here would leak, not protect.
/// </para>
/// </remarks>
public sealed class AdminP3ResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The mock's <c>DEFAULT_CLUSTER_ID</c> (Java's <c>MockAdminClient</c> value).</summary>
    private const string MockClusterId = "4A5xz_QZTB2CtL4wc0X0Jw";

    /// <summary>
    /// Rooted for the process lifetime, as every callback handed to native must be
    /// (ffi §B6 keep-alive) — even a test's.
    /// </summary>
    private static readonly AdminCallbacks.DescribeClusterCallback s_captureCluster = OnCapture;

    /// <inheritdoc cref="s_captureCluster"/>
    private static readonly AdminCallbacks.ListConfigResourcesCallback s_captureConfigResources = OnCapture;

    /// <inheritdoc cref="s_captureCluster"/>
    private static readonly AdminCallbacks.ListClientMetricsResourcesCallback s_captureClientMetrics = OnCapture;

    /// <summary>
    /// ⚠ <b>THE discriminator for this stage.</b> The gate — not the count — decides
    /// whether <c>authorizedOperations()</c> is Java's <see langword="null"/> or a reported
    /// but empty set.
    /// </summary>
    [Fact]
    public void AuthorizedOperations_TheGateDecidesNullVersusEmpty_NotTheCount()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        IntPtr result = SubmitAndCaptureCluster(admin);

        try
        {
            // The input really is the ambiguous one: the gate says "reported" while the
            // count says 0. Without this the A/B below would prove nothing.
            Assert.True(NativeMethods.DescribeClusterResultHasAuthorizedOperations(result));
            Assert.Equal(0, NativeMethods.DescribeClusterResultAuthorizedOperationCount(result));

            // ---- (A) production's real gate: reported-but-empty → an EMPTY collection ----
            DescribeClusterSnapshot reported = DescribeClusterMarshal.CopyOut(result);
            Assert.NotNull(reported.AuthorizedOperations);
            Assert.Empty(reported.AuthorizedOperations!);

            // ---- (B) the SAME root, the SAME production accessors, with only the gate
            // stubbed to false. Nothing else differs, so a difference in outcome can only
            // come from the gate. ----
            DescribeClusterSnapshot notReported = DescribeClusterMarshal.CopyOut(result, static _ => false);
            Assert.Null(notReported.AuthorizedOperations);
        }
        finally
        {
            NativeMethods.DescribeClusterResultDestroy(result);
        }
    }

    /// <summary>
    /// Every value is <b>copied out</b> before the root dies: reading the whole snapshot
    /// after <c>DescribeClusterResult_destroy</c> is safe and unchanged.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>This is the test that catches a lazily-held borrowed pointer, and nothing else
    /// will.</b> Every string and node here borrows into the result root; an implementation
    /// that stored an <see cref="IntPtr"/> and read it on demand would pass every test that
    /// inspects the snapshot <em>before</em> the destroy.
    /// </remarks>
    [Fact]
    public void EveryValue_IsCopiedOut_AndSurvivesTheRootsDestroy()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(2);
        IntPtr result = SubmitAndCaptureCluster(admin);

        DescribeClusterSnapshot snapshot;
        try
        {
            snapshot = DescribeClusterMarshal.CopyOut(result);
        }
        finally
        {
            NativeMethods.DescribeClusterResultDestroy(result);
        }

        // The root is gone. Everything below reads only owned managed state.
        Assert.Equal(MockClusterId, snapshot.ClusterId);
        Assert.Equal(2, snapshot.Nodes.Count);

        List<Node> nodes = snapshot.Nodes.OrderBy(node => node.Id).ToList();
        Assert.Equal(new[] { 0, 1 }, nodes.Select(node => node.Id));
        Assert.Equal(new[] { "localhost", "localhost" }, nodes.Select(node => node.Host));
        Assert.Equal(new[] { 1000, 1001 }, nodes.Select(node => node.Port));

        // The mock places the controller on broker 0 (Java's `MockAdminClient` does too).
        Assert.NotNull(snapshot.Controller);
        Assert.Equal(0, snapshot.Controller!.Id);
        Assert.Equal("localhost", snapshot.Controller.Host);

        Assert.NotNull(snapshot.AuthorizedOperations);
        Assert.Empty(snapshot.AuthorizedOperations!);
    }

    /// <summary>
    /// A null controller pointer becomes a <see langword="null"/> <see cref="Node"/> that
    /// the public projection surfaces as a <b>successful</b> <see langword="null"/> — not a
    /// faulted task and not a default node.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The mock cannot produce this input</b>: its <c>describe_cluster</c> always
    /// completes the controller with <c>Some(brokers[0])</c>, mirroring Java's
    /// <c>controller == null ? brokers.get(0) : controller</c>. So the branch is exercised
    /// where it actually lives — the null-pointer mapping in <see cref="NodeMarshal"/> that
    /// <see cref="DescribeClusterMarshal"/> relies on, and the projection that carries it to
    /// the public surface. Java's own null is real
    /// (<c>KafkaAdminClient.java:2531-2534</c> returns null for <c>NO_CONTROLLER_ID</c>),
    /// which is why the projection must not fault or substitute.
    /// </remarks>
    [Fact]
    public async Task NullController_ProjectsAsASuccessfulNull()
    {
        // The mapping DescribeClusterMarshal depends on.
        Assert.Null(NodeMarshal.CopyOut(IntPtr.Zero));

        DescribeClusterResult result = new DescribeClusterResult(
            Task.FromResult(new DescribeClusterSnapshot(Array.Empty<Node>(), null, "id", null)));

        Assert.Null(await TestTimeout.Run(result.Controller, s_deadline));
        Assert.Null(await TestTimeout.Run(result.AuthorizedOperations, s_deadline));
        Assert.Equal("id", await TestTimeout.Run(result.ClusterId, s_deadline));
        Assert.Empty(await TestTimeout.Run(result.Nodes, s_deadline));
    }

    /// <summary>
    /// Each of <see cref="DescribeClusterResult"/>'s four projections returns the
    /// <b>same</b> <see cref="Task"/> instance on every call, as Java's accessors return the
    /// same stored <c>KafkaFuture</c>, and all four derive from the <b>one</b> completion.
    /// </summary>
    [Fact]
    public async Task TheFourProjections_AreCachedAndAgree()
    {
        DescribeClusterSnapshot snapshot = new DescribeClusterSnapshot(
            new[] { new Node(7, "h", 9, null) },
            new Node(7, "h", 9, null),
            "cid",
            new[] { AclOperation.Describe });

        DescribeClusterResult result = new DescribeClusterResult(Task.FromResult(snapshot));

        Assert.Same(result.Nodes(), result.Nodes());
        Assert.Same(result.Controller(), result.Controller());
        Assert.Same(result.ClusterId(), result.ClusterId());
        Assert.Same(result.AuthorizedOperations(), result.AuthorizedOperations());

        Assert.Equal(7, Assert.Single(await TestTimeout.Run(result.Nodes, s_deadline)).Id);
        Assert.Equal("cid", await TestTimeout.Run(result.ClusterId, s_deadline));

        IReadOnlyCollection<AclOperation>? operations =
            await TestTimeout.Run(result.AuthorizedOperations, s_deadline);
        Assert.NotNull(operations);
        Assert.Equal(AclOperation.Describe, Assert.Single(operations!));
    }

    /// <summary>
    /// A failed call faults all four projections with the <b>same</b>
    /// <see cref="KafkaException"/> — not an <see cref="AggregateException"/> wrapper, which
    /// is why the projections are <c>async</c> methods rather than <c>ContinueWith</c>.
    /// </summary>
    [Fact]
    public async Task AFailedCall_FaultsEveryProjectionWithTheSameException()
    {
        KafkaException failure = new KafkaException("cluster describe failed");
        TaskCompletionSource<DescribeClusterSnapshot> source =
            new TaskCompletionSource<DescribeClusterSnapshot>(TaskCreationOptions.RunContinuationsAsynchronously);
        source.SetException(failure);

        DescribeClusterResult result = new DescribeClusterResult(source.Task);

        Assert.Same(failure, await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.Nodes), s_deadline));
        Assert.Same(
            failure, await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.Controller), s_deadline));
        Assert.Same(
            failure, await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.ClusterId), s_deadline));
        Assert.Same(
            failure,
            await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.AuthorizedOperations), s_deadline));
    }

    /// <summary>
    /// The sub-shape-3b walk produces an ordered <b>collection</b>, copies every element out
    /// before the root dies, and reads the composite <c>(get_type(i), get_name(i))</c>
    /// element correctly.
    /// </summary>
    /// <remarks>
    /// The mock's <c>list_config_resources</c> reports one <c>BROKER</c> and one
    /// <c>BROKER_LOGGER</c> resource per broker even with no topics, so a one-broker mock is
    /// enough input to exercise two distinct type ids against the same name.
    /// </remarks>
    [Fact]
    public async Task ListConfigResources_WalksToACollection_CopiedOutBeforeTheRootDies()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        IntPtr result = SubmitAndCaptureConfigResources(admin, Array.Empty<int>());

        SingleAdminOperation<IReadOnlyCollection<ConfigResource>> operation =
            new SingleAdminOperation<IReadOnlyCollection<ConfigResource>>("listConfigResources");
        try
        {
            KeyedResultMarshal.CompleteList(
                result,
                NativeMethods.ListConfigResourcesResultCount,
                operation,
                AdminCallbacks.ConfigResourceValue);
        }
        finally
        {
            NativeMethods.ListConfigResourcesResultDestroy(result);
        }

        // The root is gone; everything below reads owned managed state.
        IReadOnlyCollection<ConfigResource> resources = await TestTimeout.Run(() => operation.Task, s_deadline);

        Assert.Contains(resources, resource => resource.Type == ConfigResourceType.Broker && resource.Name == "0");
        Assert.Contains(
            resources, resource => resource.Type == ConfigResourceType.BrokerLogger && resource.Name == "0");

        // Same name under two types are DISTINCT resources — the composite element the
        // (result, index) reader seam exists for.
        Assert.Equal(2, resources.Count(resource => resource.Name == "0"));
    }

    /// <summary>
    /// The ABI's declared order — <c>(type id, name)</c> — is handed on unchanged: neither
    /// shuffled nor re-sorted.
    /// </summary>
    [Fact]
    public async Task ListConfigResources_PreservesTheAbisDeliveryOrder()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(3);
        IntPtr result = SubmitAndCaptureConfigResources(admin, Array.Empty<int>());

        List<ConfigResource> asDelivered = new List<ConfigResource>();
        SingleAdminOperation<IReadOnlyCollection<ConfigResource>> operation =
            new SingleAdminOperation<IReadOnlyCollection<ConfigResource>>("listConfigResources");
        try
        {
            int count = NativeMethods.ListConfigResourcesResultCount(result);
            for (int index = 0; index < count; index++)
            {
                asDelivered.Add(AdminCallbacks.ConfigResourceValue(result, index));
            }

            KeyedResultMarshal.CompleteList(
                result,
                NativeMethods.ListConfigResourcesResultCount,
                operation,
                AdminCallbacks.ConfigResourceValue);
        }
        finally
        {
            NativeMethods.ListConfigResourcesResultDestroy(result);
        }

        IReadOnlyCollection<ConfigResource> walked = await TestTimeout.Run(() => operation.Task, s_deadline);

        Assert.NotEmpty(asDelivered);

        // The input is what the header says it is — sorted by (type id, name).
        Assert.Equal(
            asDelivered.OrderBy(resource => (int)resource.Type).ThenBy(resource => resource.Name, StringComparer.Ordinal),
            asDelivered);

        // …and the walker handed it on unchanged: not shuffled, not sorted again.
        Assert.Equal(asDelivered, walked);
    }

    /// <summary>
    /// The type filter reaches the ABI: asking for only <see cref="ConfigResourceType.Broker"/>
    /// excludes the <see cref="ConfigResourceType.BrokerLogger"/> resources an unfiltered
    /// call returns.
    /// </summary>
    /// <remarks>
    /// The A/B against the unfiltered call is what makes this falsifiable — an
    /// implementation that dropped the array on the floor would return the same set twice.
    /// </remarks>
    [Fact]
    public void ListConfigResources_TheTypeFilterReachesTheAbi()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        List<ConfigResource> unfiltered = ReadAll(admin, Array.Empty<int>());
        List<ConfigResource> brokersOnly = ReadAll(admin, new[] { (int)ConfigResourceType.Broker });

        Assert.Contains(unfiltered, resource => resource.Type == ConfigResourceType.BrokerLogger);
        Assert.DoesNotContain(brokersOnly, resource => resource.Type == ConfigResourceType.BrokerLogger);
        Assert.Contains(brokersOnly, resource => resource.Type == ConfigResourceType.Broker);
    }

    /// <summary>
    /// The client-metrics walk yields an empty collection against a fresh mock — the listing
    /// type's only per-index accessor is its name, and a mock with no client-metrics
    /// resource has none.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The seeded case is Stage 2's debt, not an omission here.</b> The Rust mock
    /// creates a client-metrics resource only as a side effect of an
    /// <c>incrementalAlterConfigs</c> against a <c>CLIENT_METRICS</c> resource, which is not
    /// bound until Stage 2 — so Stage 2 must add the seeded-listing test.
    /// </remarks>
    [Fact]
    public async Task ListClientMetricsResources_WalksToAnEmptyCollection_OnAFreshMock()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        IntPtr result = SubmitAndCaptureClientMetrics(admin);

#pragma warning disable CS0618 // Java deprecates the listing type itself; mirrored, not avoided.
        SingleAdminOperation<IReadOnlyCollection<ClientMetricsResourceListing>> operation =
            new SingleAdminOperation<IReadOnlyCollection<ClientMetricsResourceListing>>(
                "listClientMetricsResources");
        try
        {
            Assert.Equal(0, NativeMethods.ListClientMetricsResourcesResultCount(result));

            KeyedResultMarshal.CompleteList(
                result,
                NativeMethods.ListClientMetricsResourcesResultCount,
                operation,
                AdminCallbacks.ClientMetricsResourceListingValue);
        }
        finally
        {
            NativeMethods.ListClientMetricsResourcesResultDestroy(result);
        }

        IReadOnlyCollection<ClientMetricsResourceListing> listings =
            await TestTimeout.Run(() => operation.Task, s_deadline);
#pragma warning restore CS0618

        Assert.Empty(listings);
    }

    /// <summary>
    /// The element reader routes its type id through
    /// <see cref="ConfigResourceMarshal.TypeFromId"/>, so the ABI's out-of-range <c>-1</c>
    /// is <b>rejected</b> rather than folded into <see cref="ConfigResourceType.Unknown"/>.
    /// </summary>
    /// <remarks>
    /// Driven through production's own reader over a real result root that is <em>empty</em>
    /// under a filter, so index 0 really is out of range. The "unknown non-negative id"
    /// half is exercised directly against the mapping in
    /// <see cref="ConfigResourceTypeFromId_MapsJavasIds_AndDegradesAnUnknownIdToUnknown"/>:
    /// the current ABI cannot produce that id, since the Rust core's
    /// <c>ConfigResourceType</c> and this binding's enum carry the same six members. That
    /// is a fact about today's core, not a property of the shape —
    /// <see cref="ConfigResourceMarshal.TypeFromId"/> exists precisely for a core that
    /// gains a member this binding does not yet have.
    /// </remarks>
    [Fact]
    public void ConfigResourceValue_RejectsTheOutOfRangeSentinel()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        IntPtr empty = SubmitAndCaptureConfigResources(admin, new[] { (int)ConfigResourceType.Group });
        try
        {
            // A one-broker mock has no group configs, so the filtered listing is empty and
            // index 0 is out of range.
            Assert.Equal(0, NativeMethods.ListConfigResourcesResultCount(empty));
            Assert.Equal(-1, NativeMethods.ListConfigResourcesResultGetType(empty, 0));

            KafkaException rejected =
                Assert.Throws<KafkaException>(() => AdminCallbacks.ConfigResourceValue(empty, 0));
            Assert.Equal(
                "The admin result produced no resource type for an index within its own count.",
                rejected.Message);
        }
        finally
        {
            NativeMethods.ListConfigResourcesResultDestroy(empty);
        }
    }

    /// <summary>
    /// <see cref="ConfigResourceMarshal.TypeFromId"/> maps each of Java's ids to its member,
    /// degrades a <b>non-negative</b> id it has no member for to
    /// <see cref="ConfigResourceType.Unknown"/> — Java's <c>Type.forId</c>
    /// (<c>ConfigResource.java:57-59</c>: <c>TYPES.getOrDefault(id, UNKNOWN)</c>) — and
    /// rejects a <b>negative</b> one.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>Asserted against the mapping directly, because no result root can carry an
    /// unrecognised id.</b> An earlier version of this test claimed to cover the degrade
    /// half and did not: its only assertions were <c>(ConfigResourceType)0 == Unknown</c>
    /// and <c>!Enum.IsDefined(…, 64)</c>, neither of which calls the mapping — so replacing
    /// the degrade with a <c>throw</c> left the suite green (M15/P3 round 1, finding 69.2).
    /// The two unknowns are the whole point: <c>-1</c> is "index out of range", <c>0</c> is
    /// Java's real <c>UNKNOWN</c> member, and merging them makes a genuine <c>UNKNOWN</c>
    /// resource and a read past the end indistinguishable.
    /// </remarks>
    [Fact]
    public void ConfigResourceTypeFromId_MapsJavasIds_AndDegradesAnUnknownIdToUnknown()
    {
        Assert.Equal(ConfigResourceType.Unknown, ConfigResourceMarshal.TypeFromId(0));
        Assert.Equal(ConfigResourceType.Topic, ConfigResourceMarshal.TypeFromId(2));
        Assert.Equal(ConfigResourceType.Broker, ConfigResourceMarshal.TypeFromId(4));
        Assert.Equal(ConfigResourceType.BrokerLogger, ConfigResourceMarshal.TypeFromId(8));
        Assert.Equal(ConfigResourceType.ClientMetrics, ConfigResourceMarshal.TypeFromId(16));
        Assert.Equal(ConfigResourceType.Group, ConfigResourceMarshal.TypeFromId(32));

        // A type a newer broker knows and this client does not: Java degrades, never fails.
        Assert.Equal(ConfigResourceType.Unknown, ConfigResourceMarshal.TypeFromId(64));
        Assert.Equal(ConfigResourceType.Unknown, ConfigResourceMarshal.TypeFromId(1));
        Assert.Equal(ConfigResourceType.Unknown, ConfigResourceMarshal.TypeFromId(int.MaxValue));

        // …and the ABI's own out-of-range return is NOT that case.
        foreach (int outOfRange in new[] { -1, int.MinValue })
        {
            KafkaException rejected =
                Assert.Throws<KafkaException>(() => ConfigResourceMarshal.TypeFromId(outOfRange));
            Assert.Equal(
                "The admin result produced no resource type for an index within its own count.",
                rejected.Message);
        }
    }

    private static List<ConfigResource> ReadAll(NativeAdminClient admin, int[] types)
    {
        IntPtr result = SubmitAndCaptureConfigResources(admin, types);
        try
        {
            int count = NativeMethods.ListConfigResourcesResultCount(result);
            List<ConfigResource> resources = new List<ConfigResource>(count);
            for (int index = 0; index < count; index++)
            {
                resources.Add(AdminCallbacks.ConfigResourceValue(result, index));
            }

            return resources;
        }
        finally
        {
            NativeMethods.ListConfigResourcesResultDestroy(result);
        }
    }

    /// <summary>
    /// Submits <c>describe_cluster_async</c> directly and hands the caller the resulting
    /// <b>owned</b> result root, which the capturing callback deliberately does not destroy
    /// — the callback owns it, and here that owner is the test.
    /// </summary>
    private static IntPtr SubmitAndCaptureCluster(NativeAdminClient admin) =>
        Capture(
            (callbackUserData) => NativeMethods.AdminClientDescribeClusterAsync(
                admin.Handle.DangerousGetHandle(),
                -1,
                includeAuthorizedOperations: true,
                includeFencedBrokers: false,
                s_captureCluster,
                callbackUserData),
            "describeCluster");

    /// <inheritdoc cref="SubmitAndCaptureCluster"/>
    private static IntPtr SubmitAndCaptureConfigResources(NativeAdminClient admin, int[] types) =>
        Capture(
            (callbackUserData) => NativeMethods.AdminClientListConfigResourcesAsync(
                admin.Handle.DangerousGetHandle(),
                types,
                types.Length,
                -1,
                s_captureConfigResources,
                callbackUserData),
            "listConfigResources");

    /// <inheritdoc cref="SubmitAndCaptureCluster"/>
    private static IntPtr SubmitAndCaptureClientMetrics(NativeAdminClient admin) =>
        Capture(
            (callbackUserData) => NativeMethods.AdminClientListClientMetricsResourcesAsync(
                admin.Handle.DangerousGetHandle(),
                -1,
                s_captureClientMetrics,
                callbackUserData),
            "listClientMetricsResources");

    private static IntPtr Capture(Action<IntPtr> submit, string operationName)
    {
        CaptureState capture = new CaptureState();
        GCHandle gcHandle = GCHandle.Alloc(capture, GCHandleType.Normal);
        try
        {
            submit(GCHandle.ToIntPtr(gcHandle));
            Assert.True(capture.Done.Wait(s_deadline), $"the {operationName} callback never fired");
        }
        finally
        {
            gcHandle.Free();
        }

        KafkaException? submitFailure = KafkaException.FromHandle(capture.Error);
        if (submitFailure is not null)
        {
            throw submitFailure;
        }

        Assert.NotEqual(IntPtr.Zero, capture.Result);
        return capture.Result;
    }

    private static void OnCapture(IntPtr result, IntPtr error, IntPtr userData)
    {
        // A callback entered from native is a no-throw boundary even in a test.
        try
        {
            CaptureState capture = (CaptureState)GCHandle.FromIntPtr(userData).Target!;
            capture.Result = result;
            capture.Error = error;
            capture.Done.Set();
        }
        catch (Exception)
        {
            // Swallow: an escaping exception would unwind into Rust. The Wait above then
            // times out and fails the test with a clear message.
        }
    }

    private sealed class CaptureState
    {
        internal IntPtr Result;

        internal IntPtr Error;

        internal ManualResetEventSlim Done { get; } = new ManualResetEventSlim(false);
    }
}
