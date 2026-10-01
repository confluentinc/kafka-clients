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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>describeConsumerGroups</c> end-to-end through <see cref="MockAdminClient"/> — the
/// public half of M15/P5's third RPC.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Every key fails here, and that is the mock's contract, not a defect.</b> The Rust
/// <c>MockAdminClient::describe_consumer_groups</c> completes each requested group with
/// <c>unsupported_version("Not implemented yet")</c>, faithfully translating Java's
/// <c>MockAdminClient.java:735</c> (<c>admin-client.md</c> §9: a method Java's own mock
/// refuses is refused here too). So this file can prove the request shape, the key set, the
/// per-key fan-out and the failure plumbing — but it can never observe a
/// <see cref="ConsumerGroupDescription"/>.
/// </para>
/// <para>
/// <b>What this file therefore does NOT cover, and who does.</b> A described value, a
/// result mixing success and failure, the <c>authorizedOperations</c> absent-vs-empty
/// split, the four nullable presence pairs and the <c>State</c> projection are all driven
/// over production's own walker in <c>Interop/AdminP5ResultMarshalTests</c>, which can
/// supply the inputs the mock withholds. What actually crossed the P/Invoke — the group-id
/// array, its count and <see cref="DescribeConsumerGroupsOptions.IncludeAuthorizedOperations"/>
/// — is read at the seam in <c>Interop/AdminP5SubmitArgumentTests</c>, because the mock
/// answers identically whatever those arguments say.
/// </para>
/// <para>
/// ⚠ The value types' own behaviour (equality, defensive copies, the <c>State</c>
/// projection at rest) belongs to <c>PublicAdminConsumerGroupDescriptionTests</c> and
/// <c>PublicAdminMemberDescriptionTests</c>; the result type's fan-out and aggregate to
/// <c>PublicAdminDescribeConsumerGroupsResultTests</c>. Nothing here re-asserts those.
/// </para>
/// </remarks>
public sealed class PublicAdminDescribeConsumerGroupsTests
{
    /// <summary>The per-await ceiling; a hang must fail the test, not the run.</summary>
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The Rust mock's refusal code — <c>UNSUPPORTED_VERSION</c>, what
    /// <c>Error::unsupported_version</c> carries.
    /// </summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws and the Rust mock translates.
    /// </summary>
    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// The request fans out to one future per requested group id, keyed by that id, and
    /// each future carries the mock's own refusal — attributed per key rather than to one
    /// shared task.
    /// </summary>
    [Fact]
    public async Task DescribeConsumerGroups_FansOutOneFuturePerGroupId()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        DescribeConsumerGroupsResult result =
            admin.DescribeConsumerGroups(new[] { "p5-dcg-a", "p5-dcg-b", "p5-dcg-c" });

        Assert.Equal(
            new[] { "p5-dcg-a", "p5-dcg-b", "p5-dcg-c" },
            result.DescribedGroups.Keys.OrderBy(id => id, StringComparer.Ordinal));

        // Distinct Task instances: a single shared future would satisfy a key-set
        // assertion while losing the per-group granularity the Java shape promises.
        Assert.Equal(3, result.DescribedGroups.Values.Distinct().Count());

        foreach (KeyValuePair<string, Task<ConsumerGroupDescription>> described in result.DescribedGroups)
        {
            KafkaException failure = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(() => described.Value), s_deadline);

            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>
    /// <c>all()</c> faults when any key does — Java's
    /// <c>DescribeConsumerGroupsResult.all()</c> is <c>KafkaFuture.allOf</c> over the
    /// per-group futures, not an independent request.
    /// </summary>
    [Fact]
    public async Task DescribeConsumerGroups_AllFaultsWhenAKeyFails()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        DescribeConsumerGroupsResult result = admin.DescribeConsumerGroups(new[] { "p5-dcg-all" });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);

        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// The ids are deduplicated <b>ordinally</b>: a repeated id yields one future, and two
    /// ids differing only in case stay two.
    /// </summary>
    /// <remarks>
    /// The comparer is load-bearing all the way out — <see cref="DescribeConsumerGroupsResult"/>
    /// hardcodes <see cref="StringComparer.Ordinal"/> for its aggregate and has a public
    /// constructor with no factory to thread a different one through, so a case-insensitive
    /// bridge would silently disagree with the result it feeds.
    /// </remarks>
    [Fact]
    public async Task DescribeConsumerGroups_DeduplicatesIdsOrdinally()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        DescribeConsumerGroupsResult result =
            admin.DescribeConsumerGroups(new[] { "p5-dup", "p5-dup", "P5-DUP" });

        Assert.Equal(2, result.DescribedGroups.Count);
        Assert.Contains("p5-dup", result.DescribedGroups.Keys);
        Assert.Contains("P5-DUP", result.DescribedGroups.Keys);

        await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
    }

    /// <summary>
    /// An empty id collection is a well-formed request for nothing: no futures, and
    /// <c>all()</c> completes with an empty map rather than faulting or hanging.
    /// </summary>
    [Fact]
    public async Task DescribeConsumerGroups_AnEmptyRequestCompletesEmpty()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        DescribeConsumerGroupsResult result =
            admin.DescribeConsumerGroups(Array.Empty<string>());

        Assert.Empty(result.DescribedGroups);

        IReadOnlyDictionary<string, ConsumerGroupDescription> all =
            await TestTimeout.Run(result.All, s_deadline);

        Assert.Empty(all);
    }

    /// <summary>
    /// <see cref="DescribeConsumerGroupsOptions.IncludeAuthorizedOperations"/> is accepted
    /// in both states and changes nothing the mock can show — which is exactly why the flag
    /// itself is asserted at the P/Invoke seam instead.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DescribeConsumerGroups_AcceptsBothAuthorizedOperationsSettings(bool include)
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        DescribeConsumerGroupsResult result = admin.DescribeConsumerGroups(
            new[] { "p5-dcg-flag" },
            new DescribeConsumerGroupsOptions { IncludeAuthorizedOperations = include });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);

        // The mock's refusal, not an ABI complaint about a malformed boolean.
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// The RPC is reachable through <see cref="IAdmin"/>, not only through the concrete
    /// mock — the interface is the shape Java's <c>Admin</c> declares.
    /// </summary>
    [Fact]
    public async Task DescribeConsumerGroups_IsReachableThroughTheInterface()
    {
        await using MockAdminClient admin = new MockAdminClient(1);
        IAdmin api = admin;

        DescribeConsumerGroupsResult result = api.DescribeConsumerGroups(new[] { "p5-dcg-iface" });

        Assert.Equal(new[] { "p5-dcg-iface" }, result.DescribedGroups.Keys);
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
    }

    /// <summary>
    /// A malformed id list is refused synchronously, as an argument fault — not deferred
    /// into a faulted future where a caller reading <c>DescribedGroups</c> would never see
    /// it.
    /// </summary>
    [Fact]
    public async Task DescribeConsumerGroups_RejectsAMalformedIdList()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        ArgumentNullException missing = Assert.Throws<ArgumentNullException>(
            () => { admin.DescribeConsumerGroups(null!); });
        Assert.Equal("groupIds", missing.ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => { admin.DescribeConsumerGroups(new string?[] { "p5-dcg-null", null }!); });
        Assert.Equal("groupIds", nullElement.ParamName);
    }

    /// <summary>
    /// A negative timeout is refused rather than silently substituting the client default,
    /// and the message names the property so the caller can find it.
    /// </summary>
    [Fact]
    public async Task DescribeConsumerGroups_RejectsANegativeTimeout()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () =>
            {
                admin.DescribeConsumerGroups(
                    new[] { "p5-dcg-timeout" },
                    new DescribeConsumerGroupsOptions { TimeoutMs = -1 });
            });

        Assert.Equal("options", failure.ParamName);
        Assert.Contains(
            "DescribeConsumerGroupsOptions.TimeoutMs",
            failure.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// After the client is closed the RPC throws <see cref="ObjectDisposedException"/>
    /// rather than reaching a destroyed native handle.
    /// </summary>
    [Fact]
    public async Task DescribeConsumerGroups_AfterDispose_Throws()
    {
        MockAdminClient admin = new MockAdminClient(1);
        await admin.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(
            () => { admin.DescribeConsumerGroups(new[] { "p5-dcg-disposed" }); });
    }
}
