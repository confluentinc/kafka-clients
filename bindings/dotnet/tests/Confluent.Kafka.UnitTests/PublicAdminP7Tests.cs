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
/// M15/P7's eight RPCs end to end against <see cref="MockAdminClient"/>, plus the public value
/// types' Java-mirroring preconditions and the secrets discipline.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The two SCRAM RPCs have no happy path here, and that is faithful.</b> Java's own
/// <c>MockAdminClient</c> throws <c>UnsupportedOperationException("Not implemented yet")</c>
/// for both (<c>MockAdminClient.java:1254-1255,1259-1260</c>) and the core mirrors it, so their
/// marshalling is asserted at the submit seam instead
/// (<c>AdminP7SubmitArgumentTests</c>) and their readers over injected accessors
/// (<c>AdminP7ResultMarshalTests</c>).
/// </para>
/// <para>
/// ⚠⚠ <b>Known divergence — <c>describeUserScramCredentials</c> cannot report "not
/// found".</b> Java distinguishes three <c>RESOURCE_NOT_FOUND</c> shapes; the ABI collapses
/// all of them into <em>success with zero credentials</em> and exposes no discriminant, so the
/// binding ships the ABI's behaviour. No managed heuristic is applied — "zero credentials
/// implies not found" is wrong for a user who genuinely has none — and no test here asserts a
/// Java behaviour the ABI cannot produce.
/// </para>
/// </remarks>
public sealed class PublicAdminP7Tests
{
    // ------------------------------------------------------------------------------------
    // Delegation tokens — create, describe, renew, expire.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// A created token carries its metadata and a non-empty MAC, and the owner defaults to the
    /// sole renewer when none is given.
    /// </summary>
    [Fact]
    public async Task CreateDelegationToken_ReturnsATokenWithItsMetadata()
    {
        using MockAdminClient admin = new MockAdminClient();

        DelegationToken token = await admin.CreateDelegationToken(
            new CreateDelegationTokenOptions
            {
                Renewers = new[] { new KafkaPrincipal("User", "alice") },
                MaxLifetimeMs = 86_400_000L,
            }).DelegationToken();

        Assert.NotEmpty(token.Hmac);
        Assert.NotEmpty(token.TokenInfo.TokenId);
        Assert.Equal(new[] { "User:alice" }, token.TokenInfo.RenewersAsString);
        Assert.Equal("User:alice", token.TokenInfo.OwnerAsString);
        Assert.True(token.TokenInfo.IssueTimestamp > 0);
        Assert.Equal(86_400_000L, token.TokenInfo.MaxTimestamp);

        // The mock mints with the ABI's `-1` expiry sentinel; renew is what sets one.
        Assert.Equal(-1L, token.TokenInfo.ExpiryTimestamp);
    }

    /// <summary>A describe with no owners filter returns every token the mock minted.</summary>
    [Fact]
    public async Task DescribeDelegationToken_ReturnsEveryTokenWhenUnfiltered()
    {
        using MockAdminClient admin = new MockAdminClient();

        DelegationToken first = await Create(admin, "alice");
        DelegationToken second = await Create(admin, "bob");

        IReadOnlyList<DelegationToken> described =
            await admin.DescribeDelegationToken().DelegationTokens();

        Assert.Equal(
            new[] { first.TokenInfo.TokenId, second.TokenInfo.TokenId }.OrderBy(id => id, StringComparer.Ordinal),
            described.Select(token => token.TokenInfo.TokenId).OrderBy(id => id, StringComparer.Ordinal));
    }

    /// <summary>
    /// Renewing and expiring a token by its MAC returns the new expiry; the MAC round-trips
    /// through both calls, which is what a truncating read would break.
    /// </summary>
    [Fact]
    public async Task RenewAndExpire_MatchTheTokenByItsMac()
    {
        using MockAdminClient admin = new MockAdminClient();

        DelegationToken token = await Create(admin, "alice");

        long renewed = await admin.RenewDelegationToken(
            token.Hmac, new RenewDelegationTokenOptions { RenewTimePeriodMs = 3_600_000L }).ExpiryTimestamp();
        Assert.Equal(3_600_000L, renewed);

        long expired = await admin.ExpireDelegationToken(
            token.Hmac, new ExpireDelegationTokenOptions { ExpiryTimePeriodMs = 0L }).ExpiryTimestamp();
        Assert.Equal(0L, expired);

        // Expired, so it is gone from the describe — the MAC matched both calls.
        Assert.Empty(await admin.DescribeDelegationToken().DelegationTokens());
    }

    /// <summary>An unknown MAC faults the returned task rather than throwing from the call.</summary>
    [Fact]
    public async Task RenewDelegationToken_UnknownMacFaultsTheTask()
    {
        using MockAdminClient admin = new MockAdminClient();

        await Assert.ThrowsAsync<KafkaException>(
            () => admin.RenewDelegationToken(new byte[] { 0xDE, 0x00, 0xAD }).ExpiryTimestamp());
    }

    // ------------------------------------------------------------------------------------
    // Features.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// <c>describeFeatures</c> returns both tables and the epoch, and the result is the same
    /// snapshot on every accessor — the whole walk plus the destroy runs here.
    /// </summary>
    [Fact]
    public async Task DescribeFeatures_ReturnsBothTablesAndTheEpoch()
    {
        using MockAdminClient admin = new MockAdminClient();

        FeatureMetadata metadata = await admin.DescribeFeatures().FeatureMetadata();

        Assert.NotNull(metadata.FinalizedFeatures);
        Assert.NotNull(metadata.SupportedFeatures);
        Assert.NotNull(metadata.FinalizedFeaturesEpoch);
    }

    /// <summary>
    /// A <c>describeFeatures</c> targeted at a node id still completes — the discriminant is
    /// honoured rather than rejected.
    /// </summary>
    [Fact]
    public async Task DescribeFeatures_AcceptsANodeId() =>
        Assert.NotNull(
            await new MockAdminClient()
                .DescribeFeatures(new DescribeFeaturesOptions { NodeId = 0 })
                .FeatureMetadata());

    /// <summary>
    /// <c>updateFeatures</c> rejects an update the mock has no bounds for, and the rejection
    /// reaches <b>every</b> requested feature's task — the keyed-void completion.
    /// </summary>
    [Fact]
    public async Task UpdateFeatures_SurfacesTheRejectionOnEveryKey()
    {
        using MockAdminClient admin = new MockAdminClient();

        UpdateFeaturesResult result = admin.UpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                ["metadata.version"] = new FeatureUpdate(9, FeatureUpdate.UpgradeType.Upgrade),
                ["group.version"] = new FeatureUpdate(2, FeatureUpdate.UpgradeType.Upgrade),
            });

        Assert.Equal(
            new[] { "group.version", "metadata.version" },
            result.Values.Keys.OrderBy(key => key, StringComparer.Ordinal));

        await Assert.ThrowsAsync<KafkaException>(() => result.All());
        foreach (Task value in result.Values.Values)
        {
            await Assert.ThrowsAsync<KafkaException>(() => value);
        }
    }

    /// <summary>
    /// ⚠⚠ <b>An empty map is REJECTED, and the rejection is load-bearing.</b> With zero keys
    /// the bridge mints zero awaitables, so <c>All()</c> is <c>WhenAll(&lt;empty&gt;)</c> and
    /// reports <b>success</b> — the ABI's whole-call error would be silently swallowed and the
    /// caller told the updates were applied. Java rejects it before enqueuing anything
    /// (<c>KafkaAdminClient.java:4590-4592</c>).
    /// </summary>
    [Fact]
    public void UpdateFeatures_RejectsAnEmptyMap()
    {
        using MockAdminClient admin = new MockAdminClient();

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => admin.UpdateFeatures(new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)));

        Assert.Contains(
            "Feature updates can not be null or empty.", thrown.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// ⚠⚠ A <b>blank</b> feature name is rejected with Java's own message and exception kind
    /// (<c>Utils.isBlank</c>, <c>KafkaAdminClient.java:4597-4599</c>) — including the
    /// whitespace-only forms, which neither the binding nor the core rejected before and which
    /// therefore reached the broker as a request Java refuses to send.
    /// </summary>
    /// <remarks>
    /// ⚠ The control character <c>U+0001</c> is in the theory because Java's <c>trim</c>
    /// strips every char <c>&lt;= ' '</c> while <see cref="string.IsNullOrWhiteSpace"/> does
    /// not — the one input that tells the two implementations apart.
    /// </remarks>
    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("   \t ")]
    [InlineData("\u0001")]
    public void UpdateFeatures_RejectsABlankFeatureName(string feature)
    {
        using MockAdminClient admin = new MockAdminClient();

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => admin.UpdateFeatures(
                new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
                {
                    [feature] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
                }));

        Assert.Contains("Provided feature can not be empty.", thrown.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The guard is on <b>blankness</b>, not on the presence of whitespace — a name with an
    /// interior space is a legal request and must reach the submit.
    /// </summary>
    [Fact]
    public void UpdateFeatures_AcceptsANameWithInteriorWhitespace()
    {
        using MockAdminClient admin = new MockAdminClient();

        UpdateFeaturesResult result = admin.UpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                ["a b"] = new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade),
            });

        Assert.Equal(new[] { "a b" }, result.Values.Keys);
    }

    /// <summary>
    /// ⚠⚠ <b>The one mock input that makes <c>updateFeatures</c> SUCCEED, and therefore the
    /// only test that reaches its per-key walk at all.</b> Every failing update takes the
    /// whole-call-error branch, where the key reader never runs.
    /// </summary>
    /// <remarks>
    /// Deleting an unseeded feature — level <c>0</c> with a downgrade type — passes the mock's
    /// validation with <c>cur = min = max = 0</c>, so the ABI hands back a populated result and
    /// the walk resolves each key by name. That makes this the guard against a **mis-wired**
    /// key reader too: a reader pointed at a sibling result resolves keys the request never
    /// asked for, leaving the real ones to <c>FailUncompleted</c> — so the awaits below fault.
    /// Measured: it is RED against a reader that throws, where
    /// <see cref="UpdateFeatures_SurfacesTheRejectionOnEveryKey"/> stays green.
    /// </remarks>
    [Fact]
    public async Task UpdateFeatures_WalksEveryKey_OnTheSuccessPath()
    {
        using MockAdminClient admin = new MockAdminClient();

        UpdateFeaturesResult result = admin.UpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                ["metadata.version"] = new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade),
                ["group.version"] = new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade),
            });

        Assert.Equal(
            new[] { "group.version", "metadata.version" },
            result.Values.Keys.OrderBy(key => key, StringComparer.Ordinal));

        // A null per-key error IS the success value for a KafkaFuture<Void> result.
        // (Status, not IsCompletedSuccessfully — the latter post-dates the netstandard2.0 floor.)
        await result.All();
        foreach (Task value in result.Values.Values)
        {
            Assert.Equal(TaskStatus.RanToCompletion, value.Status);
        }
    }

    // ------------------------------------------------------------------------------------
    // SCRAM — Java's own "Not implemented yet" (T-N10).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// Both SCRAM RPCs fault with the <b>exact</b> message Java's mock throws. The message is
    /// asserted because the code alone does not distinguish it from any other unsupported
    /// operation.
    /// </summary>
    [Fact]
    public async Task DescribeUserScramCredentials_FaultsWithJavasOwnMessage()
    {
        using MockAdminClient admin = new MockAdminClient();

        KafkaException error = await Assert.ThrowsAsync<KafkaException>(
            () => admin.DescribeUserScramCredentials(new[] { "alice" }).All());

        Assert.Equal("Not implemented yet", error.Message);
    }

    /// <summary>The alter half, same message.</summary>
    [Fact]
    public async Task AlterUserScramCredentials_FaultsWithJavasOwnMessage()
    {
        using MockAdminClient admin = new MockAdminClient();

        KafkaException error = await Assert.ThrowsAsync<KafkaException>(
            () => admin.AlterUserScramCredentials(
                new[] { new UserScramCredentialDeletion("alice", ScramMechanism.ScramSha256) }).All());

        Assert.Equal("Not implemented yet", error.Message);
    }

    /// <summary>
    /// The three <c>describeUserScramCredentials</c> accessors are derived from one snapshot,
    /// so a failure reaches all three — <c>Description</c> included.
    /// </summary>
    [Fact]
    public async Task DescribeUserScramCredentials_FailureReachesEveryAccessor()
    {
        using MockAdminClient admin = new MockAdminClient();

        DescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials();

        await Assert.ThrowsAsync<KafkaException>(() => result.All());
        await Assert.ThrowsAsync<KafkaException>(() => result.Users());
        await Assert.ThrowsAsync<KafkaException>(() => result.Description("alice"));
    }

    // ------------------------------------------------------------------------------------
    // Secrets never surface (T-N4).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>A MAC never appears in <see cref="DelegationToken.ToString"/></b> — it is a
    /// bearer credential, and a token logged at debug level would hand it to anyone reading the
    /// log.
    /// </summary>
    [Fact]
    public async Task DelegationTokenToString_MasksTheMac()
    {
        using MockAdminClient admin = new MockAdminClient();

        DelegationToken token = await Create(admin, "alice");
        string text = token.ToString();

        Assert.DoesNotContain(token.HmacAsBase64String, text, StringComparison.Ordinal);
        Assert.DoesNotContain(
            BitConverter.ToString(token.Hmac), text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("[*******]", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// ⚠⚠ <b>A SCRAM password never reaches a string.</b> Asserted over every string the
    /// upsertion can produce, and over the precondition messages of the guards that reject a
    /// bad upsertion — a message that echoed its argument would leak the password into a log.
    /// </summary>
    [Fact]
    public void ScramSecrets_NeverReachAString()
    {
        const string Password = "s3cr3t-pa55w0rd";
        const string Salt = "s4lt-v4lue";

        UserScramCredentialUpsertion upsertion = new UserScramCredentialUpsertion(
            "alice",
            new ScramCredentialInfo(ScramMechanism.ScramSha256, 4096),
            System.Text.Encoding.UTF8.GetBytes(Password),
            System.Text.Encoding.UTF8.GetBytes(Salt));

        Assert.DoesNotContain(Password, upsertion.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(Salt, upsertion.ToString(), StringComparison.Ordinal);

        ArgumentNullException thrown = Assert.Throws<ArgumentNullException>(
            () => new UserScramCredentialUpsertion(
                "alice", new ScramCredentialInfo(ScramMechanism.ScramSha256, 4096), (byte[])null!));
        Assert.DoesNotContain(Password, thrown.Message, StringComparison.Ordinal);
    }

    // ------------------------------------------------------------------------------------
    // Constructor preconditions, with Java's exact messages (T-N7).
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// <see cref="FinalizedVersionRange"/> mirrors Java's bounds check <b>and</b> its message
    /// (<c>FinalizedVersionRange.java:39</c> / <c>:42-44</c>).
    /// </summary>
    [Theory]
    [InlineData((short)-1, (short)1)]
    [InlineData((short)1, (short)-1)]
    [InlineData((short)5, (short)4)]
    public void FinalizedVersionRange_RejectsAnInvalidRange(short min, short max)
    {
        ArgumentException thrown =
            Assert.Throws<ArgumentException>(() => new FinalizedVersionRange(min, max));

        Assert.Contains(
            "Expected minVersionLevel >= 0, maxVersionLevel >= 0 and maxVersionLevel >= minVersionLevel, "
            + $"but received minVersionLevel: {min}, maxVersionLevel: {max}",
            thrown.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// <see cref="SupportedVersionRange"/> mirrors Java's own — note the different wording and
    /// the trailing period (<c>SupportedVersionRange.java:39</c> / <c>:42</c>).
    /// </summary>
    [Theory]
    [InlineData((short)-1, (short)1)]
    [InlineData((short)3, (short)2)]
    public void SupportedVersionRange_RejectsAnInvalidRange(short min, short max)
    {
        ArgumentException thrown =
            Assert.Throws<ArgumentException>(() => new SupportedVersionRange(min, max));

        Assert.Contains(
            $"Expected 0 <= minVersion <= maxVersion but received minVersion:{min}, maxVersion:{max}.",
            thrown.Message,
            StringComparison.Ordinal);
    }

    /// <summary>An equal-bounds range is legal on both types — the boundary Java allows.</summary>
    [Fact]
    public void EqualBounds_AreAccepted()
    {
        Assert.Equal(0, new FinalizedVersionRange(0, 0).MaxVersionLevel);
        Assert.Equal(0, new SupportedVersionRange(0, 0).MaxVersion);
    }

    /// <summary>
    /// <see cref="FeatureUpdate"/> mirrors Java's two throws
    /// (<c>FeatureUpdate.java:69</c> and <c>:74</c>): a downgrade needs a downgrade type, and no level may
    /// be negative.
    /// </summary>
    [Fact]
    public void FeatureUpdate_RejectsADowngradeWithoutADowngradeType()
    {
        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => new FeatureUpdate(0, FeatureUpdate.UpgradeType.Upgrade));

        Assert.Contains(
            "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the "
            + "provided maxVersionLevel:0 is < 1.",
            thrown.Message,
            StringComparison.Ordinal);
    }

    /// <summary>The negative-level throw, and its exact message.</summary>
    [Fact]
    public void FeatureUpdate_RejectsANegativeLevel()
    {
        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => new FeatureUpdate(-1, FeatureUpdate.UpgradeType.SafeDowngrade));

        Assert.Contains("Cannot specify a negative version level.", thrown.Message, StringComparison.Ordinal);
    }

    /// <summary>Deleting a feature — level <c>0</c> with a downgrade type — is legal.</summary>
    [Fact]
    public void FeatureUpdate_AcceptsADeletion() =>
        Assert.Equal(0, new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade).MaxVersionLevel);

    /// <summary>
    /// <see cref="KafkaPrincipal"/> mirrors Java's two null guards
    /// (<c>KafkaPrincipal.java:52-57</c>).
    /// </summary>
    [Fact]
    public void KafkaPrincipal_RejectsANullTypeOrName()
    {
        Assert.Contains(
            "Principal type cannot be null",
            Assert.Throws<ArgumentNullException>(() => new KafkaPrincipal(null!, "alice")).Message,
            StringComparison.Ordinal);

        Assert.Contains(
            "Principal name cannot be null",
            Assert.Throws<ArgumentNullException>(() => new KafkaPrincipal("User", null!)).Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Principal equality is over <c>(type, name)</c> only — Java excludes
    /// <c>tokenAuthenticated</c> from both <c>equals</c> and <c>hashCode</c>
    /// (<c>KafkaPrincipal.java:70-85</c>).
    /// </summary>
    [Fact]
    public void KafkaPrincipal_EqualityIgnoresTokenAuthentication()
    {
        KafkaPrincipal plain = new KafkaPrincipal("User", "alice");
        KafkaPrincipal tokenAuthenticated = new KafkaPrincipal("User", "alice", true);

        Assert.Equal(plain, tokenAuthenticated);
        Assert.Equal(plain.GetHashCode(), tokenAuthenticated.GetHashCode());
        Assert.NotEqual(plain, new KafkaPrincipal("Group", "alice"));
        Assert.Equal("User:alice", plain.ToString());
    }

    /// <summary>
    /// A mechanism name round-trips through both directions, and an unknown name falls back to
    /// <see cref="ScramMechanism.Unknown"/> rather than throwing.
    /// </summary>
    [Fact]
    public void ScramMechanism_RoundTripsItsName()
    {
        Assert.Equal("SCRAM-SHA-256", ScramMechanism.ScramSha256.MechanismName());
        Assert.Equal("SCRAM-SHA-512", ScramMechanism.ScramSha512.MechanismName());
        Assert.Equal(ScramMechanism.ScramSha512, ScramMechanisms.FromMechanismName("SCRAM-SHA-512"));
        Assert.Equal(ScramMechanism.Unknown, ScramMechanisms.FromMechanismName("SCRAM-SHA-1"));
        Assert.Equal(ScramMechanism.Unknown, ScramMechanisms.FromType(99));
    }

    private static Task<DelegationToken> Create(MockAdminClient admin, string renewer) =>
        admin.CreateDelegationToken(
            new CreateDelegationTokenOptions
            {
                Renewers = new[] { new KafkaPrincipal("User", renewer) },
            }).DelegationToken();
}
