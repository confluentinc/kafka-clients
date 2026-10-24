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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The delegation-token, feature and SCRAM RPCs end to end against the mocks, the public value types' Java
/// preconditions, and that secrets never reach a string.
/// </summary>
/// <remarks>
/// Java's mock refuses both SCRAM RPCs with "Not implemented yet", and so does the core's, so these tests assert that
/// refusal; the C handles the SCRAM requests become are read back in <c>AdminP7SubmitArgumentTests</c>, and the SCRAM
/// readers run over real C handles in <c>AdminP7ResultMarshalTests</c>.
/// </remarks>
public sealed class PublicAdminP7Tests
{
    private const int DelegationTokenNotFoundCode = 62;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly IReadOnlyDictionary<string, string> s_unreachable =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["bootstrap.servers"] = "127.0.0.1:1" };

    [Fact]
    public async Task CreateDelegationToken_ReturnsATokenWithItsMetadata()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        DelegationToken token = await admin.CreateDelegationToken(
            new CreateDelegationTokenOptions
            {
                Renewers = new[] { new KafkaPrincipal("User", "alice") },
                MaxLifetimeMs = 86_400_000L,
            }).DelegationToken().Get();

        Assert.NotEmpty(token.Hmac);
        Assert.NotEmpty(token.TokenInfo.TokenId);
        Assert.Equal(new[] { "User:alice" }, token.TokenInfo.RenewersAsString);
        Assert.Equal("User:alice", token.TokenInfo.OwnerAsString);
        Assert.True(token.TokenInfo.IssueTimestamp > 0);
        Assert.Equal(86_400_000L, token.TokenInfo.MaxTimestamp);

        // The mock mints with the ABI's `-1` expiry sentinel; renew is what sets one.
        Assert.Equal(-1L, token.TokenInfo.ExpiryTimestamp);
    }

    [Fact]
    public async Task DescribeDelegationToken_ReturnsEveryTokenWhenUnfiltered()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        DelegationToken first = await Create(admin, "alice");
        DelegationToken second = await Create(admin, "bob");

        IReadOnlyList<DelegationToken> described =
            await admin.DescribeDelegationToken().DelegationTokens().Get();

        Assert.Equal(
            new[] { first.TokenInfo.TokenId, second.TokenInfo.TokenId }.OrderBy(id => id, StringComparer.Ordinal),
            described.Select(token => token.TokenInfo.TokenId).OrderBy(id => id, StringComparer.Ordinal));
    }

    [Fact]
    public async Task RenewAndExpire_MatchTheTokenByItsMac()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        DelegationToken token = await Create(admin, "alice");

        long renewed = await admin.RenewDelegationToken(
            token.Hmac, new RenewDelegationTokenOptions { RenewTimePeriodMs = 3_600_000L }).ExpiryTimestamp().Get();
        Assert.Equal(3_600_000L, renewed);

        long expired = await admin.ExpireDelegationToken(
            token.Hmac, new ExpireDelegationTokenOptions { ExpiryTimePeriodMs = 0L }).ExpiryTimestamp().Get();
        Assert.Equal(0L, expired);

        Assert.Empty(await admin.DescribeDelegationToken().DelegationTokens().Get());
    }

    [Fact]
    public async Task RenewDelegationToken_UnknownMacFaultsTheTask()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await Assert.ThrowsAsync<KafkaException>(
            () => admin.RenewDelegationToken(new byte[] { 0xDE, 0x00, 0xAD }).ExpiryTimestamp().Get());
    }

    [Fact]
    public async Task RenewAndExpire_ANullMac_FailsTheFutureOnBothMocks()
    {
        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        AssertTokenNotFound(Assert.Throws<KafkaException>(() => sync.RenewDelegationToken(null!).ExpiryTimestamp().Get()));
        AssertTokenNotFound(Assert.Throws<KafkaException>(() => sync.ExpireDelegationToken(null!).ExpiryTimestamp().Get()));
        AssertTokenNotFound(
            await Assert.ThrowsAsync<KafkaException>(() => async.RenewDelegationToken(null!).ExpiryTimestamp().Get()));
        AssertTokenNotFound(
            await Assert.ThrowsAsync<KafkaException>(() => async.ExpireDelegationToken(null!).ExpiryTimestamp().Get()));
    }

    [Fact]
    public async Task RenewAndExpire_ANullMac_ThrowsOnTheRealClients()
    {
        KafkaAdminClient sync = new KafkaAdminClient(s_unreachable);
        try
        {
            Assert.Equal("hmac", Assert.Throws<ArgumentNullException>(() => sync.RenewDelegationToken(null!)).ParamName);
            Assert.Equal("hmac", Assert.Throws<ArgumentNullException>(() => sync.ExpireDelegationToken(null!)).ParamName);
        }
        finally
        {
            TestTimeout.Dispose(sync);
        }

        AsyncKafkaAdminClient async = new AsyncKafkaAdminClient(s_unreachable);
        try
        {
            Assert.Equal("hmac", Assert.Throws<ArgumentNullException>(() => async.RenewDelegationToken(null!)).ParamName);
            Assert.Equal("hmac", Assert.Throws<ArgumentNullException>(() => async.ExpireDelegationToken(null!)).ParamName);
        }
        finally
        {
            await TestTimeout.DisposeAsync(async);
        }
    }

    [Fact]
    public async Task DescribeFeatures_ReturnsBothTablesAndTheEpoch()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        FeatureMetadata metadata = await admin.DescribeFeatures().FeatureMetadata().Get();

        Assert.NotNull(metadata.FinalizedFeatures);
        Assert.NotNull(metadata.SupportedFeatures);
        Assert.NotNull(metadata.FinalizedFeaturesEpoch);
    }

    [Fact]
    public async Task DescribeFeatures_AcceptsANodeId() =>
        Assert.NotNull(
            await AsyncMockAdminClient.Create().Build()
                .DescribeFeatures(new DescribeFeaturesOptions { NodeId = 0 })
                .FeatureMetadata().Get());

    [Fact]
    public async Task UpdateFeatures_SurfacesTheRejectionOnEveryKey()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncUpdateFeaturesResult result = admin.UpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                ["metadata.version"] = new FeatureUpdate(9, FeatureUpdate.UpgradeType.Upgrade),
                ["group.version"] = new FeatureUpdate(2, FeatureUpdate.UpgradeType.Upgrade),
            });

        Assert.Equal(
            new[] { "group.version", "metadata.version" },
            result.Values.Keys.OrderBy(key => key, StringComparer.Ordinal));

        await Assert.ThrowsAsync<KafkaException>(() => result.All().Get());
        foreach (AsyncKafkaFuture value in result.Values.Values)
        {
            await Assert.ThrowsAsync<KafkaException>(() => value.Get());
        }
    }

    [Fact]
    public async Task G6_1_UpdateFeatures_MockAcceptsAnEmptyMap()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncUpdateFeaturesResult result =
            admin.UpdateFeatures(new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal));

        Assert.Empty(result.Values);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public void G6_1_UpdateFeatures_RealClientRejectsAnEmptyMap()
    {
        KafkaAdminClient admin = new KafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => admin.UpdateFeatures(new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)));

        Assert.Equal("Feature updates can not be null or empty.", thrown.Message);
    }

    [Fact]
    public void G6_9_UpdateFeatures_ANullName_GetsTheBlankNameMessage()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => admin.UpdateFeatures(
                new FeatureUpdateEntries(
                    new KeyValuePair<string, FeatureUpdate>(
                        null!, new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade)))));

        Assert.Equal("featureUpdates", thrown.ParamName);
        Assert.StartsWith("Provided feature can not be empty.", thrown.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("   \t ")]
    [InlineData("\u0001")]
    public void UpdateFeatures_RejectsABlankFeatureName(string feature)
    {
        KafkaAdminClient admin = new KafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => admin.UpdateFeatures(
                new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
                {
                    [feature] = new FeatureUpdate(1, FeatureUpdate.UpgradeType.Upgrade),
                }));

        Assert.Contains("Provided feature can not be empty.", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UpdateFeatures_AcceptsANameWithInteriorWhitespace()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncUpdateFeaturesResult result = admin.UpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                ["a b"] = new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade),
            });

        Assert.Equal(new[] { "a b" }, result.Values.Keys);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("   \t ")]
    [InlineData("\u0001")]
    public async Task G6_1_UpdateFeatures_MockAcceptsABlankFeatureName(string feature)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncUpdateFeaturesResult result = admin.UpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                [feature] = new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade),
            });

        Assert.Equal(new[] { feature }, result.Values.Keys);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public async Task UpdateFeatures_WalksEveryKey_OnTheSuccessPath()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncUpdateFeaturesResult result = admin.UpdateFeatures(
            new Dictionary<string, FeatureUpdate>(StringComparer.Ordinal)
            {
                ["metadata.version"] = new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade),
                ["group.version"] = new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade),
            });

        Assert.Equal(
            new[] { "group.version", "metadata.version" },
            result.Values.Keys.OrderBy(key => key, StringComparer.Ordinal));

        // A null per-key error is the success of a KafkaFuture<Void> result. Status, as IsCompletedSuccessfully post-dates
        // netstandard2.0.
        await result.All().Get();
        foreach (AsyncKafkaFuture value in result.Values.Values)
        {
            Assert.Equal(TaskStatus.RanToCompletion, value.Get().Status);
        }
    }

    [Fact]
    public async Task DescribeUserScramCredentials_FaultsWithJavasOwnMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        KafkaException error = await Assert.ThrowsAsync<KafkaException>(
            () => admin.DescribeUserScramCredentials(new[] { "alice" }).All().Get());

        Assert.Equal("Not implemented yet", error.Message);
    }

    [Fact]
    public async Task DescribeUserScramCredentials_ARepeatedUser_IsSent_AndFaultsWithJavasOwnMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeUserScramCredentialsResult result =
            admin.DescribeUserScramCredentials(new[] { "alice", "alice" });

        KafkaException error = await Assert.ThrowsAsync<KafkaException>(() => result.All().Get());
        Assert.Equal("Not implemented yet", error.Message);
        Assert.Equal(35, error.Code);
    }

    [Fact]
    public async Task AlterUserScramCredentials_FaultsWithJavasOwnMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        KafkaException error = await Assert.ThrowsAsync<KafkaException>(
            () => admin.AlterUserScramCredentials(
                new[] { new UserScramCredentialDeletion("alice", ScramMechanism.ScramSha256) }).All().Get());

        Assert.Equal("Not implemented yet", error.Message);
    }

    [Fact]
    public async Task DescribeUserScramCredentials_FailureReachesEveryAccessor()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials();

        await Assert.ThrowsAsync<KafkaException>(() => result.All().Get());
        await Assert.ThrowsAsync<KafkaException>(() => result.Users().Get());
        await Assert.ThrowsAsync<KafkaException>(() => result.Description("alice").Get());
    }

    [Fact]
    public void DescribeUserScramCredentials_NullUser_ThrowsSynchronously()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials();

        ArgumentNullException nullUser =
            Assert.Throws<ArgumentNullException>(() => { _ = result.Description(null!); });
        Assert.Equal("userName", nullUser.ParamName);
    }

    [Fact]
    public async Task DelegationTokenToString_MasksTheMac()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        DelegationToken token = await Create(admin, "alice");
        string text = token.ToString();

        Assert.DoesNotContain(token.HmacAsBase64String, text, StringComparison.Ordinal);
        Assert.DoesNotContain(
            BitConverter.ToString(token.Hmac), text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("[*******]", text, StringComparison.Ordinal);
    }

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

    [Fact]
    public void EqualBounds_AreAccepted()
    {
        Assert.Equal(0, new FinalizedVersionRange(0, 0).MaxVersionLevel);
        Assert.Equal(0, new SupportedVersionRange(0, 0).MaxVersion);
    }

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

    [Fact]
    public void FeatureUpdate_RejectsANegativeLevel()
    {
        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => new FeatureUpdate(-1, FeatureUpdate.UpgradeType.SafeDowngrade));

        Assert.Contains("Cannot specify a negative version level.", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FeatureUpdate_AcceptsADeletion() =>
        Assert.Equal(0, new FeatureUpdate(0, FeatureUpdate.UpgradeType.SafeDowngrade).MaxVersionLevel);

    [Theory]
    [InlineData(FeatureUpdate.UpgradeType.Unknown, "FeatureUpdate{maxVersionLevel:1, upgradeType:UNKNOWN}")]
    [InlineData(FeatureUpdate.UpgradeType.Upgrade, "FeatureUpdate{maxVersionLevel:1, upgradeType:UPGRADE}")]
    [InlineData(FeatureUpdate.UpgradeType.SafeDowngrade, "FeatureUpdate{maxVersionLevel:1, upgradeType:SAFE_DOWNGRADE}")]
    [InlineData(FeatureUpdate.UpgradeType.UnsafeDowngrade, "FeatureUpdate{maxVersionLevel:1, upgradeType:UNSAFE_DOWNGRADE}")]
    [InlineData((FeatureUpdate.UpgradeType)9, "FeatureUpdate{maxVersionLevel:1, upgradeType:9}")]
    public void G6_4_FeatureUpdate_ToString_PrintsJavasConstantName(FeatureUpdate.UpgradeType type, string expected) =>
        Assert.Equal(expected, new FeatureUpdate(1, type).ToString());

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

    [Fact]
    public void ScramMechanism_RoundTripsItsName()
    {
        Assert.Equal("SCRAM-SHA-256", ScramMechanism.ScramSha256.MechanismName());
        Assert.Equal("SCRAM-SHA-512", ScramMechanism.ScramSha512.MechanismName());
        Assert.Equal(ScramMechanism.ScramSha512, ScramMechanisms.FromMechanismName("SCRAM-SHA-512"));
        Assert.Equal(ScramMechanism.Unknown, ScramMechanisms.FromMechanismName("SCRAM-SHA-1"));
        Assert.Equal(ScramMechanism.Unknown, ScramMechanisms.FromType(99));
    }

    private static void AssertTokenNotFound(KafkaException failure)
    {
        Assert.Equal(DelegationTokenNotFoundCode, failure.Code);
        Assert.Equal(string.Empty, failure.Message);
    }

    private static Task<DelegationToken> Create(AsyncMockAdminClient admin, string renewer) =>
        admin.CreateDelegationToken(
            new CreateDelegationTokenOptions
            {
                Renewers = new[] { new KafkaPrincipal("User", renewer) },
            }).DelegationToken().Get();

    /// <summary>
    /// A <see cref="IReadOnlyDictionary{TKey, TValue}"/> that can yield what a real dictionary cannot, a null key, so the
    /// guard against it is reachable from a test.
    /// </summary>
    private sealed class FeatureUpdateEntries : IReadOnlyDictionary<string, FeatureUpdate>
    {
        private readonly KeyValuePair<string, FeatureUpdate>[] _entries;

        internal FeatureUpdateEntries(params KeyValuePair<string, FeatureUpdate>[] entries)
        {
            _entries = entries;
        }

        public int Count => _entries.Length;

        public IEnumerable<string> Keys => _entries.Select(entry => entry.Key);

        public IEnumerable<FeatureUpdate> Values => _entries.Select(entry => entry.Value);

        public FeatureUpdate this[string key] => throw new NotSupportedException();

        public bool ContainsKey(string key) => throw new NotSupportedException();

        public bool TryGetValue(string key, out FeatureUpdate value) => throw new NotSupportedException();

        public IEnumerator<KeyValuePair<string, FeatureUpdate>> GetEnumerator() =>
            ((IEnumerable<KeyValuePair<string, FeatureUpdate>>)_entries).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _entries.GetEnumerator();
    }
}
