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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the <b>public shape</b> of M15/P7's surface — the eight RPCs, their results, and the
/// four discriminants — against the Java classes it mirrors.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>Four result shapes across eight RPCs, and half of them cannot be told apart by
/// behaviour.</b> Two are per-key void (<c>alterUserScramCredentials</c>,
/// <c>updateFeatures</c>), one is a collection, four carry a single root value, and one
/// derives three accessors from one snapshot. A result that quietly grew a per-key surface
/// Java does not have, or lost one it does, is caught here and nowhere else.
/// </para>
/// <para>
/// ⚠ <b>Each discriminant is pinned as a NULLABLE property, not as a sentinel.</b> That the
/// owners filter is <c>IReadOnlyList&lt;KafkaPrincipal&gt;?</c> and the node id is
/// <c>int?</c> is the public half of what
/// <c>AdminP7SubmitArgumentTests</c> asserts on the wire.
/// </para>
/// </remarks>
public sealed class PublicAdminP7ShapeParityTests
{
    /// <summary>
    /// All eight RPCs return their <c>*Result</c> <b>synchronously</b> with Java's parameter
    /// shape (<c>admin-client.md</c> §1), and <c>Close</c> stays the only
    /// <see cref="Task"/>-returning member on <see cref="IAdmin"/>.
    /// </summary>
    [Theory]
    [InlineData(nameof(IAdmin.DescribeUserScramCredentials), typeof(DescribeUserScramCredentialsResult))]
    [InlineData(nameof(IAdmin.AlterUserScramCredentials), typeof(AlterUserScramCredentialsResult))]
    [InlineData(nameof(IAdmin.CreateDelegationToken), typeof(CreateDelegationTokenResult))]
    [InlineData(nameof(IAdmin.RenewDelegationToken), typeof(RenewDelegationTokenResult))]
    [InlineData(nameof(IAdmin.ExpireDelegationToken), typeof(ExpireDelegationTokenResult))]
    [InlineData(nameof(IAdmin.DescribeDelegationToken), typeof(DescribeDelegationTokenResult))]
    [InlineData(nameof(IAdmin.DescribeFeatures), typeof(DescribeFeaturesResult))]
    [InlineData(nameof(IAdmin.UpdateFeatures), typeof(UpdateFeaturesResult))]
    public void EachRpc_IsSynchronous_AndEndsInAnOptionalOptions(string name, Type resultType)
    {
        MethodInfo rpc = typeof(IAdmin).GetMethod(name)!;

        Assert.Equal(resultType, rpc.ReturnType);

        ParameterInfo[] parameters = rpc.GetParameters();
        ParameterInfo options = parameters[parameters.Length - 1];
        Assert.EndsWith("Options", options.ParameterType.Name, StringComparison.Ordinal);
        Assert.True(options.IsOptional, $"{name}'s options must be optional");
    }

    /// <summary>
    /// The RPCs' required parameters mirror Java's: the two MAC-taking calls take a
    /// <c>byte[]</c>, <c>updateFeatures</c> a map, <c>alterUserScramCredentials</c> a
    /// collection, and the four remaining take only their options.
    /// </summary>
    [Fact]
    public void EachRpc_TakesJavasOwnRequiredParameters()
    {
        Assert.Equal(
            new[] { typeof(byte[]) },
            Required(nameof(IAdmin.RenewDelegationToken)));
        Assert.Equal(
            new[] { typeof(byte[]) },
            Required(nameof(IAdmin.ExpireDelegationToken)));
        Assert.Equal(
            new[] { typeof(IReadOnlyDictionary<string, FeatureUpdate>) },
            Required(nameof(IAdmin.UpdateFeatures)));
        Assert.Equal(
            new[] { typeof(IEnumerable<UserScramCredentialAlteration>) },
            Required(nameof(IAdmin.AlterUserScramCredentials)));

        foreach (string optionsOnly in new[]
        {
            nameof(IAdmin.CreateDelegationToken),
            nameof(IAdmin.DescribeDelegationToken),
            nameof(IAdmin.DescribeFeatures),
        })
        {
            Assert.Empty(Required(optionsOnly));
        }

        // ⚠ describeUserScramCredentials' users are OPTIONAL and nullable: absent means
        // "every user" (h:8784-8785), which Java spells as a second zero-arg overload.
        ParameterInfo users = typeof(IAdmin)
            .GetMethod(nameof(IAdmin.DescribeUserScramCredentials))!.GetParameters()[0];
        Assert.Equal(typeof(IReadOnlyCollection<string>), users.ParameterType);
        Assert.True(users.IsOptional);
    }

    /// <summary>
    /// <c>Close</c> is still the only <see cref="Task"/>-returning member on
    /// <see cref="IAdmin"/> — P7 added eight RPCs and none of them is <c>async</c>.
    /// </summary>
    [Fact]
    public void Close_RemainsTheOnlyTaskReturningMember() =>
        Assert.Equal(
            new[] { nameof(IAdmin.Close) },
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

    /// <summary>
    /// The two <b>per-key void</b> results publish Java's <c>values()</c> + <c>all()</c> pair
    /// and nothing more (<c>AlterUserScramCredentialsResult.java:31</c>,
    /// <c>UpdateFeaturesResult.java:29</c>) — a per-key <see cref="Task"/>, not
    /// <c>Task&lt;T&gt;</c>, because Java's future is <c>KafkaFuture&lt;Void&gt;</c>.
    /// </summary>
    [Theory]
    [InlineData(typeof(AlterUserScramCredentialsResult))]
    [InlineData(typeof(UpdateFeaturesResult))]
    public void PerKeyVoidResults_PublishValuesAndAll(Type resultType)
    {
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, Task>),
            resultType.GetProperty("Values")!.PropertyType);
        Assert.Equal(typeof(Task), resultType.GetMethod("All", Type.EmptyTypes)!.ReturnType);

        Assert.Equal(
            new[] { "All", "Values" },
            PublicMembers(resultType));
    }

    /// <summary>
    /// The four <b>single-root-value</b> results publish exactly one accessor each, named for
    /// the Java method that returns it.
    /// </summary>
    [Theory]
    [InlineData(typeof(CreateDelegationTokenResult), "DelegationToken", typeof(Task<DelegationToken>))]
    [InlineData(typeof(RenewDelegationTokenResult), "ExpiryTimestamp", typeof(Task<long>))]
    [InlineData(typeof(ExpireDelegationTokenResult), "ExpiryTimestamp", typeof(Task<long>))]
    [InlineData(typeof(DescribeFeaturesResult), "FeatureMetadata", typeof(Task<FeatureMetadata>))]
    public void SingleValueResults_PublishExactlyOneAccessor(
        Type resultType, string accessor, Type returnType)
    {
        Assert.Equal(returnType, resultType.GetMethod(accessor, Type.EmptyTypes)!.ReturnType);
        Assert.Equal(new[] { accessor }, PublicMembers(resultType));
    }

    /// <summary>
    /// <see cref="DescribeDelegationTokenResult"/> publishes Java's single collection accessor
    /// (<c>DescribeDelegationTokenResult.java:37</c>).
    /// </summary>
    [Fact]
    public void DescribeDelegationTokenResult_PublishesOneCollectionAccessor()
    {
        Assert.Equal(
            typeof(Task<IReadOnlyList<DelegationToken>>),
            typeof(DescribeDelegationTokenResult)
                .GetMethod(nameof(DescribeDelegationTokenResult.DelegationTokens), Type.EmptyTypes)!
                .ReturnType);

        Assert.Equal(new[] { "DelegationTokens" }, PublicMembers(typeof(DescribeDelegationTokenResult)));
    }

    /// <summary>
    /// <see cref="DescribeUserScramCredentialsResult"/> publishes Java's <b>three</b>
    /// accessors, with <c>Description</c> taking the user name
    /// (<c>DescribeUserScramCredentialsResult.java:47,61,84</c>).
    /// </summary>
    [Fact]
    public void DescribeUserScramCredentialsResult_PublishesJavasThreeAccessors()
    {
        Type result = typeof(DescribeUserScramCredentialsResult);

        Assert.Equal(
            typeof(Task<IReadOnlyDictionary<string, UserScramCredentialsDescription>>),
            result.GetMethod(nameof(DescribeUserScramCredentialsResult.All), Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            typeof(Task<IReadOnlyList<string>>),
            result.GetMethod(nameof(DescribeUserScramCredentialsResult.Users), Type.EmptyTypes)!.ReturnType);

        MethodInfo description = result.GetMethod(
            nameof(DescribeUserScramCredentialsResult.Description), new[] { typeof(string) })!;
        Assert.Equal(typeof(Task<UserScramCredentialsDescription>), description.ReturnType);

        // ⚠ No per-key `Values` here: the keys come back IN the response (an empty request
        // describes every user), so there is nothing to key a future on before the submit.
        Assert.Equal(new[] { "All", "Description", "Users" }, PublicMembers(result));
    }

    /// <summary>
    /// ⚠⚠ Each of the three <b>option-borne</b> discriminants is a nullable property, never a
    /// sentinel — <see langword="null"/> and "empty" / "zero" are different requests.
    /// </summary>
    [Fact]
    public void OptionBorneDiscriminants_AreNullableProperties()
    {
        Assert.Equal(
            typeof(IReadOnlyList<KafkaPrincipal>),
            typeof(DescribeDelegationTokenOptions).GetProperty(
                nameof(DescribeDelegationTokenOptions.Owners))!.PropertyType);

        Assert.Equal(
            typeof(int?),
            typeof(DescribeFeaturesOptions).GetProperty(nameof(DescribeFeaturesOptions.NodeId))!.PropertyType);

        // The owner is optional too — absent means "the caller's own principal".
        Assert.Equal(
            typeof(KafkaPrincipal),
            typeof(CreateDelegationTokenOptions).GetProperty(
                nameof(CreateDelegationTokenOptions.Owner))!.PropertyType);

        // …but the renewer list is NOT nullable: Java defaults it to an empty list.
        Assert.Equal(
            Array.Empty<KafkaPrincipal>(),
            new CreateDelegationTokenOptions().Renewers);
    }

    /// <summary>
    /// ⚠⚠ The fourth discriminant is the <b>salt</b>, and it lives on the upsertion:
    /// <see langword="null"/> asks the core to generate one, an empty array is a literal empty
    /// salt (<c>UserScramCredentialUpsertion.java:70-80</c>).
    /// </summary>
    [Fact]
    public void Salt_IsNullableOnTheUpsertion()
    {
        UserScramCredentialUpsertion generated = new UserScramCredentialUpsertion(
            "u", new ScramCredentialInfo(ScramMechanism.ScramSha256, 4096), new byte[] { 1 }, null);
        UserScramCredentialUpsertion literal = new UserScramCredentialUpsertion(
            "u", new ScramCredentialInfo(ScramMechanism.ScramSha256, 4096), new byte[] { 1 },
            Array.Empty<byte>());

        Assert.Null(generated.Salt);
        Assert.NotNull(literal.Salt);
        Assert.Empty(literal.Salt!);
    }

    /// <summary>
    /// Both alteration kinds derive from Java's abstract <c>UserScramCredentialAlteration</c>,
    /// which is what lets one collection carry a mixed batch
    /// (<c>UserScramCredentialAlteration.java:29</c>).
    /// </summary>
    [Fact]
    public void BothAlterationKinds_ShareJavasAbstractBase()
    {
        Assert.True(typeof(UserScramCredentialAlteration).IsAbstract);
        Assert.Equal(typeof(UserScramCredentialAlteration), typeof(UserScramCredentialUpsertion).BaseType);
        Assert.Equal(typeof(UserScramCredentialAlteration), typeof(UserScramCredentialDeletion).BaseType);
        Assert.Equal(
            typeof(string),
            typeof(UserScramCredentialAlteration).GetProperty(
                nameof(UserScramCredentialAlteration.User))!.PropertyType);
    }

    /// <summary>
    /// The features family is <c>short</c> throughout — Java's own width, and the one the ABI
    /// takes. An <c>int</c> anywhere here would silently widen the request.
    /// </summary>
    [Fact]
    public void FeaturesFamily_IsShortThroughout()
    {
        Assert.Equal(
            typeof(short),
            typeof(FinalizedVersionRange).GetProperty(
                nameof(FinalizedVersionRange.MinVersionLevel))!.PropertyType);
        Assert.Equal(
            typeof(short),
            typeof(FinalizedVersionRange).GetProperty(
                nameof(FinalizedVersionRange.MaxVersionLevel))!.PropertyType);
        Assert.Equal(
            typeof(short),
            typeof(SupportedVersionRange).GetProperty(
                nameof(SupportedVersionRange.MinVersion))!.PropertyType);
        Assert.Equal(
            typeof(short),
            typeof(SupportedVersionRange).GetProperty(
                nameof(SupportedVersionRange.MaxVersion))!.PropertyType);
        Assert.Equal(
            typeof(short),
            typeof(FeatureUpdate).GetProperty(nameof(FeatureUpdate.MaxVersionLevel))!.PropertyType);
    }

    /// <summary>
    /// <see cref="FeatureMetadata"/> mirrors Java's three members, with the epoch nullable —
    /// Java's <c>Optional&lt;Long&gt;</c> (<c>FeatureMetadata.java:58</c>).
    /// </summary>
    [Fact]
    public void FeatureMetadata_MirrorsJavasThreeMembers()
    {
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, FinalizedVersionRange>),
            typeof(FeatureMetadata).GetProperty(nameof(FeatureMetadata.FinalizedFeatures))!.PropertyType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, SupportedVersionRange>),
            typeof(FeatureMetadata).GetProperty(nameof(FeatureMetadata.SupportedFeatures))!.PropertyType);
        Assert.Equal(
            typeof(long?),
            typeof(FeatureMetadata).GetProperty(nameof(FeatureMetadata.FinalizedFeaturesEpoch))!.PropertyType);

        // Not user-constructible: Java's constructor is package-private.
        Assert.Empty(typeof(FeatureMetadata).GetConstructors());
    }

    /// <summary>
    /// <see cref="FeatureUpdate.UpgradeType"/> is a <b>nested</b> enum, as in Java, and carries
    /// Java's four constants in its ordinal order (<c>FeatureUpdate.java:32-44</c>).
    /// </summary>
    [Fact]
    public void UpgradeType_IsNested_WithJavasConstants()
    {
        Assert.Equal(typeof(FeatureUpdate), typeof(FeatureUpdate.UpgradeType).DeclaringType);
        Assert.Equal(
            new[] { "Unknown", "Upgrade", "SafeDowngrade", "UnsafeDowngrade" },
            Enum.GetValues(typeof(FeatureUpdate.UpgradeType))
                .Cast<FeatureUpdate.UpgradeType>()
                .OrderBy(value => (int)value)
                .Select(value => value.ToString()));
    }

    /// <summary>
    /// The three token value types live in the root <c>Confluent.Kafka</c> namespace, matching
    /// their Java packages (<c>org.apache.kafka.common.security.auth</c> /
    /// <c>.token.delegation</c>) rather than <c>admin</c> (CLAUDE.md §2).
    /// </summary>
    [Fact]
    public void TokenValueTypes_LiveInTheRootNamespace() =>
        Assert.All(
            new[] { typeof(KafkaPrincipal), typeof(TokenInformation), typeof(DelegationToken) },
            type => Assert.Equal("Confluent.Kafka", type.Namespace));

    /// <summary>
    /// Every P7 type the user can name is <c>public</c> and outside <c>Internal</c>
    /// (CLAUDE.md §2) — the flattened row carrier stays internal.
    /// </summary>
    [Fact]
    public void TheRowCarrier_StaysInternal() =>
        Assert.Null(
            typeof(IAdmin).Assembly.GetType("Confluent.Kafka.Internal.UserScramCredentialEntry")!
                .GetConstructors()
                .FirstOrDefault(constructor => constructor.IsPublic));

    private static IEnumerable<Type> Required(string name) =>
        typeof(IAdmin).GetMethod(name)!.GetParameters()
            .Where(parameter => !parameter.IsOptional)
            .Select(parameter => parameter.ParameterType);

    /// <summary>The declared public instance members of a result type, sorted.</summary>
    private static IEnumerable<string> PublicMembers(Type resultType) =>
        resultType
            .GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(member => member.MemberType is MemberTypes.Method or MemberTypes.Property)
            .Where(member => member is not MethodInfo method || !method.IsSpecialName)
            .Select(member => member.Name)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(name => name, StringComparer.Ordinal);
}
