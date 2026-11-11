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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Common.Security.Token.Delegation;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public shape of the eight delegation-token, feature and SCRAM RPCs, their results and their four
/// discriminants, against the Java classes they mirror.
/// </summary>
/// <remarks>
/// Four result shapes across eight RPCs: two per-key void (<c>AlterUserScramCredentials</c>, <c>UpdateFeatures</c>),
/// one a collection, four a single value, and one deriving three accessors from one snapshot. Each discriminant is a
/// nullable getter, not a sentinel.
/// </remarks>
public sealed class PublicAdminP7ShapeParityTests
{
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

        // describeUserScramCredentials' users are optional and nullable: absent means every user, which Java spells as a
        // zero-argument overload.
        ParameterInfo users = typeof(IAdmin)
            .GetMethod(nameof(IAdmin.DescribeUserScramCredentials))!.GetParameters()[0];
        Assert.Equal(typeof(IReadOnlyCollection<string>), users.ParameterType);
        Assert.True(users.IsOptional);
    }

    [Fact]
    public void IAdmin_HasNoTaskReturningMember() =>
        Assert.Empty(
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name));

    [Theory]
    [InlineData(typeof(AlterUserScramCredentialsResult), typeof(KafkaFuture))]
    [InlineData(typeof(UpdateFeaturesResult), typeof(KafkaFuture))]
    [InlineData(typeof(AsyncAlterUserScramCredentialsResult), typeof(AsyncKafkaFuture))]
    [InlineData(typeof(AsyncUpdateFeaturesResult), typeof(AsyncKafkaFuture))]
    public void PerKeyVoidResults_PublishValuesAndAll(Type resultType, Type futureType)
    {
        Assert.Equal(
            typeof(IReadOnlyDictionary<,>).MakeGenericType(typeof(string), futureType),
            Getters.Get(resultType, "Values").ReturnType);
        Assert.Equal(futureType, resultType.GetMethod("All", Type.EmptyTypes)!.ReturnType);

        Assert.Equal(
            new[] { "All", "Values" },
            PublicMembers(resultType));
    }

    [Theory]
    [InlineData(typeof(CreateDelegationTokenResult), "DelegationToken", typeof(KafkaFuture<DelegationToken>))]
    [InlineData(typeof(RenewDelegationTokenResult), "ExpiryTimestamp", typeof(KafkaFuture<long>))]
    [InlineData(typeof(ExpireDelegationTokenResult), "ExpiryTimestamp", typeof(KafkaFuture<long>))]
    [InlineData(typeof(DescribeFeaturesResult), "FeatureMetadata", typeof(KafkaFuture<FeatureMetadata>))]
    [InlineData(typeof(AsyncCreateDelegationTokenResult), "DelegationToken", typeof(AsyncKafkaFuture<DelegationToken>))]
    [InlineData(typeof(AsyncRenewDelegationTokenResult), "ExpiryTimestamp", typeof(AsyncKafkaFuture<long>))]
    [InlineData(typeof(AsyncExpireDelegationTokenResult), "ExpiryTimestamp", typeof(AsyncKafkaFuture<long>))]
    [InlineData(typeof(AsyncDescribeFeaturesResult), "FeatureMetadata", typeof(AsyncKafkaFuture<FeatureMetadata>))]
    public void SingleValueResults_PublishExactlyOneAccessor(
        Type resultType, string accessor, Type returnType)
    {
        Assert.Equal(returnType, resultType.GetMethod(accessor, Type.EmptyTypes)!.ReturnType);
        Assert.Equal(new[] { accessor }, PublicMembers(resultType));
    }

    [Theory]
    [InlineData(typeof(DescribeDelegationTokenResult), typeof(KafkaFuture<IReadOnlyList<DelegationToken>>))]
    [InlineData(typeof(AsyncDescribeDelegationTokenResult), typeof(AsyncKafkaFuture<IReadOnlyList<DelegationToken>>))]
    public void DescribeDelegationTokenResult_PublishesOneCollectionAccessor(Type resultType, Type returnType)
    {
        Assert.Equal(
            returnType,
            resultType.GetMethod(nameof(DescribeDelegationTokenResult.DelegationTokens), Type.EmptyTypes)!.ReturnType);

        Assert.Equal(new[] { "DelegationTokens" }, PublicMembers(resultType));
    }

    [Theory]
    [InlineData(typeof(DescribeUserScramCredentialsResult), typeof(KafkaFuture<>))]
    [InlineData(typeof(AsyncDescribeUserScramCredentialsResult), typeof(AsyncKafkaFuture<>))]
    public void DescribeUserScramCredentialsResult_PublishesJavasThreeAccessors(Type result, Type future)
    {
        Assert.Equal(
            future.MakeGenericType(typeof(IReadOnlyDictionary<string, UserScramCredentialsDescription>)),
            result.GetMethod(nameof(DescribeUserScramCredentialsResult.All), Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            future.MakeGenericType(typeof(IReadOnlyList<string>)),
            result.GetMethod(nameof(DescribeUserScramCredentialsResult.Users), Type.EmptyTypes)!.ReturnType);

        MethodInfo description = result.GetMethod(
            nameof(DescribeUserScramCredentialsResult.Description), new[] { typeof(string) })!;
        Assert.Equal(future.MakeGenericType(typeof(UserScramCredentialsDescription)), description.ReturnType);

        // No per-key Values: the keys come back in the response, so there is nothing to key a future on before the submit.
        Assert.Equal(new[] { "All", "Description", "Users" }, PublicMembers(result));
    }

    [Fact]
    public void AlterUserScramCredentialsResult_Constructor_IsInternalAndRejectsNull()
    {
        Assert.Empty(typeof(AlterUserScramCredentialsResult).GetConstructors());
        ConstructorInfo constructor = Assert.Single(
            typeof(AlterUserScramCredentialsResult).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance),
            candidate => candidate.IsAssembly);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, KafkaFuture>),
            Assert.Single(constructor.GetParameters()).ParameterType);

        ArgumentNullException error =
            Assert.Throws<ArgumentNullException>(() => new AlterUserScramCredentialsResult(null!));
        Assert.Equal("futures", error.ParamName);
        Assert.Equal(new ArgumentNullException("futures").Message, error.Message);
    }

    [Fact]
    public void OptionBorneDiscriminants_AreNullableGetters()
    {
        Assert.Equal(
            typeof(IReadOnlyList<KafkaPrincipal>),
            Getters.Get(
                typeof(DescribeDelegationTokenOptions), nameof(DescribeDelegationTokenOptions.Owners)).ReturnType);

        Assert.Equal(
            typeof(int?),
            Getters.Get(typeof(DescribeFeaturesOptions), nameof(DescribeFeaturesOptions.NodeId)).ReturnType);

        // The owner is optional too — absent means "the caller's own principal".
        Assert.Equal(
            typeof(KafkaPrincipal),
            Getters.Get(typeof(CreateDelegationTokenOptions), nameof(CreateDelegationTokenOptions.Owner)).ReturnType);

        // But the renewer list is not nullable: Java defaults it to an empty list.
        Assert.Equal(
            Array.Empty<KafkaPrincipal>(),
            new CreateDelegationTokenOptions().Renewers());
    }

    [Fact]
    public void Salt_IsNeverNull_AsInJava()
    {
        ScramCredentialInfo info = new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096);
        UserScramCredentialUpsertion generated = new UserScramCredentialUpsertion("u", info, new byte[] { 1 });
        UserScramCredentialUpsertion fromString = new UserScramCredentialUpsertion("u", info, "p");
        UserScramCredentialUpsertion literal = new UserScramCredentialUpsertion(
            "u", info, new byte[] { 1 }, Array.Empty<byte>());

        Assert.NotEmpty(generated.Salt());
        Assert.NotEmpty(fromString.Salt());
        Assert.NotEqual(generated.Salt(), fromString.Salt());
        Assert.Empty(literal.Salt());
        Assert.Equal(
            "salt",
            Assert.Throws<ArgumentNullException>(
                () => new UserScramCredentialUpsertion("u", info, new byte[] { 1 }, null!)).ParamName);
    }

    [Fact]
    public void BothAlterationKinds_ShareJavasAbstractBase()
    {
        Assert.True(typeof(UserScramCredentialAlteration).IsAbstract);
        Assert.Equal(typeof(UserScramCredentialAlteration), typeof(UserScramCredentialUpsertion).BaseType);
        Assert.Equal(typeof(UserScramCredentialAlteration), typeof(UserScramCredentialDeletion).BaseType);
        Assert.Equal(
            typeof(string),
            typeof(UserScramCredentialAlteration).GetMethod(
                nameof(UserScramCredentialAlteration.User), Type.EmptyTypes)!.ReturnType);
    }

    [Fact]
    public void FeaturesFamily_IsShortThroughout()
    {
        Assert.Equal(
            typeof(short),
            typeof(FinalizedVersionRange).GetMethod(
                nameof(FinalizedVersionRange.MinVersionLevel), Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            typeof(short),
            typeof(FinalizedVersionRange).GetMethod(
                nameof(FinalizedVersionRange.MaxVersionLevel), Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            typeof(short),
            typeof(SupportedVersionRange).GetMethod(
                nameof(SupportedVersionRange.MinVersion), Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            typeof(short),
            typeof(SupportedVersionRange).GetMethod(
                nameof(SupportedVersionRange.MaxVersion), Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            typeof(short),
            typeof(FeatureUpdate).GetMethod(nameof(FeatureUpdate.MaxVersionLevel), Type.EmptyTypes)!.ReturnType);
    }

    [Fact]
    public void FeatureMetadata_MirrorsJavasThreeMembers()
    {
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, FinalizedVersionRange>),
            typeof(FeatureMetadata).GetMethod(nameof(FeatureMetadata.FinalizedFeatures), Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            typeof(IReadOnlyDictionary<string, SupportedVersionRange>),
            typeof(FeatureMetadata).GetMethod(nameof(FeatureMetadata.SupportedFeatures), Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            typeof(long?),
            typeof(FeatureMetadata).GetMethod(nameof(FeatureMetadata.FinalizedFeaturesEpoch), Type.EmptyTypes)!.ReturnType);

        Assert.Empty(typeof(FeatureMetadata).GetConstructors());
    }

    [Fact]
    public void UpgradeType_SitsBesideFeatureUpdate_WithJavasConstants()
    {
        Assert.Null(typeof(UpgradeType).DeclaringType);
        Assert.Equal(typeof(FeatureUpdate).Namespace, typeof(UpgradeType).Namespace);
        Assert.Equal(typeof(UpgradeType), typeof(FeatureUpdate).GetMethod(nameof(FeatureUpdate.UpgradeType), Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            new[] { "UNKNOWN", "UPGRADE", "SAFE_DOWNGRADE", "UNSAFE_DOWNGRADE" },
            Enum.GetValues(typeof(UpgradeType))
                .Cast<UpgradeType>()
                .OrderBy(value => (int)value)
                .Select(value => value.ToString()));
    }

    [Fact]
    public void DescribeUserScramCredentialsResult_IsNotDisposable() =>
        Assert.False(
            typeof(IDisposable).IsAssignableFrom(typeof(DescribeUserScramCredentialsResult))
            || typeof(IAsyncDisposable).IsAssignableFrom(typeof(DescribeUserScramCredentialsResult)));

    private static IEnumerable<Type> Required(string name) =>
        typeof(IAdmin).GetMethod(name)!.GetParameters()
            .Where(parameter => !parameter.IsOptional)
            .Select(parameter => parameter.ParameterType);

    private static IEnumerable<string> PublicMembers(Type resultType) =>
        resultType
            .GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(member => member.MemberType is MemberTypes.Method or MemberTypes.Property)
            .Where(member => member is not MethodInfo method || !method.IsSpecialName)
            .Select(member => member.Name)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(name => name, StringComparer.Ordinal);

    [Fact]
    public void TokenInformation_FromRecord_IsTheSevenArgumentConstructor()
    {
        KafkaPrincipal owner = new KafkaPrincipal("User", "owner");
        KafkaPrincipal requester = new KafkaPrincipal("User", "req");
        KafkaPrincipal renewer = new KafkaPrincipal("User", "renewer");

        TokenInformation fromRecord = TokenInformation.FromRecord("id1", owner, requester, new[] { renewer }, 1, 3, 2);

        Assert.Equal(new TokenInformation("id1", owner, requester, new[] { renewer }, 1, 3, 2), fromRecord);
        Assert.Equal(requester, fromRecord.TokenRequester());
        Assert.Equal(2, fromRecord.ExpiryTimestamp());
        Assert.Equal(
            "renewers",
            Assert.Throws<ArgumentNullException>(() => TokenInformation.FromRecord("id1", owner, requester, null!, 1, 3, 2))
                .ParamName);
    }

    [Fact]
    public void TokenInformation_SetExpiryTimestamp_ChangesNeitherEqualityNorTheHash()
    {
        KafkaPrincipal owner = new KafkaPrincipal("User", "owner");
        KafkaPrincipal renewer = new KafkaPrincipal("User", "renewer");
        TokenInformation info = new TokenInformation("id1", owner, new[] { renewer }, 1, 3, 2);
        TokenInformation same = new TokenInformation("id1", owner, new[] { renewer }, 1, 3, 2);
        HashSet<TokenInformation> set = new HashSet<TokenInformation> { same };

        same.SetExpiryTimestamp(9);

        Assert.Equal(9, same.ExpiryTimestamp());
        Assert.Equal(info, same);
        Assert.Equal(info.GetHashCode(), same.GetHashCode());
        Assert.Contains(same, set);
        Assert.Equal(new DelegationToken(info, new byte[] { 1 }), new DelegationToken(same, new byte[] { 1 }));
        Assert.NotEqual(info, new TokenInformation("id1", owner, new[] { renewer }, 1, 4, 2));
    }
}
