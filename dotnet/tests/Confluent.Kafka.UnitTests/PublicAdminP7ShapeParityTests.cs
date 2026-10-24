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
/// The public shape of the eight delegation-token, feature and SCRAM RPCs, their results and their four
/// discriminants, against the Java classes they mirror.
/// </summary>
/// <remarks>
/// Four result shapes across eight RPCs: two per-key void (<c>AlterUserScramCredentials</c>, <c>UpdateFeatures</c>),
/// one a collection, four a single value, and one deriving three accessors from one snapshot. Each discriminant is a
/// nullable property, not a sentinel.
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
            resultType.GetProperty("Values")!.PropertyType);
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

        // But the renewer list is not nullable: Java defaults it to an empty list.
        Assert.Equal(
            Array.Empty<KafkaPrincipal>(),
            new CreateDelegationTokenOptions().Renewers);
    }

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

        Assert.Empty(typeof(FeatureMetadata).GetConstructors());
    }

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

    [Fact]
    public void TokenValueTypes_LiveInTheRootNamespace() =>
        Assert.All(
            new[] { typeof(KafkaPrincipal), typeof(TokenInformation), typeof(DelegationToken) },
            type => Assert.Equal("Confluent.Kafka", type.Namespace));

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
}
