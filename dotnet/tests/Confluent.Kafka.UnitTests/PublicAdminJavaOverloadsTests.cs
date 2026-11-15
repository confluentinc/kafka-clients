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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The admin clients declare the forms without the options beside each form that takes them (none for
/// <c>RemoveMembersFromConsumerGroup</c>), with no optional parameter, and the forms without the options run the RPC
/// with new options.
/// </summary>
public sealed class PublicAdminJavaOverloadsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    public static TheoryData<Type> AdminTypes => new TheoryData<Type>
    {
        typeof(IAdmin),
        typeof(KafkaAdminClient),
        typeof(MockAdminClient),
        typeof(IAsyncAdmin),
        typeof(AsyncKafkaAdminClient),
        typeof(AsyncMockAdminClient),
    };

    [Theory]
    [MemberData(nameof(AdminTypes))]
    public void NoPublicMethod_HasAnOptionalParameter_ButATrailingCancellationToken(Type type)
    {
        Assert.Empty(
            PublicMethods(type)
                .Where(method => method.GetParameters().Any(parameter =>
                    parameter.IsOptional && !IsTrailingCancellationToken(method, parameter)))
                .Select(Signature));
    }

    [Theory]
    [MemberData(nameof(AdminTypes))]
    public void EveryFormTakingItsOptions_TakesThemRequiredAndNotNullable_BesideTheFormWithout(Type type)
    {
        List<MethodInfo> longForms = PublicMethods(type).Where(EndsInAdminOptions).ToList();

        Assert.Equal(49, longForms.Count);
        foreach (MethodInfo longForm in longForms)
        {
            ParameterInfo[] parameters = longForm.GetParameters();
            ParameterInfo options = parameters[parameters.Length - 1];
            Assert.False(options.IsOptional, $"{Signature(longForm)} takes optional options");
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(options));

            // ListConfigResources() also leaves out the types, as no type means every type.
            ParameterInfo[] leading = longForm.Name == nameof(IAdmin.ListConfigResources)
                ? Array.Empty<ParameterInfo>()
                : parameters.Take(parameters.Length - 1).ToArray();
            MethodInfo? shortForm = type.GetMethod(
                longForm.Name, leading.Select(parameter => parameter.ParameterType).ToArray());
            if (longForm.Name == nameof(IAdmin.RemoveMembersFromConsumerGroup))
            {
                Assert.Null(shortForm);
                continue;
            }

            Assert.True(shortForm is not null, $"{Signature(longForm)} has no form without its options");
            Assert.Equal(longForm.ReturnType, shortForm!.ReturnType);
            Assert.Equal(
                leading.Select(parameter => parameter.Name),
                shortForm.GetParameters().Select(parameter => parameter.Name));
        }
    }

    [Theory]
    [MemberData(nameof(AdminTypes))]
    public void TheRpcsWithSeveralDefaults_HaveExactlyJavasForms(Type type)
    {
        AssertForms(
            type,
            nameof(IAdmin.ListConfigResources),
            Type.EmptyTypes,
            new[] { typeof(IReadOnlyCollection<ConfigResourceType>), typeof(ListConfigResourcesOptions) });
        AssertForms(
            type,
            nameof(IAdmin.ListPartitionReassignments),
            Type.EmptyTypes,
            new[] { typeof(ListPartitionReassignmentsOptions) },
            new[] { typeof(IReadOnlyCollection<TopicPartition>) },
            new[] { typeof(IReadOnlyCollection<TopicPartition>), typeof(ListPartitionReassignmentsOptions) });
        AssertForms(
            type,
            nameof(IAdmin.DescribeUserScramCredentials),
            Type.EmptyTypes,
            new[] { typeof(IReadOnlyCollection<string>) },
            new[] { typeof(IReadOnlyCollection<string>), typeof(DescribeUserScramCredentialsOptions) });
    }

    [Fact]
    public void CreateTopics_WithAndWithoutOptions_CreatesTheTopic()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        admin.CreateTopics(new[] { new NewTopic("without-options", 1, 1) }).All().Get(s_deadline);
        admin.CreateTopics(new[] { new NewTopic("with-options", 1, 1) }, new CreateTopicsOptions()).All().Get(s_deadline);

        IReadOnlyCollection<string> names = admin.ListTopics().Names().Get(s_deadline);
        Assert.Contains("without-options", names);
        Assert.Contains("with-options", names);
    }

    [Fact]
    public void DeleteTopicsByName_WithAndWithoutOptions_DeletesTheTopic()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        admin.CreateTopics(new[] { new NewTopic("without-options", 1, 1), new NewTopic("with-options", 1, 1) })
            .All().Get(s_deadline);

        DeleteTopicsResult withoutOptions = admin.DeleteTopics(new[] { "without-options" });
        DeleteTopicsResult withOptions = admin.DeleteTopics(new[] { "with-options" }, new DeleteTopicsOptions());
        withoutOptions.All().Get(s_deadline);
        withOptions.All().Get(s_deadline);

        Assert.Equal(new[] { "without-options" }, withoutOptions.TopicNameValues()!.Keys);
        Assert.Equal(new[] { "with-options" }, withOptions.TopicNameValues()!.Keys);
        IReadOnlyCollection<string> names = admin.ListTopics().Names().Get(s_deadline);
        Assert.DoesNotContain("without-options", names);
        Assert.DoesNotContain("with-options", names);
    }

    [Fact]
    public void ListConfigResources_WithoutArguments_ListsEveryType_AndWithTypes_Filters()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        admin.CreateTopics(new[] { new NewTopic("listed", 1, 1) }).All().Get(s_deadline);

        IReadOnlyCollection<ConfigResource> every = admin.ListConfigResources().All().Get(s_deadline);
        IReadOnlyCollection<ConfigResource> topics = admin.ListConfigResources(
            new[] { ConfigResourceType.TOPIC }, new ListConfigResourcesOptions()).All().Get(s_deadline);

        Assert.Contains(new ConfigResource(ConfigResourceType.TOPIC, "listed"), every);
        Assert.Contains(every, resource => resource.Type() == ConfigResourceType.BROKER);
        Assert.Equal(new ConfigResource(ConfigResourceType.TOPIC, "listed"), Assert.Single(topics));
    }

    [Fact]
    public async Task AsyncCreateTopics_WithAndWithoutOptions_CreatesTheTopic()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("without-options", 1, 1) }).All().Get(), s_deadline);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("with-options", 1, 1) }, new CreateTopicsOptions()).All().Get(),
            s_deadline);

        IReadOnlyCollection<string> names = await TestTimeout.Run(() => admin.ListTopics().Names().Get(), s_deadline);
        Assert.Contains("without-options", names);
        Assert.Contains("with-options", names);
    }

    [Fact]
    public async Task AsyncDeleteTopicsByName_WithAndWithoutOptions_DeletesTheTopic()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("without-options", 1, 1), new NewTopic("with-options", 1, 1) })
                .All().Get(),
            s_deadline);

        AsyncDeleteTopicsResult withoutOptions = admin.DeleteTopics(new[] { "without-options" });
        AsyncDeleteTopicsResult withOptions = admin.DeleteTopics(new[] { "with-options" }, new DeleteTopicsOptions());
        await TestTimeout.Run(() => withoutOptions.All().Get(), s_deadline);
        await TestTimeout.Run(() => withOptions.All().Get(), s_deadline);

        Assert.Equal(new[] { "without-options" }, withoutOptions.TopicNameValues()!.Keys);
        Assert.Equal(new[] { "with-options" }, withOptions.TopicNameValues()!.Keys);
        IReadOnlyCollection<string> names = await TestTimeout.Run(() => admin.ListTopics().Names().Get(), s_deadline);
        Assert.DoesNotContain("without-options", names);
        Assert.DoesNotContain("with-options", names);
    }

    [Fact]
    public async Task AsyncListConfigResources_WithoutArguments_ListsEveryType_AndWithTypes_Filters()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic("listed", 1, 1) }).All().Get(), s_deadline);

        IReadOnlyCollection<ConfigResource> every =
            await TestTimeout.Run(() => admin.ListConfigResources().All().Get(), s_deadline);
        IReadOnlyCollection<ConfigResource> topics = await TestTimeout.Run(
            () => admin.ListConfigResources(new[] { ConfigResourceType.TOPIC }, new ListConfigResourcesOptions()).All().Get(),
            s_deadline);

        Assert.Contains(new ConfigResource(ConfigResourceType.TOPIC, "listed"), every);
        Assert.Contains(every, resource => resource.Type() == ConfigResourceType.BROKER);
        Assert.Equal(new ConfigResource(ConfigResourceType.TOPIC, "listed"), Assert.Single(topics));
    }

    private static IEnumerable<MethodInfo> PublicMethods(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static);

    private static bool IsTrailingCancellationToken(MethodInfo method, ParameterInfo parameter) =>
        parameter.ParameterType == typeof(CancellationToken) && parameter.Position == method.GetParameters().Length - 1;

    private static bool EndsInAdminOptions(MethodInfo method)
    {
        ParameterInfo[] parameters = method.GetParameters();
        if (parameters.Length == 0)
        {
            return false;
        }

        Type last = parameters[parameters.Length - 1].ParameterType;
        return last.Namespace == typeof(IAdmin).Namespace && last.Name.EndsWith("Options", StringComparison.Ordinal);
    }

    private static void AssertForms(Type type, string name, params Type[][] forms) =>
        Assert.Equal(
            forms.Select(Describe).OrderBy(form => form, StringComparer.Ordinal),
            PublicMethods(type)
                .Where(method => method.Name == name)
                .Select(method => Describe(method.GetParameters().Select(parameter => parameter.ParameterType).ToArray()))
                .OrderBy(form => form, StringComparer.Ordinal));

    private static string Describe(Type[] parameterTypes) =>
        "(" + string.Join(", ", parameterTypes.Select(parameterType => parameterType.ToString())) + ")";

    private static string Signature(MethodInfo method) =>
        method.DeclaringType!.Name + "." + method.Name + "("
        + string.Join(", ", method.GetParameters().Select(parameter => parameter.ParameterType.Name + " " + parameter.Name))
        + ")";
}
