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
using Confluent.Kafka.Common.Errors;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public shape of the log-dirs surface (<c>DescribeLogDirs</c>, <c>AlterReplicaLogDirs</c>,
/// <c>DescribeReplicaLogDirs</c>, their options, results and value types) against the Java classes it mirrors.
/// </summary>
/// <remarks>
/// These are reflection assertions: a behavioural test passes equally against a property and a method, or against
/// <c>T</c> and <c>T?</c>.
/// </remarks>
public sealed class PublicAdminLogDirsShapeParityTests
{
    [Fact]
    public void IAdmin_TheThreeNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo describeLogDirs = typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeLogDirs))!;
        MethodInfo alter = typeof(IAdmin).GetMethod(nameof(IAdmin.AlterReplicaLogDirs))!;
        MethodInfo describeReplicas = typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeReplicaLogDirs))!;

        Assert.Equal(typeof(DescribeLogDirsResult), describeLogDirs.ReturnType);
        Assert.Equal(typeof(AlterReplicaLogDirsResult), alter.ReturnType);
        Assert.Equal(typeof(DescribeReplicaLogDirsResult), describeReplicas.ReturnType);

        Assert.Equal(
            new[] { typeof(IReadOnlyCollection<int>), typeof(DescribeLogDirsOptions) },
            describeLogDirs.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyDictionary<TopicPartitionReplica, string>),
                typeof(AlterReplicaLogDirsOptions),
            },
            alter.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyCollection<TopicPartitionReplica>),
                typeof(DescribeReplicaLogDirsOptions),
            },
            describeReplicas.GetParameters().Select(parameter => parameter.ParameterType));

        foreach (MethodInfo rpc in new[] { describeLogDirs, alter, describeReplicas })
        {
            ParameterInfo[] parameters = rpc.GetParameters();
            Assert.False(parameters[0].IsOptional, $"{rpc.Name}'s input must be required");
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(parameters[0]));
            Assert.True(parameters[1].IsOptional, $"{rpc.Name}'s options must be optional");
            Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(parameters[1]));
        }

        Assert.Empty(
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name));
    }

    [Fact]
    public void DescribeLogDirsResult_MatchesJavasTwoAccessors()
    {
        MethodInfo descriptions = Getters.Get(typeof(DescribeLogDirsResult), nameof(DescribeLogDirsResult.Descriptions));

        Assert.Equal(
            typeof(IReadOnlyDictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>>),
            descriptions.ReturnType);

        MethodInfo allDescriptions = typeof(DescribeLogDirsResult).GetMethod(
            nameof(DescribeLogDirsResult.AllDescriptions), Type.EmptyTypes)!;
        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>>),
            allDescriptions.ReturnType);

        Assert.Empty(typeof(DescribeLogDirsResult).GetConstructors());

        Assert.Equal(
            new[] { nameof(DescribeLogDirsResult.AllDescriptions), nameof(DescribeLogDirsResult.Descriptions) },
            DeclaredPublicMethodNames(typeof(DescribeLogDirsResult)));
        Assert.Empty(PropertyNames(typeof(DescribeLogDirsResult)));
    }

    [Fact]
    public void TheTwoReplicaResults_DifferExactlyAsJavasDo()
    {
        MethodInfo alterValues = Getters.Get(typeof(AlterReplicaLogDirsResult), nameof(AlterReplicaLogDirsResult.Values));

        Assert.Equal(typeof(IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture>), alterValues.ReturnType);

        MethodInfo alterAll =
            typeof(AlterReplicaLogDirsResult).GetMethod(nameof(AlterReplicaLogDirsResult.All), Type.EmptyTypes)!;

        Assert.Equal(typeof(KafkaFuture), alterAll.ReturnType);

        MethodInfo describeValues =
            Getters.Get(typeof(DescribeReplicaLogDirsResult), nameof(DescribeReplicaLogDirsResult.Values));
        Assert.Equal(
            typeof(IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>),
            describeValues.ReturnType);

        MethodInfo describeAll = typeof(DescribeReplicaLogDirsResult).GetMethod(
            nameof(DescribeReplicaLogDirsResult.All), Type.EmptyTypes)!;
        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<TopicPartitionReplica,
                DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>),
            describeAll.ReturnType);

        foreach (Type type in new[] { typeof(AlterReplicaLogDirsResult), typeof(DescribeReplicaLogDirsResult) })
        {
            Assert.Empty(type.GetConstructors());

            Assert.Equal(new[] { "All", "Values" }, DeclaredPublicMethodNames(type));
            Assert.Empty(PropertyNames(type));
        }
    }

    [Fact]
    public void ReplicaLogDirInfo_IsNested_WithJavasBeanAccessors()
    {
        Type info = typeof(DescribeReplicaLogDirsResult.ReplicaLogDirInfo);

        Assert.Same(typeof(DescribeReplicaLogDirsResult), info.DeclaringType);
        Assert.Same(info, typeof(DescribeReplicaLogDirsResult).GetNestedType("ReplicaLogDirInfo"));

        Assert.Null(typeof(DescribeReplicaLogDirsResult).GetProperty("ReplicaLogDirInfo"));
        Assert.Null(typeof(DescribeReplicaLogDirsResult).GetMethod("ReplicaLogDirInfo"));

        Assert.Empty(info.GetProperties());
        Assert.Equal(
            new[]
            {
                "GetCurrentReplicaLogDir", "GetCurrentReplicaOffsetLag", "GetFutureReplicaLogDir",
                "GetFutureReplicaOffsetLag", "ToString",
            },
            DeclaredPublicMethodNames(info));

        Assert.Empty(info.GetConstructors());

        // Both directory accessors are nullable; both lags are plain longs, where -1 is a value, not an absence.
        Assert.Equal(typeof(string), info.GetMethod("GetCurrentReplicaLogDir")!.ReturnType);
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(info.GetMethod("GetCurrentReplicaLogDir")!.ReturnParameter));
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(info.GetMethod("GetFutureReplicaLogDir")!.ReturnParameter));
        Assert.Equal(typeof(long), info.GetMethod("GetCurrentReplicaOffsetLag")!.ReturnType);
        Assert.Equal(typeof(long), info.GetMethod("GetFutureReplicaOffsetLag")!.ReturnType);
    }

    [Fact]
    public void TheBeanAccessorStyle_IsConfinedToJavasOwnGetNames()
    {
        List<string> beanAccessors = new List<string>();
        foreach (Type type in typeof(IAdmin).Assembly.GetExportedTypes())
        {
            HashSet<MethodInfo> frameworkContract = FrameworkInterfaceImplementations(type);

            foreach (MethodInfo method in type.GetMethods(
                BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                if (method.IsSpecialName
                    || method.GetBaseDefinition().DeclaringType == typeof(object)
                    || frameworkContract.Contains(method))
                {
                    continue;
                }

                if (method.Name.Length > 3
                    && method.GetParameters().Length == 0
                    && method.ReturnType != typeof(void)
                    && method.Name.StartsWith("Get", StringComparison.Ordinal)
                    && char.IsUpper(method.Name[3]))
                {
                    beanAccessors.Add($"{type.Name}.{method.Name}");
                }
            }
        }

        Assert.Equal(
            new[]
            {
                "KafkaPrincipal.GetName",
                "KafkaPrincipal.GetPrincipalType",
                "ReplicaLogDirInfo.GetCurrentReplicaLogDir",
                "ReplicaLogDirInfo.GetCurrentReplicaOffsetLag",
                "ReplicaLogDirInfo.GetFutureReplicaLogDir",
                "ReplicaLogDirInfo.GetFutureReplicaOffsetLag",
                "Uuid.GetLeastSignificantBits",
                "Uuid.GetMostSignificantBits",
            },
            beanAccessors.OrderBy(name => name, StringComparer.Ordinal));
    }

    private static HashSet<MethodInfo> FrameworkInterfaceImplementations(Type type)
    {
        HashSet<MethodInfo> implementations = new HashSet<MethodInfo>();
        if (type.IsInterface)
        {
            return implementations;
        }

        foreach (Type contract in type.GetInterfaces())
        {
            if (contract.Namespace is null
                || !contract.Namespace.StartsWith("System", StringComparison.Ordinal))
            {
                continue;
            }

            foreach (MethodInfo target in type.GetInterfaceMap(contract).TargetMethods)
            {
                implementations.Add(target);
            }
        }

        return implementations;
    }

    [Fact]
    public void LogDirDescription_HasTheCordonedMember_AndTheSweepThatProvesItFindsTheOthers()
    {
        const BindingFlags Everything =
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static
            | BindingFlags.DeclaredOnly;

        string[] members = typeof(LogDirDescription).GetMembers(Everything)
            .Select(member => member.Name)
            .ToArray();

        Assert.Contains(members, name => string.Equals(name, nameof(LogDirDescription.IsCordoned), StringComparison.Ordinal));
        MethodInfo? cordoned = typeof(LogDirDescription).GetMethod(
            nameof(LogDirDescription.IsCordoned),
            BindingFlags.Public | BindingFlags.Instance,
            null,
            Type.EmptyTypes,
            null);
        Assert.NotNull(cordoned);
        Assert.Equal(typeof(bool), cordoned!.ReturnType);

        // A control: the same walk finds the other members.
        Assert.Contains(members, name => string.Equals(name, nameof(LogDirDescription.TotalBytes), StringComparison.Ordinal));
        Assert.Contains(members, name => string.Equals(name, nameof(LogDirDescription.Error), StringComparison.Ordinal));
    }

    [Fact]
    public void TheValueTypes_ExposeJavasAccessorsAsMethods()
    {
        Assert.Equal(
            new[] { "BrokerId", "Partition", "Topic" },
            Getters.NamesOf(typeof(TopicPartitionReplica)));
        Assert.Equal(typeof(string), typeof(TopicPartitionReplica).GetMethod("Topic", Type.EmptyTypes)!.ReturnType);
        Assert.Equal(typeof(int), typeof(TopicPartitionReplica).GetMethod("Partition", Type.EmptyTypes)!.ReturnType);
        Assert.Equal(typeof(int), typeof(TopicPartitionReplica).GetMethod("BrokerId", Type.EmptyTypes)!.ReturnType);

        Assert.Equal(new[] { "IsFuture", "OffsetLag", "Size" }, Getters.NamesOf(typeof(ReplicaInfo)));
        Assert.Equal(typeof(long), typeof(ReplicaInfo).GetMethod("Size", Type.EmptyTypes)!.ReturnType);
        Assert.Equal(typeof(long), typeof(ReplicaInfo).GetMethod("OffsetLag", Type.EmptyTypes)!.ReturnType);
        Assert.Equal(typeof(bool), typeof(ReplicaInfo).GetMethod("IsFuture", Type.EmptyTypes)!.ReturnType);

        Assert.Equal(
            new[] { "Error", "IsCordoned", "ReplicaInfos", "TotalBytes", "UsableBytes" },
            Getters.NamesOf(typeof(LogDirDescription)));

        Assert.Equal(typeof(bool), typeof(LogDirDescription).GetMethod("IsCordoned", Type.EmptyTypes)!.ReturnType);

        // Java's OptionalLong maps to long?, the ABI's -1 becoming null: a plain long would erase the empty case.
        Assert.Equal(typeof(long?), typeof(LogDirDescription).GetMethod("TotalBytes", Type.EmptyTypes)!.ReturnType);
        Assert.Equal(typeof(long?), typeof(LogDirDescription).GetMethod("UsableBytes", Type.EmptyTypes)!.ReturnType);

        Assert.Equal(typeof(ApiException), typeof(LogDirDescription).GetMethod("Error", Type.EmptyTypes)!.ReturnType);
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(typeof(LogDirDescription).GetMethod("Error", Type.EmptyTypes)!));

        Assert.Equal(
            typeof(IReadOnlyDictionary<TopicPartition, ReplicaInfo>),
            typeof(LogDirDescription).GetMethod("ReplicaInfos", Type.EmptyTypes)!.ReturnType);

        foreach (Type type in new[] { typeof(TopicPartitionReplica), typeof(ReplicaInfo), typeof(LogDirDescription) })
        {
            Assert.Empty(type.GetProperties());
        }
    }

    [Fact]
    public void TheConstructorVisibility_FollowsJava()
    {
        ConstructorInfo replicaInfo = Assert.Single(typeof(ReplicaInfo).GetConstructors());
        Assert.Equal(
            new[] { typeof(long), typeof(long), typeof(bool) },
            replicaInfo.GetParameters().Select(parameter => parameter.ParameterType));

        ConstructorInfo replica = Assert.Single(typeof(TopicPartitionReplica).GetConstructors());
        Assert.Equal(
            new[] { typeof(string), typeof(int), typeof(int) },
            replica.GetParameters().Select(parameter => parameter.ParameterType));
    }

    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        foreach (Type type in new[]
                 {
                     typeof(DescribeLogDirsOptions),
                     typeof(AlterReplicaLogDirsOptions),
                     typeof(DescribeReplicaLogDirsOptions),
                 })
        {
            SetterMethods.AssertGettersAndSetters(type, "TimeoutMs");

            Assert.Equal(typeof(int?), Getters.Get(type, "TimeoutMs").ReturnType);
            Assert.Equal(typeof(int?), SetterMethods.ParameterTypeOf(type, "TimeoutMs"));

            // Java's default is the client default, which is null here, not 0.
            Assert.Null((int?)Getters.Get(type, "TimeoutMs").Invoke(Activator.CreateInstance(type), null));
        }
    }

    [Fact]
    public void NoAbiIntermediateType_LeakedIntoThePublicSurface()
    {
        Assert.DoesNotContain(
            typeof(IAdmin).Assembly.GetExportedTypes(),
            type => type.Name.IndexOf("LogDirDescriptionMap", StringComparison.Ordinal) >= 0);
    }

    private static string[] DeclaredPublicMethodNames(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method => !method.IsSpecialName)
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

    private static string[] PropertyNames(Type type) =>
        type.GetProperties().Select(property => property.Name).OrderBy(name => name, StringComparer.Ordinal).ToArray();
}
