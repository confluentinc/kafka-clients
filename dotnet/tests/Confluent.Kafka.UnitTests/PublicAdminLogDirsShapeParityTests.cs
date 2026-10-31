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
        PropertyInfo descriptions =
            typeof(DescribeLogDirsResult).GetProperty(nameof(DescribeLogDirsResult.Descriptions))!;

        Assert.Equal(
            typeof(IReadOnlyDictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>>),
            descriptions.PropertyType);
        Assert.Null(descriptions.SetMethod);

        MethodInfo allDescriptions = typeof(DescribeLogDirsResult).GetMethod(
            nameof(DescribeLogDirsResult.AllDescriptions), Type.EmptyTypes)!;
        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>>),
            allDescriptions.ReturnType);

        Assert.Empty(typeof(DescribeLogDirsResult).GetConstructors());

        Assert.Equal(
            new[] { nameof(DescribeLogDirsResult.AllDescriptions) },
            DeclaredPublicMethodNames(typeof(DescribeLogDirsResult)));
        Assert.Equal(
            new[] { nameof(DescribeLogDirsResult.Descriptions) },
            PropertyNames(typeof(DescribeLogDirsResult)));
    }

    [Fact]
    public void TheTwoReplicaResults_DifferExactlyAsJavasDo()
    {
        PropertyInfo alterValues =
            typeof(AlterReplicaLogDirsResult).GetProperty(nameof(AlterReplicaLogDirsResult.Values))!;

        Assert.Equal(typeof(IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture>), alterValues.PropertyType);
        Assert.Null(alterValues.SetMethod);

        MethodInfo alterAll =
            typeof(AlterReplicaLogDirsResult).GetMethod(nameof(AlterReplicaLogDirsResult.All), Type.EmptyTypes)!;

        Assert.Equal(typeof(KafkaFuture), alterAll.ReturnType);

        PropertyInfo describeValues =
            typeof(DescribeReplicaLogDirsResult).GetProperty(nameof(DescribeReplicaLogDirsResult.Values))!;
        Assert.Equal(
            typeof(IReadOnlyDictionary<TopicPartitionReplica, KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>),
            describeValues.PropertyType);
        Assert.Null(describeValues.SetMethod);

        MethodInfo describeAll = typeof(DescribeReplicaLogDirsResult).GetMethod(
            nameof(DescribeReplicaLogDirsResult.All), Type.EmptyTypes)!;
        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyDictionary<TopicPartitionReplica,
                DescribeReplicaLogDirsResult.ReplicaLogDirInfo>>),
            describeAll.ReturnType);

        foreach (Type type in new[] { typeof(AlterReplicaLogDirsResult), typeof(DescribeReplicaLogDirsResult) })
        {
            Assert.Empty(type.GetConstructors());

            Assert.Equal(new[] { "All" }, DeclaredPublicMethodNames(type));
            Assert.Equal(new[] { "Values" }, PropertyNames(type));
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
    public void TheBeanAccessorStyle_IsConfinedToReplicaLogDirInfo()
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
                "ReplicaLogDirInfo.GetCurrentReplicaLogDir",
                "ReplicaLogDirInfo.GetCurrentReplicaOffsetLag",
                "ReplicaLogDirInfo.GetFutureReplicaLogDir",
                "ReplicaLogDirInfo.GetFutureReplicaOffsetLag",
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
        PropertyInfo? cordoned = typeof(LogDirDescription).GetProperty(
            nameof(LogDirDescription.IsCordoned),
            BindingFlags.Public | BindingFlags.Instance);
        Assert.NotNull(cordoned);
        Assert.Equal(typeof(bool), cordoned!.PropertyType);

        // A control: the same walk finds the other members.
        Assert.Contains(members, name => string.Equals(name, nameof(LogDirDescription.TotalBytes), StringComparison.Ordinal));
        Assert.Contains(members, name => string.Equals(name, nameof(LogDirDescription.Error), StringComparison.Ordinal));
    }

    [Fact]
    public void TheValueTypes_ExposeJavasAccessorsAsProperties()
    {
        Assert.Equal(
            new[] { "BrokerId", "Partition", "Topic" },
            PropertyNames(typeof(TopicPartitionReplica)));
        Assert.Equal(typeof(string), typeof(TopicPartitionReplica).GetProperty("Topic")!.PropertyType);
        Assert.Equal(typeof(int), typeof(TopicPartitionReplica).GetProperty("Partition")!.PropertyType);
        Assert.Equal(typeof(int), typeof(TopicPartitionReplica).GetProperty("BrokerId")!.PropertyType);

        Assert.Equal(new[] { "IsFuture", "OffsetLag", "Size" }, PropertyNames(typeof(ReplicaInfo)));
        Assert.Equal(typeof(long), typeof(ReplicaInfo).GetProperty("Size")!.PropertyType);
        Assert.Equal(typeof(long), typeof(ReplicaInfo).GetProperty("OffsetLag")!.PropertyType);
        Assert.Equal(typeof(bool), typeof(ReplicaInfo).GetProperty("IsFuture")!.PropertyType);

        Assert.Equal(
            new[] { "Error", "IsCordoned", "ReplicaInfos", "TotalBytes", "UsableBytes" },
            PropertyNames(typeof(LogDirDescription)));

        Assert.Equal(typeof(bool), typeof(LogDirDescription).GetProperty("IsCordoned")!.PropertyType);

        // Java's OptionalLong maps to long?, the ABI's -1 becoming null: a plain long would erase the empty case.
        Assert.Equal(typeof(long?), typeof(LogDirDescription).GetProperty("TotalBytes")!.PropertyType);
        Assert.Equal(typeof(long?), typeof(LogDirDescription).GetProperty("UsableBytes")!.PropertyType);

        // Java's ApiException maps to the binding's one KafkaException, null for a healthy directory.
        Assert.Equal(typeof(KafkaException), typeof(LogDirDescription).GetProperty("Error")!.PropertyType);
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(typeof(LogDirDescription).GetProperty("Error")!));

        Assert.Equal(
            typeof(IReadOnlyDictionary<TopicPartition, ReplicaInfo>),
            typeof(LogDirDescription).GetProperty("ReplicaInfos")!.PropertyType);

        foreach (Type type in new[] { typeof(TopicPartitionReplica), typeof(ReplicaInfo), typeof(LogDirDescription) })
        {
            Assert.All(type.GetProperties(), property => Assert.Null(property.SetMethod));
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
            Assert.Equal(new[] { "TimeoutMs" }, PropertyNames(type));

            PropertyInfo timeout = type.GetProperty("TimeoutMs")!;
            Assert.Equal(typeof(int?), timeout.PropertyType);
            Assert.NotNull(timeout.SetMethod);

            // Java's default is the client default, which is null here, not 0.
            Assert.Null((int?)timeout.GetValue(Activator.CreateInstance(type)));
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
