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
using System.Globalization;
using System.Reflection;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="NewTopic"/>'s value semantics — Java's <c>equals</c> / <c>hashCode</c> /
/// <c>toString</c> (<c>NewTopic.java:149-174</c>). Pure managed; nothing native.
/// </summary>
public sealed class PublicAdminNewTopicValueTests
{
    /// <summary>
    /// Two topics built the same way are equal and hash-equal, for every constructor form
    /// — with and without a configuration — and equality is reflexive and never matches
    /// <see langword="null"/> or a foreign type.
    /// </summary>
    [Fact]
    public void Equal_ForEachConstructorForm()
    {
        foreach (Func<NewTopic> build in new Func<NewTopic>[]
        {
            () => new NewTopic("t", 3, (short)1),
            () => new NewTopic("t", (int?)3, (short?)null),
            () => new NewTopic("t", (int?)null, (short?)null),
            () => new NewTopic("t", -1, (short)-1),
            () => new NewTopic("t", Assignments((0, new[] { 1, 2 }), (1, new[] { 2, 3 }))),
            () => new NewTopic("t", 3, (short)1) { Configs = Configs(("k", "v"), ("k2", "v2")) },
            () => new NewTopic("t", Assignments((0, new[] { 1 }))) { Configs = Configs() },
        })
        {
            NewTopic left = build();
            NewTopic right = build();

            Assert.NotSame(left, right);
            Assert.True(left.Equals(right));
            Assert.True(right.Equals(left));
            Assert.Equal(left.GetHashCode(), right.GetHashCode());

            Assert.True(left.Equals(left));
            Assert.False(left.Equals(null));
            Assert.False(left.Equals("t"));
        }

        // The two count constructors are one form: (string, int, short) forwards.
        NewTopic viaNonNullable = new NewTopic("t", 3, (short)1);
        NewTopic viaNullable = new NewTopic("t", (int?)3, (short?)1);
        Assert.Equal(viaNonNullable, viaNullable);
        Assert.Equal(viaNonNullable.GetHashCode(), viaNullable.GetHashCode());
    }

    /// <summary>
    /// Every field takes part: a difference in any one of the five makes the topics
    /// unequal. <c>-1</c> is <b>not</b> <see langword="null"/> — Java's
    /// <c>Optional.of(-1)</c> is not <c>Optional.empty()</c> — and the name compares
    /// ordinally, case included.
    /// </summary>
    [Fact]
    public void Unequal_WhenAnyFieldDiffers()
    {
        NewTopic baseline = new NewTopic("t", 3, (short)1) { Configs = Configs(("k", "v")) };

        foreach (NewTopic different in new[]
        {
            new NewTopic("T", 3, (short)1) { Configs = Configs(("k", "v")) },
            new NewTopic("u", 3, (short)1) { Configs = Configs(("k", "v")) },
            new NewTopic("t", 4, (short)1) { Configs = Configs(("k", "v")) },
            new NewTopic("t", 3, (short)2) { Configs = Configs(("k", "v")) },
            new NewTopic("t", (int?)null, (short?)1) { Configs = Configs(("k", "v")) },
            new NewTopic("t", (int?)3, (short?)null) { Configs = Configs(("k", "v")) },
            new NewTopic("t", 3, (short)1),
            new NewTopic("t", 3, (short)1) { Configs = Configs() },
            new NewTopic("t", 3, (short)1) { Configs = Configs(("k", "V")) },
            new NewTopic("t", 3, (short)1) { Configs = Configs(("K", "v")) },
            new NewTopic("t", 3, (short)1) { Configs = Configs(("k", "v"), ("k2", "v2")) },
            new NewTopic("t", Assignments((0, new[] { 1 }))) { Configs = Configs(("k", "v")) },
        })
        {
            Assert.False(baseline.Equals(different), different.ToString());
            Assert.False(different.Equals(baseline), different.ToString());
        }

        // -1 against null, for each count on its own.
        Assert.NotEqual(new NewTopic("t", (int?)-1, (short?)1), new NewTopic("t", (int?)null, (short?)1));
        Assert.NotEqual(new NewTopic("t", (int?)1, (short?)-1), new NewTopic("t", (int?)1, (short?)null));
        Assert.NotEqual(new NewTopic("t", -1, (short)-1), new NewTopic("t", (int?)null, (short?)null));

        // A null configuration is not an empty one — Java's Objects.equals(null, emptyMap).
        Assert.NotEqual(new NewTopic("t", 1, (short)1), new NewTopic("t", 1, (short)1) { Configs = Configs() });

        // The replica assignment: a different partition, broker, broker order or length.
        NewTopic assigned = new NewTopic("t", Assignments((0, new[] { 1, 2 })));
        foreach (NewTopic different in new[]
        {
            new NewTopic("t", Assignments((1, new[] { 1, 2 }))),
            new NewTopic("t", Assignments((0, new[] { 1, 3 }))),
            new NewTopic("t", Assignments((0, new[] { 2, 1 }))),
            new NewTopic("t", Assignments((0, new[] { 1 }))),
            new NewTopic("t", Assignments((0, new[] { 1, 2 }), (1, new[] { 1, 2 }))),
            new NewTopic("t", Assignments()),
        })
        {
            Assert.False(assigned.Equals(different), different.ToString());
            Assert.False(different.Equals(assigned), different.ToString());
        }
    }

    /// <summary>
    /// Java's <c>Map.equals</c> / <c>hashCode</c> do not depend on iteration order, so
    /// neither does <see cref="NewTopic"/>'s: the same entries inserted in a different
    /// order — or held by a different dictionary type — are equal and hash-equal.
    /// </summary>
    [Fact]
    public void EqualityAndHash_IgnoreInsertionOrder()
    {
        NewTopic forward = new NewTopic(
            "t", Assignments((0, new[] { 1, 2 }), (1, new[] { 2, 3 }), (2, new[] { 3, 1 })))
        {
            Configs = Configs(("a", "1"), ("b", "2"), ("c", "3")),
        };
        NewTopic backward = new NewTopic(
            "t", Assignments((2, new[] { 3, 1 }), (1, new[] { 2, 3 }), (0, new[] { 1, 2 })))
        {
            Configs = Configs(("c", "3"), ("b", "2"), ("a", "1")),
        };

        Assert.Equal(forward, backward);
        Assert.Equal(forward.GetHashCode(), backward.GetHashCode());

        NewTopic sorted = new NewTopic(
            "t",
            new SortedDictionary<int, IReadOnlyList<int>>
            {
                [1] = new[] { 2, 3 },
                [0] = new[] { 1, 2 },
                [2] = new[] { 3, 1 },
            })
        {
            Configs = new SortedDictionary<string, string> { ["b"] = "2", ["c"] = "3", ["a"] = "1" },
        };

        Assert.Equal(forward, sorted);
        Assert.Equal(forward.GetHashCode(), sorted.GetHashCode());
    }

    /// <summary>
    /// The configuration compares <b>ordinally</b> whatever comparer the user's dictionary
    /// carries — so equality stays symmetric and hash-consistent. A case-insensitive
    /// dictionary equals an ordinal one with the same spelling, and differs from one
    /// whose key differs only in case.
    /// </summary>
    [Fact]
    public void ConfigsEquality_IsOrdinal_WhateverTheDictionaryComparer()
    {
        NewTopic ordinal = new NewTopic("t", 1, (short)1)
        {
            Configs = new Dictionary<string, string>(StringComparer.Ordinal) { ["cleanup.policy"] = "compact" },
        };
        NewTopic ignoreCase = new NewTopic("t", 1, (short)1)
        {
            Configs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["cleanup.policy"] = "compact" },
        };
        NewTopic ignoreCaseUpper = new NewTopic("t", 1, (short)1)
        {
            Configs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["CLEANUP.POLICY"] = "compact" },
        };

        Assert.True(ordinal.Equals(ignoreCase));
        Assert.True(ignoreCase.Equals(ordinal));
        Assert.Equal(ordinal.GetHashCode(), ignoreCase.GetHashCode());

        // Both directions agree — a keyed lookup through the case-insensitive side would
        // have said "equal" one way and "unequal" the other.
        Assert.False(ordinal.Equals(ignoreCaseUpper));
        Assert.False(ignoreCaseUpper.Equals(ordinal));
        Assert.False(ignoreCase.Equals(ignoreCaseUpper));
        Assert.False(ignoreCaseUpper.Equals(ignoreCase));
    }

    /// <summary>
    /// <see cref="NewTopic.Configs"/> is settable, so equality and the hash code follow it —
    /// as Java's <c>configs(Map)</c> does. Documented on the property.
    /// </summary>
    [Fact]
    public void EqualityAndHash_FollowConfigsReassignment()
    {
        NewTopic left = new NewTopic("t", 1, (short)1);
        NewTopic right = new NewTopic("t", 1, (short)1);
        Assert.Equal(left, right);

        right.Configs = Configs(("k", "v"));
        Assert.NotEqual(left, right);

        left.Configs = Configs(("k", "v"));
        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());

        // A mutation behind the reference is seen too: the dictionary is held, not copied.
        Dictionary<string, string> mutable = new Dictionary<string, string> { ["k"] = "v" };
        left.Configs = mutable;
        Assert.Equal(left, right);
        mutable["k"] = "w";
        Assert.NotEqual(left, right);
    }

    /// <summary>
    /// Java's exact <c>toString</c> (<c>NewTopic.java:149-157</c>): <c>default</c> for an
    /// unset count, <c>-1</c> printed as itself, and the maps in <c>AbstractMap</c> /
    /// <c>AbstractCollection</c> form — single-entry maps, so the order is fixed.
    /// </summary>
    [Fact]
    public void ToString_IsJavasFormat()
    {
        Assert.Equal(
            "(name=t, numPartitions=3, replicationFactor=1, replicasAssignments=null, configs=null)",
            new NewTopic("t", 3, (short)1).ToString());

        Assert.Equal(
            "(name=t, numPartitions=default, replicationFactor=default, replicasAssignments=null, configs=null)",
            new NewTopic("t", (int?)null, (short?)null).ToString());

        Assert.Equal(
            "(name=t, numPartitions=-1, replicationFactor=-1, replicasAssignments=null, configs=null)",
            new NewTopic("t", -1, (short)-1).ToString());

        Assert.Equal(
            "(name=t, numPartitions=default, replicationFactor=default, replicasAssignments={0=[1, 2]}, configs={k=v})",
            new NewTopic("t", Assignments((0, new[] { 1, 2 }))) { Configs = Configs(("k", "v")) }.ToString());

        Assert.Equal(
            "(name=t, numPartitions=default, replicationFactor=default, replicasAssignments={}, configs={})",
            new NewTopic("t", Assignments()) { Configs = Configs() }.ToString());
    }

    /// <summary>
    /// The rendering does not follow the current culture: a culture whose negative sign
    /// is not <c>'-'</c> still prints Java's <c>-1</c>, for the counts and the broker ids.
    /// </summary>
    [Fact]
    public void ToString_IsCultureInvariant()
    {
        CultureInfo original = CultureInfo.CurrentCulture;
        CultureInfo tilde = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        tilde.NumberFormat.NegativeSign = "~";
        try
        {
            CultureInfo.CurrentCulture = tilde;
            Assert.Equal("~1", (-1).ToString(CultureInfo.CurrentCulture));

            Assert.Equal(
                "(name=t, numPartitions=-1, replicationFactor=-1, replicasAssignments=null, configs=null)",
                new NewTopic("t", -1, (short)-1).ToString());
            Assert.Equal(
                "(name=t, numPartitions=default, replicationFactor=default, replicasAssignments={0=[-1, 2]}, configs=null)",
                new NewTopic("t", Assignments((0, new[] { -1, 2 }))).ToString());
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    /// <summary>
    /// The three overrides are declared on <see cref="NewTopic"/> itself — not inherited
    /// from <see cref="object"/>, whose reference equality Java's type does not have.
    /// </summary>
    [Fact]
    public void Overrides_AreDeclaredOnNewTopic()
    {
        Assert.Equal(
            typeof(NewTopic),
            typeof(NewTopic).GetMethod(nameof(Equals), new[] { typeof(object) })!.DeclaringType);
        Assert.Equal(
            typeof(NewTopic),
            typeof(NewTopic).GetMethod(nameof(GetHashCode), Type.EmptyTypes)!.DeclaringType);
        Assert.Equal(
            typeof(NewTopic),
            typeof(NewTopic).GetMethod(nameof(ToString), Type.EmptyTypes)!.DeclaringType);

        // Equals(object) only — Java has no typed equals, so no IEquatable<NewTopic>.
        Assert.Empty(typeof(NewTopic).GetInterfaces());
        MethodInfo onlyEquals = Assert.Single(
            typeof(NewTopic).GetMethods(BindingFlags.Public | BindingFlags.Instance),
            method => method.Name == nameof(Equals));
        Assert.Equal(typeof(object), Assert.Single(onlyEquals.GetParameters()).ParameterType);
    }

    private static Dictionary<int, IReadOnlyList<int>> Assignments(params (int Partition, int[] Brokers)[] entries)
    {
        Dictionary<int, IReadOnlyList<int>> assignments = new Dictionary<int, IReadOnlyList<int>>();
        foreach ((int partition, int[] brokers) in entries)
        {
            assignments.Add(partition, brokers);
        }

        return assignments;
    }

    private static Dictionary<string, string> Configs(params (string Key, string Value)[] entries)
    {
        Dictionary<string, string> configs = new Dictionary<string, string>();
        foreach ((string key, string value) in entries)
        {
            configs.Add(key, value);
        }

        return configs;
    }
}
