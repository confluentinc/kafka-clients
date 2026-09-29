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
using System.Linq;
using System.Reflection;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The <see cref="ConsumerGroupMetadata"/> constructors (M17/P1 S3, its CP2 half; decision
/// D2): Java's <c>ConsumerGroupMetadataTest</c> translated, <c>MockProducerTest</c>'s
/// null-metadata test mapped to the constructor that raises it, the <c>[Obsolete]</c>
/// contract read by reflection, and the internal factory the ABI marshal uses.
/// </summary>
/// <remarks>
/// <para>
/// <c>ConsumerGroupMetadataTest.testInvalidInstanceId</c> (<c>:81</c>) is not translated. It
/// passes a null <c>Optional&lt;String&gt;</c> reference, and a .NET <c>string?</c> has no
/// state apart from <see langword="null"/> to reject: <see langword="null"/> is the empty
/// case (PLAN §5.3.2).
/// </para>
/// <para>
/// Each use of an obsolete constructor suppresses <c>CS0618</c> locally (PLAN §5.1 item 9).
/// Expected <see cref="ArgumentNullException"/> messages are built with the framework's own
/// constructor, which appends the parameter name in each target framework's format (PLAN
/// §5.1 item 2).
/// </para>
/// </remarks>
public sealed class ConsumerGroupMetadataTests
{
    // M17/P1 D2's text (Q7).
    private const string ObsoleteMessage =
        "Deprecated since Kafka 4.2: use IConsumerCommon.GroupMetadata() instead. "
        + "ConsumerGroupMetadata becomes an interface in Kafka 5.0.";

    // ConsumerGroupMetadataTest.groupId (:33).
    private const string GroupId = "group";

    private const string NonAscii = "grüße-café-Ω-日本語-😀";

    /// <summary>
    /// Java <c>ConsumerGroupMetadataTest.testAssignmentConstructor</c> (<c>:36</c>): the
    /// four-argument constructor keeps all four values, an instance id included.
    /// </summary>
    [Fact]
    public void AssignmentConstructor_KeepsAllFourValues()
    {
#pragma warning disable CS0618 // Type or member is obsolete
        ConsumerGroupMetadata groupMetadata = new(GroupId, 2, "member", "instance");
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal(GroupId, groupMetadata.GroupId);
        Assert.Equal(2, groupMetadata.GenerationId);
        Assert.Equal("member", groupMetadata.MemberId);
        Assert.Equal("instance", groupMetadata.GroupInstanceId);
    }

    /// <summary>
    /// Java <c>ConsumerGroupMetadataTest.testGroupIdConstructor</c> (<c>:52</c>): the
    /// one-argument constructor keeps the group id and supplies Java's defaults, generation
    /// -1 (<c>UNKNOWN_GENERATION_ID</c>), member <c>""</c> (<c>UNKNOWN_MEMBER_ID</c>) and no
    /// instance id.
    /// </summary>
    [Fact]
    public void GroupIdConstructor_SuppliesTheUnknownDefaults()
    {
#pragma warning disable CS0618 // Type or member is obsolete
        ConsumerGroupMetadata groupMetadata = new(GroupId);
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal(GroupId, groupMetadata.GroupId);
        Assert.Equal(-1, groupMetadata.GenerationId);
        Assert.Equal(string.Empty, groupMetadata.MemberId);
        Assert.Null(groupMetadata.GroupInstanceId);
    }

    /// <summary>
    /// Java <c>ConsumerGroupMetadataTest.testInvalidGroupId</c> (<c>:62</c>): a null group id
    /// throws <see cref="ArgumentNullException"/> for <c>groupId</c> with Java's message,
    /// "group.id can't be null".
    /// </summary>
    [Fact]
    public void NullGroupId_Throws()
    {
#pragma warning disable CS0618 // Type or member is obsolete
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ConsumerGroupMetadata(null!, 2, "member", null));
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal("groupId", ex.ParamName);
        Assert.Equal(new ArgumentNullException("groupId", "group.id can't be null").Message, ex.Message);
    }

    /// <summary>
    /// Java <c>ConsumerGroupMetadataTest.testInvalidMemberId</c> (<c>:72</c>): a null member id
    /// throws <see cref="ArgumentNullException"/> for <c>memberId</c> with Java's message,
    /// "member.id can't be null".
    /// </summary>
    [Fact]
    public void NullMemberId_Throws()
    {
#pragma warning disable CS0618 // Type or member is obsolete
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ConsumerGroupMetadata(GroupId, 2, null!, null));
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal("memberId", ex.ParamName);
        Assert.Equal(new ArgumentNullException("memberId", "member.id can't be null").Message, ex.Message);
    }

    /// <summary>
    /// With both ids null, the group id is the one reported: Java checks it first
    /// (<c>ConsumerGroupMetadata.java:42-44</c>).
    /// </summary>
    [Fact]
    public void NullGroupIdAndMemberId_ReportsTheGroupId()
    {
#pragma warning disable CS0618 // Type or member is obsolete
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ConsumerGroupMetadata(null!, 2, null!, null));
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal("groupId", ex.ParamName);
        Assert.Equal(new ArgumentNullException("groupId", "group.id can't be null").Message, ex.Message);
    }

    /// <summary>
    /// Java <c>MockProducerTest.shouldThrowOnNullConsumerGroupMetadataWhenSendOffsetsToTransaction</c>
    /// (<c>:430</c>). Its <c>NullPointerException</c> comes from
    /// <c>new ConsumerGroupMetadata(null)</c>, which runs before
    /// <c>sendOffsetsToTransaction</c> does, so the translation is the one-argument
    /// constructor's check: <see cref="ArgumentNullException"/> for <c>groupId</c>, "group.id
    /// can't be null".
    /// </summary>
    [Fact]
    public void GroupIdConstructor_NullGroupId_Throws()
    {
#pragma warning disable CS0618 // Type or member is obsolete
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ConsumerGroupMetadata(null!));
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal("groupId", ex.ParamName);
        Assert.Equal(new ArgumentNullException("groupId", "group.id can't be null").Message, ex.Message);
    }

    /// <summary>
    /// Each of these generation ids (0, -1, -2, <see cref="int.MinValue"/> and
    /// <see cref="int.MaxValue"/>) is accepted and kept as given. Java's constructor assigns
    /// the generation id unchecked (<c>ConsumerGroupMetadata.java:43</c>); semantic checks are
    /// the core's (PLAN D2).
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-2)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void GenerationId_IsKeptAsGiven(int generationId)
    {
#pragma warning disable CS0618 // Type or member is obsolete
        ConsumerGroupMetadata groupMetadata = new(GroupId, generationId, "member", null);
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal(generationId, groupMetadata.GenerationId);
    }

    /// <summary>
    /// The empty string, and a non-ASCII string that mixes scripts and includes a character
    /// outside the Basic Multilingual Plane, are each kept verbatim in all three string
    /// fields. The empty row also shows an empty instance id staying empty rather than
    /// becoming <see langword="null"/>.
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData(NonAscii)]
    public void EmptyAndNonAsciiStrings_AreKeptVerbatim(string value)
    {
#pragma warning disable CS0618 // Type or member is obsolete
        ConsumerGroupMetadata groupMetadata = new(value, 3, value, value);
#pragma warning restore CS0618 // Type or member is obsolete

        Assert.Equal(value, groupMetadata.GroupId);
        Assert.Equal(value, groupMetadata.MemberId);
        Assert.Equal(value, groupMetadata.GroupInstanceId);
    }

    /// <summary>
    /// The public constructors are exactly the two Java has, and each carries
    /// <see cref="ObsoleteAttribute"/> with D2's message at warning level
    /// (<see cref="ObsoleteAttribute.IsError"/> false), the projection of Java's
    /// <c>@Deprecated(since = "4.2", forRemoval = true)</c>.
    /// </summary>
    [Fact]
    public void ThePublicConstructors_AreObsolete_WithDecisionD2sMessage_AtWarningLevel()
    {
        ConstructorInfo[] constructors =
            typeof(ConsumerGroupMetadata).GetConstructors(BindingFlags.Public | BindingFlags.Instance);

        Assert.Equal(
            new[]
            {
                "(String): Obsolete(\"" + ObsoleteMessage + "\", IsError=False)",
                "(String, Int32, String, String): Obsolete(\"" + ObsoleteMessage + "\", IsError=False)",
            },
            constructors.Select(Describe).OrderBy(d => d, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// The internal factory <c>FromCopiedValues</c> and every non-public instance constructor
    /// carry no <see cref="ObsoleteAttribute"/>. <c>ConsumerGroupMetadataMarshal</c> calls the
    /// factory, so it needs no <c>CS0618</c> suppression; that the marshal builds without one
    /// is shown by the build, which treats warnings as errors, not by this test.
    /// </summary>
    [Fact]
    public void TheInternalFactory_AndTheNonPublicConstructors_AreNotObsolete()
    {
        MethodInfo? factory = typeof(ConsumerGroupMetadata).GetMethod(
            "FromCopiedValues", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(factory);
        Assert.True(factory!.IsAssembly, "FromCopiedValues is internal");
        Assert.Null(factory.GetCustomAttribute<ObsoleteAttribute>());

        ConstructorInfo[] nonPublic =
            typeof(ConsumerGroupMetadata).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotEmpty(nonPublic);
        Assert.All(nonPublic, c => Assert.Null(c.GetCustomAttribute<ObsoleteAttribute>()));
    }

    /// <summary>
    /// The internal factory puts each of four distinct values in its own property. The
    /// consumer's <c>GroupMetadata()</c> reaches it through
    /// <c>ConsumerGroupMetadataMarshal</c>, and a swap between the group id and the member
    /// id, both non-nullable strings, would still compile.
    /// </summary>
    [Fact]
    public void TheInternalFactory_KeepsEachValueInItsOwnProperty()
    {
        ConsumerGroupMetadata groupMetadata =
            ConsumerGroupMetadata.FromCopiedValues("factory-group", 7, "factory-member", "factory-instance");

        Assert.Equal("factory-group", groupMetadata.GroupId);
        Assert.Equal(7, groupMetadata.GenerationId);
        Assert.Equal("factory-member", groupMetadata.MemberId);
        Assert.Equal("factory-instance", groupMetadata.GroupInstanceId);
    }

    private static string Describe(ConstructorInfo constructor)
    {
        string parameters = string.Join(", ", constructor.GetParameters().Select(p => p.ParameterType.Name));
        ObsoleteAttribute? obsolete = constructor.GetCustomAttribute<ObsoleteAttribute>();
        string attribute = obsolete is null
            ? "not obsolete"
            : "Obsolete(\"" + obsolete.Message + "\", IsError=" + (obsolete.IsError ? "True" : "False") + ")";
        return "(" + parameters + "): " + attribute;
    }
}
