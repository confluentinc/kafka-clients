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

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <see cref="ClassicGroupState"/> and the two <see cref="GroupMarshal"/> members that
/// carry it across the ABI, exercised in isolation with no native call.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The contract under test is the <em>name</em>, not the ordinal.</b> Java's
/// <c>ClassicGroupState</c> has no numeric id, so the wire value is its <c>toString()</c>
/// spelling and these two members are the only place the spelling is written down. A test
/// that asserted <c>Enum.ToString()</c> round-trips would prove nothing about them — it
/// would re-derive the table from the same C# member names the table exists to decouple
/// from. Every expected spelling below is therefore a literal transcribed from
/// <c>ClassicGroupState.java:30-35</c>.
/// </para>
/// <para>
/// ⚠⚠ <b>The defect this file exists to catch is a collapsed absence.</b> A null pointer
/// (Java's <c>Optional.empty()</c> — no state reported) and an unrecognised name (a state
/// this client cannot name) are <b>different answers</b>: <see langword="null"/> and
/// <see cref="ClassicGroupState.Unknown"/> respectively. Both are asserted, because the
/// obvious wrong implementation — parsing through a null-tolerant path — returns
/// <c>Unknown</c> for both and would satisfy either assertion alone.
/// </para>
/// <para>
/// ⚠ <b>The second defect is a table copied from the wrong sibling.</b>
/// <see cref="ConsumerGroupState"/> and <see cref="GroupState"/> carry members this enum
/// does not, and <em>their</em> spellings are exactly what a copied table would accept. So
/// <c>"Assigning"</c>, <c>"Reconciling"</c> and <c>"NotReady"</c> are asserted to decode to
/// <c>Unknown</c>, and the member list is asserted to exclude all three.
/// </para>
/// <para>
/// ⚠ <b>Non-deprecation is asserted, not assumed.</b> Java marks
/// <see cref="ConsumerGroupState"/> <c>@Deprecated</c> and marks this enum not at all, so
/// an <see cref="ObsoleteAttribute"/> copied across with the rest of the template would be
/// a real fidelity defect — and a silent one, since it compiles.
/// </para>
/// <para>
/// ⚠ <b>Every name pointer is read inside its <c>using</c>.</b>
/// <see cref="Utf8Marshal.Pin(string)"/> keeps the buffer alive only for the scope; a read
/// after it is a use-after-unpin (ffi §A4), the same discipline production follows on the
/// send path.
/// </para>
/// </remarks>
public sealed class GroupMarshalClassicStateTests
{
    /// <summary>
    /// Java's six constants and their <c>toString()</c> spellings, transcribed from
    /// <c>ClassicGroupState.java:30-35</c> in declaration order.
    /// </summary>
    private static readonly (ClassicGroupState State, string Name)[] s_javaConstants =
    {
        (ClassicGroupState.Unknown, "Unknown"),
        (ClassicGroupState.PreparingRebalance, "PreparingRebalance"),
        (ClassicGroupState.CompletingRebalance, "CompletingRebalance"),
        (ClassicGroupState.Stable, "Stable"),
        (ClassicGroupState.Dead, "Dead"),
        (ClassicGroupState.Empty, "Empty"),
    };

    /// <summary>
    /// The enum declares exactly Java's six constants, in Java's order — no member added,
    /// none dropped, and in particular none of <c>Assigning</c> / <c>Reconciling</c> /
    /// <c>NotReady</c>, which belong to the other two group-state axes
    /// (<c>GroupState.java:50-57</c>).
    /// </summary>
    [Fact]
    public void TheEnum_DeclaresExactlyJavasSixConstantsInOrder()
    {
        ClassicGroupState[] declared =
            (ClassicGroupState[])Enum.GetValues(typeof(ClassicGroupState));
        string[] names = Enum.GetNames(typeof(ClassicGroupState));

        Assert.Equal(s_javaConstants.Select(c => c.State).ToArray(), declared);
        Assert.DoesNotContain("Assigning", names);
        Assert.DoesNotContain("Reconciling", names);
        Assert.DoesNotContain("NotReady", names);
    }

    /// <summary>
    /// The enum is <b>not</b> obsolete: Java carries no <c>@Deprecated</c> on
    /// <c>ClassicGroupState</c> and none on <c>ClassicGroupDescription.state()</c>, unlike
    /// <see cref="ConsumerGroupState"/>, whose Java <c>@Deprecated(since = "4.0")</c> the
    /// binding mirrors. Marking this one would overstate the Java contract.
    /// </summary>
    [Fact]
    public void TheEnum_IsNotMarkedObsolete()
    {
        Assert.Empty(typeof(ClassicGroupState)
            .GetCustomAttributes(typeof(ObsoleteAttribute), inherit: false));
    }

    /// <summary>
    /// Every Java constant survives the full boundary crossing it will actually make:
    /// encode to a name, cross as UTF-8 bytes, decode back. Asserts the intermediate name
    /// against Java's literal too, so a table that is self-consistently wrong in both
    /// directions cannot pass.
    /// </summary>
    [Fact]
    public void EveryJavaConstant_RoundTripsThroughNameAndBack()
    {
        foreach ((ClassicGroupState state, string expectedName) in s_javaConstants)
        {
            Assert.Equal(expectedName, GroupMarshal.NameFromClassicState(state));

            using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(expectedName))
            {
                Assert.Equal(state, GroupMarshal.ClassicStateFromName(pinned.Pointer));
            }
        }
    }

    /// <summary>
    /// Decoding is case-insensitive, as Java's upper-casing lookup map is
    /// (<c>ClassicGroupState.java:37-38</c>) and as the header promises.
    /// </summary>
    /// <param name="name">One casing of <c>"CompletingRebalance"</c>.</param>
    [Theory]
    [InlineData("CompletingRebalance")]
    [InlineData("completingrebalance")]
    [InlineData("COMPLETINGREBALANCE")]
    [InlineData("CoMpLeTiNgReBaLaNcE")]
    public void AName_DecodesRegardlessOfCasing(string name)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(name);

        Assert.Equal(
            ClassicGroupState.CompletingRebalance,
            GroupMarshal.ClassicStateFromName(pinned.Pointer));
    }

    /// <summary>
    /// A name this client cannot place decodes to <see cref="ClassicGroupState.Unknown"/>,
    /// never to a failure — Java's <c>parse</c> (<c>:49-52</c>), and the "too new for us to
    /// parse" case its javadoc names. <c>"Assigning"</c>, <c>"Reconciling"</c> and
    /// <c>"NotReady"</c> are in the set deliberately: each is a real spelling on one of the
    /// other two axes, and this enum has no member for any of them.
    /// </summary>
    /// <param name="name">A name outside Java's six constants.</param>
    [Theory]
    [InlineData("Assigning")]
    [InlineData("Reconciling")]
    [InlineData("NotReady")]
    [InlineData("SomeStateFromANewerBroker")]
    [InlineData("")]
    [InlineData("Stable ")]
    public void AnUnrecognisedName_DecodesToUnknown(string name)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(name);

        Assert.Equal(
            ClassicGroupState.Unknown,
            GroupMarshal.ClassicStateFromName(pinned.Pointer));
    }

    /// <summary>
    /// A null pointer is <b>absence</b> — Java's <c>Optional.empty()</c> — and is decoded as
    /// <see langword="null"/>, distinct from the <see cref="ClassicGroupState.Unknown"/>
    /// that an unrecognised name yields.
    /// </summary>
    [Fact]
    public void ANullName_DecodesToAbsenceNotUnknown()
    {
        ClassicGroupState? decoded = GroupMarshal.ClassicStateFromName(IntPtr.Zero);

        Assert.Null(decoded);
        Assert.NotEqual(ClassicGroupState.Unknown, decoded);
    }

    /// <summary>
    /// The encode direction is partial: a value no member defines — reachable in C# by a
    /// cast, and not in Java — yields <see langword="null"/> rather than a fabricated name,
    /// leaving the throw to the caller that knows the parameter to blame
    /// (ffi-marshalling.md §B5).
    /// </summary>
    [Fact]
    public void AnUndefinedValue_EncodesToNull()
    {
        IReadOnlyCollection<ClassicGroupState> defined = s_javaConstants.Select(c => c.State).ToArray();

        Assert.DoesNotContain((ClassicGroupState)int.MaxValue, defined);
        Assert.Null(GroupMarshal.NameFromClassicState((ClassicGroupState)int.MaxValue));
        Assert.Null(GroupMarshal.NameFromClassicState((ClassicGroupState)(-1)));
    }
}
