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

#pragma warning disable CS0618 // Java deprecates the state enum itself; mirrored, not avoided.

/// <summary>
/// <see cref="ConsumerGroupState"/> and the two <see cref="GroupMarshal"/> members that
/// carry it across the ABI, exercised in isolation with no native call.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The contract under test is the <em>name</em>, not the ordinal.</b> Java's
/// <c>ConsumerGroupState</c> has no numeric id, so the wire value is its <c>toString()</c>
/// spelling and these two members are the only place the spelling is written down. A test
/// that asserted <c>Enum.ToString()</c> round-trips would prove nothing about them — it
/// would re-derive the table from the same C# member names the table exists to decouple
/// from. Every expected spelling below is therefore a literal transcribed from
/// <c>ConsumerGroupState.java:32-39</c>.
/// </para>
/// <para>
/// ⚠⚠ <b>The defect this file exists to catch is a collapsed absence.</b> A null pointer
/// (Java's <c>Optional.empty()</c> — the broker reported no state) and an unrecognised name
/// (the broker reported a state this client cannot name) are <b>different answers</b>:
/// <see langword="null"/> and <see cref="ConsumerGroupState.Unknown"/> respectively. Both
/// are asserted, because the obvious wrong implementation — parsing through a null-tolerant
/// path — returns <c>Unknown</c> for both and would satisfy either assertion alone.
/// </para>
/// <para>
/// ⚠ <b>Every name pointer is read inside its <c>using</c>.</b>
/// <see cref="Utf8Marshal.Pin(string)"/> keeps the buffer alive only for the scope; a read
/// after it is a use-after-unpin (ffi §A4), the same discipline production follows on the
/// send path.
/// </para>
/// </remarks>
public sealed class GroupMarshalConsumerStateTests
{
    /// <summary>
    /// Java's eight constants and their <c>toString()</c> spellings, transcribed from
    /// <c>ConsumerGroupState.java:32-39</c> in declaration order.
    /// </summary>
    private static readonly (ConsumerGroupState State, string Name)[] s_javaConstants =
    {
        (ConsumerGroupState.Unknown, "Unknown"),
        (ConsumerGroupState.PreparingRebalance, "PreparingRebalance"),
        (ConsumerGroupState.CompletingRebalance, "CompletingRebalance"),
        (ConsumerGroupState.Stable, "Stable"),
        (ConsumerGroupState.Dead, "Dead"),
        (ConsumerGroupState.Empty, "Empty"),
        (ConsumerGroupState.Assigning, "Assigning"),
        (ConsumerGroupState.Reconciling, "Reconciling"),
    };

    /// <summary>
    /// The enum declares exactly Java's eight constants, in Java's order — no member added,
    /// none dropped, and in particular no <c>NotReady</c>, which only
    /// <see cref="GroupState"/> has (<c>GroupState.java:57</c>).
    /// </summary>
    [Fact]
    public void TheEnum_DeclaresExactlyJavasEightConstantsInOrder()
    {
        ConsumerGroupState[] declared =
            (ConsumerGroupState[])Enum.GetValues(typeof(ConsumerGroupState));

        Assert.Equal(s_javaConstants.Select(c => c.State).ToArray(), declared);
        Assert.DoesNotContain("NotReady", Enum.GetNames(typeof(ConsumerGroupState)));
    }

    /// <summary>
    /// Java's <c>@Deprecated(since = "4.0", forRemoval = true)</c> (<c>:30</c>) is mirrored
    /// as a warning-severity <see cref="ObsoleteAttribute"/> — not an error, which would be
    /// stricter than Java and would make the deprecated admin surface that names this type
    /// uncompilable.
    /// </summary>
    [Fact]
    public void TheEnum_IsMarkedObsoleteAsAWarning()
    {
        ObsoleteAttribute? obsolete = typeof(ConsumerGroupState)
            .GetCustomAttributes(typeof(ObsoleteAttribute), inherit: false)
            .Cast<ObsoleteAttribute>()
            .SingleOrDefault();

        Assert.NotNull(obsolete);
        Assert.False(obsolete!.IsError);
        Assert.Contains("4.0", obsolete.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(GroupState), obsolete.Message, StringComparison.Ordinal);
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
        foreach ((ConsumerGroupState state, string expectedName) in s_javaConstants)
        {
            Assert.Equal(expectedName, GroupMarshal.NameFromConsumerState(state));

            using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(expectedName))
            {
                Assert.Equal(state, GroupMarshal.ConsumerStateFromName(pinned.Pointer));
            }
        }
    }

    /// <summary>
    /// Decoding is case-insensitive, as Java's upper-casing lookup map is
    /// (<c>ConsumerGroupState.java:41-42</c>) and as the header promises.
    /// </summary>
    [Theory]
    [InlineData("PreparingRebalance")]
    [InlineData("preparingrebalance")]
    [InlineData("PREPARINGREBALANCE")]
    [InlineData("PrEpArInGrEbAlAnCe")]
    public void AName_DecodesRegardlessOfCasing(string name)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(name);

        Assert.Equal(
            ConsumerGroupState.PreparingRebalance,
            GroupMarshal.ConsumerStateFromName(pinned.Pointer));
    }

    /// <summary>
    /// A name this client cannot place decodes to <see cref="ConsumerGroupState.Unknown"/>,
    /// never to a failure — Java's <c>parse</c> (<c>:53-56</c>). <c>"NotReady"</c> is in the
    /// set deliberately: it is a real <see cref="GroupState"/> spelling a broker could send
    /// on the wrong axis, and this enum has no member for it.
    /// </summary>
    /// <param name="name">A name outside Java's eight constants.</param>
    [Theory]
    [InlineData("NotReady")]
    [InlineData("SomeStateFromANewerBroker")]
    [InlineData("")]
    [InlineData("Stable ")]
    public void AnUnrecognisedName_DecodesToUnknown(string name)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(name);

        Assert.Equal(
            ConsumerGroupState.Unknown,
            GroupMarshal.ConsumerStateFromName(pinned.Pointer));
    }

    /// <summary>
    /// A null pointer is <b>absence</b> — Java's <c>Optional.empty()</c> — and is decoded as
    /// <see langword="null"/>, distinct from the <see cref="ConsumerGroupState.Unknown"/>
    /// that an unrecognised name yields.
    /// </summary>
    [Fact]
    public void ANullName_DecodesToAbsenceNotUnknown()
    {
        ConsumerGroupState? decoded = GroupMarshal.ConsumerStateFromName(IntPtr.Zero);

        Assert.Null(decoded);
        Assert.NotEqual(ConsumerGroupState.Unknown, decoded);
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
        IReadOnlyCollection<ConsumerGroupState> defined = s_javaConstants.Select(c => c.State).ToArray();

        Assert.DoesNotContain((ConsumerGroupState)int.MaxValue, defined);
        Assert.Null(GroupMarshal.NameFromConsumerState((ConsumerGroupState)int.MaxValue));
        Assert.Null(GroupMarshal.NameFromConsumerState((ConsumerGroupState)(-1)));
    }
}

#pragma warning restore CS0618
