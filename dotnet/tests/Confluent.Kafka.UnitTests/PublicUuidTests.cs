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

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the public <see cref="Uuid"/> against known vectors: its text form, Java's <c>Uuid.toString()</c> (URL-safe,
/// unpadded base64 of 16 big-endian bytes), Java's <c>fromString</c> rejections, the reserved constants, the signed
/// comparison, and <c>RandomUuid</c>.
/// </summary>
public sealed class PublicUuidTests
{
    [Theory]
    [InlineData(0L, 0L, "AAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData(1L, 2L, "AAAAAAAAAAEAAAAAAAAAAg")]
    [InlineData(-1L, -1L, "_____________________w")]
    [InlineData(0x0123456789ABCDEFL, 0x1122334455667788L, "ASNFZ4mrze8RIjNEVWZ3iA")]
    public void ToString_MatchesJavasBase64UrlForm(long mostSignificant, long leastSignificant, string expected)
    {
        Assert.Equal(expected, new Uuid(mostSignificant, leastSignificant).ToString());
    }

    [Theory]
    [InlineData(0L, 0L)]
    [InlineData(1L, 2L)]
    [InlineData(-1L, -1L)]
    [InlineData(long.MinValue, long.MaxValue)]
    [InlineData(0x0123456789ABCDEFL, 0x1122334455667788L)]
    public void Parse_InvertsToString(long mostSignificant, long leastSignificant)
    {
        Uuid original = new Uuid(mostSignificant, leastSignificant);

        Uuid parsed = Uuid.Parse(original.ToString());

        Assert.Equal(original, parsed);
        Assert.Equal(mostSignificant, parsed.MostSignificantBits);
        Assert.Equal(leastSignificant, parsed.LeastSignificantBits);
    }

    [Fact]
    public void ToString_UsesTheUrlSafeAlphabet_AndNoPadding()
    {
        string text = new Uuid(-1L, -1L).ToString();

        Assert.Equal(22, text.Length);
        Assert.DoesNotContain("+", text, StringComparison.Ordinal);
        Assert.DoesNotContain("/", text, StringComparison.Ordinal);
        Assert.DoesNotContain("=", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Zero_IsTheAllZeroIdentifier()
    {
        Assert.Equal(new Uuid(0L, 0L), Uuid.ZERO_UUID);
        Assert.Equal(0L, Uuid.ZERO_UUID.MostSignificantBits);
        Assert.Equal(0L, Uuid.ZERO_UUID.LeastSignificantBits);
    }

    [Fact]
    public void EqualityAndHashing_ComparesBothHalves()
    {
        Uuid a = new Uuid(1L, 2L);
        Uuid b = new Uuid(1L, 2L);
        Uuid c = new Uuid(1L, 3L);

        Assert.True(a == b);
        Assert.False(a == c);
        Assert.True(a != c);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.True(a.Equals((object)b));
        Assert.False(a.Equals("not a uuid"));
    }

    [Fact]
    public void Parse_RejectsWhatJavaRejects()
    {
        Assert.Equal("value", Assert.Throws<ArgumentNullException>(() => Uuid.Parse(null!)).ParamName);

        string twentyFive = new string('A', 25);
        IllegalArgumentException tooLong = Assert.Throws<IllegalArgumentException>(() => Uuid.Parse(twentyFive));
        Assert.Equal(
            "Input string with prefix `" + new string('A', 24) + "` is too long to be decoded as a base64 UUID",
            tooLong.Message);

        string twentyThree = new string('A', 23);
        IllegalArgumentException wrongCount = Assert.Throws<IllegalArgumentException>(() => Uuid.Parse(twentyThree));
        Assert.Equal(
            "Input string `" + twentyThree + "` decoded as 17 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
            wrongCount.Message);

        IllegalArgumentException wrongLength = Assert.Throws<IllegalArgumentException>(() => Uuid.Parse("AAAA"));
        Assert.Equal(
            "Input string `AAAA` decoded as 3 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
            wrongLength.Message);

        IllegalArgumentException notBase64 = Assert.Throws<IllegalArgumentException>(() => Uuid.Parse("!!!!"));
        Assert.Equal("Input string `!!!!` is not a valid base64 UUID", notBase64.Message);
    }

    [Fact]
    public void Parse_RejectsTheStandardBase64Alphabet()
    {
        string urlSafe = new Uuid(-1L, -1L).ToString();
        string standard = urlSafe.Replace('_', '/');
        Assert.NotEqual(urlSafe, standard);

        IllegalArgumentException slash = Assert.Throws<IllegalArgumentException>(() => Uuid.Parse(standard));
        Assert.Equal("Input string `" + standard + "` is not a valid base64 UUID", slash.Message);

        Assert.False(Uuid.TryParse(standard, out Uuid rejected));
        Assert.Equal(Uuid.ZERO_UUID, rejected);

        Assert.Throws<IllegalArgumentException>(() => Uuid.Parse("AAAAAAAAAAAAAAAAAAAA+A"));
        Assert.Equal("AAAAAAAAAAAAAAAAAAAA-A", Uuid.Parse("AAAAAAAAAAAAAAAAAAAA-A").ToString());

        Assert.Equal(new Uuid(-1L, -1L), Uuid.Parse(urlSafe));
    }

    [Fact]
    public void Parse_AcceptsThePaddedFormJavaAccepts()
    {
        Uuid expected = new Uuid(1L, 2L);

        Assert.Equal(expected, Uuid.Parse("AAAAAAAAAAEAAAAAAAAAAg=="));
        Assert.True(Uuid.TryParse("AAAAAAAAAAEAAAAAAAAAAg==", out Uuid parsed));
        Assert.Equal(expected, parsed);

        Assert.Equal("AAAAAAAAAAEAAAAAAAAAAg", parsed.ToString());
    }

    [Fact]
    public void Parse_RejectsPaddingJavaRejects()
    {
        string padded = new Uuid(1L, 2L) + "==";
        Assert.Equal(24, padded.Length);
        Assert.Equal(new Uuid(1L, 2L), Uuid.Parse(padded));

        string shortOfPadding = padded.Substring(0, padded.Length - 1);
        Assert.Equal(23, shortOfPadding.Length);

        IllegalArgumentException rejected =
            Assert.Throws<IllegalArgumentException>(() => Uuid.Parse(shortOfPadding));
        Assert.Equal("Input string `" + shortOfPadding + "` is not a valid base64 UUID", rejected.Message);

        Assert.False(Uuid.TryParse(shortOfPadding, out Uuid fromTryParse));
        Assert.Equal(Uuid.ZERO_UUID, fromTryParse);

        Assert.Equal(new Uuid(1L, 2L), Uuid.Parse(padded.Substring(0, 22)));
    }

    [Fact]
    public void TryParse_ReportsFailureWithoutThrowing()
    {
        Assert.False(Uuid.TryParse(null, out Uuid fromNull));
        Assert.Equal(Uuid.ZERO_UUID, fromNull);

        Assert.False(Uuid.TryParse("AAAAAAAAAAAAAAAAAAAAAAAAA", out _));
        Assert.False(Uuid.TryParse("AAAAAAAAAAAAAAAAAAAAAAA", out _));
        Assert.False(Uuid.TryParse("AAAA", out _));
        Assert.False(Uuid.TryParse("!!!!", out _));

        Assert.True(Uuid.TryParse("AAAAAAAAAAEAAAAAAAAAAg", out Uuid parsed));
        Assert.Equal(new Uuid(1L, 2L), parsed);
    }

    [Fact]
    public void ReservedConstants_MirrorJava()
    {
        Assert.Equal(new Uuid(0L, 1L), Uuid.ONE_UUID);
        Assert.Equal("AAAAAAAAAAAAAAAAAAAAAQ", Uuid.ONE_UUID.ToString());
        Assert.Equal(Uuid.ONE_UUID, Uuid.METADATA_TOPIC_ID);

        Assert.Equal(2, Uuid.RESERVED.Count);
        Assert.Contains(Uuid.ZERO_UUID, Uuid.RESERVED);
        Assert.Contains(Uuid.ONE_UUID, Uuid.RESERVED);
        Assert.Equal(
            new HashSet<Uuid> { Uuid.ZERO_UUID, Uuid.ONE_UUID },
            new HashSet<Uuid>(Uuid.RESERVED));

        ICollection<Uuid> asCollection = Assert.IsAssignableFrom<ICollection<Uuid>>(Uuid.RESERVED);
        Assert.True(asCollection.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => asCollection.Remove(Uuid.ZERO_UUID));
        Assert.Throws<NotSupportedException>(() => asCollection.Add(new Uuid(0L, 2L)));
    }

    [Fact]
    public void Shape_AddsJavasConstants_RandomUuid_AndComparable()
    {
        Assert.Equal(
            new HashSet<Type> { typeof(IEquatable<Uuid>), typeof(IComparable<Uuid>) },
            new HashSet<Type>(typeof(Uuid).GetInterfaces()));

        foreach ((string name, Type type) in new[]
        {
            (nameof(Uuid.ONE_UUID), typeof(Uuid)),
            (nameof(Uuid.METADATA_TOPIC_ID), typeof(Uuid)),
            (nameof(Uuid.RESERVED), typeof(IReadOnlyCollection<Uuid>)),
        })
        {
            PropertyInfo property = typeof(Uuid).GetProperty(name, BindingFlags.Public | BindingFlags.Static)!;
            Assert.NotNull(property);
            Assert.Equal(type, property.PropertyType);
            Assert.Null(property.SetMethod);
        }

        MethodInfo randomUuid = Assert.Single(
            typeof(Uuid).GetMethods(BindingFlags.Public | BindingFlags.Static),
            method => method.Name == nameof(Uuid.RandomUuid));
        Assert.Equal(typeof(Uuid), randomUuid.ReturnType);
        Assert.Empty(randomUuid.GetParameters());

        MethodInfo compareTo = typeof(Uuid).GetMethod(nameof(Uuid.CompareTo), new[] { typeof(Uuid) })!;
        Assert.Equal(typeof(int), compareTo.ReturnType);
    }

    [Fact]
    public void CompareTo_IsJavasSignedOrder_MostSignificantFirst()
    {
        Uuid[] ascending =
        {
            new Uuid(long.MinValue, 0L),
            new Uuid(-1L, 0L),
            new Uuid(0L, long.MinValue),
            new Uuid(0L, -1L),
            new Uuid(0L, 0L),
            new Uuid(0L, 1L),
            new Uuid(0L, long.MaxValue),
            new Uuid(1L, long.MinValue),
            new Uuid(1L, 0L),
            new Uuid(long.MaxValue, 0L),
        };

        for (int i = 0; i < ascending.Length; i++)
        {
            for (int j = 0; j < ascending.Length; j++)
            {
                int expected = i < j ? -1 : i > j ? 1 : 0;
                Assert.Equal(expected, ascending[i].CompareTo(ascending[j]));

                Assert.Equal(expected == 0, ascending[i].Equals(ascending[j]));
            }
        }

        Assert.Equal(-1, new Uuid(0L, long.MaxValue).CompareTo(new Uuid(1L, long.MinValue)));
        Assert.Equal(1, new Uuid(long.MaxValue, long.MinValue).CompareTo(new Uuid(long.MinValue, long.MaxValue)));

        List<Uuid> scrambled = new List<Uuid>
        {
            ascending[4], ascending[9], ascending[0], ascending[6], ascending[2],
            ascending[8], ascending[1], ascending[5], ascending[3], ascending[7],
        };
        scrambled.Sort();
        Assert.Equal(ascending, scrambled);
    }

    [Fact]
    public void RandomUuid_IsVersion4_NeverReserved_AndNeverContainsADash()
    {
        HashSet<Uuid> seen = new HashSet<Uuid>();
        for (int i = 0; i < 1000; i++)
        {
            Uuid uuid = Uuid.RandomUuid();

            Assert.DoesNotContain(uuid, Uuid.RESERVED);
            Assert.DoesNotContain("-", uuid.ToString(), StringComparison.Ordinal);
            Assert.Equal(4L, (uuid.MostSignificantBits >> 12) & 0xF);
            Assert.Equal(2UL, (ulong)uuid.LeastSignificantBits >> 62);
            Assert.True(seen.Add(uuid), "a random 122-bit draw repeated");
        }
    }

    [Fact]
    public void RandomUuid_Seam_SkipsReservedAndDashContainingCandidates()
    {
        Uuid dashLeading = new Uuid(unchecked((long)0xF800000000000000UL), 1L);
        Uuid dashInside = new Uuid(0x03E0000000000000L, 1L);
        Uuid valid = new Uuid(0x0123456789ABCDEFL, 0x1122334455667788L);
        Assert.Equal('-', dashLeading.ToString()[0]);
        Assert.Equal("A-AAAAAAAAAAAAAAAAAAAQ", dashInside.ToString());
        Assert.DoesNotContain("-", valid.ToString(), StringComparison.Ordinal);

        Queue<Uuid> candidates = new Queue<Uuid>(
            new[] { Uuid.ZERO_UUID, Uuid.ONE_UUID, dashLeading, dashInside, valid });

        Uuid chosen = Uuid.RandomUuid(candidates.Dequeue);

        Assert.Equal(valid, chosen);
        Assert.Empty(candidates);

        Uuid underscoreLeading = new Uuid(unchecked((long)0xFC00000000000000UL), 1L);
        Assert.Equal('_', underscoreLeading.ToString()[0]);
        Queue<Uuid> single = new Queue<Uuid>(new[] { underscoreLeading, valid });
        Assert.Equal(underscoreLeading, Uuid.RandomUuid(single.Dequeue));
        Assert.Single(single);
    }

    [Fact]
    public void FromVersion4Bytes_StampsJavasVersionAndVariantBits()
    {
        byte[] ones = Enumerable.Repeat((byte)0xFF, 16).ToArray();
        Uuid fromOnes = Uuid.FromVersion4Bytes(ones);
        Assert.Equal(unchecked((long)0xFFFFFFFFFFFF4FFFUL), fromOnes.MostSignificantBits);
        Assert.Equal(unchecked((long)0xBFFFFFFFFFFFFFFFUL), fromOnes.LeastSignificantBits);

        Uuid fromZeros = Uuid.FromVersion4Bytes(new byte[16]);
        Assert.Equal(0x0000000000004000L, fromZeros.MostSignificantBits);
        Assert.Equal(long.MinValue, fromZeros.LeastSignificantBits);
    }
}
