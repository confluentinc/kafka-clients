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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Internal;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests <see cref="JavaText"/>: each value prints as Java's <c>String.valueOf</c> prints it, and each exception as
/// Java's <c>Throwable.toString()</c>.
/// </summary>
public sealed class JavaTextTests
{
    public static TheoryData<double, string> Doubles => new TheoryData<double, string>
    {
        { 1.0, "1.0" },
        { 0.0, "0.0" },
        { -0.0, "-0.0" },
        { 100.0, "100.0" },
        { -1.5, "-1.5" },
        { 0.1, "0.1" },
        { 0.1 + 0.2, "0.30000000000000004" },
        { 1.0 / 3.0, "0.3333333333333333" },
        { 2.0 / 3.0, "0.6666666666666666" },
        { 123456.789, "123456.789" },
        { 1234567.0, "1234567.0" },
        { 9999999.0, "9999999.0" },
        { Below(1.0E7), "9999999.999999998" },
        { 1.0E7, "1.0E7" },
        { 1.0E10, "1.0E10" },
        { 1.0E23, "1.0E23" },
        { 9.223372036854776E18, "9.223372036854776E18" },
        { 0.001, "0.001" },
        { Below(0.001), "9.999999999999998E-4" },
        { 1.0E-4, "1.0E-4" },
        { 1.0E-5, "1.0E-5" },
        { -1.0E-5, "-1.0E-5" },
        { double.MaxValue, "1.7976931348623157E308" },
        { 2.2250738585072014E-308, "2.2250738585072014E-308" },
        { double.Epsilon, "4.9E-324" },
        { 2 * double.Epsilon, "9.9E-324" },
        { double.NaN, "NaN" },
        { double.PositiveInfinity, "Infinity" },
        { double.NegativeInfinity, "-Infinity" },
    };

    public static TheoryData<float, string> Floats => new TheoryData<float, string>
    {
        { 1.0f, "1.0" },
        { -0.0f, "-0.0" },
        { 0.1f, "0.1" },
        { 1.0f / 3.0f, "0.33333334" },
        { 16777216.0f, "1.6777216E7" },
        { 1.0E10f, "1.0E10" },
        { 1.0E-5f, "1.0E-5" },
        { float.MaxValue, "3.4028235E38" },
        { float.Epsilon, "1.4E-45" },
        { float.NaN, "NaN" },
        { float.NegativeInfinity, "-Infinity" },
    };

    [Fact]
    public void ValueOf_Null_IsNull()
    {
        Assert.Equal("null", JavaText.ValueOf(null));
    }

    [Fact]
    public void ValueOf_String_IsUnchanged()
    {
        Assert.Equal("café", JavaText.ValueOf("café"));
        Assert.Equal(string.Empty, JavaText.ValueOf(string.Empty));
        Assert.Equal("null", JavaText.ValueOf("null"));
    }

    [Fact]
    public void ValueOf_Boolean_IsLowerCase()
    {
        Assert.Equal("true", JavaText.ValueOf(true));
        Assert.Equal("false", JavaText.ValueOf(false));
    }

    [Fact]
    public void ValueOf_Integers_UnderANonInvariantCulture_AreInvariant()
    {
        WithGermanCulture(() =>
        {
            Assert.Equal("−1", (-1L).ToString(CultureInfo.CurrentCulture));
            Assert.Equal("-1234567890123", JavaText.ValueOf(-1234567890123L));
            Assert.Equal("9223372036854775807", JavaText.ValueOf(long.MaxValue));
            Assert.Equal("-2147483648", JavaText.ValueOf(int.MinValue));
            Assert.Equal("-32768", JavaText.ValueOf(short.MinValue));
            Assert.Equal("-128", JavaText.ValueOf(sbyte.MinValue));
            Assert.Equal("255", JavaText.ValueOf(byte.MaxValue));
            Assert.Equal("65535", JavaText.ValueOf(ushort.MaxValue));
            Assert.Equal("4294967295", JavaText.ValueOf(uint.MaxValue));
            Assert.Equal("18446744073709551615", JavaText.ValueOf(ulong.MaxValue));
        });
    }

    [Theory]
    [MemberData(nameof(Doubles))]
    public void ValueOf_Double_IsJavasDoubleToString(double value, string expected)
    {
        Assert.Equal(expected, JavaText.ValueOf(value));
        Assert.Equal(expected, JavaText.DoubleToString(value));
    }

    [Theory]
    [MemberData(nameof(Floats))]
    public void ValueOf_Float_IsJavasFloatToString(float value, string expected)
    {
        Assert.Equal(expected, JavaText.ValueOf(value));
        Assert.Equal(expected, JavaText.FloatToString(value));
    }

    [Fact]
    public void ValueOf_Numbers_UnderANonInvariantCulture_AreInvariant()
    {
        WithGermanCulture(() =>
        {
            Assert.Equal("1,5", 1.5.ToString(CultureInfo.CurrentCulture));
            Assert.Equal("1.5", JavaText.ValueOf(1.5));
            Assert.Equal("-1.5", JavaText.ValueOf(-1.5));
            Assert.Equal("-1.0E-5", JavaText.ValueOf(-1.0E-5));
            Assert.Equal("0.1", JavaText.ValueOf(0.1f));
            Assert.Equal("[1.5, -2.0]", JavaText.ValueOf(new[] { 1.5, -2.0 }));
            Assert.Equal("-1.5", JavaText.ValueOf(-1.5m));
        });
    }

    [Fact]
    public void ValueOf_Set_IsBracketedAndCommaSeparated()
    {
        Assert.Equal("[t]", JavaText.ValueOf(new HashSet<string> { "t" }));
        Assert.Equal("[1, 2, 3]", JavaText.ValueOf(new SortedSet<int> { 3, 1, 2 }));
        Assert.Equal("[]", JavaText.ValueOf(new HashSet<string>()));
    }

    [Fact]
    public void ValueOf_CollectionWithANullElement_PrintsNull()
    {
        Assert.Equal("[a, null, b]", JavaText.ValueOf(new List<string?> { "a", null, "b" }));
        Assert.Equal("[null]", JavaText.ValueOf(new object?[] { null }));
        Assert.Equal("[, a]", JavaText.ValueOf(new[] { string.Empty, "a" }));
    }

    [Fact]
    public void ValueOf_SelfContainingCollectionOrMap_PrintsJavasPlaceholder()
    {
        List<object> list = new List<object> { 1 };
        list.Add(list);
        Dictionary<string, object> map = new Dictionary<string, object>();
        map["self"] = map;

        Assert.Equal("[1, (this Collection)]", JavaText.ValueOf(list));
        Assert.Equal("{self=(this Map)}", JavaText.ValueOf(map));
    }

    [Fact]
    public void ValueOf_CollectionElements_AreFormattedTheJavaWay()
    {
        object?[] elements = { true, 1.0E10, new TopicPartition("t", 0), new List<int> { 1, 2 } };

        Assert.Equal("[true, 1.0E10, t-0, [1, 2]]", JavaText.ValueOf(elements));
    }

    [Fact]
    public void ValueOf_ReadOnlyCollectionOnly_IsACollection()
    {
        Assert.Equal("[1, 2]", JavaText.ValueOf(new ReadOnlyItems(1, 2)));
    }

    [Fact]
    public void ValueOf_Map_IsBracedWithKeyEqualsValue()
    {
        Dictionary<TopicPartition, long> offsets = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition("t", 0)] = 5,
        };
        SortedDictionary<string, object?> values = new SortedDictionary<string, object?>
        {
            ["a"] = null,
            ["b"] = true,
            ["c"] = 0.5,
            ["d"] = new SortedSet<string> { "x", "y" },
        };

        Assert.Equal("{t-0=5}", JavaText.ValueOf(offsets));
        Assert.Equal("{a=null, b=true, c=0.5, d=[x, y]}", JavaText.ValueOf(values));
        Assert.Equal("{}", JavaText.ValueOf(new Dictionary<string, int>()));
    }

    [Fact]
    public void ValueOf_NonGenericMap_IsBracedWithKeyEqualsValue()
    {
        Assert.Equal("{k=1}", JavaText.ValueOf(new Hashtable { ["k"] = 1 }));
    }

    [Fact]
    public void ValueOf_ReadOnlyDictionaryOnly_IsAMap()
    {
        Assert.Equal("{a=1, b=null}", JavaText.ValueOf(new ReadOnlyMap(("a", 1), ("b", null))));
        Assert.Equal("{}", JavaText.ValueOf(EmptyReadOnlyDictionary<string, int>.Instance));
    }

    [Fact]
    public void ValueOf_Exception_IsJavasThrowableToString()
    {
        Assert.Equal("System.InvalidOperationException: m", JavaText.ValueOf(new InvalidOperationException("m")));
        Assert.Equal("Confluent.Kafka.Common.KafkaException", JavaText.ValueOf(new KafkaException()));
    }

    [Fact]
    public void ValueOf_AnythingElse_IsItsToString()
    {
        Assert.Equal("t-3", JavaText.ValueOf(new TopicPartition("t", 3)));
        Assert.Equal("x", JavaText.ValueOf('x'));
    }

    [Fact]
    public void ToString_ExceptionWithAMessage_IsTheTypeFullNameAndTheMessage()
    {
        Assert.Equal("System.ArgumentException: bad", JavaText.ToString(new ArgumentException("bad")));
        Assert.Equal(
            "Confluent.Kafka.Common.KafkaException: broker said no",
            JavaText.ToString(new KafkaException("broker said no")));
        Assert.Equal(
            "Confluent.Kafka.Common.Errors.SerializationException: bad bytes",
            JavaText.ToString(new SerializationException("bad bytes")));
    }

    [Fact]
    public void ToString_ExceptionWithAnEmptyMessage_KeepsTheSeparator()
    {
        Assert.Equal("System.Exception: ", JavaText.ToString(new Exception(string.Empty)));
        Assert.Equal("Confluent.Kafka.Common.KafkaException: ", JavaText.ToString(new KafkaException(string.Empty)));
    }

    [Fact]
    public void ToString_ExceptionWithoutAJavaMessage_IsTheTypeFullName()
    {
        Assert.Equal("Confluent.Kafka.Common.KafkaException", JavaText.ToString(new KafkaException()));
        Assert.Equal(
            "Confluent.Kafka.Common.Errors.SerializationException",
            JavaText.ToString(new SerializationException((Exception?)null)));
    }

    private static double Below(double value) =>
        BitConverter.Int64BitsToDouble(BitConverter.DoubleToInt64Bits(value) - 1);

    private static void WithGermanCulture(Action action)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo german = (CultureInfo)CultureInfo.GetCultureInfo("de-DE").Clone();
        german.NumberFormat.NegativeSign = "−";
        CultureInfo.CurrentCulture = german;
        try
        {
            action();
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    /// <summary>A map that is only an <see cref="IReadOnlyDictionary{TKey, TValue}"/>.</summary>
    private sealed class ReadOnlyMap : IReadOnlyDictionary<string, int?>
    {
        private readonly SortedDictionary<string, int?> _entries = new SortedDictionary<string, int?>();

        internal ReadOnlyMap(params (string Key, int? Value)[] entries)
        {
            foreach ((string key, int? value) in entries)
            {
                _entries[key] = value;
            }
        }

        public int Count => _entries.Count;

        public IEnumerable<string> Keys => _entries.Keys;

        public IEnumerable<int?> Values => _entries.Values;

        public int? this[string key] => _entries[key];

        public bool ContainsKey(string key) => _entries.ContainsKey(key);

        public bool TryGetValue(string key, out int? value) => _entries.TryGetValue(key, out value);

        public IEnumerator<KeyValuePair<string, int?>> GetEnumerator() => _entries.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>A collection that is only an <see cref="IReadOnlyCollection{T}"/>.</summary>
    private sealed class ReadOnlyItems : IReadOnlyCollection<int>
    {
        private readonly int[] _items;

        internal ReadOnlyItems(params int[] items)
        {
            _items = items;
        }

        public int Count => _items.Length;

        public IEnumerator<int> GetEnumerator() => ((IEnumerable<int>)_items).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
