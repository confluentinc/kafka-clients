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
using System.Numerics;
using System.Runtime.InteropServices;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The text <see cref="JavaText.CoreText"/> sends, read back through a real producer configuration with the core's
/// typed getters: the core rounds an integer or decimal text to the nearest double, every double, float and half
/// reads back bit for bit, and Java's <c>convertToString</c> vectors read back as Java's parsed values. The CLASS
/// vectors and the untyped one have no read-back: the core defines no CLASS key and never parses an unknown key.
/// </summary>
public sealed class ConfigCoreTextReadBackTests
{
    private const string DoubleKey = "sasl.kerberos.ticket.renew.window.factor";
    private const string SecondDoubleKey = "sasl.kerberos.ticket.renew.jitter";

    [Theory]
    [InlineData("2^53+3")]
    [InlineData("-(2^53+1)")]
    [InlineData("2^64+2049")]
    [InlineData("10^400")]
    [InlineData("decimal")]
    [InlineData("ulong 2^63+1025")]
#if NET7_0_OR_GREATER
    [InlineData("Int128")]
#endif
    public void TheCoreRounds_AnExactNumberText_ToTheNearestDouble(string vector)
    {
        (object value, double expected) = RoundingVector(vector);
        string text = JavaText.CoreText(value, ConfigTypes.Producer[DoubleKey]);

        using CoreConfig config = CoreConfig.Create((DoubleKey, text));

        AssertSameDouble(expected, config.GetDouble(DoubleKey), text);
    }

    [Fact]
    public void TheDoubleCoreText_ReadsBackBitForBit()
    {
        var values = new List<double>
        {
            double.MaxValue, double.MinValue, double.Epsilon, -double.Epsilon, 0.0, -0.0, double.NaN,
            double.PositiveInfinity, double.NegativeInfinity, 0.1, 1e23, 1e21, 1e-7, 6.6939e20, 2.5e-323,
            BitConverter.Int64BitsToDouble(0x000FFFFFFFFFFFFF), BitConverter.Int64BitsToDouble(0x0010000000000000),
        };
        for (long bits = 1; bits <= 64; bits++)
        {
            values.Add(BitConverter.Int64BitsToDouble(bits));
        }

        var random = new Random(20261105);
        byte[] bytes = new byte[8];
        for (int i = 0; i < 200; i++)
        {
            random.NextBytes(bytes);
            values.Add(BitConverter.Int64BitsToDouble(BitConverter.ToInt64(bytes, 0) & unchecked((long)0x800FFFFFFFFFFFFFUL)));
        }

        for (int i = 0; i < 3000; i++)
        {
            random.NextBytes(bytes);
            values.Add(BitConverter.ToDouble(bytes, 0));
        }

        var vectors = new List<(string Text, double Expected)>(values.Count);
        foreach (double value in values)
        {
            vectors.Add((JavaText.CoreText(value, ConfigType.Double), value));
        }

        AssertReadBackBitForBit(vectors);
    }

    [Fact]
    public void TheFloatCoreText_ReadsBackAsItsWidenedDoubleBitForBit()
    {
        var floats = new List<float>
        {
            float.MaxValue, float.MinValue, float.Epsilon, -float.Epsilon, 0f, -0f, float.NaN, float.PositiveInfinity,
            float.NegativeInfinity, 0.1f, 1e7f, 1e-5f, 1.17549435E-38f,
        };
        for (int k = 2; k <= 64; k++)
        {
            floats.Add(k * float.Epsilon);
        }

        var random = new Random(20261105);
        byte[] bytes = new byte[4];
        for (int i = 0; i < 2000; i++)
        {
            random.NextBytes(bytes);
            floats.Add(BitConverter.ToSingle(bytes, 0));
        }

        var vectors = new List<(string Text, double Expected)>(floats.Count);
        foreach (float value in floats)
        {
            vectors.Add((JavaText.CoreText(value, ConfigType.Double), value));
        }

        AssertReadBackBitForBit(vectors);
    }

#if NET5_0_OR_GREATER
    [Fact]
    public void TheHalfCoreText_ReadsBackAsItsWidenedDoubleBitForBit()
    {
        var halves = new List<Half> { Half.MaxValue, Half.MinValue, Half.Epsilon, Half.NegativeZero, Half.NaN };
        for (int bits = 0; bits < 0x10000; bits += 61)
        {
            halves.Add(BitConverter.Int16BitsToHalf((short)bits));
        }

        var vectors = new List<(string Text, double Expected)>(halves.Count);
        foreach (Half value in halves)
        {
            vectors.Add((JavaText.CoreText(value, ConfigType.Double), (double)value));
        }

        AssertReadBackBitForBit(vectors);
    }
#endif

    [Fact]
    public void TestConvertValueToStringBoolean_TheCoreReadsJavasValue()
    {
        const string key = "partitioner.ignore.keys";
        Assert.Equal(ConfigType.Boolean, ConfigTypes.Producer[key]);

        using CoreConfig config = CoreConfig.Create((key, JavaText.CoreText(true, ConfigTypes.Producer[key])));

        Assert.True(config.GetBoolean(key));
    }

    // Java's vector is Short.MAX_VALUE; both SHORT keys the core defines have a range below it, so the largest value
    // in range is sent instead.
    [Fact]
    public void TestConvertValueToStringShort_TheCoreReadsJavasValue()
    {
        const string key = "sasl.login.refresh.buffer.seconds";
        Assert.Equal(ConfigType.Short, ConfigTypes.Producer[key]);

        using CoreConfig config = CoreConfig.Create((key, JavaText.CoreText((short)3600, ConfigTypes.Producer[key])));

        Assert.Equal((short)3600, config.GetShort(key));
    }

    [Fact]
    public void TestConvertValueToStringInt_TheCoreReadsJavasValue()
    {
        const string key = "receive.buffer.bytes";
        Assert.Equal(ConfigType.Int, ConfigTypes.Producer[key]);

        using CoreConfig config = CoreConfig.Create((key, JavaText.CoreText(int.MaxValue, ConfigTypes.Producer[key])));

        Assert.Equal(int.MaxValue, config.GetInt(key));
    }

    [Fact]
    public void TestConvertValueToStringLong_TheCoreReadsJavasValue()
    {
        const string key = "connections.max.idle.ms";
        Assert.Equal(ConfigType.Long, ConfigTypes.Producer[key]);

        using CoreConfig config = CoreConfig.Create((key, JavaText.CoreText(long.MaxValue, ConfigTypes.Producer[key])));

        Assert.Equal(long.MaxValue, config.GetLong(key));
    }

    [Fact]
    public void TestConvertValueToStringDouble_TheCoreReadsJavasValue()
    {
        Assert.Equal(ConfigType.Double, ConfigTypes.Producer[DoubleKey]);
        foreach (double value in new[] { 3.125d, double.MaxValue, 102400000d, -102400000d })
        {
            string text = JavaText.CoreText(value, ConfigTypes.Producer[DoubleKey]);

            using CoreConfig config = CoreConfig.Create((DoubleKey, text));

            AssertSameDouble(value, config.GetDouble(DoubleKey), text);
        }
    }

    [Fact]
    public void TestConvertValueToStringString_TheCoreReadsJavasValue()
    {
        const string key = "sasl.kerberos.service.name";
        Assert.Equal(ConfigType.String, ConfigTypes.Producer[key]);

        using CoreConfig config = CoreConfig.Create((key, JavaText.CoreText("foobar", ConfigTypes.Producer[key])));

        Assert.Equal("foobar", config.GetString(key));
    }

    [Fact]
    public void TestConvertValueToStringList_TheCoreReadsJavasValue()
    {
        const string key = "ssl.cipher.suites";
        Assert.Equal(ConfigType.List, ConfigTypes.Producer[key]);
        var value = new List<string?> { "a", "bc", "d" };

        using CoreConfig config = CoreConfig.Create((key, JavaText.CoreText(value, ConfigTypes.Producer[key])));

        Assert.Equal(value, config.GetList(key));
    }

    private static (object Value, double Expected) RoundingVector(string vector) => vector switch
    {
        "2^53+3" => (BigInteger.Pow(2, 53) + 3, 9007199254740996.0),
        "-(2^53+1)" => (-(BigInteger.Pow(2, 53) + 1), -9007199254740992.0),
        "2^64+2049" => (BigInteger.Pow(2, 64) + 2049, 18446744073709555712.0),
        "10^400" => (BigInteger.Pow(10, 400), double.PositiveInfinity),
        "decimal" => (2090639523243.3333527855107988m, BitConverter.Int64BitsToDouble(0x427E6C3D37DAB555)),
        "ulong 2^63+1025" => (9223372036854776833UL, 9223372036854777856.0),
#if NET7_0_OR_GREATER
        "Int128" => (
            Int128.Parse("31640652437294794598055815133", CultureInfo.InvariantCulture),
            BitConverter.Int64BitsToDouble(0x45D98F2231646568)),
#endif
        _ => throw new ArgumentOutOfRangeException(nameof(vector), vector, null),
    };

    private static void AssertReadBackBitForBit(IReadOnlyList<(string Text, double Expected)> vectors)
    {
        for (int i = 0; i < vectors.Count; i += 2)
        {
            (string firstText, double firstExpected) = vectors[i];
            (string secondText, double secondExpected) = vectors[Math.Min(i + 1, vectors.Count - 1)];

            using CoreConfig config = CoreConfig.Create((DoubleKey, firstText), (SecondDoubleKey, secondText));

            AssertSameDouble(firstExpected, config.GetDouble(DoubleKey), firstText);
            AssertSameDouble(secondExpected, config.GetDouble(SecondDoubleKey), secondText);
        }
    }

    private static void AssertSameDouble(double expected, double actual, string text)
    {
        bool same = double.IsNaN(expected)
            ? double.IsNaN(actual)
            : BitConverter.DoubleToInt64Bits(expected) == BitConverter.DoubleToInt64Bits(actual);
        Assert.True(
            same,
            string.Format(
                CultureInfo.InvariantCulture,
                "'{0}' read back as {1:R} (0x{2:X16}), expected {3:R} (0x{4:X16})",
                text,
                actual,
                BitConverter.DoubleToInt64Bits(actual),
                expected,
                BitConverter.DoubleToInt64Bits(expected)));
    }

    /// <summary>A producer configuration built from key/text pairs, read back with the core's typed getters.</summary>
    private sealed class CoreConfig : IDisposable
    {
        private IntPtr _config;

        private CoreConfig(IntPtr config) => _config = config;

        internal static CoreConfig Create(params (string Key, string Text)[] entries)
        {
            var pins = new List<Utf8Marshal.PinnedUtf8String>(entries.Length * 2 + 2);
            IntPtr map = IntPtr.Zero;
            IntPtr outErr = IntPtr.Zero;
            try
            {
                map = NativeMethods.MapNew(ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                Put(map, pins, "bootstrap.servers", "localhost:9092");
                foreach ((string key, string text) in entries)
                {
                    Put(map, pins, key, text);
                }

                IntPtr config = NativeMethods.ProducerConfigNew(map, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                return new CoreConfig(config);
            }
            finally
            {
                if (map != IntPtr.Zero)
                {
                    IntPtr destroyErr = IntPtr.Zero;
                    NativeMethods.MapDestroy(map, ref destroyErr);
                    KafkaException.ThrowIfFfiFailed(ref destroyErr);
                }

                foreach (Utf8Marshal.PinnedUtf8String pin in pins)
                {
                    pin.Dispose();
                }
            }
        }

        internal bool GetBoolean(string key)
        {
            using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(key);
            IntPtr outErr = IntPtr.Zero;
            sbyte present = Native.GetBoolean(_config, name.Pointer, out sbyte value, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Assert.Equal(1, present);
            return value != 0;
        }

        internal short GetShort(string key)
        {
            using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(key);
            IntPtr outErr = IntPtr.Zero;
            sbyte present = Native.GetShort(_config, name.Pointer, out short value, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Assert.Equal(1, present);
            return value;
        }

        internal int GetInt(string key)
        {
            using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(key);
            IntPtr outErr = IntPtr.Zero;
            sbyte present = Native.GetInt(_config, name.Pointer, out int value, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Assert.Equal(1, present);
            return value;
        }

        internal long GetLong(string key)
        {
            using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(key);
            IntPtr outErr = IntPtr.Zero;
            sbyte present = Native.GetLong(_config, name.Pointer, out long value, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Assert.Equal(1, present);
            return value;
        }

        internal double GetDouble(string key)
        {
            using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(key);
            IntPtr outErr = IntPtr.Zero;
            sbyte present = Native.GetDouble(_config, name.Pointer, out double value, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Assert.Equal(1, present);
            return value;
        }

        internal string? GetString(string key)
        {
            using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(key);
            IntPtr outErr = IntPtr.Zero;
            IntPtr value = Native.GetString(_config, name.Pointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return Utf8Marshal.PtrToString(value);
        }

        internal List<string?> GetList(string key)
        {
            using Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(key);
            IntPtr outErr = IntPtr.Zero;
            IntPtr list = Native.GetList(_config, name.Pointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Assert.NotEqual(IntPtr.Zero, list);
            try
            {
                int size = NativeMethods.ListSize(list, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                var values = new List<string?>(size);
                for (int i = 0; i < size; i++)
                {
                    IntPtr element = NativeMethods.ListGet(list, i, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    values.Add(Utf8Marshal.PtrToString(element));
                }

                return values;
            }
            finally
            {
                IntPtr destroyErr = IntPtr.Zero;
                NativeMethods.ListDestroy(list, ref destroyErr);
                KafkaException.ThrowIfFfiFailed(ref destroyErr);
            }
        }

        public void Dispose()
        {
            if (_config == IntPtr.Zero)
            {
                return;
            }

            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ProducerConfigDestroy(_config, ref outErr);
            _config = IntPtr.Zero;
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        private static void Put(IntPtr map, List<Utf8Marshal.PinnedUtf8String> pins, string key, string text)
        {
            Utf8Marshal.PinnedUtf8String pinnedKey = Utf8Marshal.Pin(key);
            pins.Add(pinnedKey);
            Utf8Marshal.PinnedUtf8String pinnedText = Utf8Marshal.Pin(text);
            pins.Add(pinnedText);
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MapPut(map, pinnedKey.Pointer, pinnedText.Pointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    /// <summary>The producer configuration's typed getters, which the binding never calls.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerConfig_get_boolean", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte GetBoolean(IntPtr self, IntPtr key, out sbyte value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerConfig_get_short", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte GetShort(IntPtr self, IntPtr key, out short value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerConfig_get_int", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte GetInt(IntPtr self, IntPtr key, out int value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerConfig_get_long", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte GetLong(IntPtr self, IntPtr key, out long value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerConfig_get_double", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte GetDouble(IntPtr self, IntPtr key, out double value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerConfig_get_string", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr GetString(IntPtr self, IntPtr key, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_producer_ProducerConfig_get_list", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr GetList(IntPtr self, IntPtr key, ref IntPtr outErr);
    }
}
