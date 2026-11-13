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
#if NET
using System.Collections.Immutable;
#endif
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Numerics;

using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Internal;

/// <summary>
/// Tests <see cref="ClientConfigs.Prepare"/>: every key before any value, the client's own serializer keys skipped,
/// the null rule, Java's <c>ConfigDef.parseType</c> type check with Java's messages, and the text sent to the core.
/// Translates the binding half of Java's <c>ConfigDefTest</c> and <c>UtilsTest</c> cases and Python's helper-level
/// config tests.
/// </summary>
public sealed class ClientConfigsTests
{
    private const string NotAnInt = "Expected value to be a 32-bit integer, but it was a ";
    private const string NotAShort = "Expected value to be a 16-bit integer (short), but it was a ";
    private const string NotALong = "Expected value to be a 64-bit integer (long), but it was a ";
    private const string NotADouble = "Expected value to be a double, but it was a ";
    private const string NotAString = "Expected value to be a string, but it was a ";
    private const string NotABoolean = "Expected value to be either true or false";
    private const string NotAList = "Expected a comma separated list.";
    private const string NotAClass = "Expected a Class instance or class name.";
    private const string KeyMustBeAString = "Key must be a string.";
    private const string Hidden = "[hidden]";

    private static readonly Dictionary<string, string> s_producerKeyOfType = new(StringComparer.Ordinal)
    {
        ["INT"] = "batch.size",
        ["LONG"] = "linger.ms",
        ["SHORT"] = "sasl.login.refresh.min.period.seconds",
        ["DOUBLE"] = "sasl.login.refresh.window.factor",
        ["STRING"] = "client.id",
        ["PASSWORD"] = "ssl.key.password",
        ["BOOLEAN"] = "enable.idempotence",
        ["LIST"] = "bootstrap.servers",
        ["CLASS"] = "partitioner.class",
    };

    private static readonly Dictionary<string, object> s_values = new(StringComparer.Ordinal)
    {
        ["42.5"] = 42.5,
        ["1.5"] = 1.5,
        ["long.MaxValue"] = long.MaxValue,
        ["object"] = new object(),
        ["true"] = true,
        ["false"] = false,
        ["1"] = 1,
        ["5"] = 5,
        ["53"] = 53,
        ["int60"] = 60,
        ["long16384"] = 16384L,
        ["sbyte"] = (sbyte)-8,
        ["byte"] = (byte)255,
        ["byte7"] = (byte)7,
        ["short"] = (short)-300,
        ["short7"] = (short)7,
        ["ushort"] = (ushort)65535,
        ["int"] = -7,
        ["uint"] = 7u,
        ["uint4000000000"] = 4000000000u,
        ["long"] = long.MinValue,
        ["ulong"] = 7UL,
        ["ulong.MaxValue"] = ulong.MaxValue,
        ["float"] = 0.1f,
        ["float1.5"] = 1.5f,
        ["double"] = 0.1,
        ["decimal"] = 0.1m,
        ["decimal1.5"] = 1.5m,
        ["BigInteger"] = BigInteger.Parse("123456789012345678901234567890", CultureInfo.InvariantCulture),
        ["BigInteger2^53+3"] = (BigInteger.One << 53) + 3,
        ["nint"] = (nint)7,
        ["nuint"] = (nuint)7,
        ["char"] = 'x',
        ["TimeSpan"] = TimeSpan.FromMinutes(1),
        ["DayOfWeek"] = DayOfWeek.Monday,
        ["ShortEnum"] = ShortEnum.A,
        ["LongEnum"] = LongEnum.A,
#if NET5_0_OR_GREATER
        ["Half"] = (Half)0.5,
#endif
#if NET7_0_OR_GREATER
        ["Int128"] = (Int128)7,
        ["Int128.MaxValue"] = Int128.MaxValue,
        ["UInt128"] = (UInt128)7,
        ["UInt128.MaxValue"] = UInt128.MaxValue,
#endif
    };

    private static readonly Dictionary<ConfigType, (object Value, string Text)> s_good = new()
    {
        [ConfigType.Int] = (7, "7"),
        [ConfigType.Long] = (7L, "7"),
        [ConfigType.Short] = ((short)7, "7"),
        [ConfigType.Double] = (1.5, "1.5"),
        [ConfigType.String] = ("x", "x"),
        [ConfigType.Password] = ("secret", "secret"),
        [ConfigType.Boolean] = (true, "true"),
        [ConfigType.List] = (new[] { "a", "b" }, "a,b"),
        [ConfigType.Class] = (typeof(string), "System.String"),
    };

    private static readonly Dictionary<ConfigType, (object Value, string ValueText, string Message)> s_bad = new()
    {
        [ConfigType.Int] = (42.5, "42.5", NotAnInt + "System.Double"),
        [ConfigType.Long] = (new object(), "System.Object", NotALong + "System.Object"),
        [ConfigType.Short] = (1.5, "1.5", NotAShort + "System.Double"),
        [ConfigType.Double] = (new object(), "System.Object", NotADouble + "System.Object"),
        [ConfigType.String] = (new object(), "System.Object", NotAString + "System.Object"),
        [ConfigType.Password] = (5, Hidden, NotAString + "System.Int32"),
        [ConfigType.Boolean] = (1, "1", NotABoolean),
        [ConfigType.List] = (53, "53", NotAList),
        [ConfigType.Class] = (5, "5", NotAClass),
    };

    private static readonly string[] s_producerSerdeKeys =
    {
        "key.serializer", "value.serializer", "serializer.encoding", "key.serializer.encoding",
        "value.serializer.encoding",
    };

    private static readonly string[] s_consumerSerdeKeys =
    {
        "key.deserializer", "value.deserializer", "deserializer.encoding", "key.deserializer.encoding",
        "value.deserializer.encoding",
    };

    private enum ShortEnum : short
    {
        A = 1,
    }

    private enum LongEnum : long
    {
        A = 1,
    }

    public static TheoryData<string, string> TableKeys
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (string client in new[] { "producer", "consumer", "admin" })
            {
                foreach (string key in TypesOf(Kind(client)).Keys.OrderBy(key => key, StringComparer.Ordinal))
                {
                    data.Add(client, key);
                }
            }

            return data;
        }
    }

    [Fact]
    public void ConfigDefTest_testBasicTypes_SendsEachValueAsTheCoreParsesIt()
    {
        var configs = new Dictionary<string, object?>
        {
            ["batch.size"] = "1   ",
            ["linger.ms"] = 2,
            ["bootstrap.servers"] = " a , b, c",
            ["sasl.login.refresh.window.factor"] = 42.5,
            ["partitioner.class"] = "java.lang.String",
            ["enable.idempotence"] = "true",
            ["enable.metrics.push"] = "FalSE",
            ["partitioner.ignore.keys"] = "TRUE",
            ["ssl.key.password"] = "password",
        };

        Assert.Equal(
            new[]
            {
                Pair("batch.size", "1   "),
                Pair("linger.ms", "2"),
                Pair("bootstrap.servers", " a , b, c"),
                Pair("sasl.login.refresh.window.factor", "42.5"),
                Pair("partitioner.class", "java.lang.String"),
                Pair("enable.idempotence", "true"),
                Pair("enable.metrics.push", "FalSE"),
                Pair("partitioner.ignore.keys", "TRUE"),
                Pair("ssl.key.password", "password"),
            },
            ClientConfigs.Prepare(configs, ClientKind.Producer));
    }

    [Theory]
    [InlineData("INT", "42.5", "42.5", NotAnInt + "System.Double")]
    [InlineData("INT", "long.MaxValue", "9223372036854775807", NotAnInt + "System.Int64")]
    [InlineData("INT", "object", "System.Object", NotAnInt + "System.Object")]
    [InlineData("LONG", "object", "System.Object", NotALong + "System.Object")]
    [InlineData("DOUBLE", "object", "System.Object", NotADouble + "System.Object")]
    [InlineData("STRING", "object", "System.Object", NotAString + "System.Object")]
    [InlineData("LIST", "53", "53", NotAList)]
    [InlineData("LIST", "object", "System.Object", NotAList)]
    public void ConfigDefTest_testBadInputs_RefusesAValueOfAnotherType(
        string type, string label, string valueText, string message)
    {
        string key = s_producerKeyOfType[type];

        Assert.Equal(Invalid(key, valueText, message), Refusal(ClientKind.Producer, key, s_values[label]));
    }

    [Theory]
    [InlineData("INT", "hello")]
    [InlineData("INT", "42.5")]
    [InlineData("INT", "9223372036854775807")]
    [InlineData("LONG", "hello")]
    [InlineData("LONG", "42.5")]
    [InlineData("LONG", "922337203685477580700")]
    [InlineData("DOUBLE", "hello")]
    [InlineData("BOOLEAN", "hello")]
    [InlineData("BOOLEAN", "truee")]
    [InlineData("BOOLEAN", "fals")]
    [InlineData("CLASS", "ClassDoesNotExist")]
    public void ConfigDefTest_testBadInputs_SendsTheTextToTheCoreUnchanged(string type, string text)
    {
        string key = s_producerKeyOfType[type];

        Assert.Equal(new[] { Pair(key, text) }, ClientConfigs.Prepare(Map(key, text), ClientKind.Producer));
    }

    [Theory]
    [InlineData("producer")]
    [InlineData("consumer")]
    [InlineData("admin")]
    public void UtilsTest_testCastToStringObjectMapNonStringKey_RefusesANullKey(string client)
    {
        var configs = new Entries((null, "value"));

        Assert.Equal(
            "Invalid value value for configuration null: " + KeyMustBeAString,
            RefusalOf(configs, Kind(client)));
    }

    [Theory]
    [InlineData("producer")]
    [InlineData("consumer")]
    [InlineData("admin")]
    public void UtilsTest_testCastToStringObjectMap_KeepsTypedValues(string client)
    {
        var configs = new Entries(("key1", "value1"), ("key2", 1));

        Assert.Equal(
            new[] { Pair("key1", "value1"), Pair("key2", "1") },
            ClientConfigs.Prepare(configs, Kind(client)));
    }

    [Fact]
    public void Prepare_ChecksEveryKeyBeforeAnyValue()
    {
        Assert.Equal(
            "Invalid value x for configuration null: " + KeyMustBeAString,
            RefusalOf(new Entries(("batch.size", 42.5), (null, "x")), ClientKind.Producer));
        Assert.Equal(
            "Invalid value x for configuration null: " + KeyMustBeAString,
            RefusalOf(new Entries(("acks", null), (null, "x")), ClientKind.Producer));
        Assert.Equal(
            "Invalid value null for configuration null: " + KeyMustBeAString,
            RefusalOf(new Entries(("key.serializer", 5), (null, null)), ClientKind.Producer));
    }

    [Fact]
    public void TestConfig_test_basic_types()
    {
        AssertSent(ClientKind.Producer, "batch.size", "1   ", "1   ");
        AssertSent(ClientKind.Producer, "linger.ms", 2, "2");
        AssertSent(ClientKind.Producer, "client.id", "hello", "hello");
        AssertSent(ClientKind.Producer, "bootstrap.servers", new[] { " a ", "b" }, " a ,b");
        AssertSent(ClientKind.Producer, "sasl.login.refresh.window.factor", 42.5, "42.5");
        AssertSent(ClientKind.Producer, "partitioner.class", typeof(string), "System.String");
        AssertSent(ClientKind.Producer, "enable.idempotence", true, "true");
        AssertSent(ClientKind.Producer, "enable.idempotence", "FalSE", "FalSE");
        AssertSent(ClientKind.Producer, "ssl.key.password", "password", "password");
        Assert.Empty(ClientConfigs.Prepare(Map("transaction.timeout.ms", null), ClientKind.Producer));
        AssertSent(
            ClientKind.Producer, "bootstrap.servers", new ReadOnlyCollection<string>(new[] { "a", "b" }), "a,b");
        AssertSent(ClientKind.Producer, "sasl.login.refresh.window.factor", 3, "3");
    }

    [Theory]
    [InlineData("INT", "42.5", "42.5", NotAnInt + "System.Double")]
    [InlineData("INT", "object", "System.Object", NotAnInt + "System.Object")]
    [InlineData("INT", "true", "true", NotAnInt + "System.Boolean")]
    [InlineData("LONG", "object", "System.Object", NotALong + "System.Object")]
    [InlineData("SHORT", "1.5", "1.5", NotAShort + "System.Double")]
    [InlineData("DOUBLE", "object", "System.Object", NotADouble + "System.Object")]
    [InlineData("DOUBLE", "false", "false", NotADouble + "System.Boolean")]
    [InlineData("STRING", "object", "System.Object", NotAString + "System.Object")]
    [InlineData("PASSWORD", "5", Hidden, NotAString + "System.Int32")]
    [InlineData("LIST", "53", "53", NotAList)]
    [InlineData("LIST", "object", "System.Object", NotAList)]
    [InlineData("BOOLEAN", "1", "1", NotABoolean)]
    [InlineData("CLASS", "5", "5", NotAClass)]
    public void TestConfig_test_bad_inputs(string type, string label, string valueText, string message)
    {
        string key = s_producerKeyOfType[type];

        Assert.Equal(Invalid(key, valueText, message), Refusal(ClientKind.Producer, key, s_values[label]));
    }

    [Theory]
    [InlineData("producer")]
    [InlineData("consumer")]
    [InlineData("admin")]
    public void APasswordOfAnotherType_IsRefusedWithoutShowingIt(string client)
    {
        Assert.Equal(
            Invalid("ssl.key.password", Hidden, NotAString + "System.Char[]"),
            Refusal(Kind(client), "ssl.key.password", "s3cret".ToCharArray()));
        Assert.Equal(
            Invalid("sasl.jaas.config", Hidden, NotAString + "System.Byte[]"),
            Refusal(Kind(client), "sasl.jaas.config", new byte[] { 0x73, 0x33 }));
        Assert.Equal(
            Invalid("client.id", "[s, 3]", NotAString + "System.Char[]"),
            Refusal(Kind(client), "client.id", "s3".ToCharArray()));
    }

    [Theory]
    [InlineData("INT", "hello")]
    [InlineData("INT", "42.5")]
    [InlineData("INT", "9223372036854775807")]
    [InlineData("LONG", "xxx")]
    [InlineData("SHORT", "32768")]
    [InlineData("DOUBLE", "inf")]
    [InlineData("BOOLEAN", "truee")]
    [InlineData("LIST", "a,b")]
    [InlineData("CLASS", "ClassDoesNotExist")]
    public void TestConfig_test_value_checks_are_the_cores(string type, string text)
    {
        string key = s_producerKeyOfType[type];

        Assert.Equal(new[] { Pair(key, text) }, ClientConfigs.Prepare(Map(key, text), ClientKind.Producer));
    }

    [Fact]
    public void TestConfig_test_value_checks_are_the_cores_ALongOnAnIntKeyIsRefusedByItsType()
    {
        Assert.Equal(
            Invalid("batch.size", "9223372036854775807", NotAnInt + "System.Int64"),
            Refusal(ClientKind.Producer, "batch.size", long.MaxValue));
    }

    [Fact]
    public void TestConfig_test_prepare_converts_known_keys_for_the_core()
    {
        var configs = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = " a:9092 , b:9092",
            ["linger.ms"] = 5,
            ["enable.idempotence"] = "TRUE",
            ["sasl.login.refresh.window.factor"] = 0.8,
            ["acks"] = " all ",
            ["unknown.key"] = 1,
            ["key.serializer"] = "x.Y",
            ["transactional.id"] = null,
        };

        Assert.Equal(
            new[]
            {
                Pair("bootstrap.servers", " a:9092 , b:9092"),
                Pair("linger.ms", "5"),
                Pair("enable.idempotence", "TRUE"),
                Pair("sasl.login.refresh.window.factor", "0.80000000000000004"),
                Pair("acks", " all "),
                Pair("unknown.key", "1"),
            },
            ClientConfigs.Prepare(configs, ClientKind.Producer));
        Assert.Equal(8, configs.Count);
        Assert.Equal("x.Y", configs["key.serializer"]);

        var typed = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = new[] { "a:9092", "b:9092" },
            ["enable.idempotence"] = true,
            ["sasl.login.refresh.window.factor"] = 3,
        };
        Assert.Equal(
            new[]
            {
                Pair("bootstrap.servers", "a:9092,b:9092"),
                Pair("enable.idempotence", "true"),
                Pair("sasl.login.refresh.window.factor", "3"),
            },
            ClientConfigs.Prepare(typed, ClientKind.Producer));
    }

    [Theory]
    [InlineData("producer", "key.serializer")]
    [InlineData("producer", "value.serializer")]
    [InlineData("consumer", "key.deserializer")]
    [InlineData("consumer", "value.deserializer")]
    public void TestConfig_test_a_given_serde_argument_replaces_its_config_key(string client, string key)
    {
        Assert.Empty(ClientConfigs.Prepare(Map(key, new object()), Kind(client)));
    }

    [Fact]
    public void TestConfig_test_prepare_uses_the_client_config_def()
    {
        Assert.True(ConfigTypes.Consumer.ContainsKey("max.poll.records"));
        Assert.False(ConfigTypes.Producer.ContainsKey("max.poll.records"));

        Assert.Equal(
            Invalid("max.poll.records", "1.5", NotAnInt + "System.Double"),
            Refusal(ClientKind.Consumer, "max.poll.records", 1.5));
        Assert.Equal(
            new[] { Pair("max.poll.records", "many") },
            ClientConfigs.Prepare(Map("max.poll.records", "many"), ClientKind.Consumer));
        Assert.Equal(
            new[] { Pair("max.poll.records", "1.5") },
            ClientConfigs.Prepare(Map("max.poll.records", 1.5), ClientKind.Producer));
    }

    [Fact]
    public void TestConfig_test_key_must_be_a_string()
    {
        Assert.Equal(
            "Invalid value x for configuration null: " + KeyMustBeAString,
            RefusalOf(new Entries((null, "x")), ClientKind.Producer));
    }

    [Fact]
    public void TestConfig_test_only_the_own_role_serde_keys_stay_with_the_binding()
    {
        var serializerKeys = new[]
        {
            Pair("key.serializer", "x.S"),
            Pair("value.serializer", "x.S"),
            Pair("serializer.encoding", "UTF-16"),
            Pair("key.serializer.encoding", "ascii"),
            Pair("value.serializer.encoding", "UTF-16BE"),
        };
        var deserializerKeys = new[]
        {
            Pair("key.deserializer", "x.D"),
            Pair("value.deserializer", "x.D"),
            Pair("deserializer.encoding", "UTF-16"),
            Pair("key.deserializer.encoding", "ascii"),
            Pair("value.deserializer.encoding", "UTF-16BE"),
        };
        var configs = new Dictionary<string, object?>();
        foreach (KeyValuePair<string, string> pair in serializerKeys.Concat(deserializerKeys))
        {
            configs.Add(pair.Key, pair.Value);
        }

        Assert.Equal(deserializerKeys, ClientConfigs.Prepare(configs, ClientKind.Producer));
        Assert.Equal(serializerKeys, ClientConfigs.Prepare(configs, ClientKind.Consumer));
        Assert.Equal(serializerKeys.Concat(deserializerKeys), ClientConfigs.Prepare(configs, ClientKind.Admin));
        Assert.Equal(10, configs.Count);
    }

    [Fact]
    public void TestConfigMatrix_test_the_table_covers_every_type_here()
    {
        var types = new HashSet<ConfigType>();
        int cases = 0;
        foreach (ClientKind client in new[] { ClientKind.Producer, ClientKind.Consumer, ClientKind.Admin })
        {
            types.UnionWith(TypesOf(client).Values);
            cases += TypesOf(client).Count;
            Assert.DoesNotContain(NullRulesOf(client).Keys, key => !TypesOf(client).ContainsKey(key));
        }

        Assert.Subset(new HashSet<ConfigType>(s_good.Keys), types);
        Assert.Equal(
            ((ConfigType[])Enum.GetValues(typeof(ConfigType))).OrderBy(type => type),
            s_good.Keys.OrderBy(type => type));
        Assert.Equal(s_good.Keys.OrderBy(type => type), s_bad.Keys.OrderBy(type => type));
        Assert.True(cases > 300, $"{cases} keys");
        Assert.Equal(cases, TableKeys.Count());
    }

    [Theory]
    [MemberData(nameof(TableKeys))]
    public void TestConfigMatrix_test_a_value_of_the_keys_type_reaches_the_core(string client, string key)
    {
        ClientKind kind = Kind(client);
        (object value, string text) = s_good[TypesOf(kind)[key]];

        IReadOnlyList<KeyValuePair<string, string>> sent = ClientConfigs.Prepare(Map(key, value), kind);

        if (IsOwnRoleSerdeKey(client, key))
        {
            Assert.Empty(sent);
        }
        else
        {
            Assert.Equal(new[] { Pair(key, text) }, sent);
        }
    }

    [Theory]
    [MemberData(nameof(TableKeys))]
    public void TestConfigMatrix_test_a_value_of_the_wrong_type_is_rejected(string client, string key)
    {
        ClientKind kind = Kind(client);
        (object value, string valueText, string message) = s_bad[TypesOf(kind)[key]];

        if (IsOwnRoleSerdeKey(client, key))
        {
            Assert.Empty(ClientConfigs.Prepare(Map(key, value), kind));
            return;
        }

        Assert.Equal(Invalid(key, valueText, message), Refusal(kind, key, value));
    }

    [Theory]
    [MemberData(nameof(TableKeys))]
    public void TestConfigMatrix_test_none_follows_the_keys_validator(string client, string key)
    {
        ClientKind kind = Kind(client);
        if (IsOwnRoleSerdeKey(client, key)
            || !NullRulesOf(kind).TryGetValue(key, out (ConfigNullForm Form, string Message) rule))
        {
            Assert.Empty(ClientConfigs.Prepare(Map(key, null), kind));
            return;
        }

        string expected = rule.Form == ConfigNullForm.Named ? Invalid(key, "null", rule.Message) : rule.Message;
        Assert.Equal(expected, Refusal(kind, key, null));
    }

    [Theory]
    [InlineData("producer", "linger.ms")]
    [InlineData("producer", "max.in.flight.requests.per.connection")]
    [InlineData("consumer", "max.poll.records")]
    [InlineData("consumer", "request.timeout.ms")]
    [InlineData("admin", "retries")]
    [InlineData("producer", "sasl.login.refresh.window.factor")]
    [InlineData("producer", "compression.gzip.level")]
    [InlineData("producer", "compression.zstd.level")]
    public void TestConfigNull_test_range(string client, string key)
    {
        Assert.Equal(Invalid(key, "null", "Value must be non-null"), Refusal(Kind(client), key, null));
    }

    [Theory]
    [InlineData("producer", "acks", "all, -1, 0, 1")]
    [InlineData("producer", "compression.type", "none, gzip, snappy, lz4, zstd")]
    [InlineData("consumer", "isolation.level", "read_committed, read_uncommitted")]
    [InlineData("admin", "client.dns.lookup", "use_all_dns_ips, resolve_canonical_bootstrap_servers_only")]
    [InlineData("consumer", "metrics.recording.level", "INFO, DEBUG, TRACE")]
    public void TestConfigNull_test_valid_string(string client, string key, string valid)
    {
        Assert.Equal(
            Invalid(key, "null", "String must be one of: " + valid),
            Refusal(Kind(client), key, null));
    }

    [Theory]
    [InlineData("admin", "security.protocol", "SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT")]
    [InlineData("consumer", "group.protocol", "CONSUMER, CLASSIC")]
    [InlineData("producer", "metadata.recovery.strategy", "REBOOTSTRAP, NONE")]
    [InlineData("producer", "buffer.memory.allocation.strategy", "FULL, INCREMENTAL")]
    [InlineData("consumer", "sasl.oauthbearer.assertion.algorithm", "ES256, RS256")]
    public void TestConfigNull_test_case_insensitive_valid_string(string client, string key, string valid)
    {
        Assert.Equal(
            Invalid(key, "null", "String must be one of (case insensitive): " + valid),
            Refusal(Kind(client), key, null));
    }

    [Theory]
    [InlineData("producer", "bootstrap.servers")]
    [InlineData("consumer", "metric.reporters")]
    public void TestConfigNull_test_valid_list_refusing_null(string client, string key)
    {
        Assert.Equal($"Configuration '{key}' values must not be null.", Refusal(Kind(client), key, null));
    }

    [Theory]
    [InlineData(
        "consumer",
        "auto.offset.reset",
        "The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'.")]
    [InlineData("consumer", "share.acknowledgement.mode", "The value must either be 'implicit' or 'explicit'.")]
    [InlineData("consumer", "share.acquire.mode", "The value must either be 'batch_optimized' or 'record_limit'.")]
    public void TestConfigNull_test_custom_validator(string client, string key, string tail)
    {
        Assert.Equal(
            Invalid(key, "null", $"Invalid value `null` for configuration {key}. {tail}"),
            Refusal(Kind(client), key, null));
    }

    [Theory]
    [InlineData("producer", "transactional.id")]
    [InlineData("producer", "client.id")]
    [InlineData("consumer", "group.id")]
    [InlineData("admin", "sasl.jaas.config")]
    [InlineData("producer", "key.serializer")]
    [InlineData("consumer", "value.deserializer")]
    [InlineData("producer", "unknown.key")]
    public void TestConfigNull_test_none_allowed_is_not_set(string client, string key)
    {
        Assert.Empty(ClientConfigs.Prepare(Map(key, null), Kind(client)));
    }

    [Theory]
    [InlineData("producer")]
    [InlineData("consumer")]
    public void OwnRoleSerializerKeys_AreNeitherCheckedNorSent(string client)
    {
        string[] keys = client == "producer" ? s_producerSerdeKeys : s_consumerSerdeKeys;
        var configs = new Dictionary<string, object?>
        {
            [keys[0]] = 5,
            [keys[1]] = new object(),
            [keys[2]] = 1.5,
            [keys[3]] = null,
            [keys[4]] = typeof(string),
        };

        Assert.Empty(ClientConfigs.Prepare(configs, Kind(client)));
        Assert.Empty(ClientConfigs.Prepare(Map(keys[0], 5), Kind(client)));
    }

    [Fact]
    public void OtherRoleSerializerKeys_AreSentAsAnyOtherKey()
    {
        Assert.Equal(
            new[] { Pair("key.deserializer", "x.D") },
            ClientConfigs.Prepare(Map("key.deserializer", "x.D"), ClientKind.Producer));
        Assert.Equal(
            new[] { Pair("key.deserializer", "5") },
            ClientConfigs.Prepare(Map("key.deserializer", 5), ClientKind.Producer));
        Assert.Equal(
            new[] { Pair("key.serializer", "x.S") },
            ClientConfigs.Prepare(Map("key.serializer", "x.S"), ClientKind.Consumer));
        Assert.Equal(
            new[] { Pair("key.serializer", "5") },
            ClientConfigs.Prepare(Map("key.serializer", 5), ClientKind.Consumer));
        foreach (string key in s_producerSerdeKeys.Concat(s_consumerSerdeKeys))
        {
            Assert.Equal(new[] { Pair(key, "5") }, ClientConfigs.Prepare(Map(key, 5), ClientKind.Admin));
        }
    }

    [Theory]
    [InlineData("batch.size", "BigInteger", "123456789012345678901234567890", NotAnInt + "System.Numerics.BigInteger")]
    [InlineData("linger.ms", "BigInteger", "123456789012345678901234567890", NotALong + "System.Numerics.BigInteger")]
    [InlineData("linger.ms", "uint", "7", NotALong + "System.UInt32")]
    [InlineData("linger.ms", "ulong", "7", NotALong + "System.UInt64")]
    [InlineData("linger.ms", "decimal1.5", "1.5", NotALong + "System.Decimal")]
    [InlineData("linger.ms", "1.5", "1.5", NotALong + "System.Double")]
    [InlineData("linger.ms", "nint", "7", NotALong + "System.IntPtr")]
    [InlineData("batch.size", "long16384", "16384", NotAnInt + "System.Int64")]
    [InlineData("batch.size", "short7", "7", NotAnInt + "System.Int16")]
    [InlineData("batch.size", "byte7", "7", NotAnInt + "System.Byte")]
    [InlineData("batch.size", "float1.5", "1.5", NotAnInt + "System.Single")]
    [InlineData("batch.size", "nuint", "7", NotAnInt + "System.UIntPtr")]
    [InlineData("sasl.login.refresh.min.period.seconds", "int60", "60", NotAShort + "System.Int32")]
    [InlineData("sasl.login.refresh.buffer.seconds", "int60", "60", NotAShort + "System.Int32")]
    [InlineData("sasl.login.refresh.window.factor", "nint", "7", NotADouble + "System.IntPtr")]
    [InlineData("sasl.login.refresh.window.factor", "nuint", "7", NotADouble + "System.UIntPtr")]
#if NET5_0_OR_GREATER
    [InlineData("batch.size", "Half", "0.5", NotAnInt + "System.Half")]
#endif
#if NET7_0_OR_GREATER
    [InlineData("linger.ms", "Int128", "7", NotALong + "System.Int128")]
    [InlineData("batch.size", "UInt128", "7", NotAnInt + "System.UInt128")]
#endif
    public void JavaClasses_AKeyRefusesANumberTypeItsJavaTypeDoesNotTake(
        string key, string label, string valueText, string message)
    {
        Assert.Equal(Invalid(key, valueText, message), Refusal(ClientKind.Producer, key, s_values[label]));
    }

    [Fact]
    public void JavaClasses_AShortKeyTakesAShortOnEveryClient()
    {
        foreach (ClientKind client in new[] { ClientKind.Producer, ClientKind.Consumer, ClientKind.Admin })
        {
            AssertSent(client, "sasl.login.refresh.min.period.seconds", (short)60, "60");
            AssertSent(client, "sasl.login.refresh.buffer.seconds", (short)300, "300");
            Assert.Equal(
                Invalid("sasl.login.refresh.min.period.seconds", "60", NotAShort + "System.Int32"),
                Refusal(client, "sasl.login.refresh.min.period.seconds", 60));
        }
    }

    [Fact]
    public void JavaClasses_ALongKeyTakesAnIntOrALong()
    {
        AssertSent(ClientKind.Producer, "linger.ms", 5, "5");
        AssertSent(ClientKind.Producer, "linger.ms", 5L, "5");
        AssertSent(ClientKind.Producer, "linger.ms", long.MaxValue, "9223372036854775807");
        AssertSent(ClientKind.Producer, "batch.size", int.MinValue, "-2147483648");
    }

    [Theory]
    [InlineData("sbyte", "-8")]
    [InlineData("byte", "255")]
    [InlineData("short", "-300")]
    [InlineData("ushort", "65535")]
    [InlineData("int", "-7")]
    [InlineData("uint4000000000", "4000000000")]
    [InlineData("long", "-9223372036854775808")]
    [InlineData("ulong.MaxValue", "18446744073709551615")]
    [InlineData("float", "0.10000000149011612")]
    [InlineData("double", "0.10000000000000001")]
    [InlineData("decimal", "0.1")]
    [InlineData("BigInteger2^53+3", "9007199254740995")]
#if NET5_0_OR_GREATER
    [InlineData("Half", "0.5")]
#endif
#if NET7_0_OR_GREATER
    [InlineData("Int128.MaxValue", "170141183460469231731687303715884105727")]
    [InlineData("UInt128.MaxValue", "340282366920938463463374607431768211455")]
#endif
    public void JavaClasses_ADoubleKeyTakesEveryNumberButAPointer(string label, string text)
    {
        AssertSent(ClientKind.Producer, "sasl.login.refresh.window.factor", s_values[label], text);
    }

    [Theory]
    [InlineData("batch.size", "char", "x", NotAnInt + "System.Char")]
    [InlineData("sasl.login.refresh.min.period.seconds", "char", "x", NotAShort + "System.Char")]
    [InlineData("linger.ms", "char", "x", NotALong + "System.Char")]
    [InlineData("sasl.login.refresh.window.factor", "char", "x", NotADouble + "System.Char")]
    [InlineData("batch.size", "TimeSpan", "00:01:00", NotAnInt + "System.TimeSpan")]
    [InlineData("sasl.login.refresh.min.period.seconds", "TimeSpan", "00:01:00", NotAShort + "System.TimeSpan")]
    [InlineData("linger.ms", "TimeSpan", "00:01:00", NotALong + "System.TimeSpan")]
    [InlineData("sasl.login.refresh.window.factor", "TimeSpan", "00:01:00", NotADouble + "System.TimeSpan")]
    [InlineData("batch.size", "DayOfWeek", "Monday", NotAnInt + "System.DayOfWeek")]
    [InlineData("linger.ms", "DayOfWeek", "Monday", NotALong + "System.DayOfWeek")]
    [InlineData("sasl.login.refresh.window.factor", "DayOfWeek", "Monday", NotADouble + "System.DayOfWeek")]
    [InlineData(
        "sasl.login.refresh.min.period.seconds",
        "ShortEnum",
        "A",
        NotAShort + "Confluent.Kafka.UnitTests.Internal.ClientConfigsTests+ShortEnum")]
    [InlineData(
        "linger.ms", "LongEnum", "A", NotALong + "Confluent.Kafka.UnitTests.Internal.ClientConfigsTests+LongEnum")]
    [InlineData("batch.size", "true", "true", NotAnInt + "System.Boolean")]
    [InlineData("sasl.login.refresh.window.factor", "false", "false", NotADouble + "System.Boolean")]
    public void JavaClasses_ACharATimeSpanAnEnumOrABoolIsNotANumber(
        string key, string label, string valueText, string message)
    {
        Assert.Equal(Invalid(key, valueText, message), Refusal(ClientKind.Producer, key, s_values[label]));
    }

    [Fact]
    public void JavaClasses_AGenericTypeIsNamedByItsToStringNotItsAssemblyQualifiedFullName()
    {
        Assert.Equal(
            Invalid("client.id", "[a]", NotAString + "System.Collections.Generic.List`1[System.String]"),
            Refusal(ClientKind.Producer, "client.id", new List<string> { "a" }));
        Assert.Equal(
            Invalid(
                "batch.size", "{a=1}", NotAnInt + "System.Collections.Generic.Dictionary`2[System.String,System.Int32]"),
            Refusal(ClientKind.Producer, "batch.size", new Dictionary<string, int> { ["a"] = 1 }));
    }

    [Fact]
    public void Lists_AListKeyTakesAnIList()
    {
        AssertSent(ClientKind.Producer, "bootstrap.servers", new[] { "a", "b" }, "a,b");
        AssertSent(ClientKind.Producer, "bootstrap.servers", new List<string> { "a", "b" }, "a,b");
        AssertSent(
            ClientKind.Producer, "bootstrap.servers", new ReadOnlyCollection<string>(new[] { "a", "b" }), "a,b");
#if NET
        AssertSent(ClientKind.Producer, "bootstrap.servers", ImmutableList.Create("a", "b"), "a,b");
#endif
        AssertSent(ClientKind.Producer, "bootstrap.servers", new ArrayList { "a", 1 }, "a,1");
        AssertSent(ClientKind.Producer, "bootstrap.servers", new object?[] { "a", null }, "a,null");
        AssertSent(ClientKind.Producer, "bootstrap.servers", new[] { 1, 2 }, "1,2");
        AssertSent(ClientKind.Producer, "bootstrap.servers", new[] { 0.5, 1.5 }, "0.5,1.5");
        AssertSent(ClientKind.Producer, "bootstrap.servers", Array.Empty<string>(), string.Empty);
        AssertSent(ClientKind.Producer, "bootstrap.servers", "a, b", "a, b");
    }

    [Fact]
    public void Lists_AListKeyRefusesASetADictionaryAndAnArrayThatIsNotAList()
    {
        var multiDimensional = new int[,] { { 1, 2 }, { 3, 4 } };
        var nonZeroBased = Array.CreateInstance(typeof(string), new[] { 1 }, new[] { 1 });
        nonZeroBased.SetValue("a", 1);

        Assert.Equal(
            Invalid("bootstrap.servers", "[a]", NotAList),
            Refusal(ClientKind.Producer, "bootstrap.servers", new HashSet<string> { "a" }));
        Assert.Equal(
            Invalid("bootstrap.servers", "{a=1}", NotAList),
            Refusal(ClientKind.Producer, "bootstrap.servers", new Dictionary<string, int> { ["a"] = 1 }));
        Assert.Equal(
            Invalid("bootstrap.servers", "[1, 2, 3, 4]", NotAList),
            Refusal(ClientKind.Producer, "bootstrap.servers", multiDimensional));
        Assert.Equal(
            Invalid("bootstrap.servers", "[a]", NotAList),
            Refusal(ClientKind.Producer, "bootstrap.servers", nonZeroBased));
    }

    [Fact]
    public void Lists_AListKeyRefusesADictionaryThatIsAlsoAList()
    {
        Assert.Equal(
            Invalid("bootstrap.servers", "{a=1}", NotAList),
            Refusal(ClientKind.Producer, "bootstrap.servers", new ListMap { ["a"] = 1 }));
        Assert.Equal(
            Invalid("bootstrap.servers", "{a=1}", NotAList),
            Refusal(ClientKind.Producer, "bootstrap.servers", new PairList { new("a", 1) }));
        Assert.Equal(
            Invalid("bootstrap.servers", "{a=1}", NotAList),
            Refusal(ClientKind.Producer, "bootstrap.servers", new ReadOnlyPairList { new("a", 1) }));
#if NET9_0_OR_GREATER
        Assert.Equal(
            Invalid("bootstrap.servers", "{a=1, b=2}", NotAList),
            Refusal(
                ClientKind.Producer,
                "bootstrap.servers",
                new OrderedDictionary<string, int> { ["a"] = 1, ["b"] = 2 }));
#endif
    }

    [Fact]
    public void Lists_ALazySequenceIsRefusedWithoutBeingEnumerated()
    {
        var lazy = new LazySequence();

        Assert.Equal(
            Invalid("bootstrap.servers", "lazy", NotAList),
            Refusal(ClientKind.Producer, "bootstrap.servers", lazy));
        Assert.Equal(0, lazy.Enumerations);
    }

    [Fact]
    public void Classes_AClassKeyTakesATypeSentAsItsFullName()
    {
        AssertSent(ClientKind.Producer, "partitioner.class", typeof(string), "System.String");
        AssertSent(
            ClientKind.Producer,
            "partitioner.class",
            typeof(LazySequence),
            "Confluent.Kafka.UnitTests.Internal.ClientConfigsTests+LazySequence");
        AssertSent(ClientKind.Producer, "partitioner.class", "com.example.MyPartitioner", "com.example.MyPartitioner");
        Assert.Equal(
            Invalid("partitioner.class", "System.Object", NotAClass),
            Refusal(ClientKind.Producer, "partitioner.class", new object()));
    }

    [Fact]
    public void Text_AKeyNotInTheTableIsNotTypeCheckedAndGetsItsValuesText()
    {
        var configs = new Dictionary<string, object?>
        {
            ["unknown.list"] = new List<string> { "a", "b" },
            ["unknown.map"] = new Dictionary<string, int> { ["k"] = 1 },
            ["unknown.bool"] = false,
            ["unknown.double"] = 0.1,
            ["unknown.type"] = typeof(string),
            ["unknown.timespan"] = TimeSpan.FromMinutes(1),
            ["unknown.null"] = null,
        };

        Assert.Equal(
            new[]
            {
                Pair("unknown.list", "[a, b]"),
                Pair("unknown.map", "{k=1}"),
                Pair("unknown.bool", "false"),
                Pair("unknown.double", "0.10000000000000001"),
                Pair("unknown.type", "System.String"),
                Pair("unknown.timespan", "00:01:00"),
            },
            ClientConfigs.Prepare(configs, ClientKind.Producer));
    }

    [Fact]
    public void Text_AToStringThatReturnsNullIsJavasNull()
    {
        var configs = new Dictionary<string, object?>
        {
            ["unknown.object"] = new NullText(),
            ["unknown.formattable"] = new NullFormattable(),
            ["bootstrap.servers"] = new object[] { new NullText(), new NullFormattable() },
        };

        Assert.Equal(
            new[]
            {
                Pair("unknown.object", "null"),
                Pair("unknown.formattable", "null"),
                Pair("bootstrap.servers", "null,null"),
            },
            ClientConfigs.Prepare(configs, ClientKind.Producer));
        Assert.Equal(
            Invalid("batch.size", "null", NotAnInt + "Confluent.Kafka.UnitTests.Internal.ClientConfigsTests+NullFormattable"),
            Refusal(ClientKind.Producer, "batch.size", new NullFormattable()));
    }

    [Fact]
    public void Text_ThePairsFollowTheDictionarysEnumerationOrder()
    {
        var configs = new Entries(("linger.ms", 5), ("acks", "all"), ("unknown.key", "x"), ("batch.size", 7));

        Assert.Equal(
            new[] { Pair("linger.ms", "5"), Pair("acks", "all"), Pair("unknown.key", "x"), Pair("batch.size", "7") },
            ClientConfigs.Prepare(configs, ClientKind.Producer));
    }

    [Theory]
    [InlineData("producer")]
    [InlineData("consumer")]
    [InlineData("admin")]
    public void NullMap_ThrowsArgumentNullExceptionNamingConfigs(string client)
    {
        ArgumentNullException exception =
            Assert.Throws<ArgumentNullException>(() => ClientConfigs.Prepare(null!, Kind(client)));

        Assert.Equal("configs", exception.ParamName);
    }

    [Theory]
    [InlineData("de-DE")]
    [InlineData("sv-SE")]
    public void Culture_DoesNotChangeAnyTextOrMessage(string culture)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try
        {
            Assert.NotEqual("-1.5", (-1.5).ToString(CultureInfo.CurrentCulture));
            var configs = new Dictionary<string, object?>
            {
                ["sasl.login.refresh.window.factor"] = -1.5,
                ["linger.ms"] = -1L,
                ["bootstrap.servers"] = new object[] { -1, 0.5 },
                ["unknown.decimal"] = -0.5m,
                ["unknown.float"] = -1.5f,
            };

            Assert.Equal(
                new[]
                {
                    Pair("sasl.login.refresh.window.factor", "-1.5"),
                    Pair("linger.ms", "-1"),
                    Pair("bootstrap.servers", "-1,0.5"),
                    Pair("unknown.decimal", "-0.5"),
                    Pair("unknown.float", "-1.5"),
                },
                ClientConfigs.Prepare(configs, ClientKind.Producer));
            Assert.Equal(
                Invalid("batch.size", "-1.5", NotAnInt + "System.Double"),
                Refusal(ClientKind.Producer, "batch.size", -1.5));
            Assert.Equal(
                Invalid("linger.ms", "-1.5", NotALong + "System.Decimal"),
                Refusal(ClientKind.Producer, "linger.ms", -1.5m));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void Fixture_TheKeyOfEachTypeHasThatTypeInTheProducerTable()
    {
        foreach (KeyValuePair<string, string> entry in s_producerKeyOfType)
        {
            Assert.Equal(entry.Key, TypeLabel(ConfigTypes.Producer[entry.Value]));
        }

        Assert.Equal(ConfigType.Short, ConfigTypes.Producer["sasl.login.refresh.buffer.seconds"]);
        Assert.Equal(ConfigType.Int, ConfigTypes.Producer["transaction.timeout.ms"]);
        Assert.False(ConfigTypes.ProducerNull.ContainsKey("transaction.timeout.ms"));
        Assert.Equal(ConfigType.Boolean, ConfigTypes.Producer["enable.metrics.push"]);
        Assert.Equal(ConfigType.Boolean, ConfigTypes.Producer["partitioner.ignore.keys"]);
        Assert.False(ConfigTypes.Producer.ContainsKey("key.deserializer"));
        Assert.False(ConfigTypes.Consumer.ContainsKey("key.serializer"));
    }

    private static ClientKind Kind(string client) => client switch
    {
        "producer" => ClientKind.Producer,
        "consumer" => ClientKind.Consumer,
        "admin" => ClientKind.Admin,
        _ => throw new ArgumentOutOfRangeException(nameof(client), client, null),
    };

    private static IReadOnlyDictionary<string, ConfigType> TypesOf(ClientKind client) => client switch
    {
        ClientKind.Producer => ConfigTypes.Producer,
        ClientKind.Consumer => ConfigTypes.Consumer,
        _ => ConfigTypes.Admin,
    };

    private static IReadOnlyDictionary<string, (ConfigNullForm Form, string Message)> NullRulesOf(ClientKind client) =>
        client switch
        {
            ClientKind.Producer => ConfigTypes.ProducerNull,
            ClientKind.Consumer => ConfigTypes.ConsumerNull,
            _ => ConfigTypes.AdminNull,
        };

    private static string TypeLabel(ConfigType type) => type.ToString().ToUpperInvariant();

    private static bool IsOwnRoleSerdeKey(string client, string key) => client switch
    {
        "producer" => s_producerSerdeKeys.Contains(key),
        "consumer" => s_consumerSerdeKeys.Contains(key),
        _ => false,
    };

    private static Dictionary<string, object?> Map(string key, object? value) => new() { [key] = value };

    private static KeyValuePair<string, string> Pair(string key, string text) => new(key, text);

    private static string Invalid(string key, string valueText, string message) =>
        $"Invalid value {valueText} for configuration {key}: {message}";

    private static void AssertSent(ClientKind client, string key, object value, string text) =>
        Assert.Equal(new[] { Pair(key, text) }, ClientConfigs.Prepare(Map(key, value), client));

    private static string Refusal(ClientKind client, string key, object? value) =>
        RefusalOf(Map(key, value), client);

    private static string RefusalOf(IReadOnlyDictionary<string, object?> configs, ClientKind client)
    {
        ConfigException exception = Assert.Throws<ConfigException>(() => ClientConfigs.Prepare(configs, client));
        Assert.Equal(-10, exception.Code);
        Assert.Null(exception.InnerException);
        return exception.Message;
    }

    /// <summary>A sequence that counts its enumerations, whose <see cref="object.ToString"/> is <c>lazy</c>.</summary>
    private sealed class LazySequence : IEnumerable<string>
    {
        internal int Enumerations { get; private set; }

        public IEnumerator<string> GetEnumerator()
        {
            Enumerations++;
            return new List<string> { "a" }.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public override string ToString() => "lazy";
    }

    /// <summary>An object whose <see cref="object.ToString"/> returns <see langword="null"/>.</summary>
    private sealed class NullText
    {
        public override string? ToString() => null;
    }

    /// <summary>A formattable value whose <c>ToString(format, provider)</c> returns <see langword="null"/>.</summary>
    private sealed class NullFormattable : IFormattable
    {
        public string ToString(string? format, IFormatProvider? formatProvider) => null!;

        public override string ToString() => "not the formattable text";
    }

    /// <summary>A non-generic dictionary that is also a non-generic list; its list members throw.</summary>
    private sealed class ListMap : Hashtable, IList
    {
        object? IList.this[int index]
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        int IList.Add(object? value) => throw new NotSupportedException();

        bool IList.Contains(object? value) => throw new NotSupportedException();

        int IList.IndexOf(object? value) => throw new NotSupportedException();

        void IList.Insert(int index, object? value) => throw new NotSupportedException();

        void IList.Remove(object? value) => throw new NotSupportedException();

        void IList.RemoveAt(int index) => throw new NotSupportedException();
    }

    /// <summary>A list of pairs that is also an <see cref="IDictionary{TKey, TValue}"/>, but not an <see cref="IDictionary"/>.</summary>
    private sealed class PairList : Collection<KeyValuePair<string, int>>, IDictionary<string, int>
    {
        public ICollection<string> Keys => this.Select(pair => pair.Key).ToList();

        public ICollection<int> Values => this.Select(pair => pair.Value).ToList();

        public int this[string key]
        {
            get => TryGetValue(key, out int value) ? value : throw new KeyNotFoundException();
            set => throw new NotSupportedException();
        }

        public void Add(string key, int value) => Add(new KeyValuePair<string, int>(key, value));

        public bool ContainsKey(string key) => TryGetValue(key, out _);

        public bool Remove(string key) => throw new NotSupportedException();

        public bool TryGetValue(string key, out int value)
        {
            foreach (KeyValuePair<string, int> pair in this)
            {
                if (string.Equals(pair.Key, key, StringComparison.Ordinal))
                {
                    value = pair.Value;
                    return true;
                }
            }

            value = 0;
            return false;
        }
    }

    /// <summary>
    /// A list of pairs that is also an <see cref="IReadOnlyDictionary{TKey, TValue}"/>, but not an
    /// <see cref="IDictionary"/>.
    /// </summary>
    private sealed class ReadOnlyPairList : Collection<KeyValuePair<string, int>>, IReadOnlyDictionary<string, int>
    {
        public IEnumerable<string> Keys => this.Select(pair => pair.Key);

        public IEnumerable<int> Values => this.Select(pair => pair.Value);

        public int this[string key] => TryGetValue(key, out int value) ? value : throw new KeyNotFoundException();

        public bool ContainsKey(string key) => TryGetValue(key, out _);

        public bool TryGetValue(string key, out int value)
        {
            foreach (KeyValuePair<string, int> pair in this)
            {
                if (string.Equals(pair.Key, key, StringComparison.Ordinal))
                {
                    value = pair.Value;
                    return true;
                }
            }

            value = 0;
            return false;
        }
    }

    /// <summary>A read-only dictionary over the given entries, in their order, which may hold a <c>null</c> key.</summary>
    internal sealed class Entries : IReadOnlyDictionary<string, object?>
    {
        private readonly KeyValuePair<string, object?>[] _entries;

        internal Entries(params (string? Key, object? Value)[] entries) =>
            _entries = entries.Select(entry => new KeyValuePair<string, object?>(entry.Key!, entry.Value)).ToArray();

        public int Count => _entries.Length;

        public IEnumerable<string> Keys => _entries.Select(entry => entry.Key);

        public IEnumerable<object?> Values => _entries.Select(entry => entry.Value);

        public object? this[string key] => TryGetValue(key, out object? value) ? value : throw new KeyNotFoundException();

        public bool ContainsKey(string key) => TryGetValue(key, out _);

        public bool TryGetValue(string key, out object? value)
        {
            foreach (KeyValuePair<string, object?> entry in _entries)
            {
                if (string.Equals(entry.Key, key, StringComparison.Ordinal))
                {
                    value = entry.Value;
                    return true;
                }
            }

            value = null;
            return false;
        }

        public IEnumerator<KeyValuePair<string, object?>> GetEnumerator() =>
            ((IEnumerable<KeyValuePair<string, object?>>)_entries).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
