// Copyright 2026 Confluent Inc.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>Create</c>, the .NET form of every constructor of the real clients (Java's <c>KafkaProducer(Map)</c> /
/// <c>KafkaConsumer(Map)</c> and <c>(Map, serde, serde)</c>): a given serde wins and is never configured; a null one
/// is built once from the function its configuration key holds, then configured, the key's before the value's; what
/// fails before Java's constructor <c>try</c> is bare and runs no function, and what fails inside it is Java's "Failed
/// to construct kafka …" around the cause, with the serdes Java's constructor would hold disposed. Translates Java's
/// <c>KafkaProducerTest.testConstructorWithSerializers</c>, <c>testNoSerializerProvided</c>,
/// <c>testSerializerClose</c> and <c>KafkaConsumerTest.testConstructorClose</c> for the configured route.
/// </summary>
public sealed class PublicClientCreateTests
{
    private const string SyncProducer = "KafkaProducer";

    private const string SyncConsumer = "KafkaConsumer";

    private const string ClassicUnsupported =
        "Classic group protocol is not yet supported in this client; set group.protocol=consumer (KIP-848).";

    public static TheoryData<string> Flavors => new TheoryData<string> { SyncProducer, SyncConsumer };

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WithOnlyTheConfiguration_BuildsEachSerdeOnce_AndConfiguresIt_KeyThenValue(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Probe key = NewProbe(flavor, "key", log);
        Probe value = NewProbe(flavor, "value", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, key);
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, value);

        object client = await Create(flavor, configs);

        Assert.Equal(
            new[] { "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)" },
            log.ToArray());
        IReadOnlyDictionary<string, object?> keyConfigs = Assert.Single(key.Configured);
        IReadOnlyDictionary<string, object?> valueConfigs = Assert.Single(value.Configured);
        AssertACopyOf(configs, keyConfigs);
        AssertACopyOf(configs, valueConfigs);
        Assert.NotSame(keyConfigs, valueConfigs);

        await Close(client);

        Assert.Equal(
            new[]
            {
                "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)",
                "key Dispose", "value Dispose",
            },
            log.ToArray());
        await Close(client);
        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WithBothSerdesGiven_UsesThem_CallsNoFunction_AndConfiguresNothing(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Probe key = NewProbe(flavor, "key", log);
        Probe value = NewProbe(flavor, "value", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "configured key", log, NewProbe(flavor, "configured key", log));
        configs[ValueConfig(flavor)] = "not a function, and never read";

        object client = await Create(flavor, configs, key, value);

        Assert.Empty(log);
        await Close(client);
        Assert.Equal(new[] { "key Dispose", "value Dispose" }, log.ToArray());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WithBothSerdesGiven_NeedsNoSerdeKey(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();

        object client = await Create(flavor, Configs(flavor), NewProbe(flavor, "key", log), NewProbe(flavor, "value", log));

        await Close(client);
        Assert.Equal(new[] { "key Dispose", "value Dispose" }, log.ToArray());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WithANullKeyOnly_BuildsTheKey_AndTakesTheGivenValueAsItIs(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Probe key = NewProbe(flavor, "key", log);
        Probe value = NewProbe(flavor, "value", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, key);
        configs[ValueConfig(flavor)] = Function(flavor, "configured value", log, NewProbe(flavor, "configured value", log));

        object client = await Create(flavor, configs, null, value);

        Assert.Equal(new[] { "key function", "key Configure(isKey: True)" }, log.ToArray());
        Assert.Empty(value.Configured);
        await Close(client);
        Assert.Equal(
            new[] { "key function", "key Configure(isKey: True)", "key Dispose", "value Dispose" }, log.ToArray());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WithANullValueOnly_BuildsTheValue_AndTakesTheGivenKeyAsItIs(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Probe key = NewProbe(flavor, "key", log);
        Probe value = NewProbe(flavor, "value", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "configured key", log, NewProbe(flavor, "configured key", log));
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, value);

        object client = await Create(flavor, configs, key, null);

        Assert.Equal(new[] { "value function", "value Configure(isKey: False)" }, log.ToArray());
        Assert.Empty(key.Configured);
        await Close(client);
        Assert.Equal(
            new[] { "value function", "value Configure(isKey: False)", "key Dispose", "value Dispose" }, log.ToArray());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WithANullConfiguration_ThrowsArgumentNull_BeforeAMissingSerde(string flavor)
    {
        ArgumentNullException configsOnly = await Assert.ThrowsAsync<ArgumentNullException>(() => Create(flavor, null!));
        ArgumentNullException withNulls =
            await Assert.ThrowsAsync<ArgumentNullException>(() => Create(flavor, null!, null, null));

        Assert.Equal("configs", configsOnly.ParamName);
        Assert.Equal("configs", withNulls.ParamName);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task KafkaProducerTest_testNoSerializerProvided_ANullSerdeWithoutItsKey_IsJavasConfigException_TheKeysFirst(
        string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Dictionary<string, object?> neither = Configs(flavor);
        Dictionary<string, object?> valueOnly = Configs(flavor);
        valueOnly[ValueConfig(flavor)] = Function(flavor, "value", log, NewProbe(flavor, "value", log));
        Dictionary<string, object?> keyOnly = Configs(flavor);
        keyOnly[KeyConfig(flavor)] = Function(flavor, "key", log, NewProbe(flavor, "key", log));
        Dictionary<string, object?> nullKey = Configs(flavor);
        nullKey[KeyConfig(flavor)] = null;
        nullKey[ValueConfig(flavor)] = Function(flavor, "value", log, NewProbe(flavor, "value", log));

        ConfigException fromNeither = await Assert.ThrowsAsync<ConfigException>(() => Create(flavor, neither));
        ConfigException fromValueOnly = await Assert.ThrowsAsync<ConfigException>(() => Create(flavor, valueOnly));
        ConfigException fromKeyOnly = await Assert.ThrowsAsync<ConfigException>(() => Create(flavor, keyOnly));
        ConfigException fromNullKey = await Assert.ThrowsAsync<ConfigException>(() => Create(flavor, nullKey));
        ConfigException fromAGivenKey = await Assert.ThrowsAsync<ConfigException>(
            () => Create(flavor, neither, NewProbe(flavor, "given key", log), null));
        ConfigException fromAGivenValue = await Assert.ThrowsAsync<ConfigException>(
            () => Create(flavor, neither, null, NewProbe(flavor, "given value", log)));

        Assert.Equal(MustBeNonNull(KeyConfig(flavor)), fromNeither.Message);
        Assert.Equal(MustBeNonNull(KeyConfig(flavor)), fromValueOnly.Message);
        Assert.Equal(MustBeNonNull(ValueConfig(flavor)), fromKeyOnly.Message);
        Assert.Equal(MustBeNonNull(KeyConfig(flavor)), fromNullKey.Message);
        Assert.Equal(MustBeNonNull(ValueConfig(flavor)), fromAGivenKey.Message);
        Assert.Equal(MustBeNonNull(KeyConfig(flavor)), fromAGivenValue.Message);
        Assert.Null(fromNeither.InnerException);
        Assert.Empty(log);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_RefusesAKeyThatHoldsAnythingButAFunctionOfTheSerde_Bare_AndRunsNoFunction(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Probe instance = NewProbe(flavor, "an instance", log);
        string nested = typeof(PublicClientCreateTests).FullName + "+";
        (object Value, string Text, string Type)[] kinds =
        {
            ("org.apache.kafka.common.serialization.StringSerializer", "org.apache.kafka.common.serialization.StringSerializer", "string"),
            (typeof(StringSerializer), "Confluent.Kafka.Common.Serialization.StringSerializer", "System.RuntimeType"),
            (instance, "an instance", nested + instance.GetType().Name),
            (new Func<object>(() => instance), "System.Func`1[System.Object]", "System.Func<object>"),
            (new Func<int>(() => 1), "System.Func`1[System.Int32]", "System.Func<int>"),
            (OfAnotherClient(flavor), OfAnotherClientText(flavor), OfAnotherClientType(flavor)),
        };

        foreach ((object held, string text, string type) in kinds)
        {
            Dictionary<string, object?> badKey = Configs(flavor);
            badKey[KeyConfig(flavor)] = held;
            badKey[ValueConfig(flavor)] = Function(flavor, "value", log, NewProbe(flavor, "value", log));
            Dictionary<string, object?> badValue = Configs(flavor);
            badValue[KeyConfig(flavor)] = Function(flavor, "key", log, NewProbe(flavor, "key", log));
            badValue[ValueConfig(flavor)] = held;

            ConfigException fromKey = await Assert.ThrowsAsync<ConfigException>(() => Create(flavor, badKey));
            ConfigException fromValue = await Assert.ThrowsAsync<ConfigException>(() => Create(flavor, badValue));

            Assert.Equal(NotAFunction(flavor, KeyConfig(flavor), text, type), fromKey.Message);
            Assert.Equal(NotAFunction(flavor, ValueConfig(flavor), text, type), fromValue.Message);
            Assert.Null(fromKey.InnerException);
        }

        Assert.Empty(log);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_RefusesAConfigurationValue_Bare_AndRunsNoFunction(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Dictionary<string, object?> wrongType = Configs(flavor);
        wrongType[KeyConfig(flavor)] = Function(flavor, "key", log, NewProbe(flavor, "key", log));
        wrongType[ValueConfig(flavor)] = Function(flavor, "value", log, NewProbe(flavor, "value", log));
        wrongType["request.timeout.ms"] = 1.5;
        Dictionary<string, object?> notANumber = new Dictionary<string, object?>(wrongType)
        {
            ["request.timeout.ms"] = "hello",
        };

        ConfigException fromTheBinding = await Assert.ThrowsAsync<ConfigException>(() => Create(flavor, wrongType));
        ConfigException fromTheCore = await Assert.ThrowsAsync<ConfigException>(() => Create(flavor, notANumber));

        Assert.Equal(
            "Invalid value 1.5 for configuration request.timeout.ms: Expected value to be a 32-bit integer, but it was a "
                + "System.Double",
            fromTheBinding.Message);
        Assert.Equal(
            "Invalid value hello for configuration request.timeout.ms: Not a number of type INT", fromTheCore.Message);
        Assert.Empty(log);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WhenTheKeyFunctionFails_IsJavasWrappedFailure_AndLeavesAGivenValueAlone(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        InvalidOperationException thrown = new InvalidOperationException("no key today");
        Probe value = NewProbe(flavor, "value", log);
        Dictionary<string, object?> throwing = Configs(flavor);
        throwing[KeyConfig(flavor)] = Function(flavor, "key", log, () => throw thrown);
        Dictionary<string, object?> returningNull = Configs(flavor);
        returningNull[KeyConfig(flavor)] = Function(flavor, "key", log, () => null);

        KafkaException fromThrowing = await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, throwing, null, value));
        KafkaException fromNull = await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, returningNull, null, value));

        Assert.Equal(FailedToConstruct(flavor), fromThrowing.Message);
        Assert.Same(thrown, fromThrowing.InnerException);
        Assert.Equal(FailedToConstruct(flavor), fromNull.Message);
        ConfigException cause = Assert.IsType<ConfigException>(fromNull.InnerException);
        Assert.Equal(ReturnedNull(flavor, KeyConfig(flavor)), cause.Message);
        Assert.Equal(new[] { "key function", "key function" }, log.ToArray());
        Assert.Equal(0, value.Disposals);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WhenTheValueFunctionFails_IsJavasWrappedFailure_AndDisposesTheKey_BuiltOrGiven(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        InvalidOperationException thrown = new InvalidOperationException("no value today");
        Probe builtKey = NewProbe(flavor, "built key", log);
        Probe givenKey = NewProbe(flavor, "given key", log);
        Dictionary<string, object?> throwing = Configs(flavor);
        throwing[KeyConfig(flavor)] = Function(flavor, "key", log, builtKey);
        throwing[ValueConfig(flavor)] = Function(flavor, "value", log, () => throw thrown);
        Dictionary<string, object?> returningNull = Configs(flavor);
        returningNull[ValueConfig(flavor)] = Function(flavor, "value", log, () => null);

        KafkaException fromThrowing = await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, throwing));
        KafkaException fromNull =
            await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, returningNull, givenKey, null));

        Assert.Equal(FailedToConstruct(flavor), fromThrowing.Message);
        Assert.Same(thrown, fromThrowing.InnerException);
        Assert.Equal(FailedToConstruct(flavor), fromNull.Message);
        ConfigException cause = Assert.IsType<ConfigException>(fromNull.InnerException);
        Assert.Equal(ReturnedNull(flavor, ValueConfig(flavor)), cause.Message);
        Assert.Equal(
            new[]
            {
                "key function", "built key Configure(isKey: True)", "value function", "built key Dispose",
                "value function", "given key Dispose",
            },
            log.ToArray());
        Assert.Equal(1, builtKey.Disposals);
        Assert.Equal(1, givenKey.Disposals);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WhenConfigureThrows_IsJavasWrappedFailure_AndDisposesThatSerde_AndTheKeyBeforeIt(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        InvalidOperationException keyThrown = new InvalidOperationException("the key cannot be configured");
        InvalidOperationException valueThrown = new InvalidOperationException("the value cannot be configured");
        Probe failingKey = NewProbe(flavor, "failing key", log);
        failingKey.ConfigureThrows = keyThrown;
        Probe key = NewProbe(flavor, "key", log);
        Probe failingValue = NewProbe(flavor, "failing value", log);
        failingValue.ConfigureThrows = valueThrown;
        Dictionary<string, object?> keyFails = Configs(flavor);
        keyFails[KeyConfig(flavor)] = Function(flavor, "key", log, failingKey);
        keyFails[ValueConfig(flavor)] = Function(flavor, "value", log, NewProbe(flavor, "never built", log));
        Dictionary<string, object?> valueFails = Configs(flavor);
        valueFails[KeyConfig(flavor)] = Function(flavor, "key", log, key);
        valueFails[ValueConfig(flavor)] = Function(flavor, "value", log, failingValue);

        KafkaException fromKey = await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, keyFails));
        KafkaException fromValue = await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, valueFails));

        Assert.Equal(FailedToConstruct(flavor), fromKey.Message);
        Assert.Same(keyThrown, fromKey.InnerException);
        Assert.Equal(FailedToConstruct(flavor), fromValue.Message);
        Assert.Same(valueThrown, fromValue.InnerException);
        Assert.Equal(
            new[]
            {
                "key function", "failing key Configure(isKey: True)", "failing key Dispose",
                "key function", "key Configure(isKey: True)", "value function", "failing value Configure(isKey: False)",
                "failing value Dispose", "key Dispose",
            },
            log.ToArray());
        Assert.Equal(1, failingKey.Disposals);
        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, failingValue.Disposals);
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WhenABuiltSerdeIsOneTheClientRefuses_IsJavasWrappedFailure_AroundTheRefusal(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        object closing = Closing(flavor);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, () => closing);
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, NewProbe(flavor, "never built", log));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, configs));
        IllegalArgumentException given =
            await Assert.ThrowsAsync<IllegalArgumentException>(() => Create(flavor, configs, closing, null));

        Assert.Equal(FailedToConstruct(flavor), failure.Message);
        IllegalArgumentException cause = Assert.IsType<IllegalArgumentException>(failure.InnerException);
        Assert.Equal(ClosingRefused(flavor, closing), cause.Message);
        Assert.Equal(ClosingRefused(flavor, closing), given.Message);
        Assert.Equal(new[] { "key function" }, log.ToArray());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_BuildsASerdeThatHasNoConfigure_AndNoDispose_WithoutCallingEither(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, () => Plain(flavor));
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, () => Plain(flavor));

        object client = await Create(flavor, configs);
        await Close(client);

        Assert.Equal(new[] { "key function", "value function" }, log.ToArray());
    }

    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WhenTheCoreFailsToBuildTheClient_DisposesTheSerdesItBuilt_KeyThenValue(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Probe key = NewProbe(flavor, "key", log);
        Probe value = NewProbe(flavor, "value", log);
        Dictionary<string, object?> configs = FailingInTheCore(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, key);
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, value);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, configs));

        Assert.Equal(FailedToConstruct(flavor), failure.Message);
        Assert.Equal(
            new[]
            {
                "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)",
                "key Dispose", "value Dispose",
            },
            log.ToArray());
    }

    [Fact]
    public void KafkaConsumerCreate_WithTheClassicProtocol_IsBare_DisposesABuiltDeserializer_AndLeavesAGivenOne()
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Probe key = NewProbe(SyncConsumer, "built key", log);
        Probe value = NewProbe(SyncConsumer, "given value", log);
        Dictionary<string, object?> configs = Configs(SyncConsumer);
        configs["group.protocol"] = "classic";
        configs[KeyConfig(SyncConsumer)] = Function(SyncConsumer, "key", log, key);

        UnsupportedOperationException failure = Assert.Throws<UnsupportedOperationException>(
            () => KafkaConsumer<string, string>.Create(configs, null, (IDeserializer<string>)value));

        Assert.Equal(ClassicUnsupported, failure.Message);
        Assert.Equal(new[] { "key function", "built key Configure(isKey: True)", "built key Dispose" }, log.ToArray());
        Assert.Equal(0, value.Disposals);
    }

    [Fact]
    public void KafkaProducerCreate_TakesAFunctionDeclaredToReturnTheClass_AndConfiguresItsEncodingFromTheConfiguration()
    {
        StringSerializer? key = null;
        StringSerializer? value = null;
        Dictionary<string, object?> configs = Configs(SyncProducer);
        configs["key.serializer"] = new Func<StringSerializer>(() => key = new StringSerializer());
        configs["value.serializer"] = new Func<StringSerializer>(() => value = new StringSerializer());
        configs["key.serializer.encoding"] = "UTF-16";
        configs["serializer.encoding"] = "ISO-8859-1";

        using (TestTimeout.Disposal(KafkaProducer<string, string>.Create(configs)))
        {
            Assert.Equal(Encoding.BigEndianUnicode.GetPreamble().Length + 2, key!.Serialize("t", "k")!.Value.Length);
            Assert.Equal(new byte[] { 0xE9 }, value!.Serialize("t", "é")!.Value.ToArray());
        }
    }

    [Fact]
    public void KafkaConsumerCreate_TakesAFunctionDeclaredToReturnTheClass_AndConfiguresItsEncodingFromTheConfiguration()
    {
        StringDeserializer? key = null;
        StringDeserializer? value = null;
        Dictionary<string, object?> configs = Configs(SyncConsumer);
        configs["key.deserializer"] = new Func<StringDeserializer>(() => key = new StringDeserializer());
        configs["value.deserializer"] = new Func<StringDeserializer>(() => value = new StringDeserializer());
        configs["key.deserializer.encoding"] = "UTF-16BE";
        configs["deserializer.encoding"] = "ISO-8859-1";

        using (TestTimeout.Disposal(KafkaConsumer<string, string>.Create(configs)))
        {
            Assert.Equal("k", key!.Deserialize("t", new byte[] { 0x00, 0x6B }));
            Assert.Equal("é", value!.Deserialize("t", new byte[] { 0xE9 }));
        }
    }

    private static bool IsProducer(string flavor) => flavor == SyncProducer;

    private static string KeyConfig(string flavor) => IsProducer(flavor) ? "key.serializer" : "key.deserializer";

    private static string ValueConfig(string flavor) => IsProducer(flavor) ? "value.serializer" : "value.deserializer";

    private static string FailedToConstruct(string flavor) =>
        IsProducer(flavor) ? "Failed to construct kafka producer" : "Failed to construct kafka consumer";

    private static string MustBeNonNull(string config) =>
        "Invalid value null for configuration " + config + ": must be non-null.";

    private static string SerdeInterface(string flavor) => flavor switch
    {
        SyncProducer => "Confluent.Kafka.Common.Serialization.ISerializer<string>",
        _ => "Confluent.Kafka.Common.Serialization.IDeserializer<string>",
    };

    private static string NotAFunction(string flavor, string config, string text, string type) =>
        "Invalid value " + text + " for configuration " + config + ": Expected value to be a System.Func<"
            + SerdeInterface(flavor) + ">, but it was a " + type;

    private static string ReturnedNull(string flavor, string config) =>
        "Invalid value null for configuration " + config + ": Expected the function to return a " + SerdeInterface(flavor)
            + ", but it returned null";

    private static string ClosingRefused(string flavor, object closing) =>
        closing.GetType().ToString() + " declares Close() but does not implement IDisposable, so the "
            + (IsProducer(flavor) ? "producer" : "consumer") + " would never call it.";

    // A function of the right shape for a client of another kind.
    private static object OfAnotherClient(string flavor) => flavor switch
    {
        SyncProducer => new Func<IDeserializer<string>>(() => new StringDeserializer()),
        _ => new Func<ISerializer<string>>(() => new StringSerializer()),
    };

    private static string OfAnotherClientText(string flavor) => flavor switch
    {
        SyncProducer => "System.Func`1[Confluent.Kafka.Common.Serialization.IDeserializer`1[System.String]]",
        _ => "System.Func`1[Confluent.Kafka.Common.Serialization.ISerializer`1[System.String]]",
    };

    private static string OfAnotherClientType(string flavor) => flavor switch
    {
        SyncProducer => "System.Func<Confluent.Kafka.Common.Serialization.IDeserializer<string>>",
        _ => "System.Func<Confluent.Kafka.Common.Serialization.ISerializer<string>>",
    };

    private static Dictionary<string, object?> Configs(string flavor) =>
        IsProducer(flavor)
            ? new Dictionary<string, object?> { ["bootstrap.servers"] = "localhost:1" }
            : new Dictionary<string, object?>
            {
                ["bootstrap.servers"] = "localhost:1",
                ["group.protocol"] = "consumer",
                ["group.id"] = "create-group",
                ["enable.auto.commit"] = "false",
            };

    // A configuration the core accepts and then cannot build a client from, which Java reports from inside its try.
    private static Dictionary<string, object?> FailingInTheCore(string flavor)
    {
        Dictionary<string, object?> configs = Configs(flavor);
        if (IsProducer(flavor))
        {
            configs["bootstrap.servers"] = "unresolvable.invalid:9092";
        }
        else
        {
            configs["group.id"] = string.Empty;
        }

        return configs;
    }

    private static void AssertACopyOf(
        IReadOnlyDictionary<string, object?> expected, IReadOnlyDictionary<string, object?> actual)
    {
        Assert.NotSame(expected, actual);
        Assert.Equal(expected.Count, actual.Count);
        foreach (KeyValuePair<string, object?> entry in expected)
        {
            Assert.Same(entry.Value, actual[entry.Key]);
        }
    }

    private static Probe NewProbe(string flavor, string name, ConcurrentQueue<string> log) => flavor switch
    {
        SyncProducer => new SerializerProbe(name, log),
        _ => new DeserializerProbe(name, log),
    };

    private static object Plain(string flavor) => flavor switch
    {
        SyncProducer => new PlainSerializer(),
        _ => new PlainDeserializer(),
    };

    private static object Closing(string flavor) => flavor switch
    {
        SyncProducer => new ClosingSerializer(),
        _ => new ClosingDeserializer(),
    };

    private static object Function(string flavor, string name, ConcurrentQueue<string> log, object serde) =>
        Function(flavor, name, log, () => serde);

    // The function a configuration key holds, declared as the client reads it; it logs each call.
    private static object Function(string flavor, string name, ConcurrentQueue<string> log, Func<object?> make)
    {
        object? Logged()
        {
            log.Enqueue(name + " function");
            return make();
        }

        return flavor switch
        {
            SyncProducer => new Func<ISerializer<string>>(() => (ISerializer<string>)Logged()!),
            _ => new Func<IDeserializer<string>>(() => (IDeserializer<string>)Logged()!),
        };
    }

    private static Task<object> Create(string flavor, IReadOnlyDictionary<string, object?> configs)
    {
        try
        {
            return flavor switch
            {
                SyncProducer => Task.FromResult<object>(KafkaProducer<string, string>.Create(configs)),
                _ => Task.FromResult<object>(KafkaConsumer<string, string>.Create(configs)),
            };
        }
        catch (Exception failure)
        {
            return Task.FromException<object>(failure);
        }
    }

    private static Task<object> Create(
        string flavor, IReadOnlyDictionary<string, object?> configs, object? key, object? value)
    {
        try
        {
            return flavor switch
            {
                SyncProducer => Task.FromResult<object>(
                    KafkaProducer<string, string>.Create(configs, (ISerializer<string>?)key, (ISerializer<string>?)value)),
                _ => Task.FromResult<object>(
                    KafkaConsumer<string, string>.Create(configs, (IDeserializer<string>?)key, (IDeserializer<string>?)value)),
            };
        }
        catch (Exception failure)
        {
            return Task.FromException<object>(failure);
        }
    }

    private static Task Close(object client)
    {
        TestTimeout.Dispose((IDisposable)client);
        return Task.CompletedTask;
    }

    /// <summary>A serde that logs its <c>Configure</c> and its disposal, under its name.</summary>
    private abstract class Probe
    {
        private readonly string _name;

        private readonly ConcurrentQueue<string> _log;

        private readonly ConcurrentQueue<IReadOnlyDictionary<string, object?>> _configured =
            new ConcurrentQueue<IReadOnlyDictionary<string, object?>>();

        private int _disposals;

        protected Probe(string name, ConcurrentQueue<string> log)
        {
            _name = name;
            _log = log;
        }

        internal Exception? ConfigureThrows { get; set; }

        internal IReadOnlyCollection<IReadOnlyDictionary<string, object?>> Configured => _configured;

        internal int Disposals => System.Threading.Volatile.Read(ref _disposals);

        public override string ToString() => _name;

        protected void OnConfigure(IReadOnlyDictionary<string, object?> configs, bool isKey)
        {
            _log.Enqueue(_name + " Configure(isKey: " + isKey + ")");
            _configured.Enqueue(configs);
            if (ConfigureThrows is not null)
            {
                throw ConfigureThrows;
            }
        }

        protected void OnDispose()
        {
            System.Threading.Interlocked.Increment(ref _disposals);
            _log.Enqueue(_name + " Dispose");
        }
    }

    private sealed class SerializerProbe : Probe, ISerializerWithConfigure<string>, IDisposable
    {
        internal SerializerProbe(string name, ConcurrentQueue<string> log)
            : base(name, log)
        {
        }

        public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey) => OnConfigure(configs, isKey);

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => null;

        public void Dispose() => OnDispose();
    }

    private sealed class DeserializerProbe : Probe, IDeserializerWithConfigure<string>, IDisposable
    {
        internal DeserializerProbe(string name, ConcurrentQueue<string> log)
            : base(name, log)
        {
        }

        public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey) => OnConfigure(configs, isKey);

        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;

        public void Dispose() => OnDispose();
    }

    private sealed class PlainSerializer : ISerializer<string>
    {
        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => null;
    }

    private sealed class PlainDeserializer : IDeserializer<string>
    {
        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;
    }

    private sealed class ClosingSerializer : ISerializer<string>
    {
        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => null;

        public void Close()
        {
        }
    }

    private sealed class ClosingDeserializer : IDeserializer<string>
    {
        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;

        public void Close()
        {
        }
    }
}
