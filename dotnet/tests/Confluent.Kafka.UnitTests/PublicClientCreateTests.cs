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
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
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

    private const string AsyncProducer = "AsyncKafkaProducer";

    private const string AsyncConsumer = "AsyncKafkaConsumer";

    private const string ClassicUnsupported =
        "Classic group protocol is not yet supported in this client; set group.protocol=consumer (KIP-848).";

    public static TheoryData<string> Flavors =>
        new TheoryData<string> { SyncProducer, SyncConsumer, AsyncProducer, AsyncConsumer };

    public static TheoryData<string> AsyncFlavors => new TheoryData<string> { AsyncProducer, AsyncConsumer };

    [Theory]
    [InlineData(typeof(KafkaProducer<,>), typeof(KafkaProducer<,>))]
    [InlineData(typeof(KafkaConsumer<,>), typeof(KafkaConsumer<,>))]
    [InlineData(typeof(AsyncKafkaProducer<,>), typeof(Task<>))]
    [InlineData(typeof(AsyncKafkaConsumer<,>), typeof(Task<>))]
    public void ARealClient_HasNoPublicConstructor_AndIsBuiltByItsTwoCreateOverloads(Type client, Type returned)
    {
        Assert.Empty(client.GetConstructors(BindingFlags.Public | BindingFlags.Instance));

        MethodInfo[] factories = client
            .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Where(method => method.Name == "Create")
            .OrderBy(method => method.GetParameters().Length)
            .ToArray();
        bool awaited = returned == typeof(Task<>);
        Assert.Equal(2, factories.Length);
        Assert.All(factories, factory => Assert.Equal(returned, factory.ReturnType.GetGenericTypeDefinition()));
        Assert.Equal(
            awaited ? new[] { "configs", "cancellationToken" } : new[] { "configs" },
            factories[0].GetParameters().Select(parameter => parameter.Name));
        string serde = client.Name.IndexOf("Producer", StringComparison.Ordinal) >= 0 ? "Serializer" : "Deserializer";
        string[] withSerdes = { "configs", "key" + serde, "value" + serde };
        Assert.Equal(
            awaited ? withSerdes.Concat(new[] { "cancellationToken" }) : withSerdes,
            factories[1].GetParameters().Select(parameter => parameter.Name));
    }

    [Theory]
    [InlineData(typeof(MockProducer<,>))]
    [InlineData(typeof(AsyncMockProducer<,>))]
    [InlineData(typeof(MockConsumer<,>))]
    [InlineData(typeof(AsyncMockConsumer<,>))]
    public void AMock_KeepsItsPublicConstructors_AndHasNoCreate(Type mock)
    {
        Assert.NotEmpty(mock.GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        Assert.DoesNotContain(
            mock.GetMethods(BindingFlags.Public | BindingFlags.Static), method => method.Name == "Create");
    }

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

    // Java's appendSerializerToConfig / appendDeserializerToConfig copies the map once, at the call, and the
    // constructor reads only that copy.
    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_ReadsTheConfigurationOnce_AtTheCall_WhateverAFunctionDoesToTheCallersDictionary(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Probe key = NewProbe(flavor, "key", log);
        Probe value = NewProbe(flavor, "value", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs["client.id"] = "as-called";
        configs[KeyConfig(flavor)] = Function(
            flavor,
            "key",
            log,
            () =>
            {
                // A value the client refuses when it reads it, a changed entry, a new one, a removed one, and another
                // function for the value.
                configs["request.timeout.ms"] = "not a number";
                configs["client.id"] = "changed";
                configs["added.by.the.function"] = "late";
                configs.Remove("bootstrap.servers");
                configs[ValueConfig(flavor)] = Function(flavor, "late value", log, NewProbe(flavor, "late value", log));
                return key;
            });
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, value);
        Dictionary<string, object?> asCalled = new Dictionary<string, object?>(configs);

        // The client is built: neither the binding's checks nor the core read the dictionary again.
        object client = await Create(flavor, configs);

        Assert.Equal(
            new[] { "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)" },
            log.ToArray());
        IReadOnlyDictionary<string, object?> keyConfigs = Assert.Single(key.Configured);
        IReadOnlyDictionary<string, object?> valueConfigs = Assert.Single(value.Configured);
        AssertACopyOf(asCalled, keyConfigs);
        AssertACopyOf(asCalled, valueConfigs);
        Assert.NotSame(keyConfigs, valueConfigs);
        Assert.Equal("changed", configs["client.id"]);

        await Close(client);
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
            (OfTheOtherForm(flavor), OfTheOtherFormText(flavor), OfTheOtherFormType(flavor)),
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

    // Java's KafkaProducer holds the key serializer, given or built, before it builds the value serializer, and its
    // catch closes it. Java's consumer builds both inside one new Deserializers(…) and holds neither when the value
    // fails: a given key stays its owner's. A key the client built has no other owner and is disposed on all four.
    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WhenTheValueFunctionFails_IsJavasWrappedFailure_DisposesABuiltKey_AndAGivenOneOnlyOnAProducer(
        string flavor)
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
        string[] builtKeyFailure =
        {
            "key function", "built key Configure(isKey: True)", "value function", "built key Dispose", "value function",
        };
        Assert.Equal(
            IsProducer(flavor) ? builtKeyFailure.Concat(new[] { "given key Dispose" }) : builtKeyFailure,
            log.ToArray());
        Assert.Equal(1, builtKey.Disposals);
        Assert.Equal(IsProducer(flavor) ? 1 : 0, givenKey.Disposals);
    }

    // The same split when the value serde is built and then fails: it is disposed first, then the key.
    [Theory]
    [MemberData(nameof(Flavors))]
    public async Task Create_WhenTheValuesConfigureThrows_DisposesTheValue_AndAGivenKeyOnlyOnAProducer(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        InvalidOperationException thrown = new InvalidOperationException("the value cannot be configured");
        Probe givenKey = NewProbe(flavor, "given key", log);
        Probe failingValue = NewProbe(flavor, "failing value", log);
        failingValue.ConfigureThrows = thrown;
        Dictionary<string, object?> configs = Configs(flavor);
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, failingValue);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, configs, givenKey, null));

        Assert.Equal(FailedToConstruct(flavor), failure.Message);
        Assert.Same(thrown, failure.InnerException);
        string[] valueFailure = { "value function", "failing value Configure(isKey: False)", "failing value Dispose" };
        Assert.Equal(
            IsProducer(flavor) ? valueFailure.Concat(new[] { "given key Dispose" }) : valueFailure,
            log.ToArray());
        Assert.Equal(1, failingValue.Disposals);
        Assert.Equal(IsProducer(flavor) ? 1 : 0, givenKey.Disposals);
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

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_ThrowsItsChecksFromTheCall_AndReportsTheRestThroughItsTask(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Dictionary<string, object?> failing = FailingInTheCore(flavor);
        failing[KeyConfig(flavor)] = Function(flavor, "key", log, NewProbe(flavor, "key", log));
        failing[ValueConfig(flavor)] = Function(flavor, "value", log, NewProbe(flavor, "value", log));
        Dictionary<string, object?> notANumber = new Dictionary<string, object?>(failing)
        {
            ["request.timeout.ms"] = "hello",
        };

        Assert.Throws<ArgumentNullException>(() => { _ = CreateTask(flavor, null!, CancellationToken.None); });
        ConfigException missing =
            Assert.Throws<ConfigException>(() => { _ = CreateTask(flavor, Configs(flavor), CancellationToken.None); });
        ConfigException refused =
            Assert.Throws<ConfigException>(() => { _ = CreateTask(flavor, notANumber, CancellationToken.None); });
        Assert.Empty(log);
        Task created = CreateTask(flavor, failing, CancellationToken.None);
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => created);

        Assert.Equal(MustBeNonNull(KeyConfig(flavor)), missing.Message);
        Assert.Equal(
            "Invalid value hello for configuration request.timeout.ms: Not a number of type INT", refused.Message);
        Assert.Equal(FailedToConstruct(flavor), failure.Message);
        Assert.Equal(
            new[]
            {
                "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)",
                "key Dispose", "value Dispose",
            },
            log.ToArray());
    }

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_ReturnsWhileAConfigureIsPending_AndCompletesOnlyOnceBothAreConfigured(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        TaskCompletionSource<bool> keyGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> valueGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> valueStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        AsyncProbe key = (AsyncProbe)NewProbe(flavor, "key", log);
        key.ConfigureWaits = _ => keyGate.Task;
        AsyncProbe value = (AsyncProbe)NewProbe(flavor, "value", log);
        value.ConfigureWaits = _ =>
        {
            valueStarted.TrySetResult(true);
            return valueGate.Task;
        };
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, key);
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, value);

        // The call comes back on this thread while the key's Configure waits: nothing waits for it on a thread.
        Task<object> created = Create(flavor, configs);

        Assert.False(created.IsCompleted);
        Assert.Equal(new[] { "key function", "key Configure(isKey: True)" }, log.ToArray());

        keyGate.SetResult(true);
        await TestTimeout.Run(() => valueStarted.Task, TimeSpan.FromSeconds(30));

        Assert.False(created.IsCompleted);
        Assert.Equal(
            new[] { "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)" },
            log.ToArray());

        valueGate.SetResult(true);
        object? client = null;
        await TestTimeout.Run(async () => client = await created, TimeSpan.FromSeconds(30));
        await Close(client!);

        Assert.Equal(
            new[]
            {
                "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)",
                "key Dispose", "value Dispose",
            },
            log.ToArray());
    }

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_ReadsTheConfigurationOnce_AtTheCall_WhateverChangesWhileTheKeysConfigureIsPending(
        string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        TaskCompletionSource<bool> keyGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        AsyncProbe key = (AsyncProbe)NewProbe(flavor, "key", log);
        key.ConfigureWaits = _ => keyGate.Task;
        Probe value = NewProbe(flavor, "value", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs["client.id"] = "as-called";
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, key);
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, value);
        Dictionary<string, object?> asCalled = new Dictionary<string, object?>(configs);

        Task<object> created = Create(flavor, configs);

        Assert.False(created.IsCompleted);
        Assert.Equal(new[] { "key function", "key Configure(isKey: True)" }, log.ToArray());

        // The caller has its dictionary back, and reuses it while the key's Configure is pending: a value the client
        // refuses when it reads it, a changed entry, a new one, a removed one, and another function for the value.
        configs["request.timeout.ms"] = "not a number";
        configs["client.id"] = "changed";
        configs["added.after.the.call"] = "late";
        configs.Remove("bootstrap.servers");
        configs[ValueConfig(flavor)] = Function(flavor, "late value", log, NewProbe(flavor, "late value", log));

        // The key serde owns the map it was given, and changes it too.
        IReadOnlyDictionary<string, object?> keyConfigs = Assert.Single(key.Configured);
        AssertACopyOf(asCalled, keyConfigs);
        IDictionary<string, object?> keysOwn = Assert.IsAssignableFrom<IDictionary<string, object?>>(keyConfigs);
        keysOwn["client.id"] = "the key serde's";
        keysOwn["added.by.the.key.serde"] = "its own";
        keysOwn.Remove("bootstrap.servers");

        keyGate.SetResult(true);
        object? client = null;
        await TestTimeout.Run(async () => client = await created, TimeSpan.FromSeconds(30));

        Assert.Equal(
            new[] { "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)" },
            log.ToArray());
        IReadOnlyDictionary<string, object?> valueConfigs = Assert.Single(value.Configured);
        AssertACopyOf(asCalled, valueConfigs);
        Assert.NotSame(keyConfigs, valueConfigs);
        Assert.False(configs.ContainsKey("added.by.the.key.serde"));
        Assert.Equal("changed", configs["client.id"]);

        // And the value serde's map is its own as well.
        IDictionary<string, object?> valuesOwn = Assert.IsAssignableFrom<IDictionary<string, object?>>(valueConfigs);
        valuesOwn["added.by.the.value.serde"] = "its own";
        Assert.False(keyConfigs.ContainsKey("added.by.the.value.serde"));
        Assert.False(configs.ContainsKey("added.by.the.value.serde"));

        await Close(client!);
    }

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_PassesItsTokenToConfigure(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        using CancellationTokenSource source = new CancellationTokenSource();
        List<CancellationToken> seen = new List<CancellationToken>();
        AsyncProbe key = (AsyncProbe)NewProbe(flavor, "key", log);
        key.ConfigureWaits = token => Record(seen, token);
        AsyncProbe value = (AsyncProbe)NewProbe(flavor, "value", log);
        value.ConfigureWaits = token => Record(seen, token);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, key);
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, value);

        object client = await Create(flavor, configs, source.Token);
        await Close(client);

        Assert.Equal(new[] { source.Token, source.Token }, seen);
    }

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_WithATokenAlreadyCancelled_IsCancelledAfterItsChecks_AndRunsNoFunction(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        using CancellationTokenSource source = new CancellationTokenSource();
        source.Cancel();
        Probe value = NewProbe(flavor, "given value", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, NewProbe(flavor, "key", log));

        OperationCanceledException cancelled = Assert.ThrowsAny<OperationCanceledException>(
            () => { _ = CreateTask(flavor, configs, null, value, source.Token); });
        ConfigException missing = await Assert.ThrowsAsync<ConfigException>(
            () => Create(flavor, Configs(flavor), null, value, source.Token));

        Assert.Equal(source.Token, cancelled.CancellationToken);
        Assert.Equal(MustBeNonNull(KeyConfig(flavor)), missing.Message);
        Assert.Empty(log);
    }

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_CancelledWhileTheKeyIsConfigured_IsCancelled_DisposesTheBuiltKey_AndLeavesAGivenValue(
        string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        using CancellationTokenSource source = new CancellationTokenSource();
        TaskCompletionSource<bool> gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        AsyncProbe key = (AsyncProbe)NewProbe(flavor, "key", log);
        key.ConfigureWaits = token => ThrowWhenCancelled(gate.Task, token);
        Probe value = NewProbe(flavor, "given value", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, key);

        Task<object> created = Create(flavor, configs, null, value, source.Token);
        source.Cancel();
        gate.SetResult(true);
        OperationCanceledException cancelled =
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => created);

        Assert.Equal(source.Token, cancelled.CancellationToken);
        Assert.Equal(new[] { "key function", "key Configure(isKey: True)", "key Dispose" }, log.ToArray());
        Assert.Equal(0, value.Disposals);
    }

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_CancelledWhileTheValueIsConfigured_IsCancelled_DisposesWhatItBuilt_AndLeavesAGivenKey(
        string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        using CancellationTokenSource source = new CancellationTokenSource();
        AsyncProbe value = (AsyncProbe)NewProbe(flavor, "value", log);
        value.ConfigureWaits = token =>
        {
            source.Cancel();
            return ThrowWhenCancelled(Task.CompletedTask, token);
        };
        Probe givenKey = NewProbe(flavor, "given key", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, NewProbe(flavor, "built key", log));
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, value);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Create(flavor, configs, givenKey, null, source.Token));

        Assert.Equal(new[] { "value function", "value Configure(isKey: False)", "value Dispose" }, log.ToArray());
        Assert.Equal(0, givenKey.Disposals);

        using CancellationTokenSource second = new CancellationTokenSource();
        ConcurrentQueue<string> secondLog = new ConcurrentQueue<string>();
        AsyncProbe secondValue = (AsyncProbe)NewProbe(flavor, "value", secondLog);
        secondValue.ConfigureWaits = token =>
        {
            second.Cancel();
            return ThrowWhenCancelled(Task.CompletedTask, token);
        };
        Dictionary<string, object?> both = Configs(flavor);
        both[KeyConfig(flavor)] = Function(flavor, "key", secondLog, NewProbe(flavor, "key", secondLog));
        both[ValueConfig(flavor)] = Function(flavor, "value", secondLog, secondValue);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Create(flavor, both, second.Token));

        Assert.Equal(
            new[]
            {
                "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)",
                "value Dispose", "key Dispose",
            },
            secondLog.ToArray());
    }

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_CancelledOnceBothAreConfigured_IsCancelledBeforeTheClientIsBuilt_AndDisposesWhatItBuilt(
        string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        using CancellationTokenSource source = new CancellationTokenSource();
        AsyncProbe value = (AsyncProbe)NewProbe(flavor, "value", log);
        value.ConfigureWaits = _ =>
        {
            // The Configure itself succeeds; the token is read once more before the core builds the client.
            source.Cancel();
            return Task.CompletedTask;
        };
        Probe givenKey = NewProbe(flavor, "given key", log);
        Dictionary<string, object?> configs = Configs(flavor);
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, value);

        OperationCanceledException cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Create(flavor, configs, givenKey, null, source.Token));

        Assert.Equal(source.Token, cancelled.CancellationToken);
        Assert.Equal(new[] { "value function", "value Configure(isKey: False)", "value Dispose" }, log.ToArray());
        Assert.Equal(0, givenKey.Disposals);
    }

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_WhenConfigureIsCancelledByAnotherToken_IsJavasWrappedFailure(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        OperationCanceledException thrown = new OperationCanceledException("the registry call was cancelled");
        Probe key = NewProbe(flavor, "key", log);
        key.ConfigureThrows = thrown;
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, key);
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, NewProbe(flavor, "never built", log));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => Create(flavor, configs));

        Assert.Equal(FailedToConstruct(flavor), failure.Message);
        Assert.Same(thrown, failure.InnerException);
        Assert.Equal(new[] { "key function", "key Configure(isKey: True)", "key Dispose" }, log.ToArray());
    }

    [Theory]
    [MemberData(nameof(AsyncFlavors))]
    public async Task AsyncCreate_BuiltSerdesAreDisposedByTheBlockingDisposeToo(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Dictionary<string, object?> configs = Configs(flavor);
        configs[KeyConfig(flavor)] = Function(flavor, "key", log, NewProbe(flavor, "key", log));
        configs[ValueConfig(flavor)] = Function(flavor, "value", log, NewProbe(flavor, "value", log));

        object client = await Create(flavor, configs);
        TestTimeout.Dispose((IDisposable)client);

        Assert.Equal(
            new[]
            {
                "key function", "key Configure(isKey: True)", "value function", "value Configure(isKey: False)",
                "key Dispose", "value Dispose",
            },
            log.ToArray());
    }

    [Fact]
    public async Task AsyncKafkaProducerCreate_TakesAFunctionDeclaredToReturnTheClass_AndConfiguresItsEncodingFromTheConfiguration()
    {
        AsyncStringSerializer? key = null;
        AsyncStringSerializer? value = null;
        Dictionary<string, object?> configs = Configs(AsyncProducer);
        configs["key.serializer"] = new Func<AsyncStringSerializer>(() => key = new AsyncStringSerializer());
        configs["value.serializer"] = new Func<AsyncStringSerializer>(() => value = new AsyncStringSerializer());
        configs["key.serializer.encoding"] = "UTF-16";
        configs["serializer.encoding"] = "ISO-8859-1";

        await using (TestTimeout.AsyncDisposal(await AsyncKafkaProducer<string, string>.Create(configs)))
        {
            ReadOnlyMemory<byte>? keyBytes = await key!.Serialize("t", new Headers(), "k", CancellationToken.None);
            ReadOnlyMemory<byte>? valueBytes = await value!.Serialize("t", new Headers(), "é", CancellationToken.None);
            Assert.Equal(Encoding.BigEndianUnicode.GetPreamble().Length + 2, keyBytes!.Value.Length);
            Assert.Equal(new byte[] { 0xE9 }, valueBytes!.Value.ToArray());
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumerCreate_TakesAFunctionDeclaredToReturnTheClass_AndConfiguresItsEncodingFromTheConfiguration()
    {
        AsyncStringDeserializer? key = null;
        AsyncStringDeserializer? value = null;
        Dictionary<string, object?> configs = Configs(AsyncConsumer);
        configs["key.deserializer"] = new Func<AsyncStringDeserializer>(() => key = new AsyncStringDeserializer());
        configs["value.deserializer"] = new Func<AsyncStringDeserializer>(() => value = new AsyncStringDeserializer());
        configs["key.deserializer.encoding"] = "UTF-16BE";
        configs["deserializer.encoding"] = "ISO-8859-1";

        await using (TestTimeout.AsyncDisposal(await AsyncKafkaConsumer<string, string>.Create(configs)))
        {
            Assert.Equal("k", await key!.Deserialize("t", new Headers(), new byte[] { 0x00, 0x6B }, CancellationToken.None));
            Assert.Equal("é", await value!.Deserialize("t", new Headers(), new byte[] { 0xE9 }, CancellationToken.None));
        }
    }

    [Fact]
    public async Task AsyncKafkaConsumerCreate_WithTheClassicProtocol_IsBare_DisposesABuiltDeserializer_AndLeavesAGivenOne()
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        Probe value = NewProbe(AsyncConsumer, "given value", log);
        Dictionary<string, object?> configs = Configs(AsyncConsumer);
        configs["group.protocol"] = "classic";
        configs[KeyConfig(AsyncConsumer)] = Function(AsyncConsumer, "key", log, NewProbe(AsyncConsumer, "built key", log));

        UnsupportedOperationException failure = await Assert.ThrowsAsync<UnsupportedOperationException>(
            () => Create(AsyncConsumer, configs, null, value));

        Assert.Equal(ClassicUnsupported, failure.Message);
        Assert.Equal(new[] { "key function", "built key Configure(isKey: True)", "built key Dispose" }, log.ToArray());
        Assert.Equal(0, value.Disposals);
    }

    // The asynchronous client's own call, whose checks throw from it.
    private static Task CreateTask(
        string flavor, IReadOnlyDictionary<string, object?> configs, CancellationToken cancellationToken) =>
        flavor == AsyncProducer
            ? AsyncKafkaProducer<string, string>.Create(configs, cancellationToken)
            : AsyncKafkaConsumer<string, string>.Create(configs, cancellationToken);

    private static Task CreateTask(
        string flavor,
        IReadOnlyDictionary<string, object?> configs,
        object? key,
        object? value,
        CancellationToken cancellationToken) =>
        flavor == AsyncProducer
            ? AsyncKafkaProducer<string, string>.Create(
                configs, (IAsyncSerializer<string>?)key, (IAsyncSerializer<string>?)value, cancellationToken)
            : AsyncKafkaConsumer<string, string>.Create(
                configs, (IAsyncDeserializer<string>?)key, (IAsyncDeserializer<string>?)value, cancellationToken);

    private static Task Record(List<CancellationToken> seen, CancellationToken cancellationToken)
    {
        seen.Add(cancellationToken);
        return Task.CompletedTask;
    }

    private static async Task ThrowWhenCancelled(Task gate, CancellationToken cancellationToken)
    {
        await gate.ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
    }

    private static bool IsProducer(string flavor) => flavor is SyncProducer or AsyncProducer;

    private static bool IsAsync(string flavor) => flavor is AsyncProducer or AsyncConsumer;

    private static string KeyConfig(string flavor) => IsProducer(flavor) ? "key.serializer" : "key.deserializer";

    private static string ValueConfig(string flavor) => IsProducer(flavor) ? "value.serializer" : "value.deserializer";

    private static string FailedToConstruct(string flavor) =>
        IsProducer(flavor) ? "Failed to construct kafka producer" : "Failed to construct kafka consumer";

    private static string MustBeNonNull(string config) =>
        "Invalid value null for configuration " + config + ": must be non-null.";

    private static string SerdeInterface(string flavor) => flavor switch
    {
        SyncProducer => "ISerializer",
        SyncConsumer => "IDeserializer",
        AsyncProducer => "IAsyncSerializer",
        _ => "IAsyncDeserializer",
    };

    private static string SerdeInterfaceText(string flavor) =>
        "Confluent.Kafka.Common.Serialization." + SerdeInterface(flavor) + "<string>";

    private static string NotAFunction(string flavor, string config, string text, string type) =>
        "Invalid value " + text + " for configuration " + config + ": Expected value to be a System.Func<"
            + SerdeInterfaceText(flavor) + ">, but it was a " + type;

    private static string ReturnedNull(string flavor, string config) =>
        "Invalid value null for configuration " + config + ": Expected the function to return a "
            + SerdeInterfaceText(flavor) + ", but it returned null";

    private static string ClosingRefused(string flavor, object closing)
    {
        string client = IsProducer(flavor) ? "producer" : "consumer";
        return IsAsync(flavor)
            ? closing.GetType() + " implements IDisposable but does not implement IAsyncDisposable, so the " + client
                + " would never dispose it."
            : closing.GetType() + " declares Close() but does not implement IDisposable, so the " + client
                + " would never call it.";
    }

    // The function of the client's other form: the asynchronous serde for a synchronous client, and the reverse.
    private static object OfTheOtherForm(string flavor) => flavor switch
    {
        SyncProducer => new Func<IAsyncSerializer<string>>(() => new AsyncStringSerializer()),
        SyncConsumer => new Func<IAsyncDeserializer<string>>(() => new AsyncStringDeserializer()),
        AsyncProducer => new Func<ISerializer<string>>(() => new StringSerializer()),
        _ => new Func<IDeserializer<string>>(() => new StringDeserializer()),
    };

    private static string OtherForm(string flavor) => flavor switch
    {
        SyncProducer => "IAsyncSerializer",
        SyncConsumer => "IAsyncDeserializer",
        AsyncProducer => "ISerializer",
        _ => "IDeserializer",
    };

    private static string OfTheOtherFormText(string flavor) =>
        "System.Func`1[Confluent.Kafka.Common.Serialization." + OtherForm(flavor) + "`1[System.String]]";

    private static string OfTheOtherFormType(string flavor) =>
        "System.Func<Confluent.Kafka.Common.Serialization." + OtherForm(flavor) + "<string>>";

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
        SyncConsumer => new DeserializerProbe(name, log),
        AsyncProducer => new AsyncSerializerProbe(name, log),
        _ => new AsyncDeserializerProbe(name, log),
    };

    private static object Plain(string flavor) => flavor switch
    {
        SyncProducer => new PlainSerializer(),
        SyncConsumer => new PlainDeserializer(),
        AsyncProducer => new PlainAsyncSerializer(),
        _ => new PlainAsyncDeserializer(),
    };

    // A serde the client refuses, given or built: its close is one the client would never call.
    private static object Closing(string flavor) => flavor switch
    {
        SyncProducer => new ClosingSerializer(),
        SyncConsumer => new ClosingDeserializer(),
        AsyncProducer => new DisposableOnlyAsyncSerializer(),
        _ => new DisposableOnlyAsyncDeserializer(),
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
            SyncConsumer => new Func<IDeserializer<string>>(() => (IDeserializer<string>)Logged()!),
            AsyncProducer => new Func<IAsyncSerializer<string>>(() => (IAsyncSerializer<string>)Logged()!),
            _ => new Func<IAsyncDeserializer<string>>(() => (IAsyncDeserializer<string>)Logged()!),
        };
    }

    // What the call throws comes back as a failed task, so a synchronous client's failure, an asynchronous client's
    // check and an asynchronous client's failed task are asserted alike.
    private static Task<object> Create(
        string flavor, IReadOnlyDictionary<string, object?> configs, CancellationToken cancellationToken = default)
    {
        try
        {
            return flavor switch
            {
                SyncProducer => Task.FromResult<object>(KafkaProducer<string, string>.Create(configs)),
                SyncConsumer => Task.FromResult<object>(KafkaConsumer<string, string>.Create(configs)),
                AsyncProducer => Upcast(AsyncKafkaProducer<string, string>.Create(configs, cancellationToken)),
                _ => Upcast(AsyncKafkaConsumer<string, string>.Create(configs, cancellationToken)),
            };
        }
        catch (Exception failure)
        {
            return Task.FromException<object>(failure);
        }
    }

    private static Task<object> Create(
        string flavor,
        IReadOnlyDictionary<string, object?> configs,
        object? key,
        object? value,
        CancellationToken cancellationToken = default)
    {
        try
        {
            return flavor switch
            {
                SyncProducer => Task.FromResult<object>(
                    KafkaProducer<string, string>.Create(configs, (ISerializer<string>?)key, (ISerializer<string>?)value)),
                SyncConsumer => Task.FromResult<object>(
                    KafkaConsumer<string, string>.Create(configs, (IDeserializer<string>?)key, (IDeserializer<string>?)value)),
                AsyncProducer => Upcast(
                    AsyncKafkaProducer<string, string>.Create(
                        configs, (IAsyncSerializer<string>?)key, (IAsyncSerializer<string>?)value, cancellationToken)),
                _ => Upcast(
                    AsyncKafkaConsumer<string, string>.Create(
                        configs, (IAsyncDeserializer<string>?)key, (IAsyncDeserializer<string>?)value, cancellationToken)),
            };
        }
        catch (Exception failure)
        {
            return Task.FromException<object>(failure);
        }
    }

    private static async Task<object> Upcast<TClient>(Task<TClient> client)
        where TClient : class =>
        await client.ConfigureAwait(false);

    private static Task Close(object client)
    {
        if (client is IAsyncDisposable asynchronous)
        {
            return TestTimeout.DisposeAsync(asynchronous);
        }

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

        internal int Disposals => Volatile.Read(ref _disposals);

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
            Interlocked.Increment(ref _disposals);
            _log.Enqueue(_name + " Dispose");
        }
    }

    /// <summary>The asynchronous form: its <c>Configure</c> may wait, and may do something once it is done.</summary>
    private abstract class AsyncProbe : Probe
    {
        protected AsyncProbe(string name, ConcurrentQueue<string> log)
            : base(name, log)
        {
        }

        // What Configure awaits, with the token it was given, after logging the call.
        internal Func<CancellationToken, Task>? ConfigureWaits { get; set; }

        public async ValueTask Configure(
            IReadOnlyDictionary<string, object?> configs, bool isKey, CancellationToken cancellationToken)
        {
            OnConfigure(configs, isKey);
            if (ConfigureWaits is not null)
            {
                await ConfigureWaits(cancellationToken).ConfigureAwait(false);
            }
        }

        public ValueTask DisposeAsync()
        {
            OnDispose();
            return default;
        }
    }

    private sealed class AsyncSerializerProbe : AsyncProbe, IAsyncSerializerWithConfigure<string>, IAsyncDisposable
    {
        internal AsyncSerializerProbe(string name, ConcurrentQueue<string> log)
            : base(name, log)
        {
        }

        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, string? data, CancellationToken cancellationToken) => default;
    }

    private sealed class AsyncDeserializerProbe : AsyncProbe, IAsyncDeserializerWithConfigure<string>, IAsyncDisposable
    {
        internal AsyncDeserializerProbe(string name, ConcurrentQueue<string> log)
            : base(name, log)
        {
        }

        public ValueTask<string?> Deserialize(
            string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) => default;
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

    private sealed class PlainAsyncSerializer : IAsyncSerializer<string>
    {
        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, string? data, CancellationToken cancellationToken) => default;
    }

    private sealed class PlainAsyncDeserializer : IAsyncDeserializer<string>
    {
        public ValueTask<string?> Deserialize(
            string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) => default;
    }

    private sealed class DisposableOnlyAsyncSerializer : IAsyncSerializer<string>, IDisposable
    {
        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, string? data, CancellationToken cancellationToken) => default;

        public void Dispose()
        {
        }
    }

    private sealed class DisposableOnlyAsyncDeserializer : IAsyncDeserializer<string>, IDisposable
    {
        public ValueTask<string?> Deserialize(
            string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) => default;

        public void Dispose()
        {
        }
    }
}
