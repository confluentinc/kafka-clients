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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Producer;
using Confluent.Kafka.UnitTests.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Typed configuration values through the public constructors of the six clients, on an unreachable broker: Java's
/// config-class and client tests and Python's client-level config tests, each refusal asserted with its exact message
/// and each client disposed.
/// </summary>
public sealed class PublicClientConfigTests
{
    private const string KafkaProducerClient = "KafkaProducer";
    private const string AsyncKafkaProducerClient = "AsyncKafkaProducer";
    private const string KafkaConsumerClient = "KafkaConsumer";
    private const string AsyncKafkaConsumerClient = "AsyncKafkaConsumer";
    private const string KafkaAdminClientClient = "KafkaAdminClient";
    private const string AsyncKafkaAdminClientClient = "AsyncKafkaAdminClient";

    private const string Unreachable = "127.0.0.1:1";
    private const int ConfigErrorCode = -10;
    private const int UnknownServerErrorCode = -1;
    private const int InvalidGroupIdCode = 24;

    private const string NullAcks =
        "Invalid value null for configuration acks: String must be one of: all, -1, 0, 1";

    private const string NullMaxPollRecords =
        "Invalid value null for configuration max.poll.records: Value must be non-null";

    private const string NullBootstrapServers = "Configuration 'bootstrap.servers' values must not be null.";

    private const string InvalidConfigKey =
        "A configuration key must not contain a NUL character: such a string cannot be passed to the native client " +
        "unchanged.";

    private const string InvalidConfigValue =
        "A configuration value must not contain a NUL character: such a string cannot be passed to the native " +
        "client unchanged.";

    private const string GroupIdNotDefinedMessage =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer " +
        "configuration.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly string[] s_serializerKeys =
    {
        "key.serializer", "value.serializer", "serializer.encoding", "key.serializer.encoding",
        "value.serializer.encoding",
    };

    private static readonly string[] s_deserializerKeys =
    {
        "key.deserializer", "value.deserializer", "deserializer.encoding", "key.deserializer.encoding",
        "value.deserializer.encoding",
    };

    public static TheoryData<string> AllClients => new TheoryData<string>
    {
        KafkaProducerClient,
        AsyncKafkaProducerClient,
        KafkaConsumerClient,
        AsyncKafkaConsumerClient,
        KafkaAdminClientClient,
        AsyncKafkaAdminClientClient,
    };

    // Java's tests take security.protocol and metadata.recovery.strategy from CommonClientConfigs, which is not
    // translated, so those keys stay literals.
    [Theory]
    [InlineData(
        "ProducerConfigTest.testInvalidCompressionType", ProducerConfig.COMPRESSION_TYPE_CONFIG, "abc",
        "String must be one of: none, gzip, snappy, lz4, zstd")]
    [InlineData(
        "ProducerConfigTest.testInvalidSecurityProtocol", "security.protocol", "abc",
        "String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT")]
    [InlineData(
        "ProducerConfigTest.testInvalidMetadataRecoveryStrategy", "metadata.recovery.strategy", "abc",
        "String must be one of (case insensitive): REBOOTSTRAP, NONE")]
    [InlineData(
        "ProducerConfigTest.testInvalidBufferMemoryAllocationStrategy",
        ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG, "abc",
        "String must be one of (case insensitive): FULL, INCREMENTAL")]
    [InlineData(
        "ConsumerConfigTest.testInvalidSecurityProtocol", "security.protocol", "abc",
        "String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT")]
    [InlineData(
        "ConsumerConfigTest.testInvalidMetadataRecoveryStrategy", "metadata.recovery.strategy", "abc",
        "String must be one of (case insensitive): REBOOTSTRAP, NONE")]
    [InlineData(
        "ConsumerConfigTest.testInvalidGroupInstanceId", ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "",
        "String must be non-empty")]
    [InlineData(
        "ConsumerConfigTest.testProtocolConfigValidation(invalid)", ConsumerConfig.GROUP_PROTOCOL_CONFIG, "invalid",
        "String must be one of (case insensitive): CONSUMER, CLASSIC")]
    [InlineData(
        "AdminClientConfigTest.testInvalidMetadataRecoveryStrategy", "metadata.recovery.strategy", "abc",
        "String must be one of (case insensitive): REBOOTSTRAP, NONE")]
    public void JavaConfigClasses_AConfigurationTheClientRejects(string test, string key, string value, string message)
    {
        string client = ClientOfJavaTest(test);

        Assert.Equal(Invalid(key, value, message), Refusal(client, Configs(client, (key, value))).Message);
    }

    [Fact]
    public void ProducerConfigTest_testValidBufferMemoryAllocationStrategy()
    {
        Constructs(
            KafkaProducerClient,
            Configs(
                KafkaProducerClient,
                (ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG,
                    ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL)));
    }

    [Theory]
    [InlineData(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000)]
    [InlineData(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000)]
    [InlineData(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "RoundRobinAssignor")]
    public void ConsumerConfigTest_testUnsupportedConfigsWithConsumerGroupProtocol(string key, object value)
    {
        Dictionary<string, object?> configs = Configs(
            KafkaConsumerClient,
            (ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.ToString()),
            (key, value));

        Assert.Equal(
            key + " cannot be set when " + ConsumerConfig.GROUP_PROTOCOL_CONFIG + "=" +
            GroupProtocol.CONSUMER.ToString(),
            Refusal(KafkaConsumerClient, configs).Message);
    }

    [Fact]
    public void ProducerConfigTest_testUpperboundCheckOfEnableIdempotence()
    {
        Dictionary<string, object?> configs = Configs(
            KafkaProducerClient, (ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 6));

        Assert.Equal(
            "To use the idempotent producer, " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION +
            " must be set to at most 5. Current value is 6.",
            Refusal(KafkaProducerClient, configs).Message);

        configs[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = 5;
        Constructs(KafkaProducerClient, configs);
    }

    [Fact]
    public void ProducerConfigTest_testTwoPhaseCommitIncompatibleWithTransactionTimeout()
    {
        Dictionary<string, object?> configs = Configs(
            KafkaProducerClient,
            (ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
            (ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-txn-id"),
            (ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG, true),
            (ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000));

        Assert.Equal(
            "Cannot set " + ProducerConfig.TRANSACTION_TIMEOUT_CONFIG + " when " +
            ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG + " is set to true. " +
            "Transactions will not expire with two-phase commit enabled.",
            Refusal(KafkaProducerClient, configs).Message);

        configs.Remove(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        Constructs(KafkaProducerClient, configs);

        configs[ProducerConfig.TRANSACTION_TIMEOUT_CONFIG] = 60000;
        configs[ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG] = false;
        Constructs(KafkaProducerClient, configs);
    }

    [Theory]
    [InlineData("KafkaProducerTest.testOsDefaultSocketBufferSizes")]
    [InlineData("KafkaConsumerTest.testOsDefaultSocketBufferSizes")]
    public void JavaClients_TheOsDefaultSocketBufferSizesAreAccepted(string test)
    {
        string client = ClientOfJavaTest(test);

        Constructs(client, Configs(client, ("send.buffer.bytes", -1), ("receive.buffer.bytes", -1)));
    }

    [Theory]
    [InlineData("KafkaProducerTest.testInvalidSocketSendBufferSize", "send.buffer.bytes")]
    [InlineData("KafkaProducerTest.testInvalidSocketReceiveBufferSize", "receive.buffer.bytes")]
    [InlineData("KafkaConsumerTest.testInvalidSocketSendBufferSize", "send.buffer.bytes")]
    [InlineData("KafkaConsumerTest.testInvalidSocketReceiveBufferSize", "receive.buffer.bytes")]
    public void JavaClients_AnInvalidSocketBufferSizeIsRefused(string test, string key)
    {
        string client = ClientOfJavaTest(test);

        Assert.Equal(
            Invalid(key, "-2", "Value must be at least -1"),
            Refusal(client, Configs(client, (key, -2))).Message);
    }

    [Fact]
    public void KafkaProducerTest_testDeliveryTimeoutAndLingerMsConfig()
    {
        Dictionary<string, object?> configs = Configs(
            KafkaProducerClient,
            ("client.id", "testDeliveryTimeoutAndLingerMsConfig"),
            ("delivery.timeout.ms", 1000),
            ("linger.ms", 1000),
            ("request.timeout.ms", 1));

        KafkaException failure = Assert.Throws<KafkaException>(() => Constructs(KafkaProducerClient, configs));
        Assert.Equal("Failed to construct kafka producer", failure.Message);
        Assert.Equal(UnknownServerErrorCode, failure.Code);
        ConfigException cause = Assert.IsType<ConfigException>(failure.InnerException);
        Assert.Equal(
            "delivery.timeout.ms should be equal to or larger than linger.ms + request.timeout.ms",
            cause.Message);
        Assert.Equal(ConfigErrorCode, cause.Code);

        configs["linger.ms"] = 999;
        Constructs(KafkaProducerClient, configs);
    }

    [Theory]
    [InlineData(KafkaAdminClientClient)]
    [InlineData(AsyncKafkaAdminClientClient)]
    public void KafkaAdminClientTest_testDefaultApiTimeoutAndRequestTimeoutConflicts(string client)
    {
        KafkaException failure = Assert.Throws<KafkaException>(
            () => Constructs(client, Configs(client, ("default.api.timeout.ms", 500))));

        Assert.Equal("Failed to create new KafkaAdminClient", failure.Message);
        Assert.Equal(UnknownServerErrorCode, failure.Code);
        ConfigException cause = Assert.IsType<ConfigException>(failure.InnerException);
        Assert.Equal(
            "The specified value of default.api.timeout.ms must be no smaller than the value of request.timeout.ms.",
            cause.Message);
        Assert.Equal(ConfigErrorCode, cause.Code);
    }

    [Theory]
    [MemberData(nameof(AllClients))]
    public void KafkaProducerTest_testConstructorWithNotStringKey_ANullKeyIsRefusedByEveryClient(string client)
    {
        var configs = new ClientConfigsTests.Entries(("bootstrap.servers", Unreachable), (null, "value"));

        Assert.Equal(
            "Invalid value value for configuration null: Key must be a string.",
            Refusal(client, configs).Message);
    }

    [Theory]
    [InlineData("batch.size", "hello", "Not a number of type INT")]
    [InlineData("batch.size", "42.5", "Not a number of type INT")]
    [InlineData("batch.size", "9223372036854775807", "Not a number of type INT")]
    [InlineData("linger.ms", "hello", "Not a number of type LONG")]
    [InlineData("linger.ms", "42.5", "Not a number of type LONG")]
    [InlineData("linger.ms", "922337203685477580700", "Not a number of type LONG")]
    [InlineData("sasl.login.refresh.window.factor", "hello", "Not a number of type DOUBLE")]
    [InlineData("enable.idempotence", "hello", "Expected value to be either true or false")]
    [InlineData("enable.idempotence", "truee", "Expected value to be either true or false")]
    [InlineData("enable.idempotence", "fals", "Expected value to be either true or false")]
    public void ConfigDefTest_testBadInputs_TheCoreRefusesTheText(string key, string text, string message)
    {
        Assert.Equal(
            Invalid(key, text, message),
            Refusal(KafkaProducerClient, Configs(KafkaProducerClient, (key, text))).Message);
    }

    [Fact]
    public void ConfigDefTest_testValidateCannotParse()
    {
        Assert.Equal(
            "Invalid value non_integer for configuration batch.size: Not a number of type INT",
            Refusal(KafkaProducerClient, Configs(KafkaProducerClient, ("batch.size", "non_integer"))).Message);
    }

    [Fact]
    public void TestConfig_test_bad_number_text_reaches_the_core()
    {
        Assert.Equal(
            "Invalid value many for configuration max.poll.records: Not a number of type INT",
            Refusal(
                KafkaConsumerClient,
                Configs(KafkaConsumerClient, ("group.id", "g"), ("max.poll.records", "many"))).Message);
        Assert.Equal(
            "Invalid value abc for configuration linger.ms: Not a number of type LONG",
            Refusal(KafkaProducerClient, Configs(KafkaProducerClient, ("linger.ms", "abc"))).Message);
    }

    [Fact]
    public void TestConfig_test_unsupported_sasl_mechanism_is_rejected_by_the_core()
    {
        Assert.Equal(
            "Invalid value SCRAM-SHA-256 for configuration sasl.mechanism: only PLAIN is supported by this client",
            Refusal(
                KafkaProducerClient,
                Configs(
                    KafkaProducerClient,
                    ("security.protocol", "SASL_PLAINTEXT"),
                    ("sasl.mechanism", "SCRAM-SHA-256"))).Message);
        Assert.Equal(
            "Invalid value GSSAPI for configuration sasl.mechanism: only PLAIN is supported by this client",
            Refusal(KafkaProducerClient, Configs(KafkaProducerClient, ("security.protocol", "SASL_SSL"))).Message);
    }

    [Theory]
    [InlineData(KafkaProducerClient)]
    [InlineData(KafkaConsumerClient)]
    public void TestConfig_test_interceptor_and_partitioner_keys_reach_the_core(string client)
    {
        var keys = new Dictionary<string, object?>
        {
            ["interceptor.classes"] = "com.example.Interceptor",
            ["partitioner.class"] = "com.example.MyPartitioner",
            ["partitioner.type"] = "RoundRobinPartitioner",
        };

        Assert.Equal(
            keys.Select(pair => new KeyValuePair<string, string>(pair.Key, (string)pair.Value!)),
            ClientConfigs.Prepare(keys, Kind(client)));
        Constructs(client, Configs(client, keys.Select(pair => (pair.Key, pair.Value)).ToArray()));
    }

    [Fact]
    public void TestConfig_test_old_client_keys_are_accepted()
    {
        Dictionary<string, object?> configs = Configs(
            KafkaProducerClient,
            ("error_cb", new Action<string>(_ => { })),
            ("on_delivery", new Action<string>(_ => { })),
            ("logger", "x"));

        Assert.Equal(
            new[] { "bootstrap.servers", "error_cb", "logger", "on_delivery" },
            ClientConfigs.Prepare(configs, ClientKind.Producer)
                .Select(pair => pair.Key)
                .OrderBy(key => key, StringComparer.Ordinal));
        Constructs(KafkaProducerClient, configs);
    }

    [Theory]
    [MemberData(nameof(AllClients))]
    public void TestConfigMatrix_test_each_client_takes_a_typed_key_of_every_type(string client)
    {
        Dictionary<string, object?> configs = Configs(
            client,
            ("request.timeout.ms", 30000),
            ("metadata.max.age.ms", 300000L),
            ("sasl.login.refresh.min.period.seconds", (short)60),
            ("sasl.login.refresh.window.factor", 0.8),
            ("sasl.mechanism", "GSSAPI"),
            ("ssl.keystore.password", "keystore-secret"),
            ("ssl.enabled.protocols", new[] { "TLSv1.2", "TLSv1.3" }),
            ("ssl.engine.factory.class", typeof(string)),
            ("client.id", "typed-keys"));
        switch (Kind(client))
        {
            case ClientKind.Producer:
                configs["enable.idempotence"] = true;
                break;
            case ClientKind.Consumer:
                configs["group.id"] = "typed";
                configs["enable.auto.commit"] = false;
                break;
            default:
                configs["retries"] = 0;
                configs["enable.metrics.push"] = false;
                break;
        }

        Constructs(client, configs);
    }

    [Theory]
    [InlineData(KafkaProducerClient, "acks", NullAcks)]
    [InlineData(AsyncKafkaProducerClient, "acks", NullAcks)]
    [InlineData(KafkaConsumerClient, "max.poll.records", NullMaxPollRecords)]
    [InlineData(AsyncKafkaConsumerClient, "max.poll.records", NullMaxPollRecords)]
    [InlineData(KafkaAdminClientClient, "bootstrap.servers", NullBootstrapServers)]
    [InlineData(AsyncKafkaAdminClientClient, "bootstrap.servers", NullBootstrapServers)]
    public void TestConfigNull_test_the_clients_refuse_a_null_their_validator_rejects(
        string client, string key, string message)
    {
        Assert.Equal(message, Refusal(client, Configs(client, (key, null))).Message);
    }

    [Theory]
    [InlineData(KafkaAdminClientClient)]
    [InlineData(AsyncKafkaAdminClientClient)]
    public void TestAdminClient_test_config_values_checked_by_admin_client_config(string client)
    {
        Assert.Equal(
            "Invalid value 1.5 for configuration request.timeout.ms: " +
            "Expected value to be a 32-bit integer, but it was a System.Double",
            Refusal(client, Configs(client, ("request.timeout.ms", 1.5))).Message);
        Assert.Equal(
            "Invalid value true for configuration retries: " +
            "Expected value to be a 32-bit integer, but it was a System.Boolean",
            Refusal(client, Configs(client, ("retries", true))).Message);
        Constructs(
            client,
            new Dictionary<string, object?>
            {
                ["bootstrap.servers"] = new[] { Unreachable, "127.0.0.1:2" },
                ["client.id"] = null,
            });
        Assert.Equal(
            "Invalid value 5 for configuration bootstrap.controllers: Expected a comma separated list.",
            Refusal(client, new Dictionary<string, object?> { ["bootstrap.controllers"] = 5 }).Message);
    }

    [Fact]
    public void TestProducerFamily_test_kafka_producer_applies_javas_config_validators()
    {
        Assert.Equal(
            "Invalid value -1 for configuration batch.size: Value must be at least 0",
            Refusal(KafkaProducerClient, Configs(KafkaProducerClient, ("batch.size", -1))).Message);
    }

    [Theory]
    [MemberData(nameof(AllClients))]
    public void TestNoneIsNullProducerCommon_test_clients_null_configs(string client)
    {
        string parameter = client is KafkaAdminClientClient or AsyncKafkaAdminClientClient ? "conf" : "configs";
        Assert.Equal(parameter, Assert.Throws<ArgumentNullException>(() => Create(client, null!)).ParamName);
    }

    [Theory]
    [InlineData(KafkaConsumerClient)]
    [InlineData(AsyncKafkaConsumerClient)]
    public void ANullGroupId_IsNotDefined_GroupMetadataThrowsInvalidGroupId(string client)
    {
        var consumer = (IConsumerCommon)Create(client, Configs(client, ("group.id", null)));
        try
        {
            InvalidGroupIdException failure = Assert.Throws<InvalidGroupIdException>(() => consumer.GroupMetadata());

            Assert.Equal(InvalidGroupIdCode, failure.Code);
            Assert.Equal(GroupIdNotDefinedMessage, failure.Message);
        }
        finally
        {
            TestTimeout.Dispose((IDisposable)consumer, s_deadline);
        }
    }

    [Theory]
    [InlineData(KafkaProducerClient, "batch.size", "INT")]
    [InlineData(AsyncKafkaProducerClient, "batch.size", "INT")]
    [InlineData(KafkaConsumerClient, "request.timeout.ms", "INT")]
    [InlineData(AsyncKafkaConsumerClient, "request.timeout.ms", "INT")]
    [InlineData(KafkaAdminClientClient, "request.timeout.ms", "INT")]
    [InlineData(AsyncKafkaAdminClientClient, "request.timeout.ms", "INT")]
    public void ACoreConfigError_IsAConfigException_CaughtAsAKafkaException(string client, string key, string type)
    {
        KafkaException? caught = null;
        try
        {
            Constructs(client, Configs(client, (key, "abc")));
        }
        catch (KafkaException failure)
        {
            caught = failure;
        }

        ConfigException error = Assert.IsType<ConfigException>(caught);
        Assert.Equal(Invalid(key, "abc", "Not a number of type " + type), error.Message);
        Assert.Equal(ConfigErrorCode, error.Code);
    }

    [Theory]
    [MemberData(nameof(AllClients))]
    public void SerializerKeys_TheOwnRolesAreNotChecked_TheOtherRolesReachTheCoreAsUnknownKeys(string client)
    {
        Dictionary<string, object?> configs = Configs(client);
        foreach (string key in s_serializerKeys.Concat(s_deserializerKeys))
        {
            configs[key] = 5;
        }

        configs["value.serializer"] = new object();
        configs["value.deserializer"] = new object();
        configs["serializer.encoding"] = null;
        configs["deserializer.encoding"] = typeof(string);

        Constructs(client, configs);
    }

    [Theory]
    [MemberData(nameof(AllClients))]
    public void ANulInAConfigurationKey_IsRejectedBeforeTheClientIsCreated(string client)
    {
        ArgumentException rejected = NulRefusal(client, Configs(client, ("client.id\0", "x")));

        Assert.Equal(new ArgumentException(InvalidConfigKey, ConfigsParameter(client)).Message, rejected.Message);
    }

    [Theory]
    [MemberData(nameof(AllClients))]
    public void ANulInAConfigurationValue_IsRejectedWithoutTheValue(string client)
    {
        ArgumentException rejected = NulRefusal(client, Configs(client, ("ssl.key.password", "s3cret\0tail")));

        Assert.Equal(new ArgumentException(InvalidConfigValue, ConfigsParameter(client)).Message, rejected.Message);
        Assert.DoesNotContain("s3cret", rejected.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(AllClients))]
    public void ANulSuffixedKey_CannotReplaceTheKeyItSpells(string client)
    {
        ArgumentException rejected = NulRefusal(
            client, Configs(client, ("security.protocol", "SASL_SSL"), ("security.protocol\0", "PLAINTEXT")));

        Assert.Equal(new ArgumentException(InvalidConfigKey, ConfigsParameter(client)).Message, rejected.Message);
    }

    [Theory]
    [MemberData(nameof(AllClients))]
    public void AValueOfTheWrongType_IsRefusedBeforeANulInAnotherKey(string client)
    {
        Assert.Equal(
            Invalid("client.id", "System.Object", "Expected value to be a string, but it was a System.Object"),
            Refusal(client, Configs(client, ("x\0", "y"), ("client.id", new object()))).Message);
    }

    private static ArgumentException NulRefusal(string client, IReadOnlyDictionary<string, object?> configs)
    {
        ArgumentException rejected = Assert.Throws<ArgumentException>(() => Constructs(client, configs));
        Assert.Equal(ConfigsParameter(client), rejected.ParamName);
        return rejected;
    }

    private static string ConfigsParameter(string client) =>
        client is KafkaAdminClientClient or AsyncKafkaAdminClientClient ? "conf" : "configs";

    private static string ClientOfJavaTest(string test) =>
        test.StartsWith("Producer", StringComparison.Ordinal) || test.StartsWith("KafkaProducer", StringComparison.Ordinal)
            ? KafkaProducerClient
            : test.StartsWith("Consumer", StringComparison.Ordinal) || test.StartsWith("KafkaConsumer", StringComparison.Ordinal)
                ? KafkaConsumerClient
                : KafkaAdminClientClient;

    private static ClientKind Kind(string client) => client switch
    {
        KafkaProducerClient or AsyncKafkaProducerClient => ClientKind.Producer,
        KafkaConsumerClient or AsyncKafkaConsumerClient => ClientKind.Consumer,
        _ => ClientKind.Admin,
    };

    private static Dictionary<string, object?> Configs(string client, params (string Key, object? Value)[] entries)
    {
        var configs = new Dictionary<string, object?>(StringComparer.Ordinal) { ["bootstrap.servers"] = Unreachable };
        if (Kind(client) == ClientKind.Consumer)
        {
            configs["group.protocol"] = "consumer";
        }

        foreach ((string key, object? value) in entries)
        {
            configs[key] = value;
        }

        return configs;
    }

    private static IDisposable Create(string client, IReadOnlyDictionary<string, object?> configs) => client switch
    {
        KafkaProducerClient => new KafkaProducer<byte[], byte[]>(configs, Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer()),
        AsyncKafkaProducerClient => new AsyncKafkaProducer<byte[], byte[]>(configs, Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer()),
        KafkaConsumerClient => new KafkaConsumer<byte[], byte[]>(configs, Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer()),
        AsyncKafkaConsumerClient => new AsyncKafkaConsumer<byte[], byte[]>(configs, Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer()),
        KafkaAdminClientClient => IAdmin.Create(configs),
        AsyncKafkaAdminClientClient => IAsyncAdmin.Create(configs),
        _ => throw new ArgumentOutOfRangeException(nameof(client), client, null),
    };

    private static void Constructs(string client, IReadOnlyDictionary<string, object?> configs) =>
        TestTimeout.Dispose(Create(client, configs), s_deadline);

    private static ConfigException Refusal(string client, IReadOnlyDictionary<string, object?> configs)
    {
        ConfigException failure = Assert.Throws<ConfigException>(() => Constructs(client, configs));
        Assert.Equal(ConfigErrorCode, failure.Code);
        Assert.Null(failure.InnerException);
        return failure;
    }

    private static string Invalid(string key, string value, string message) =>
        "Invalid value " + value + " for configuration " + key + ": " + message;
}
