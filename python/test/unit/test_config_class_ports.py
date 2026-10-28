# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Java ports for the config classes translated for their constants (CLAUDE.md,
Python Binding Conventions, Scope), and the inventory of their Java tests
(Apache Kafka 4.4.0).

The classes' ``ConfigDef`` machinery is not translated *(deviation)*: no
constructor parses a configuration, and ``getString`` / ``originals`` /
``appendSerializerToConfig`` / ``metricsReporters`` / ``addClientSaslSupport``
are not generated. A Java test of those is skipped, naming the core's
translation of it; a test of a configuration the client rejects is ported
through the client's constructor, which runs the same ``ConfigDef`` check in the
core (the message asserted in full, where Java asserts it names the key).

  ProducerConfigTest, 13 tests:
    testInvalidCompressionType, testInvalidSecurityProtocol, testInvalidMetadataRecoveryStrategy,
    testInvalidBufferMemoryAllocationStrategy | ported here
    testValidBufferMemoryAllocationStrategy | ported here (accepted; the parsed value is not readable, getString not generated)
    testAppendSerializerToConfig, testAppendSerializerToConfigWithException | skipped | appendSerializerToConfig not generated (the serde route is the binding's, test_serde_supply.py); core: none
    testDefaultMetadataRecoveryStrategy, testDefaultBufferMemoryAllocationStrategy | skipped | getString not generated; core: producer_config.rs test_default_metadata_recovery_strategy, test_default_buffer_memory_allocation_strategy
    testCaseInsensitiveSecurityProtocol | skipped | originals() not generated, and a SASL_SSL client needs a SASL setup; core: producer_config.rs test_case_insensitive_security_protocol
    testUpperboundCheckOfEnableIdempotence, testTwoPhaseCommitIncompatibleWithTransactionTimeout | skipped | the ConfigDef constructor's postProcessParsedConfig, not generated; core: producer_config.rs test_upperbound_check_of_enable_idempotence, test_two_phase_commit_incompatible_with_transaction_timeout
    testValidateConfigPropertiesFile | skipped | reads the Java docs' properties file; core: none
  ConsumerConfigTest, 17 tests:
    testInvalidSecurityProtocol, testInvalidMetadataRecoveryStrategy, testInvalidGroupInstanceId | ported here
    testProtocolConfigValidation (invalid row) | ported here; the valid rows read the parsed value, not generated
    testOverrideClientId, testOverrideEnableAutoCommit, testDefaultConsumerGroupConfig, testRemoteAssignorConfig, testRemoteAssignorWithClassicGroupProtocol,
    testUnsupportedConfigsWithConsumerGroupProtocol, testDefaultMetadataRecoveryStrategy, testCaseInsensitiveSecurityProtocol | skipped | read the parsed ConsumerConfig, not generated; core: consumer_config.rs, the tests of the same name
    testAppendDeserializerToConfig, testAppendDeserializerToConfigWithException | skipped | appendDeserializerToConfig not generated (test_serde_supply.py); core: none
    testDefaultPartitionAssignor | skipped | assignors are not translated (KIP-848, consumer-threading.md §20); core: none
    testValidateConfigPropertiesFile | skipped | reads the Java docs' properties file; core: none
  ShareConsumerConfigTest, 1 test:
    testUnsupportedShareConsumerConfigs | skipped | ShareConsumerConfig is not translated while the share consumer is not (consumer-threading.md §20); core: none
  CommonClientConfigsTest, 3 tests | skipped | CommonClientConfigs is not translated: not @InterfaceAudience.Public in Kafka 4.4; core: common_client_configs.rs test_exponential_backoff_defaults, test_invalid_sasl_mechanism
  SaslConfigsTest, 11 tests | skipped | each parses a ConfigDef built by addClientSaslSupport(ConfigDef), not generated; core: none
  AdminClientConfigTest, 2 tests | see test_admin_java_ports.py
"""

from __future__ import annotations

from typing import Any

import pytest

from confluent_kafka.common.config import ConfigError
from confluent_kafka.consumer import ConsumerConfig, KafkaConsumer
from confluent_kafka.producer import KafkaProducer, ProducerConfig

BOOTSTRAP = {ProducerConfig.BOOTSTRAP_SERVERS_CONFIG: "localhost:9092"}

REJECTED: list[tuple[str, Any, str, str, str]] = [
    ("ProducerConfigTest.testInvalidCompressionType", KafkaProducer,
     ProducerConfig.COMPRESSION_TYPE_CONFIG, "abc",
     "String must be one of: none, gzip, snappy, lz4, zstd"),
    ("ProducerConfigTest.testInvalidSecurityProtocol", KafkaProducer,
     "security.protocol", "abc",
     "String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT"),
    ("ProducerConfigTest.testInvalidMetadataRecoveryStrategy", KafkaProducer,
     "metadata.recovery.strategy", "abc",
     "String must be one of (case insensitive): REBOOTSTRAP, NONE"),
    ("ProducerConfigTest.testInvalidBufferMemoryAllocationStrategy", KafkaProducer,
     ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG, "abc",
     "String must be one of (case insensitive): FULL, INCREMENTAL"),
    ("ConsumerConfigTest.testInvalidSecurityProtocol", KafkaConsumer,
     "security.protocol", "abc",
     "String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT"),
    ("ConsumerConfigTest.testInvalidMetadataRecoveryStrategy", KafkaConsumer,
     "metadata.recovery.strategy", "abc",
     "String must be one of (case insensitive): REBOOTSTRAP, NONE"),
    ("ConsumerConfigTest.testInvalidGroupInstanceId", KafkaConsumer,
     ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "", "String must be non-empty"),
    ("ConsumerConfigTest.testProtocolConfigValidation[invalid]", KafkaConsumer,
     ConsumerConfig.GROUP_PROTOCOL_CONFIG, "invalid",
     "String must be one of (case insensitive): CONSUMER, CLASSIC"),
]


@pytest.mark.parametrize("label,client,key,value,message", REJECTED, ids=[r[0] for r in REJECTED])
def test_a_configuration_the_client_rejects(label: str, client: Any, key: str, value: str,
                                            message: str) -> None:
    with pytest.raises(ConfigError) as e:
        client(configs={**BOOTSTRAP, key: value})
    assert str(e.value) == f"Invalid value {value} for configuration {key}: {message}"


def test_valid_buffer_memory_allocation_strategy() -> None:
    # ProducerConfigTest.testValidBufferMemoryAllocationStrategy: accepted.
    producer = KafkaProducer(configs={
        **BOOTSTRAP,
        ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_CONFIG:
            ProducerConfig.BUFFER_MEMORY_ALLOCATION_STRATEGY_INCREMENTAL})
    producer.close(timeout=0)
