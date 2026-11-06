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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// the Java source of `org.apache.kafka.common.errors.AuthenticationException`;
// `cargo xtask check-generated` checks it is up to date.

'use strict';

const _throwable = require('../../_throwable');
const { InvalidConfigurationError } = require('./invalid_configuration_error');

/**
 * This exception indicates that SASL authentication has failed. On
 * authentication failure, clients abort the operation requested and raise one
 * of the subclasses of this exception:
 *
 * `SaslAuthenticationException` if SASL handshake fails with invalid
 * credentials or any other failure specific to the SASL mechanism used for
 * authentication `UnsupportedSaslMechanismException` if the SASL mechanism
 * requested by the client is not supported on the broker.
 * `IllegalSaslStateException` if an unexpected request is received on during
 * SASL handshake. This could be due to misconfigured security protocol.
 * `SslAuthenticationException` if SSL handshake failed due to any
 * `SSLException`.
 *
 * Java: `org.apache.kafka.common.errors.AuthenticationException`.
 */
class AuthenticationError extends InvalidConfigurationError {}

_throwable.setFfiId(AuthenticationError, -7); // kafka_common_Error_AUTHENTICATION

module.exports = { AuthenticationError };
