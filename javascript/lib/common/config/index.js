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

'use strict';

// The common.config namespace: the public classes of Java's
// org.apache.kafka.common.config this client has. The config classes'
// constants and ConfigError are generated from the Java sources.

const { SslClientAuth } = require('./ssl_client_auth');
// BEGIN GENERATED REQUIRES (cargo xtask generate-error-codes; do not edit)
const { ConfigError } = require('./config_error');
const { SaslConfigs } = require('./sasl_configs');
const { SecurityConfig } = require('./security_config');
const { SslConfigs } = require('./ssl_configs');
// END GENERATED REQUIRES

module.exports = {
  SslClientAuth,
  // BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
  ConfigError,
  SaslConfigs,
  SecurityConfig,
  SslConfigs,
  // END GENERATED EXPORTS
};
