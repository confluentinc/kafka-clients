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

//! Translation of `org.apache.kafka.common.config.SaslConfigs` (constants only).
//!
//! Per the Milestone 1 plan, SASL is out of scope for this milestone — these
//! constants exist solely so the producer config validator can reject
//! unsupported SASL settings with the canonical key name in the error
//! message.

pub const SASL_MECHANISM: &str = "sasl.mechanism";
pub const GSSAPI_MECHANISM: &str = "GSSAPI";
pub const DEFAULT_SASL_MECHANISM: &str = GSSAPI_MECHANISM;

pub const SASL_JAAS_CONFIG: &str = "sasl.jaas.config";

pub const SASL_CLIENT_CALLBACK_HANDLER_CLASS: &str = "sasl.client.callback.handler.class";
pub const SASL_LOGIN_CALLBACK_HANDLER_CLASS: &str = "sasl.login.callback.handler.class";
pub const SASL_LOGIN_CLASS: &str = "sasl.login.class";

pub const SASL_KERBEROS_SERVICE_NAME: &str = "sasl.kerberos.service.name";
pub const SASL_KERBEROS_KINIT_CMD: &str = "sasl.kerberos.kinit.cmd";
pub const DEFAULT_KERBEROS_KINIT_CMD: &str = "/usr/bin/kinit";

pub const SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR: &str = "sasl.kerberos.ticket.renew.window.factor";
pub const DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR: f64 = 0.80;

pub const SASL_KERBEROS_TICKET_RENEW_JITTER: &str = "sasl.kerberos.ticket.renew.jitter";
pub const DEFAULT_KERBEROS_TICKET_RENEW_JITTER: f64 = 0.05;

pub const SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN: &str = "sasl.kerberos.min.time.before.relogin";
pub const DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN: i64 = 60_000;

pub const SASL_LOGIN_REFRESH_WINDOW_FACTOR: &str = "sasl.login.refresh.window.factor";
pub const DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR: f64 = 0.80;

pub const SASL_LOGIN_REFRESH_WINDOW_JITTER: &str = "sasl.login.refresh.window.jitter";
pub const DEFAULT_LOGIN_REFRESH_WINDOW_JITTER: f64 = 0.05;

pub const SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS: &str = "sasl.login.refresh.min.period.seconds";
pub const DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS: i16 = 60;

pub const SASL_LOGIN_REFRESH_BUFFER_SECONDS: &str = "sasl.login.refresh.buffer.seconds";
pub const DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS: i16 = 300;

pub const SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL: &str = "sasl.oauthbearer.token.endpoint.url";
pub const SASL_OAUTHBEARER_JWKS_ENDPOINT_URL: &str = "sasl.oauthbearer.jwks.endpoint.url";
pub const SASL_OAUTHBEARER_SCOPE_CLAIM_NAME: &str = "sasl.oauthbearer.scope.claim.name";
pub const SASL_OAUTHBEARER_SUB_CLAIM_NAME: &str = "sasl.oauthbearer.sub.claim.name";
pub const SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID: &str = "sasl.oauthbearer.client.credentials.client.id";
pub const SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET: &str = "sasl.oauthbearer.client.credentials.client.secret";
pub const SASL_OAUTHBEARER_SCOPE: &str = "sasl.oauthbearer.scope";
