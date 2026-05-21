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

//! Translation of `org.apache.kafka.common.config.SaslConfigs`.
//!
//! Per the Milestone 1 plan, SASL is out of scope for this milestone — these
//! constants exist so the producer config validator can reject unsupported
//! SASL settings with the canonical key name in the error message and so
//! the producer schema includes the SASL keys (matching the Java client's
//! schema surface). The actual SASL handshake stack lands in Phase 9.

pub const SASL_MECHANISM: &str = "sasl.mechanism";
pub const GSSAPI_MECHANISM: &str = "GSSAPI";
pub const DEFAULT_SASL_MECHANISM: &str = GSSAPI_MECHANISM;

pub const SASL_JAAS_CONFIG: &str = "sasl.jaas.config";

// ----- Phase 9b fresh-impl extension: typed PLAIN credentials -----
//
// Java's ProducerConfig does NOT have `sasl.username` / `sasl.password`
// keys — Java users must always go through `sasl.jaas.config`. The
// Rust translation accepts both forms because the JAAS string format
// is awkward for a PLAIN-only client (Milestone 1) and copy-pasting
// passwords through JAAS-escape rules is error-prone.
//
// When BOTH `sasl.jaas.config` and `sasl.username`/`sasl.password` are
// set, the JAAS config wins (matches Java's precedence — JAAS is the
// canonical source). When NEITHER is set on a SASL-bearing protocol,
// producer construction fails with a clear "credentials required"
// message at `KafkaProducer::new` time (Phase 9b commit 6).

/// Fresh-impl convenience: PLAIN-mechanism username. Used as an
/// alternative to `sasl.jaas.config`. Not present in Java's
/// `ProducerConfig` schema.
pub const SASL_USERNAME: &str = "sasl.username";

/// Fresh-impl convenience: PLAIN-mechanism password (treated as a
/// `Type::Password` so it is masked in `Debug`). Not present in
/// Java's `ProducerConfig` schema.
pub const SASL_PASSWORD: &str = "sasl.password";

/// Doc for [`SASL_USERNAME`].
pub const SASL_USERNAME_DOC: &str = "PLAIN-mechanism username. Fresh-impl convenience that bypasses sasl.jaas.config — set this together with sasl.password instead of constructing a full JAAS config string. Only honoured when security.protocol is SASL_PLAINTEXT or SASL_SSL and sasl.mechanism is PLAIN. Not present in the Apache Kafka Java client.";

/// Doc for [`SASL_PASSWORD`].
pub const SASL_PASSWORD_DOC: &str = "PLAIN-mechanism password. Fresh-impl convenience that bypasses sasl.jaas.config — set this together with sasl.username instead of constructing a full JAAS config string. Only honoured when security.protocol is SASL_PLAINTEXT or SASL_SSL and sasl.mechanism is PLAIN. Not present in the Apache Kafka Java client.";

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
// ----- Doc constants (used by `add_client_sasl_support`) -----

pub const SASL_MECHANISM_DOC: &str = "SASL mechanism used for client connections. This may be any mechanism for which a security provider is available. GSSAPI is the default mechanism.";
pub const SASL_JAAS_CONFIG_DOC: &str =
    "JAAS login context parameters for SASL connections in the format used by JAAS configuration files.";
pub const SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC: &str = "The fully qualified name of a SASL client callback handler class that implements the AuthenticateCallbackHandler interface.";
pub const SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC: &str = "The fully qualified name of a SASL login callback handler class that implements the AuthenticateCallbackHandler interface.";
pub const SASL_LOGIN_CLASS_DOC: &str = "The fully qualified name of a class that implements the Login interface.";
pub const SASL_KERBEROS_SERVICE_NAME_DOC: &str = "The Kerberos principal name that Kafka runs as. This can be defined either in Kafka's JAAS config or in Kafka's config.";
pub const SASL_KERBEROS_KINIT_CMD_DOC: &str = "Kerberos kinit command path.";
pub const SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC: &str = "Login thread will sleep until the specified window factor of time from last refresh to ticket's expiry has been reached.";
pub const SASL_KERBEROS_TICKET_RENEW_JITTER_DOC: &str = "Percentage of random jitter added to the renewal time.";
pub const SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC: &str = "Login thread sleep time between refresh attempts.";
pub const SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC: &str = "Login refresh thread will sleep until the specified window factor relative to the credential's lifetime has been reached.";
pub const SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC: &str = "The maximum amount of random jitter relative to the credential's lifetime that is added to the login refresh thread's sleep time.";
pub const SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC: &str =
    "The desired minimum time for the login refresh thread to wait before refreshing a credential, in seconds.";
pub const SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC: &str =
    "The amount of buffer time before credential expiration to maintain when refreshing a credential, in seconds.";

pub const SASL_LOGIN_CONNECT_TIMEOUT_MS: &str = "sasl.login.connect.timeout.ms";
pub const SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC: &str =
    "The (optional) value in milliseconds for the external authentication provider connection timeout.";
pub const SASL_LOGIN_READ_TIMEOUT_MS: &str = "sasl.login.read.timeout.ms";
pub const SASL_LOGIN_READ_TIMEOUT_MS_DOC: &str =
    "The (optional) value in milliseconds for the external authentication provider read timeout.";

pub const SASL_LOGIN_RETRY_BACKOFF_MAX_MS: &str = "sasl.login.retry.backoff.max.ms";
pub const DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS: i64 = 10_000;
pub const SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC: &str =
    "The (optional) maximum wait between login attempts to the external authentication provider.";

pub const SASL_LOGIN_RETRY_BACKOFF_MS: &str = "sasl.login.retry.backoff.ms";
pub const DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS: i64 = 100;
pub const SASL_LOGIN_RETRY_BACKOFF_MS_DOC: &str =
    "The (optional) initial wait between login attempts to the external authentication provider.";

pub const SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS: &str = "sasl.oauthbearer.jwt.retriever.class";
pub const DEFAULT_SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS: &str =
    "org.apache.kafka.common.security.oauthbearer.DefaultJwtRetriever";
pub const SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS_DOC: &str = "The fully-qualified class name of a JwtRetriever implementation used to request tokens from the identity provider.";

pub const SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS: &str = "sasl.oauthbearer.jwt.validator.class";
pub const DEFAULT_SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS: &str =
    "org.apache.kafka.common.security.oauthbearer.DefaultJwtValidator";
pub const SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS_DOC: &str = "The fully-qualified class name of a JwtValidator implementation used to validate the JWT from the identity provider.";

pub const SASL_OAUTHBEARER_SCOPE_DOC: &str =
    "The level of access a client application is granted to a resource or API which is included in the token request.";
pub const SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID_DOC: &str =
    "The ID (defined in/by the OAuth identity provider) to identify the client requesting the token.";
pub const SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET_DOC: &str =
    "The secret of the client requesting the token.";

pub const SASL_OAUTHBEARER_ASSERTION_ALGORITHM: &str = "sasl.oauthbearer.assertion.algorithm";
pub const DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM: &str = "RS256";
pub const SASL_OAUTHBEARER_ASSERTION_ALGORITHM_DOC: &str =
    "The algorithm the Apache Kafka client should use to sign the assertion sent to the identity provider.";
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD: &str = "sasl.oauthbearer.assertion.claim.aud";
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD_DOC: &str =
    "The JWT aud (Audience) claim to include in the JWT assertion.";
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS: &str = "sasl.oauthbearer.assertion.claim.exp.seconds";
pub const DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS: i32 = 300;
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS_DOC: &str =
    "The number of seconds in the future for which the JWT is valid.";
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS: &str = "sasl.oauthbearer.assertion.claim.iss";
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS_DOC: &str =
    "The value used as the iss (Issuer) claim in the JWT assertion.";
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE: &str = "sasl.oauthbearer.assertion.claim.jti.include";
pub const DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE: bool = false;
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE_DOC: &str = "Flag that determines if the JWT assertion should generate a unique ID for the JWT and include it in the jti claim.";
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS: &str = "sasl.oauthbearer.assertion.claim.nbf.seconds";
pub const DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS: i32 = 60;
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS_DOC: &str =
    "The number of seconds in the past from which the JWT is valid.";
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB: &str = "sasl.oauthbearer.assertion.claim.sub";
pub const SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB_DOC: &str =
    "The value used as the sub (Subject) claim in the JWT assertion.";
pub const SASL_OAUTHBEARER_ASSERTION_FILE: &str = "sasl.oauthbearer.assertion.file";
pub const SASL_OAUTHBEARER_ASSERTION_FILE_DOC: &str = "File that contains a pre-generated JWT assertion.";
pub const SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE: &str = "sasl.oauthbearer.assertion.private.key.file";
pub const SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE_DOC: &str =
    "File that contains a private key in PEM format used to sign the JWT assertion.";
pub const SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE: &str = "sasl.oauthbearer.assertion.private.key.passphrase";
pub const SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE_DOC: &str =
    "Optional passphrase to decrypt the private key file specified by sasl.oauthbearer.assertion.private.key.file.";
pub const SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE: &str = "sasl.oauthbearer.assertion.template.file";
pub const SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE_DOC: &str =
    "File containing the JWT headers and/or payload claims used when creating the JWT assertion.";

pub const DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME: &str = "scope";
pub const SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC: &str =
    "The OAuth claim for the scope, defaulting to 'scope'. Override if the OIDC provider uses a different name.";

pub const DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME: &str = "sub";
pub const SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC: &str =
    "The OAuth claim for the subject, defaulting to 'sub'. Override if the OIDC provider uses a different name.";

pub const SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC: &str = "The URL for the OAuth/OIDC identity provider.";
pub const SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC: &str =
    "The OAuth/OIDC provider URL from which the provider's JWKS can be retrieved.";

pub const SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS: &str = "sasl.oauthbearer.jwks.endpoint.refresh.ms";
pub const DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS: i64 = 60 * 60 * 1000;
pub const SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC: &str =
    "The (optional) value in milliseconds for the broker to wait between refreshing its JWKS cache.";

pub const SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS: &str =
    "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms";
pub const DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS: i64 = 10_000;
pub const SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC: &str =
    "The (optional) maximum wait between attempts to retrieve the JWKS from the external authentication provider.";

pub const SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS: &str = "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms";
pub const DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS: i64 = 100;
pub const SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC: &str =
    "The (optional) initial wait between JWKS retrieval attempts from the external authentication provider.";

pub const SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS: &str = "sasl.oauthbearer.clock.skew.seconds";
pub const DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS: i32 = 30;
pub const SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC: &str =
    "The (optional) value in seconds to allow for differences between the OAuth/OIDC identity provider and the broker.";

pub const SASL_OAUTHBEARER_EXPECTED_AUDIENCE: &str = "sasl.oauthbearer.expected.audience";
pub const SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC: &str =
    "The (optional) comma-delimited list of expected audiences for the JWT.";
pub const SASL_OAUTHBEARER_EXPECTED_ISSUER: &str = "sasl.oauthbearer.expected.issuer";
pub const SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC: &str =
    "The (optional) setting for the broker to use to verify that the JWT was created by the expected issuer.";

pub const SASL_OAUTHBEARER_HEADER_URLENCODE: &str = "sasl.oauthbearer.header.urlencode";
pub const DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE: bool = false;
pub const SASL_OAUTHBEARER_HEADER_URLENCODE_DOC: &str = "The (optional) setting to enable the OAuth client to URL-encode the client_id and client_secret in the authorization header.";

pub const SASL_OAUTHBEARER_SCOPE: &str = "sasl.oauthbearer.scope";

use std::sync::Arc;

use crate::common::config::config_def::{
    CaseInsensitiveValidString, ConfigDef, ConfigValue, Importance, Range, Type, ValidList, Validator,
};
use crate::common::errors::KafkaError;

/// Add the standard SASL client configuration options to `def`. Mirrors
/// `SaslConfigs.addClientSaslSupport(ConfigDef)`.
///
/// Phase 7a registers the keys for schema parity. The actual SASL stack
/// is deferred to Phase 9; until then `ProducerConfig` rejects
/// `security.protocol ∈ {SASL_PLAINTEXT, SASL_SSL}` at construction so
/// these keys are never reached.
pub fn add_client_sasl_support(def: &mut ConfigDef) -> Result<(), KafkaError> {
    let any_list_no_null: Arc<dyn Validator> = Arc::new(ValidList::any_non_duplicate_values(true, false));
    let refresh_window_factor: Arc<dyn Validator> = Arc::new(Range::between(0.5, 1.0));
    let refresh_window_jitter: Arc<dyn Validator> = Arc::new(Range::between(0.0, 0.25));
    let refresh_min_period: Arc<dyn Validator> = Arc::new(Range::between(0, 900));
    let refresh_buffer: Arc<dyn Validator> = Arc::new(Range::between(0, 3600));
    let exp_seconds: Arc<dyn Validator> = Arc::new(Range::between(0, 86_400));
    let nbf_seconds: Arc<dyn Validator> = Arc::new(Range::between(0, 3600));
    let assertion_alg: Arc<dyn Validator> = Arc::new(CaseInsensitiveValidString::in_set(["ES256", "RS256"]));

    def.define(
        SASL_KERBEROS_SERVICE_NAME,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_KERBEROS_SERVICE_NAME_DOC,
    )?
    .define(
        SASL_KERBEROS_KINIT_CMD,
        Type::String,
        Some(ConfigValue::String(DEFAULT_KERBEROS_KINIT_CMD.to_owned())),
        None,
        Importance::Low,
        SASL_KERBEROS_KINIT_CMD_DOC,
    )?
    .define(
        SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
        Type::Double,
        Some(ConfigValue::Double(DEFAULT_KERBEROS_TICKET_RENEW_WINDOW_FACTOR)),
        None,
        Importance::Low,
        SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_DOC,
    )?
    .define(
        SASL_KERBEROS_TICKET_RENEW_JITTER,
        Type::Double,
        Some(ConfigValue::Double(DEFAULT_KERBEROS_TICKET_RENEW_JITTER)),
        None,
        Importance::Low,
        SASL_KERBEROS_TICKET_RENEW_JITTER_DOC,
    )?
    .define(
        SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
        Type::Long,
        Some(ConfigValue::Long(DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN)),
        None,
        Importance::Low,
        SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_DOC,
    )?
    .define(
        SASL_LOGIN_REFRESH_WINDOW_FACTOR,
        Type::Double,
        Some(ConfigValue::Double(DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR)),
        Some(refresh_window_factor),
        Importance::Low,
        SASL_LOGIN_REFRESH_WINDOW_FACTOR_DOC,
    )?
    .define(
        SASL_LOGIN_REFRESH_WINDOW_JITTER,
        Type::Double,
        Some(ConfigValue::Double(DEFAULT_LOGIN_REFRESH_WINDOW_JITTER)),
        Some(refresh_window_jitter),
        Importance::Low,
        SASL_LOGIN_REFRESH_WINDOW_JITTER_DOC,
    )?
    .define(
        SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
        Type::Short,
        Some(ConfigValue::Short(DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS)),
        Some(refresh_min_period),
        Importance::Low,
        SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_DOC,
    )?
    .define(
        SASL_LOGIN_REFRESH_BUFFER_SECONDS,
        Type::Short,
        Some(ConfigValue::Short(DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS)),
        Some(refresh_buffer),
        Importance::Low,
        SASL_LOGIN_REFRESH_BUFFER_SECONDS_DOC,
    )?
    .define(
        SASL_MECHANISM,
        Type::String,
        Some(ConfigValue::String(DEFAULT_SASL_MECHANISM.to_owned())),
        None,
        Importance::Medium,
        SASL_MECHANISM_DOC,
    )?
    .define(
        SASL_JAAS_CONFIG,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_JAAS_CONFIG_DOC,
    )?
    .define(
        SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        Type::Class,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_CLIENT_CALLBACK_HANDLER_CLASS_DOC,
    )?
    .define(
        SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        Type::Class,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_LOGIN_CALLBACK_HANDLER_CLASS_DOC,
    )?
    .define(
        SASL_LOGIN_CLASS,
        Type::Class,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_LOGIN_CLASS_DOC,
    )?
    .define(
        SASL_LOGIN_CONNECT_TIMEOUT_MS,
        Type::Int,
        Some(ConfigValue::Null),
        None,
        Importance::Low,
        SASL_LOGIN_CONNECT_TIMEOUT_MS_DOC,
    )?
    .define(
        SASL_LOGIN_READ_TIMEOUT_MS,
        Type::Int,
        Some(ConfigValue::Null),
        None,
        Importance::Low,
        SASL_LOGIN_READ_TIMEOUT_MS_DOC,
    )?
    .define(
        SASL_LOGIN_RETRY_BACKOFF_MAX_MS,
        Type::Long,
        Some(ConfigValue::Long(DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MAX_MS)),
        None,
        Importance::Low,
        SASL_LOGIN_RETRY_BACKOFF_MAX_MS_DOC,
    )?
    .define(
        SASL_LOGIN_RETRY_BACKOFF_MS,
        Type::Long,
        Some(ConfigValue::Long(DEFAULT_SASL_LOGIN_RETRY_BACKOFF_MS)),
        None,
        Importance::Low,
        SASL_LOGIN_RETRY_BACKOFF_MS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS,
        Type::Class,
        Some(ConfigValue::Class(DEFAULT_SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS.to_owned())),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS,
        Type::Class,
        Some(ConfigValue::Class(DEFAULT_SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS.to_owned())),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_SCOPE,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_SCOPE_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_ID_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_CLIENT_CREDENTIALS_CLIENT_SECRET_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_ALGORITHM,
        Type::String,
        Some(ConfigValue::String(DEFAULT_SASL_OAUTHBEARER_ASSERTION_ALGORITHM.to_owned())),
        Some(assertion_alg),
        Importance::Medium,
        SASL_OAUTHBEARER_ASSERTION_ALGORITHM_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_ASSERTION_CLAIM_AUD_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS,
        Type::Int,
        Some(ConfigValue::Int(DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS)),
        Some(exp_seconds),
        Importance::Low,
        SASL_OAUTHBEARER_ASSERTION_CLAIM_EXP_SECONDS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_ASSERTION_CLAIM_ISS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE,
        Type::Boolean,
        Some(ConfigValue::Boolean(DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE)),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_ASSERTION_CLAIM_JTI_INCLUDE_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS,
        Type::Int,
        Some(ConfigValue::Int(DEFAULT_SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS)),
        Some(nbf_seconds),
        Importance::Low,
        SASL_OAUTHBEARER_ASSERTION_CLAIM_NBF_SECONDS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_ASSERTION_CLAIM_SUB_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_FILE,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_ASSERTION_FILE_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_FILE_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_ASSERTION_PRIVATE_KEY_PASSPHRASE_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_ASSERTION_TEMPLATE_FILE_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_SCOPE_CLAIM_NAME,
        Type::String,
        Some(ConfigValue::String(DEFAULT_SASL_OAUTHBEARER_SCOPE_CLAIM_NAME.to_owned())),
        None,
        Importance::Low,
        SASL_OAUTHBEARER_SCOPE_CLAIM_NAME_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_SUB_CLAIM_NAME,
        Type::String,
        Some(ConfigValue::String(DEFAULT_SASL_OAUTHBEARER_SUB_CLAIM_NAME.to_owned())),
        None,
        Importance::Low,
        SASL_OAUTHBEARER_SUB_CLAIM_NAME_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_JWKS_ENDPOINT_URL,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_OAUTHBEARER_JWKS_ENDPOINT_URL_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS,
        Type::Long,
        Some(ConfigValue::Long(DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS)),
        None,
        Importance::Low,
        SASL_OAUTHBEARER_JWKS_ENDPOINT_REFRESH_MS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS,
        Type::Long,
        Some(ConfigValue::Long(DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS)),
        None,
        Importance::Low,
        SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MAX_MS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS,
        Type::Long,
        Some(ConfigValue::Long(DEFAULT_SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS)),
        None,
        Importance::Low,
        SASL_OAUTHBEARER_JWKS_ENDPOINT_RETRY_BACKOFF_MS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS,
        Type::Int,
        Some(ConfigValue::Int(DEFAULT_SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS)),
        None,
        Importance::Low,
        SASL_OAUTHBEARER_CLOCK_SKEW_SECONDS_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_EXPECTED_AUDIENCE,
        Type::List,
        Some(ConfigValue::List(Vec::new())),
        Some(any_list_no_null),
        Importance::Low,
        SASL_OAUTHBEARER_EXPECTED_AUDIENCE_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_EXPECTED_ISSUER,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Low,
        SASL_OAUTHBEARER_EXPECTED_ISSUER_DOC,
    )?
    .define(
        SASL_OAUTHBEARER_HEADER_URLENCODE,
        Type::Boolean,
        Some(ConfigValue::Boolean(DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE)),
        None,
        Importance::Low,
        SASL_OAUTHBEARER_HEADER_URLENCODE_DOC,
    )?
    // Phase 9b fresh-impl extension: typed PLAIN credentials. See
    // module doc for rationale. Both keys default to null; producer
    // construction enforces non-null *only* when security.protocol
    // is SASL_PLAINTEXT/SASL_SSL AND sasl.jaas.config is also unset.
    .define(
        SASL_USERNAME,
        Type::String,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_USERNAME_DOC,
    )?
    .define(
        SASL_PASSWORD,
        Type::Password,
        Some(ConfigValue::Null),
        None,
        Importance::Medium,
        SASL_PASSWORD_DOC,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_client_sasl_support_registers_core_keys() {
        let mut def = ConfigDef::new();
        add_client_sasl_support(&mut def).unwrap();
        for key in [
            SASL_MECHANISM,
            SASL_JAAS_CONFIG,
            SASL_KERBEROS_SERVICE_NAME,
            SASL_LOGIN_CLASS,
            SASL_OAUTHBEARER_JWT_RETRIEVER_CLASS,
            SASL_OAUTHBEARER_JWT_VALIDATOR_CLASS,
            SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
            SASL_OAUTHBEARER_HEADER_URLENCODE,
        ] {
            assert!(def.config_key(key).is_some(), "missing SASL key: {key}");
        }
    }
}
