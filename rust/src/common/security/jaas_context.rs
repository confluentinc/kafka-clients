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

//! The JAAS login context of a SASL client.
//!
//! Translated from `org.apache.kafka.common.security.JaasContext`, without the
//! two parts this client has no counterpart for: `loadServerContext`, the
//! broker's context, and `defaultContext`, which reads the JVM's static JAAS
//! configuration (the `java.security.auth.login.config` file) when
//! `sasl.jaas.config` is not set.

use std::collections::HashSet;

use crate::common::Error;
use crate::common::config::SaslConfigs;
use crate::common::config::types::Password;
use crate::common::security::JaasConfig;
use crate::common::security::JaasUtils;
use crate::common::security::jaas_config::AppConfigurationEntry;

/// The JAAS login context of a SASL connection: its name, its type and the
/// login module entries of its configuration.
#[doc(alias = "org.apache.kafka.common.security.JaasContext")]
pub(crate) struct JaasContext {
    name: String,
    r#type: Type,
    configuration: JaasConfig,
    configuration_entries: Vec<AppConfigurationEntry>,
    dynamic_jaas_config: Option<Password>,
}

impl std::fmt::Debug for JaasContext {
    /// Renders no option value and not the `sasl.jaas.config` value, which
    /// hold the credentials.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JaasContext")
            .field("name", &self.name)
            .field("type", &self.r#type)
            .field("configuration_entries", &self.configuration_entries)
            .field("dynamic_jaas_config", &self.dynamic_jaas_config)
            .finish_non_exhaustive()
    }
}

/// The type of the SASL login context, it should be SERVER for the broker and
/// CLIENT for the clients (consumer, producer, etc.). This is used to validate
/// behaviour (e.g. some functionality is only available in the broker or
/// clients).
#[doc(alias = "org.apache.kafka.common.security.JaasContext$Type")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Type {
    /// The context of a client: a producer, a consumer or an admin client.
    Client,
    /// The context of a broker.
    #[expect(dead_code, reason = "the broker's type: this client builds no broker context")]
    Server,
}

impl JaasContext {
    /// The context name of a client's login context.
    const GLOBAL_CONTEXT_NAME_CLIENT: &str = "KafkaClient";

    /// Returns an instance of this class.
    ///
    /// If JAAS configuration property `sasl.jaas.config` is specified, the
    /// configuration object is created by parsing the property value.
    /// Otherwise, the default Configuration is returned. The context name is
    /// always `KafkaClient`.
    ///
    /// Without `sasl.jaas.config` the result is `None`: Java's default
    /// configuration is the JVM's static JAAS configuration, which this client
    /// does not have.
    ///
    /// # Errors
    ///
    /// [`Error::is_local_illegal_argument_error`], Java's
    /// `IllegalArgumentException`, when `sasl.jaas.config` is not valid (see
    /// [`JaasConfig::new`]), names more than one login module, or names a
    /// login module that is not allowed (see
    /// [`Self::return_if_login_module_is_not_allowed`]).
    #[doc(alias = "org.apache.kafka.common.security.JaasContext#loadClientContext")]
    pub(crate) fn load_client_context(configs: &SaslConfigs) -> Result<Option<JaasContext>, Error> {
        let dynamic_jaas_config = configs.jaas_config.as_ref();
        Self::load(Type::Client, Self::GLOBAL_CONTEXT_NAME_CLIENT, dynamic_jaas_config)
    }

    /// The context `global_context_name` of `dynamic_jaas_config`, the value
    /// of `sasl.jaas.config`, or `None` without one.
    ///
    /// Java's `load` also takes the listener's context name, which only its
    /// static-configuration branch reads.
    ///
    /// # Errors
    ///
    /// [`Error::is_local_illegal_argument_error`] as for
    /// [`Self::load_client_context`].
    #[doc(alias = "org.apache.kafka.common.security.JaasContext#load")]
    pub(crate) fn load(
        context_type: Type,
        global_context_name: &str,
        dynamic_jaas_config: Option<&Password>,
    ) -> Result<Option<JaasContext>, Error> {
        let Some(dynamic_jaas_config) = dynamic_jaas_config else {
            return Ok(None);
        };
        let jaas_config = JaasConfig::new(global_context_name, dynamic_jaas_config.value())?;
        let context_modules = jaas_config.get_app_configuration_entry(global_context_name).unwrap_or_default();
        if context_modules.is_empty() {
            return Err(Error::local_illegal_argument(
                "JAAS config property does not contain any login modules",
            ));
        } else if context_modules.len() != 1 {
            return Err(Error::local_illegal_argument(format!(
                "JAAS config property contains {} login modules, should be 1 module",
                context_modules.len()
            )));
        }
        Self::return_if_login_module_is_not_allowed(&context_modules[0])?;
        Self::new(
            global_context_name,
            context_type,
            jaas_config,
            Some(dynamic_jaas_config.clone()),
        )
        .map(Some)
    }

    /// Fails if the login module of `app_configuration_entry`, its name
    /// trimmed, is not allowed: when the system property
    /// `org.apache.kafka.allowed.login.modules` is set, a module it does not
    /// list; otherwise a module the deprecated
    /// `org.apache.kafka.disallowed.login.modules` lists, by default
    /// `com.sun.security.auth.module.JndiLoginModule` and
    /// `com.sun.security.auth.module.LdapLoginModule`.
    ///
    /// Rust has no JVM system properties, so neither is set: the two default
    /// modules are the ones refused.
    ///
    /// # Errors
    ///
    /// [`Error::is_local_illegal_argument_error`] for a login module that is
    /// not allowed.
    #[doc(alias = "org.apache.kafka.common.security.JaasContext#throwIfLoginModuleIsNotAllowed")]
    pub(crate) fn return_if_login_module_is_not_allowed(
        app_configuration_entry: &AppConfigurationEntry,
    ) -> Result<(), Error> {
        Self::return_if_login_module_is_not_allowed_by(app_configuration_entry, None, None)
    }

    /// [`Self::return_if_login_module_is_not_allowed`] with the values of the
    /// two system properties it reads, `allowed_property` for
    /// `org.apache.kafka.allowed.login.modules` and `disallowed_property` for
    /// `org.apache.kafka.disallowed.login.modules`.
    fn return_if_login_module_is_not_allowed_by(
        app_configuration_entry: &AppConfigurationEntry,
        allowed_property: Option<&str>,
        disallowed_property: Option<&str>,
    ) -> Result<(), Error> {
        if disallowed_property.is_some() {
            log::warn!(
                "System property '{}' is deprecated and will be removed in a future release. Use '{}' instead.",
                JaasUtils::DISALLOWED_LOGIN_MODULES_CONFIG,
                JaasUtils::ALLOWED_LOGIN_MODULES_CONFIG
            );
        }
        let login_module_name = trim(app_configuration_entry.login_module_name());
        if let Some(allowed_property) = allowed_property {
            if !split_and_trim(allowed_property).contains(login_module_name) {
                return Err(Error::local_illegal_argument(format!(
                    "{login_module_name} is not allowed. Update System property '{}' to allow {login_module_name}",
                    JaasUtils::ALLOWED_LOGIN_MODULES_CONFIG
                )));
            }
            return Ok(());
        }
        let disallowed_property = disallowed_property.unwrap_or(JaasUtils::DISALLOWED_LOGIN_MODULES_DEFAULT);
        if split_and_trim(disallowed_property).contains(login_module_name) {
            return Err(Error::local_illegal_argument(format!(
                "{login_module_name} is not allowed. The system property '{}' is deprecated. Use the {} to \
                 allow this module. e.g.,-D{}={login_module_name}",
                JaasUtils::DISALLOWED_LOGIN_MODULES_CONFIG,
                JaasUtils::ALLOWED_LOGIN_MODULES_CONFIG,
                JaasUtils::ALLOWED_LOGIN_MODULES_CONFIG
            )));
        }
        Ok(())
    }

    /// The context `name` of `configuration`.
    ///
    /// # Errors
    ///
    /// [`Error::is_local_illegal_argument_error`] if `configuration` has no
    /// context `name`.
    #[doc(alias = "org.apache.kafka.common.security.JaasContext#JaasContext")]
    pub(crate) fn new(
        name: &str,
        r#type: Type,
        configuration: JaasConfig,
        dynamic_jaas_config: Option<Password>,
    ) -> Result<JaasContext, Error> {
        let Some(entries) = configuration.get_app_configuration_entry(name) else {
            return Err(Error::local_illegal_argument(format!(
                "Could not find a '{name}' entry in this JAAS configuration."
            )));
        };
        let configuration_entries = entries.to_vec();
        Ok(JaasContext {
            name: name.to_owned(),
            r#type,
            configuration,
            configuration_entries,
            dynamic_jaas_config,
        })
    }

    /// The name of the login context.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// The type of the login context.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn r#type(&self) -> Type {
        self.r#type
    }

    /// The JAAS configuration the context was loaded from.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn configuration(&self) -> &JaasConfig {
        &self.configuration
    }

    /// The login module entries of the context.
    pub(crate) fn configuration_entries(&self) -> &[AppConfigurationEntry] {
        &self.configuration_entries
    }

    /// The value of `sasl.jaas.config` the context was parsed from.
    #[cfg_attr(not(test), expect(dead_code))]
    pub(crate) fn dynamic_jaas_config(&self) -> Option<&Password> {
        self.dynamic_jaas_config.as_ref()
    }

    /// Returns the configuration option for `key` from this context.
    /// If login module name is specified, return option value only from that
    /// module.
    #[doc(alias = "org.apache.kafka.common.security.JaasContext#configEntryOption")]
    pub(crate) fn config_entry_option<'a>(
        configuration_entries: &'a [AppConfigurationEntry],
        key: &str,
        login_module_name: Option<&str>,
    ) -> Option<&'a str> {
        configuration_entries
            .iter()
            .filter(|entry| login_module_name.is_none_or(|name| name == entry.login_module_name()))
            .find_map(|entry| entry.options().get(&Some(key.to_owned())))
            .map(String::as_str)
    }
}

/// Java's `String.trim()`: the string without the characters up to `U+0020`
/// at either end.
fn trim(s: &str) -> &str {
    s.trim_matches(|c: char| c <= ' ')
}

/// The trimmed elements of Java's `property.split(",")`, which drops the
/// trailing empty strings of a split, but keeps the one element of a value
/// with no `,`.
fn split_and_trim(property: &str) -> HashSet<&str> {
    let mut elements: Vec<&str> = property.split(',').collect();
    if elements.len() > 1 {
        while elements.last() == Some(&"") {
            elements.pop();
        }
    }
    elements.into_iter().map(trim).collect()
}

/// Translated from `org.apache.kafka.common.security.JaasContextTest`, for the
/// client's dynamic configuration.
///
/// Java's tests also check, for each value, that the JDK's parser of the JVM's
/// static JAAS file reads it as `JaasConfig` does (the second half of
/// `checkConfiguration`, `checkInvalidConfiguration` and
/// `testMultipleLoginModules`, and the listener overrides of
/// `testDisallowedLoginModulesSystemProperty`), and they load the broker's
/// context from such a file (`testLoadForServerWithListenerNameOverride`,
/// `testLoadForServerWithListenerNameAndFallback`,
/// `testLoadForServerWithWrongListenerName`). This client has no static JAAS
/// configuration and no broker context, so those parts are not translated. The
/// system properties Java sets are passed to
/// `return_if_login_module_is_not_allowed_by`.
#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::common::security::jaas_config::LoginModuleControlFlag;
    use crate::common::security::jaas_config::tests::{JAVA_OUTCOMES, assert_entries};

    use LoginModuleControlFlag::{Optional, Required, Requisite, Sufficient};

    type Options = HashMap<String, String>;

    /// `sasl.jaas.config` values a Kafka 4.4.0 producer, consumer and admin
    /// client refuse at construction under `SASL_PLAINTEXT`, each with the
    /// message of the `IllegalArgumentException` that ends the error chain
    /// (empty for the JDK's bare one). Under `PLAINTEXT` each client was built.
    pub(crate) const CLIENT_CONSTRUCTION_REFUSALS: &[(&str, &str)] = &[
        (
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"a\" password=\"b\"",
            "JAAS config entry not terminated by semi-colon",
        ),
        (
            "org.apache.kafka.common.security.plain.PlainLoginModule requiredX username=\"a\" password=\"b\";",
            "Invalid login module control flag 'requiredX' in JAAS config",
        ),
        (
            "org.apache.kafka.common.security.plain.PlainLoginModule username=\"a\" password=\"b\";",
            "Invalid login module control flag 'username' in JAAS config",
        ),
        ("", "Login module not specified in JAAS config"),
        ("   ", "Login module not specified in JAAS config"),
        (
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"a\" password=\"b\"; \
             org.apache.kafka.common.security.plain.PlainLoginModule required username=\"c\" password=\"d\";",
            "JAAS config property contains 2 login modules, should be 1 module",
        ),
        (
            "com.sun.security.auth.module.JndiLoginModule required username=\"a\" password=\"b\";",
            "com.sun.security.auth.module.JndiLoginModule is not allowed. The system property \
             'org.apache.kafka.disallowed.login.modules' is deprecated. Use the \
             org.apache.kafka.allowed.login.modules to allow this module. \
             e.g.,-Dorg.apache.kafka.allowed.login.modules=com.sun.security.auth.module.JndiLoginModule",
        ),
        ("= required username=\"a\" password=\"b\";", ""),
    ];

    /// Asserts that `error` is a client constructor's refusal of a
    /// `sasl.jaas.config` value as Kafka 4.4.0 reports it:
    /// `KafkaException(constructor_message)`, caused by
    /// `KafkaException("Failed to create new NetworkClient")`
    /// (`ClientUtils#createNetworkClient`), caused by the
    /// `IllegalArgumentException(message)` of `JaasConfig` or `JaasContext`.
    pub(crate) fn assert_sasl_jaas_config_refusal(error: &Error, constructor_message: &str, message: &str) {
        assert!(error.is_kafka_error(), "{error:?}");
        assert_eq!(constructor_message, error.message(), "{error:?}");
        let network_client = error.source().unwrap_or_else(|| panic!("no cause: {error:?}"));
        assert!(network_client.is_kafka_error(), "{network_client:?}");
        assert_eq!("Failed to create new NetworkClient", network_client.message());
        let cause = network_client
            .source()
            .unwrap_or_else(|| panic!("no cause: {network_client:?}"));
        assert!(cause.is_local_illegal_argument_error(), "{cause:?}");
        assert!(!cause.is_kafka_error(), "{cause:?}");
        assert_eq!(message, cause.message());
        assert!(cause.source().is_none(), "{cause:?}");
    }

    #[test]
    fn test_config_no_options() {
        check_configuration("test.testConfigNoOptions", Required, &Options::new());
    }

    #[test]
    fn test_control_flag() {
        let control_flags = [Required, Requisite, Sufficient, Optional];
        let options = Options::from([("propName".to_string(), "propValue".to_string())]);
        for control_flag in control_flags {
            check_configuration("test.testControlFlag", control_flag, &options);
        }
    }

    #[test]
    fn test_single_option() {
        let options = Options::from([("propName".to_string(), "propValue".to_string())]);
        check_configuration("test.testSingleOption", Requisite, &options);
    }

    #[test]
    fn test_multiple_options() {
        let options: Options = (0..10).map(|i| (format!("propName{i}"), format!("propValue{i}"))).collect();
        check_configuration("test.testMultipleOptions", Sufficient, &options);
    }

    #[test]
    fn test_quoted_option_value() {
        let options = Options::from([
            ("propName".to_string(), "prop value".to_string()),
            ("propName2".to_string(), "value1 = 1, value2 = 2".to_string()),
        ]);
        let config = format!(
            "test.testQuotedOptionValue required propName=\"{}\" propName2=\"{}\";",
            options["propName"], options["propName2"]
        );
        check_configuration_with(&config, "test.testQuotedOptionValue", Required, &options);
    }

    #[test]
    fn test_quoted_option_name() {
        let options = Options::from([("prop name".to_string(), "propValue".to_string())]);
        let config = "test.testQuotedOptionName required \"prop name\"=propValue;";
        check_configuration_with(config, "test.testQuotedOptionName", Required, &options);
    }

    #[test]
    fn test_multiple_login_modules() {
        let mut builder = String::new();
        let module_count = 3;
        let mut module_options = Vec::new();
        for i in 0..module_count {
            let options = Options::from([
                ("index".to_string(), format!("Index{i}")),
                ("module".to_string(), format!("Module{i}")),
            ]);
            let module = jaas_config_prop(&format!("test.Module{i}"), Required, &options);
            module_options.push(options);
            builder.push(' ');
            builder.push_str(&module);
        }
        let jaas_config_prop = builder;

        let client_context_name = "CLIENT";
        let configuration = JaasConfig::new(client_context_name, &jaas_config_prop).unwrap();
        let dynamic_entries = configuration.get_app_configuration_entry(client_context_name).unwrap();
        assert_eq!(module_count, dynamic_entries.len());

        for (i, entry) in dynamic_entries.iter().enumerate() {
            check_entry(entry, &format!("test.Module{i}"), Required, &module_options[i]);
        }
    }

    #[test]
    fn test_missing_login_module() {
        check_invalid_configuration(
            "  required option1=value1;",
            "Invalid login module control flag 'option1' in JAAS config",
        );
    }

    #[test]
    fn test_missing_control_flag() {
        check_invalid_configuration(
            "test.loginModule option1=value1;",
            "Invalid login module control flag 'option1' in JAAS config",
        );
    }

    #[test]
    fn test_missing_option_value() {
        check_invalid_configuration(
            "loginModule required option1;",
            "Value not specified for key 'option1' in JAAS config",
        );
    }

    #[test]
    fn test_missing_semicolon() {
        check_invalid_configuration(
            "test.testMissingSemicolon required option1=value1",
            "JAAS config entry not terminated by semi-colon",
        );
    }

    #[test]
    fn test_numeric_option_without_quotes() {
        check_invalid_configuration(
            "test.testNumericOptionWithoutQuotes required option1=3;",
            "Value not specified for key 'option1' in JAAS config",
        );
    }

    #[test]
    fn test_invalid_control_flag() {
        check_invalid_configuration(
            "test.testInvalidControlFlag { option1=3;",
            "Login module control flag is not available in the JAAS config",
        );
    }

    #[test]
    fn test_disallowed_login_modules_system_property() {
        // test JndiLoginModule is not allowed by default
        let jaas_config_prop1 = "com.sun.security.auth.module.JndiLoginModule required;";
        assert_eq!(
            not_allowed_message("com.sun.security.auth.module.JndiLoginModule"),
            configuration_entry(Type::Client, Some(jaas_config_prop1))
                .unwrap_err()
                .message()
        );

        // test LdapLoginModule is not allowed by default
        let jaas_config_prop2 = "com.sun.security.auth.module.LdapLoginModule required;";
        assert_eq!(
            not_allowed_message("com.sun.security.auth.module.LdapLoginModule"),
            configuration_entry(Type::Client, Some(jaas_config_prop2))
                .unwrap_err()
                .message()
        );

        // test org.apache.kafka.disallowed.login.modules system property with multiple modules
        let disallowed =
            " com.ibm.security.auth.module.LdapLoginModule , com.ibm.security.auth.module.Krb5LoginModule ";

        let jaas_config_prop3 = "com.ibm.security.auth.module.LdapLoginModule required;";
        let error =
            JaasContext::return_if_login_module_is_not_allowed_by(&entry(jaas_config_prop3), None, Some(disallowed))
                .unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!(
            not_allowed_message("com.ibm.security.auth.module.LdapLoginModule"),
            error.message()
        );

        // clear disallowed login modules
        for login_module in [
            "com.sun.security.auth.module.JndiLoginModule",
            "com.sun.security.auth.module.LdapLoginModule",
        ] {
            let entry = entry(&jaas_config_prop(login_module, Required, &Options::new()));
            check_entry(&entry, login_module, Required, &Options::new());
            JaasContext::return_if_login_module_is_not_allowed_by(&entry, None, Some("")).unwrap();
        }
    }

    #[test]
    fn test_allowed_login_modules_system_property() {
        let ldap = AppConfigurationEntry::new(
            Some("com.ibm.security.auth.module.LdapLoginModule".to_string()),
            Required,
            HashMap::new(),
        )
        .unwrap();
        let jndi = AppConfigurationEntry::new(
            Some("com.sun.security.auth.module.JndiLoginModule".to_string()),
            Required,
            HashMap::new(),
        )
        .unwrap();
        let allowed_error = |module: &str| {
            format!(
                "{module} is not allowed. Update System property 'org.apache.kafka.allowed.login.modules' to allow {module}"
            )
        };
        // default
        JaasContext::return_if_login_module_is_not_allowed(&ldap).unwrap();

        // set allowed list, but not set disallowed list
        let allowed = Some("com.ibm.security.auth.module.LdapLoginModule");
        JaasContext::return_if_login_module_is_not_allowed_by(&ldap, allowed, None).unwrap();
        let error = JaasContext::return_if_login_module_is_not_allowed_by(&jndi, allowed, None).unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!(allowed_error("com.sun.security.auth.module.JndiLoginModule"), error.message());

        // set both allowed list and disallowed list
        let disallowed = Some("com.ibm.security.auth.module.LdapLoginModule");
        JaasContext::return_if_login_module_is_not_allowed_by(&ldap, allowed, disallowed).unwrap();
        let error = JaasContext::return_if_login_module_is_not_allowed_by(&jndi, allowed, disallowed).unwrap_err();
        assert_eq!(allowed_error("com.sun.security.auth.module.JndiLoginModule"), error.message());

        // set disallowed list, but not set allowed list
        let error = JaasContext::return_if_login_module_is_not_allowed_by(&ldap, None, disallowed).unwrap_err();
        // Ensure the error message includes the deprecation warning for the disallowed login modules config
        assert!(
            error
                .message()
                .contains("The system property 'org.apache.kafka.disallowed.login.modules' is deprecated."),
            "{}",
            error.message()
        );
        assert_eq!(
            not_allowed_message("com.ibm.security.auth.module.LdapLoginModule"),
            error.message()
        );
        JaasContext::return_if_login_module_is_not_allowed_by(&jndi, None, disallowed).unwrap();
    }

    #[test]
    fn test_numeric_option_with_quotes() {
        let options = Options::from([("option1".to_string(), "3".to_string())]);
        let config = "test.testNumericOptionWithQuotes required option1=\"3\";";
        check_configuration_with(config, "test.testNumericOptionWithQuotes", Required, &options);
    }

    /// The values of `jaas_config`'s vectors as a client's `sasl.jaas.config`:
    /// what Kafka 4.4.0's `JaasContext.loadClientContext` made of each. It
    /// parses as `JaasConfig` does, then refuses the eight values that name
    /// more or fewer than one login module, or a refused one.
    #[test]
    fn test_load_client_context_matches_java() {
        const REFUSED: &[(&str, &str)] = &[
            (
                " test.Module0 required index=Index0 module=Module0; test.Module1 required index=Index1 \
                 module=Module1; test.Module2 required index=Index2 module=Module2;",
                "JAAS config property contains 3 login modules, should be 1 module",
            ),
            (
                "com.sun.security.auth.module.JndiLoginModule required;",
                "com.sun.security.auth.module.JndiLoginModule is not allowed. The system property \
                 'org.apache.kafka.disallowed.login.modules' is deprecated. Use the \
                 org.apache.kafka.allowed.login.modules to allow this module. \
                 e.g.,-Dorg.apache.kafka.allowed.login.modules=com.sun.security.auth.module.JndiLoginModule",
            ),
            (
                "com.sun.security.auth.module.LdapLoginModule required;",
                "com.sun.security.auth.module.LdapLoginModule is not allowed. The system property \
                 'org.apache.kafka.disallowed.login.modules' is deprecated. Use the \
                 org.apache.kafka.allowed.login.modules to allow this module. \
                 e.g.,-Dorg.apache.kafka.allowed.login.modules=com.sun.security.auth.module.LdapLoginModule",
            ),
            (
                "\" com.sun.security.auth.module.JndiLoginModule \" required;",
                "com.sun.security.auth.module.JndiLoginModule is not allowed. The system property \
                 'org.apache.kafka.disallowed.login.modules' is deprecated. Use the \
                 org.apache.kafka.allowed.login.modules to allow this module. \
                 e.g.,-Dorg.apache.kafka.allowed.login.modules=com.sun.security.auth.module.JndiLoginModule",
            ),
            (
                "m required k=v;m2 required;",
                "JAAS config property contains 2 login modules, should be 1 module",
            ),
            (
                "m required k=v; n required;",
                "JAAS config property contains 2 login modules, should be 1 module",
            ),
            (
                "m required; n required; o required;",
                "JAAS config property contains 3 login modules, should be 1 module",
            ),
            (
                "m required k=v; n optional k=w;",
                "JAAS config property contains 2 login modules, should be 1 module",
            ),
        ];
        for (value, _) in REFUSED {
            assert!(
                JAVA_OUTCOMES.iter().any(|(v, outcome)| v == value && outcome.is_ok()),
                "{value:?}"
            );
        }
        for (value, outcome) in JAVA_OUTCOMES {
            let configs = SaslConfigs { jaas_config: Some(Password::new(*value)), ..SaslConfigs::default() };
            let refused = REFUSED.iter().find(|(v, _)| v == value).map(|(_, message)| *message);
            match (JaasContext::load_client_context(&configs), refused.map_or(*outcome, Err)) {
                (Ok(Some(context)), Ok(entries)) => {
                    assert_entries(value, context.configuration_entries(), entries);
                    assert_eq!("KafkaClient", context.name());
                    assert_eq!(Type::Client, context.r#type());
                    assert_eq!(Some(*value), context.dynamic_jaas_config().map(Password::value));
                    assert_entries(
                        value,
                        context.configuration().get_app_configuration_entry("KafkaClient").unwrap(),
                        entries,
                    );
                },
                (Err(error), Err(message)) => {
                    assert!(error.is_local_illegal_argument_error(), "{value:?}: {error:?}");
                    assert_eq!(message, error.message(), "{value:?}");
                },
                (actual, expected) => panic!("{value:?}: expected {expected:?}, got {actual:?}"),
            }
        }
    }

    /// Without `sasl.jaas.config` there is no context: Java's would be the
    /// JVM's static JAAS configuration.
    #[test]
    fn test_load_client_context_without_sasl_jaas_config() {
        assert!(JaasContext::load_client_context(&SaslConfigs::default()).unwrap().is_none());
    }

    /// `JaasContext.configEntryOption`, as Kafka 4.4.0 answers it: the first
    /// entry with the option, of the named login module if one is named.
    #[test]
    fn test_config_entry_option() {
        let configuration = JaasConfig::new("KafkaClient", "a required k=\"1\"; b required k=\"2\" j=\"3\";").unwrap();
        let entries = configuration.get_app_configuration_entry("KafkaClient").unwrap();
        assert_eq!(Some("1"), JaasContext::config_entry_option(entries, "k", None));
        assert_eq!(Some("2"), JaasContext::config_entry_option(entries, "k", Some("b")));
        assert_eq!(Some("3"), JaasContext::config_entry_option(entries, "j", None));
        assert_eq!(None, JaasContext::config_entry_option(entries, "j", Some("a")));
        assert_eq!(None, JaasContext::config_entry_option(entries, "x", None));
    }

    /// The constructor finds the context's entries in its configuration.
    #[test]
    fn test_new_requires_the_context_in_the_configuration() {
        let configuration = JaasConfig::new("KafkaClient", "m required;").unwrap();
        let error = JaasContext::new("Other", Type::Client, configuration, None).unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!("Could not find a 'Other' entry in this JAAS configuration.", error.message());
    }

    /// Java's `String.trim` and `String.split(",")`, which the module checks
    /// apply to the login module name and the system properties.
    #[test]
    fn test_split_and_trim_is_java_s() {
        assert_eq!(HashSet::from([""]), split_and_trim(""));
        assert_eq!(HashSet::new(), split_and_trim(","));
        assert_eq!(HashSet::from(["a"]), split_and_trim("a,,"));
        assert_eq!(HashSet::from(["", "a"]), split_and_trim(",a"));
        assert_eq!(HashSet::from(["a", "b", ""]), split_and_trim(" a ,\tb, "));
        assert_eq!("a\u{a0}", trim("\u{0}\u{1f} a\u{a0}\n"));
    }

    /// `{:?}` renders neither the `sasl.jaas.config` value nor an option
    /// value.
    #[test]
    fn test_debug_hides_credentials() {
        let configs = SaslConfigs {
            jaas_config: Some(Password::new(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"jaas-user\" password=\"jaas-S3cr3t\";",
            )),
            ..SaslConfigs::default()
        };
        let context = JaasContext::load_client_context(&configs).unwrap().unwrap();
        for rendered in [format!("{context:?}"), format!("{context:#?}")] {
            assert!(!rendered.contains("jaas-S3cr3t"), "{rendered}");
            assert!(!rendered.contains("jaas-user"), "{rendered}");
            assert!(rendered.contains("KafkaClient"), "{rendered}");
        }
    }

    /// Java's message for a login module the default list refuses.
    fn not_allowed_message(login_module: &str) -> String {
        format!(
            "{login_module} is not allowed. The system property 'org.apache.kafka.disallowed.login.modules' is \
             deprecated. Use the org.apache.kafka.allowed.login.modules to allow this module. \
             e.g.,-Dorg.apache.kafka.allowed.login.modules={login_module}"
        )
    }

    /// The single entry `JaasConfig` parses from `jaas_config_prop`, before
    /// any login module check.
    fn entry(jaas_config_prop: &str) -> AppConfigurationEntry {
        let configuration = JaasConfig::new("CLIENT", jaas_config_prop).unwrap();
        let entries = configuration.get_app_configuration_entry("CLIENT").unwrap();
        assert_eq!(1, entries.len());
        entries[0].clone()
    }

    /// The context name of `context_type`: Java's `contextType.name()`.
    fn context_name(context_type: Type) -> &'static str {
        match context_type {
            Type::Client => "CLIENT",
            Type::Server => "SERVER",
        }
    }

    fn configuration_entry(context_type: Type, jaas_config_prop: Option<&str>) -> Result<AppConfigurationEntry, Error> {
        let sasl_jaas_config = jaas_config_prop.map(Password::new);
        let context = JaasContext::load(context_type, context_name(context_type), sasl_jaas_config.as_ref())?
            .expect("a context for the property");
        let entries = context.configuration_entries();
        assert_eq!(1, entries.len());
        Ok(entries[0].clone())
    }

    fn control_flag(login_module_control_flag: LoginModuleControlFlag) -> String {
        // LoginModuleControlFlag.toString() has format "LoginModuleControlFlag: flag"
        let flag = login_module_control_flag.to_string();
        flag.split(' ').next_back().unwrap().to_string()
    }

    fn jaas_config_prop(login_module: &str, control_flag: LoginModuleControlFlag, options: &Options) -> String {
        let mut builder = String::new();
        builder.push_str(login_module);
        builder.push(' ');
        builder.push_str(&self::control_flag(control_flag));
        for (key, value) in options {
            builder.push(' ');
            builder.push_str(key);
            builder.push('=');
            builder.push_str(value);
        }
        builder.push(';');
        builder
    }

    fn check_configuration(login_module: &str, control_flag: LoginModuleControlFlag, options: &Options) {
        let jaas_config_prop = jaas_config_prop(login_module, control_flag, options);
        check_configuration_with(&jaas_config_prop, login_module, control_flag, options);
    }

    fn check_entry(
        entry: &AppConfigurationEntry,
        login_module: &str,
        control_flag: LoginModuleControlFlag,
        options: &Options,
    ) {
        assert_eq!(login_module, entry.login_module_name());
        assert_eq!(control_flag, entry.control_flag());
        let options: HashMap<Option<String>, String> =
            options.iter().map(|(key, value)| (Some(key.clone()), value.clone())).collect();
        assert_eq!(&options, entry.options());
    }

    fn check_configuration_with(
        jaas_config_prop: &str,
        login_module: &str,
        control_flag: LoginModuleControlFlag,
        options: &Options,
    ) {
        let dynamic_entry = configuration_entry(Type::Client, Some(jaas_config_prop)).unwrap();
        check_entry(&dynamic_entry, login_module, control_flag, options);
    }

    /// Java asserts the `IllegalArgumentException` alone; its message, from
    /// Kafka 4.4.0, is asserted too.
    fn check_invalid_configuration(jaas_config_prop: &str, message: &str) {
        let error = configuration_entry(Type::Client, Some(jaas_config_prop))
            .expect_err("Invalid JAAS configuration property didn't throw exception");
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!(message, error.message());
    }
}
