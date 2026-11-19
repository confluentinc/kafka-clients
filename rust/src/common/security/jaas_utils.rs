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

//! Translated from `org.apache.kafka.common.security.JaasUtils`.

/// The names of the JVM system properties that restrict the login modules a
/// JAAS configuration may name.
///
/// Java's class also names `java.security.auth.login.config`, the JVM's static
/// JAAS configuration file, and the `serviceName` option of Kerberos, neither
/// of which this client reads.
#[doc(alias = "org.apache.kafka.common.security.JaasUtils")]
pub(crate) struct JaasUtils;

impl JaasUtils {
    /// The system property listing the login modules a JAAS configuration may
    /// name, separated by commas.
    #[doc(alias = "org.apache.kafka.common.security.JaasUtils#ALLOWED_LOGIN_MODULES_CONFIG")]
    pub(crate) const ALLOWED_LOGIN_MODULES_CONFIG: &str = "org.apache.kafka.allowed.login.modules";

    /// The system property listing the login modules a JAAS configuration may
    /// not name, separated by commas. Deprecated since Kafka 4.2 in favour of
    /// [`Self::ALLOWED_LOGIN_MODULES_CONFIG`].
    #[doc(alias = "org.apache.kafka.common.security.JaasUtils#DISALLOWED_LOGIN_MODULES_CONFIG")]
    pub(crate) const DISALLOWED_LOGIN_MODULES_CONFIG: &str = "org.apache.kafka.disallowed.login.modules";

    /// The login modules a JAAS configuration may not name when neither
    /// system property is set. Deprecated since Kafka 4.2.
    #[doc(alias = "org.apache.kafka.common.security.JaasUtils#DISALLOWED_LOGIN_MODULES_DEFAULT")]
    pub(crate) const DISALLOWED_LOGIN_MODULES_DEFAULT: &str =
        "com.sun.security.auth.module.JndiLoginModule,com.sun.security.auth.module.LdapLoginModule";
}
