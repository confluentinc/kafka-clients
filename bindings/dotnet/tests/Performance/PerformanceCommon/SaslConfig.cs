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

using System.Collections.Generic;

namespace Confluent.Kafka.Performance;

/// <summary>The client-config form a SASL mapping targets (§1.2 / §7).</summary>
public enum SaslForm
{
    /// <summary>Java form — <c>sasl.mechanism</c> + a <c>PlainLoginModule</c> <c>sasl.jaas.config</c> (our binding, v3).</summary>
    Java,

    /// <summary>librdkafka form — <c>sasl.mechanism</c> + <c>sasl.username</c> / <c>sasl.password</c> (ckd, v2).</summary>
    Librdkafka,
}

/// <summary>
/// SASL config from the environment, form-parameterized — the C# analog of
/// <c>sasl_config_from_env(v2=...)</c> shared by the Python producer / consumer perf tests (§3.5 / §7).
/// Enabled only when <c>SECURITY_PROTOCOL ∈ {SASL_PLAINTEXT, SASL_SSL}</c> and the mechanism + username +
/// password are all set; otherwise it contributes no keys. The same env inputs map to Java-form or
/// librdkafka-form keys per <paramref name="form"/>, so one config drives both clients.
/// </summary>
public static class SaslConfig
{
    /// <summary>
    /// Returns the SASL config keys for the requested <paramref name="form"/>, or an empty dictionary when
    /// SASL is not enabled (PLAINTEXT/SSL, or incomplete credentials).
    /// </summary>
    public static IReadOnlyDictionary<string, string> FromEnv(SaslForm form)
    {
        var result = new Dictionary<string, string>();

        string? securityProtocol = PerfEnv.GetStringOrNull("SECURITY_PROTOCOL");
        string? mechanism = PerfEnv.GetStringOrNull("SASL_MECHANISM");
        string? username = PerfEnv.GetStringOrNull("SASL_USERNAME");
        string? password = PerfEnv.GetStringOrNull("SASL_PASSWORD");

        bool enabled = (securityProtocol == "SASL_PLAINTEXT" || securityProtocol == "SASL_SSL")
            && !string.IsNullOrEmpty(mechanism)
            && !string.IsNullOrEmpty(username)
            && !string.IsNullOrEmpty(password);
        if (!enabled)
        {
            return result;
        }

        result["security.protocol"] = securityProtocol!;
        result["sasl.mechanism"] = mechanism!;

        if (form == SaslForm.Java)
        {
            // Byte-identical to producer_performance_test.py's jaas string (cosmetic "\n\t" separators).
            result["sasl.jaas.config"] =
                "org.apache.kafka.common.security.plain.PlainLoginModule required \n\t"
                + $"username=\"{username}\" \n\tpassword=\"{password}\";";
        }
        else
        {
            result["sasl.username"] = username!;
            result["sasl.password"] = password!;
        }

        return result;
    }
}
