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

/// <summary>
/// The client-config form a SASL mapping targets: Java's <c>sasl.jaas.config</c> (this binding, v3) or librdkafka's
/// <c>sasl.username</c> / <c>sasl.password</c> (confluent-kafka-dotnet, v2).
/// </summary>
public enum SaslForm
{
    Java,

    Librdkafka,
}

/// <summary>
/// SASL config from the environment, Python's <c>sasl_config_from_env</c>: keys only for <c>SASL_PLAINTEXT</c> or
/// <c>SASL_SSL</c> with a mechanism, username and password, in the requested <see cref="SaslForm"/>.
/// </summary>
public static class SaslConfig
{
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
            // Python's jaas string, its "\n\t" separators included.
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
