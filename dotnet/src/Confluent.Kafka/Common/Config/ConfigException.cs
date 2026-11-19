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

// GENERATED, DO NOT EDIT. Produced from the Java source by `cargo xtask generate-error-codes`
// and checked for staleness by `cargo xtask check-generated`.

using System;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Config;

/// <summary>
/// Thrown if the user supplies an invalid configuration. Java's <c>org.apache.kafka.common.config.ConfigException</c>.
/// </summary>
public class ConfigException : global::Confluent.Kafka.Common.KafkaException
{
    /// <summary>Initializes the exception, as Java's <c>ConfigException(String message)</c>.</summary>
    public ConfigException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ConfigException(String name, Object value)</c>.</summary>
    public ConfigException(string? name, object? value)
        : this(name, value, null)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>ConfigException(String name, Object value, String message)</c>.
    /// </summary>
    public ConfigException(string? name, object? value, string? message)
        : base("Invalid value " + JavaText.ValueOf(value) + " for configuration " + JavaText.ValueOf(name) + (message is null ? "" : ": " + JavaText.ValueOf(message)))
    {
    }

    internal ConfigException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => -10;
}
