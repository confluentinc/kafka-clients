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

using System;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Config;

/// <summary>
/// An invalid configuration supplied by the user, Java's <c>ConfigException</c>: a <see cref="KafkaException"/> whose
/// <see cref="KafkaException.Code"/> is -10 and which is not retriable.
/// </summary>
public class ConfigException : KafkaException
{
    /// <summary>Initializes an exception with a message.</summary>
    public ConfigException(string? message)
        : base(Config, message, isRetriable: false)
    {
    }

    /// <summary>
    /// Initializes an exception for an invalid value of a configuration, whose message is
    /// "Invalid value <paramref name="value"/> for configuration <paramref name="name"/>".
    /// </summary>
    public ConfigException(string? name, object? value)
        : this(name, value, null)
    {
    }

    /// <summary>
    /// Initializes an exception for an invalid value of a configuration, whose message is
    /// "Invalid value <paramref name="value"/> for configuration <paramref name="name"/>", followed by
    /// ": <paramref name="message"/>" when <paramref name="message"/> is not <see langword="null"/>.
    /// </summary>
    public ConfigException(string? name, object? value, string? message)
        : base(Config, InvalidValueMessage(name, value, message), isRetriable: false)
    {
    }

    // Private behind a factory: an internal (string?, Exception?) constructor would win overload resolution over the
    // public (name, value) one for a null value inside the assembly and its test assembly.
    private ConfigException(string? message, Exception? innerException)
        : base(Config, message, isRetriable: false, innerException)
    {
    }

    internal static ConfigException FromCore(string? message, Exception? innerException) =>
        new ConfigException(message, innerException);

    private static string InvalidValueMessage(string? name, object? value, string? message) =>
        "Invalid value " + JavaText.ValueOf(value) + " for configuration " + JavaText.ValueOf(name)
        + (message is null ? string.Empty : ": " + message);
}
