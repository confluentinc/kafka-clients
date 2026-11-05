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
using System.Collections.Generic;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Errors;

/// <summary>
/// The client has attempted to perform an operation on an invalid topic. For example the topic name is too long,
/// contains invalid characters etc. This exception is not retriable because the operation won't suddenly become valid.
/// Java's <c>org.apache.kafka.common.errors.InvalidTopicException</c>.
/// </summary>
public class InvalidTopicException : global::Confluent.Kafka.Common.Errors.InvalidConfigurationException
{
    /// <summary>Initializes the exception, as Java's <c>InvalidTopicException()</c>.</summary>
    public InvalidTopicException()
    {
        InvalidTopics = Array.Empty<string>();
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>InvalidTopicException(String message, Throwable cause)</c>.
    /// </summary>
    public InvalidTopicException(string? message, Exception? innerException)
        : base(message, innerException)
    {
        InvalidTopics = Array.Empty<string>();
    }

    /// <summary>Initializes the exception, as Java's <c>InvalidTopicException(String message)</c>.</summary>
    public InvalidTopicException(string? message)
        : base(message)
    {
        InvalidTopics = Array.Empty<string>();
    }

    /// <summary>Initializes the exception, as Java's <c>InvalidTopicException(Throwable cause)</c>.</summary>
    public InvalidTopicException(Exception? innerException)
        : base(innerException)
    {
        InvalidTopics = Array.Empty<string>();
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>InvalidTopicException(Set&lt;String&gt; invalidTopics)</c>.
    /// </summary>
    public InvalidTopicException(IReadOnlyCollection<string> invalidTopics)
        : base("Invalid topics: " + JavaText.ValueOf(invalidTopics))
    {
        InvalidTopics = invalidTopics;
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>InvalidTopicException(String message, Set&lt;String&gt;
    /// invalidTopics)</c>.
    /// </summary>
    public InvalidTopicException(string? message, IReadOnlyCollection<string> invalidTopics)
        : base(message)
    {
        InvalidTopics = invalidTopics;
    }

    internal InvalidTopicException(CoreError _, string? message, Exception? innerException, IReadOnlyCollection<string> invalidTopics)
        : base(_, message, innerException)
    {
        InvalidTopics = invalidTopics;
    }

    /// <summary>Java's <c>invalidTopics()</c>.</summary>
    public IReadOnlyCollection<string> InvalidTopics { get; }

    internal override int FfiId => 17;
}
