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

/// <summary>Java's <c>org.apache.kafka.common.errors.TopicAuthorizationException</c>.</summary>
public class TopicAuthorizationException : global::Confluent.Kafka.Common.Errors.AuthorizationException
{
    /// <summary>
    /// Initializes the exception, as Java's <c>TopicAuthorizationException(String message, Set&lt;String&gt;
    /// unauthorizedTopics)</c>.
    /// </summary>
    public TopicAuthorizationException(string? message, IReadOnlyCollection<string> unauthorizedTopics)
        : base(message)
    {
        UnauthorizedTopics = unauthorizedTopics;
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>TopicAuthorizationException(Set&lt;String&gt; unauthorizedTopics)</c>.
    /// </summary>
    public TopicAuthorizationException(IReadOnlyCollection<string> unauthorizedTopics)
        : this("Not authorized to access topics: " + JavaText.ValueOf(unauthorizedTopics), unauthorizedTopics)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>TopicAuthorizationException(String message)</c>.</summary>
    public TopicAuthorizationException(string? message)
        : this(message, Array.Empty<string>())
    {
    }

    internal TopicAuthorizationException(CoreError _, string? message, Exception? innerException, IReadOnlyCollection<string> unauthorizedTopics)
        : base(_, message, innerException)
    {
        UnauthorizedTopics = unauthorizedTopics;
    }

    /// <summary>Java's <c>unauthorizedTopics()</c>.</summary>
    public IReadOnlyCollection<string> UnauthorizedTopics { get; }

    internal override int FfiId => 29;
}
