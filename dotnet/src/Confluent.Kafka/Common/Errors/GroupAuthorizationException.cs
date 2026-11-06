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

namespace Confluent.Kafka.Common.Errors;

/// <summary>Java's <c>org.apache.kafka.common.errors.GroupAuthorizationException</c>.</summary>
public class GroupAuthorizationException : global::Confluent.Kafka.Common.Errors.AuthorizationException
{
    /// <summary>
    /// Initializes the exception, as Java's <c>GroupAuthorizationException(String message, String groupId)</c>.
    /// </summary>
    public GroupAuthorizationException(string? message, string? groupId)
        : base(message)
    {
        GroupId = groupId;
    }

    /// <summary>Initializes the exception, as Java's <c>GroupAuthorizationException(String message)</c>.</summary>
    public GroupAuthorizationException(string? message)
        : this(message, null)
    {
    }

    internal GroupAuthorizationException(CoreError _, string? message, Exception? innerException, string? groupId)
        : base(_, message, innerException)
    {
        GroupId = groupId;
    }

    /// <summary>Java's <c>groupId()</c>.</summary>
    public string? GroupId { get; }

    /// <summary>Creates the exception, as Java's <c>forGroupId(String groupId)</c>.</summary>
    public static GroupAuthorizationException ForGroupId(string? groupId)
    {
        return new GroupAuthorizationException("Not authorized to access group: " + JavaText.ValueOf(groupId), groupId);
    }

    internal override int FfiId => 30;
}
