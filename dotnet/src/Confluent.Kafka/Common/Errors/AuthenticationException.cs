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

/// <summary>
/// This exception indicates that SASL authentication has failed. On authentication failure, clients abort the operation
/// requested and raise one of the subclasses of this exception:
/// <list type="bullet">
/// <item><description><c>SaslAuthenticationException</c> if SASL handshake fails with invalid credentials or any other
/// failure specific to the SASL mechanism used for authentication</description></item>
/// <item><description><c>UnsupportedSaslMechanismException</c> if the SASL mechanism requested by the client is not
/// supported on the broker.</description></item>
/// <item><description><c>IllegalSaslStateException</c> if an unexpected request is received on during SASL handshake.
/// This could be due to misconfigured security protocol.</description></item>
/// <item><description><c>SslAuthenticationException</c> if SSL handshake failed due to any
/// <c>SSLException</c>.</description></item>
/// </list>
/// Java's <c>org.apache.kafka.common.errors.AuthenticationException</c>.
/// </summary>
public class AuthenticationException : global::Confluent.Kafka.Common.Errors.InvalidConfigurationException
{
    /// <summary>Initializes the exception, as Java's <c>AuthenticationException(String message)</c>.</summary>
    public AuthenticationException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>AuthenticationException(Throwable cause)</c>.</summary>
    public AuthenticationException(Exception? innerException)
        : base(innerException)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>AuthenticationException(String message, Throwable cause)</c>.
    /// </summary>
    public AuthenticationException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    internal AuthenticationException(CoreError _, string? message, Exception? innerException)
        : base(_, message, innerException)
    {
    }

    internal override int FfiId => -7;
}
