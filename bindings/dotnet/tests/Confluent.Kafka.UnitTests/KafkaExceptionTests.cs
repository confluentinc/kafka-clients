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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public <see cref="KafkaException"/> error model: operational errors from
/// the core map to a flat <see cref="KafkaException"/> (code, message and the
/// hierarchy predicates) via <c>KafkaException.FromHandle</c>, which frees the error
/// handle (ffi §A5/§B5). The broker-free operational error source is
/// <c>KafkaConsumer_new</c> with <c>group.protocol=classic</c> (PLAN D1). The M17/P1
/// S2 tests (decision D6) that need an error handle build it with
/// <c>kafka_common_Error_new</c>, which accepts any code.
/// </summary>
public sealed class KafkaExceptionTests
{
    // The Kafka protocol code for UNSUPPORTED_VERSION (org.apache.kafka Errors),
    // which the core's `unsupported_version` error carries.
    private const int UnsupportedVersionCode = 35;

    private static Dictionary<string, string> BaseConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    [Fact]
    public void ClassicGroupProtocol_ThrowsFlatKafkaException_UnsupportedVersion()
    {
        Dictionary<string, string> config = BaseConfig();
        config["group.protocol"] = "classic";

        KafkaException ex = Assert.Throws<KafkaException>(() => NativeConsumer.Create(config));

        Assert.Equal(UnsupportedVersionCode, ex.Code);

        // The I1-guard FALSE case: unsupported_version is not retriable. A missing
        // [MarshalAs(I1)] would read a 4-byte BOOL and could flip this — asserting
        // false pins the bool marshalling.
        Assert.False(ex.IsRetriable);

        Assert.Contains("Classic group protocol", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultGroupProtocol_IsClassic_ThrowsUnsupportedVersion()
    {
        // No group.protocol set → the core defaults to "classic" (PLAN D1).
        KafkaException ex = Assert.Throws<KafkaException>(() => NativeConsumer.Create(BaseConfig()));

        Assert.Equal(UnsupportedVersionCode, ex.Code);
        Assert.Contains("Classic group protocol", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NonAsciiInvalidConfigValue_ErrorMessageEchoesValue()
    {
        // An invalid group.protocol value fails config validation, and the message
        // echoes the value — round-tripping non-ASCII through the error message
        // (the NUL-terminated output form, ffi §A3/§B3).
        Dictionary<string, string> config = BaseConfig();
        config["group.protocol"] = "café";

        KafkaException ex = Assert.Throws<KafkaException>(() => NativeConsumer.Create(config));

        Assert.Contains("café", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FromHandle_NullHandle_ReturnsNull()
    {
        // A null (IntPtr.Zero) error handle means success — no throw, null result.
        Assert.Null(KafkaException.FromHandle(IntPtr.Zero));
    }

    [Fact]
    public void ErrorPath_RepeatedManyTimes_NoDoubleFreeCorruption()
    {
        // FromHandle frees the error handle in a finally, exactly once. A double
        // free would corrupt the native allocator and crash under repetition, so
        // driving the error path many times is the practical single-free guard.
        Dictionary<string, string> config = BaseConfig();
        config["group.protocol"] = "classic";

        for (int i = 0; i < 200; i++)
        {
            Assert.Throws<KafkaException>(() => NativeConsumer.Create(config));
        }
    }

    // ---- M17/P1 S2: the five hierarchy predicates (decision D6) ----

    // The expected sets, derived from the Java sources and not from the core. A code
    // belongs to a set when the exception class that Errors.java maps it to is, or
    // transitively extends, the class the property tests. Classes and their `extends`
    // clauses: kafka/clients/src/main/java/org/apache/kafka/common/errors/*.java, plus
    // kafka/clients/src/main/java/org/apache/kafka/common/InvalidRecordException.java.
    // Code -> class: kafka/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java.
    // A disagreement with the core is a core finding (PLAN §5.2 S2), not a test to adjust.

    // The code of TransactionAbortableException, a leaf.
    private static readonly int[] s_transactionAbortable =
    {
        120, // TRANSACTION_ABORTABLE: TransactionAbortableException
    };

    // The codes of the subclasses of ApplicationRecoverableException, which is abstract.
    private static readonly int[] s_applicationRecoverable =
    {
        22, // ILLEGAL_GENERATION: IllegalGenerationException
        25, // UNKNOWN_MEMBER_ID: UnknownMemberIdException
        47, // INVALID_PRODUCER_EPOCH: InvalidProducerEpochException
        49, // INVALID_PRODUCER_ID_MAPPING: InvalidPidMappingException
        82, // FENCED_INSTANCE_ID: FencedInstanceIdException
        90, // PRODUCER_FENCED: ProducerFencedException
    };

    // The codes of the subclasses of AuthorizationException, which extends
    // InvalidConfigurationException. AuthorizationException itself has no code.
    private static readonly int[] s_authorization =
    {
        29, // TOPIC_AUTHORIZATION_FAILED: TopicAuthorizationException
        30, // GROUP_AUTHORIZATION_FAILED: GroupAuthorizationException
        31, // CLUSTER_AUTHORIZATION_FAILED: ClusterAuthorizationException
        53, // TRANSACTIONAL_ID_AUTHORIZATION_FAILED: TransactionalIdAuthorizationException
        65, // DELEGATION_TOKEN_AUTHORIZATION_FAILED: DelegationTokenAuthorizationException
    };

    // The codes of the subclasses of AuthenticationException, which extends
    // InvalidConfigurationException. AuthenticationException itself has no code.
    private static readonly int[] s_authentication =
    {
        33, // UNSUPPORTED_SASL_MECHANISM: UnsupportedSaslMechanismException
        34, // ILLEGAL_SASL_STATE: IllegalSaslStateException
        58, // SASL_AUTHENTICATION_FAILED: SaslAuthenticationException
    };

    // The codes of InvalidConfigurationException itself and of its other direct subclasses.
    private static readonly int[] s_invalidConfigurationProper =
    {
        17, // INVALID_TOPIC_EXCEPTION: InvalidTopicException
        18, // RECORD_LIST_TOO_LARGE: RecordBatchTooLargeException
        21, // INVALID_REQUIRED_ACKS: InvalidRequiredAcksException
        35, // UNSUPPORTED_VERSION: UnsupportedVersionException
        38, // INVALID_REPLICATION_FACTOR: InvalidReplicationFactorException
        40, // INVALID_CONFIG: InvalidConfigurationException
        43, // UNSUPPORTED_FOR_MESSAGE_FORMAT: UnsupportedForMessageFormatException
        87, // INVALID_RECORD: org.apache.kafka.common.InvalidRecordException
    };

    // The codes of OutOfOrderSequenceException and of its subclass UnknownProducerIdException.
    private static readonly int[] s_outOfOrderSequence =
    {
        45, // OUT_OF_ORDER_SEQUENCE_NUMBER: OutOfOrderSequenceException
        59, // UNKNOWN_PRODUCER_ID: UnknownProducerIdException
    };

    /// <summary>
    /// Each D6 row, built with <c>kafka_common_Error_new(code, message)</c> and read through
    /// <c>KafkaException.FromHandle</c>, reads back its code, its message verbatim,
    /// <see cref="KafkaException.IsRetriable"/> and each of D6's predicates exactly as the
    /// row says (PLAN D6's truth table). The flags are compared as one rendered string in
    /// which each value carries its column's name.
    /// </summary>
    [Theory]
    [InlineData(120, "TRANSACTION_ABORTABLE", true, false, false, false, false, false)]
    [InlineData(90, "PRODUCER_FENCED", false, true, false, false, false, false)]
    [InlineData(47, "INVALID_PRODUCER_EPOCH", false, true, false, false, false, false)]
    [InlineData(29, "TOPIC_AUTHORIZATION_FAILED", false, false, true, true, false, false)]
    [InlineData(53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", false, false, true, true, false, false)]
    [InlineData(35, "UNSUPPORTED_VERSION", false, false, true, false, false, false)]
    [InlineData(45, "OUT_OF_ORDER_SEQUENCE_NUMBER", false, false, false, false, true, false)]
    [InlineData(59, "UNKNOWN_PRODUCER_ID", false, false, false, false, true, false)]
    [InlineData(7, "REQUEST_TIMED_OUT", false, false, false, false, false, true)]
    [InlineData(48, "INVALID_TXN_STATE", false, false, false, false, false, false)]
    public void TheD6TruthTable_EachRowReadsBackWhole(
        int code,
        string name,
        bool transactionAbortable,
        bool applicationRecoverable,
        bool invalidConfiguration,
        bool authorization,
        bool outOfOrderSequence,
        bool retriable)
    {
        string message = "S2 row " + name;

        KafkaException ex = FromNewError(code, message);

        Assert.Equal(code, ex.Code);
        Assert.Equal(message, ex.Message);
        Assert.Equal(
            Flags(transactionAbortable, applicationRecoverable, invalidConfiguration, authorization, outOfOrderSequence, retriable),
            Flags(ex));
    }

    /// <summary>
    /// <c>kafka_common_Error_new</c> maps a code that has no assigned error to
    /// <c>UNKNOWN_SERVER_ERROR</c>: here 1000, inside the <c>int16</c> protocol range, and
    /// 40000, outside it. Each reads back as <c>Code == -1</c> with its message verbatim,
    /// and answers <see langword="false"/> to <see cref="KafkaException.IsRetriable"/> and
    /// to each of D6's predicates. This is why the sweep below skips a code that reads
    /// back as -1.
    /// </summary>
    [Theory]
    [InlineData(1000)]
    [InlineData(40000)]
    public void ACodeWithNoAssignedError_ReadsBackAsUnknownServerError(int code)
    {
        string message = "S2 unassigned " + code.ToString(CultureInfo.InvariantCulture);

        KafkaException ex = FromNewError(code, message);

        Assert.Equal(-1, ex.Code);
        Assert.Equal(message, ex.Message);
        Assert.Equal(Flags(false, false, false, false, false, false), Flags(ex));
    }

    /// <summary>
    /// These constructors, none of which takes an error handle, leave each of D6's
    /// predicates <see langword="false"/>: <see cref="KafkaException"/>'s public <c>()</c>,
    /// <c>(message)</c> and <c>(message, inner)</c>, its internal
    /// <c>(code, message, isRetriable)</c>, and <see cref="SerializationException"/>'s
    /// <c>()</c>, <c>(message)</c> and <c>(message, inner)</c>. The public ones also leave
    /// <c>Code</c> 0 and <c>IsRetriable</c> false. The internal one keeps the code and the
    /// flag it is given. It is given 29, an authorization code (the truth table's row 29),
    /// so deriving <see cref="KafkaException.IsInvalidConfigurationError"/> or
    /// <see cref="KafkaException.IsAuthorizationError"/> from the code would show here.
    /// </summary>
    [Fact]
    public void TheConstructorsWithoutAHandle_LeaveEveryPredicateFalse()
    {
        InvalidOperationException inner = new("inner");
        string allFalse = Flags(false, false, false, false, false, false);

        Assert.Equal(
            new[]
            {
                "KafkaException(): Code=0 " + allFalse,
                "KafkaException(message): Code=0 " + allFalse,
                "KafkaException(message, inner): Code=0 " + allFalse,
                "KafkaException(29, message, true): Code=29 " + Flags(false, false, false, false, false, true),
                "SerializationException(): Code=0 " + allFalse,
                "SerializationException(message): Code=0 " + allFalse,
                "SerializationException(message, inner): Code=0 " + allFalse,
            },
            new[]
            {
                Row("KafkaException()", new KafkaException()),
                Row("KafkaException(message)", new KafkaException("m")),
                Row("KafkaException(message, inner)", new KafkaException("m", inner)),
                Row("KafkaException(29, message, true)", new KafkaException(29, "m", isRetriable: true)),
                Row("SerializationException()", new SerializationException()),
                Row("SerializationException(message)", new SerializationException("m")),
                Row("SerializationException(message, inner)", new SerializationException("m", inner)),
            });
    }

    /// <summary>
    /// The both-directions sweep (root <c>CLAUDE.md</c> §10.4). For every code in -1..200
    /// except 0 that has an assigned error, built with <c>kafka_common_Error_new</c> and
    /// read through <c>KafkaException.FromHandle</c>, the property equals membership in its
    /// expected set above. A swept code wrongly added to the core's set, or missing from
    /// it, therefore turns this red, including a code the ten-row table does not sample. A
    /// code that reads back as -1 when it is not -1 has no assigned error and is skipped,
    /// the core test's own rule (<c>errors.rs:2016-2020</c>). Every other code must read
    /// back as itself, and every member of the expected set must be among the codes
    /// checked, so a skipped member fails the test instead of passing unseen.
    /// </summary>
    [Theory]
    [InlineData(nameof(KafkaException.IsTransactionAbortableError))]
    [InlineData(nameof(KafkaException.IsApplicationRecoverableError))]
    [InlineData(nameof(KafkaException.IsInvalidConfigurationError))]
    [InlineData(nameof(KafkaException.IsAuthorizationError))]
    [InlineData(nameof(KafkaException.IsOutOfOrderSequenceError))]
    public void EachPredicate_MatchesTheJavaExtendsChain_InBothDirections(string property)
    {
        (int[] expected, Func<KafkaException, bool> read) = PredicateUnderTest(property);

        List<string> mismatches = new();
        HashSet<int> checkedCodes = new();
        for (int code = -1; code <= 200; code++)
        {
            if (code == 0)
            {
                continue;
            }

            KafkaException ex = FromNewError(code, "S2 sweep");
            if (code != -1 && ex.Code == -1)
            {
                continue;
            }

            Assert.Equal(code, ex.Code);
            checkedCodes.Add(code);

            bool inJavaSet = Array.IndexOf(expected, code) >= 0;
            bool actual = read(ex);
            if (actual != inJavaSet)
            {
                mismatches.Add(string.Format(
                    CultureInfo.InvariantCulture,
                    "code {0}: {1} is {2}, the Java extends chain says {3}",
                    code,
                    property,
                    actual,
                    inJavaSet));
            }
        }

        Assert.Empty(mismatches);
        Assert.Subset(checkedCodes, new HashSet<int>(expected));
    }

    /// <summary>
    /// The expected set for <paramref name="property"/> and a reader for it. The
    /// InvalidConfiguration set holds the codes of every class under
    /// <c>InvalidConfigurationException</c>: the class itself and its other direct
    /// subclasses, and the subclasses of <c>AuthenticationException</c> and
    /// <c>AuthorizationException</c>, which extend it.
    /// </summary>
    private static (int[] Expected, Func<KafkaException, bool> Read) PredicateUnderTest(string property) =>
        property switch
        {
            nameof(KafkaException.IsTransactionAbortableError) =>
                (s_transactionAbortable, e => e.IsTransactionAbortableError),
            nameof(KafkaException.IsApplicationRecoverableError) =>
                (s_applicationRecoverable, e => e.IsApplicationRecoverableError),
            nameof(KafkaException.IsInvalidConfigurationError) =>
                (s_invalidConfigurationProper.Concat(s_authentication).Concat(s_authorization).ToArray(),
                    e => e.IsInvalidConfigurationError),
            nameof(KafkaException.IsAuthorizationError) =>
                (s_authorization, e => e.IsAuthorizationError),
            nameof(KafkaException.IsOutOfOrderSequenceError) =>
                (s_outOfOrderSequence, e => e.IsOutOfOrderSequenceError),
            _ => throw new ArgumentOutOfRangeException(nameof(property), property, "Unknown predicate."),
        };

    /// <summary>
    /// Builds an owned error with <c>kafka_common_Error_new</c> and maps it through
    /// <c>KafkaException.FromHandle</c>, which reads every value out and then frees the
    /// handle. The message is copied by the core, so its pin may end after the call.
    /// </summary>
    private static KafkaException FromNewError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);

        KafkaException? ex = KafkaException.FromHandle(error);
        Assert.NotNull(ex);
        return ex!;
    }

    private static string Row(string constructor, KafkaException ex) =>
        constructor + ": Code=" + ex.Code.ToString(CultureInfo.InvariantCulture) + " " + Flags(ex);

    private static string Flags(KafkaException ex) =>
        Flags(
            ex.IsTransactionAbortableError,
            ex.IsApplicationRecoverableError,
            ex.IsInvalidConfigurationError,
            ex.IsAuthorizationError,
            ex.IsOutOfOrderSequenceError,
            ex.IsRetriable);

    private static string Flags(
        bool transactionAbortable,
        bool applicationRecoverable,
        bool invalidConfiguration,
        bool authorization,
        bool outOfOrderSequence,
        bool retriable) =>
        string.Format(
            CultureInfo.InvariantCulture,
            "TransactionAbortable={0} ApplicationRecoverable={1} InvalidConfiguration={2} Authorization={3} OutOfOrderSequence={4} Retriable={5}",
            transactionAbortable,
            applicationRecoverable,
            invalidConfiguration,
            authorization,
            outOfOrderSequence,
            retriable);
}
