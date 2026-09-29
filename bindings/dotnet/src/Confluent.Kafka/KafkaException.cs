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

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka;

/// <summary>
/// The exception raised for an <b>operational</b> Kafka error surfaced by the
/// Rust core across the C ABI — the .NET realization of the Java client's
/// <c>org.apache.kafka.common.KafkaException</c> hierarchy.
/// </summary>
/// <remarks>
/// <para>
/// This maps the ABI's <c>kafka_common_Error_t</c> handle onto a managed
/// exception: <see cref="Code"/> and the hierarchy predicates carry the
/// error's classification, and <see cref="Exception.Message"/> carries its
/// message. It is raised only for errors that <em>originate in the core</em>;
/// programmer errors (a null or out-of-range argument, use after disposal)
/// are validated in the binding before the native call and raise the
/// standard .NET exceptions (<see cref="ArgumentNullException"/>,
/// <see cref="ArgumentException"/>, <see cref="ObjectDisposedException"/>, …)
/// instead — never this type.
/// </para>
/// <para>
/// The type is intentionally <b>flat</b>: the ABI identifies an error by its code
/// and answers Java's class-hierarchy questions through predicates, so a single
/// exception carrying the code, the message and those answers is the faithful
/// shape today. A Java-style hierarchy of typed subclasses <em>may</em> be
/// introduced later, deriving from this same base — so <c>catch (KafkaException)</c>
/// keeps working unchanged. That is a purely additive, non-breaking evolution;
/// no timeline is implied.
/// </para>
/// <para>
/// <b>The hierarchy predicates</b> are <see cref="IsRetriable"/>, which tests Java's
/// <c>RetriableException</c>, and <see cref="IsTransactionAbortableError"/>,
/// <see cref="IsApplicationRecoverableError"/>, <see cref="IsInvalidConfigurationError"/>,
/// <see cref="IsAuthorizationError"/> and <see cref="IsOutOfOrderSequenceError"/>, which
/// each name the Java class they test. Where Java code tests an error with
/// <c>instanceof</c> one of those classes, test the matching property here. Each is the
/// core's answer for the error's Java class, copied out of the error handle when the
/// exception is built from one. They encode Java's <c>extends</c> chain, so they are
/// <b>not</b> complements of one another: one error can answer <see langword="true"/> to
/// several (29 and 53 do, to both <see cref="IsAuthorizationError"/> and
/// <see cref="IsInvalidConfigurationError"/>) or to none (48,
/// <c>INVALID_TXN_STATE</c>).
/// </para>
/// <para>
/// Each answer follows the error's class, not its <see cref="Code"/>: the codes each
/// property lists are those of the classes it answers <see langword="true"/> for, and an
/// error the core raises as Java's base <c>KafkaException</c>, rather than as one of its
/// subclasses, answers <see langword="false"/> to every hierarchy predicate whatever
/// <see cref="Code"/> it carries.
/// </para>
/// <para>
/// A <b>leaf</b> class needs no predicate (root <c>CLAUDE.md</c> §10.4): compare
/// <see cref="Code"/> instead. <c>ProducerFencedException</c> is such a leaf, so a
/// fenced producer is recognized by <c>Code == 90</c> (<c>PRODUCER_FENCED</c>).
/// </para>
/// <para>
/// There is no <c>IsFatal</c> flag: the Rust core's error redesign
/// (<c>a8205c5c</c>, "Error redesign") deliberately does not expose fatality
/// on the ABI's error handle — fatality is contextual (the same error class is
/// fatal in one call path and recoverable in another), not a property of the
/// error's type, so the core keeps it as a free function with a single
/// internal caller (CLAUDE.md §10.4) rather than a queryable flag. The
/// binding cannot recover a faithful value here and does not attempt to fake
/// one.
/// </para>
/// </remarks>
public class KafkaException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaException"/> class with a
    /// default message and no error classification (<see cref="Code"/> is 0 and
    /// every hierarchy predicate is <see langword="false"/>).
    /// </summary>
    public KafkaException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaException"/> class with the
    /// specified message and no error classification (<see cref="Code"/> is 0 and
    /// every hierarchy predicate is <see langword="false"/>).
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public KafkaException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaException"/> class with the
    /// specified message, a reference to the inner exception that is the cause of
    /// this exception, and no error classification (<see cref="Code"/> is 0 and every
    /// hierarchy predicate is <see langword="false"/>).
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">
    /// The exception that is the cause of the current exception, or
    /// <see langword="null"/> if none is specified.
    /// </param>
    public KafkaException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes a new instance with a code, a message and the retriable flag, and
    /// every other hierarchy predicate <see langword="false"/>. It derives nothing from
    /// <paramref name="code"/>: the binding forwards codes verbatim and never branches
    /// on one (ffi §B5).
    /// </summary>
    internal KafkaException(int code, string? message, bool isRetriable)
        : this(
            code,
            message,
            isRetriable,
            isTransactionAbortableError: false,
            isApplicationRecoverableError: false,
            isInvalidConfigurationError: false,
            isAuthorizationError: false,
            isOutOfOrderSequenceError: false)
    {
    }

    /// <summary>
    /// Initializes a new instance with a code, a message, the retriable flag and the answer
    /// of each other hierarchy predicate, each kept as given, and with
    /// <paramref name="innerException"/> as its <see cref="Exception.InnerException"/>. It
    /// derives nothing from <paramref name="code"/>: the binding forwards codes verbatim and
    /// never branches on one (ffi §B5).
    /// </summary>
    internal KafkaException(
        int code,
        string? message,
        bool isRetriable,
        bool isTransactionAbortableError,
        bool isApplicationRecoverableError,
        bool isInvalidConfigurationError,
        bool isAuthorizationError,
        bool isOutOfOrderSequenceError,
        Exception? innerException = null)
        : base(message, innerException)
    {
        Code = code;
        IsRetriable = isRetriable;
        IsTransactionAbortableError = isTransactionAbortableError;
        IsApplicationRecoverableError = isApplicationRecoverableError;
        IsInvalidConfigurationError = isInvalidConfigurationError;
        IsAuthorizationError = isAuthorizationError;
        IsOutOfOrderSequenceError = isOutOfOrderSequenceError;
    }

    /// <summary>
    /// The numeric error code, mirroring the Kafka protocol error code carried by
    /// the core error (<c>kafka_common_Error_code</c>).
    /// </summary>
    public int Code { get; }

    /// <summary>
    /// Whether the failed operation may succeed if retried
    /// (<c>kafka_common_Error_is_retriable_error</c>).
    /// </summary>
    public bool IsRetriable { get; }

    /// <summary>
    /// Whether the error's Java class is
    /// <c>org.apache.kafka.common.errors.TransactionAbortableException</c>: the
    /// transaction can be aborted and retried
    /// (<c>kafka_common_Error_is_transaction_abortable_error</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// It answers <see langword="true"/> for exactly the class that owns code 120
    /// (<c>TRANSACTION_ABORTABLE</c>).
    /// </para>
    /// <para>
    /// <b>Polarity.</b> <c>TransactionAbortableException</c> is a leaf that extends
    /// <c>ApiException</c> directly, so this nests in none of the other hierarchy
    /// predicates and none nests in it. A leaf needs no predicate; this one is a
    /// property because the ABI's commit and send-offsets operations name it as the
    /// signal that the transaction must be aborted.
    /// </para>
    /// <para>
    /// Handling a transaction error: see the remarks on <see cref="IProducer{TKey, TValue}"/> and
    /// <see cref="IAsyncProducer{TKey, TValue}"/>.
    /// </para>
    /// </remarks>
    public bool IsTransactionAbortableError { get; }

    /// <summary>
    /// Whether the error's Java class extends
    /// <c>org.apache.kafka.common.errors.ApplicationRecoverableException</c>, which, in its
    /// javadoc's words, "indicates that the error is fatal to the producer, and the
    /// application needs to restart the producer after handling the error"
    /// (<c>kafka_common_Error_is_application_recoverable_error</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// It answers <see langword="true"/> for exactly the classes that own these codes: 22
    /// (<c>ILLEGAL_GENERATION</c>), 25 (<c>UNKNOWN_MEMBER_ID</c>), 47
    /// (<c>INVALID_PRODUCER_EPOCH</c>), 49 (<c>INVALID_PRODUCER_ID_MAPPING</c>), 82
    /// (<c>FENCED_INSTANCE_ID</c>) and 90 (<c>PRODUCER_FENCED</c>).
    /// </para>
    /// <para>
    /// <b>Polarity.</b> <c>ApplicationRecoverableException</c> extends
    /// <c>ApiException</c> directly, so this nests in none of the other hierarchy
    /// predicates and none nests in it. It is wider than fencing: 90 is one of its
    /// codes, and the class remarks say how to recognize a fenced producer alone.
    /// </para>
    /// <para>
    /// Handling a transaction error: see the remarks on <see cref="IProducer{TKey, TValue}"/> and
    /// <see cref="IAsyncProducer{TKey, TValue}"/>.
    /// </para>
    /// </remarks>
    public bool IsApplicationRecoverableError { get; }

    /// <summary>
    /// Whether the error's Java class is, or extends,
    /// <c>org.apache.kafka.common.errors.InvalidConfigurationException</c>
    /// (<c>kafka_common_Error_is_invalid_configuration_error</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Among the classes that own a protocol code, it answers <see langword="true"/> for
    /// exactly those that own these: 17 (<c>INVALID_TOPIC_EXCEPTION</c>, which the
    /// header names <c>INVALID_TOPIC_ERROR</c>), 18 (<c>RECORD_LIST_TOO_LARGE</c>), 21
    /// (<c>INVALID_REQUIRED_ACKS</c>), 29
    /// (<c>TOPIC_AUTHORIZATION_FAILED</c>), 30 (<c>GROUP_AUTHORIZATION_FAILED</c>), 31
    /// (<c>CLUSTER_AUTHORIZATION_FAILED</c>), 33 (<c>UNSUPPORTED_SASL_MECHANISM</c>), 34
    /// (<c>ILLEGAL_SASL_STATE</c>), 35 (<c>UNSUPPORTED_VERSION</c>), 38
    /// (<c>INVALID_REPLICATION_FACTOR</c>), 40 (<c>INVALID_CONFIG</c>), 43
    /// (<c>UNSUPPORTED_FOR_MESSAGE_FORMAT</c>), 53
    /// (<c>TRANSACTIONAL_ID_AUTHORIZATION_FAILED</c>), 58
    /// (<c>SASL_AUTHENTICATION_FAILED</c>), 65
    /// (<c>DELEGATION_TOKEN_AUTHORIZATION_FAILED</c>) and 87 (<c>INVALID_RECORD</c>). It
    /// also answers <see langword="true"/> for Java's <c>AuthenticationException</c>,
    /// <c>AuthorizationException</c> and <c>SslAuthenticationException</c> themselves,
    /// whose codes are the local -7 (<c>AUTHENTICATION</c>), -9 (<c>AUTHORIZATION</c>)
    /// and -16 (<c>SSL_AUTHENTICATION</c>). Those classes own no protocol code, so the
    /// core gives each a negative one (the header's <c>kafka_common_ErrorCode_t</c>).
    /// </para>
    /// <para>
    /// <b>Polarity.</b> Wider than its name: Java's <c>AuthenticationException</c> and
    /// <c>AuthorizationException</c> both extend <c>InvalidConfigurationException</c>,
    /// so every error that answers <see langword="true"/> to
    /// <see cref="IsAuthorizationError"/> answers it here too, 29 and 53 among them.
    /// </para>
    /// <para>
    /// Handling a transaction error: see the remarks on <see cref="IProducer{TKey, TValue}"/> and
    /// <see cref="IAsyncProducer{TKey, TValue}"/>.
    /// </para>
    /// </remarks>
    public bool IsInvalidConfigurationError { get; }

    /// <summary>
    /// Whether the error's Java class is, or extends,
    /// <c>org.apache.kafka.common.errors.AuthorizationException</c>: an authorization
    /// failure (<c>kafka_common_Error_is_authorization_error</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Among the classes that own a protocol code, it answers <see langword="true"/> for
    /// exactly those that own these: 29 (<c>TOPIC_AUTHORIZATION_FAILED</c>), 30
    /// (<c>GROUP_AUTHORIZATION_FAILED</c>), 31 (<c>CLUSTER_AUTHORIZATION_FAILED</c>), 53
    /// (<c>TRANSACTIONAL_ID_AUTHORIZATION_FAILED</c>) and 65
    /// (<c>DELEGATION_TOKEN_AUTHORIZATION_FAILED</c>). It also answers
    /// <see langword="true"/> for Java's <c>AuthorizationException</c> itself, whose code
    /// is the local -9 (<c>AUTHORIZATION</c>): it owns no protocol code.
    /// </para>
    /// <para>
    /// <b>Polarity.</b> Nested inside <see cref="IsInvalidConfigurationError"/>:
    /// <c>AuthorizationException</c> extends <c>InvalidConfigurationException</c>, so
    /// every error that answers <see langword="true"/> here answers it there too.
    /// </para>
    /// <para>
    /// Handling a transaction error: see the remarks on <see cref="IProducer{TKey, TValue}"/> and
    /// <see cref="IAsyncProducer{TKey, TValue}"/>.
    /// </para>
    /// </remarks>
    public bool IsAuthorizationError { get; }

    /// <summary>
    /// Whether the error's Java class is, or extends,
    /// <c>org.apache.kafka.common.errors.OutOfOrderSequenceException</c>
    /// (<c>kafka_common_Error_is_out_of_order_sequence_error</c>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// It answers <see langword="true"/> for exactly the classes that own these codes: 45
    /// (<c>OUT_OF_ORDER_SEQUENCE_NUMBER</c>) and 59 (<c>UNKNOWN_PRODUCER_ID</c>).
    /// </para>
    /// <para>
    /// <b>Polarity.</b> Java's <c>UnknownProducerIdException</c> extends
    /// <c>OutOfOrderSequenceException</c>, so 59 answers <see langword="true"/> here as
    /// 45 does. <c>OutOfOrderSequenceException</c> extends <c>ApiException</c>
    /// directly, so this nests in none of the other hierarchy predicates.
    /// </para>
    /// <para>
    /// Handling a transaction error: see the remarks on <see cref="IProducer{TKey, TValue}"/> and
    /// <see cref="IAsyncProducer{TKey, TValue}"/>.
    /// </para>
    /// </remarks>
    public bool IsOutOfOrderSequenceError { get; }

    /// <summary>
    /// Builds a <see cref="KafkaException"/> from a <c>kafka_common_Error_t</c>
    /// handle returned by the ABI, then <b>frees the handle exactly once</b>.
    /// </summary>
    /// <param name="error">
    /// The error handle from an <c>out_error</c> slot or an error-returning
    /// function. <see cref="IntPtr.Zero"/> means success.
    /// </param>
    /// <returns>
    /// <see langword="null"/> when <paramref name="error"/> is
    /// <see cref="IntPtr.Zero"/> (success); otherwise the mapped exception, which
    /// the caller typically throws.
    /// </returns>
    /// <remarks>
    /// The message is read <b>before</b> the handle is freed (the borrowed
    /// <c>const char*</c> dies with the handle), all values are copied out, and the
    /// handle is destroyed in a <c>finally</c> so it is freed exactly once even if
    /// construction throws. The returned exception holds only copied values — never
    /// the handle (ffi §A5/§B5).
    /// </remarks>
    internal static KafkaException? FromHandle(IntPtr error)
    {
        if (error == IntPtr.Zero)
        {
            return null;
        }

        try
        {
            // Read every value out BEFORE the free (the message is a borrowed const
            // char* that dies with the handle). The read itself is shared with
            // FromBorrowedHandle so the two overloads cannot drift; the ONLY
            // difference between them is this method's finally.
            return FromBorrowedHandle(error);
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    /// <summary>
    /// Builds a <see cref="KafkaException"/> from a <b>borrowed</b>
    /// <c>kafka_common_KafkaError_t</c> pointer — the non-destroying sibling of
    /// <see cref="FromHandle(IntPtr)"/>. Reads the accessors and copies every value
    /// out; the handle is <b>never</b> freed, because this side of the ownership line
    /// does not own it.
    /// </summary>
    /// <param name="error">
    /// A borrowed error pointer, valid only until its owning root is destroyed.
    /// <see cref="IntPtr.Zero"/> means "no error" (success).
    /// </param>
    /// <returns>
    /// <see langword="null"/> when <paramref name="error"/> is
    /// <see cref="IntPtr.Zero"/>; otherwise the mapped exception, holding only copied
    /// values.
    /// </returns>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Which overload to use is decided by const-ness in the header, and the same
    /// C type appears on both sides of that line.</b> An admin per-key error
    /// (<c>kafka_admin_*Result_get_error(result, i)</c>) is returned as
    /// <c>const kafka_common_KafkaError_t*</c> and the header says verbatim: "The
    /// pointer is borrowed from the result handle — read it with the
    /// <c>kafka_common_KafkaError_*</c> accessors, but do <b>not</b> destroy it." It
    /// dies with the result root, which the completion trampoline destroys exactly
    /// once. Using <see cref="FromHandle(IntPtr)"/> there frees it a second time — a
    /// double free, i.e. a process abort that <em>no managed assertion can catch</em>.
    /// </para>
    /// <para>
    /// The mirror-image mistake costs a leak: an admin completion callback's
    /// <c>error</c> <b>parameter</b> is a <em>non-const</em>
    /// <c>kafka_common_KafkaError_t*</c> that the callback <b>owns and must free</b>,
    /// so that one takes <see cref="FromHandle(IntPtr)"/>. Read the accessor's
    /// signature, never the type name.
    /// </para>
    /// </remarks>
    internal static KafkaException? FromBorrowedHandle(IntPtr error)
    {
        if (error == IntPtr.Zero)
        {
            return null;
        }

        int code = NativeMethods.Code(error);
        string? message = Utf8Marshal.PtrToString(NativeMethods.Message(error));
        bool isRetriable = NativeMethods.IsRetriable(error);

        // The Java-hierarchy predicates M17/P1 D6 added, read eagerly here with the
        // others: the exception keeps copied values only, never the handle (ffi §A5).
        bool isTransactionAbortableError = NativeMethods.IsTransactionAbortableError(error);
        bool isApplicationRecoverableError = NativeMethods.IsApplicationRecoverableError(error);
        bool isInvalidConfigurationError = NativeMethods.IsInvalidConfigurationError(error);
        bool isAuthorizationError = NativeMethods.IsAuthorizationError(error);
        bool isOutOfOrderSequenceError = NativeMethods.IsOutOfOrderSequenceError(error);

        return new KafkaException(
            code,
            message,
            isRetriable,
            isTransactionAbortableError,
            isApplicationRecoverableError,
            isInvalidConfigurationError,
            isAuthorizationError,
            isOutOfOrderSequenceError);
    }
}
