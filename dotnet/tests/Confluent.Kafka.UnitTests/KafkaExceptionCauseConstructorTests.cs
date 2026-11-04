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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the cause-only constructors of <see cref="KafkaException"/> and <see cref="SerializationException"/>, Java's
/// <c>KafkaException(Throwable)</c>: the message is the cause's <c>toString()</c>, never .NET's
/// <see cref="Exception.ToString"/>.
/// </summary>
public sealed class KafkaExceptionCauseConstructorTests
{
    private static KafkaException Create(string type, Exception? cause) =>
        type == nameof(SerializationException) ? new SerializationException(cause) : new KafkaException(cause);

    private static Exception Thrown(Exception exception)
    {
        try
        {
            throw exception;
        }
        catch (Exception caught)
        {
            return caught;
        }
    }

    [Theory]
    [InlineData(nameof(KafkaException))]
    [InlineData(nameof(SerializationException))]
    public void CauseWithAMessage_MessageIsTheCauseTypeFullNameAndMessage(string type)
    {
        Exception cause = Thrown(new InvalidOperationException("the cause: café"));

        KafkaException exception = Create(type, cause);

        Assert.Equal(type, exception.GetType().Name);
        Assert.Equal("System.InvalidOperationException: the cause: café", exception.Message);
        Assert.Same(cause, exception.InnerException);
        Assert.Equal(-1, exception.Code);
        Assert.False(exception.IsRetriable);
    }

    [Theory]
    [InlineData(nameof(KafkaException))]
    [InlineData(nameof(SerializationException))]
    public void CauseWithAnEmptyMessage_MessageIsTheCauseTypeFullNameAndASeparator(string type)
    {
        Exception cause = Thrown(new Exception(string.Empty));

        KafkaException exception = Create(type, cause);

        Assert.Equal(type, exception.GetType().Name);
        Assert.Equal("System.Exception: ", exception.Message);
        Assert.Same(cause, exception.InnerException);
        Assert.Equal(-1, exception.Code);
        Assert.False(exception.IsRetriable);
    }

    [Theory]
    [InlineData(nameof(KafkaException))]
    [InlineData(nameof(SerializationException))]
    public void CauseWithANullJavaMessage_MessageIsTheCauseTypeFullName(string type)
    {
        Exception cause = Thrown(new KafkaException((string?)null));

        KafkaException exception = Create(type, cause);

        Assert.Equal(type, exception.GetType().Name);
        Assert.Equal("Confluent.Kafka.Common.KafkaException", exception.Message);
        Assert.Same(cause, exception.InnerException);
        Assert.Equal(-1, exception.Code);
        Assert.False(exception.IsRetriable);
    }

    [Theory]
    [InlineData(nameof(KafkaException))]
    [InlineData(nameof(SerializationException))]
    public void CauseWithAJavaToStringOverride_MessageIsTheOverride(string type)
    {
        Exception cause = Thrown(new OverridingException("its own text"));

        KafkaException exception = Create(type, cause);

        Assert.Equal("OverridingException{its own text}", exception.Message);
        Assert.Same(cause, exception.InnerException);
    }

    [Theory]
    [InlineData(nameof(KafkaException))]
    [InlineData(nameof(SerializationException))]
    public void NullCause_MessageIsEmpty(string type)
    {
        KafkaException exception = Create(type, null);
        KafkaException parameterless = type == nameof(SerializationException)
            ? new SerializationException()
            : new KafkaException();

        Assert.Equal(type, exception.GetType().Name);
        Assert.Equal(string.Empty, exception.Message);
        Assert.Equal(string.Empty, parameterless.Message);
        Assert.Null(exception.InnerException);
        Assert.Equal(-1, exception.Code);
        Assert.Equal(-1, parameterless.Code);
        Assert.False(exception.IsRetriable);
    }

    /// <summary>
    /// A <see cref="KafkaException"/> that overrides Java's <c>toString()</c>, as Java's
    /// <c>QuotaViolationException</c> does.
    /// </summary>
    private sealed class OverridingException : KafkaException
    {
        private readonly string _text;

        internal OverridingException(string text)
        {
            _text = text;
        }

        internal override string JavaToString() => "OverridingException{" + _text + "}";
    }
}
