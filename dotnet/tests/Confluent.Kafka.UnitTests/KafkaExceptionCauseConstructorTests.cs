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
        Assert.Equal(0, exception.Code);
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
        Assert.Equal(0, exception.Code);
        Assert.False(exception.IsRetriable);
    }

    [Theory]
    [InlineData(nameof(KafkaException))]
    [InlineData(nameof(SerializationException))]
    public void NullCause_MessageIsTheDefaultMessage(string type)
    {
        KafkaException exception = Create(type, null);
        KafkaException parameterless = type == nameof(SerializationException)
            ? new SerializationException()
            : new KafkaException();
        string expected = type == nameof(SerializationException)
            ? "Exception of type 'Confluent.Kafka.Common.Errors.SerializationException' was thrown."
            : "Exception of type 'Confluent.Kafka.Common.KafkaException' was thrown.";

        Assert.Equal(type, exception.GetType().Name);
        Assert.Equal(expected, exception.Message);
        Assert.Equal(parameterless.Message, exception.Message);
        Assert.Null(exception.InnerException);
        Assert.Equal(0, exception.Code);
        Assert.False(exception.IsRetriable);
    }
}
