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
using System.Linq;
using System.Reflection;
using System.Threading;

using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// No public member takes an optional parameter other than a trailing <see cref="CancellationToken"/>: a default is
/// compiled into each caller, so adding a parameter to a method breaks every caller compiled before it with
/// <see cref="MissingMethodException"/>, where a new overload breaks none. Java's overloads stay overloads.
/// </summary>
public sealed class PublicOptionalParameterTests
{
    // The admin client's optional options are replaced by overloads in a change of their own; drop this with it.
    private const string AdminNamespace = "Confluent.Kafka.Admin";

    [Fact]
    public void PublicMembers_TakeNoOptionalParameterButATrailingCancellationToken()
    {
        Assembly library = typeof(KafkaProducer<,>).Assembly;
        List<string> optional = new List<string>();
        foreach (Type type in library.GetExportedTypes().Where(t => t.Namespace != AdminNamespace))
        {
            const BindingFlags Declared =
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static |
                BindingFlags.DeclaredOnly;
            IEnumerable<MethodBase> members = type.GetMethods(Declared).Cast<MethodBase>()
                .Concat(type.GetConstructors(Declared))
                .Where(m => m.IsPublic || m.IsFamily || m.IsFamilyOrAssembly);
            foreach (MethodBase member in members)
            {
                ParameterInfo[] parameters = member.GetParameters();
                for (int i = 0; i < parameters.Length; i++)
                {
                    bool trailingToken =
                        parameters[i].ParameterType == typeof(CancellationToken) && i == parameters.Length - 1;
                    if (parameters[i].IsOptional && !trailingToken)
                    {
                        optional.Add(type.FullName + "." + member.Name + "(" + parameters[i].Name + ")");
                    }
                }
            }
        }

        Assert.Empty(optional);
    }
}
