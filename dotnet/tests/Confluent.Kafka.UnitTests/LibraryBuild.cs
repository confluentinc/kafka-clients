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

using System.Diagnostics;
using System.Reflection;

using Confluent.Kafka.Producer;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// How the library under test was compiled: an unoptimized (Debug) build makes every async method's state machine a
/// heap object, which an allocation budget measured on the optimized build does not include.
/// </summary>
internal static class LibraryBuild
{
    internal static readonly bool IsOptimized =
        typeof(KafkaProducer<,>).Assembly.GetCustomAttribute<DebuggableAttribute>()?.IsJITOptimizerDisabled != true;
}
