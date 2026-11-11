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

using System.Threading.Tasks;

namespace Confluent.Kafka.Performance.V3;

/// <summary>The <c>MODE=transactional</c> entry: the transactional engine over this exe's client.</summary>
internal static class TransactionalMain
{
    internal static Task<int> Run()
    {
        TransactionalBenchmarkConfig config = TransactionalBenchmarkConfig.FromEnv();
        return TransactionalBenchmark.Run(new V3TransactionalFactory(config), config);
    }
}
