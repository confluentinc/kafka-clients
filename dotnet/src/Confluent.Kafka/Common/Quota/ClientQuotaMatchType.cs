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

namespace Confluent.Kafka.Common.Quota;

/// <summary>
/// How a <see cref="ClientQuotaFilterComponent"/> matches entity names. The values are Java's
/// <c>DescribeClientQuotasRequest</c> match-type constants; the third is the state Java encodes as a
/// null <c>Optional</c> reference.
/// </summary>
public enum ClientQuotaMatchType
{
    /// <summary>Match the component's name exactly — Java's <c>ofEntity</c>.</summary>
    Exact = 0,

    /// <summary>Match the built-in default entity for the type — Java's <c>ofDefaultEntity</c>.</summary>
    Default = 1,

    /// <summary>Match any <em>named</em> entity of the type — Java's <c>ofEntityType</c>.</summary>
    Specified = 2,
}
