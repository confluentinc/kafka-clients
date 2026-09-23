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

namespace Confluent.Kafka;

/// <summary>
/// How a <see cref="ClientQuotaFilterComponent"/> matches entity names. The values are the C ABI's
/// <c>match_types</c> wire constants, which make explicit the third state Java encodes as a null
/// <c>Optional</c> reference (PLAN D37).
/// </summary>
public enum ClientQuotaMatchType
{
    /// <summary>Match the component's name exactly — Java's <c>ofEntity</c> (<c>:51</c>).</summary>
    Exact = 0,

    /// <summary>
    /// Match the built-in default entity for the type — Java's <c>ofDefaultEntity</c> (<c>:61</c>).
    /// </summary>
    Default = 1,

    /// <summary>
    /// Match any <em>named</em> entity of the type — Java's <c>ofEntityType</c> (<c>:71</c>).
    /// </summary>
    Specified = 2,
}
