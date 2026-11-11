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

#if !NET5_0_OR_GREATER
#pragma warning disable IDE0130 // the runtime's namespace, which the trimmer looks the attribute up in
namespace System.Diagnostics.CodeAnalysis;
#pragma warning restore IDE0130

/// <summary>
/// .NET 5's attribute, for the netstandard2.0 build, which lacks it: the trimmer recognizes it by its full name, so
/// this internal copy suppresses a trim warning as the runtime's does.
/// </summary>
[AttributeUsage(AttributeTargets.All, Inherited = false, AllowMultiple = true)]
internal sealed class UnconditionalSuppressMessageAttribute : Attribute
{
    public UnconditionalSuppressMessageAttribute(string category, string checkId)
    {
        Category = category;
        CheckId = checkId;
    }

    public string Category { get; }

    public string CheckId { get; }

    public string? Justification { get; set; }
}
#endif
