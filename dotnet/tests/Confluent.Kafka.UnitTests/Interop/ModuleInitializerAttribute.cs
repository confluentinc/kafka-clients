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

#if NETFRAMEWORK
#pragma warning disable IDE0130 // the namespace the compiler looks the attribute up in
namespace System.Runtime.CompilerServices;
#pragma warning restore IDE0130

/// <summary>
/// The compiler's module-initializer attribute, for the net462 build, which lacks it (see
/// <c>NativeLibraryInitializer</c>).
/// </summary>
[AttributeUsage(AttributeTargets.Method, Inherited = false)]
internal sealed class ModuleInitializerAttribute : Attribute
{
}
#endif
