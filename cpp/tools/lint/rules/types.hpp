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

#pragma once

// cpp/CLAUDE.md §5 (and §4.6-§4.8) as shape sets: for a Java type in a given
// position, the C++ types a header may write there. The first shape is the
// default; a shape with a `decision` needs that line in decisions.txt (§4.6).

#include <optional>
#include <string>
#include <vector>

#include "cpp_type.hpp"
#include "java_model.hpp"

namespace cklint::rules {

/// Where a Java type appears.
enum class Position : std::uint8_t {
    PARAMETER,      ///< taken by a method or constructor
    RETURN,         ///< returned by a method
    TYPE_ARGUMENT,  ///< inside another type: `List<String>`, a template argument
    CONSTANT,       ///< a `public static final` field
};

/// What a non-default shape needs in decisions.txt (§4.6).
enum class Decision : std::uint8_t {
    NONE,
    NULLABLE,  ///< Java gives null a meaning: `std::optional<T>`
    ORDERED,   ///< Java builds a TreeSet / TreeMap: `std::set`, `std::map`
    SET,       ///< a `Collection` Java builds as a `Set`: `std::unordered_set`
};

const char* decision_name(Decision d);

struct Shape {
    CppType type;
    Decision decision = Decision::NONE;
};

/// The shapes a Java type may take, or why it has none.
struct Mapping {
    std::vector<Shape> shapes;
    /// Non-empty when §5 has no row: generation stops until one is decided.
    std::string unmapped;

    [[nodiscard]] bool ok() const { return unmapped.empty(); }
};

/// The Java types §5 maps to placeholders while they are not translated.
bool is_untranslated_placeholder(std::string_view binary_name);

/// Whether `binary_name` is a Throwable in the model (its extends chain
/// reaches java.lang.Throwable).
bool is_throwable(const java::Model& model, std::string_view binary_name);

/// Whether a type belongs to the binding's public surface: Public audience,
/// not in an `internals` package or an unsupported package (§1.4).
bool is_public_api(const java::Type& type);

/// The shapes of `ref` at `position`. `parameter_name` is the Java name of a
/// parameter (§4.8's `Integer timeoutMs`), empty elsewhere.
Mapping map_type(const java::Model& model, const java::TypeRef& ref, Position position,
                 std::string_view parameter_name = {});

}  // namespace cklint::rules
