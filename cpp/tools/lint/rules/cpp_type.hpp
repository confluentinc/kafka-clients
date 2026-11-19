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

// A C++ type as a header spells it, structured so that the rules (which
// produce it from Java) and the clang-tidy plugin (which reads it from
// Clang's sugared QualType) compare structure, not text.

#include <cstdint>
#include <string>
#include <vector>

namespace cklint::rules {

struct CppType {
    enum class Ref : std::uint8_t { NONE, LVALUE, RVALUE };

    /// Fully qualified, as written through typedefs: `int32_t`, `std::string_view`,
    /// `confluent::kafka::common::TopicPartition`, a template parameter `K`, `void`.
    std::string name;
    std::vector<CppType> arguments;
    bool is_const = false;
    Ref ref = Ref::NONE;

    /// `const std::unordered_map<std::string, int32_t>&`.
    [[nodiscard]] std::string to_string() const;
    bool operator==(const CppType&) const = default;
};

/// A type by value: `CppType{name, arguments}`.
CppType value(std::string name, std::vector<CppType> arguments = {});
/// `const T&`.
CppType const_ref(CppType t);
/// `T&`.
CppType mutable_ref(CppType t);
/// `const T` (a span's element).
CppType const_of(CppType t);

}  // namespace cklint::rules
