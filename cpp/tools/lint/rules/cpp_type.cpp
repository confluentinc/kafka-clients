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

#include "cpp_type.hpp"

#include <utility>

namespace cklint::rules {

std::string CppType::to_string() const {
    std::string out = is_const ? "const " + name : name;
    if (!arguments.empty()) {
        out += '<';
        for (std::size_t i = 0; i < arguments.size(); ++i) {
            if (i > 0) {
                out += ", ";
            }
            out += arguments[i].to_string();
        }
        out += '>';
    }
    if (ref == Ref::LVALUE) {
        out += '&';
    } else if (ref == Ref::RVALUE) {
        out += "&&";
    }
    return out;
}

CppType value(std::string name, std::vector<CppType> arguments) {
    CppType t;
    t.name = std::move(name);
    t.arguments = std::move(arguments);
    return t;
}

CppType const_ref(CppType t) {
    t.is_const = true;
    t.ref = CppType::Ref::LVALUE;
    return t;
}

CppType mutable_ref(CppType t) {
    t.ref = CppType::Ref::LVALUE;
    return t;
}

CppType const_of(CppType t) {
    t.is_const = true;
    return t;
}

}  // namespace cklint::rules
