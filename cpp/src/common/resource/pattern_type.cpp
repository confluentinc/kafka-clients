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

#include <confluent/kafka/common/resource/pattern_type.hpp>

#include <array>
#include <string_view>
#include <utility>

namespace confluent::kafka::common::resource {

int8_t code(PatternType pattern_type) {
    return static_cast<int8_t>(pattern_type);
}

bool is_unknown(PatternType pattern_type) {
    return pattern_type == PatternType::UNKNOWN;
}

bool is_specific(PatternType pattern_type) {
    return pattern_type != PatternType::UNKNOWN && pattern_type != PatternType::ANY &&
           pattern_type != PatternType::MATCH;
}

namespace pattern_type {

PatternType from_code(int8_t code) {
    switch (code) {
        case 1:
            return PatternType::ANY;
        case 2:
            return PatternType::MATCH;
        case 3:
            return PatternType::LITERAL;
        case 4:
            return PatternType::PREFIXED;
        default:
            return PatternType::UNKNOWN;
    }
}

PatternType from_string(std::string_view name) {
    static constexpr std::array<std::pair<std::string_view, PatternType>, 5> NAMES{{
        {"UNKNOWN", PatternType::UNKNOWN},
        {"ANY", PatternType::ANY},
        {"MATCH", PatternType::MATCH},
        {"LITERAL", PatternType::LITERAL},
        {"PREFIXED", PatternType::PREFIXED},
    }};
    for (const auto& [constant, type] : NAMES) {
        if (name == constant) {
            return type;
        }
    }
    return PatternType::UNKNOWN;
}

}  // namespace pattern_type

}  // namespace confluent::kafka::common::resource
