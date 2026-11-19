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

#include <confluent/kafka/export.hpp>

#include <cstdint>
#include <string_view>

namespace confluent::kafka::common::resource {

/// Resource pattern type.
enum class PatternType : int8_t {
    /// Represents any PatternType which this client cannot understand, perhaps because this client is too old.
    UNKNOWN = 0,
    /// In a filter, matches any resource pattern type.
    ANY = 1,
    /// In a filter, will perform pattern matching.
    ///
    /// e.g. Given a filter of `ResourcePatternFilter(TOPIC, "payments.received", MATCH)`, the filter match
    /// any ResourcePattern that matches topic 'payments.received'. This might include:
    /// - A Literal pattern with the same type and name, e.g. `ResourcePattern(TOPIC, "payments.received", LITERAL)`
    /// - A Wildcard pattern with the same type, e.g. `ResourcePattern(TOPIC, "*", LITERAL)`
    /// - A Prefixed pattern with the same type and where the name is a matching prefix, e.g.
    ///   `ResourcePattern(TOPIC, "payments.", PREFIXED)`
    MATCH = 2,
    /// A literal resource name.
    ///
    /// A literal name defines the full name of a resource, e.g. topic with name 'foo', or group with name 'bob'.
    ///
    /// The special wildcard character `*` can be used to represent a resource with any name.
    LITERAL = 3,
    /// A prefixed resource name.
    ///
    /// A prefixed name defines a prefix for a resource, e.g. topics with names that start with 'foo'.
    PREFIXED = 4,
};

/// @param pattern_type the pattern type
/// @return the code of this resource.
CONFLUENT_KAFKA_CPP_API int8_t code(PatternType pattern_type);

/// @param pattern_type the pattern type
/// @return whether this resource pattern type is UNKNOWN.
CONFLUENT_KAFKA_CPP_API bool is_unknown(PatternType pattern_type);

/// @param pattern_type the pattern type
/// @return whether this resource pattern type is a concrete type, rather than UNKNOWN or one of the filter types.
CONFLUENT_KAFKA_CPP_API bool is_specific(PatternType pattern_type);

namespace pattern_type {

/// Return the PatternType with the provided code or `PatternType::UNKNOWN` if one cannot be found.
/// @param code the code
/// @return the pattern type
CONFLUENT_KAFKA_CPP_API PatternType from_code(int8_t code);

/// Return the PatternType with the provided name or `PatternType::UNKNOWN` if one cannot be found.
/// @param name the constant's name, matched exactly
/// @return the pattern type
CONFLUENT_KAFKA_CPP_API PatternType from_string(std::string_view name);

}  // namespace pattern_type
}  // namespace confluent::kafka::common::resource
