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

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>

namespace confluent::kafka::common::acl {

/// Represents whether an ACL grants or denies permissions.
enum class AclPermissionType : int8_t {
    /// Represents any AclPermissionType which this client cannot understand, perhaps because this client is too old.
    UNKNOWN = 0,
    /// In a filter, matches any AclPermissionType.
    ANY = 1,
    /// Disallows access.
    DENY = 2,
    /// Grants access.
    ALLOW = 3,
};

/// Return the code of this permission type.
/// @param acl_permission_type the permission type
/// @return its code
inline int8_t code(AclPermissionType acl_permission_type) {
    return static_cast<int8_t>(acl_permission_type);
}

/// Return true if this permission type is UNKNOWN.
/// @param acl_permission_type the permission type
/// @return whether it is UNKNOWN
inline bool is_unknown(AclPermissionType acl_permission_type) {
    return acl_permission_type == AclPermissionType::UNKNOWN;
}

namespace acl_permission_type {

/// Return the AclPermissionType with the provided code or `AclPermissionType::UNKNOWN` if one cannot be found.
/// @param code the code
/// @return the permission type
inline AclPermissionType from_code(int8_t code) {
    switch (code) {
        case 1:
            return AclPermissionType::ANY;
        case 2:
            return AclPermissionType::DENY;
        case 3:
            return AclPermissionType::ALLOW;
        default:
            return AclPermissionType::UNKNOWN;
    }
}

/// Parse the given string as an ACL permission.
/// @param str The string to parse.
/// @return The AclPermissionType, or UNKNOWN if the string could not be matched.
inline AclPermissionType from_string(std::string_view str) {
    std::string upper(str);
    std::transform(upper.begin(), upper.end(), upper.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    static constexpr std::array<std::pair<std::string_view, AclPermissionType>, 4> names{{
        {"UNKNOWN", AclPermissionType::UNKNOWN},
        {"ANY", AclPermissionType::ANY},
        {"DENY", AclPermissionType::DENY},
        {"ALLOW", AclPermissionType::ALLOW},
    }};
    for (const auto& [name, type] : names) {
        if (upper == name) {
            return type;
        }
    }
    return AclPermissionType::UNKNOWN;
}

}  // namespace acl_permission_type
}  // namespace confluent::kafka::common::acl
