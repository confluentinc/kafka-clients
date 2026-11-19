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

#include <confluent/kafka/common/acl/acl_permission_type.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <string>
#include <utility>

namespace confluent::kafka::common::acl {

int8_t code(AclPermissionType acl_permission_type) {
    return static_cast<int8_t>(acl_permission_type);
}

bool is_unknown(AclPermissionType acl_permission_type) {
    return acl_permission_type == AclPermissionType::UNKNOWN;
}

namespace acl_permission_type {

AclPermissionType from_code(int8_t code) {
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

AclPermissionType from_string(std::string_view str) {
    std::string upper(str);
    std::ranges::transform(upper, upper.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    static constexpr std::array<std::pair<std::string_view, AclPermissionType>, 4> NAMES{{
        {"UNKNOWN", AclPermissionType::UNKNOWN},
        {"ANY", AclPermissionType::ANY},
        {"DENY", AclPermissionType::DENY},
        {"ALLOW", AclPermissionType::ALLOW},
    }};
    for (const auto& [name, type] : NAMES) {
        if (upper == name) {
            return type;
        }
    }
    return AclPermissionType::UNKNOWN;
}

}  // namespace acl_permission_type

}  // namespace confluent::kafka::common::acl
