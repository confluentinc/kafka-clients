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
CONFLUENT_KAFKA_CPP_API int8_t code(AclPermissionType acl_permission_type);

/// Return true if this permission type is UNKNOWN.
/// @param acl_permission_type the permission type
/// @return whether it is UNKNOWN
CONFLUENT_KAFKA_CPP_API bool is_unknown(AclPermissionType acl_permission_type);

namespace acl_permission_type {

/// Return the AclPermissionType with the provided code or `AclPermissionType::UNKNOWN` if one cannot be found.
/// @param code the code
/// @return the permission type
CONFLUENT_KAFKA_CPP_API AclPermissionType from_code(int8_t code);

/// Parse the given string as an ACL permission.
/// @param str The string to parse.
/// @return The AclPermissionType, or UNKNOWN if the string could not be matched.
CONFLUENT_KAFKA_CPP_API AclPermissionType from_string(std::string_view str);

}  // namespace acl_permission_type
}  // namespace confluent::kafka::common::acl
