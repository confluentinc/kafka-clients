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

#include <confluent/kafka/detail/ffi.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>

namespace confluent::kafka::common::config {

/// Describes whether the server should require or request client authentication.
///
/// Java's constructor sets no integer field, so the values are the C header's (cpp/CLAUDE.md §3.3).
enum class SslClientAuth : int32_t {
    /// Java's `REQUIRED`.
    REQUIRED = kafka_common_config_SslClientAuth_REQUIRED,
    /// Java's `REQUESTED`.
    REQUESTED = kafka_common_config_SslClientAuth_REQUESTED,
    /// Java's `NONE`.
    NONE = kafka_common_config_SslClientAuth_NONE,
};

/// Returns the constant's name in lower case, as Java's toString: `required`, `requested`, `none`.
/// @param ssl_client_auth the client authentication setting
/// @return its name in lower case
inline std::string to_string(SslClientAuth ssl_client_auth) {
    switch (ssl_client_auth) {
        case SslClientAuth::REQUIRED:
            return "required";
        case SslClientAuth::REQUESTED:
            return "requested";
        case SslClientAuth::NONE:
            return "none";
    }
    return {};
}

/// Writes `to_string(ssl_client_auth)` to `out`.
/// @param out the stream
/// @param ssl_client_auth the client authentication setting
/// @return `out`
inline std::ostream& operator<<(std::ostream& out, SslClientAuth ssl_client_auth) {
    return out << to_string(ssl_client_auth);
}

namespace ssl_client_auth {

/// Every constant, in Java's declaration order.
inline constexpr std::array<SslClientAuth, 3> VALUES{SslClientAuth::REQUIRED, SslClientAuth::REQUESTED,
                                                     SslClientAuth::NONE};

/// Returns the setting named `key`, ignoring case: `NONE` for `std::nullopt`, and `std::nullopt` for a
/// name that is none of the constants.
/// @param key the configured value, or `std::nullopt`
/// @return the setting, or `std::nullopt` when `key` names none
inline std::optional<SslClientAuth> for_config(std::optional<std::string_view> key) {
    if (!key) {
        return SslClientAuth::NONE;
    }
    std::string upper_case_key(*key);
    std::transform(upper_case_key.begin(), upper_case_key.end(), upper_case_key.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    static constexpr std::array<std::string_view, 3> names{"REQUIRED", "REQUESTED", "NONE"};
    for (std::size_t i = 0; i < VALUES.size(); ++i) {
        if (upper_case_key == names[i]) {
            return VALUES[i];
        }
    }
    return std::nullopt;
}

}  // namespace ssl_client_auth
}  // namespace confluent::kafka::common::config
