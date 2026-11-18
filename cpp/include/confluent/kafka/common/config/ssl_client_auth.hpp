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

#include <array>
#include <cstdint>
#include <iosfwd>
#include <optional>
#include <string>
#include <string_view>

namespace confluent::kafka::common::config {

/// Describes whether the server should require or request client authentication.
enum class SslClientAuth : int32_t {
    /// Java's `REQUIRED`.
    REQUIRED = 0,
    /// Java's `REQUESTED`.
    REQUESTED = 1,
    /// Java's `NONE`.
    NONE = 2,
};

/// Returns the constant's name in lower case, as Java's toString: `required`, `requested`, `none`.
/// @param ssl_client_auth the client authentication setting
/// @return its name in lower case
CONFLUENT_KAFKA_CPP_API std::string to_string(SslClientAuth ssl_client_auth);

/// Writes `to_string(ssl_client_auth)` to `out`.
/// @param out the stream
/// @param ssl_client_auth the client authentication setting
/// @return `out`
CONFLUENT_KAFKA_CPP_API std::ostream& operator<<(std::ostream& out, SslClientAuth ssl_client_auth);

namespace ssl_client_auth {

/// Every constant, in Java's declaration order.
inline constexpr std::array<SslClientAuth, 3> VALUES{SslClientAuth::REQUIRED, SslClientAuth::REQUESTED,
                                                     SslClientAuth::NONE};

/// Returns the setting named `key`, ignoring case: `NONE` for `std::nullopt`, and `std::nullopt` for a
/// name that is none of the constants.
/// @param key the configured value, or `std::nullopt`
/// @return the setting, or `std::nullopt` when `key` names none
CONFLUENT_KAFKA_CPP_API std::optional<SslClientAuth> for_config(std::optional<std::string_view> key);

}  // namespace ssl_client_auth
}  // namespace confluent::kafka::common::config
