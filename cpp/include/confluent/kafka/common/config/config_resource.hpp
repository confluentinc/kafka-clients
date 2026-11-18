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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>

namespace confluent::kafka::common::config {

/// A class representing resources that have configs.
class CONFLUENT_KAFKA_CPP_API ConfigResource {
public:
    /// Type of resource.
    enum class Type : int8_t {
        /// A consumer group's configs.
        GROUP = 32,
        /// A client metrics subscription's configs.
        CLIENT_METRICS = 16,
        /// A broker's logger levels.
        BROKER_LOGGER = 8,
        /// A broker's configs.
        BROKER = 4,
        /// A topic's configs.
        TOPIC = 2,
        /// A resource type this client does not know.
        UNKNOWN = 0,
    };

    /// Create an instance of this class with the provided parameters.
    /// @param type a non-null resource type
    /// @param name a non-null resource name
    ConfigResource(Type type, std::string_view name);

    /// Copies `other`, sharing its immutable value.
    /// @param other the config resource to copy
    ConfigResource(const ConfigResource& other);

    /// Moves `other`, which is left empty and may only be assigned to or destroyed.
    /// @param other the config resource to move
    ConfigResource(ConfigResource&& other) noexcept;

    /// Copies `other` into this config resource.
    /// @param other the config resource to copy
    /// @return this config resource
    ConfigResource& operator=(const ConfigResource& other);

    /// Moves `other` into this config resource; `other` is left empty.
    /// @param other the config resource to move
    /// @return this config resource
    ConfigResource& operator=(ConfigResource&& other) noexcept;

    ~ConfigResource();

    /// Return the resource type.
    /// @return the resource type
    [[nodiscard]] Type type() const;

    /// Return the resource name.
    /// @return the resource name
    [[nodiscard]] const std::string& name() const;

    /// Returns true if this is the default resource of a resource type.
    /// Resource name is empty for the default resource.
    /// @return whether the name is empty
    [[nodiscard]] bool is_default() const;

    /// Whether `a` and `b` name the same resource.
    /// @param a a config resource
    /// @param b another config resource
    /// @return true when the type and the name are equal
    friend CONFLUENT_KAFKA_CPP_API bool operator==(const ConfigResource& a, const ConfigResource& b);

    /// Returns `ConfigResource(type=<type>, name='<name>')`, as Java's toString.
    /// @param resource a config resource
    /// @return its description
    friend CONFLUENT_KAFKA_CPP_API std::string to_string(const ConfigResource& resource);

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout.
    struct Impl;
    std::shared_ptr<const Impl> impl_;
};

/// Returns the id of a resource type.
/// @param type the resource type
/// @return its id
CONFLUENT_KAFKA_CPP_API int8_t id(ConfigResource::Type type);

/// Writes `to_string(resource)` to `out`.
/// @param out the stream
/// @param resource a config resource
/// @return `out`
CONFLUENT_KAFKA_CPP_API std::ostream& operator<<(std::ostream& out, const ConfigResource& resource);

namespace config_resource::type {

/// Returns the resource type of `id`, or UNKNOWN.
/// @param id the id
/// @return the resource type
CONFLUENT_KAFKA_CPP_API ConfigResource::Type for_id(int8_t id);

}  // namespace config_resource::type
}  // namespace confluent::kafka::common::config

/// Hashes a config resource from its type and name, as Java's hashCode reads them.
template <>
struct CONFLUENT_KAFKA_CPP_API std::hash<confluent::kafka::common::config::ConfigResource> {
    /// Returns the hash of `r`.
    /// @param r a config resource
    /// @return its hash
    std::size_t operator()(const confluent::kafka::common::config::ConfigResource& r) const noexcept;
};
