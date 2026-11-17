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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

namespace confluent::kafka::common::config {

/// A class representing resources that have configs.
class ConfigResource {
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
    ConfigResource(Type type, std::string_view name)
        : impl_(std::make_shared<const Impl>(Impl{.type = type, .name = std::string(name)})) {}

    /// Return the resource type.
    /// @return the resource type
    [[nodiscard]] Type type() const { return impl_->type; }

    /// Return the resource name.
    /// @return the resource name
    [[nodiscard]] const std::string& name() const { return impl_->name; }

    /// Returns true if this is the default resource of a resource type.
    /// Resource name is empty for the default resource.
    /// @return whether the name is empty
    [[nodiscard]] bool is_default() const { return impl_->name.empty(); }

    /// Whether `a` and `b` name the same resource.
    /// @param a a config resource
    /// @param b another config resource
    /// @return true when the type and the name are equal
    friend bool operator==(const ConfigResource& a, const ConfigResource& b) {
        return a.impl_ == b.impl_ || (a.type() == b.type() && a.name() == b.name());
    }

    /// Returns `ConfigResource(type=<type>, name='<name>')`, as Java's toString.
    /// @param resource a config resource
    /// @return its description
    friend std::string to_string(const ConfigResource& resource) {
        return "ConfigResource(type=" + std::string(type_name(resource.type())) + ", name='" + resource.name() + "')";
    }

private:
    // Java's Enum.name() of a Type, for toString; Type does not override toString, so cpp/CLAUDE.md §3.3
    // gives it no public to_string.
    static std::string_view type_name(Type type) {
        switch (type) {
            case Type::GROUP:
                return "GROUP";
            case Type::CLIENT_METRICS:
                return "CLIENT_METRICS";
            case Type::BROKER_LOGGER:
                return "BROKER_LOGGER";
            case Type::BROKER:
                return "BROKER";
            case Type::TOPIC:
                return "TOPIC";
            case Type::UNKNOWN:
                return "UNKNOWN";
        }
        return "UNKNOWN";
    }

    // cpp/CLAUDE.md §12.3: one pointer; the value is immutable, so copies share it.
    struct Impl {
        Type type;
        std::string name;
    };
    std::shared_ptr<const Impl> impl_;
};

/// Returns the id of a resource type.
/// @param type the resource type
/// @return its id
inline int8_t id(ConfigResource::Type type) {
    return static_cast<int8_t>(type);
}

/// Writes `to_string(resource)` to `out`.
/// @param out the stream
/// @param resource a config resource
/// @return `out`
inline std::ostream& operator<<(std::ostream& out, const ConfigResource& resource) {
    return out << to_string(resource);
}

namespace config_resource::type {

/// Returns the resource type of `id`, or UNKNOWN.
/// @param id the id
/// @return the resource type
inline ConfigResource::Type for_id(int8_t id) {
    switch (id) {
        case 32:
            return ConfigResource::Type::GROUP;
        case 16:
            return ConfigResource::Type::CLIENT_METRICS;
        case 8:
            return ConfigResource::Type::BROKER_LOGGER;
        case 4:
            return ConfigResource::Type::BROKER;
        case 2:
            return ConfigResource::Type::TOPIC;
        default:
            return ConfigResource::Type::UNKNOWN;
    }
}

}  // namespace config_resource::type
}  // namespace confluent::kafka::common::config

/// Hashes a config resource from its type and name, as Java's hashCode reads them.
template <>
struct std::hash<confluent::kafka::common::config::ConfigResource> {
    std::size_t operator()(const confluent::kafka::common::config::ConfigResource& r) const noexcept {
        return (31U * std::hash<confluent::kafka::common::config::ConfigResource::Type>{}(r.type())) +
               std::hash<std::string>{}(r.name());
    }
};
