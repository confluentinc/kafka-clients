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
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>

namespace confluent::kafka::common {

/// The `ClusterResource` class encapsulates metadata for a Kafka cluster.
class ClusterResource {
public:
    /// Create `ClusterResource` with a cluster id. Note that cluster id may be `std::nullopt` if the
    /// metadata request was sent to a broker without support for cluster ids.
    /// @param cluster_id The cluster id
    explicit ClusterResource(std::optional<std::string_view> cluster_id)
        : impl_(std::make_shared<const Impl>(
              Impl{cluster_id ? std::optional<std::string>(std::string(*cluster_id)) : std::nullopt})) {}

    /// Return the cluster id. Note that it may be `std::nullopt` if the metadata request was sent to a broker
    /// without support for cluster ids.
    /// @return the cluster id, or `std::nullopt`
    [[nodiscard]] const std::optional<std::string>& cluster_id() const { return impl_->cluster_id; }

    /// Whether `a` and `b` have the same cluster id.
    /// @param a a cluster resource
    /// @param b another cluster resource
    /// @return true when the cluster ids are equal, both absent included
    friend bool operator==(const ClusterResource& a, const ClusterResource& b) {
        return a.impl_ == b.impl_ || a.cluster_id() == b.cluster_id();
    }

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout. The value is
    // immutable, so copies share it.
    struct Impl {
        std::optional<std::string> cluster_id;
    };
    std::shared_ptr<const Impl> impl_;
};

/// Returns `ClusterResource(clusterId=<id>)`, as Java's toString, with `null` for an absent id.
/// @param resource a cluster resource
/// @return the class name and the cluster id
inline std::string to_string(const ClusterResource& resource) {
    return "ClusterResource(clusterId=" + resource.cluster_id().value_or("null") + ")";
}

/// Writes `to_string(resource)` to `out`.
/// @param out the stream
/// @param resource a cluster resource
/// @return `out`
inline std::ostream& operator<<(std::ostream& out, const ClusterResource& resource) {
    return out << to_string(resource);
}

}  // namespace confluent::kafka::common

/// Hashes a cluster resource from its cluster id, as Java's Objects.hash(clusterId).
template <>
struct std::hash<confluent::kafka::common::ClusterResource> {
    std::size_t operator()(const confluent::kafka::common::ClusterResource& resource) const noexcept {
        const auto& id = resource.cluster_id();
        return 31U + (id ? std::hash<std::string>{}(*id) : 0U);
    }
};
