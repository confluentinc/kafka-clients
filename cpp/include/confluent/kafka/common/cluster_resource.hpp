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
#include <functional>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace confluent::kafka::common {

/// The `ClusterResource` class encapsulates metadata for a Kafka cluster.
class CONFLUENT_KAFKA_CPP_API ClusterResource {
public:
    /// Create `ClusterResource` with a cluster id. Note that cluster id may be `std::nullopt` if the
    /// metadata request was sent to a broker without support for cluster ids.
    /// @param cluster_id The cluster id
    explicit ClusterResource(std::optional<std::string_view> cluster_id);

    /// Copies `other`, sharing its immutable value.
    /// @param other the cluster resource to copy
    ClusterResource(const ClusterResource& other);

    /// Moves `other`, which is left empty and may only be assigned to or destroyed.
    /// @param other the cluster resource to move
    ClusterResource(ClusterResource&& other) noexcept;

    /// Copies `other` into this cluster resource.
    /// @param other the cluster resource to copy
    /// @return this cluster resource
    ClusterResource& operator=(const ClusterResource& other);

    /// Moves `other` into this cluster resource; `other` is left empty.
    /// @param other the cluster resource to move
    /// @return this cluster resource
    ClusterResource& operator=(ClusterResource&& other) noexcept;

    ~ClusterResource();

    /// Return the cluster id. Note that it may be `std::nullopt` if the metadata request was sent to a broker
    /// without support for cluster ids.
    /// @return the cluster id, or `std::nullopt`
    [[nodiscard]] const std::optional<std::string>& cluster_id() const;

    /// Whether `a` and `b` have the same cluster id.
    /// @param a a cluster resource
    /// @param b another cluster resource
    /// @return true when the cluster ids are equal, both absent included
    friend CONFLUENT_KAFKA_CPP_API bool operator==(const ClusterResource& a, const ClusterResource& b);

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout.
    struct Impl;
    std::shared_ptr<const Impl> impl_;
};

/// Returns `ClusterResource(clusterId=<id>)`, as Java's toString, with `null` for an absent id.
/// @param resource a cluster resource
/// @return the class name and the cluster id
CONFLUENT_KAFKA_CPP_API std::string to_string(const ClusterResource& resource);

/// Writes `to_string(resource)` to `out`.
/// @param out the stream
/// @param resource a cluster resource
/// @return `out`
CONFLUENT_KAFKA_CPP_API std::ostream& operator<<(std::ostream& out, const ClusterResource& resource);

}  // namespace confluent::kafka::common

/// Hashes a cluster resource from its cluster id, as Java's Objects.hash(clusterId).
template <>
struct CONFLUENT_KAFKA_CPP_API std::hash<confluent::kafka::common::ClusterResource> {
    /// Returns the hash of `resource`.
    /// @param resource a cluster resource
    /// @return its hash
    std::size_t operator()(const confluent::kafka::common::ClusterResource& resource) const noexcept;
};
