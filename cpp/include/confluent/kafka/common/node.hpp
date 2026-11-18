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
#include <optional>
#include <string>
#include <string_view>

namespace confluent::kafka::common {

/// Information about a Kafka node
class CONFLUENT_KAFKA_CPP_API Node {
public:
    /// Creates the node `id` at `host`:`port`, with no rack and not fenced.
    /// @param id the node id
    /// @param host the host name, or `std::nullopt`
    /// @param port the port
    Node(int32_t id, std::optional<std::string_view> host, int32_t port);

    /// Creates the node `id` at `host`:`port` in `rack`, not fenced.
    /// @param id the node id
    /// @param host the host name, or `std::nullopt`
    /// @param port the port
    /// @param rack the rack, or `std::nullopt` when it has none
    Node(int32_t id, std::optional<std::string_view> host, int32_t port, std::optional<std::string_view> rack);

    /// Creates the node `id` at `host`:`port` in `rack`.
    /// @param id the node id
    /// @param host the host name, or `std::nullopt`
    /// @param port the port
    /// @param rack the rack, or `std::nullopt` when it has none
    /// @param is_fenced whether the broker is fenced
    Node(int32_t id, std::optional<std::string_view> host, int32_t port, std::optional<std::string_view> rack,
         bool is_fenced);

    /// Copies `other`, sharing its immutable value.
    /// @param other the node to copy
    Node(const Node& other);

    /// Moves `other`, which is left empty and may only be assigned to or destroyed.
    /// @param other the node to move
    Node(Node&& other) noexcept;

    /// Copies `other` into this node.
    /// @param other the node to copy
    /// @return this node
    Node& operator=(const Node& other);

    /// Moves `other` into this node; `other` is left empty.
    /// @param other the node to move
    /// @return this node
    Node& operator=(Node&& other) noexcept;

    ~Node();

    /// Returns the placeholder node: id -1, empty host, port -1.
    /// @return the placeholder node
    static const Node& no_node();

    /// Check whether this node is empty, which may be the case if no_node() is used as a placeholder
    /// in a response payload with an error.
    /// @return true if it is, false otherwise
    [[nodiscard]] bool is_empty() const;

    /// The node id of this node
    /// @return the node id
    [[nodiscard]] int32_t id() const;

    /// String representation of the node id.
    /// Typically, the integer id is used to serialize over the wire, the string representation is used as an
    /// identifier with NetworkClient code
    /// @return the node id as a string
    [[nodiscard]] const std::string& id_string() const;

    /// The host name for this node
    /// @return the host name, or `std::nullopt`
    [[nodiscard]] const std::optional<std::string>& host() const;

    /// The port for this node
    /// @return the port
    [[nodiscard]] int32_t port() const;

    /// True if this node has a defined rack
    /// @return whether the rack is defined
    [[nodiscard]] bool has_rack() const;

    /// The rack for this node
    /// @return the rack, or `std::nullopt`
    [[nodiscard]] const std::optional<std::string>& rack() const;

    /// Returns whether this node is fenced.
    ///
    /// This applies to broker nodes only. For controller quorum nodes, this field
    /// is not relevant and is defined to be `false`.
    /// @return whether this node is fenced
    [[nodiscard]] bool is_fenced() const;

    /// Whether `a` and `b` have the same id, host, port, rack and fencing.
    /// @param a a node
    /// @param b another node
    /// @return true when every field but the id string is equal, as Java's equals
    friend CONFLUENT_KAFKA_CPP_API bool operator==(const Node& a, const Node& b);

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout.
    struct Impl;
    std::shared_ptr<const Impl> impl_;
};

/// Returns `<host>:<port> (id: <id> rack: <rack> isFenced: <is_fenced>)`, as Java's toString,
/// with `null` for an absent host or rack.
/// @param node a node
/// @return the node's fields
CONFLUENT_KAFKA_CPP_API std::string to_string(const Node& node);

/// Writes `to_string(node)` to `out`.
/// @param out the stream
/// @param node a node
/// @return `out`
CONFLUENT_KAFKA_CPP_API std::ostream& operator<<(std::ostream& out, const Node& node);

}  // namespace confluent::kafka::common

/// Hashes a node from the fields Java's hashCode reads, in its order.
template <>
struct CONFLUENT_KAFKA_CPP_API std::hash<confluent::kafka::common::Node> {
    /// Returns the hash of `node`.
    /// @param node a node
    /// @return its hash
    std::size_t operator()(const confluent::kafka::common::Node& node) const noexcept;
};
