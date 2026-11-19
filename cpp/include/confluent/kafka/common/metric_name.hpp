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
#include <string>
#include <string_view>
#include <unordered_map>

namespace confluent::kafka::common {

/// The `MetricName` class encapsulates a metric's name, logical group and its related attributes. It should be
/// constructed using metrics.metric_name(...).
///
/// This class captures the following parameters:
///
///      **name** The name of the metric
///      **group** logical group name of the metrics to which this metric belongs.
///      **description** A human-readable description to include in the metric. This is optional.
///      **tags** additional key/value attributes of the metric. This is optional.
///
/// group, tags parameters can be used to create unique metric names while reporting in JMX or any custom reporting.
///
/// Ex: standard JMX MBean can be constructed like **domainName:type=group,key1=val1,key2=val2**
class CONFLUENT_KAFKA_CPP_API MetricName {
public:
    /// Please create MetricName by method `Metrics::metric_name(name, group, description, tags)`
    ///
    /// @param name        The name of the metric
    /// @param group       logical group name of the metrics to which this metric belongs
    /// @param description A human-readable description to include in the metric
    /// @param tags        additional key/value attributes of the metric
    MetricName(std::string_view name, std::string_view group, std::string_view description,
               const std::unordered_map<std::string, std::string>& tags);

    /// Copies `other`, sharing its immutable value.
    /// @param other the metric name to copy
    MetricName(const MetricName& other);

    /// Moves `other`, which is left empty and may only be assigned to or destroyed.
    /// @param other the metric name to move
    MetricName(MetricName&& other) noexcept;

    /// Copies `other` into this metric name.
    /// @param other the metric name to copy
    /// @return this metric name
    MetricName& operator=(const MetricName& other);

    /// Moves `other` into this metric name; `other` is left empty.
    /// @param other the metric name to move
    /// @return this metric name
    MetricName& operator=(MetricName&& other) noexcept;

    ~MetricName();

    /// Returns the name of the metric.
    /// @return the name
    [[nodiscard]] const std::string& name() const;

    /// Returns the logical group name of the metrics to which this metric belongs.
    /// @return the group
    [[nodiscard]] const std::string& group() const;

    /// Returns the additional key/value attributes of the metric.
    /// @return the tags
    [[nodiscard]] const std::unordered_map<std::string, std::string>& tags() const;

    /// Returns the human-readable description of the metric.
    /// @return the description
    [[nodiscard]] const std::string& description() const;

    /// Whether `a` and `b` have the same name, group and tags; the description is not compared, as in Java.
    /// @param a a metric name
    /// @param b another metric name
    /// @return true when the group, the name and the tags are equal
    friend CONFLUENT_KAFKA_CPP_API bool operator==(const MetricName& a, const MetricName& b);

private:
    // cpp/CLAUDE.md §12.3: one pointer, so a field Java adds later keeps the layout.
    struct Impl;
    std::shared_ptr<const Impl> impl_;
};

/// Returns `MetricName [name=<name>, group=<group>, description=<description>, tags={<k>=<v>, ...}]`, as Java's
/// toString; the tags are printed in key order, where Java prints its map's iteration order.
/// @param metric_name a metric name
/// @return the metric name's fields
CONFLUENT_KAFKA_CPP_API std::string to_string(const MetricName& metric_name);

/// Writes `to_string(metric_name)` to `out`.
/// @param out the stream
/// @param metric_name a metric name
/// @return `out`
CONFLUENT_KAFKA_CPP_API std::ostream& operator<<(std::ostream& out, const MetricName& metric_name);

}  // namespace confluent::kafka::common

/// Hashes a metric name from its group, name and tags, as Java's hashCode reads them; the tags' hash is the
/// order-independent sum of Java's Map.hashCode.
template <>
struct CONFLUENT_KAFKA_CPP_API std::hash<confluent::kafka::common::MetricName> {
    /// Returns the hash of `metric_name`.
    /// @param metric_name a metric name
    /// @return its hash
    std::size_t operator()(const confluent::kafka::common::MetricName& metric_name) const noexcept;
};
