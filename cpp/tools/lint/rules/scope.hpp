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

// cpp/CLAUDE.md §1.4: the Java types the binding translates, computed from
// the client families outward, minus what scope-excluded.txt names.

#include <map>
#include <set>
#include <string>
#include <vector>

#include "data_files.hpp"
#include "java_model.hpp"

namespace cklint::rules {

struct Scope {
    /// Binary name -> why it is in scope: `seed`, or the edge that reached it
    /// (`org.apache.kafka.clients.producer.Producer#send(ProducerRecord)`).
    std::map<std::string, std::string> types;
    /// Lines of scope-excluded.txt that matched (line numbers), for staleness.
    std::set<std::size_t> used_exclusions;
    /// Seeds missing from the model.
    std::vector<std::string> missing_seeds;
};

/// The seeds: the Producer, Consumer, ShareConsumer and Admin families and
/// the configuration classes of §7.
const std::vector<std::string>& scope_seeds();

/// Computes the scope. Errors are not followed (§6 scopes them from the FFI
/// enum); neither are deprecated members and types, nor non-Public ones.
Scope compute_scope(const java::Model& model, const DataFile& excluded);

}  // namespace cklint::rules
