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

// The linter's data files (cpp/tools/lint/data/): one item per line, fields
// separated by spaces, the last field free text; `#` starts a comment line.
// Every line must be used by a check, or it is reported as stale.
//
//   decisions.txt       <Java member> <parameter|return> <nullable|ordered|set> <body|field|client-code|javadoc> <why>
//   deviations.txt      <kind> <subject> <§rule|ledger:<title>> <why>
//   pairs.txt           <Java member> <C++ name> <why>
//   scope-excluded.txt  <FQN|package.*> <§rule> <why>
//   baseline.txt        <missing-class|partial-class> <FQN>
//
// A Java member is `<binary name>#<signature>` in design/current/java-deprecated.txt's
// form (`org.apache.kafka.common.Node#rack()`), or `<binary name>#<FIELD>`.

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace cklint::rules {

struct DataLine {
    std::size_t line = 0;
    std::vector<std::string> fields;  ///< the fixed fields, then the free text (possibly empty)
};

struct DataFile {
    std::string path;
    std::vector<DataLine> lines;
    /// Malformed lines: `path:line: message`.
    std::vector<std::string> errors;
};

/// Parses `text` with `fixed` space-separated fields per line, then the free
/// text; `free_text_required` makes an empty free text an error.
DataFile parse_data_file(std::string path, std::string_view text, std::size_t fixed, bool free_text_required);

/// Reads and parses a file; a missing file is an empty one.
DataFile read_data_file(const std::string& path, std::size_t fixed, bool free_text_required);

/// The formats above, validated field by field.
DataFile read_decisions(const std::string& path);
DataFile read_deviations(const std::string& path);
DataFile read_pairs(const std::string& path);
DataFile read_scope_excluded(const std::string& path);
DataFile read_baseline(const std::string& path);

/// Same parsing, from memory, for tests.
DataFile parse_decisions(std::string path, std::string_view text);
DataFile parse_scope_excluded(std::string path, std::string_view text);

}  // namespace cklint::rules
