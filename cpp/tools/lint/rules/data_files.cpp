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

#include "data_files.hpp"

#include <algorithm>
#include <fstream>
#include <functional>
#include <set>
#include <sstream>
#include <utility>

namespace cklint::rules {
namespace {

std::string read(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

void error(DataFile& file, const DataLine& line, const std::string& message) {
    file.errors.push_back(file.path + ":" + std::to_string(line.line) + ": " + message);
}

/// Checks field `index` of every line against `allowed`.
void check_field(DataFile& file, std::size_t index, const std::set<std::string, std::less<>>& allowed,
                 std::string_view what) {
    for (const auto& line : file.lines) {
        if (index < line.fields.size() && allowed.count(line.fields[index]) == 0) {
            std::string list;
            for (const auto& a : allowed) {
                list += (list.empty() ? "" : ", ") + a;
            }
            error(file, line, std::string(what) + " `" + line.fields[index] + "` is not one of " + list);
        }
    }
}

/// A Java member: `<binary name>#<signature or FIELD>`.
void check_member(DataFile& file, std::size_t index) {
    for (const auto& line : file.lines) {
        const auto& f = line.fields[index];
        const auto hash = f.find('#');
        if (hash == std::string::npos || hash == 0 || hash + 1 == f.size()) {
            error(file, line, "`" + f + "` is not a Java member `<binary name>#<signature>`");
        }
    }
}

/// The errors in line order, whichever check found them.
DataFile sorted(DataFile file) {
    auto line_of = [&](const std::string& e) {
        const auto start = file.path.size() + 1;
        return std::stoul(e.substr(start, e.find(':', start) - start));
    };
    std::stable_sort(file.errors.begin(), file.errors.end(),
                     [&](const auto& a, const auto& b) { return line_of(a) < line_of(b); });
    return file;
}

DataFile decisions(DataFile file) {
    check_member(file, 0);
    check_field(file, 2, {"nullable", "ordered", "set"}, "decision");
    check_field(file, 3, {"body", "field", "client-code", "javadoc"}, "source");
    return sorted(std::move(file));
}

DataFile scope_excluded(DataFile file) {
    for (const auto& line : file.lines) {
        if (line.fields[1].rfind("§", 0) != 0) {
            error(file, line, "the rule `" + line.fields[1] + "` must cite cpp/CLAUDE.md as §<n>");
        }
    }
    return sorted(std::move(file));
}

}  // namespace

DataFile parse_data_file(std::string path, std::string_view text, std::size_t fixed, bool free_text_required) {
    DataFile file;
    file.path = std::move(path);
    std::size_t number = 0;
    std::size_t start = 0;
    while (start <= text.size()) {
        auto end = text.find('\n', start);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        auto raw = text.substr(start, end - start);
        start = end + 1;
        ++number;
        if (!raw.empty() && raw.back() == '\r') {
            raw.remove_suffix(1);
        }
        const auto first = raw.find_first_not_of(' ');
        if (first == std::string_view::npos || raw[first] == '#') {
            if (end == text.size()) {
                break;
            }
            continue;
        }
        DataLine line;
        line.line = number;
        std::size_t pos = first;
        while (line.fields.size() < fixed && pos < raw.size()) {
            const auto space = raw.find(' ', pos);
            line.fields.emplace_back(raw.substr(pos, space == std::string_view::npos ? raw.size() - pos : space - pos));
            pos = space == std::string_view::npos ? raw.size() : raw.find_first_not_of(' ', space);
            if (pos == std::string_view::npos) {
                pos = raw.size();
            }
        }
        line.fields.emplace_back(raw.substr(pos));
        if (line.fields.size() < fixed + 1) {
            error(file, line,
                  "expected " + std::to_string(fixed) + " fields" + (free_text_required ? " and a reason" : ""));
        } else if (free_text_required && line.fields.back().empty()) {
            error(file, line, "missing the reason");
        } else {
            file.lines.push_back(std::move(line));
        }
        if (end == text.size()) {
            break;
        }
    }
    return file;
}

DataFile read_data_file(const std::string& path, std::size_t fixed, bool free_text_required) {
    return parse_data_file(path, read(path), fixed, free_text_required);
}

DataFile read_decisions(const std::string& path) {
    return decisions(read_data_file(path, 4, true));
}

DataFile parse_decisions(std::string path, std::string_view text) {
    return decisions(parse_data_file(std::move(path), text, 4, true));
}

DataFile read_deviations(const std::string& path) {
    auto file = read_data_file(path, 3, true);
    for (const auto& line : file.lines) {
        const auto& ref = line.fields[2];
        if (ref.rfind("§", 0) != 0 && ref.rfind("ledger:", 0) != 0) {
            error(file, line, "the reference `" + ref + "` must be a §rule or ledger:<title>");
        }
    }
    return sorted(std::move(file));
}

DataFile read_pairs(const std::string& path) {
    auto file = read_data_file(path, 2, true);
    check_member(file, 0);
    return sorted(std::move(file));
}

DataFile read_scope_excluded(const std::string& path) {
    return scope_excluded(read_data_file(path, 2, true));
}

DataFile parse_scope_excluded(std::string path, std::string_view text) {
    return scope_excluded(parse_data_file(std::move(path), text, 2, true));
}

DataFile read_baseline(const std::string& path) {
    auto file = read_data_file(path, 2, false);
    check_field(file, 0, {"missing-class", "partial-class"}, "kind");
    return sorted(std::move(file));
}

}  // namespace cklint::rules
