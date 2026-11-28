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

// confluent-kafka-header: the textual rules of a public header (cpp/CLAUDE.md
// §1.2, §3.1, §13.4), read from every header the translation unit includes.

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "checks.hpp"
#include "clang/Basic/SourceManager.h"

namespace cklint::tidy {
namespace {

std::vector<std::string_view> lines_of(std::string_view text) {
    std::vector<std::string_view> out;
    std::size_t start = 0;
    while (start <= text.size()) {
        const auto end = text.find('\n', start);
        out.push_back(text.substr(start, end == std::string_view::npos ? std::string_view::npos : end - start));
        if (end == std::string_view::npos) {
            break;
        }
        start = end + 1;
    }
    return out;
}

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return {};
    }
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

/// Whether `line` includes the C header, directly or through src/detail/ffi.hpp.
bool is_c_header_include(std::string_view line) {
    const auto first = line.find_first_not_of(" \t");
    if (first == std::string_view::npos || line.substr(first, 1) != "#") {
        return false;
    }
    return line.find("include") != std::string_view::npos &&
           (line.find("confluent_kafka_bindings.h") != std::string_view::npos ||
            line.find("confluent_kafka.h") != std::string_view::npos ||
            line.find("detail/ffi.hpp") != std::string_view::npos);
}

}  // namespace

void HeaderCheck::check(const clang::ast_matchers::MatchFinder::MatchResult& result) {
    const auto& sm = *result.SourceManager;
    for (auto it = sm.fileinfo_begin(); it != sm.fileinfo_end(); ++it) {
        const auto rel = relative_to_include_root(it->first.getName(), settings());
        if (rel.rfind("confluent/kafka/", 0) != 0 || rel.size() < 4 || rel.compare(rel.size() - 4, 4, ".hpp") != 0) {
            continue;
        }
        const auto fid = sm.translateFile(it->first);
        if (fid.isInvalid()) {
            continue;
        }
        const auto text = sm.getBufferDataOrNone(fid);
        if (!text) {
            continue;
        }
        const auto at_line = [&](std::size_t line) { return sm.translateLineCol(fid, static_cast<unsigned>(line), 1); };
        const auto lines = lines_of(*text);

        // §13.4: the Apache 2.0 header, copyright Confluent Inc.
        bool apache = false;
        bool confluent = false;
        for (std::size_t i = 0; i < lines.size() && i < 20; ++i) {
            apache =
                apache || lines[i].find("Licensed under the Apache License, Version 2.0") != std::string_view::npos;
            confluent = confluent || lines[i].find("Confluent Inc.") != std::string_view::npos;
        }
        if (!apache || !confluent) {
            diag(at_line(1), "<%0> lacks the Apache 2.0 header, copyright Confluent Inc. (cpp/CLAUDE.md §3.1, §13.4)")
                << rel;
        }
        // §3.1
        bool pragma_once = false;
        for (const auto& l : lines) {
            pragma_once = pragma_once || l == "#pragma once";
        }
        if (!pragma_once) {
            diag(at_line(1), "<%0> lacks #pragma once (cpp/CLAUDE.md §3.1)") << rel;
        }
        for (std::size_t i = 0; i < lines.size(); ++i) {
            const auto& l = lines[i];
            if (is_c_header_include(l)) {
                diag(at_line(i + 1),
                     "a public header never includes the C header: the library's sources reach it through "
                     "src/detail/ffi.hpp (cpp/CLAUDE.md §1.2)");
            }
            if (l.find("TODO") != std::string_view::npos || l.find("FIXME") != std::string_view::npos) {
                diag(at_line(i + 1),
                     "no TODO or FIXME in the binding: finish it, or fail with the right error "
                     "(cpp/CLAUDE.md §13.4)");
            }
            if (l.find("NOLINT(confluent-kafka-") != std::string_view::npos ||
                l.find("NOLINTNEXTLINE(confluent-kafka-") != std::string_view::npos) {
                // Reported on line 1: a NOLINT on its own line would silence this report too.
                diag(at_line(1),
                     "line %0 silences a confluent-kafka check: a deviation goes in tools/lint/data/deviations.txt, "
                     "with its rule or accepted ledger entry")
                    << static_cast<unsigned>(i + 1);
            }
        }
        // §3.1: one umbrella header per module includes every header of it.
        // A module header of a nested module (common/record.hpp beside common/record/) is not
        // required in its parent's: users include the module they use.
        const auto dir_end = rel.rfind('/');
        const auto dir = rel.substr(0, dir_end);
        std::error_code ec;
        const bool is_module_header =
            std::filesystem::is_directory(settings().include_root + "/" + rel.substr(0, rel.size() - 4), ec);
        if (!is_module_header && dir != "confluent/kafka" && dir.rfind("confluent/kafka/detail", 0) != 0) {
            const auto umbrella = dir + ".hpp";
            const auto umbrella_text = read_file(settings().include_root + "/" + umbrella);
            if (umbrella_text.empty()) {
                diag(at_line(1), "<%0> has no module header <%1> including it (cpp/CLAUDE.md §3.1)") << rel << umbrella;
            } else if (umbrella_text.find("<" + rel + ">") == std::string::npos &&
                       umbrella_text.find("\"" + rel + "\"") == std::string::npos) {
                diag(at_line(1), "the module header <%0> does not include <%1> (cpp/CLAUDE.md §3.1)")
                    << umbrella << rel;
            }
        }
    }
}

}  // namespace cklint::tidy
