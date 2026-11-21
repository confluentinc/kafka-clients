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

// ck_java_model: the Java side of the C++ translation linter, as a command.
//
//   ck_java_model parse [-j <threads>] <dir>...
//       parse every .java file under each <dir> and report syntax errors and
//       timing
//   ck_java_model dump [-j <threads>] <source-root>
//       print the Java model of a source root (kafka/clients/src/main/java),
//       one declaration per line; exits 1 if the model has diagnostics
//   ck_java_model scope [-j <threads>] <source-root>
//       print the types cpp/CLAUDE.md §1.4 puts in scope, each with what
//       reached it; scope-excluded.txt is read from the linter's data/
//   ck_java_model expected [-j <threads>] <source-root> <binary name>...
//       print the C++ shape cpp/CLAUDE.md derives from each Java type
//   ck_java_model markers [-j <threads>] <source-root>
//       print the marker of every in-scope type and of its public members
//   ck_java_model javadoc [-j <threads>] <source-root> <marker>
//       print the Markdown each Javadoc of a marker translates to, as
//       `cargo xtask javadoc <marker>` does, minus its renames
//   ck_java_model api-diff <old inventory> <new inventory>
//       compare two API inventories (the confluent-kafka-inventory check's):
//       a declaration removed or changed is breaking, one added is additive
//       (cpp/CLAUDE.md §12.4); exits 1 when something breaks
//   ck_java_model words [-j <threads>] <source-root> < markers
//       print `<marker>\t<words>` for each marker read from stdin, as
//       `cargo xtask javadoc --words` does: the word count of each Javadoc a
//       doc translating it is measured against
//
// -j defaults to the number of hardware threads.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "expected.hpp"
#include "java_model.hpp"
#include "java_syntax.hpp"
#include "javadoc.hpp"
#include "scope.hpp"

namespace {

namespace fs = std::filesystem;

std::string read_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

std::vector<fs::path> java_files_under(const std::vector<std::string>& roots) {
    std::vector<fs::path> files;
    for (const auto& root : roots) {
        for (const auto& entry : fs::recursive_directory_iterator(root)) {
            if (entry.is_regular_file() && entry.path().extension() == ".java") {
                files.push_back(entry.path());
            }
        }
    }
    std::sort(files.begin(), files.end());
    return files;
}

struct ParsedFile {
    std::vector<cklint::java::SyntaxError> errors;
    std::chrono::steady_clock::duration elapsed{};
};

// Parses every file on `jobs` threads; the result at index i is files[i]'s,
// so what is printed does not depend on the scheduling.
std::vector<ParsedFile> parse_all(const std::vector<fs::path>& files, unsigned jobs) {
    std::vector<ParsedFile> results(files.size());
    std::atomic<std::size_t> next{0};
    auto worker = [&] {
        for (std::size_t i = next++; i < files.size(); i = next++) {
            const auto text = read_file(files[i]);
            const auto start = std::chrono::steady_clock::now();
            results[i].errors = cklint::java::check_syntax(text).errors;
            results[i].elapsed = std::chrono::steady_clock::now() - start;
        }
    };
    std::vector<std::thread> threads;
    threads.reserve(jobs);
    for (unsigned t = 0; t < jobs; ++t) {
        threads.emplace_back(worker);
    }
    for (auto& thread : threads) {
        thread.join();
    }
    return results;
}

int run_parse(const std::vector<std::string>& roots, unsigned jobs) {
    using std::chrono::duration_cast;
    using std::chrono::milliseconds;
    const auto files = java_files_under(roots);
    if (files.empty()) {
        std::cerr << "no .java file found\n";
        return EXIT_FAILURE;
    }
    const auto start = std::chrono::steady_clock::now();
    const auto results = parse_all(files, jobs);
    const auto total = duration_cast<milliseconds>(std::chrono::steady_clock::now() - start);

    std::size_t failed = 0;
    std::size_t slowest = 0;
    for (std::size_t i = 0; i < files.size(); ++i) {
        if (results[i].elapsed > results[slowest].elapsed) {
            slowest = i;
        }
        if (results[i].errors.empty()) {
            continue;
        }
        ++failed;
        for (const auto& error : results[i].errors) {
            std::cerr << files[i].string() << ':' << error.line << ':' << error.column + 1 << ": " << error.message
                      << '\n';
        }
    }
    std::cout << "parsed " << files.size() << " files in " << total.count() << " ms on " << jobs << " threads; slowest "
              << files[slowest].string() << " (" << duration_cast<milliseconds>(results[slowest].elapsed).count()
              << " ms); " << failed << " with syntax errors\n";
    return failed == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

int run_dump(const std::string& root, unsigned jobs) {
    const auto model = cklint::java::load_model(root, jobs);
    std::cout << cklint::java::dump(model);
    return model.diagnostics.empty() ? EXIT_SUCCESS : EXIT_FAILURE;
}

int run_scope(const std::string& root, unsigned jobs) {
    const auto model = cklint::java::load_model(root, jobs);
    const auto excluded = cklint::rules::read_scope_excluded(std::string(CK_LINT_DATA_DIR) + "/scope-excluded.txt");
    for (const auto& e : excluded.errors) {
        std::cerr << e << '\n';
    }
    const auto scope = cklint::rules::compute_scope(model, excluded);
    for (const auto& [name, reason] : scope.types) {
        std::cout << name << '\t' << reason << '\n';
    }
    for (const auto& seed : scope.missing_seeds) {
        std::cerr << "seed not in the model: " << seed << '\n';
    }
    for (const auto& line : excluded.lines) {
        if (scope.used_exclusions.count(line.line) == 0) {
            std::cerr << excluded.path << ':' << line.line << ": excludes nothing in scope: " << line.fields[0] << '\n';
        }
    }
    std::cerr << scope.types.size() << " types in scope\n";
    const bool clean =
        excluded.errors.empty() && scope.missing_seeds.empty() && scope.used_exclusions.size() == excluded.lines.size();
    return clean ? EXIT_SUCCESS : EXIT_FAILURE;
}

int run_expected(const std::string& root, const std::vector<std::string>& names, unsigned jobs) {
    const auto model = cklint::java::load_model(root, jobs);
    int status = EXIT_SUCCESS;
    for (const auto& name : names) {
        const auto* type = model.find(name);
        if (type == nullptr) {
            std::cerr << "no type " << name << " in the model\n";
            status = EXIT_FAILURE;
            continue;
        }
        const auto e = cklint::rules::expected(model, *type);
        std::cout << cklint::rules::render(e) << '\n';
        if (!e.problems.empty()) {
            status = EXIT_FAILURE;
        }
    }
    return status;
}

int run_markers(const std::string& root, unsigned jobs) {
    const auto model = cklint::java::load_model(root, jobs);
    const auto excluded = cklint::rules::read_scope_excluded(std::string(CK_LINT_DATA_DIR) + "/scope-excluded.txt");
    const auto scope = cklint::rules::compute_scope(model, excluded);
    for (const auto& [name, reason] : scope.types) {
        const auto* type = model.find(name);
        if (type == nullptr) {
            continue;
        }
        std::cout << name << '\n';
        std::vector<std::string> members;
        for (const auto* list : {&type->constructors, &type->methods}) {
            for (const auto& m : *list) {
                if (!m.implicit && m.visibility == cklint::java::Visibility::PUBLIC) {
                    members.push_back(m.signature());
                }
            }
        }
        for (const auto& f : type->fields) {
            if (f.visibility == cklint::java::Visibility::PUBLIC) {
                members.push_back(f.name);
            }
        }
        for (const auto& c : type->enum_constants) {
            members.push_back(c.name);
        }
        std::sort(members.begin(), members.end());
        members.erase(std::unique(members.begin(), members.end()), members.end());
        for (const auto& m : members) {
            std::cout << name << '#' << m << '\n';
        }
    }
    return EXIT_SUCCESS;
}

int run_javadoc(const std::string& root, const std::string& marker, unsigned jobs) {
    const auto model = cklint::java::load_model(root, jobs);
    const auto docs = cklint::rules::followed_javadocs(model, marker);
    if (docs.empty()) {
        std::cout << '`' << marker << "` has no Javadoc\n";
        return EXIT_SUCCESS;
    }
    for (std::size_t i = 0; i < docs.size(); ++i) {
        const auto markdown = cklint::rules::javadoc_markdown(docs[i]);
        std::cout << (i > 0 ? "\n----\n\n" : "") << markdown << "\n\n(" << cklint::rules::word_count(markdown)
                  << " words)\n";
    }
    return EXIT_SUCCESS;
}

int run_words(const std::string& root, unsigned jobs) {
    const auto model = cklint::java::load_model(root, jobs);
    std::string marker;
    while (std::getline(std::cin, marker)) {
        if (marker.empty()) {
            continue;
        }
        const auto hash = marker.find('#');
        if (model.find(marker.substr(0, hash)) == nullptr) {
            std::cout << marker << "\t?\n";
            continue;
        }
        std::string words;
        for (const auto w : cklint::rules::javadoc_words(model, marker)) {
            words += (words.empty() ? "" : ",") + std::to_string(w);
        }
        std::cout << marker << '\t' << words << '\n';
    }
    return EXIT_SUCCESS;
}

struct InventoryLine {
    std::string kind;
    std::string detail;
};

std::vector<std::pair<std::string, InventoryLine>> read_inventory(const std::string& path, bool& ok) {
    std::vector<std::pair<std::string, InventoryLine>> out;
    std::ifstream in(path);
    ok = static_cast<bool>(in);
    std::string line;
    while (std::getline(in, line)) {
        const auto first = line.find('\t');
        const auto second = line.find('\t', first == std::string::npos ? first : first + 1);
        if (first == std::string::npos || second == std::string::npos) {
            continue;
        }
        out.emplace_back(line.substr(first + 1, second - first - 1),
                         InventoryLine{line.substr(0, first), line.substr(second + 1)});
    }
    return out;
}

int run_api_diff(const std::string& old_path, const std::string& new_path) {
    bool old_ok = false;
    bool new_ok = false;
    const auto before = read_inventory(old_path, old_ok);
    const auto after = read_inventory(new_path, new_ok);
    if (!old_ok || !new_ok) {
        std::cerr << "cannot read " << (old_ok ? new_path : old_path) << '\n';
        return 2;
    }
    std::map<std::string, InventoryLine> now(after.begin(), after.end());
    std::map<std::string, InventoryLine> was(before.begin(), before.end());
    std::size_t breaking = 0;
    std::size_t additive = 0;
    for (const auto& [key, line] : before) {
        const auto it = now.find(key);
        if (it == now.end()) {
            std::cout << "breaking: removed " << line.kind << ' ' << key << '\n';
            ++breaking;
        } else if (it->second.kind != line.kind || it->second.detail != line.detail) {
            std::cout << "breaking: changed " << line.kind << ' ' << key << ": " << line.detail << " -> "
                      << it->second.detail << '\n';
            ++breaking;
        }
    }
    for (const auto& [key, line] : after) {
        if (was.count(key) == 0) {
            std::cout << "additive: added " << line.kind << ' ' << key << '\n';
            ++additive;
        }
    }
    std::cout << breaking << " breaking, " << additive << " additive\n";
    return breaking == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}

int usage() {
    std::cerr << "usage: ck_java_model parse [-j <threads>] <dir>...\n"
                 "       ck_java_model dump [-j <threads>] <source-root>\n"
                 "       ck_java_model scope [-j <threads>] <source-root>\n"
                 "       ck_java_model expected [-j <threads>] <source-root> <binary name>...\n"
                 "       ck_java_model markers [-j <threads>] <source-root>\n"
                 "       ck_java_model javadoc [-j <threads>] <source-root> <marker>\n"
                 "       ck_java_model api-diff <old inventory> <new inventory>\n"
                 "       ck_java_model words [-j <threads>] <source-root> < markers\n";
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    std::vector<std::string> args(argv + 1, argv + argc);
    if (args.empty()) {
        return usage();
    }
    const auto command = args.front();
    args.erase(args.begin());
    unsigned jobs = std::max(1U, std::thread::hardware_concurrency());
    if (args.size() >= 2 && args.front() == "-j") {
        jobs = static_cast<unsigned>(std::max(1, std::atoi(args[1].c_str())));
        args.erase(args.begin(), args.begin() + 2);
    }
    if (command == "parse" && !args.empty()) {
        return run_parse(args, jobs);
    }
    if (command == "dump" && args.size() == 1) {
        return run_dump(args.front(), jobs);
    }
    if (command == "scope" && args.size() == 1) {
        return run_scope(args.front(), jobs);
    }
    if (command == "markers" && args.size() == 1) {
        return run_markers(args.front(), jobs);
    }
    if (command == "api-diff" && args.size() == 2) {
        return run_api_diff(args[0], args[1]);
    }
    if (command == "javadoc" && args.size() == 2) {
        return run_javadoc(args[0], args[1], jobs);
    }
    if (command == "words" && args.size() == 1) {
        return run_words(args.front(), jobs);
    }
    if (command == "expected" && args.size() >= 2) {
        return run_expected(args.front(), {std::next(args.begin()), args.end()}, jobs);
    }
    return usage();
}
