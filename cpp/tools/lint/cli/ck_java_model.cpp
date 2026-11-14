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
//
// -j defaults to the number of hardware threads.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "java_model.hpp"
#include "java_syntax.hpp"

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

int usage() {
    std::cerr << "usage: ck_java_model parse [-j <threads>] <dir>...\n"
                 "       ck_java_model dump [-j <threads>] <source-root>\n";
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
    return usage();
}
