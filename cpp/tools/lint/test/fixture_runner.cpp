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

// Runs the clang-tidy plugin the way `make lint` does and compares what it
// reports with what the headers expect.
//
//   fixture_runner checks <clang-tidy> <plugin> <check>...
//       the plugin loads and registers every <check>
//   fixture_runner run <clang-tidy> <plugin> <java-root> <include-root> <data-dir> <work-dir> [<compiler arg>...]
//       includes every header under <include-root>/confluent/kafka in one
//       translation unit, runs the confluent-kafka-* checks on it, and
//       requires each diagnostic to match one expectation in the headers and
//       each expectation to be met:
//         // EXPECT: <check> <text>       a diagnostic on this line
//         // EXPECT@<line>: <check> <text> a diagnostic on <line> of this file
//       <text> must appear in the diagnostic's message.

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

namespace {

namespace fs = std::filesystem;

struct Expectation {
    std::string file;
    int line = 0;
    std::string check;
    std::string text;
    bool met = false;
};

struct Diagnostic {
    std::string file;
    int line = 0;
    std::string check;
    std::string message;
};

std::string canonical(const std::string& path) {
    std::error_code ec;
    auto p = fs::weakly_canonical(path, ec);
    return ec ? path : p.generic_string();
}

std::string quote(const std::string& s) {
    return "'" + s + "'";
}

std::string run_command(const std::string& command, int& status) {
    std::array<char, 4096> buffer{};
    std::string out;
    std::unique_ptr<FILE, int (*)(FILE*)> pipe(popen((command + " 2>&1").c_str(), "r"), pclose);
    if (!pipe) {
        status = -1;
        return out;
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
        out += buffer.data();
    }
    status = pclose(pipe.release());
    return out;
}

std::vector<std::string> headers_under(const fs::path& root) {
    std::vector<std::string> out;
    if (!fs::exists(root)) {
        return out;
    }
    for (const auto& e : fs::recursive_directory_iterator(root)) {
        if (e.is_regular_file() && e.path().extension() == ".hpp") {
            out.push_back(e.path().generic_string());
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<Expectation> expectations_in(const std::string& file) {
    static const std::regex here(R"(//\s*EXPECT:\s*(\S+)\s+(.*\S))");
    static const std::regex there(R"(//\s*EXPECT@(\d+):\s*(\S+)\s+(.*\S))");
    std::vector<Expectation> out;
    std::ifstream in(file);
    std::string line;
    for (int number = 1; std::getline(in, line); ++number) {
        std::smatch m;
        auto rest = line;
        while (std::regex_search(rest, m, there) || std::regex_search(rest, m, here)) {
            if (m.size() == 4) {
                out.push_back(Expectation{canonical(file), std::stoi(m[1]), m[2], m[3]});
            } else {
                out.push_back(Expectation{canonical(file), number, m[1], m[2]});
            }
            rest = m.suffix();
        }
    }
    return out;
}

int run_checks(const std::vector<std::string>& args) {
    int status = 0;
    const auto out = run_command(
        quote(args[0]) + " -load " + quote(args[1]) + " -checks='-*,confluent-kafka-*' --list-checks", status);
    int missing = 0;
    for (std::size_t i = 2; i < args.size(); ++i) {
        if (out.find(args[i]) == std::string::npos) {
            std::cerr << "not registered: " << args[i] << '\n';
            ++missing;
        }
    }
    if (status != 0 || missing != 0) {
        std::cerr << out;
        return EXIT_FAILURE;
    }
    std::cout << "the plugin registers " << args.size() - 2 << " checks\n";
    return EXIT_SUCCESS;
}

int run_fixture(const std::vector<std::string>& args) {
    const auto& clang_tidy = args[0];
    const auto& plugin = args[1];
    const auto& java_root = args[2];
    const auto include_root = canonical(args[3]);
    const auto& data_dir = args[4];
    const fs::path work = args[5];
    fs::create_directories(work);

    const auto headers = headers_under(fs::path(include_root) / "confluent" / "kafka");
    std::vector<Expectation> expected;
    {
        std::ofstream probe(work / "probe.cpp");
        for (const auto& h : headers) {
            probe << "#include <" << fs::relative(h, include_root).generic_string() << ">\n";
            const auto found = expectations_in(h);
            expected.insert(expected.end(), found.begin(), found.end());
        }
    }
    const std::string config = "{Checks: '-*,confluent-kafka-*', CheckOptions: {ConfluentKafkaJavaRoot: '" + java_root +
                               "', ConfluentKafkaDataDir: '" + data_dir + "', ConfluentKafkaIncludeRoot: '" +
                               include_root + "'}}";
    std::string command = quote(clang_tidy) + " -load " + quote(plugin) + " --quiet -header-filter='.*' -config=\"" +
                          config + "\" " + quote((work / "probe.cpp").string()) + " -- -std=c++20 -I" +
                          quote(include_root);
    for (std::size_t i = 6; i < args.size(); ++i) {
        command += " " + quote(args[i]);
    }
    int status = 0;
    const auto out = run_command(command, status);

    static const std::regex diagnostic(R"(^(.+):(\d+):\d+: (warning|error): (.*) \[([^\]]+)\]$)");
    static const std::regex compiler_error(R"(^(.+):(\d+):\d+: (fatal )?error: )");
    std::vector<Diagnostic> found;
    bool broken = false;
    std::istringstream lines(out);
    std::string line;
    while (std::getline(lines, line)) {
        std::smatch m;
        if (std::regex_match(line, m, diagnostic) && m[5].str().rfind("confluent-kafka-", 0) == 0) {
            found.push_back(Diagnostic{canonical(m[1]), std::stoi(m[2]), m[5], m[4]});
        } else if (std::regex_search(line, m, compiler_error)) {
            std::cerr << "does not compile: " << line << '\n';
            broken = true;
        }
    }

    int failures = broken ? 1 : 0;
    for (const auto& d : found) {
        auto it = std::find_if(expected.begin(), expected.end(), [&](const Expectation& e) {
            return !e.met && e.file == d.file && e.line == d.line && e.check == d.check &&
                   d.message.find(e.text) != std::string::npos;
        });
        if (it == expected.end()) {
            std::cerr << "unexpected: " << d.file << ':' << d.line << ": [" << d.check << "] " << d.message << '\n';
            ++failures;
        } else {
            it->met = true;
        }
    }
    for (const auto& e : expected) {
        if (!e.met) {
            std::cerr << "missing: " << e.file << ':' << e.line << ": [" << e.check << "] ..." << e.text << "...\n";
            ++failures;
        }
    }
    if (failures != 0) {
        std::cerr << "--- clang-tidy output ---\n" << out;
        return EXIT_FAILURE;
    }
    std::cout << headers.size() << " headers, " << found.size() << " diagnostics, all expected\n";
    return EXIT_SUCCESS;
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::string> args(argv + 1, argv + argc);
    if (args.size() >= 3 && args[0] == "checks") {
        return run_checks({args.begin() + 1, args.end()});
    }
    if (args.size() >= 7 && args[0] == "run") {
        return run_fixture({args.begin() + 1, args.end()});
    }
    std::cerr << "usage: fixture_runner checks <clang-tidy> <plugin> <check>...\n"
                 "       fixture_runner run <clang-tidy> <plugin> <java-root> <include-root> <data-dir> <work-dir> "
                 "[<compiler arg>...]\n";
    return 2;
}
