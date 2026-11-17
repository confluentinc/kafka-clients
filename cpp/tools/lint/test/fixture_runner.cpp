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
//   fixture_runner mutate <clang-tidy> <plugin> <java-root> <include-root> <data-dir> <work-dir> <mutation> [<compiler
//   arg>...]
//       copies <include-root> to <work-dir>, applies the one edit <mutation>
//       describes, and requires the checks to report what it expects:
//         file: <header, relative to the include root>
//         replace: <text>      (once; \n stands for a newline)
//         with: <text>
//         expect: <check> <text>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
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

struct Run {
    std::vector<Diagnostic> found;
    bool broken = false;
    std::string output;
    std::size_t headers = 0;
};

/// Runs the checks on every header under `include_root`.
Run run_checks_on(const std::string& clang_tidy, const std::string& plugin, const std::string& java_root,
                  const std::string& include_root, const std::string& data_dir, const fs::path& work,
                  const std::vector<std::string>& compiler_args, std::vector<Expectation>* expected);

int run_fixture(const std::vector<std::string>& args) {
    std::vector<Expectation> expected;
    const std::vector<std::string> compiler_args(args.begin() + 6, args.end());
    const auto run =
        run_checks_on(args[0], args[1], args[2], canonical(args[3]), args[4], args[5], compiler_args, &expected);
    int failures = run.broken ? 1 : 0;
    for (const auto& d : run.found) {
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
        std::cerr << "--- clang-tidy output ---\n" << run.output;
        return EXIT_FAILURE;
    }
    std::cout << run.headers << " headers, " << run.found.size() << " diagnostics, all expected\n";
    return EXIT_SUCCESS;
}

std::string unescape(std::string text) {
    std::string out;
    for (std::size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '\\' && i + 1 < text.size() && text[i + 1] == 'n') {
            out += '\n';
            ++i;
        } else {
            out += text[i];
        }
    }
    return out;
}

int run_mutation(const std::vector<std::string>& args) {
    const fs::path work = args[5];
    const fs::path mutation = args[6];
    std::map<std::string, std::string> fields;
    {
        std::ifstream in(mutation);
        std::string line;
        while (std::getline(in, line)) {
            const auto colon = line.find(": ");
            if (line.empty() || line[0] == '#' || colon == std::string::npos) {
                continue;
            }
            fields[line.substr(0, colon)] = line.substr(colon + 2);
        }
    }
    for (const char* key : {"file", "replace", "with", "expect"}) {
        if (fields.count(key) == 0) {
            std::cerr << mutation << ": no `" << key << ":` line\n";
            return EXIT_FAILURE;
        }
    }
    const auto include = work / "include";
    fs::remove_all(include);
    fs::create_directories(include);
    fs::copy(args[3], include, fs::copy_options::recursive);
    const auto target = include / fields["file"];
    std::string text;
    {
        std::ifstream in(target, std::ios::binary);
        std::ostringstream all;
        all << in.rdbuf();
        text = all.str();
    }
    const auto replace = unescape(fields["replace"]);
    const auto at = text.find(replace);
    if (at == std::string::npos) {
        std::cerr << mutation << ": `" << replace << "` is not in " << fields["file"] << '\n';
        return EXIT_FAILURE;
    }
    text.replace(at, replace.size(), unescape(fields["with"]));
    std::ofstream(target, std::ios::binary) << text;

    const auto space = fields["expect"].find(' ');
    const auto check = fields["expect"].substr(0, space);
    const auto wanted = space == std::string::npos ? std::string() : fields["expect"].substr(space + 1);
    const std::vector<std::string> compiler_args(args.begin() + 7, args.end());
    const auto run =
        run_checks_on(args[0], args[1], args[2], canonical(include.string()), args[4], work, compiler_args, nullptr);
    const bool caught = std::any_of(run.found.begin(), run.found.end(), [&](const Diagnostic& d) {
        return d.check == check && d.message.find(wanted) != std::string::npos;
    });
    if (run.broken || !caught) {
        std::cerr << mutation.filename() << (run.broken ? ": the mutated headers do not compile\n" : ": not caught\n")
                  << "--- clang-tidy output ---\n"
                  << run.output;
        return EXIT_FAILURE;
    }
    std::cout << mutation.filename().string() << ": caught by " << check << '\n';
    return EXIT_SUCCESS;
}

Run run_checks_on(const std::string& clang_tidy, const std::string& plugin, const std::string& java_root,
                  const std::string& include_root, const std::string& data_dir, const fs::path& work,
                  const std::vector<std::string>& compiler_args, std::vector<Expectation>* expected) {
    Run run;
    fs::create_directories(work);
    const auto headers = headers_under(fs::path(include_root) / "confluent" / "kafka");
    run.headers = headers.size();
    {
        std::ofstream probe(work / "probe.cpp");
        for (const auto& h : headers) {
            probe << "#include <" << fs::relative(h, include_root).generic_string() << ">\n";
            if (expected != nullptr) {
                const auto found = expectations_in(h);
                expected->insert(expected->end(), found.begin(), found.end());
            }
        }
    }
    // Diagnostics about the translation unit as a whole (the surface check) are
    // placed at line 1 of the probe: <include-root>/../probe-expectations.txt
    // lists them, one `<check> <text>` per line.
    if (expected != nullptr) {
        std::ifstream in(fs::path(include_root).parent_path() / "probe-expectations.txt");
        std::string line;
        while (std::getline(in, line)) {
            const auto space = line.find(' ');
            if (line.empty() || line[0] == '#' || space == std::string::npos) {
                continue;
            }
            expected->push_back(Expectation{canonical((work / "probe.cpp").string()), 1, line.substr(0, space),
                                            line.substr(space + 1)});
        }
    }
    // A fixture may name the checks it exercises in <include-root>/../checks.txt,
    // one per line; its headers are wrong on purpose for those, not for others.
    std::string checks = "confluent-kafka-*";
    {
        std::ifstream in(fs::path(include_root).parent_path() / "checks.txt");
        std::string line;
        std::string listed;
        while (std::getline(in, line)) {
            if (!line.empty() && line[0] != '#') {
                listed += (listed.empty() ? "" : ",") + line;
            }
        }
        if (!listed.empty()) {
            checks = listed;
        }
    }
    // A fixture's own contract-change ledger: <include-root>/../ledger.md.
    std::string ledger_option;
    if (const auto ledger = fs::path(include_root).parent_path() / "ledger.md"; fs::exists(ledger)) {
        ledger_option = ", ConfluentKafkaLedger: '" + ledger.string() + "'";
    }
    const std::string config = "{Checks: '-*," + checks + "', CheckOptions: {ConfluentKafkaJavaRoot: '" + java_root +
                               "', ConfluentKafkaDataDir: '" + data_dir + "', ConfluentKafkaIncludeRoot: '" +
                               include_root + "'" + ledger_option + "}}";
    std::string command = quote(clang_tidy) + " -load " + quote(plugin) + " --quiet -header-filter='.*' -config=\"" +
                          config + "\" " + quote((work / "probe.cpp").string()) + " -- -std=c++20 -I" +
                          quote(include_root);
    for (const auto& a : compiler_args) {
        command += " " + quote(a);
    }
    int status = 0;
    run.output = run_command(command, status);
    static const std::regex diagnostic(R"(^(.+):(\d+):\d+: (warning|error): (.*) \[([^\]]+)\]$)");
    static const std::regex compiler_error(R"(^(.+):(\d+):\d+: (fatal )?error: )");
    std::istringstream lines(run.output);
    std::string line;
    while (std::getline(lines, line)) {
        std::smatch m;
        if (std::regex_match(line, m, diagnostic) && m[5].str().rfind("confluent-kafka-", 0) == 0) {
            run.found.push_back(Diagnostic{canonical(m[1]), std::stoi(m[2]), m[5], m[4]});
        } else if (std::regex_search(line, m, compiler_error)) {
            std::cerr << "does not compile: " << line << '\n';
            run.broken = true;
        }
    }
    return run;
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
    if (args.size() >= 8 && args[0] == "mutate") {
        return run_mutation({args.begin() + 1, args.end()});
    }
    std::cerr << "usage: fixture_runner checks <clang-tidy> <plugin> <check>...\n"
                 "       fixture_runner run <clang-tidy> <plugin> <java-root> <include-root> <data-dir> <work-dir> "
                 "[<compiler arg>...]\n"
                 "       fixture_runner mutate <clang-tidy> <plugin> <java-root> <include-root> <data-dir> <work-dir> "
                 "<mutation> [<compiler arg>...]\n";
    return 2;
}
