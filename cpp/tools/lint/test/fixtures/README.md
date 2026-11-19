# Linter fixtures

Each directory is one fixture: a Java source tree (`java/`), headers (`include/`) and,
optionally, data files (`data/`). `fixture_runner run` includes every header under
`include/confluent/kafka` in one translation unit, runs the `confluent-kafka-*` checks through
the real `clang-tidy` and the plugin, and requires each diagnostic to match one expectation:

- `// EXPECT: <check> <text>` a diagnostic of `<check>` on this line, its message containing `<text>`;
- `// EXPECT@<line>: <check> <text>` the same on `<line>` of this file.

Line numbers matter, so the headers are not formatted (`.clang-format` disables it, and the
Makefile's `format` targets skip this directory).
