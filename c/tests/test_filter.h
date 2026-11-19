// Copyright 2025 Confluent Inc.
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

// Runs only the tests the environment variable C_TEST_FILTER names, so that a
// test selection (the root Makefile's `select-tests`) can run single tests of a
// test program. Unset or empty, every test runs.
//
// C_TEST_FILTER is a comma-separated list of entries. `<program>` runs every
// test of that program; `<program>:<test>` runs the test function of that name.
// <program> is the program's ctest name: its source file's name without `test_`
// and `.c` (`mock_producer` for test_mock_producer.c). A program the list does
// not name runs none of its tests, and an entry naming a test the program does
// not run fails the program, so a misspelt name cannot pass as a selection of
// nothing:
//
//   C_TEST_FILTER=mock_producer:test_destroy_null,kafka_future ctest -R '^(mock_producer|kafka_future)$'
//
// Include it after unity.h: it replaces Unity's RUN_TEST and UNITY_END. Unity's
// own name filter (UNITY_USE_COMMAND_LINE_ARGS) is not used: only the runners
// Unity generates apply it, it matches substrings, and its exact mode cannot be
// combined with a file name, since UNITY_BEGIN() records the absolute path.
#ifndef TEST_FILTER_H
#define TEST_FILTER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "unity.h"

// The test functions RUN_TEST was given, selected or not.
static const char **test_filter_seen = NULL;
static size_t test_filter_seen_len = 0;

// The filter, or NULL when every test runs.
static inline const char *test_filter_value(void) {
    const char *filter = getenv("C_TEST_FILTER");
    return (filter != NULL && filter[0] != '\0') ? filter : NULL;
}

// This program's ctest name, from the file UNITY_BEGIN() records: `*name` and
// the returned length.
static inline size_t test_filter_program(const char **name) {
    const char *base = Unity.TestFile != NULL ? Unity.TestFile : "";
    for (const char *c = base; *c != '\0'; c++) {
        if (*c == '/' || *c == '\\') {
            base = c + 1;
        }
    }
    if (strncmp(base, "test_", 5) == 0) {
        base += 5;
    }
    size_t len = strlen(base);
    if (len > 2 && strcmp(base + len - 2, ".c") == 0) {
        len -= 2;
    }
    *name = base;
    return len;
}

// The test an entry of the filter names in this program: `*test` and its
// length, NULL when the entry is about another program, and "" with length 0
// when it names the whole program.
static inline const char *test_filter_entry(const char *entry, size_t len, size_t *test_len) {
    const char *program;
    size_t program_len = test_filter_program(&program);
    if (len < program_len || strncmp(entry, program, program_len) != 0) {
        return NULL;
    }
    if (len == program_len) {
        *test_len = 0;
        return "";
    }
    if (entry[program_len] != ':') {
        return NULL;
    }
    *test_len = len - program_len - 1;
    return entry + program_len + 1;
}

// Calls `visit` on each entry of the filter about this program; stops and
// returns 1 at the first call returning 1.
static inline int test_filter_each(const char *filter, int (*visit)(const char *test, size_t len, const void *arg),
                                   const void *arg) {
    const char *entry = filter;
    for (;;) {
        const char *end = strchr(entry, ',');
        size_t len = end != NULL ? (size_t)(end - entry) : strlen(entry);
        size_t test_len;
        const char *test = test_filter_entry(entry, len, &test_len);
        if (test != NULL && visit(test, test_len, arg)) {
            return 1;
        }
        if (end == NULL) {
            return 0;
        }
        entry = end + 1;
    }
}

static inline int test_filter_matches(const char *test, size_t len, const void *name) {
    return len == 0 || (strlen((const char *)name) == len && strncmp(test, (const char *)name, len) == 0);
}

static inline void test_filter_run(UnityTestFunction func, const char *name, int line) {
    const char **seen = (const char **)realloc((void *)test_filter_seen, (test_filter_seen_len + 1) * sizeof *seen);
    if (seen != NULL) {
        test_filter_seen = seen;
        test_filter_seen[test_filter_seen_len++] = name;
    }
    const char *filter = test_filter_value();
    if (filter == NULL || test_filter_each(filter, test_filter_matches, name)) {
        UnityDefaultTestRun(func, name, line);
    }
}

// Reports an entry naming a test RUN_TEST was never given.
static inline int test_filter_check(const char *test, size_t len, const void *unknown) {
    if (len == 0) {
        return 0;
    }
    for (size_t i = 0; i < test_filter_seen_len; i++) {
        if (strlen(test_filter_seen[i]) == len && strncmp(test, test_filter_seen[i], len) == 0) {
            return 0;
        }
    }
    const char *program;
    size_t program_len = test_filter_program(&program);
    fprintf(stderr, "C_TEST_FILTER: %.*s has no test %.*s\n", (int)program_len, program, (int)len, test);
    *(int *)unknown = 1;
    return 0;
}

static inline int test_filter_end(void) {
    int unknown = 0;
    const char *filter = test_filter_value();
    if (filter != NULL) {
        test_filter_each(filter, test_filter_check, &unknown);
    }
    free((void *)test_filter_seen);
    test_filter_seen = NULL;
    test_filter_seen_len = 0;
    int failures = UnityEnd();
    return failures != 0 ? failures : unknown;
}

#undef RUN_TEST
#define RUN_TEST(func) test_filter_run(func, #func, __LINE__)
#undef UNITY_END
#define UNITY_END() test_filter_end()

#endif /* TEST_FILTER_H */
