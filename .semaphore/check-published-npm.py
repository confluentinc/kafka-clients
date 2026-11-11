#!/usr/bin/env python3
#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Reads the npm registry for the Node.js client's packages: create-release-tag.yml and
# release-npm.sh refuse a version that is already there, and javascript-post-install.yml checks a
# published version. Standard library only.
#
# Usage, from the repository root:
#   check-published-npm.py versions <package>
#       Prints each version of <package> on the registry, one per line, and nothing if the
#       registry has no such package. Fails if the registry cannot be read.
#   check-published-npm.py check <version>
#       Checks that the main package and the two platform packages are on the registry at
#       <version>; that the dist-tag of each (`preview` for a pre-release, `latest` for a release,
#       as check-npm.py dist-tag gives it) points at it or at a later version (npm's semver
#       precedence), so the check can be rerun once a newer version is out; that, for a
#       pre-release, the main package's `latest`, and its `rc` and `alpha` when present, stay on
#       the earlier client (a major version below 4); that the main package's optionalDependencies name
#       exactly the two platform packages at <version>; then downloads the three tarballs,
#       checks each against the sha512 integrity the registry states, and runs check-npm.py's
#       package checks on them (--published).
#   check-published-npm.py self-test
#       Checks the version ordering and the dist-tag rules above on fixed cases, without the
#       network.
#
# The registry is https://registry.npmjs.org, or NPM_REGISTRY_URL. Nothing here holds a token:
# every read is anonymous.

import argparse
import base64
import hashlib
import http.client
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

MAIN_PACKAGE = "@confluentinc/kafka-javascript"
# The platform packages of the preview (javascript/npm/).
PLATFORMS = ("linux-x64-gnu", "darwin-arm64")
# A pre-release's dist-tag (check-npm.py dist-tag); a release's is `latest`.
DIST_TAG = "preview"
# The main package's dist-tags that the earlier, librdkafka-based client holds with its 0.x and
# 1.x releases: a version of this client (MIN_MAJOR or above, check-npm.py's) must never move them.
EARLIER_CLIENT_TAGS = ("latest", "rc", "alpha")
MIN_MAJOR = 4
# A semver version: major.minor.patch, an optional pre-release, an optional build (ignored).
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
                    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z.-]+)?")
# Answers worth retrying besides 5xx: a request timeout and throttling.
RETRIED_HTTP_CODES = (408, 429)
ATTEMPTS = 5


def registry():
    return os.environ.get("NPM_REGISTRY_URL", "https://registry.npmjs.org").rstrip("/")


def fetch(url, allow_missing=False):
    """Returns the body of `url`, or None for a 404 when `allow_missing`. Retries other errors."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            request = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(request, timeout=120) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code == 404 and allow_missing:
                return None
            if attempt == ATTEMPTS or (error.code < 500 and error.code not in RETRIED_HTTP_CODES):
                raise
            problem = error
        except (OSError, http.client.HTTPException) as error:
            # http.client.IncompleteRead, a body cut short, is not an OSError.
            if attempt == ATTEMPTS:
                raise
            problem = error
        print(f"{url}: {problem}; retrying in 10s", file=sys.stderr)
        time.sleep(10)


def packument(name):
    """The registry's document of package `name`, or None if it has no such package."""
    body = fetch(f"{registry()}/{urllib.parse.quote(name, safe='@')}", allow_missing=True)
    if body is None:
        return None
    document = json.loads(body)
    if not isinstance(document.get("versions"), dict):
        # A package whose every version was unpublished has no `versions`, only
        # `time.unpublished`.
        if not isinstance(document.get("time", {}).get("unpublished"), dict):
            raise ValueError(f"the registry's document of {name} has no versions")
        document["versions"] = {}
    return document


def versions(name):
    """Every version of `name` the registry has ever accepted. npm never takes a
    version again once it was published, even after it is unpublished: an
    unpublished version leaves `versions` but stays in `time` (or, when the
    whole package was unpublished, in `time.unpublished.versions`)."""
    document = packument(name)
    if document is None:
        return []
    seen = list(document["versions"])
    times = document.get("time", {})
    seen += [v for v in times if v not in ("created", "modified", "unpublished") and v not in seen]
    unpublished = times.get("unpublished", {})
    if isinstance(unpublished, dict):
        seen += [v for v in unpublished.get("versions", []) if v not in seen]
    return seen


def semver_key(version):
    """The sort key of `version` by semver precedence, as npm orders versions, or None if it is
    not a semver version. A release sorts after its pre-releases; pre-release identifiers compare
    numerically when numeric, lexically otherwise, a numeric one below an alphanumeric one, and a
    shorter list below a longer one it starts."""
    match = SEMVER.fullmatch(version) if isinstance(version, str) else None
    if match is None:
        return None
    major, minor, patch, pre = match.groups()
    release = (int(major), int(minor), int(patch))
    if pre is None:
        return release, (1,)
    identifiers = tuple((0, int(i), "") if i.isdigit() else (1, 0, i) for i in pre.split("."))
    return release, (0, identifiers)


def expected_dist_tag(version):
    """The dist-tag `version` goes out under, as check-npm.py dist-tag gives it for this client:
    `preview` for a pre-release, `latest` for a release."""
    return DIST_TAG if "-" in version else "latest"


def dist_tag_problems(name, tags, version):
    """What is wrong with the dist-tags `tags` of package `name` once `version` is published: its
    dist-tag must be at `version` or later, and, for a pre-release, the main package's
    earlier-client tags below MIN_MAJOR."""
    problems = []
    dist_tag = expected_dist_tag(version)
    tagged = tags.get(dist_tag)
    if semver_key(tagged) is None or semver_key(tagged) < semver_key(version):
        problems.append(f"the {dist_tag} dist-tag of {name} is {tagged!r}, not {version} or a later version")
    if name == MAIN_PACKAGE and dist_tag == DIST_TAG:
        for tag in EARLIER_CLIENT_TAGS:
            if tag not in tags:
                continue
            key = semver_key(tags[tag])
            if key is None or key[0][0] >= MIN_MAJOR:
                problems.append(f"the {tag} dist-tag of {name} is {tags[tag]!r}: it must stay on the earlier "
                                f"client, a version below {MIN_MAJOR}.0.0")
    return problems


def self_test():
    # Explicit checks, not `assert`: `python3 -O` would drop those.
    def expect(condition, what):
        if not condition:
            sys.exit(f"check-published-npm: self-test failed: {what}")

    ordered = ["1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha.beta", "1.0.0-beta", "1.0.0-beta.2", "1.0.0-beta.11",
               "1.0.0-rc.1", "1.0.0", "1.6.0", "4.4.0-alpha.9", "4.4.0-beta.1", "4.4.0-beta.2", "4.4.0-beta.10",
               "4.4.0-dev.1", "4.4.0-rc.1", "4.4.0", "4.4.1-beta.1", "4.10.0-beta.1", "10.0.0"]
    keys = [semver_key(v) for v in ordered]
    for (a, key_a), (b, key_b) in zip(zip(ordered, keys), zip(ordered[1:], keys[1:])):
        expect(key_a is not None and key_b is not None and key_a < key_b, f"{a} sorts before {b}")
    expect(semver_key("4.4.0-beta.1+build.5") == semver_key("4.4.0-beta.1"), "build metadata is ignored")
    for bad in (None, "", "4.4", "v4.4.0", "04.4.0", "4.4.0-"):
        expect(semver_key(bad) is None, f"{bad!r} is not a version")
    platform = f"{MAIN_PACKAGE}-{PLATFORMS[0]}"
    cases = [
        (MAIN_PACKAGE, {"latest": "1.6.0", "preview": "4.4.0-beta.1"}, "4.4.0-beta.1", []),
        # A rerun for an older version once a newer pre-release is out.
        (MAIN_PACKAGE, {"latest": "1.6.0", "preview": "4.4.0-beta.2", "rc": "1.0.0-rc.1"}, "4.4.0-beta.1", []),
        (MAIN_PACKAGE, {"latest": "1.6.0", "preview": "4.4.0-beta.1"}, "4.4.0-beta.2",
         [f"the preview dist-tag of {MAIN_PACKAGE} is '4.4.0-beta.1', not 4.4.0-beta.2 or a later version"]),
        (MAIN_PACKAGE, {"latest": "4.4.0-beta.1", "preview": "4.4.0-beta.1", "alpha": "4.4.0-alpha.1"},
         "4.4.0-beta.1",
         [f"the latest dist-tag of {MAIN_PACKAGE} is '4.4.0-beta.1': it must stay on the earlier client, a "
          "version below 4.0.0",
          f"the alpha dist-tag of {MAIN_PACKAGE} is '4.4.0-alpha.1': it must stay on the earlier client, a "
          "version below 4.0.0"]),
        (MAIN_PACKAGE, {"latest": "1.6.0"}, "4.4.0-beta.1",
         [f"the preview dist-tag of {MAIN_PACKAGE} is None, not 4.4.0-beta.1 or a later version"]),
        # The registry sets `latest` on a platform package's first publish: harmless.
        (platform, {"latest": "4.4.0-beta.1", "preview": "4.4.0-beta.1"}, "4.4.0-beta.1", []),
        # A release goes out under latest, which then leaves the earlier client.
        (MAIN_PACKAGE, {"latest": "4.5.0", "preview": "4.5.0-preview.2"}, "4.5.0", []),
        (MAIN_PACKAGE, {"latest": "1.6.0", "preview": "4.5.0"}, "4.5.0",
         [f"the latest dist-tag of {MAIN_PACKAGE} is '1.6.0', not 4.5.0 or a later version"]),
    ]
    for name, tags, version, expected in cases:
        actual = dist_tag_problems(name, tags, version)
        expect(actual == expected, f"{name} {tags} {version}: {actual}, not {expected}")
    print("check-published-npm: self-test passed")


def check(version):
    names = [MAIN_PACKAGE] + [f"{MAIN_PACKAGE}-{platform}" for platform in PLATFORMS]
    problems = []
    with tempfile.TemporaryDirectory() as directory:
        for name in names:
            document = packument(name)
            if document is None or version not in document["versions"]:
                problems.append(f"{name} {version} is not on {registry()}")
                continue
            problems += dist_tag_problems(name, document.get("dist-tags", {}), version)
            manifest = document["versions"][version]
            if name == MAIN_PACKAGE:
                optional = {f"{MAIN_PACKAGE}-{platform}": version for platform in PLATFORMS}
                if manifest.get("optionalDependencies") != optional:
                    problems.append(f"{name} {version} has the optionalDependencies "
                                    f"{manifest.get('optionalDependencies')!r}, not {optional}")
            dist = manifest.get("dist", {})
            integrity = dist.get("integrity", "")
            tarball_url = dist.get("tarball", "")
            # The local name check-npm.py expects is npm pack's, `confluentinc-<name>-<v>.tgz`; the
            # registry stores a scoped package's tarball as `<name>/-/<name without scope>-<v>.tgz`.
            file = f"{name[1:].replace('/', '-')}-{version}.tgz"
            served = f"/{name}/-/{name.split('/', 1)[1]}-{version}.tgz"
            if not integrity.startswith("sha512-") or not tarball_url.endswith(served):
                problems.append(f"{name} {version} has the tarball {tarball_url!r} with the integrity {integrity!r}")
                continue
            data = fetch(tarball_url)
            actual = "sha512-" + base64.b64encode(hashlib.sha512(data).digest()).decode()
            if actual != integrity:
                problems.append(f"the tarball of {name} {version} has the integrity {actual}, not {integrity}, the "
                                "registry's")
                continue
            with open(os.path.join(directory, file), "wb") as f:
                f.write(data)
            print(f"{hashlib.sha256(data).hexdigest()}  {file}  ({integrity})")
        if problems:
            sys.exit("check-published-npm: " + "\n  ".join(problems))
        result = subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "check-npm.py"),
                                 "package", directory, "--version", version, "--published"])
        if result.returncode != 0:
            sys.exit(f"check-published-npm: the tarballs of {version} on {registry()} do not pass check-npm.py")
    print(f"{', '.join(names)} {version} are on {registry()}, {expected_dist_tag(version)} at it or later, with the tarballs "
          "check-npm.py accepts")


def main():
    parser = argparse.ArgumentParser(description="Reads the npm registry for the Node.js client's packages.")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("versions")
    listing.add_argument("package")
    checking = commands.add_parser("check")
    checking.add_argument("version")
    commands.add_parser("self-test")
    args = parser.parse_args()
    if args.command == "versions":
        for version in versions(args.package):
            print(version)
    elif args.command == "self-test":
        self_test()
    else:
        check(args.version)


if __name__ == "__main__":
    main()
