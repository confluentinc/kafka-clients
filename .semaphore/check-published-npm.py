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
#       Checks that the main package and the three platform packages are on the registry at
#       <version>; that the `preview` dist-tag of each points at it; that the main package's
#       optionalDependencies name exactly the three platform packages at <version>; then
#       downloads the four tarballs, checks each against the sha512 integrity the registry
#       states, and runs check-npm.py's package checks on them (--published).
#
# The registry is https://registry.npmjs.org, or NPM_REGISTRY_URL. Nothing here holds a token:
# every read is anonymous.

import argparse
import base64
import hashlib
import http.client
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

MAIN_PACKAGE = "@confluentinc/kafka-javascript"
PLATFORMS = ("linux-x64-gnu", "linux-arm64-gnu", "darwin-arm64")
DIST_TAG = "preview"
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


def check(version):
    names = [MAIN_PACKAGE] + [f"{MAIN_PACKAGE}-{platform}" for platform in PLATFORMS]
    problems = []
    with tempfile.TemporaryDirectory() as directory:
        for name in names:
            document = packument(name)
            if document is None or version not in document["versions"]:
                problems.append(f"{name} {version} is not on {registry()}")
                continue
            tagged = document.get("dist-tags", {}).get(DIST_TAG)
            if tagged != version:
                problems.append(f"the {DIST_TAG} dist-tag of {name} is {tagged!r}, not {version}")
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
    print(f"{', '.join(names)} {version} are on {registry()} under {DIST_TAG}, with the tarballs check-npm.py accepts")


def main():
    parser = argparse.ArgumentParser(description="Reads the npm registry for the Node.js client's packages.")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("versions")
    listing.add_argument("package")
    checking = commands.add_parser("check")
    checking.add_argument("version")
    args = parser.parse_args()
    if args.command == "versions":
        for version in versions(args.package):
            print(version)
    else:
        check(args.version)


if __name__ == "__main__":
    main()
