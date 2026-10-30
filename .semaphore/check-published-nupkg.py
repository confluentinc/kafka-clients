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
# Reads what a NuGet gallery serves, for .semaphore/publish-nupkg.sh and the "Release (.NET):
# post-publish check" blocks of publish-nuget-test.yml and publish-nuget.yml. Standard library
# only. A gallery adds its repository signature, .signature.p7s, to every package it accepts, so
# the package it serves is not byte-identical to the package uploaded: it is compared entry by
# entry instead, with every other entry identical.
#
# Usage, from the repository root:
#   check-published-nupkg.py versions <service-index> <package-id>
#       Prints each version of the package on the gallery, one per line, in the normalized
#       lowercase form of its flat container, and nothing if the gallery has no such package.
#       Fails if the gallery cannot be read.
#   check-published-nupkg.py check <service-index> <package-id> <version> <uploaded-nupkg>
#                            [--attempts <n>]
#       Waits until the gallery serves the version (every 30 s, <n> times, by default 40), then
#       checks that the package it serves is the uploaded package plus the gallery's signature,
#       which names the gallery's service index, and that it passes check-nupkg.py's checks as a
#       published package. A gallery that does not serve the version in time is reported as
#       such: the version is not defective, and the check can run again later.
#
# <service-index> is the gallery's NuGet V3 service index, https://api.nuget.org/v3/index.json or
# https://apiint.nugettest.org/v3/index.json.

import argparse
import http.client
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile

SIGNATURE = ".signature.p7s"


# Answers worth retrying besides 5xx: a request timeout and throttling.
RETRIED_HTTP_CODES = (408, 429)


def fetch(url, allow_missing=False):
    """Returns the body of `url`, or None for a 404 when `allow_missing`. Retries other errors."""
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url), timeout=120) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code == 404 and allow_missing:
                return None
            if attempt == 3 or (error.code < 500 and error.code not in RETRIED_HTTP_CODES):
                raise
            problem = error
        except (OSError, http.client.HTTPException) as error:
            # http.client.IncompleteRead, a body cut short, is not an OSError.
            if attempt == 3:
                raise
            problem = error
        print(f"{url}: {problem}; retrying in 10s")
        time.sleep(10)


def flat_container(service_index):
    """The PackageBaseAddress/3.0.0 resource of the service index, with a trailing slash."""
    index = json.loads(fetch(service_index))
    for resource in index.get("resources", []):
        if resource.get("@type") == "PackageBaseAddress/3.0.0":
            base = resource["@id"]
            return base if base.endswith("/") else base + "/"
    sys.exit(f"{service_index} has no PackageBaseAddress/3.0.0 resource")


def versions(base, package_id):
    body = fetch(f"{base}{package_id.lower()}/index.json", allow_missing=True)
    if body is None:
        return []
    listed = json.loads(body).get("versions")
    if not isinstance(listed, list):
        raise ValueError(f"{base}{package_id.lower()}/index.json has no version list")
    return listed


def entries(data):
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        result = {}
        for info in archive.infolist():
            if info.filename in result:
                sys.exit(f"the package has two entries named {info.filename}")
            result[info.filename] = archive.read(info)
        return result


def check(service_index, package_id, version, uploaded_path, attempts):
    wanted = version.lower()
    base = None
    served = None
    for attempt in range(1, attempts + 1):
        try:
            if base is None:
                base = flat_container(service_index)
            if wanted in versions(base, package_id):
                url = f"{base}{package_id.lower()}/{wanted}/{package_id.lower()}.{wanted}.nupkg"
                # The package file can lag the version list.
                served = fetch(url, allow_missing=True)
                if served is not None:
                    break
                print(f"{service_index} lists {package_id} {version} but does not serve its package yet")
            else:
                print(f"{package_id} {version} is not served by {service_index} yet")
        except (OSError, ValueError, http.client.HTTPException) as error:
            print(f"could not read {package_id} {version} from {service_index}: {error}")
        if attempt < attempts:
            print(f"attempt {attempt}/{attempts}; retrying in 30s")
            time.sleep(30)
    if served is None:
        sys.exit(f"{service_index} does not serve {package_id} {version} after {attempts} attempts. This is not a "
                 f"defect of the package: the gallery may still be validating it, which on the NuGet test gallery "
                 f"has taken hours and even days. Run this check again later.")

    with open(uploaded_path, "rb") as f:
        uploaded = f.read()
    served_entries = entries(served)
    uploaded_entries = entries(uploaded)
    problems = []
    if SIGNATURE not in served_entries:
        problems.append(f"{url} has no repository signature ({SIGNATURE})")
    elif service_index.encode("ascii") not in served_entries[SIGNATURE]:
        # A repository signature carries its gallery's service index (nuget-v3-service-index-url).
        problems.append(f"the repository signature of {url} does not name {service_index}")
    if SIGNATURE in uploaded_entries:
        problems.append(f"the uploaded package already holds a {SIGNATURE}")
    served_entries.pop(SIGNATURE, None)
    for name in sorted(set(served_entries) | set(uploaded_entries)):
        if name not in uploaded_entries:
            problems.append(f"{url} has the entry {name}, which was not uploaded")
        elif name not in served_entries:
            problems.append(f"{url} lacks the uploaded entry {name}")
        elif served_entries[name] != uploaded_entries[name]:
            problems.append(f"{url} has a different {name} than the uploaded package")
    if problems:
        sys.exit("\n".join(problems))

    # The structural checks of the published package, from its own copy.
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, f"{package_id}.{version}.nupkg")
        with open(path, "wb") as f:
            f.write(served)
        rids = [name.split("/")[1] for name in uploaded_entries if name.startswith("runtimes/") and name.count("/") == 3]
        subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "check-nupkg.py"), "package", path,
             "--version", version, "--rids", " ".join(sorted(rids)), "--published"],
            check=True)
    print(f"{service_index} serves {package_id} {version}: the {len(uploaded_entries)} entries of the uploaded "
          f"package, unchanged, and its repository signature")


def main():
    parser = argparse.ArgumentParser(description="Reads what a NuGet gallery serves.")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("versions")
    listing.add_argument("service_index")
    listing.add_argument("package_id")
    checking = commands.add_parser("check")
    checking.add_argument("service_index")
    checking.add_argument("package_id")
    checking.add_argument("version")
    checking.add_argument("uploaded")
    checking.add_argument("--attempts", type=int, default=40)
    args = parser.parse_args()
    if args.command == "versions":
        for version in versions(flat_container(args.service_index), args.package_id):
            print(version)
    else:
        check(args.service_index, args.package_id, args.version, args.uploaded, args.attempts)


if __name__ == "__main__":
    main()
