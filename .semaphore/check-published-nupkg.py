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
# Lists the versions of a package on a NuGet gallery, for create-release-tag.yml, which refuses to
# tag a .NET version that is already on nuget.org. Standard library only.
#
# Usage, from the repository root:
#   check-published-nupkg.py versions <service-index> <package-id>
#       Prints each version of the package on the gallery, one per line, in the normalized
#       lowercase form of its flat container, and nothing if the gallery has no such package.
#       Fails if the gallery cannot be read.
#
# <service-index> is the gallery's NuGet V3 service index, https://api.nuget.org/v3/index.json.

import argparse
import http.client
import json
import sys
import time
import urllib.error
import urllib.request


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
        print(f"{url}: {problem}; retrying in 10s", file=sys.stderr)
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


def main():
    parser = argparse.ArgumentParser(description="Lists the versions of a package on a NuGet gallery.")
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("versions")
    listing.add_argument("service_index")
    listing.add_argument("package_id")
    args = parser.parse_args()
    for version in versions(flat_container(args.service_index), args.package_id):
        print(version)


if __name__ == "__main__":
    main()
