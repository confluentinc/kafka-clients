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
# Checks that an index lists every uploaded wheel, not yanked, with the
# checksum of the uploaded bytes, serves exactly those bytes for download, and
# lists no other file for the version. Used by the "Release (Python):
# post-publish checksum" blocks of semaphore.yml. Standard library only.
#
# Usage: check-published-wheels.py <index> <package> <version> <uploaded.sha256>
#   <index> is https://test.pypi.org or https://pypi.org, and
#   <uploaded.sha256> holds `<sha256>  <wheel>` for each uploaded wheel.

import hashlib
import json
import re
import sys
import time
import urllib.request


def fetch(url, headers=None):
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}), timeout=120) as response:
                return response.read()
        except OSError as error:
            if attempt == 3:
                raise
            print(f"{url}: {error}; retrying in 10s")
            time.sleep(10)


def main():
    if len(sys.argv) != 5:
        sys.exit(f"usage: {sys.argv[0]} <index> <package> <version> <uploaded.sha256>")
    index, package, version, uploaded_path = sys.argv[1:5]
    dist = re.sub(r"[-_.]+", "_", package)
    uploaded = {}
    for line in open(uploaded_path):
        if line.strip():
            checksum, name = line.split()
            uploaded[name] = checksum
    if not uploaded:
        sys.exit(f"{uploaded_path} lists no wheels")

    # Retries because newly uploaded files take some time to appear. The
    # listing and the downloads are served separately, so each is checked
    # against the upload.
    listing_url = f"{index}/simple/{package}/"
    for attempt in range(1, 21):
        try:
            page = json.loads(fetch(listing_url, {"Accept": "application/vnd.pypi.simple.v1+json"}))
            files = {f["filename"]: f for f in page["files"]}
        except (OSError, ValueError, KeyError) as error:
            print(f"could not read {listing_url}: {error}")
            files = {}
        missing = sorted(name for name in uploaded if name not in files)
        if not missing:
            break
        print(f"{len(missing)} of the {len(uploaded)} wheels are not listed on {index} yet (attempt {attempt}/20); retrying in 30s")
        time.sleep(30)
    else:
        sys.exit(f"{index} did not list {', '.join(missing)}")

    problems = []
    for name in sorted(files):
        if (name.startswith(f"{dist}-{version}-") or name == f"{dist}-{version}.tar.gz") and name not in uploaded:
            problems.append(f"{name} is on {index} but was not uploaded by this workflow")
    for name, checksum in sorted(uploaded.items()):
        file = files[name]
        downloaded = hashlib.sha256(fetch(file["url"])).hexdigest()
        print(f"{name}\n  uploaded:   {checksum}\n  listed:     {file['hashes'].get('sha256')}\n  downloaded: {downloaded}")
        if file.get("yanked"):
            problems.append(f"{name} is yanked on {index}")
        if file["hashes"].get("sha256") != checksum or downloaded != checksum:
            problems.append(f"{name} on {index} does not match the wheel that was uploaded")
    if problems:
        sys.exit("\n".join(problems))
    print(f"{index} lists and serves exactly the {len(uploaded)} uploaded wheels of {package} {version}")


if __name__ == "__main__":
    main()
