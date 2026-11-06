Publishing a release of @confluentinc/kafka-javascript
=====================================================

This bundle is a release of the Node.js client, recorded by the tag build of
javascript/v<version>, in its "Release (JavaScript): record" job. It holds:

  confluentinc-kafka-javascript-linux-x64-gnu-<version>.tgz    platform package
  confluentinc-kafka-javascript-linux-arm64-gnu-<version>.tgz  platform package
  confluentinc-kafka-javascript-darwin-arm64-<version>.tgz     platform package
  confluentinc-kafka-javascript-<version>.tgz                  main package, published last
  SHA256SUMS                                                   the SHA-256 of the tarballs
  publish.sh                                                   publishes them
  README.txt                                                   this file

Every tarball is byte for byte the one the tag build installed and tested on
Linux x64, Linux arm64 and macOS arm64. Nothing in the pipeline publishes, and
no machine of it holds an npm token: publishing is these steps, by hand.

1. Download the bundle

   In the tag build's workflow in Semaphore, open Artifacts and download
   npm/release/confluent-kafka-javascript-<version>-release.zip.

2. Prepare your machine (once)

   You need Node.js 20 or newer, which includes npm 10, and unzip. No
   compiler, Rust or Python is needed. Log in with an npm account that can
   publish to the @confluentinc scope, with two-factor authentication on:

     npm login

3. Verify and publish

     unzip confluent-kafka-javascript-<version>-release.zip -d release && cd release
     shasum -a 256 -c SHA256SUMS
     ./publish.sh

   Before publishing, compare the SHA-256 of the zip and of each tarball with
   the ones the record job printed at the end of its log, which no later job
   can change. The tarballs must also match SHA256SUMS; publish.sh checks that
   too, and that npm is logged in.

   publish.sh runs `npm publish --dry-run` on each tarball still to publish,
   then publishes them in this order, asking for a fresh one-time password
   before each: the three platform packages, then the main package, so that
   the main package's optional dependencies exist when it appears. Each is
   published with --access public --tag preview. A package already published
   with the same tarball is skipped, so an interrupted run can be repeated; a
   package published with another tarball stops it, since npm never lets a
   version be published twice: release a new version instead.

   It publishes only a pre-release version (-alpha.N, -beta.N, -dev.N or
   -rc.N), under the preview dist-tag. The package name is shared with the
   earlier librdkafka-based client, whose 1.x versions hold the latest, rc
   and alpha dist-tags; a stable version would move latest, which has to be
   agreed with the team that owns the package first.

   What publish.sh runs, if you prefer to type the commands yourself:

     npm publish confluentinc-kafka-javascript-linux-x64-gnu-<version>.tgz --access public --tag preview \
         --registry https://registry.npmjs.org/ --@confluentinc:registry=https://registry.npmjs.org/ --otp=<code>
     npm publish confluentinc-kafka-javascript-linux-arm64-gnu-<version>.tgz --access public --tag preview \
         --registry https://registry.npmjs.org/ --@confluentinc:registry=https://registry.npmjs.org/ --otp=<code>
     npm publish confluentinc-kafka-javascript-darwin-arm64-<version>.tgz --access public --tag preview \
         --registry https://registry.npmjs.org/ --@confluentinc:registry=https://registry.npmjs.org/ --otp=<code>
     npm publish confluentinc-kafka-javascript-<version>.tgz --access public --tag preview \
         --registry https://registry.npmjs.org/ --@confluentinc:registry=https://registry.npmjs.org/ --otp=<code>

   The two registry options matter if your ~/.npmrc maps @confluentinc to
   another registry: npm would otherwise publish there. Without --tag preview,
   npm moves the main package's latest dist-tag to this version, and every
   plain `npm install` of the earlier client's users gets it. Typed by hand or
   not, run ./publish.sh afterwards: it skips what is published and checks
   the dist-tags, preview at <version> on each package and the main package's
   latest (rc and alpha too, when set) still on the earlier client.

4. Run the post-install task

   In Semaphore, run the javascript-post-install task on master with
   JAVASCRIPT_VERSION=<version>. It installs the published package from
   npmjs.com, as a user does, on every distribution and Node.js version of
   its matrix, and checks that the registry has the four packages at
   <version> under preview, that their tarballs match the integrity the
   registry states, and that they pass the package checks. The task runs
   from master, not from the tag build, so it cannot compare the registry
   with the recorded bundle; publish.sh does that before it finishes: it
   reads each package back from npmjs.com and checks that the registry
   serves exactly the tarball of this bundle. A quick check by hand:

     npm view @confluentinc/kafka-javascript dist-tags
     npm install @confluentinc/kafka-javascript@preview
