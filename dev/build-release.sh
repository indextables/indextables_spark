#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Build per-Spark-version release jars locally, mirroring datafusion-comet's
# dev/release/build-release-comet.sh profile loop. This is the local
# equivalent of what .github/workflows/release.yml does per matrix leg
# (minus GPG signing and Maven Central publishing).
#
# Usage:
#   dev/build-release.sh <base-version> [output-dir]
#
# Example:
#   dev/build-release.sh 0.6.0
#   -> dist/indextables_spark-0.6.0_spark_3.5.8-<platform>-shaded.jar
#      dist/indextables_spark-0.6.0_spark_4.0.3-<platform>-shaded.jar
#      dist/indextables_spark-0.6.0_spark_4.1.2-<platform>-shaded.jar
#
# Notes:
#   - The Spark patch version in each artifact name comes from the pom's
#     spark.version per profile (single source of truth), not from this script.
#   - tantivy4java for the current platform must already be in ~/.m2
#     (run ./scripts/setup.sh once). The platform classifier is auto-detected,
#     so on an Apple Silicon Mac this produces darwin-aarch64 jars, on Linux
#     x86_64 it produces linux-x86_64 jars, etc.
#   - `mvn clean` per leg is mandatory: ANTLR-generated sources are not
#     regenerated on a profile switch and fail against the other ANTLR runtime.
#   - JDK 17 works for all three legs (Spark 4.x requires it).

set -euo pipefail

BASE_VERSION=${1:?Usage: dev/build-release.sh <base-version> [output-dir]  (e.g. dev/build-release.sh 0.6.0)}
OUTPUT_DIR=${2:-dist}

PROFILES=(spark-3.5 spark-4.0 spark-4.1)

cd "$(dirname "$0")/.."

command -v mvn >/dev/null || { echo "[ERROR] mvn not found on PATH" >&2; exit 1; }

ORIGINAL_VERSION=$(mvn -q help:evaluate -Dexpression=project.version -DforceStdout)
[[ -n "$ORIGINAL_VERSION" ]] || { echo "[ERROR] could not read project.version from pom.xml" >&2; exit 1; }

# Restore the pom's original version on any exit (success, failure, ctrl-C).
# versions:set (not `git checkout pom.xml`) so unrelated uncommitted pom edits survive.
restore_version() {
    mvn -q versions:set -DnewVersion="$ORIGINAL_VERSION" -DgenerateBackupPoms=false >/dev/null 2>&1 || \
        echo "[WARN] failed to restore pom version to $ORIGINAL_VERSION — check pom.xml" >&2
}
trap restore_version EXIT

mkdir -p "$OUTPUT_DIR"

for profile in "${PROFILES[@]}"; do
    spark_ver=$(mvn -q help:evaluate -Dexpression=spark.version -DforceStdout -P"$profile")
    version="${BASE_VERSION}_spark_${spark_ver}"

    echo ""
    echo "=================================================================="
    echo "==> Building -P$profile as $version"
    echo "=================================================================="
    mvn -q versions:set -DnewVersion="$version" -DgenerateBackupPoms=false
    mvn clean package -P"$profile" -DskipTests

    shaded=(target/indextables_spark-"$version"-*-shaded.jar)
    [[ -f "${shaded[0]}" ]] || { echo "[ERROR] shaded jar not found for $profile" >&2; exit 1; }
    cp "${shaded[@]}" "$OUTPUT_DIR"/
done

echo ""
echo "==> Release jars in $OUTPUT_DIR/:"
ls -l "$OUTPUT_DIR"/indextables_spark-"$BASE_VERSION"_spark_*-shaded.jar
