#!/usr/bin/env bash
# Build everything that is published for one Spark profile, unsigned, and
# record digests. Runs the project's Maven build, so it runs only in a job
# that has no credentials.
#
# Environment:
#   SRC           checkout of the commit being released
#   PROFILE       spark-3.5 | spark-4.0 | spark-4.1
#   BASE_VERSION  the tag without its "v" (0.6.0)
#   EXPECTED_VERSIONS  the versions plan.sh announced, one per profile
#   NATIVE_DIR    the native job's artifact (build-native.sh output)
#   OUT           output directory; becomes the job's artifact:
#                   files/           the six files of this version, under the
#                                    names they are published with
#                   build-info.txt   profile, versions, tool versions
#                   SHA256SUMS       digests of everything above
#
# The published version is <BASE_VERSION>_spark_<spark.version>, where
# spark.version is what the profile sets in pom.xml at the released commit,
# so the suffix always names the Spark version the jar was compiled against.
# It must be the version plan.sh worked out from the same pom.xml and showed
# in the Plan summary; if Maven disagrees, the build stops.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

: "${SRC:?}" "${PROFILE:?}" "${BASE_VERSION:?}" "${EXPECTED_VERSIONS:?}" "${NATIVE_DIR:?}" "${OUT:?}"

# Pinned here because they are invoked by coordinate, not through pom.xml.
VERSIONS_PLUGIN=org.codehaus.mojo:versions-maven-plugin:2.22.0
HELP_PLUGIN=org.apache.maven.plugins:maven-help-plugin:3.5.1

need mvn java unzip
spark_line "$PROFILE" > /dev/null
[[ "$BASE_VERSION" =~ $BASE_RE ]] || die "'$BASE_VERSION' is not a base version"
[ -f "$SRC/pom.xml" ] || die "no pom.xml in $SRC"

# --- the native jar built by this run, and nothing else -----------------------
verify_sums "$NATIVE_DIR"
t4j_version="$(pom_tantivy4java_version "$SRC/pom.xml")"
[ "$(kv "$NATIVE_DIR/native-inputs.txt" tantivy4java_version)" = "$t4j_version" ] \
  || die "the native artifact is not tantivy4java $t4j_version"
t4j_jar="tantivy4java-$t4j_version-$NATIVE_CLASSIFIER.jar"
[ -f "$NATIVE_DIR/repository/$t4j_jar" ] || die "the native artifact has no $t4j_jar"
t4j_sha="$(hash_of sha256 "$NATIVE_DIR/repository/$t4j_jar")"

dest="$M2_REPO/io/indextables/tantivy4java/$t4j_version"
[ ! -e "$dest" ] \
  || die "tantivy4java $t4j_version is already in the local Maven repository ($dest); the release build must take it from this run's native job only"
mkdir -p "$dest"
cp "$NATIVE_DIR/repository/"* "$dest/"

mvn_cmd=(mvn -B -ntp "-Dmaven.repo.local=$M2_REPO" "-Dplatform.classifier=$NATIVE_CLASSIFIER")

# --- version ------------------------------------------------------------------
spark_version="$(cd "$SRC" && "${mvn_cmd[@]}" -q "-P$PROFILE" "$HELP_PLUGIN:evaluate" \
  -Dexpression=spark.version -DforceStdout | tail -n 1 | tr -d '[:space:]')"
version="${BASE_VERSION}_spark_${spark_version}"
check_version "$BASE_VERSION" "$PROFILE" "$version"
announced="$(profile_version "$EXPECTED_VERSIONS" "$PROFILE")"
[ "$version" = "$announced" ] \
  || die "Maven derives $version for $PROFILE, but the plan announced $announced"
echo "Building $ARTIFACT_ID $version (profile $PROFILE, Spark $spark_version)"

(cd "$SRC" && "${mvn_cmd[@]}" "$VERSIONS_PLUGIN:set" "-DnewVersion=$version" -DgenerateBackupPoms=false)

# --- build --------------------------------------------------------------------
# `verify` with the release profile produces the jar, the sources jar, the
# scaladoc jar, the shaded jar and the jar-with-dependencies. Signing is
# switched off (there is no key in this job) and nothing is deployed.
rm -f "$SRC/dependency-reduced-pom.xml"
(cd "$SRC" && "${mvn_cmd[@]}" "-P$PROFILE,release" -DskipTests -Dgpg.skip=true verify)

[ "$(hash_of sha256 "$dest/$t4j_jar")" = "$t4j_sha" ] \
  || die "$t4j_jar in the local Maven repository changed during the build"

# --- collect ------------------------------------------------------------------
# Target file -> published name. The published pom is the dependency-reduced
# pom that the shade plugin writes; the assembly is attached under the
# classifier "jar-with-dependencies" although its file in target/ carries the
# platform classifier as well. Both match what is on Maven Central for
# earlier releases.
p="$ARTIFACT_ID-$version"
t="$SRC/target"
rm -rf "$OUT"
mkdir -p "$OUT/files"
collect() { # collect <source file> <published name>
  [ -f "$1" ] || die "the build did not produce $1"
  cp "$1" "$OUT/files/$2"
}
collect "$SRC/dependency-reduced-pom.xml" "$p.pom"
collect "$t/$p.jar" "$p.jar"
collect "$t/$p-sources.jar" "$p-sources.jar"
collect "$t/$p-javadoc.jar" "$p-javadoc.jar"
collect "$t/$p-$NATIVE_CLASSIFIER-shaded.jar" "$p-$NATIVE_CLASSIFIER-shaded.jar"
collect "$t/$p-$NATIVE_CLASSIFIER-jar-with-dependencies.jar" "$p-jar-with-dependencies.jar"

# A jar this script does not know about means pom.xml attaches something new.
# Stop, so that publishing it (or not) is a decision and not an accident.
for f in "$t/$ARTIFACT_ID-"*.jar; do
  case "$(basename "$f")" in
    "$p.jar" | "$p-sources.jar" | "$p-javadoc.jar" | "$p-$NATIVE_CLASSIFIER-shaded.jar" | "$p-$NATIVE_CLASSIFIER-jar-with-dependencies.jar") ;;
    *) die "the build produced $(basename "$f"), which the release scripts do not know; update ARTIFACT_SUFFIXES in lib.sh and build-artifacts.sh if it should be published" ;;
  esac
done

{
  echo "profile=$PROFILE"
  echo "spark_version=$spark_version"
  echo "version=$version"
  echo "tantivy4java_version=$t4j_version"
  echo "tantivy4java_jar_sha256=$t4j_sha"
  echo "java=$("${JAVA_HOME:+$JAVA_HOME/bin/}java" -version 2>&1 | head -n 1)"
  echo "maven=$(mvn --version 2> /dev/null | head -n 1)"
} > "$OUT/build-info.txt"
write_sums "$OUT"

output version "$version"
{
  echo "### $ARTIFACT_ID $version ($PROFILE)"
  echo
  echo "| File | SHA-256 |"
  echo "|---|---|"
  grep ' files/' "$OUT/SHA256SUMS" | while read -r sum path; do
    echo "| \`${path#files/}\` | \`$sum\` |"
  done
} | summary
