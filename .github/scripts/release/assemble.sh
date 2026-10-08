#!/usr/bin/env bash
# Turn the build jobs' outputs into the exact trees that will be signed and
# uploaded, after checking them. Runs no project code: it only reads files.
#
# Everything in IN was produced by jobs that ran the project's build, so it is
# treated as untrusted until checked. What comes out is an allow-list result:
# only the expected coordinates, only the expected file names.
#
# Environment:
#   IN            directory with the downloaded artifacts:
#                   native/               build-native.sh output
#                   artifacts-<profile>/  build-artifacts.sh output, per profile
#   OUT           staging directory to create:
#                   versions.txt          the versions, one per line
#                   release-notes.md      the "Build inputs" block
#                   bundles/<version>/    Maven repository layout for that
#                                         version: the six files and their
#                                         md5/sha1/sha256/sha512 (no signatures)
#                   MANIFEST.sha256       SHA-256 of every file above
#   TAG, COMMIT, BASE_VERSION   from plan.sh
#   RUN_URL       link to this workflow run (optional, for the notes)
#
# Step outputs: manifest_sha256 (digest of MANIFEST.sha256), versions.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

: "${IN:?}" "${OUT:?}" "${TAG:?}" "${COMMIT:?}" "${BASE_VERSION:?}"
need unzip python3
[[ "$TAG" =~ $TAG_RE ]] || die "'$TAG' is not a release tag"
[ "v$BASE_VERSION" = "$TAG" ] || die "base version $BASE_VERSION does not belong to tag $TAG"
[[ "$COMMIT" =~ $SHA_RE ]] || die "'$COMMIT' is not a commit id"

# check_pom <pom> <version>: coordinates must be the ones this repository
# publishes, and the metadata Maven Central requires must be present.
check_pom() {
  python3 - "$1" "$GROUP_ID" "$ARTIFACT_ID" "$2" << 'PY' || die "$(basename "$1") failed the pom checks (above)"
import sys
import xml.etree.ElementTree as ET

path, group, artifact, version = sys.argv[1:5]
ns = "{http://maven.apache.org/POM/4.0.0}"
try:
    root = ET.parse(path).getroot()
except ET.ParseError as exc:
    sys.exit("pom is not well-formed XML: %s" % exc)

def text(*names):
    node = root
    for name in names:
        node = node.find(ns + name) if node is not None else None
    return (node.text or "").strip() if node is not None else ""

problems = []
if root.tag != ns + "project":
    problems.append("root element is not a Maven <project>")
if root.find(ns + "parent") is not None:
    problems.append("has a <parent>; coordinates must be stated in the pom itself")
for names, want in ((("groupId",), group), (("artifactId",), artifact), (("version",), version)):
    got = text(*names)
    if got != want:
        problems.append("%s is '%s', expected '%s'" % ("/".join(names), got, want))
if text("packaging") not in ("", "jar"):
    problems.append("packaging is '%s', expected jar" % text("packaging"))
required = (("name",), ("description",), ("url",),
            ("licenses", "license", "name"), ("licenses", "license", "url"),
            ("developers", "developer", "name"),
            ("scm", "connection"), ("scm", "url"))
for names in required:
    if not text(*names):
        problems.append("missing %s (required by Maven Central)" % "/".join(names))
if problems:
    sys.exit("\n".join(problems))
PY
}

# --- the native build ---------------------------------------------------------
native="$IN/native"
verify_sums "$native"
ninfo="$native/native-inputs.txt"
t4j_version="$(kv "$ninfo" tantivy4java_version)"
[[ "$t4j_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "the native artifact does not state a tantivy4java version"
[ "$(kv "$ninfo" cargo_lock_unchanged)" = true ] || die "the native build does not record an unchanged Cargo.lock"
t4j_jar="$native/repository/tantivy4java-$t4j_version-$NATIVE_CLASSIFIER.jar"
[ -f "$t4j_jar" ] || die "the native artifact has no $(basename "$t4j_jar")"
t4j_sha="$(hash_of sha256 "$t4j_jar")"
libs="$(unzip -Z1 "$t4j_jar" | grep -E '\.so$' || true)"
[ -n "$libs" ] || die "$(basename "$t4j_jar") contains no native library (*.so)"

rm -rf "$OUT"
mkdir -p "$OUT/bundles"
: > "$OUT/versions.txt"

# --- each Spark profile -------------------------------------------------------
for profile in $PROFILES; do
  d="$IN/artifacts-$profile"
  [ -d "$d" ] || die "no build output for $profile"
  verify_sums "$d"
  info="$d/build-info.txt"
  [ "$(kv "$info" profile)" = "$profile" ] || die "artifacts-$profile was not built with profile $profile"
  version="$(kv "$info" version)"
  check_version "$BASE_VERSION" "$profile" "$version"
  [ "${BASE_VERSION}_spark_$(kv "$info" spark_version)" = "$version" ] \
    || die "artifacts-$profile: version $version does not match its recorded Spark version"

  expected="$({ artifact_names "$version" | sed 's|^|files/|'; echo build-info.txt; } | LC_ALL=C sort)"
  actual="$(cd "$d" && find . -type f ! -path ./SHA256SUMS | sed 's|^\./||' | LC_ALL=C sort)"
  [ "$expected" = "$actual" ] \
    || die "artifacts-$profile does not hold exactly the expected files. Expected: $(echo $expected). Found: $(echo $actual)"

  [ "$(kv "$info" tantivy4java_jar_sha256)" = "$t4j_sha" ] \
    || die "artifacts-$profile was not built with this run's tantivy4java jar"

  p="$ARTIFACT_ID-$version"
  check_pom "$d/files/$p.pom" "$version"
  for name in $(artifact_names "$version"); do
    case "$name" in
      *.jar) unzip -tqq "$d/files/$name" > /dev/null 2>&1 || die "$name is not a readable jar" ;;
    esac
  done

  # The native library inside the published jars must be, byte for byte, the
  # one this run built from the pinned commits.
  for jar in "$p-$NATIVE_CLASSIFIER-shaded.jar" "$p-jar-with-dependencies.jar"; do
    while IFS= read -r lib; do
      want="$(unzip -p "$t4j_jar" "$lib" | hash_stream sha256)"
      got="$(unzip -p "$d/files/$jar" "$lib" 2> /dev/null | hash_stream sha256 || true)"
      [ "$got" = "$want" ] || die "$jar does not contain this run's $lib"
    done <<< "$libs"
  done

  vd="$OUT/bundles/$version/$GROUP_PATH/$version"
  mkdir -p "$vd"
  for name in $(artifact_names "$version"); do
    cp "$d/files/$name" "$vd/$name"
    for algo in $CHECKSUM_ALGOS; do
      printf '%s' "$(hash_of "$algo" "$vd/$name")" > "$vd/$name.$algo"
    done
  done
  echo "$version" >> "$OUT/versions.txt"
done

# --- notes ----------------------------------------------------------------------
# The values below were written by the native job, which runs build code, and
# they end up in the release notes. Identifiers must have their exact form;
# free-text tool versions are shown only if they are plain text.
SAFE_RE='^[A-Za-z0-9 ._,:/@()+="-]*$'
safe() {
  if [ "${#1}" -le 200 ] && [[ "$1" =~ $SAFE_RE ]]; then printf '%s' "$1"; else printf '(not shown: unexpected characters)'; fi
}
[[ "$(kv "$ninfo" tantivy4java_tag)" == "v$t4j_version" ]] || die "the native artifact's tag is not v$t4j_version"
[[ "$(kv "$ninfo" tantivy4java_commit)" =~ $SHA_RE ]] || die "the native artifact does not state a tantivy4java commit"
for key in quickwit_commit tantivy_commit; do
  value="$(kv "$ninfo" "$key")"
  [ "$value" = "-" ] || [[ "$value" =~ $SHA_RE ]] || die "the native artifact's $key is not a commit id"
done
[[ "$(kv "$ninfo" cargo_lock_sha256)" =~ ^[0-9a-f]{64}$ ]] || die "the native artifact does not state a Cargo.lock digest"
{
  echo "<!-- release-build-inputs:start -->"
  echo "### Build inputs"
  echo
  if [ -n "${RUN_URL:-}" ]; then
    echo "Built from commit \`$COMMIT\` (tag \`$TAG\`) by [this workflow run]($RUN_URL)."
  else
    echo "Built from commit \`$COMMIT\` (tag \`$TAG\`)."
  fi
  echo "The native library was compiled in that run from the commits below; nothing was restored from a cache."
  echo
  echo "| Input | Value |"
  echo "|---|---|"
  echo "| tantivy4java | \`$(kv "$ninfo" tantivy4java_tag)\` at \`$(kv "$ninfo" tantivy4java_commit)\` |"
  echo "| quickwit fork | \`$(kv "$ninfo" quickwit_commit)\` |"
  echo "| tantivy fork | \`$(kv "$ninfo" tantivy_commit)\` |"
  other="$(kv "$ninfo" other_git_dependencies)"
  [ -z "$other" ] || echo "| other git dependencies | \`$(safe "$other")\` |"
  echo "| Cargo.lock | SHA-256 \`$(kv "$ninfo" cargo_lock_sha256)\`, unchanged by the build |"
  for key in rustc cargo protoc java maven runner_image; do
    value="$(kv "$ninfo" "$key")"
    [ -z "$value" ] || echo "| ${key//_/ } | \`$(safe "$value")\` |"
  done
  echo
  echo "| Version | SHA-256 of \`-$NATIVE_CLASSIFIER-shaded.jar\` |"
  echo "|---|---|"
  while IFS= read -r version; do
    f="$OUT/bundles/$version/$GROUP_PATH/$version/$ARTIFACT_ID-$version-$NATIVE_CLASSIFIER-shaded.jar"
    echo "| \`$version\` | \`$(hash_of sha256 "$f")\` |"
  done < "$OUT/versions.txt"
  echo "<!-- release-build-inputs:end -->"
} > "$OUT/release-notes.md"

write_sums "$OUT" MANIFEST.sha256
manifest_sha="$(hash_of sha256 "$OUT/MANIFEST.sha256")"
output manifest_sha256 "$manifest_sha"
output versions "$(tr '\n' ' ' < "$OUT/versions.txt" | sed 's/ $//')"

{
  echo "## Staged for $TAG"
  echo
  echo "Manifest SHA-256: \`$manifest_sha\`. The publish job signs and uploads exactly the files this manifest lists, and refuses anything else."
  echo
  sed -e '/^<!-- release-build-inputs/d' "$OUT/release-notes.md"
  echo
  echo "<details><summary>Every staged file</summary>"
  echo
  echo "| File | SHA-256 |"
  echo "|---|---|"
  grep -v -E '\.(md5|sha1|sha256|sha512)$' "$OUT/MANIFEST.sha256" | while read -r sum path; do
    echo "| \`$(basename "$path")\` | \`$sum\` |"
  done
  echo
  echo "</details>"
} | summary
