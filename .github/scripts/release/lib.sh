# Shared by the release scripts. Sourced, never executed.
# Works with bash 3.2 or newer and with either GNU or BSD userland, so that
# test.sh can run the same code on a developer machine and on the runner.

# What is published. Changing any of these changes the published coordinates.
GROUP_ID=io.indextables
ARTIFACT_ID=indextables_spark
GROUP_PATH=io/indextables/indextables_spark
NATIVE_CLASSIFIER=linux-x86_64

# One artifact version is published per Spark profile. Must list the same
# profiles, in the same order, as the build matrix in release.yml.
PROFILES="spark-3.5 spark-4.0 spark-4.1"

# The files that make up one published version, as suffixes of
# <artifactId>-<version>. This is an allow-list: a version directory holding
# any other file is refused.
ARTIFACT_SUFFIXES=".pom .jar -sources.jar -javadoc.jar -${NATIVE_CLASSIFIER}-shaded.jar -jar-with-dependencies.jar"
CHECKSUM_ALGOS="md5 sha1 sha256 sha512"

# v<major>.<minor>.<patch> with an optional pre-release part (v0.6.0-rc2).
TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$'
BASE_RE='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+(\.[0-9A-Za-z]+)*)?$'
SHA_RE='^[0-9a-f]{40}$'

die() { echo "::error::$*" >&2; exit 1; }
warn() { echo "::warning::$*" >&2; }

# need <command...>: fail unless every command is on PATH.
need() {
  local c
  for c in "$@"; do
    command -v "$c" > /dev/null 2>&1 || die "required command not found: $c"
  done
}

# hash_stream <md5|sha1|sha256|sha512>: hex digest of standard input.
hash_stream() {
  local out
  case "$1" in
    md5)
      if command -v md5sum > /dev/null 2>&1; then out="$(md5sum)"; else out="$(md5 -q)"; fi ;;
    sha1 | sha256 | sha512)
      if command -v "${1}sum" > /dev/null 2>&1; then out="$("${1}sum")"; else out="$(shasum -a "${1#sha}")"; fi ;;
    *) die "unknown digest algorithm: $1" ;;
  esac
  out="${out%% *}"
  [[ "$out" =~ ^[0-9a-f]+$ ]] || die "could not compute a $1 digest"
  printf '%s\n' "$out"
}

# hash_of <algorithm> <file>
hash_of() {
  [ -f "$2" ] || die "no such file: $2"
  hash_stream "$1" < "$2"
}

# output <name> <value>: set a step output (single line) and echo it.
output() {
  case "$2" in *$'\n'*) die "refusing to write a multi-line value for output '$1'" ;; esac
  if [ -n "${GITHUB_OUTPUT:-}" ]; then printf '%s=%s\n' "$1" "$2" >> "$GITHUB_OUTPUT"; fi
  printf 'output: %s=%s\n' "$1" "$2"
}

# summary: append standard input to the job summary (or print it when there
# is none, as in test.sh).
summary() {
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then cat >> "$GITHUB_STEP_SUMMARY"; else cat; fi
}

# spark_line <profile>: "3.5" for "spark-3.5". Fails for an unknown profile.
spark_line() {
  local p
  for p in $PROFILES; do
    if [ "$p" = "$1" ]; then printf '%s\n' "${1#spark-}"; return 0; fi
  done
  die "unknown Spark profile: $1 (expected one of: $PROFILES)"
}

# check_version <base> <profile> <version>: the version must be
# <base>_spark_<x.y.z> where x.y is the profile's Spark line.
check_version() {
  local base="$1" line re
  line="$(spark_line "$2")" || exit 1
  re="^${base//./\\.}_spark_${line//./\\.}\\.[0-9]+$"
  [[ "$3" =~ $re ]] || die "version '$3' is not ${base}_spark_${line}.<patch> (profile $2)"
}

# pom_tantivy4java_version <pom.xml>: same extraction as scripts/setup.sh.
pom_tantivy4java_version() {
  local v
  v="$(grep -A2 '<artifactId>tantivy4java</artifactId>' "$1" | grep '<version>' \
    | sed 's/.*<version>\(.*\)<\/version>.*/\1/' | tr -d '[:space:]' || true)"
  [[ "$v" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "could not read the tantivy4java version from $1 (got '$v')"
  printf '%s\n' "$v"
}

# kv <file> <key>: value of key=value in a build-info file (first match).
kv() {
  sed -n "s/^$2=//p" "$1" | head -n 1
}

# artifact_names <version>: the allow-listed file names of one version.
artifact_names() {
  local sfx
  for sfx in $ARTIFACT_SUFFIXES; do printf '%s-%s%s\n' "$ARTIFACT_ID" "$1" "$sfx"; done
}

# write_sums <dir> [name]: write <dir>/<name> (default SHA256SUMS) listing the
# SHA-256 of every other regular file under <dir>, in sha256sum format.
write_sums() {
  local dir="$1" name="${2:-SHA256SUMS}"
  [ -d "$dir" ] || die "no such directory: $dir"
  (
    cd "$dir" || exit 1
    find . -type f ! -path "./$name" ! -path "./$name.tmp" | sed 's|^\./||' | LC_ALL=C sort \
      | while IFS= read -r path; do
        printf '%s  %s\n' "$(hash_of sha256 "$path")" "$path"
      done > "$name.tmp"
    mv "$name.tmp" "$name"
  )
}

# verify_sums <dir> [name]: <dir>/<name> must list exactly the regular files
# under <dir> (no more, no fewer, nothing that is not a regular file) and
# every digest must match. The directory may come from another job, so
# nothing in it is used before this passes.
verify_sums() {
  local dir="$1" name="${2:-SHA256SUMS}" listed actual sum path
  [ -f "$dir/$name" ] || die "$dir has no $name"
  [ -z "$(find "$dir" ! -type f ! -type d | head -n 1)" ] \
    || die "$dir contains something that is neither a regular file nor a directory"
  listed="$(awk '{ print $2 }' "$dir/$name" | LC_ALL=C sort)"
  actual="$(cd "$dir" && find . -type f ! -path "./$name" | sed 's|^\./||' | LC_ALL=C sort)"
  [ -n "$actual" ] || die "$dir is empty"
  [ "$listed" = "$actual" ] || die "the files in $dir are not exactly the files listed in $name"
  while read -r sum path; do
    [ "$(hash_of sha256 "$dir/$path")" = "$sum" ] || die "digest mismatch for $path in $dir"
  done < "$dir/$name"
}
