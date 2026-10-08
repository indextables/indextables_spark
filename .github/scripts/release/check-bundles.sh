#!/usr/bin/env bash
# Check the upload bundles against what Maven Central requires and against
# what was staged, before any of them is uploaded.
#
#   check-bundles.sh <bundle dir> <staging dir>
#
# Environment:
#   GNUPGHOME     keyring holding the signing key (public part is enough)
#   SIGNING_KEY   fingerprint every signature must come from
#   BASE_VERSION  the tag without its "v"
#
# For each version, the bundle must contain exactly, under
# io/indextables/indextables_spark/<version>/:
#   the pom, the jar, the sources jar, the javadoc jar, the shaded jar and the
#   jar-with-dependencies, each with .asc, .md5, .sha1, .sha256 and .sha512
# (36 entries, the same set that is on Maven Central for earlier releases),
# and nothing else. Every checksum must match, every signature must verify
# and come from SIGNING_KEY, and every file must be identical to the staged
# one.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

bundles="${1:?usage: check-bundles.sh <bundle dir> <staging dir>}"
staging="${2:?usage: check-bundles.sh <bundle dir> <staging dir>}"
: "${GNUPGHOME:?}" "${SIGNING_KEY:?}" "${BASE_VERSION:?}"
export GNUPGHOME
need gpg unzip
[ -s "$staging/versions.txt" ] || die "$staging lists no versions"

# Maven Central accepts bundles up to 1 GB.
max_bytes=1073741824

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

count=0
set -- $PROFILES
while IFS= read -r version <&4; do
  [ $# -gt 0 ] || die "more versions than Spark profiles"
  check_version "$BASE_VERSION" "$1" "$version"
  shift

  zip="$bundles/$ARTIFACT_ID-$version-bundle.zip"
  [ -f "$zip" ] || die "no bundle for $version"
  bytes="$(wc -c < "$zip" | tr -d '[:space:]')"
  [ "$bytes" -lt "$max_bytes" ] || die "bundle for $version is $bytes bytes; Maven Central accepts at most $max_bytes"

  prefix="$GROUP_PATH/$version"
  expected="$(
    for name in $(artifact_names "$version"); do
      for ext in "" .asc .md5 .sha1 .sha256 .sha512; do echo "$prefix/$name$ext"; done
    done | LC_ALL=C sort
  )"
  actual="$(unzip -Z1 "$zip" | grep -v '/$' | LC_ALL=C sort)"
  if [ "$expected" != "$actual" ]; then
    echo "Expected entries:" >&2
    printf '%s\n' "$expected" >&2
    echo "Actual entries:" >&2
    printf '%s\n' "$actual" >&2
    die "bundle for $version does not hold exactly the expected entries"
  fi

  x="$tmp/$version"
  mkdir -p "$x"
  unzip -q "$zip" -d "$x"
  for name in $(artifact_names "$version"); do
    f="$x/$prefix/$name"
    for algo in $CHECKSUM_ALGOS; do
      [ "$(cat "$f.$algo")" = "$(hash_of "$algo" "$f")" ] || die "$name.$algo does not match $name"
    done
    status="$(gpg --batch --no-tty --status-fd 1 --verify "$f.asc" "$f" 2> /dev/null)" \
      || die "the signature of $name does not verify"
    signer="$(printf '%s\n' "$status" | awk '$2 == "VALIDSIG" { print $NF }')"
    [ "$signer" = "$SIGNING_KEY" ] || die "$name is signed by '$signer', expected $SIGNING_KEY"
    [ "$(hash_of sha256 "$f")" = "$(hash_of sha256 "$staging/bundles/$version/$prefix/$name")" ] \
      || die "$name in the bundle differs from the staged file"
  done
  rm -rf "$x"
  count=$((count + 1))
  echo "Bundle for $version: $(printf '%s\n' "$actual" | grep -c .) entries, $bytes bytes, signatures by $SIGNING_KEY"
done 4< "$staging/versions.txt"
[ $# -eq 0 ] || die "no bundle for: $*"

{
  echo "### Bundles checked"
  echo
  echo "$count bundles, each with the pom, five jars, and a signature and four checksums for every one of them. All signatures verify and were made by \`$SIGNING_KEY\`."
} | summary
