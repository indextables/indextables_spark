#!/usr/bin/env bash
# Check a downloaded staging directory before anything in it is signed.
#
#   verify-staging.sh <staging dir>
#
# Environment:
#   MANIFEST_SHA256  the digest the assemble job reported as a job output
#   BASE_VERSION     the tag without its "v"
#   EXPECTED_VERSIONS  the versions plan.sh announced, one per profile
#
# The staging directory travels between jobs as a workflow artifact. Its
# manifest digest travels separately, as a job output, so the two can be
# compared here. After this passes, the directory holds exactly the files the
# assemble job checked and listed: nothing added, removed or changed, and
# every version in it belongs to the tag being released.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

staging="${1:?usage: verify-staging.sh <staging dir>}"
: "${MANIFEST_SHA256:?}" "${BASE_VERSION:?}" "${EXPECTED_VERSIONS:?}"
[[ "$MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ ]] || die "MANIFEST_SHA256 is not a SHA-256 digest"

[ -f "$staging/MANIFEST.sha256" ] || die "$staging has no MANIFEST.sha256"
got="$(hash_of sha256 "$staging/MANIFEST.sha256")"
[ "$got" = "$MANIFEST_SHA256" ] \
  || die "the staging manifest has SHA-256 $got, but the assemble job reported $MANIFEST_SHA256"
verify_sums "$staging" MANIFEST.sha256

# Independent of what assemble.sh did: one version per Spark profile, each
# belonging to this tag, and the manifest may only list the notes, the
# version list and the allow-listed files of those versions.
[ -s "$staging/versions.txt" ] || die "the staging directory lists no versions"
set -- $PROFILES
while IFS= read -r version; do
  [ $# -gt 0 ] || die "the staging directory has more versions than Spark profiles"
  check_version "$BASE_VERSION" "$1" "$version"
  shift
done < "$staging/versions.txt"
[ $# -eq 0 ] || die "the staging directory has no version for: $*"
[ "$(tr '\n' ' ' < "$staging/versions.txt" | sed 's/ $//')" = "$EXPECTED_VERSIONS" ] \
  || die "the staged versions are not the versions the plan announced ($EXPECTED_VERSIONS)"

expected="$(
  {
    echo release-notes.md
    echo versions.txt
    while IFS= read -r version; do
      for name in $(artifact_names "$version"); do
        echo "bundles/$version/$GROUP_PATH/$version/$name"
        for algo in $CHECKSUM_ALGOS; do echo "bundles/$version/$GROUP_PATH/$version/$name.$algo"; done
      done
    done < "$staging/versions.txt"
  } | LC_ALL=C sort
)"
listed="$(awk '{ print $2 }' "$staging/MANIFEST.sha256" | LC_ALL=C sort)"
[ "$expected" = "$listed" ] || die "the staging manifest lists files other than the expected ones"

echo "Staging directory verified: manifest $MANIFEST_SHA256, versions: $(tr '\n' ' ' < "$staging/versions.txt")"
