#!/usr/bin/env bash
# Sign every staged file and zip one upload bundle per version.
#
#   sign-bundles.sh <staging dir> <bundle dir>
#
# Environment:
#   GNUPGHOME       keyring holding the signing key (see signing-key.sh)
#   SIGNING_KEY     fingerprint of the key to sign with
#   GPG_PASSPHRASE  its passphrase (may be empty for an unprotected key)
#
# Writes <file>.asc next to each of the six files of each version (checksum
# files are not signed, as before), then
# <bundle dir>/indextables_spark-<version>-bundle.zip holding that version's
# Maven repository layout. Run verify-staging.sh first: this script signs
# whatever is in the staging directory.
#
# The same script runs in the rehearse job (throwaway key) and in the publish
# job (release key), so a dry run exercises the exact signing code.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

staging="${1:?usage: sign-bundles.sh <staging dir> <bundle dir>}"
out="${2:?usage: sign-bundles.sh <staging dir> <bundle dir>}"
: "${GNUPGHOME:?}" "${SIGNING_KEY:?}"
export GNUPGHOME
passphrase="${GPG_PASSPHRASE:-}"
need gpg zip
[ -s "$staging/versions.txt" ] || die "$staging lists no versions"

mkdir -p "$out"
out="$(cd "$out" && pwd)"

while IFS= read -r version <&4; do
  root="$staging/bundles/$version"
  [ -d "$root/$GROUP_PATH/$version" ] || die "nothing staged for $version"
  for name in $(artifact_names "$version"); do
    f="$root/$GROUP_PATH/$version/$name"
    [ -f "$f" ] || die "staged file missing: $name"
    [ ! -e "$f.asc" ] || die "$name is already signed; sign-bundles.sh needs a clean staging directory"
    gpg --batch --yes --no-tty --pinentry-mode loopback --passphrase-fd 3 \
      --local-user "$SIGNING_KEY" --armor --detach-sign --output "$f.asc" "$f" \
      3<<< "$passphrase" < /dev/null \
      || die "could not sign $name"
  done
  zip="$out/$ARTIFACT_ID-$version-bundle.zip"
  rm -f "$zip"
  (cd "$root" && zip -q -r -X "$zip" "${GROUP_PATH%%/*}")
  echo "Signed and bundled $version: $zip"
done 4< "$staging/versions.txt"
