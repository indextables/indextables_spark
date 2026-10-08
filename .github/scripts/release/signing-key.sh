#!/usr/bin/env bash
# Manage the keyring used for signing, in a directory of its own.
#
#   signing-key.sh import     import the private key in GPG_PRIVATE_KEY
#   signing-key.sh generate   create a throwaway key protected by
#                             GPG_PASSPHRASE (rehearsal only)
#   signing-key.sh remove     stop the agent and delete the keyring
#
# GNUPGHOME must name a directory called "release-gnupg"; it is created empty
# by import/generate and deleted by remove. import and generate set the step
# output `fingerprint` to the primary key's fingerprint; the keyring must hold
# exactly one secret key.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

: "${GNUPGHOME:?}"
[ "$(basename "$GNUPGHOME")" = release-gnupg ] || die "GNUPGHOME must be a directory named release-gnupg (got $GNUPGHOME)"
export GNUPGHOME

fresh_home() {
  rm -rf "$GNUPGHOME"
  (umask 077 && mkdir -p "$GNUPGHOME")
}

case "${1:-}" in
  import)
    need gpg
    : "${GPG_PRIVATE_KEY:?}"
    fresh_home
    printf '%s\n' "$GPG_PRIVATE_KEY" | gpg --batch --quiet --no-tty --import \
      || die "could not import the signing key"
    ;;
  generate)
    need gpg
    : "${GPG_PASSPHRASE:?}"
    fresh_home
    gpg --batch --quiet --no-tty --pinentry-mode loopback --passphrase-fd 3 \
      --quick-generate-key "Release rehearsal (throwaway key, never published) <rehearsal@invalid>" \
      ed25519 sign 1d 3<<< "$GPG_PASSPHRASE" < /dev/null \
      || die "could not generate a throwaway key"
    ;;
  remove)
    if command -v gpgconf > /dev/null 2>&1; then gpgconf --kill all > /dev/null 2>&1 || true; fi
    rm -rf "$GNUPGHOME"
    echo "Keyring removed."
    exit 0
    ;;
  *) die "usage: signing-key.sh import|generate|remove" ;;
esac

fingerprints="$(gpg --batch --no-tty --with-colons --list-secret-keys \
  | awk -F: '$1 == "sec" { primary = 1; next } primary && $1 == "fpr" { print $10; primary = 0 }')"
[ "$(printf '%s' "$fingerprints" | grep -c . || true)" = 1 ] \
  || die "the keyring must hold exactly one secret key"
[[ "$fingerprints" =~ ^[0-9A-F]{40}$ ]] || die "could not read the key's fingerprint"
output fingerprint "$fingerprints"
