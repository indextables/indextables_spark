#!/usr/bin/env bash
# Attach the shaded jars to the GitHub Release for the tag, creating the
# release if it does not exist yet, and record the build inputs in its notes.
#
#   github-release.sh <staging dir>
#
# Environment:
#   GH_TOKEN, GH_REPO   token with contents: write, and owner/repository
#   TAG, COMMIT         from plan.sh
#   PRERELEASE          true marks a newly created release as a pre-release
#
# The tag must still point to COMMIT: the artifacts were built from that
# commit, and a tag that moved since the run started is not released.
#
# Assets are the same as before: one
# indextables_spark-<version>-linux-x86_64-shaded.jar per Spark version,
# replacing an asset of the same name if the release already has one. The
# notes (hand-written for an existing release, generated for a new one) are
# kept; only the block between the release-build-inputs markers is added or
# replaced.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

staging="${1:?usage: github-release.sh <staging dir>}"
: "${GH_TOKEN:?}" "${GH_REPO:?}" "${TAG:?}" "${COMMIT:?}"
need gh jq
[[ "$TAG" =~ $TAG_RE ]] || die "'$TAG' is not a release tag"
[ -s "$staging/versions.txt" ] && [ -s "$staging/release-notes.md" ] || die "$staging is not a staging directory"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# --- the tag has not moved ----------------------------------------------------
gh api "repos/$GH_REPO/git/ref/tags/$TAG" > "$tmp/ref.json" || die "tag $TAG does not exist in $GH_REPO"
sha="$(jq -r '.object.sha // empty' "$tmp/ref.json")"
if [ "$(jq -r '.object.type // empty' "$tmp/ref.json")" = tag ]; then
  # Annotated tag: the ref points to a tag object; follow it to the commit.
  sha="$(gh api "repos/$GH_REPO/git/tags/$sha" --jq '.object.sha')"
fi
[ "$sha" = "$COMMIT" ] || die "tag $TAG now points to $sha, but this run built $COMMIT; not releasing"

# --- assets -------------------------------------------------------------------
assets=()
while IFS= read -r version; do
  f="$staging/bundles/$version/$GROUP_PATH/$version/$ARTIFACT_ID-$version-$NATIVE_CLASSIFIER-shaded.jar"
  [ -f "$f" ] || die "staged shaded jar missing for $version"
  assets+=("$f")
done < "$staging/versions.txt"

if gh release view "$TAG" --json body --jq '.body' > "$tmp/body.md" 2> "$tmp/view.err"; then
  echo "Release $TAG exists; attaching the jars."
  gh release upload "$TAG" --clobber "${assets[@]}"
elif grep -qi 'not found' "$tmp/view.err"; then
  echo "Creating release $TAG."
  flags=(--verify-tag --generate-notes)
  if [ "${PRERELEASE:-false}" = true ]; then flags+=(--prerelease); fi
  gh release create "$TAG" "${flags[@]}" "${assets[@]}"
  gh release view "$TAG" --json body --jq '.body' > "$tmp/body.md"
else
  cat "$tmp/view.err" >&2
  die "could not look up release $TAG"
fi

# --- notes --------------------------------------------------------------------
# Keep the notes as they are (hand-written or generated), drop a build-inputs
# block left by an earlier run and trailing blank lines, and append the
# current block. Notes edited in the browser have CRLF line ends.
tr -d '\r' < "$tmp/body.md" | awk '
  $0 == "<!-- release-build-inputs:start -->" { skip = 1 }
  !skip {
    if ($0 ~ /^[[:space:]]*$/) { blank = blank $0 "\n" } else { printf "%s%s\n", blank, $0; blank = "" }
  }
  $0 == "<!-- release-build-inputs:end -->" { skip = 0 }
' > "$tmp/notes.md"
if [ -s "$tmp/notes.md" ]; then echo >> "$tmp/notes.md"; fi
cat "$staging/release-notes.md" >> "$tmp/notes.md"
gh release edit "$TAG" --notes-file "$tmp/notes.md" > /dev/null

url="$(gh release view "$TAG" --json url --jq '.url' 2> /dev/null || true)"
{
  echo "### GitHub Release"
  echo
  echo "${url:-Release $TAG}: ${#assets[@]} shaded jars attached."
} | summary
