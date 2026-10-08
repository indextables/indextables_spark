#!/usr/bin/env bash
# Decide what this run does, before anything is built.
#
# Reads the triggering event and the dispatch inputs (as strings, from the
# environment), validates the tag, resolves it to a commit and checks that the
# commit is on the default branch. Sets the step outputs:
#
#   mode        dry-run | publish
#   tag         the release tag (v0.6.0)
#   commit      the commit the tag points to; every later job uses this
#   base        the tag without its "v" (0.6.0)
#   prerelease  true when the base version has a pre-release part (0.6.0-rc2)
#   central     true unless a publish run asked to skip Maven Central
#
# Only "publish" lets the publish job start, and only these two cases give it:
#   * workflow_dispatch from the default branch with dry-run set to false
#   * a pushed tag (not enabled in release.yml; see the README)
# Every other combination is a dry run or an error.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

: "${EVENT_NAME:?}" "${REF:?}" "${DEFAULT_BRANCH:?}"
repo_dir="${REPO_DIR:-.}"
need git

case "$EVENT_NAME" in
  workflow_dispatch)
    tag="${INPUT_TAG:-}"
    case "${INPUT_DRY_RUN:-}" in
      true) mode=dry-run ;;
      false)
        [ "$REF" = "refs/heads/$DEFAULT_BRANCH" ] \
          || die "A release can only be dispatched from $DEFAULT_BRANCH (this run is on $REF). Dry runs may be dispatched from any ref."
        mode=publish ;;
      *) die "dry-run must be true or false (got '${INPUT_DRY_RUN:-}')" ;;
    esac
    ;;
  push)
    [ "${REF_TYPE:-}" = tag ] || die "push runs are only supported for tags (this run is on $REF)"
    tag="${REF_NAME:-}"
    mode=publish
    ;;
  *) die "unsupported event: $EVENT_NAME" ;;
esac

[[ "$tag" =~ $TAG_RE ]] || die "'$tag' is not a release tag (expected v<major>.<minor>.<patch>, optionally with a pre-release part such as -rc1)"
base="${tag#v}"

commit="$(git -C "$repo_dir" rev-parse --verify --quiet "refs/tags/$tag^{commit}" || true)"
[[ "$commit" =~ $SHA_RE ]] || die "tag $tag does not exist in this repository"

# The source that gets published must have been merged: the tag's commit has
# to be the tip of the default branch or one of its ancestors.
git -C "$repo_dir" rev-parse --verify --quiet "refs/remotes/origin/$DEFAULT_BRANCH^{commit}" > /dev/null \
  || die "origin/$DEFAULT_BRANCH is not available; the checkout must fetch full history"
git -C "$repo_dir" merge-base --is-ancestor "$commit" "refs/remotes/origin/$DEFAULT_BRANCH" \
  || die "tag $tag ($commit) is not an ancestor of $DEFAULT_BRANCH; only merged commits can be released"

case "$base" in *-*) prerelease=true ;; *) prerelease=false ;; esac

central=true
if [ "$mode" = publish ] && [ "${INPUT_SKIP_CENTRAL:-false}" = true ]; then central=false; fi

output mode "$mode"
output tag "$tag"
output commit "$commit"
output base "$base"
output prerelease "$prerelease"
output central "$central"

{
  if [ "$mode" = publish ]; then
    echo "## Release $tag"
    echo
    if [ "$central" = true ]; then
      echo "This run **publishes**: it uploads to Maven Central and creates or updates the GitHub Release, once the \`release\` environment has been approved."
    else
      echo "This run creates or updates the **GitHub Release only** (Maven Central was skipped by input), once the \`release\` environment has been approved."
    fi
  else
    echo "## Dry run for $tag"
    echo
    echo "Nothing is uploaded and no release is created. The publish job is not started, so no approval is requested and no secret is read."
  fi
  echo
  echo "| | |"
  echo "|---|---|"
  echo "| Tag | \`$tag\` |"
  echo "| Commit | \`$commit\` |"
  echo "| Versions | \`${base}_spark_<Spark version of each profile>\` |"
  echo "| Workflow ref | \`$REF\` |"
} | summary
