#!/usr/bin/env bash
# Decide what this run does, before anything is built.
#
# Reads the triggering event and the dispatch inputs (as strings, from the
# environment), validates the tag, resolves it to a commit, checks that the
# commit is on the default branch and works out the versions that would be
# published. Sets the step outputs:
#
#   mode        dry-run | publish
#   tag         the release tag (v0.6.0)
#   commit      the commit the tag points to; every later job uses this
#   base        the tag without its "v" (0.6.0)
#   versions    the versions to publish, one per Spark profile, in PROFILES
#               order (0.6.0_spark_3.5.9 0.6.0_spark_4.0.4 0.6.0_spark_4.1.3)
#   prerelease  true when the base version has a pre-release part (0.6.0-rc2)
#   central     true unless a publish run asked to skip Maven Central
#
# Only "publish" lets the publish job start, and only these two cases give it:
#   * workflow_dispatch from the default branch with dry-run set to false
#   * a pushed tag (not enabled in release.yml; see the README)
# Every other combination is a dry run or an error.
#
# Versions. <base>_spark_<spark.version>, where spark.version is what each
# profile sets in pom.xml at the tagged commit. That value changes whenever
# the Spark dependency is bumped, and with it the published artifact names.
# So that this never happens unnoticed:
#   * the Plan summary lists the exact versions, and for each Spark line the
#     version last published to Maven Central;
#   * a publish run whose Spark versions differ from the last published ones
#     (or that cannot be compared) stops here unless the releaser has typed
#     the Spark versions into the expected-spark-versions input;
#   * when that input is given it must match what pom.xml says, in any run.
# The build jobs later check that Maven derives the same versions.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

: "${EVENT_NAME:?}" "${REF:?}" "${DEFAULT_BRANCH:?}"
repo_dir="${REPO_DIR:-.}"
need git python3 curl

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

# --- versions -----------------------------------------------------------------
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
git -C "$repo_dir" show "$commit:pom.xml" > "$tmp/pom.xml" 2> /dev/null || die "the tagged commit has no pom.xml"
# pom.xml is read as data; nothing from the tagged commit is executed here.
cat > "$tmp/spark_versions.py" << 'PY'
import sys
import xml.etree.ElementTree as ET

ns = "{http://maven.apache.org/POM/4.0.0}"
try:
    root = ET.parse(sys.argv[1]).getroot()
except ET.ParseError as exc:
    sys.exit("pom.xml is not well-formed XML: %s" % exc)

def child(node, name):
    for c in (node if node is not None else []):
        if c.tag == ns + name:
            return c
    return None

def spark_version(node):
    c = child(child(node, "properties"), "spark.version")
    return (c.text or "").strip() if c is not None else ""

top = spark_version(root)
by_profile = {}
for p in (child(root, "profiles") if child(root, "profiles") is not None else []):
    pid = child(p, "id")
    if pid is not None:
        by_profile[(pid.text or "").strip()] = spark_version(p)
out = []
for name in sys.argv[2:]:
    if name not in by_profile:
        sys.exit("pom.xml at the tag has no profile %s" % name)
    value = by_profile[name] or top
    if not value or "$" in value:
        sys.exit("pom.xml at the tag does not state spark.version for profile %s" % name)
    out.append(value)
print(" ".join(out))
PY
# shellcheck disable=SC2086
spark_versions="$(python3 "$tmp/spark_versions.py" "$tmp/pom.xml" $PROFILES)" \
  || die "could not read the Spark versions from pom.xml at $tag (above)"

versions=""
# shellcheck disable=SC2086
set -- $spark_versions
for profile in $PROFILES; do
  check_version "$base" "$profile" "${base}_spark_$1"
  versions="${versions:+$versions }${base}_spark_$1"
  shift
done

# What the releaser expects, if they said so: the same Spark versions, in any
# order, separated by spaces or commas.
expected="$(printf '%s' "${INPUT_EXPECTED_SPARK:-}" | tr ',' ' ' | tr -s ' ' '\n' | grep . | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//' || true)"
confirmed=false
if [ -n "$expected" ]; then
  derived_sorted="$(printf '%s\n' $spark_versions | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//')"
  [ "$expected" = "$derived_sorted" ] \
    || die "expected-spark-versions is '$expected', but pom.xml at $tag gives '$derived_sorted'. Nothing was built. Check which is right before trying again."
  confirmed=true
fi

# Compare with what was last published for each Spark line.
metadata=""
if curl -fsS --connect-timeout 15 --max-time 60 -o "$tmp/metadata.xml" "$CENTRAL_REPO/$GROUP_PATH/maven-metadata.xml" 2> /dev/null; then
  metadata="$tmp/metadata.xml"
fi
changed=""
comparison=""
# shellcheck disable=SC2086
set -- $spark_versions
for profile in $PROFILES; do
  line="$(spark_line "$profile")"
  if [ -z "$metadata" ]; then
    last="unknown"
  else
    last="$(grep -o -E "<version>[^<]*_spark_${line//./\\.}\.[0-9]+</version>" "$metadata" | tail -n 1 \
      | sed -E 's/.*_spark_([0-9.]+)<.*/\1/' || true)"
    last="${last:-none}"
  fi
  case "$last" in
    "$1") note="unchanged" ;;
    none) note="first release for Spark $line" ;;
    unknown) note="could not be compared"; changed="$changed $line" ;;
    *) note="**changed** from $last"; changed="$changed $line" ;;
  esac
  comparison="$comparison| \`${base}_spark_$1\` | ${last} | $note |"$'\n'
  shift
done

if [ -n "$changed" ] && [ "$confirmed" != true ]; then
  if [ -z "$metadata" ]; then
    what="The versions last published to Maven Central could not be read, so the artifact names could not be compared with the last release."
  else
    what="The Spark version in the artifact names differs from the last release for Spark$changed. Consumers that name the full version need the new one."
  fi
  if [ "$mode" = publish ]; then
    die "$what This run would publish: $versions. If that is intended, dispatch again with expected-spark-versions set to: $spark_versions"
  fi
  warn "$what A publish run will ask for expected-spark-versions: $spark_versions"
fi

output mode "$mode"
output tag "$tag"
output commit "$commit"
output base "$base"
output versions "$versions"
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
  echo "| Workflow ref | \`$REF\` |"
  echo
  echo "### Versions"
  echo
  echo "| To be published | Spark version last on Maven Central | |"
  echo "|---|---|---|"
  printf '%s' "$comparison"
  echo
  if [ "$confirmed" = true ]; then
    echo "The Spark versions were confirmed with \`expected-spark-versions\`."
  elif [ -n "$changed" ]; then
    echo "> **The artifact names are not the same as in the last release.** To publish them, set \`expected-spark-versions\` to \`$spark_versions\`."
  else
    echo "Same Spark versions as the last release. (\`expected-spark-versions\` would be \`$spark_versions\`.)"
  fi
} | summary
