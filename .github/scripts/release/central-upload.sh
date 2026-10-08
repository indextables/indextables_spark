#!/usr/bin/env bash
# Upload the checked bundles to the Maven Central Portal and wait until each
# one is validated. Nothing is published by this script.
#
#   central-upload.sh upload <bundle dir> <staging dir> <state dir>
#   central-upload.sh drop <state dir>
#
# Environment:
#   CENTRAL_USERNAME, CENTRAL_PASSWORD   the Portal user token
#   CENTRAL_WAIT_SECONDS                 how long to wait for one validation
#                                        (default 1200)
#   CENTRAL_POLL_SECONDS                 pause between status checks (default 10)
#
# Publishing type is USER_MANAGED, as it has been so far: an upload that
# passes validation waits in the Portal as VALIDATED until a person presses
# Publish (or Drop) at https://central.sonatype.com/publishing/deployments.
# That page is where a release is made public, and where it can still be
# abandoned.
#
# `upload` handles one version at a time: upload, then wait for VALIDATED,
# then the next. If anything goes wrong, it drops every deployment it has
# created in this run and fails, so a failed job leaves nothing waiting in the
# Portal (it says so explicitly if a drop is refused). A version that is
# already on Maven Central is skipped when its files are identical to the
# staged ones (a re-run after a partial publish) and is an error otherwise.
#
# `drop` drops the deployments recorded in the state directory. The workflow
# calls it when a later step of the publish job fails.
#
# API: https://central.sonatype.org/publish/publish-portal-api/
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

API="${CENTRAL_API_URL:-https://central.sonatype.com/api/v1/publisher}"
REPO="${CENTRAL_REPO_URL:-https://repo1.maven.org/maven2}"
PORTAL="https://central.sonatype.com/publishing/deployments"
POLL="${CENTRAL_POLL_SECONDS:-10}"
WAIT="${CENTRAL_WAIT_SECONDS:-1200}"
UUID_RE='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

need curl jq base64

auth_file=""
auth_setup() {
  : "${CENTRAL_USERNAME:?}" "${CENTRAL_PASSWORD:?}"
  local token
  token="$(printf '%s:%s' "$CENTRAL_USERNAME" "$CENTRAL_PASSWORD" | base64 | tr -d '\n')"
  # The encoded token is a secret in its own right and is not masked by the
  # runner unless it is told to.
  echo "::add-mask::$token"
  auth_file="$(mktemp)"
  trap 'rm -f "$auth_file"' EXIT
  printf 'Authorization: Bearer %s\n' "$token" > "$auth_file"
}

# api <method> <url> <response body file> [curl options]: prints the HTTP
# status, or 000 when there was no HTTP response. The token goes to curl
# through a header file, not the command line, and only ever to $API.
api() {
  local method="$1" url="$2" body="$3" code
  shift 3
  case "$url" in "$API"/*) ;; *) die "refusing to send the Portal token to $url" ;; esac
  : > "$body"
  code="$(curl -sS -X "$method" -H "@$auth_file" -o "$body" -w '%{http_code}' \
    --connect-timeout 30 "$@" "$url")" || code=000
  printf '%s\n' "$code"
}

excerpt() { head -c 600 "$1" 2> /dev/null | tr '\n' ' '; }

# drop_all: drop every deployment this run created. Returns non-zero if any
# could not be dropped.
drop_all() {
  local version id code rc=0
  if [ ! -s "$state/deployments.txt" ]; then
    echo "No deployment from this run is waiting in the Portal."
    return 0
  fi
  while read -r version id <&5; do
    code="$(api DELETE "$API/deployment/$id" "$state/drop.body" --max-time 120)"
    if [ "$code" = 204 ]; then
      echo "Dropped deployment $id ($version)."
    else
      rc=1
      echo "::error::Could not drop deployment $id ($version): HTTP $code $(excerpt "$state/drop.body"). Drop it by hand at $PORTAL before releasing again; do not publish it." >&2
    fi
  done 5< "$state/deployments.txt"
  mv "$state/deployments.txt" "$state/deployments.dropped"
  return "$rc"
}

fail() {
  echo "::error::$*" >&2
  drop_all || true
  exit 1
}

# already_published <version>: 0 when the version is on Maven Central with
# exactly the staged files, 1 when it is not there. Anything else is fatal.
already_published() {
  local version="$1" dir name code remote
  dir="$REPO/$GROUP_PATH/$version"
  code="$(curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 30 --max-time 120 \
    "$dir/$ARTIFACT_ID-$version.pom")" || code=000
  case "$code" in
    404) return 1 ;;
    200) ;;
    *) die "could not tell whether $version is already on Maven Central (HTTP $code from $dir); nothing was uploaded" ;;
  esac
  for name in $(artifact_names "$version"); do
    remote="$(curl -fsS --connect-timeout 30 --max-time 120 "$dir/$name.sha1" | tr -d '[:space:]')" \
      || die "$version is on Maven Central but $name.sha1 could not be read; nothing was uploaded"
    [ "$remote" = "$(hash_of sha1 "$staging/bundles/$version/$GROUP_PATH/$version/$name")" ] \
      || die "$version is already on Maven Central with different content ($name). A published version cannot be replaced: release a new version instead. Nothing was uploaded."
  done
  return 0
}

# wait_validated <version> <deployment id>
wait_validated() {
  local version="$1" id="$2" code deployment_state deadline errors=0
  deadline=$(($(date +%s) + WAIT))
  while :; do
    code="$(api POST "$API/status?id=$id" "$state/status.body" --max-time 60)"
    deployment_state=""
    if [ "$code" = 200 ]; then
      deployment_state="$(jq -r '.deploymentState // empty' "$state/status.body" 2> /dev/null || true)"
      errors=0
    else
      errors=$((errors + 1))
      [ "$errors" -lt 6 ] || fail "status of deployment $id ($version) could not be read six times in a row (last: HTTP $code $(excerpt "$state/status.body"))"
    fi
    case "$deployment_state" in
      VALIDATED)
        echo "$version: VALIDATED (deployment $id)"
        return 0 ;;
      PUBLISHING | PUBLISHED)
        warn "$version: deployment $id is already $deployment_state; someone pressed Publish while this job was running"
        return 0 ;;
      FAILED)
        echo "Validation errors for $version:" >&2
        jq '.errors // .' "$state/status.body" >&2 || cat "$state/status.body" >&2
        fail "Maven Central rejected the bundle for $version (deployment $id); details above" ;;
      *) echo "$version: ${deployment_state:-no status yet} (deployment $id)" ;;
    esac
    [ "$(date +%s)" -lt "$deadline" ] || fail "deployment $id ($version) was not validated within $WAIT seconds (last state: ${deployment_state:-unknown})"
    sleep "$POLL"
  done
}

cmd="${1:-}"
case "$cmd" in
  drop)
    state="${2:?usage: central-upload.sh drop <state dir>}"
    auth_setup
    drop_all
    exit $?
    ;;
  upload) ;;
  *) die "usage: central-upload.sh upload <bundle dir> <staging dir> <state dir> | drop <state dir>" ;;
esac

bundles="${2:?}"
staging="${3:?}"
state="${4:?}"
: "${CENTRAL_USERNAME:?}" "${CENTRAL_PASSWORD:?}"
[ -s "$staging/versions.txt" ] || die "$staging lists no versions"
mkdir -p "$state"
[ ! -s "$state/deployments.txt" ] || die "$state already records deployments from this job; refusing to upload twice"
: > "$state/deployments.txt"
: > "$state/result.txt"

# Decide everything that can be decided before the first upload.
to_upload=""
while IFS= read -r version <&4; do
  [ -f "$bundles/$ARTIFACT_ID-$version-bundle.zip" ] || die "no bundle for $version; nothing was uploaded"
  if already_published "$version"; then
    echo "$version is already on Maven Central with identical files; not uploading it again."
    echo "$version - ALREADY_PUBLISHED" >> "$state/result.txt"
  else
    to_upload="$to_upload $version"
  fi
done 4< "$staging/versions.txt"

auth_setup
for version in $to_upload; do
  zip="$bundles/$ARTIFACT_ID-$version-bundle.zip"
  echo "Uploading $(basename "$zip") ..."
  code="$(api POST "$API/upload?name=$ARTIFACT_ID-$version&publishingType=USER_MANAGED" "$state/upload.body" \
    --max-time 900 -F "bundle=@$zip;type=application/octet-stream")"
  case "$code" in
    200 | 201) ;;
    000) fail "the upload of $version did not complete. It may or may not have reached the Portal: look for a deployment named $ARTIFACT_ID-$version at $PORTAL and drop it if it is there." ;;
    *) fail "the upload of $version was refused: HTTP $code $(excerpt "$state/upload.body")" ;;
  esac
  id="$(tr -d '[:space:]' < "$state/upload.body")"
  [[ "$id" =~ $UUID_RE ]] \
    || fail "the upload of $version returned an unexpected response ($(excerpt "$state/upload.body")); look for a deployment named $ARTIFACT_ID-$version at $PORTAL and drop it"
  echo "$version $id" >> "$state/deployments.txt"
  echo "Uploaded $version as deployment $id"
  wait_validated "$version" "$id"
  echo "$version $id VALIDATED" >> "$state/result.txt"
done

{
  echo "### Maven Central"
  echo
  echo "| Version | Deployment | State |"
  echo "|---|---|---|"
  while read -r version id deployment_state; do
    echo "| \`$version\` | \`$id\` | $deployment_state |"
  done < "$state/result.txt"
  echo
  if [ -n "$to_upload" ]; then
    echo "**Not public yet.** Each VALIDATED deployment waits at <$PORTAL> until someone presses **Publish**. Publish all of them, or **Drop** all of them to abandon the release. Publishing cannot be undone."
  fi
} | summary
