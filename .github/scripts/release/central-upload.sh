#!/usr/bin/env bash
# Upload the checked bundles to the Maven Central Portal and wait until each
# one is validated. Nothing is published by this script.
#
#   central-upload.sh upload <bundle dir> <staging dir> <state dir>
#   central-upload.sh drop <state dir>
#
# Environment:
#   CENTRAL_USERNAME, CENTRAL_PASSWORD   the Portal user token
#   GITHUB_RUN_ID, GITHUB_RUN_ATTEMPT    set by the runner; part of each
#                                        deployment's name
#
# Publishing type is USER_MANAGED, as it has been so far: an upload that
# passes validation waits in the Portal as VALIDATED until a person presses
# Publish (or Drop) at https://central.sonatype.com/publishing/deployments.
# That page is where a release is made public, and where it can still be
# abandoned.
#
# `upload` handles one version at a time: upload, then wait for VALIDATED,
# then the next. Each deployment is named
# indextables_spark-<version>-run<run id>-<attempt>, and its id is written to
# the log and the job summary as soon as the Portal returns it, so the
# deployments of this job can be told apart from any other in the Portal. A
# version that is already on Maven Central is skipped when its files are
# identical to the staged ones (a re-run after a partial publish) and is an
# error otherwise.
#
# When anything goes wrong, the deployments this job created are dropped. The
# Portal accepts a drop only for a deployment that is VALIDATED or FAILED, so
# a deployment that is still being validated is first polled until it settles
# (for a bounded time). What cannot be dropped stays recorded in the state
# directory.
#
# `drop` does the same for whatever is still recorded there. The workflow
# runs it as the last step whenever the job failed or was cancelled, also
# when the upload step itself was killed in the middle. If a deployment still
# cannot be dropped, or an upload was cut off before the Portal answered, the
# step fails and the job summary says exactly what to drop by hand.
#
# API: https://central.sonatype.org/publish/publish-portal-api/
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

API="$CENTRAL_API"
POLL="$(test_setting CENTRAL_POLL_SECONDS 10)"
WAIT="$(test_setting CENTRAL_WAIT_SECONDS 1200)"
# How long a drop waits for a deployment that is still being validated. Kept
# well under five minutes: that is what a cancelled job gets to finish its
# remaining steps.
DROP_WAIT="$(test_setting CENTRAL_DROP_WAIT_SECONDS 180)"
UUID_RE='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

run_id="${GITHUB_RUN_ID:-0}"
run_attempt="${GITHUB_RUN_ATTEMPT:-0}"
[[ "$run_id" =~ ^[0-9]+$ ]] && [[ "$run_attempt" =~ ^[0-9]+$ ]] || die "GITHUB_RUN_ID and GITHUB_RUN_ATTEMPT must be numbers"
RUN_LABEL="run${run_id}-${run_attempt}"

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

# deployment_state <id>: the deployment's state, or nothing if it could not
# be read.
deployment_state() {
  local code
  code="$(api POST "$API/status?id=$1" "$state/status.body" --max-time 60)"
  if [ "$code" = 200 ]; then jq -r '.deploymentState // empty' "$state/status.body" 2> /dev/null || true; fi
}

# settle <id>: poll until the deployment is in a state the Portal will not
# move on from by itself (VALIDATED, FAILED, PUBLISHING, PUBLISHED), for at
# most DROP_WAIT seconds. Prints the last state seen, or UNKNOWN.
settle() {
  local s deadline
  deadline=$(($(date +%s) + DROP_WAIT))
  while :; do
    s="$(deployment_state "$1")"
    case "$s" in VALIDATED | FAILED | PUBLISHING | PUBLISHED) break ;; esac
    [ "$(date +%s)" -lt "$deadline" ] || break
    sleep "$POLL"
  done
  printf '%s\n' "${s:-UNKNOWN}"
}

# drop_all <last|retry-later>: drop every deployment still recorded for this
# job. Returns non-zero if anything is left for a person to deal with. With
# "last" (the final step of the job) what is left is written to the summary.
drop_all() {
  local final="$1" version id name s code rc=0
  : > "$state/left.tmp"
  if [ -s "$state/deployments.txt" ]; then
    while read -r version id name <&5; do
      s="$(settle "$id")"
      case "$s" in
        PUBLISHING | PUBLISHED)
          # Not kept for another attempt (it never can be dropped), but
          # remembered so that every later call reports it.
          echo "$version $id $name $s" >> "$state/published.txt"
          continue ;;
      esac
      code="$(api DELETE "$API/deployment/$id" "$state/drop.body" --max-time 120)"
      if [ "$code" = 204 ]; then
        echo "Dropped deployment $id ($name)."
        echo "$version $id $name" >> "$state/dropped.txt"
      else
        rc=1
        echo "$version $id $name" >> "$state/keep.tmp"
        echo "| \`$version\` | \`$id\` | \`$name\` | $s; the drop was refused (HTTP $code). **Drop it by hand. Do not publish it.** |" >> "$state/left.tmp"
        echo "::error::Could not drop deployment $id ($name), state $s: HTTP $code $(excerpt "$state/drop.body")" >&2
      fi
    done 5< "$state/deployments.txt"
  fi
  # Keep only what is still in the Portal, so that a later call tries again.
  if [ -s "$state/keep.tmp" ]; then mv "$state/keep.tmp" "$state/deployments.txt"; else rm -f "$state/keep.tmp"; : > "$state/deployments.txt"; fi

  if [ -s "$state/published.txt" ]; then
    rc=1
    while read -r version id name s <&5; do
      echo "| \`$version\` | \`$id\` | \`$name\` | $s: it can no longer be dropped. Someone pressed Publish; this version is or will be on Maven Central although the job failed. |" >> "$state/left.tmp"
      echo "::error::Deployment $id ($name) is $s and can no longer be dropped." >&2
    done 5< "$state/published.txt"
  fi

  if [ -s "$state/uploading.txt" ]; then
    rc=1
    read -r version name < "$state/uploading.txt"
    echo "| \`$version\` | unknown | \`$name\` | The upload was cut off before the Portal answered. **If a deployment with this name exists, drop it by hand. Do not publish it.** |" >> "$state/left.tmp"
    echo "::error::The upload named $name was cut off before the Portal answered; it may or may not exist there." >&2
  fi

  if [ "$rc" = 0 ]; then
    echo "No deployment from this job is left in the Portal."
  elif [ "$final" = last ]; then
    {
      echo "### Action needed: deployments left in the Central Portal"
      echo
      echo "This job failed and could not clean up everything it uploaded. Open <$CENTRAL_PORTAL> and deal with each line below before releasing again."
      echo
      echo "| Version | Deployment id | Deployment name | What to do |"
      echo "|---|---|---|---|"
      cat "$state/left.tmp"
    } | summary
  else
    echo "Some deployments could not be dropped yet (above); the last step of this job tries again."
  fi
  rm -f "$state/left.tmp"
  return "$rc"
}

fail() {
  echo "::error::$*" >&2
  drop_all retry-later || true
  exit 1
}

# already_published <version>: 0 when the version is on Maven Central with
# exactly the staged files, 1 when it is not there. Anything else is fatal.
already_published() {
  local version="$1" dir name code remote
  dir="$CENTRAL_REPO/$GROUP_PATH/$version"
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
  local version="$1" id="$2" code s deadline errors=0
  deadline=$(($(date +%s) + WAIT))
  while :; do
    code="$(api POST "$API/status?id=$id" "$state/status.body" --max-time 60)"
    s=""
    if [ "$code" = 200 ]; then
      s="$(jq -r '.deploymentState // empty' "$state/status.body" 2> /dev/null || true)"
      errors=0
    else
      errors=$((errors + 1))
      [ "$errors" -lt 6 ] || fail "status of deployment $id ($version) could not be read six times in a row (last: HTTP $code $(excerpt "$state/status.body"))"
    fi
    case "$s" in
      VALIDATED)
        echo "$version: VALIDATED (deployment $id)"
        return 0 ;;
      PUBLISHING | PUBLISHED)
        warn "$version: deployment $id is already $s; someone pressed Publish while this job was running"
        return 0 ;;
      FAILED)
        echo "Validation errors for $version:" >&2
        jq '.errors // .' "$state/status.body" >&2 || cat "$state/status.body" >&2
        fail "Maven Central rejected the bundle for $version (deployment $id); details above" ;;
      *) echo "$version: ${s:-no status yet} (deployment $id)" ;;
    esac
    [ "$(date +%s)" -lt "$deadline" ] || fail "deployment $id ($version) was not validated within $WAIT seconds (last state: ${s:-unknown})"
    sleep "$POLL"
  done
}

cmd="${1:-}"
case "$cmd" in
  drop)
    state="${2:?usage: central-upload.sh drop <state dir>}"
    if [ ! -d "$state" ]; then
      echo "The upload step did not start; there is nothing to drop."
      exit 0
    fi
    auth_setup
    drop_all last
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
[ ! -e "$state" ] || die "$state already exists: this job has uploaded before; refusing to upload twice"
mkdir -p "$state"
: > "$state/deployments.txt"
: > "$state/result.txt"

# Decide everything that can be decided before the first upload.
to_upload=""
while IFS= read -r version <&4; do
  [ -f "$bundles/$ARTIFACT_ID-$version-bundle.zip" ] || die "no bundle for $version; nothing was uploaded"
  if already_published "$version"; then
    echo "$version is already on Maven Central with identical files; not uploading it again."
    echo "$version - - ALREADY_PUBLISHED" >> "$state/result.txt"
  else
    to_upload="$to_upload $version"
  fi
done 4< "$staging/versions.txt"

auth_setup
if [ -n "$to_upload" ]; then
  {
    echo "### Uploads to the Central Portal"
    echo
    echo "Written as each upload is accepted, so the ids are here even if the job stops later."
    echo
  } | summary
fi
for version in $to_upload; do
  zip="$bundles/$ARTIFACT_ID-$version-bundle.zip"
  name="$ARTIFACT_ID-$version-$RUN_LABEL"
  echo "Uploading $(basename "$zip") as $name ..."
  # From here until the Portal's answer is recorded, a deployment may exist
  # that this job has no id for. The marker tells the drop step to say so.
  echo "$version $name" > "$state/uploading.txt"
  code="$(api POST "$API/upload?name=$name&publishingType=USER_MANAGED" "$state/upload.body" \
    --max-time 900 -F "bundle=@$zip;type=application/octet-stream")"
  case "$code" in
    200 | 201) ;;
    000) fail "the upload of $version did not complete. It may or may not have reached the Portal: look for a deployment named $name at $CENTRAL_PORTAL and drop it if it is there." ;;
    *)
      rm -f "$state/uploading.txt"
      fail "the upload of $version was refused: HTTP $code $(excerpt "$state/upload.body")" ;;
  esac
  id="$(tr -d '[:space:]' < "$state/upload.body")"
  [[ "$id" =~ $UUID_RE ]] \
    || fail "the upload of $version returned an unexpected response ($(excerpt "$state/upload.body")); look for a deployment named $name at $CENTRAL_PORTAL and drop it"
  echo "$version $id $name" >> "$state/deployments.txt"
  rm -f "$state/uploading.txt"
  echo "Uploaded $version as deployment $id, named $name"
  echo "- Uploaded \`$name\`: deployment \`$id\`" | summary
  wait_validated "$version" "$id"
  echo "$version $id $name VALIDATED" >> "$state/result.txt"
done

{
  echo
  echo "### Maven Central"
  echo
  echo "| Version | Deployment id | Deployment name | State |"
  echo "|---|---|---|---|"
  while read -r version id name s; do
    echo "| \`$version\` | \`$id\` | \`$name\` | $s |"
  done < "$state/result.txt"
  echo
  if [ -n "$to_upload" ]; then
    echo "**Not public yet.** Each VALIDATED deployment waits at <$CENTRAL_PORTAL> until someone presses **Publish**."
    echo
    echo "Publish **only** the deployments whose id is in this table, and all of them (or **Drop** all of them to abandon the release). A deployment for one of these versions with any other id or name comes from a different run: drop it, do not publish it. Publishing cannot be undone."
  fi
} | summary
