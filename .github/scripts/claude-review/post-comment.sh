#!/usr/bin/env bash
# Post the rendered review as ONE comment on the pull request: update the
# comment a previous run left, or create it.
#
# Best effort by design. The verdict is carried by the job conclusion, the job
# summary and the verdict.json artifact; a comment that cannot be written
# (for example because the token is read-only for this run) produces a warning
# and must not change the verdict in either direction.
#
# Required environment:
#   GH_TOKEN      token with pull-requests: write
#   REPO          owner/name
#   PR_NUMBER, HEAD_SHA
#   COMMENT_FILE  body written by report.sh; its first line is the marker
set -euo pipefail

: "${REPO:?}" "${PR_NUMBER:?}" "${HEAD_SHA:?}" "${COMMENT_FILE:?}"

MARKER='<!-- claude-review-verdict -->'
# Comments written with the workflow token belong to this account. Matching on
# it keeps a marker pasted by anyone else from being mistaken for ours.
BOT_LOGIN='github-actions[bot]'

warn() { echo "::warning title=Review comment::$1"; exit 0; }

case "$PR_NUMBER" in ''|*[!0-9]*) echo "PR_NUMBER is not a number" >&2; exit 1 ;; esac
[ -s "$COMMENT_FILE" ] || { echo "comment file is missing or empty" >&2; exit 1; }
[ "$(head -n 1 "$COMMENT_FILE")" = "$MARKER" ] || { echo "comment file does not start with the marker" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Do not let a superseded run overwrite the comment of the commit that replaced it.
if ! gh api "repos/$REPO/pulls/$PR_NUMBER" > "$work/pr.json" 2> "$work/err"; then
  warn "could not read the pull request; comment not posted"
fi
if [ "$(jq -r '.head.sha // empty' "$work/pr.json")" != "$HEAD_SHA" ]; then
  warn "the pull request head moved on; comment for the older commit not posted"
fi

if ! gh api --paginate "repos/$REPO/issues/$PR_NUMBER/comments?per_page=100" > "$work/comments.json" 2> "$work/err"; then
  warn "could not list existing comments; comment not posted"
fi
# --paginate emits one JSON array per page; jq reads them as a stream.
if ! jq -r --arg marker "$MARKER" --arg bot "$BOT_LOGIN" \
  '.[] | select(.user.login == $bot) | select((.body // "") | startswith($marker)) | .id' \
  "$work/comments.json" > "$work/ids" 2> "$work/err"; then
  warn "could not parse the comment list; comment not posted"
fi
existing=$(tail -n 1 "$work/ids")

if ! jq -n --rawfile body "$COMMENT_FILE" '{body: $body}' > "$work/payload.json"; then
  warn "could not build the comment payload"
fi

case "$existing" in
  '')
    if ! gh api -X POST "repos/$REPO/issues/$PR_NUMBER/comments" --input "$work/payload.json" > "$work/resp.json" 2> "$work/err"; then
      sed -e 's/^/gh: /' "$work/err" >&2 || true
      warn "could not create the comment"
    fi
    echo "Created review comment." ;;
  *[!0-9]*)
    warn "unexpected comment id; comment not posted" ;;
  *)
    if ! gh api -X PATCH "repos/$REPO/issues/comments/$existing" --input "$work/payload.json" > "$work/resp.json" 2> "$work/err"; then
      sed -e 's/^/gh: /' "$work/err" >&2 || true
      warn "could not update the existing comment"
    fi
    echo "Updated review comment $existing." ;;
esac
