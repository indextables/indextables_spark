#!/usr/bin/env bash
# Fetch a pull request's diff through the REST API into a directory outside the
# workspace, and decide whether it is fit for automated review.
#
# The pull request is never checked out: its content reaches the reviewer only
# as this one data file.
#
# "Not reviewable" is not a script failure. It is reported through the step
# outputs `ready` (true|false) and `reason` (a fixed code from the list in
# report.sh), so that the verdict job can turn it into an explicit "review did
# not complete" instead of a pass. The script exits non-zero only on an
# internal error, which also ends without a verdict.
#
# Required environment:
#   GH_TOKEN         read-only token used by `gh api`
#   REPO             owner/name
#   PR_NUMBER        pull request number from the event payload
#   HEAD_SHA         head commit from the event payload; the diff must be of it
#   OUT_DIR          destination directory, outside GITHUB_WORKSPACE
#   HAS_CREDENTIAL   "true" when the reviewer's credential reached this run
#   GITHUB_OUTPUT    step output file
# Optional limits (bytes / lines):
#   MAX_DIFF_BYTES, MAX_DIFF_LINES, MAX_LINE_BYTES
set -euo pipefail

: "${REPO:?}" "${PR_NUMBER:?}" "${HEAD_SHA:?}" "${OUT_DIR:?}" "${GITHUB_OUTPUT:?}"

# Beyond these the diff is not reviewed at all rather than reviewed in part.
MAX_DIFF_BYTES="${MAX_DIFF_BYTES:-512000}"
MAX_DIFF_LINES="${MAX_DIFF_LINES:-12000}"
# The reviewer's file-reading tool cuts lines off at 2000 characters. A longer
# line would be only partly visible, so it is treated like a truncated diff.
MAX_LINE_BYTES="${MAX_LINE_BYTES:-2000}"

diff_file="$OUT_DIR/pr.diff"
tmp_file="$OUT_DIR/pr.diff.partial"
pr_json="$OUT_DIR/pr.json"
err_file="$OUT_DIR/api.err"

finish() { # finish <ready:true|false> <reason-code>
  rm -f "$tmp_file" "$pr_json" "$err_file"
  if [ "$1" != "true" ]; then
    rm -f "$diff_file"
    echo "::warning title=Review precheck::No review will run for this commit: $2"
  fi
  printf 'ready=%s\nreason=%s\n' "$1" "$2" >> "$GITHUB_OUTPUT"
  exit 0
}

# Values from the event payload are still checked before they go into a URL.
case "$PR_NUMBER" in ''|*[!0-9]*) finish false bad_input ;; esac
if ! printf '%s' "$HEAD_SHA" | grep -Eq '^[0-9a-f]{40}$'; then finish false bad_input; fi
if ! printf '%s' "$REPO" | grep -Eq '^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$'; then finish false bad_input; fi

# The diff must not land in the checkout the reviewer treats as the base branch.
if [ -n "${GITHUB_WORKSPACE:-}" ]; then
  case "$OUT_DIR/" in "$GITHUB_WORKSPACE"/*) echo "OUT_DIR is inside the workspace" >&2; exit 1 ;; esac
fi
mkdir -p "$OUT_DIR"
rm -f "$diff_file" "$tmp_file" "$pr_json" "$err_file"

# Checked before any API call so the cause is named, rather than surfacing
# later as an authentication error from the review step.
if [ "${HAS_CREDENTIAL:-false}" != "true" ]; then finish false credential_unavailable; fi

# One request for the whole diff. The API answers very large diffs with an
# error (HTTP 406) instead of a partial body; any failure ends without a verdict.
if ! gh api -H "Accept: application/vnd.github.diff" "repos/$REPO/pulls/$PR_NUMBER" > "$tmp_file" 2> "$err_file"; then
  sed -e 's/^/gh: /' "$err_file" >&2 || true
  if grep -Eqi 'HTTP 406|too large|exceeded the maximum' "$err_file"; then
    finish false diff_too_large
  fi
  finish false diff_fetch_failed
fi

# Fetched after the diff: if the head is still the commit this run was started
# for, the diff above is the diff of that commit.
if ! gh api "repos/$REPO/pulls/$PR_NUMBER" > "$pr_json" 2> "$err_file"; then
  sed -e 's/^/gh: /' "$err_file" >&2 || true
  finish false pr_fetch_failed
fi
if ! api_head=$(jq -er '.head.sha | strings' "$pr_json") ||
   ! api_state=$(jq -er '.state | strings' "$pr_json") ||
   ! api_files=$(jq -er '.changed_files | numbers | floor' "$pr_json"); then
  finish false pr_fetch_failed
fi
if [ "$api_state" != "open" ]; then finish false pr_not_open; fi
if [ "$api_head" != "$HEAD_SHA" ]; then finish false head_changed; fi

bytes=$(LC_ALL=C wc -c < "$tmp_file" | tr -d '[:space:]')
lines=$(LC_ALL=C wc -l < "$tmp_file" | tr -d '[:space:]')
echo "diff: $bytes bytes, $lines lines; pull request reports $api_files changed file(s)"

if [ "$bytes" -eq 0 ] || [ "$api_files" -eq 0 ]; then finish false empty_diff; fi
if [ "$bytes" -gt "$MAX_DIFF_BYTES" ] || [ "$lines" -gt "$MAX_DIFF_LINES" ]; then finish false diff_too_large; fi

# A text diff has no NUL bytes; one would also make the text tools below unreliable.
clean_bytes=$(LC_ALL=C tr -d '\000' < "$tmp_file" | LC_ALL=C wc -c | tr -d '[:space:]')
if [ "$clean_bytes" -ne "$bytes" ]; then finish false diff_unreadable; fi

# Completeness: one "diff --git" header per changed file. Inside hunks every
# line starts with '+', '-', ' ' or '\', so only real headers match at column 0.
headers=$(LC_ALL=C grep -a -c '^diff --git ' "$tmp_file" || true)
if [ "$headers" -ne "$api_files" ]; then
  echo "diff has $headers file header(s), expected $api_files" >&2
  finish false diff_incomplete
fi

longest=$(LC_ALL=C awk '{ n = length($0); if (n > m) m = n } END { print m + 0 }' "$tmp_file")
if [ "$longest" -gt "$MAX_LINE_BYTES" ]; then
  echo "longest diff line is $longest bytes" >&2
  finish false diff_line_too_long
fi

mv "$tmp_file" "$diff_file"
chmod 0444 "$diff_file"
echo "Diff written to $diff_file"
finish true ok
