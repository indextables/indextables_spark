#!/usr/bin/env bash
# Turn the review job's result into a verdict: validate the reviewer's
# structured output, write verdict.json and the comment body, append the job
# summary, and set the step outputs `verdict` (pass|fail|none) and `reason`.
#
# Fails closed: anything other than a schema-valid output from a review job
# that succeeded yields `none`, never `pass`. This script does not itself fail
# on a missing or bad verdict; the workflow's last step makes the job
# conclusion follow the `verdict` output, after the artifact is uploaded.
#
# Reviewer output is untrusted. It is only ever parsed by jq and written to
# files through render.jq; it is never echoed to the log, never written to
# GITHUB_OUTPUT and never expanded by the shell.
#
# Required environment:
#   REVIEW_RESULT      result of the review job (success|failure|cancelled|skipped)
#   PRECHECK_REASON    `reason` output of fetch-diff.sh ("ok" when a review ran)
#   STRUCTURED_OUTPUT  raw structured output of the review step (may be empty)
#   PR_NUMBER, HEAD_SHA
#   OUT_DIR            where verdict.json and comment.md are written
#   GITHUB_OUTPUT, GITHUB_STEP_SUMMARY
#   GITHUB_SERVER_URL, GITHUB_REPOSITORY, GITHUB_RUN_ID   (set by the runner)
# Optional:
#   COMMENT_MAX_CHARS  cap for the comment body (GitHub rejects > 65536)
set -euo pipefail

: "${PR_NUMBER:?}" "${HEAD_SHA:?}" "${OUT_DIR:?}" "${GITHUB_OUTPUT:?}" "${GITHUB_STEP_SUMMARY:?}"
: "${GITHUB_SERVER_URL:?}" "${GITHUB_REPOSITORY:?}" "${GITHUB_RUN_ID:?}"

here="$(cd "$(dirname "$0")" && pwd)"
REVIEW_RESULT="${REVIEW_RESULT:-}"
PRECHECK_REASON="${PRECHECK_REASON:-}"
STRUCTURED_OUTPUT="${STRUCTURED_OUTPUT:-}"
COMMENT_MAX_CHARS="${COMMENT_MAX_CHARS:-60000}"

case "$PR_NUMBER" in ''|*[!0-9]*) echo "PR_NUMBER is not a number" >&2; exit 1 ;; esac
if ! printf '%s' "$HEAD_SHA" | grep -Eq '^[0-9a-f]{40}$'; then echo "HEAD_SHA is not a commit id" >&2; exit 1; fi

mkdir -p "$OUT_DIR"
run_url="$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID"

# Fixed explanation for every way a run can end without a verdict.
reason_text() {
  case "$1" in
    credential_unavailable) echo "the reviewer's credential was not available to this run. Runs started by Dependabot can be denied repository Actions secrets; see the header of .github/workflows/claude-review.yml." ;;
    diff_too_large)         echo "the diff is larger than the automated reviewer accepts." ;;
    diff_line_too_long)     echo "the diff has a line longer than the reviewer can read in full." ;;
    diff_incomplete)        echo "the diff returned by the API does not cover every changed file." ;;
    diff_unreadable)        echo "the diff contains binary data where text was expected." ;;
    empty_diff)             echo "the pull request has no changes to review." ;;
    head_changed)           echo "the pull request received new commits while this run was in progress; a newer run covers them." ;;
    pr_not_open)            echo "the pull request was no longer open." ;;
    diff_fetch_failed)      echo "the diff could not be fetched from the API." ;;
    pr_fetch_failed)        echo "the pull request could not be read from the API." ;;
    bad_input)              echo "the event payload did not have the expected shape." ;;
    precheck_missing)       echo "the review job reported no precheck result." ;;
    review_failed)          echo "the review step failed, timed out or was cancelled." ;;
    no_output)              echo "the reviewer returned no structured output." ;;
    invalid_output)         echo "the reviewer's output did not match the required schema." ;;
    *)                      echo "unknown reason." ;;
  esac
}

verdict=none
reason=""
note=""
review=null

case "$PRECHECK_REASON" in
  ok|"") ;;
  credential_unavailable|diff_too_large|diff_line_too_long|diff_incomplete|diff_unreadable|empty_diff|head_changed|pr_not_open|diff_fetch_failed|pr_fetch_failed|bad_input)
    reason="$PRECHECK_REASON" ;;
  *) reason=precheck_missing ;;
esac

if [ -z "$reason" ]; then
  if [ "$REVIEW_RESULT" != "success" ]; then
    reason=review_failed
  elif [ "$PRECHECK_REASON" != "ok" ]; then
    reason=precheck_missing
  elif [ -z "$STRUCTURED_OUTPUT" ]; then
    reason=no_output
  elif ! review=$(printf '%s' "$STRUCTURED_OUTPUT" | jq -c -s -f "$here/validate.jq" 2> "$OUT_DIR/validate.err"); then
    # validate.jq's messages are fixed strings; a parse error reports a position only.
    echo "validation failed: $(head -c 300 "$OUT_DIR/validate.err" | tr -c '[:print:]' ' ')" >&2
    review=null
    reason=invalid_output
  fi
  rm -f "$OUT_DIR/validate.err"
fi

model_verdict=""
findings=0
blocking=0
if [ -z "$reason" ]; then
  model_verdict=$(printf '%s' "$review" | jq -r '.verdict')
  findings=$(printf '%s' "$review" | jq -r '.findings | length')
  blocking=$(printf '%s' "$review" | jq -r '[.findings[] | select(.severity == "critical" or .severity == "high")] | length')
  if [ "$model_verdict" = "pass" ] && [ "$blocking" -eq 0 ]; then
    verdict=pass
    reason=reviewer_pass
  elif [ "$model_verdict" = "pass" ]; then
    # A pass that lists blocking findings contradicts itself; the stricter reading wins.
    verdict=fail
    reason=blocking_findings_override
    note="The reviewer answered pass but listed critical or high findings; this is treated as a fail."
  else
    verdict=fail
    reason=reviewer_fail
  fi
fi

jq -n -r -f "$here/render.jq" \
  --arg status "$verdict" \
  --arg reason_text "$(reason_text "$reason")" \
  --arg note "$note" \
  --argjson review "$review" \
  --arg pr "$PR_NUMBER" \
  --arg sha "$HEAD_SHA" \
  --arg run_url "$run_url" \
  --argjson max_chars "$COMMENT_MAX_CHARS" > "$OUT_DIR/comment.md"

jq -n \
  --argjson pr "$PR_NUMBER" \
  --arg head_sha "$HEAD_SHA" \
  --arg verdict "$verdict" \
  --arg reason "$reason" \
  --arg model_verdict "$model_verdict" \
  --argjson findings "$findings" \
  --argjson blocking_findings "$blocking" \
  --arg run_url "$run_url" \
  '{pr: $pr, head_sha: $head_sha, verdict: $verdict, reason: $reason,
    model_verdict: (if $model_verdict == "" then null else $model_verdict end),
    findings: $findings, blocking_findings: $blocking_findings, run_url: $run_url}' > "$OUT_DIR/verdict.json"

# The job summary shows the same text as the comment, without the marker line.
tail -n +2 "$OUT_DIR/comment.md" >> "$GITHUB_STEP_SUMMARY"

printf 'verdict=%s\nreason=%s\n' "$verdict" "$reason" >> "$GITHUB_OUTPUT"
echo "verdict=$verdict reason=$reason findings=$findings blocking=$blocking"
