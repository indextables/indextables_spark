# Automated pull request review: scripts

Used by `.github/workflows/claude-review.yml`. The workflow header describes the
trust model; this file describes the moving parts and how to test them.

## Flow

| Job | Step | Script | Holds |
|-----|------|--------|-------|
| `review` | Fetch the pull request diff | `fetch-diff.sh` | read-only token |
| `review` | Review with Claude | (action) | Claude credential, read-only token |
| `verdict` | Validate and render | `report.sh`, `validate.jq`, `render.jq` | nothing sensitive |
| `verdict` | Post or update the comment | `post-comment.sh` | token with `pull-requests: write` |
| `verdict` | Enforce the verdict | (inline) | nothing |

The pull request is never checked out. `fetch-diff.sh` writes its diff to
`$RUNNER_TEMP/claude-review/pr.diff`; the reviewer reads that file and a checkout
of the default branch, with the `Read`, `Grep` and `Glob` tools only.

## Outcomes

The `verdict` job is green only for `pass`.

| `verdict` | When | Job |
|-----------|------|-----|
| `pass` | Schema-valid output, verdict `pass`, no `critical` or `high` finding | green |
| `fail` | Schema-valid output with verdict `fail`, or `pass` with a `critical`/`high` finding | red |
| `none` | Everything else: see the reason codes | red, "did not complete" |

`verdict.json` (artifact `claude-review-verdict`) carries `pr`, `head_sha`,
`verdict`, `reason`, the counts and the run URL. It contains no reviewer text.

Reason codes for `none`:

| Code | Meaning |
|------|---------|
| `credential_unavailable` | `CLAUDE_CODE_OAUTH_TOKEN` did not reach the run |
| `diff_too_large` | Over `MAX_DIFF_BYTES` / `MAX_DIFF_LINES`, or refused by the API as too large |
| `diff_line_too_long` | A line exceeds `MAX_LINE_BYTES`; the reviewer would see it cut off |
| `diff_incomplete` | Fewer `diff --git` headers than the pull request's changed files |
| `diff_unreadable` | NUL bytes in the diff |
| `empty_diff` | Nothing to review |
| `head_changed` | New commits arrived during the run; a newer run covers them |
| `pr_not_open` | Closed or merged meanwhile |
| `diff_fetch_failed`, `pr_fetch_failed` | API errors |
| `bad_input` | Event payload values not in the expected shape |
| `precheck_missing` | The review job reported no precheck result |
| `review_failed` | The review step failed, timed out or was cancelled |
| `no_output`, `invalid_output` | No structured output, or output that fails `validate.jq` |

## Changing things

- **Who gets reviewed:** the JSON array in the `review` job's `if:`.
- **Output shape:** the `--json-schema` in the workflow and `validate.jq` must
  agree; `test.sh` compares their limits and enumerations.
- **Size limits:** defaults at the top of `fetch-diff.sh`. Of the last 60 pull
  requests when this was written, the largest diff was about 200 KB and 4,100
  lines, with no line over 805 bytes.
- **Reviewer tools:** do not add `--allowedTools`. A bare `Read` there approves
  reading any path on the runner, which includes the reviewer's own process
  environment.

## Tests

```bash
bash .github/scripts/claude-review/test.sh
```

Offline: `gh` is stubbed. Covers accepted and rejected diffs, every reason code,
valid and invalid reviewer output, hostile text in findings, comment creation
and update, and static checks of the workflow file (trigger, permissions, gate,
tool list, schema). Needs `jq`; the workflow checks also need `ruby`.

`.github/workflows/claude-review-scripts.yml` runs the same tests on a hosted
runner for every pull request that touches these files.

The review workflow itself runs only from the default branch, so a pull request
that changes it or these scripts is still reviewed by the version already
merged. The tests are the only check such a change gets before it is live.
