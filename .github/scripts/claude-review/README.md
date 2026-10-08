# Automated pull request review: scripts

Used by `.github/workflows/claude-review.yml`. The workflow header describes the
trust model; this file describes the moving parts, how to test them, and how to
change the pinned versions.

## Flow

| Job | Step | Script | Holds |
|-----|------|--------|-------|
| `review` | Fetch the pull request diff | `fetch-diff.sh` | read-only token |
| `review` | Review with Claude | (action) | Claude credential (from the `claude` environment), read-only token |
| `verdict` | Validate and render | `report.sh`, `validate.jq`, `render.jq` | nothing sensitive |
| `verdict` | Post or update the comment | `post-comment.sh` | token with `pull-requests: write` |
| `verdict` | Enforce the verdict | (inline) | nothing |

The pull request is never checked out. `fetch-diff.sh` writes its diff to
`$RUNNER_TEMP/claude-review/pr.diff`; the reviewer reads that file and a checkout
of the default branch, with the `Read`, `Grep` and `Glob` tools only.

## Outcomes

The machine-readable result is the conclusion of the **Verdict** check run on
the pull request's head commit. It is `success` only for `pass`.

| `verdict` | When | Verdict check |
|-----------|------|---------------|
| `pass` | Schema-valid output, verdict `pass`, no `critical` or `high` finding | green |
| `fail` | Schema-valid output with verdict `fail`, or `pass` with a `critical`/`high` finding | red |
| `none` | Everything else: see the reason codes | red, "did not complete" |

**A Verdict check that is missing or skipped is not a pass.** The jobs are
skipped for forks, drafts, authors outside the allowlist and pull requests that
do not target the default branch, and nothing runs for events other than the
four trigger types. If Verdict is ever made a required status check, remember
that GitHub counts a skipped job as satisfying the requirement.

A pass is advisory. On a pull request opened by Dependabot the comment and the
job summary say so explicitly: the review checks the version changes against
the project's pinning and major-version rules; it cannot assess what the new
releases contain.

Nothing is uploaded as an artifact. If something later needs to consume the
result, read the check run rather than adding a file.

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

## Pinned versions

Every action is pinned to a full commit id, with the version tag in a trailing
comment, and the runner image is `ubuntu-24.04` rather than `ubuntu-latest`.
`test.sh` fails on a tag or branch reference.

The commit and its version live together on each `uses:` line, for example
`actions/checkout@<commit> # v7.0.1`, so there is no second list to keep in
step. Dependabot updates both parts. To bump one by hand, resolve the tag and
replace the commit and the comment wherever the action is used:

```bash
gh api repos/OWNER/REPO/commits/TAG --jq .sha
```

The Claude action is not an ordinary bump. Each release hard-codes a CLI
version, and the CLI is what enforces the tool list, the permission mode and
the deny rules that keep the reviewer away from its own credential. The
reviewer itself reports a change of that pin as `high`, so such a pull request
does not get a green verdict on its own. Before merging one, re-check the
confinement described below.

## Credential scope

`CLAUDE_CODE_OAUTH_TOKEN` is used by two jobs: `review` here and the job in
`.github/workflows/claude.yml` (the `@claude` mention workflow). Both run in
the `claude` environment with `deployment: false`, which uses the environment
for its secret and its branch rule and creates no deployment records.

The environment is meant to be set up as follows (repository settings, not
code): deployment branches limited to the default branch, no required
reviewers, no wait timer, and the token stored there. A workflow defined on
any other branch then cannot read the token, because GitHub matches the branch
rule against the ref a run executes on, and both `pull_request_target` and
`issue_comment` execute on the default branch.

Two things to keep in mind:

- **The scope is only real once the repository-level secret of the same name
  is deleted.** While it exists, any workflow on any branch can read it, and
  these jobs silently fall back to it.
- If neither copy exists, the review ends as `credential_unavailable` (red)
  and the mention workflow fails with an authentication error. Neither passes.
- Runs started by Dependabot have been seen to receive repository-level
  Actions secrets here. That they also receive the environment's secret is
  expected, since the same runs are treated as ordinary Actions runs, but it
  can only be observed once the repository-level copy is gone: the first
  review Dependabot starts after that either produces a verdict or ends as
  `credential_unavailable`.

`test.sh` fails if any workflow uses the credential outside that environment or
on an event whose definition a pull request controls.

## Read confinement: what has and has not been checked

The reviewer must not be able to read anything outside the checkout and the
diff directory. Three settings provide that: no `--allowedTools` (so nothing is
pre-approved), `--permission-mode dontAsk` (so anything needing approval is
refused), and the deny list in `settings`.

**This was probed with a local CLI (2.1.284) on macOS only.** There, reads,
greps and globs outside the two locations were refused, the deny list held even
with reads deliberately pre-approved, and pre-approving `Read` was shown to
open every path.

The first run on the default branch (2026-10-08, started by Dependabot,
`ubuntu-24.04`, CLI 2.1.292) showed that the workflow works end to end on Linux
with the pinned CLI: the arguments were accepted, the run received the
credential (`Secret source: Actions`), a valid verdict came back and the
comment was posted. **It says nothing about confinement**: the reviewer made no
attempt to read outside its two locations (the run logged zero permission
denials), so nothing was there to be refused.

**Confinement on a Linux runner therefore remains unobserved.** That matters
because on Linux the credential is also present in the reviewer's process
environment under `/proc`, which macOS does not have. `/proc` is covered both
by the working-directory limit and by an explicit deny rule, but neither has
been seen to hold there.

Re-check whenever the Claude action pin, `claude_args` or `settings` change: in
a private scratch repository with a throwaway credential, run the same action
commit, `claude_args` and `settings` and ask the reviewer to read
`/proc/self/environ`, a file planted outside the allowed directories and
`.git/config`. Keep `show_full_output` off so that a read which does succeed is
not printed to the log.

## Changing things

- **Who gets reviewed:** the JSON array in the `review` job's `if:`.
- **Output shape:** the `--json-schema` in the workflow and `validate.jq` must
  agree; `test.sh` compares their limits and enumerations.
- **Size limits:** defaults at the top of `fetch-diff.sh`. Of the last 60 pull
  requests when this was written, the largest diff was about 200 KB and 4,100
  lines, with no line over 805 bytes.
- **Reviewer tools, `claude_args`, `settings`:** do not add `--allowedTools`. A
  bare `Read` there approves reading any path on the runner, which includes the
  reviewer's own process environment. After any change here, re-check the
  confinement (see above).

## Tests

```bash
bash .github/scripts/claude-review/test.sh
```

Offline: `gh` is stubbed. Covers accepted and rejected diffs, every reason code,
valid and invalid reviewer output, hostile text in findings, comment creation
and update, static checks of the workflow file (trigger, permissions, gate,
tool list, schema, pins). Needs `jq`; the checks that parse YAML also need
`ruby`.

`.github/workflows/claude-review-scripts.yml` runs the same tests on a hosted
runner for every pull request that touches these files or any workflow file.

The review workflow itself runs only from the default branch, so a pull request
that changes it or these scripts is still reviewed by the version already
merged. The tests are the only check such a change gets before it is live.
