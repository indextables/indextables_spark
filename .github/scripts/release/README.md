# Release scripts

The scripts behind `.github/workflows/release.yml`, and the runbook for a
release.

## What a release publishes

For a tag `v<BASE>` (for example `v0.6.0`), one artifact version per Spark
profile, `<BASE>_spark_<SPARKVER>`:

| | |
|---|---|
| Maven Central | `io.indextables:indextables_spark:<BASE>_spark_<SPARKVER>`: the pom, the jar, `-sources`, `-javadoc`, `-linux-x86_64-shaded` and `-jar-with-dependencies`, each with a signature and md5, sha1, sha256 and sha512 checksums |
| GitHub Release for the tag | the `-linux-x86_64-shaded` jar of each version |

`<SPARKVER>` is the `spark.version` the profile sets in `pom.xml` at the
tagged commit, so the suffix names the Spark version the jar was compiled
against. The file set is the one on Maven Central for `0.6.0-rc2`; the
allow-list is `ARTIFACT_SUFFIXES` in `lib.sh`.

## Jobs and trust

```
plan ─► native ─► build (x3) ─► assemble ─► rehearse ─► publish
        └──── run project code ────┘   └── run these scripts only ──┘
        no secrets, read-only token                    environment: release
```

| Job | Runs | Has |
|---|---|---|
| `plan` | `plan.sh` | read-only token |
| `native` | `build-native.sh`: tantivy4java's Maven and cargo build, from pinned commits, no cache | read-only token |
| `build` | `build-artifacts.sh`: this project's Maven build at the tagged commit, with the release profile and signing switched off | read-only token |
| `assemble` | `assemble.sh`: checks the build outputs, lays out what will be uploaded, writes a manifest of digests | read-only token |
| `rehearse` | `verify-staging.sh`, `signing-key.sh generate`, `sign-bundles.sh`, `check-bundles.sh` with a throwaway key | read-only token |
| `publish` | `verify-staging.sh`, `signing-key.sh import`, `sign-bundles.sh`, `check-bundles.sh`, `central-upload.sh`, `github-release.sh` | `release` environment, the four secrets, `contents: write` |

What executes in `publish`, beside the signing key and the Central token:

* `actions/checkout` (sparse, this directory, from the commit the workflow
  was started on) and `actions/download-artifact`, both pinned to a commit;
* the scripts in this directory, from that same commit;
* `bash`, coreutils, `gpg`, `zip`, `unzip`, `curl`, `jq` and `gh` from the
  runner image.

No Maven, no JVM, no cargo, and nothing from the tagged commit. The files it
signs were produced by jobs that did run project code; it signs exactly the
files in the manifest whose digest `assemble` reported as a job output, and
`assemble` and `verify-staging.sh` accept only the expected coordinates and
file names. A build cannot add another artifact, another version or another
`io.indextables` coordinate to what gets signed. What a build can still do is
put different bytes inside the six expected files: that is what the tagged
commit's review, the pins and the digests in the run summary are for.

Each secret is passed to the one step that needs it. The key is imported into
a keyring under the runner's temporary directory, and removed before the
first request to Maven Central or GitHub.

## Native inputs

`native-pins.txt` lists, per tantivy4java version, the tantivy4java commit
and the quickwit and tantivy fork commits a release may build. The build
fetches that tantivy4java commit by id, refuses to continue if the tag
`v<version>` points anywhere else, and checks the fork commits against the
`Cargo.lock` at that commit. It also fails if the build modifies `Cargo.lock`
(cargo does that when it cannot use the locked versions), if a dependency
points to a path outside the checkout, or if a cargo config overrides
dependency sources. protoc is the same pinned release and digest as in
`scripts/setup.sh`. Rust comes from the runner image and is recorded, not
pinned.

The resolved commits and tool versions are written to the `native` job
summary, the `assemble` job summary and the release notes.

When `pom.xml` moves to a new tantivy4java version, add its line in the same
pull request:

```
bash .github/scripts/release/build-native.sh print-pin <version>
```

The `Release Scripts` workflow fails on a pull request that changes the
version without adding a pin.

`scripts/setup.sh` is not used by the release. It still clones the quickwit
fork at its default branch for developer and CI builds; current tantivy4java
versions take both forks from `Cargo.lock` and ignore that clone.

## Settings this depends on

These are repository settings, not files, and the workflow cannot enforce
them on itself:

1. Environment `release`: required reviewer set; deployment branches and tags
   limited to **`main` only**. With `v*` tags also admitted, a workflow file
   on any `v*` tag can ask for this environment (see below).
2. `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`, `CENTRAL_USERNAME` and
   `CENTRAL_PASSWORD` stored as secrets **of that environment**, and the
   repository-level secrets of the same names deleted. While a
   repository-level copy exists, every workflow in the repository can read it
   and none of the separation above applies.
3. `main` protected, so that the workflow file and these scripts change only
   through reviewed pull requests.

## Tag push or dispatch

The workflow is started by `workflow_dispatch` only. A real release must be
dispatched from `main`, with the tag as input; the tag's commit must be on
`main`.

* **Dispatch from `main`** (implemented): the workflow file and the scripts
  that run beside the secrets are the reviewed ones on `main`. The tag only
  selects which source is built, by the unprivileged jobs. Left open: whoever
  can dispatch workflows chooses the tag (bounded by "must be on `main`") and
  whoever approves the environment is the last check; and the guarantee rests
  on settings 1 and 3 above.
* **Tag push** (not enabled): pushing `v*` runs the workflow file and scripts
  *of the tagged commit*. Anyone who can push a `v*` tag, on any commit,
  merged or not, then decides what the job holding the secrets executes; the
  only control left is the reviewer, who approves a run without seeing which
  workflow file it uses. To enable it anyway, add `push: tags: ['v*']` under
  `on:` in `release.yml` (`plan.sh` already handles the event) and admit `v*`
  tags in the environment.

Creating a GitHub Release (and with it the tag) in the browser therefore does
not publish anything. Do that first if you want hand-written notes, then
dispatch the workflow: it attaches the jars to the existing release and adds
a "Build inputs" block to its notes without touching the rest.

## Runbook

### Dry run

Actions → Release → Run workflow → branch `main`, `tag` = the tag, leave
`dry-run` ticked. Any existing `v*` tag on `main` whose tantivy4java version
is pinned works, including one that is already released (`v0.6.0-rc2`).

A dry run builds everything, assembles it, and signs, bundles and checks it
with a throwaway key. It does not start the `publish` job: it asks for no
approval, reads no secret, uploads nothing, creates no release. If a run you
meant as a dry run shows a pending approval for `release`, it is not a dry
run: reject it.

What to look at:

* **Plan** summary: "Dry run for …", the tag and its commit.
* **native** summary: the tantivy4java, quickwit and tantivy commits.
* **Assemble** summary: three versions with the Spark versions you expect;
  the manifest digest; the digests of all files.
* **Rehearse** log: each bundle lists 36 entries and passes.

A dry run may also be dispatched from a branch, to try a change to the
workflow or these scripts before it is merged.

### Release

1. Dispatch from `main` with `dry-run` unticked.
2. Wait for `rehearse` to pass, read the Plan and Assemble summaries, then
   approve the `release` environment. Up to here nothing has left the run;
   rejecting the approval ends it.
3. `publish` signs, uploads one bundle per version to the Central Portal as
   `USER_MANAGED`, waits until each is `VALIDATED`, then creates or updates
   the GitHub Release.
4. Open <https://central.sonatype.com/publishing/deployments>. There is one
   deployment per version, named `indextables_spark-<version>`. Press
   **Publish** on each. This is the step that cannot be undone.

### Aborting

| When | How | What is left behind |
|---|---|---|
| Before approval | Reject the approval, or cancel the run | Nothing |
| While `publish` runs | Cancel the run | The job drops the deployments it uploaded; check the Portal. The GitHub Release may exist with some jars |
| After `publish`, before pressing Publish | **Drop** every deployment in the Portal; delete or edit the GitHub Release by hand | Nothing on Maven Central |
| After pressing Publish | Not possible. Publish the remaining versions, and release a new version if this one is wrong | |

### When `publish` fails

`central-upload.sh` handles one version at a time and drops what it uploaded
when anything fails, so a failed job normally leaves nothing in the Portal;
if a drop is refused the log says which deployment to drop by hand. Then use
**Re-run failed jobs**: the re-run signs and uploads the same staged files
(same digests), not a rebuild.

If some versions were already published by hand and others were not, re-run
the failed job as well: a version that is already on Maven Central with
identical files is skipped, and one with different files stops the run.

## What the tests cover, and what they cannot

`bash .github/scripts/release/test.sh` runs offline: `mvn`, `curl` and `gh`
are stubs. It covers the decision logic of every script (including every
refusal), the layout of the bundles, the upload state machine (rejection,
timeouts, drops, re-runs) and the shape of `release.yml` (which job has
secrets, the dry-run guards, pins). On a pull request it runs on the runner
image with the real `gpg`, generating, exporting, importing and signing with
a throwaway key.

Not covered by any test, and seen for the first time in a real run:

* the Maven and cargo builds themselves (first seen in a dry run);
* artifacts passing between jobs (first seen in a dry run);
* importing the real key and using the real passphrase;
* the Central Portal accepting the token, the bundle and the signatures;
* `gh` creating or updating the release with the job's token.

The last three happen in `publish` only. If one of them fails, nothing has
been published: the upload is `USER_MANAGED`, and a failed job drops it.
