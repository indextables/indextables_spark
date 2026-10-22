# Release scripts

The scripts behind `.github/workflows/release.yml`, how to adopt that
workflow, and the runbook for a release.

## What a release publishes

For a tag `v<BASE>` (for example `v0.6.0`), one artifact version per Spark
profile, `<BASE>_spark_<SPARKVER>`:

| | |
|---|---|
| Maven Central | `io.indextables:indextables_spark:<BASE>_spark_<SPARKVER>`: the pom, the jar, `-sources`, `-javadoc`, `-linux-x86_64-shaded` and `-jar-with-dependencies`, each with a signature and md5, sha1, sha256 and sha512 checksums |
| GitHub Release for the tag | the `-linux-x86_64-shaded` jar of each version |

The file set is the one on Maven Central for `0.6.0-rc2`; the allow-list is
`ARTIFACT_SUFFIXES` in `lib.sh`.

### The Spark version in the name

`<SPARKVER>` is the `spark.version` that the profile sets in `pom.xml` at the
tagged commit, so the name says which Spark the jar was compiled against. It
also means that a dependency update which moves Spark from 3.5.9 to 3.5.10
changes the artifact names of the next release, and anyone who depends on
`…_spark_3.5.9` has to change their build. That must not happen unnoticed:

* `plan` reads the versions from `pom.xml` at the tag and lists them in the
  Plan summary, next to the Spark version last published to Maven Central for
  each Spark line.
* A publish run whose Spark versions differ from the last published ones, or
  that cannot reach Maven Central to compare, stops in `plan`, before
  anything is built. To go ahead, dispatch again with
  `expected-spark-versions` set to the Spark versions you expect, for example
  `3.5.10 4.0.4 4.1.3` (any order, spaces or commas). The error message and
  the Plan summary of a dry run print the exact value.
* Whenever `expected-spark-versions` is given, in a dry run too, it must
  match `pom.xml`, or the run stops.
* When nothing changed since the last release the input can stay empty.

The build jobs check that Maven arrives at the versions `plan` announced, and
`assemble`, `rehearse` and `publish` accept exactly those versions.

## Jobs and trust

```
plan ─► native ─► build (x3) ─► assemble ─► rehearse ─► publish
        └──── run project code ────┘   └── run these scripts only ──┘
        no secrets, read-only token                    environment: release
```

| Job | Runs | Has |
|---|---|---|
| `plan` | `plan.sh` (reads `pom.xml` at the tag as data; fetches the public Maven Central metadata) | read-only token |
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

Limits of that description, so that it is not read as more than it is:

* It is about `release.yml` on `main`. It says nothing about other workflow
  files or other refs; see "Adopting this workflow".
* Each secret is passed to the one step that needs it, and the keyring is
  deleted before the first request to Maven Central or GitHub. The secret
  values themselves stay in the runner's memory for the whole job, as with
  any GitHub Actions job; deleting the keyring removes the key from disk and
  stops the agent, it does not make the job unable to obtain the key again.
* The scripts reach fixed addresses only (the Portal API, repo1, the three
  GitHub repositories). No variable redirects them; three timing settings
  exist for `test.sh` and stop the script if they are set without the test
  flag, which `release.yml` never sets.
* A re-run ("Re-run failed jobs" or "Re-run all jobs") uses the workflow file
  and the scripts of the commit the run was started on, not the current
  `main`. A fix to a script only takes effect in a newly dispatched run.
* rustc and cargo, Maven, the JDK patch level and the runner image are
  whatever the runner provides on the day. They are recorded in the summaries
  and the release notes, not pinned. Pinned: the actions, protoc, the
  tantivy4java commit and through its `Cargo.lock` every Rust dependency, and
  the Maven plugins that the scripts call by coordinate.

## Native inputs

`native-pins.txt` lists, per tantivy4java version, the tantivy4java commit
and the quickwit and tantivy fork commits a release may build. The build
fetches that tantivy4java commit by id, refuses to continue if the tag
`v<version>` points anywhere else, and checks the fork commits against the
`Cargo.lock` at that commit. It also fails if the build modifies `Cargo.lock`
(cargo does that when it cannot use the locked versions), if a dependency
points to a path outside the checkout, or if a cargo config overrides
dependency sources. protoc is the same pinned release and digest as in
`scripts/setup.sh`.

The resolved commits and tool versions are written to the `native` job
summary, the `assemble` job summary and the release notes.

When `pom.xml` moves to a new tantivy4java version, add its line in the same
pull request:

```
bash .github/scripts/release/build-native.sh print-pin <version>
```

`print-pin` prints the line and, on standard error, where each commit can be
reached from. A commit id by itself proves little: GitHub serves a commit
that exists only in somebody's fork through the parent repository's URL. So
`print-pin` clones the quickwit and tantivy forks and

* says so when a fork commit is on the fork's default branch (the normal
  case; both commits pinned for 0.34.4 are the tips of `main`);
* warns, naming the branches or tags, when it is on another branch or tag
  only: pin it only if building from that branch is intended;
* refuses to print a pin when it is on no branch or tag of the fork.

Review that output before committing the line. The release build itself does
not repeat the check; it builds what the reviewed pin file says.

The `Release Scripts` workflow fails on a pull request that changes the
tantivy4java version without adding a pin.

`scripts/setup.sh` is not used by the release. It still clones the quickwit
fork at its default branch for developer and CI builds; current tantivy4java
versions take both forks from `Cargo.lock` and ignore that clone.

## Adopting this workflow

Merging the workflow does not by itself protect the key. Two facts decide
that, and both are repository settings:

* While `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`, `CENTRAL_USERNAME` and
  `CENTRAL_PASSWORD` exist as repository-level secrets, any workflow on any
  ref can read them.
* The tags `v0.6.0-rc1` and `v0.6.0-rc2` carry the previous `release.yml`,
  which has a `workflow_dispatch` trigger. It can be dispatched on the tag
  ref, and then runs the old workflow, with the project build beside the
  key. Tags cannot be edited away; only the environment rule and the removal
  of the repository-level secrets close this.

Do the steps in this order. Each one is the owner's or the releaser's to do
in the repository settings; nothing in this directory can do them.

1. **Add the four secrets to the `release` environment** (Settings →
   Environments → release → Environment secrets), with the same names. Leave
   the repository-level copies in place for now.
2. **Narrow the environment.** Deployment branches and tags: selected, `main`
   only (remove the `v*` tag rule). Required reviewer set. "Allow
   administrators to bypass configured protection rules": off. From here on,
   a run on a tag ref, including the old workflow on the two rc tags, is
   refused by the environment, but it could still read the repository-level
   secrets from a job that names no environment, which is why step 4 is not
   optional.
3. **Dry run, then the release candidate rehearsal** described in the
   runbook below.
4. **Delete the repository-level copies of the four secrets as soon as the
   rehearsal has passed.** Not after the next real release: until they are
   gone, the separation in this workflow protects nothing.

One thing the rehearsal cannot show: while both copies exist, a secret that
is missing or mistyped in the environment is silently covered by the
repository-level one. After step 4 such a mistake shows up as a failed key
import or a refused upload, before anything is uploaded. Compare the four
names in the environment by eye before deleting.

`main` must stay protected, so that the workflow file and these scripts
change only through reviewed pull requests.

## Tag push or dispatch

The workflow is started by `workflow_dispatch` only. A real release must be
dispatched from `main`, with the tag as input; the tag's commit must be on
`main`.

* **Dispatch from `main`** (implemented): the workflow file and the scripts
  that run beside the secrets are the reviewed ones on `main`. The tag only
  selects which source is built, by the unprivileged jobs. Left open: whoever
  can dispatch workflows chooses the tag (bounded by "must be on `main`") and
  whoever approves the environment is the last check; and the guarantee rests
  on the settings above.
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

* **Plan** summary: "Dry run for …", the tag and its commit, the versions,
  and whether they differ from the last release.
* **native** summary: the tantivy4java, quickwit and tantivy commits.
* **Assemble** summary: the three versions; the manifest digest; the digests
  of all files. For `v0.6.0-rc2` the three `.pom` digests should equal the
  published ones (`…/<version>/indextables_spark-<version>.pom.sha256` on
  repo1): the pom is generated the same way as before.
* **Rehearse** log: each bundle lists 36 entries and passes.

A dry run may also be dispatched from a branch, to try a change to the
workflow or these scripts before it is merged.

### Release candidate rehearsal (the first real use)

A dry run stops before the key, the Central token, the Portal and the Publish
button. The only way to exercise those is a real release, so make the first
one a release candidate on a new tag, for example `v0.6.0-rc3`, and take it
all the way through Publish.

It has to be a new tag. An existing one cannot stand in: `0.6.0-rc2` is
already on Maven Central, a rebuild is not byte-identical, and the upload
step stops before uploading anything when it sees that. And it has to
include pressing Publish, not "upload, then drop": dropping leaves a public
GitHub pre-release with jars that are on no repository, and it skips the one
step that no test and no dry run reaches.

Do it after steps 1 and 2 of "Adopting this workflow". At each point, check:

1. **Before dispatching.** Create the tag on a commit of `main`. Dry-run it
   first; note the `expected-spark-versions` value from the Plan summary if
   it says the names changed.
2. **Dispatch from `main`**, `dry-run` unticked (and `expected-spark-versions`
   if needed).
   * An approval prompt for `release` appears, and only now: the dry run a
     moment ago did not ask. A run dispatched from any other branch or from a
     tag with `dry-run` unticked fails in `plan` and never asks.
   * That is this workflow's own guard. The rule that also stops other
     workflow files (the old one on the rc tags) is the environment's; confirm
     it by reading it, not by dispatching the old workflow:
     `gh api repos/indextables/indextables_spark/environments/release/deployment-branch-policies`
     must list `main` and nothing else.
   * Read the Plan and Assemble summaries, then approve.
3. **While `publish` runs.**
   * The "Import the signing key" step logs `output: fingerprint=…`. It must
     be the release key. The signatures on Maven Central for `0.6.0-rc2` name
     `A9CD03EDB082ECB61DB66028EDA69278FFE47B19` as their signing key; the log
     prints the primary key's fingerprint, which is the same value unless the
     key signs with a subkey (`gpg --show-keys --with-subkey-fingerprints` on
     the public key shows both).
   * The summary lists each upload as it happens, with its deployment id.
4. **After `publish`, before pressing Publish.** Open
   <https://central.sonatype.com/publishing/deployments>.
   * Exactly three deployments from this run, named
     `indextables_spark-<version>-run<run id>-<attempt>`, state VALIDATED.
     Their ids are the three in the job summary. A deployment for these
     versions with another id or name is from another run: drop it.
   * Each holds six files (pom, jar, sources, javadoc, linux-x86_64-shaded,
     jar-with-dependencies), each with a signature.
   * The GitHub Release for the tag has three jars, and its notes end with a
     "Build inputs" block naming the commits.
5. **Press Publish** on each of the three. This cannot be undone.
6. **After Publish** (Maven Central takes a while to show a new version).
   Resolve one coordinate from Maven Central and compare it with the run:

   ```
   v=0.6.0-rc3_spark_3.5.9   # one of the three versions
   curl -fsSL "https://repo1.maven.org/maven2/io/indextables/indextables_spark/$v/indextables_spark-$v-linux-x86_64-shaded.jar" | sha256sum
   ```

   The digest must be the one in the "Build inputs" block of the release
   notes (and in the Assemble summary) for that version, and the jar attached
   to the GitHub Release must have it too.
7. **Delete the repository-level secrets** (step 4 of "Adopting this
   workflow").

### Release

1. Dispatch from `main` with `dry-run` unticked; add
   `expected-spark-versions` if the Plan summary of the dry run asked for it.
2. Wait for `rehearse` to pass, read the Plan and Assemble summaries, then
   approve the `release` environment. Up to here nothing has left the run;
   rejecting the approval ends it.
3. `publish` signs, uploads one bundle per version to the Central Portal as
   `USER_MANAGED`, waits until each is `VALIDATED`, then creates or updates
   the GitHub Release.
4. Open <https://central.sonatype.com/publishing/deployments>. Press
   **Publish** on the deployments whose ids are in the job summary, on all of
   them and on no other. This is the step that cannot be undone.

`skip-central` makes a publish run leave Maven Central alone and only attach
the jars to the GitHub Release.

### Jars already on the GitHub Release

A jar that is already attached to the release is left alone when it is
identical to the one this run built (a re-run of the same run). When it
differs, the GitHub Release step stops and changes nothing: a jar on a
published release may already have been downloaded, and a rebuild is never
byte-identical. This matters most with `skip-central`, where nothing on the
Maven Central side stops a second run for an already released tag. If
replacing the jars is really what you want, dispatch again with
`replace-release-assets` ticked.

### Aborting

| When | How | What is left behind |
|---|---|---|
| Before approval | Reject the approval, or cancel the run | Nothing |
| While `publish` runs | Cancel the run | The last step drops the deployments the job uploaded and says in the summary if it could not; check the Portal. The GitHub Release may exist with some jars |
| After `publish`, before pressing Publish | **Drop** every deployment in the Portal; delete or edit the GitHub Release by hand | Nothing on Maven Central |
| After pressing Publish | Not possible. Publish the remaining versions, and release a new version if this one is wrong | |

### When `publish` fails

`central-upload.sh` handles one version at a time, and when anything fails
(or the run is cancelled) the job drops what it uploaded. The Portal accepts
a drop only once a deployment is VALIDATED or FAILED, so a deployment that is
still being validated is polled first, for up to three minutes, in the
failing step and again in the job's last step.

If something is still left, the last step fails and the job summary has a
section **"Action needed: deployments left in the Central Portal"**. It
lists each deployment by id and name with what to do: drop it by hand and do
not publish it; or, for an upload that was cut off before the Portal
answered, look for a deployment of that name and drop it if it exists. The
run id and attempt in the name tell this job's deployments from any other.

Then use **Re-run failed jobs**: the re-run signs and uploads the same staged
files (same digests), not a rebuild, under a new attempt number. It uses the
scripts of the original run; if the failure was a bug in a script, fix it on
`main` and dispatch a new run instead (a new build, so only possible while
none of the versions has been published).

If some versions were already published by hand and others were not, re-run
the failed job as well: a version that is already on Maven Central with
identical files is skipped, and one with different files stops the run.

## What the tests cover, and what they cannot

`bash .github/scripts/release/test.sh` runs offline: `mvn`, `curl` and `gh`
are stubs, and git is pointed at fixture repositories. It covers the
decision logic of every script (including every refusal), the layout of the
bundles, the upload state machine (rejection, timeouts and cancellation
during validation, refused drops, stale deployments, re-runs) and the shape
of `release.yml` (which job has secrets, the dry-run guards, pins, that no
test-only setting is set). On a pull request it runs on the runner image
with the real `gpg`, generating, exporting, importing and signing with a
throwaway key.

Not covered by any test, and seen for the first time in a real run:

* the Maven and cargo builds themselves (first seen in a dry run);
* artifacts passing between jobs (first seen in a dry run);
* importing the real key and using the real passphrase;
* the Central Portal accepting the token, the bundle and the signatures, and
  its answers to status and drop requests (the stub follows the Portal's API
  documentation);
* `gh` creating or updating the release with the job's token;
* the Publish button.

The last four happen in or after `publish` only, which is what the release
candidate rehearsal is for. If one of the first three of them fails, nothing
has been published: the upload is `USER_MANAGED`, and a failed job drops it.
