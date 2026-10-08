#!/usr/bin/env bash
# Offline tests for the release scripts and for the shape of release.yml.
# No network, no credentials, nothing is uploaded: mvn, curl and gh are
# replaced by stubs that serve fixtures and record what they were asked to do.
#
#   bash .github/scripts/release/test.sh
#
# Needs bash (3.2 or newer), git, zip, unzip, jq and python3. Optional:
#   gpg   the signing tests use a real throwaway key in a temporary keyring;
#         without gpg they use a stub (REQUIRE_GPG=1 makes that a failure)
#   ruby  for the checks that parse release.yml (REQUIRE_WORKFLOW_CHECKS=1
#         makes a missing ruby a failure)
# Exits non-zero if any check fails.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
workflow="$root/.github/workflows/release.yml"
T="$(mktemp -d)"
T="$(cd "$T" && pwd -P)"
trap 'rm -rf "$T"' EXIT
# shellcheck source=lib.sh
. "$here/lib.sh"

pass=0
fail=0
ok() { pass=$((pass + 1)); printf 'ok    %s\n' "$1"; }
bad() { fail=$((fail + 1)); printf 'FAIL  %s\n' "$1"; }
check() { # check <name> <command...>
  local name="$1"; shift
  if "$@" > /dev/null 2>&1; then ok "$name"; else bad "$name"; fi
}
is() { [ "$1" = "$2" ]; }
has() { grep -qF -- "$2" "$1"; }
hasnt() { ! grep -qF -- "$2" "$1"; }
# run <log> <command...>: run with outputs captured; step outputs go to <log>.out
run() {
  local log="$1"; shift
  GITHUB_OUTPUT="$log.out" GITHUB_STEP_SUMMARY="$log.summary" "$@" > "$log" 2>&1
}
fails_with() { # fails_with <text> <log> <command...>: must fail and say <text>
  local text="$1" log="$2"; shift 2
  if run "$log" "$@"; then return 1; fi
  grep -qF -- "$text" "$log"
}
out() { sed -n "s/^$2=//p" "$1.out" 2> /dev/null | tail -n 1; }

export GIT_AUTHOR_NAME=test GIT_AUTHOR_EMAIL=test@invalid GIT_COMMITTER_NAME=test GIT_COMMITTER_EMAIL=test@invalid
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
unset GITHUB_OUTPUT GITHUB_STEP_SUMMARY GNUPGHOME JAVA_HOME

SHA_A=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
SHA_B=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
SHA_C=cccccccccccccccccccccccccccccccccccccccc
SHA_D=dddddddddddddddddddddddddddddddddddddddd

# ---------------------------------------------------------------------------
# lib.sh
# ---------------------------------------------------------------------------
printf 'abc' > "$T/abc"
check "lib: md5" is "$(hash_of md5 "$T/abc")" 900150983cd24fb0d6963f7d28e17f72
check "lib: sha1" is "$(hash_of sha1 "$T/abc")" a9993e364706816aba3e25717850c26c9cd0d89d
check "lib: sha256" is "$(hash_of sha256 "$T/abc")" ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
check "lib: sha512" is "$(hash_of sha512 "$T/abc")" ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f
check "lib: version for the profile's Spark line is accepted" bash -c ". '$here/lib.sh'; check_version 0.6.0-rc2 spark-4.0 0.6.0-rc2_spark_4.0.3"
check "lib: version for another Spark line is refused" bash -c "! (. '$here/lib.sh'; check_version 0.6.0 spark-4.0 0.6.0_spark_4.1.2)"
check "lib: version for another base is refused" bash -c "! (. '$here/lib.sh'; check_version 0.6.0 spark-4.0 0.6.1_spark_4.0.3)"
check "lib: a longer base does not match by prefix" bash -c "! (. '$here/lib.sh'; check_version 0.6.0 spark-3.5 0.6.0-rc2_spark_3.5.8)"
check "lib: an unknown profile is refused" bash -c "! (. '$here/lib.sh'; check_version 0.6.0 spark-9.9 0.6.0_spark_9.9.1)"
check "lib: the six published files" is "$(artifact_names 1.0.0_spark_3.5.8 | tr '\n' ' ')" \
  "indextables_spark-1.0.0_spark_3.5.8.pom indextables_spark-1.0.0_spark_3.5.8.jar indextables_spark-1.0.0_spark_3.5.8-sources.jar indextables_spark-1.0.0_spark_3.5.8-javadoc.jar indextables_spark-1.0.0_spark_3.5.8-linux-x86_64-shaded.jar indextables_spark-1.0.0_spark_3.5.8-jar-with-dependencies.jar "
mkdir -p "$T/sums/sub" && echo one > "$T/sums/a" && echo two > "$T/sums/sub/b"
(. "$here/lib.sh"; write_sums "$T/sums")
check "lib: verify_sums accepts what write_sums wrote" bash -c ". '$here/lib.sh'; verify_sums '$T/sums'"
echo three > "$T/sums/extra"
check "lib: verify_sums refuses an unlisted file" bash -c "! (. '$here/lib.sh'; verify_sums '$T/sums')"
rm "$T/sums/extra"; echo changed > "$T/sums/a"
check "lib: verify_sums refuses a changed file" bash -c "! (. '$here/lib.sh'; verify_sums '$T/sums')"
echo one > "$T/sums/a"; ln -s a "$T/sums/link"
check "lib: verify_sums refuses a symbolic link" bash -c "! (. '$here/lib.sh'; verify_sums '$T/sums')"

# ---------------------------------------------------------------------------
# plan.sh
# ---------------------------------------------------------------------------
P="$T/plan"
mkdir -p "$P"
git init -q -b main "$P/upstream"
(
  cd "$P/upstream"
  echo 1 > f && git add f && git commit -q -m one && git tag v1.2.3
  echo 2 > f && git commit -q -am two && git tag -a v1.3.0-rc1 -m rc
  git checkout -q -b side && echo 3 > f && git commit -q -am three && git tag v9.9.9
  git checkout -q main
)
git clone -q "$P/upstream" "$P/repo"
V123="$(git -C "$P/upstream" rev-parse 'v1.2.3^{commit}')"
RC1="$(git -C "$P/upstream" rev-parse 'v1.3.0-rc1^{commit}')"
plan() { # plan <log> EVENT REF REF_TYPE REF_NAME TAG DRY SKIP
  run "$1" env EVENT_NAME="$2" REF="$3" REF_TYPE="$4" REF_NAME="$5" INPUT_TAG="$6" INPUT_DRY_RUN="$7" \
    INPUT_SKIP_CENTRAL="$8" DEFAULT_BRANCH=main REPO_DIR="$P/repo" bash "$here/plan.sh"
}
L="$P/log"
check "plan: dispatch with dry-run ticked is a dry run" plan "$L.1" workflow_dispatch refs/heads/main branch main v1.2.3 true false
check "plan: ... mode dry-run" is "$(out "$L.1" mode)" dry-run
check "plan: ... tag resolved to its commit" is "$(out "$L.1" commit)" "$V123"
check "plan: ... base version" is "$(out "$L.1" base)" 1.2.3
check "plan: ... not a pre-release" is "$(out "$L.1" prerelease)" false
check "plan: ... summary says nothing is uploaded" has "$L.1.summary" "Nothing is uploaded"
check "plan: dispatch from main with dry-run unticked publishes" plan "$L.2" workflow_dispatch refs/heads/main branch main v1.3.0-rc1 false false
check "plan: ... mode publish" is "$(out "$L.2" mode)" publish
check "plan: ... annotated tag resolved to its commit" is "$(out "$L.2" commit)" "$RC1"
check "plan: ... pre-release" is "$(out "$L.2" prerelease)" true
check "plan: ... Central included" is "$(out "$L.2" central)" true
check "plan: publish from another branch is refused" fails_with "can only be dispatched from main" "$L.3" \
  env EVENT_NAME=workflow_dispatch REF=refs/heads/feature/x INPUT_TAG=v1.2.3 INPUT_DRY_RUN=false DEFAULT_BRANCH=main REPO_DIR="$P/repo" bash "$here/plan.sh"
check "plan: ... and sets no mode" test -z "$(out "$L.3" mode)"
check "plan: publish from a tag ref is refused" fails_with "can only be dispatched from main" "$L.3b" \
  env EVENT_NAME=workflow_dispatch REF=refs/tags/v1.2.3 INPUT_TAG=v1.2.3 INPUT_DRY_RUN=false DEFAULT_BRANCH=main REPO_DIR="$P/repo" bash "$here/plan.sh"
check "plan: a dry run may be dispatched from another branch" plan "$L.4" workflow_dispatch refs/heads/feature/x branch feature/x v1.2.3 true false
check "plan: ... mode dry-run" is "$(out "$L.4" mode)" dry-run
for v in "" TRUE False 0 1 yes "true " "false "; do
  check "plan: dry-run '$v' is refused" fails_with "dry-run must be true or false" "$L.5" \
    env EVENT_NAME=workflow_dispatch REF=refs/heads/main INPUT_TAG=v1.2.3 INPUT_DRY_RUN="$v" DEFAULT_BRANCH=main REPO_DIR="$P/repo" bash "$here/plan.sh"
done
check "plan: skip-central in a publish run" plan "$L.6" workflow_dispatch refs/heads/main branch main v1.2.3 false true
check "plan: ... Central skipped" is "$(out "$L.6" central)" false
check "plan: skip-central has no effect in a dry run" plan "$L.7" workflow_dispatch refs/heads/main branch main v1.2.3 true true
check "plan: ... Central not skipped" is "$(out "$L.7" central)" true
check "plan: a pushed tag publishes (when that trigger is enabled)" plan "$L.8" push refs/tags/v1.2.3 tag v1.2.3 "" "" ""
check "plan: ... mode publish" is "$(out "$L.8" mode)" publish
check "plan: a pushed branch is refused" fails_with "only supported for tags" "$L.9" \
  env EVENT_NAME=push REF=refs/heads/main REF_TYPE=branch REF_NAME=main DEFAULT_BRANCH=main REPO_DIR="$P/repo" bash "$here/plan.sh"
check "plan: ... also when the branch is named like a release tag" bash -c "! EVENT_NAME=push REF=refs/heads/v1.2.3 REF_TYPE=branch REF_NAME=v1.2.3 DEFAULT_BRANCH=main REPO_DIR='$P/repo' GITHUB_OUTPUT='$L.9b.out' bash '$here/plan.sh'"
check "plan: ... and sets no mode" test -z "$(out "$L.9b" mode)"
for e in pull_request pull_request_target schedule release workflow_run ""; do
  check "plan: event '$e' is refused" bash -c "! EVENT_NAME='$e' REF=refs/heads/main DEFAULT_BRANCH=main INPUT_TAG=v1.2.3 INPUT_DRY_RUN=false REPO_DIR='$P/repo' bash '$here/plan.sh'"
done
for t in main v1.2 1.2.3 v1.2.3_spark_3.5.8 'v1.2.3;id' 'v1.2.3 ' v1.2.3-rc_1 '../v1.2.3' ''; do
  check "plan: tag '$t' is refused" fails_with "is not a release tag" "$L.10" \
    env EVENT_NAME=workflow_dispatch REF=refs/heads/main INPUT_TAG="$t" INPUT_DRY_RUN=true DEFAULT_BRANCH=main REPO_DIR="$P/repo" bash "$here/plan.sh"
done
check "plan: a tag that does not exist is refused" fails_with "does not exist" "$L.11" \
  env EVENT_NAME=workflow_dispatch REF=refs/heads/main INPUT_TAG=v7.7.7 INPUT_DRY_RUN=true DEFAULT_BRANCH=main REPO_DIR="$P/repo" bash "$here/plan.sh"
check "plan: a tag that is not on main is refused" fails_with "is not an ancestor of main" "$L.12" \
  env EVENT_NAME=workflow_dispatch REF=refs/heads/main INPUT_TAG=v9.9.9 INPUT_DRY_RUN=false DEFAULT_BRANCH=main REPO_DIR="$P/repo" bash "$here/plan.sh"
check "plan: ... in a dry run as well" fails_with "is not an ancestor of main" "$L.13" \
  env EVENT_NAME=workflow_dispatch REF=refs/heads/main INPUT_TAG=v9.9.9 INPUT_DRY_RUN=true DEFAULT_BRANCH=main REPO_DIR="$P/repo" bash "$here/plan.sh"

# ---------------------------------------------------------------------------
# Stubs for the build tools. $FAKE holds fixtures and switches.
# ---------------------------------------------------------------------------
mkdir -p "$T/bin" "$T/fx"
FAKE="$T/fake"; mkdir -p "$FAKE"; export FAKE

# Fixture jars: a tantivy4java jar with a native library, and project jars.
mkdir -p "$T/fx/t4j/native" "$T/fx/t4j/io"
echo "native library built in this run" > "$T/fx/t4j/native/libtantivy4java.so"
echo class > "$T/fx/t4j/io/A.class"
(cd "$T/fx/t4j" && zip -q -r "$T/fx/t4j.jar" .)
mkdir -p "$T/fx/other/native"
echo "some other native library" > "$T/fx/other/native/libtantivy4java.so"
(cd "$T/fx/other" && zip -q -r "$T/fx/other-native.jar" .)
mkdir -p "$T/fx/plain" && echo x > "$T/fx/plain/X.class"
(cd "$T/fx/plain" && zip -q -r "$T/fx/plain.jar" .)
cp "$T/fx/t4j.jar" "$T/fx/fat.jar" && (cd "$T/fx/plain" && zip -q "$T/fx/fat.jar" X.class)
export FX="$T/fx"

cat > "$T/bin/mvn" << 'STUB'
#!/usr/bin/env bash
# Stub Maven: enough of tantivy4java's `clean install` and of this project's
# help:evaluate, versions:set and `verify` for the release scripts.
set -eu
echo "mvn $*" >> "$FAKE/mvn.log"
repo=""; profile=""; newversion=""; goal=""
for a in "$@"; do
  case "$a" in
    -Dmaven.repo.local=*) repo="${a#-Dmaven.repo.local=}" ;;
    -P*) profile="${a#-P}" ;;
    -DnewVersion=*) newversion="${a#-DnewVersion=}" ;;
    --version) echo "Apache Maven 3.9.9 (stub)"; exit 0 ;;
    *:evaluate) goal=evaluate ;;
    *:set) goal=set ;;
    install) goal=install ;;
    verify) goal=verify ;;
    deploy | *:deploy | *:publish | *:sign) echo "stub mvn: refusing $a" >&2; exit 96 ;;
  esac
done
case "$goal" in
  install) # tantivy4java
    v="$(cat "$FAKE/t4j.version")"
    d="$repo/io/indextables/tantivy4java/$v"
    mkdir -p "$d"
    cp "${FAKE_T4J_JAR:-$FX/t4j.jar}" "$d/tantivy4java-$v-linux-x86_64.jar"
    cp "${FAKE_T4J_JAR:-$FX/t4j.jar}" "$d/tantivy4java-$v.jar"
    echo "<project/>" > "$d/tantivy4java-$v.pom"
    echo "#stub" > "$d/_remote.repositories"
    if [ -f "$FAKE/touch-lock" ]; then echo "# changed by the build" >> native/Cargo.lock; fi
    ;;
  evaluate)
    cat "$FAKE/spark.${profile}"
    ;;
  set)
    echo "$newversion" > .stamped
    ;;
  verify)
    case " $* " in *" -Dgpg.skip=true "*) ;; *) echo "stub mvn: verify without -Dgpg.skip=true" >&2; exit 95 ;; esac
    v="$(cat .stamped)"; p="indextables_spark-$v"
    mkdir -p target
    cp "$FX/plain.jar" "target/$p.jar"
    cp "$FX/plain.jar" "target/$p-sources.jar"
    [ -f "$FAKE/no-javadoc" ] || cp "$FX/plain.jar" "target/$p-javadoc.jar"
    cp "${FAKE_FAT_JAR:-$FX/fat.jar}" "target/$p-linux-x86_64-shaded.jar"
    cp "${FAKE_FAT_JAR:-$FX/fat.jar}" "target/$p-linux-x86_64-jar-with-dependencies.jar"
    [ ! -f "$FAKE/extra-jar" ] || cp "$FX/plain.jar" "target/$p-tests.jar"
    if [ -f "$FAKE/swap-t4j" ]; then
      t="$(cat "$FAKE/t4j.version")"
      cp "$FX/other-native.jar" "$repo/io/indextables/tantivy4java/$t/tantivy4java-$t-linux-x86_64.jar"
    fi
    sed -e "s/@VERSION@/$v/" "${FAKE_POM:-$FX/published.pom}" > dependency-reduced-pom.xml
    ;;
  *) echo "stub mvn: unexpected invocation: $*" >&2; exit 97 ;;
esac
STUB
for tool in java cargo rustc; do
  printf '#!/usr/bin/env bash\necho "%s 1.0 (stub)" >&2\necho "%s 1.0 (stub)"\n' "$tool" "$tool" > "$T/bin/$tool"
done
printf '#!/usr/bin/env bash\ncase "$1" in --exists) exit 0 ;; --modversion) echo 3.0.0 ;; esac\n' > "$T/bin/pkg-config"
chmod +x "$T/bin/"*
STUBPATH="$T/bin:$PATH"

cat > "$T/fx/published.pom" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>io.indextables</groupId>
  <artifactId>indextables_spark</artifactId>
  <version>@VERSION@</version>
  <name>IndexTables4Spark</name>
  <description>Fast search file format</description>
  <url>https://example.invalid/</url>
  <licenses><license><name>Apache License, Version 2.0</name><url>https://www.apache.org/licenses/LICENSE-2.0.txt</url></license></licenses>
  <developers><developer><name>Contributors</name></developer></developers>
  <scm><connection>scm:git:git://example.invalid/x.git</connection><url>https://example.invalid/x</url></scm>
</project>
EOF

# ---------------------------------------------------------------------------
# build-native.sh
# ---------------------------------------------------------------------------
N="$T/native"; mkdir -p "$N"
QW=1111111111111111111111111111111111111111
TV=2222222222222222222222222222222222222222
MR=3333333333333333333333333333333333333333
new_t4j() { # new_t4j <dir>: a tantivy4java-like repository with tag v0.34.4
  rm -rf "$1"; git init -q -b main "$1"
  mkdir -p "$1/native" "$1/.cargo"
  cat > "$1/native/Cargo.toml" << EOF
[package]
name = "tantivy4java"
[dependencies]
tantivy = { git = "https://github.com/indextables/tantivy", rev = "$TV" }
quickwit-storage = { git = "https://github.com/indextables/quickwit", rev = "$QW" }
EOF
  cat > "$1/native/Cargo.lock" << EOF
version = 3
[[package]]
name = "quickwit-storage"
source = "git+https://github.com/indextables/quickwit?rev=$QW#$QW"
[[package]]
name = "quickwit-proto"
source = "git+https://github.com/indextables/quickwit?rev=$QW#$QW"
[[package]]
name = "tantivy"
source = "git+https://github.com/indextables/tantivy?rev=$TV#$TV"
[[package]]
name = "mrecordlog"
source = "git+https://github.com/quickwit-oss/mrecordlog?rev=3333333#$MR"
[[package]]
name = "serde"
source = "registry+https://github.com/rust-lang/crates.io-index"
EOF
  printf '[target.x86_64-unknown-linux-gnu]\nrustflags = ["-C", "target-cpu=x86-64"]\n' > "$1/.cargo/config.toml"
  git -C "$1" add -A && git -C "$1" commit -q -m release
  git -C "$1" config uploadpack.allowAnySHA1InWant true
}
new_t4j "$N/t4j"
git -C "$N/t4j" tag v0.34.4
T4J_COMMIT="$(git -C "$N/t4j" rev-parse HEAD)"
printf '<dependency>\n  <artifactId>tantivy4java</artifactId>\n  <version>0.34.4</version>\n</dependency>\n' > "$N/pom.xml"
printf '# comment\n0.34.4 %s %s %s\n0.1.0 %s - -\n' "$T4J_COMMIT" "$QW" "$TV" "$SHA_A" > "$N/pins.txt"
echo 0.34.4 > "$FAKE/t4j.version"
native() { # native <log> <subcommand> [VAR=value...]
  local log="$1" sub="$2"; shift 2
  run "$log" env PATH="$STUBPATH" T4J_REPO="file://$N/t4j" POM="$N/pom.xml" PINS="$N/pins.txt" WORK="$N/work" OUT="$N/out" \
    M2_REPO="$N/m2" "$@" bash "$here/build-native.sh" "$sub"
}
L="$N/log"
check "native: resolve accepts the pinned tag" native "$L.1" resolve
check "native: ... records the tantivy4java commit" has "$N/out/native-inputs.txt" "tantivy4java_commit=$T4J_COMMIT"
check "native: ... records the quickwit fork commit" has "$N/out/native-inputs.txt" "quickwit_commit=$QW"
check "native: ... records the tantivy fork commit" has "$N/out/native-inputs.txt" "tantivy_commit=$TV"
check "native: ... records other git dependencies with their commit" has "$N/out/native-inputs.txt" "other_git_dependencies=https://github.com/quickwit-oss/mrecordlog@$MR"
check "native: ... checked out exactly the pinned commit" is "$(git -C "$N/work/tantivy4java" rev-parse HEAD)" "$T4J_COMMIT"
check "native: print-pin prints the pin line" is "$(PATH="$STUBPATH" T4J_REPO="file://$N/t4j" bash "$here/build-native.sh" print-pin 0.34.4 2> /dev/null)" "0.34.4 $T4J_COMMIT $QW $TV"

check "native: build needs the toolchain step first" fails_with "toolchain" "$L.2" native "$L.2" build
mkdir -p "$N/work/protoc/bin" "$N/work/protoc/include" && printf '#!/bin/sh\necho "libprotoc 25.5"\n' > "$N/work/protoc/bin/protoc" && chmod +x "$N/work/protoc/bin/protoc"
mkdir -p "$N/m2/io/indextables/tantivy4java/0.34.4" && echo stale > "$N/m2/io/indextables/tantivy4java/0.34.4/tantivy4java-0.34.4-linux-x86_64.jar"
echo stale > "$N/m2/io/indextables/tantivy4java/0.34.4/tantivy4java-0.34.4-left-over.jar"
check "native: build collects the jar" native "$L.3" build
check "native: ... a jar that was there before the build is not reused" has "$L.3" "removing it before the build"
check "native: ... the collected jar is the one this build installed" is "$(hash_of sha256 "$N/out/repository/tantivy4java-0.34.4-linux-x86_64.jar")" "$(hash_of sha256 "$T/fx/t4j.jar")"
check "native: ... nothing that was there before the build is collected" test ! -e "$N/out/repository/tantivy4java-0.34.4-left-over.jar"
check "native: ... Maven bookkeeping files are left out" test ! -e "$N/out/repository/_remote.repositories"
check "native: ... digests cover the output" bash -c ". '$here/lib.sh'; verify_sums '$N/out'"
check "native: ... records an unchanged Cargo.lock" has "$N/out/native-inputs.txt" "cargo_lock_unchanged=true"
check "native: ... records the native library digest" has "$N/out/native-inputs.txt" "native_library=native/libtantivy4java.so sha256 $(hash_of sha256 "$T/fx/t4j/native/libtantivy4java.so")"
check "native: ... summary lists the commits" has "$L.3.summary" "$T4J_COMMIT"
cp -R "$N/out" "$N/out.good"

touch "$FAKE/touch-lock"
check "native: a build that rewrites Cargo.lock is refused" fails_with "modified native/Cargo.lock" "$L.4" native "$L.4" build
rm "$FAKE/touch-lock"
git -C "$N/work/tantivy4java" checkout -q -- native/Cargo.lock
check "native: a jar without a native library is refused" fails_with "contains no native library" "$L.5" native "$L.5" build FAKE_T4J_JAR="$T/fx/plain.jar"

printf '0.34.4 %s %s %s\n' "$SHA_A" "$QW" "$TV" > "$N/pins.moved"
check "native: a tag that does not point to the pinned commit is refused" fails_with "points to $T4J_COMMIT, but native-pins.txt pins $SHA_A" "$L.6" native "$L.6" resolve PINS="$N/pins.moved"
printf '0.1.0 %s - -\n' "$SHA_A" > "$N/pins.none"
check "native: a version without a pin is refused" fails_with "exactly one line for tantivy4java 0.34.4" "$L.7" native "$L.7" resolve PINS="$N/pins.none"
printf '0.34.4 %s %s %s\n0.34.4 %s %s %s\n' "$T4J_COMMIT" "$QW" "$TV" "$T4J_COMMIT" "$QW" "$TV" > "$N/pins.twice"
check "native: a version pinned twice is refused" fails_with "exactly one line" "$L.8" native "$L.8" resolve PINS="$N/pins.twice"
printf '0.34.4 %s %s %s\n' "$T4J_COMMIT" "$SHA_B" "$TV" > "$N/pins.qw"
check "native: a quickwit fork commit other than the pinned one is refused" fails_with "resolves the quickwit fork to $QW" "$L.9" native "$L.9" resolve PINS="$N/pins.qw"
printf '0.34.4 %s %s %s\n' "$T4J_COMMIT" "$QW" "$SHA_B" > "$N/pins.tv"
check "native: a tantivy fork commit other than the pinned one is refused" fails_with "resolves the tantivy fork to $TV" "$L.10" native "$L.10" resolve PINS="$N/pins.tv"
printf '0.34.4 %s main %s\n' "$T4J_COMMIT" "$TV" > "$N/pins.branch"
check "native: a pin that is not a commit id is refused" fails_with "not a full commit id" "$L.11" native "$L.11" resolve PINS="$N/pins.branch"

variant() { # variant <name> <edit command>: a repository that differs in one way
  new_t4j "$N/v-$1"; (cd "$N/v-$1" && eval "$2" && git add -A && git commit -q -m variant && git tag v0.34.4)
  printf '0.34.4 %s %s %s\n' "$(git -C "$N/v-$1" rev-parse HEAD)" "$QW" "$TV" > "$N/pins.$1"
}
variant path "echo 'quickwit-common = { path = \"../../quickwit/quickwit-common\" }' >> native/Cargo.toml"
check "native: a dependency on a sibling checkout is refused" fails_with "outside the tantivy4java checkout" "$L.12" native "$L.12" resolve T4J_REPO="file://$N/v-path" PINS="$N/pins.path"
variant patch "printf '[patch.crates-io]\nserde = { path = \"/tmp/serde\" }\n' >> .cargo/config.toml"
check "native: a cargo config that overrides sources is refused" fails_with "overrides dependency sources" "$L.13" native "$L.13" resolve T4J_REPO="file://$N/v-patch" PINS="$N/pins.patch"
variant two "printf '[[package]]\nname = \"quickwit-x\"\nsource = \"git+https://github.com/indextables/quickwit?rev=$SHA_C#$SHA_C\"\n' >> native/Cargo.lock"
check "native: a lock file with two quickwit commits is refused" fails_with "more than one commit" "$L.14" native "$L.14" resolve T4J_REPO="file://$N/v-two" PINS="$N/pins.two"
variant short "printf '[[package]]\nname = \"y\"\nsource = \"git+https://github.com/indextables/quickwit?branch=main\"\n' >> native/Cargo.lock"
check "native: a git dependency without a resolved commit is refused" fails_with "without a resolved commit" "$L.15" native "$L.15" resolve T4J_REPO="file://$N/v-short" PINS="$N/pins.short"
new_t4j "$N/v-annot"; git -C "$N/v-annot" tag -a v0.34.4 -m annotated
printf '0.34.4 %s %s %s\n' "$(git -C "$N/v-annot" rev-parse HEAD)" "$QW" "$TV" > "$N/pins.annot"
check "native: an annotated tag is resolved to its commit" native "$L.16" resolve T4J_REPO="file://$N/v-annot" PINS="$N/pins.annot"

# toolchain: a download with the wrong digest is never unpacked.
cat > "$T/bin/curl" << 'STUB'
#!/usr/bin/env bash
# Stub curl for the protoc download: writes a small file to the -o target.
o=""; while [ $# -gt 0 ]; do case "$1" in -o) o="$2"; shift 2 ;; *) shift ;; esac; done
echo "not protoc" > "$o"
STUB
chmod +x "$T/bin/curl"
native "$L.0" resolve > /dev/null 2>&1
check "native: a protoc download with the wrong digest is refused" fails_with "expected e1ed237a17b2e851cf9662cb5ad02b46e70ff8e060e05984725bc4b4228c6b28" "$L.17" native "$L.17" toolchain
check "native: ... and is not unpacked" test ! -e "$N/work/protoc/bin/protoc"
rm "$T/bin/curl"

check "pins: protoc version is the one scripts/setup.sh uses" is \
  "$(sed -n 's/^PROTOC_VERSION=//p' "$here/build-native.sh")" "$(sed -n 's/^PROTOC_VERSION="\(.*\)"$/\1/p' "$root/scripts/setup.sh")"
check "pins: protoc linux-x86_64 digest is the one scripts/setup.sh uses" is \
  "$(sed -n 's/^PROTOC_SHA256=//p' "$here/build-native.sh")" "$(sed -n 's/^ *linux-x86_64) *echo "\([0-9a-f]*\)".*/\1/p' "$root/scripts/setup.sh")"
pom_t4j="$(pom_tantivy4java_version "$root/pom.xml" 2> /dev/null)"
check "pins: pom.xml's tantivy4java version ($pom_t4j) has exactly one pin" is \
  "$(awk -v v="$pom_t4j" '!/^[[:space:]]*#/ && $1 == v && NF == 4' "$here/native-pins.txt" | grep -c .)" 1
check "pins: every pin is a full commit id or '-'" test -z \
  "$(grep -v '^[[:space:]]*#' "$here/native-pins.txt" | grep -v -E '^[0-9]+\.[0-9]+\.[0-9]+ [0-9a-f]{40} ([0-9a-f]{40}|-) ([0-9a-f]{40}|-)$')"

# ---------------------------------------------------------------------------
# build-artifacts.sh
# ---------------------------------------------------------------------------
B="$T/build"; mkdir -p "$B"
echo 3.5.8 > "$FAKE/spark.spark-3.5"; echo 4.0.3 > "$FAKE/spark.spark-4.0"; echo 4.1.2 > "$FAKE/spark.spark-4.1"
artifacts() { # artifacts <log> <profile> [VAR=value...]
  local log="$1" profile="$2"; shift 2
  rm -rf "$B/src-$profile" "$B/m2-$profile"; mkdir -p "$B/src-$profile"; cp "$N/pom.xml" "$B/src-$profile/pom.xml"
  run "$log" env PATH="$STUBPATH" SRC="$B/src-$profile" PROFILE="$profile" BASE_VERSION=1.2.3 NATIVE_DIR="$N/out.good" \
    OUT="$B/out/artifacts-$profile" M2_REPO="$B/m2-$profile" "$@" bash "$here/build-artifacts.sh"
}
L="$B/log"
: > "$FAKE/mvn.log"
check "build: spark-3.5" artifacts "$L.35" spark-3.5
check "build: spark-4.0" artifacts "$L.40" spark-4.0
check "build: spark-4.1" artifacts "$L.41" spark-4.1
check "build: version is <base>_spark_<spark.version of the profile>" is "$(out "$L.40" version)" 1.2.3_spark_4.0.3
check "build: files carry their published names" is "$(ls "$B/out/artifacts-spark-4.0/files" | LC_ALL=C sort | tr '\n' ' ')" "$(artifact_names 1.2.3_spark_4.0.3 | LC_ALL=C sort | tr '\n' ' ')"
check "build: the published pom is the dependency-reduced pom" has "$B/out/artifacts-spark-4.0/files/indextables_spark-1.2.3_spark_4.0.3.pom" "<version>1.2.3_spark_4.0.3</version>"
check "build: digests cover the output" bash -c ". '$here/lib.sh'; verify_sums '$B/out/artifacts-spark-4.0'"
check "build: Maven is never asked to sign, deploy or publish" test -z "$(grep -E '(^| )(deploy|install)( |$)|:deploy|:publish|:sign' "$FAKE/mvn.log")"
check "build: the release profile runs with signing off" has "$FAKE/mvn.log" "-Pspark-4.0,release -DskipTests -Dgpg.skip=true verify"
check "build: plugins invoked by coordinate are pinned" test -z "$(grep -E ':(evaluate|set)' "$FAKE/mvn.log" | grep -v -E '[a-z.-]+:[a-z-]+:[0-9.]+:(evaluate|set)')"
cp -R "$B/out" "$B/out.good"

mkdir -p "$B/m2-pre/io/indextables/tantivy4java/0.34.4"
check "build: a tantivy4java already in the local repository is refused" fails_with "already in the local Maven repository" "$L.1" \
  env PATH="$STUBPATH" SRC="$B/src-spark-3.5" PROFILE=spark-3.5 BASE_VERSION=1.2.3 NATIVE_DIR="$N/out.good" OUT="$B/out/x" M2_REPO="$B/m2-pre" bash "$here/build-artifacts.sh"
cp -R "$N/out.good" "$B/native.bad" && echo tampered >> "$B/native.bad/repository/tantivy4java-0.34.4-linux-x86_64.jar"
check "build: a native artifact that does not match its digests is refused" fails_with "digest mismatch" "$L.2" artifacts "$L.2" spark-3.5 NATIVE_DIR="$B/native.bad"
echo 4.0.3 > "$FAKE/spark.spark-3.5"
check "build: a Spark version outside the profile's line is refused" fails_with "is not 1.2.3_spark_3.5.<patch>" "$L.3" artifacts "$L.3" spark-3.5
echo 3.5.8 > "$FAKE/spark.spark-3.5"
check "build: an unknown profile is refused" fails_with "unknown Spark profile" "$L.4" artifacts "$L.4" spark-5.0
touch "$FAKE/no-javadoc"
check "build: a missing javadoc jar is refused" fails_with "did not produce" "$L.5" artifacts "$L.5" spark-3.5
rm "$FAKE/no-javadoc"; touch "$FAKE/extra-jar"
check "build: a jar the scripts do not know is refused" fails_with "which the release scripts do not know" "$L.6" artifacts "$L.6" spark-3.5
rm "$FAKE/extra-jar"; touch "$FAKE/swap-t4j"
check "build: a tantivy4java jar replaced during the build is refused" fails_with "changed during the build" "$L.7" artifacts "$L.7" spark-3.5
rm "$FAKE/swap-t4j"
rm -rf "$B/out" && cp -R "$B/out.good" "$B/out"

# ---------------------------------------------------------------------------
# assemble.sh and verify-staging.sh
# ---------------------------------------------------------------------------
A="$T/assemble"; mkdir -p "$A"
new_in() { rm -rf "$A/in"; mkdir -p "$A/in"; cp -R "$N/out.good" "$A/in/native"; cp -R "$B/out.good/"* "$A/in/"; }
assemble() { # assemble <log> [VAR=value...]
  local log="$1"; shift
  run "$log" env IN="$A/in" OUT="$A/staging" TAG=v1.2.3 COMMIT="$SHA_D" BASE_VERSION=1.2.3 RUN_URL=https://example.invalid/run/1 "$@" bash "$here/assemble.sh"
}
resum() { (. "$here/lib.sh"; write_sums "$1"); }
L="$A/log"
new_in
check "assemble: accepts the three builds" assemble "$L.1"
check "assemble: versions, in profile order" is "$(out "$L.1" versions)" "1.2.3_spark_3.5.8 1.2.3_spark_4.0.3 1.2.3_spark_4.1.2"
vdir="$A/staging/bundles/1.2.3_spark_4.0.3/io/indextables/indextables_spark/1.2.3_spark_4.0.3"
check "assemble: Maven repository layout, six files and four checksums each" is "$(ls "$vdir" | grep -c .)" 30
check "assemble: no signatures yet" test -z "$(find "$A/staging" -name '*.asc')"
check "assemble: checksum files hold the bare digest" is "$(cat "$vdir/indextables_spark-1.2.3_spark_4.0.3.pom.sha1")" "$(hash_of sha1 "$vdir/indextables_spark-1.2.3_spark_4.0.3.pom")"
check "assemble: ... without a trailing newline" is "$(wc -c < "$vdir/indextables_spark-1.2.3_spark_4.0.3.pom.md5" | tr -d ' ')" 32
check "assemble: manifest digest is reported" is "$(out "$L.1" manifest_sha256)" "$(hash_of sha256 "$A/staging/MANIFEST.sha256")"
check "assemble: notes name the tantivy4java commit" has "$A/staging/release-notes.md" "\`v0.34.4\` at \`$T4J_COMMIT\`"
check "assemble: notes name the fork commits" test -n "$(grep -F "$QW" "$A/staging/release-notes.md")" -a -n "$(grep -F "$TV" "$A/staging/release-notes.md")"
check "assemble: notes name the released commit" has "$A/staging/release-notes.md" "$SHA_D"
check "assemble: summary shows the manifest digest" has "$L.1.summary" "$(out "$L.1" manifest_sha256)"
MANIFEST="$(out "$L.1" manifest_sha256)"
rm -rf "$A/staging.good" && cp -R "$A/staging" "$A/staging.good"

check "assemble: a tag that does not match the base version is refused" fails_with "does not belong to tag" "$L.2" assemble "$L.2" TAG=v1.2.4
new_in; rm -rf "$A/in/artifacts-spark-4.1"
check "assemble: a missing profile is refused" fails_with "no build output for spark-4.1" "$L.3" assemble "$L.3"
new_in; echo x > "$A/in/artifacts-spark-4.0/files/evil.jar"; resum "$A/in/artifacts-spark-4.0"
check "assemble: an extra file is refused, even with valid digests" fails_with "does not hold exactly the expected files" "$L.4" assemble "$L.4"
new_in; echo x >> "$A/in/artifacts-spark-4.0/files/indextables_spark-1.2.3_spark_4.0.3.jar"
check "assemble: a file changed after its digest was taken is refused" fails_with "digest mismatch" "$L.5" assemble "$L.5"
new_in; sed -i.bak 's/^version=.*/version=1.2.3_spark_4.1.2/' "$A/in/artifacts-spark-4.0/build-info.txt"; rm "$A/in/artifacts-spark-4.0/build-info.txt.bak"; resum "$A/in/artifacts-spark-4.0"
check "assemble: a version for another Spark line is refused" fails_with "is not 1.2.3_spark_4.0.<patch>" "$L.6" assemble "$L.6"
pom_case() { # pom_case <name> <sed expression> <expected message>
  new_in
  local f="$A/in/artifacts-spark-3.5/files/indextables_spark-1.2.3_spark_3.5.8.pom"
  sed -i.bak "$2" "$f"; rm "$f.bak"; resum "$A/in/artifacts-spark-3.5"
  check "assemble: a pom with $1 is refused" fails_with "$3" "$L.pom" assemble "$L.pom"
}
pom_case "another groupId" 's|<groupId>io.indextables</groupId>|<groupId>io.other</groupId>|' "groupId is 'io.other'"
pom_case "another artifactId" 's|<artifactId>indextables_spark</artifactId>|<artifactId>tantivy4java</artifactId>|' "artifactId is 'tantivy4java'"
pom_case "another version" 's|<version>1.2.3_spark_3.5.8</version>|<version>9.9.9</version>|' "version is '9.9.9'"
pom_case "no license" 's|<licenses>.*</licenses>||' "missing licenses/license/name"
pom_case "no scm" 's|<scm>.*</scm>||' "missing scm/connection"
pom_case "a parent" 's|<modelVersion>|<parent><groupId>x</groupId></parent><modelVersion>|' "has a <parent>"
pom_case "broken XML" 's|</project>||' "not well-formed"
new_in
for f in linux-x86_64-shaded jar-with-dependencies; do cp "$T/fx/other-native.jar" "$A/in/artifacts-spark-4.1/files/indextables_spark-1.2.3_spark_4.1.2-$f.jar"; done
resum "$A/in/artifacts-spark-4.1"
check "assemble: a jar with another native library is refused" fails_with "does not contain this run's native/libtantivy4java.so" "$L.7" assemble "$L.7"
new_in; echo "not a zip" > "$A/in/artifacts-spark-3.5/files/indextables_spark-1.2.3_spark_3.5.8-sources.jar"; resum "$A/in/artifacts-spark-3.5"
check "assemble: a jar that is not a zip file is refused" fails_with "is not a readable jar" "$L.8" assemble "$L.8"
new_in; sed -i.bak 's/^tantivy4java_jar_sha256=.*/tantivy4java_jar_sha256=0000/' "$A/in/artifacts-spark-3.5/build-info.txt"; rm "$A/in/artifacts-spark-3.5/build-info.txt.bak"; resum "$A/in/artifacts-spark-3.5"
check "assemble: a build that used another tantivy4java jar is refused" fails_with "was not built with this run's tantivy4java jar" "$L.9" assemble "$L.9"
new_in; sed -i.bak '/^cargo_lock_unchanged=/d' "$A/in/native/native-inputs.txt"; rm "$A/in/native/native-inputs.txt.bak"; resum "$A/in/native"
check "assemble: a native build without an unchanged Cargo.lock is refused" fails_with "does not record an unchanged Cargo.lock" "$L.10" assemble "$L.10"
new_in; sed -i.bak 's/^tantivy4java_commit=.*/tantivy4java_commit=main/' "$A/in/native/native-inputs.txt"; rm "$A/in/native/native-inputs.txt.bak"; resum "$A/in/native"
check "assemble: a tantivy4java commit that is not a commit id is refused" fails_with "does not state a tantivy4java commit" "$L.11" assemble "$L.11"
# shellcheck disable=SC2016
new_in; echo 'rustc=rustc 1.0 `x` <img src=x> @everyone' >> "$A/in/native/native-inputs.txt"; resum "$A/in/native"
check "assemble: tool text from the build that is not plain text stays out of the notes" assemble "$L.12"
check "assemble: ... a placeholder is shown instead" has "$A/staging/release-notes.md" "(not shown: unexpected characters)"
check "assemble: ... the text itself is absent" hasnt "$A/staging/release-notes.md" "<img"

staging_copy() { rm -rf "$A/s"; cp -R "$A/staging.good" "$A/s"; }
verify() { run "$1" env MANIFEST_SHA256="${2:-$MANIFEST}" BASE_VERSION="${3:-1.2.3}" bash "$here/verify-staging.sh" "$A/s"; }
L="$A/vlog"
staging_copy
check "verify-staging: accepts the assembled directory" verify "$L.1"
check "verify-staging: another manifest digest is refused" fails_with "but the assemble job reported" "$L.2" verify "$L.2" "$(printf 'x' | hash_stream sha256)"
check "verify-staging: a malformed digest is refused" fails_with "is not a SHA-256 digest" "$L.3" verify "$L.3" abc
staging_copy; echo x > "$A/s/bundles/1.2.3_spark_3.5.8/io/indextables/indextables_spark/1.2.3_spark_3.5.8/extra.jar"
check "verify-staging: an added file is refused" fails_with "not exactly the files listed" "$L.4" verify "$L.4"
staging_copy; echo x >> "$A/s/bundles/1.2.3_spark_3.5.8/io/indextables/indextables_spark/1.2.3_spark_3.5.8/indextables_spark-1.2.3_spark_3.5.8.jar"
check "verify-staging: a changed file is refused" fails_with "digest mismatch" "$L.5" verify "$L.5"
staging_copy; rm "$A/s/release-notes.md"
check "verify-staging: a removed file is refused" fails_with "not exactly the files listed" "$L.6" verify "$L.6"
staging_copy
check "verify-staging: versions of another tag are refused" fails_with "is not 1.2.4_spark_3.5.<patch>" "$L.7" verify "$L.7" "$MANIFEST" 1.2.4
# A self-consistent directory for other coordinates: valid manifest, wrong content.
staging_copy
mkdir -p "$A/s/bundles/1.2.3_spark_3.5.8/io/indextables/tantivy4java/9.9.9" && echo x > "$A/s/bundles/1.2.3_spark_3.5.8/io/indextables/tantivy4java/9.9.9/tantivy4java-9.9.9.jar"
(. "$here/lib.sh"; write_sums "$A/s" MANIFEST.sha256)
check "verify-staging: other coordinates are refused even with a matching manifest" fails_with "lists files other than the expected ones" "$L.8" verify "$L.8" "$(hash_of sha256 "$A/s/MANIFEST.sha256")"

# ---------------------------------------------------------------------------
# signing-key.sh, sign-bundles.sh, check-bundles.sh
# ---------------------------------------------------------------------------
S="$T/sign"; mkdir -p "$S"
SIGNPATH="$PATH"
REAL_GPG=0
if command -v gpg > /dev/null 2>&1; then
  REAL_GPG=1
  echo "note  signing tests use gpg ($(gpg --version | head -n 1)) with a throwaway key"
  # gpg's agent socket needs a short path; mktemp directories can be too long.
  KEYHOME="$(mktemp -d /tmp/rt.XXXXXX)/release-gnupg"
  trap 'rm -rf "$T" "$(dirname "$KEYHOME")"' EXIT
else
  if [ "${REQUIRE_GPG:-0}" = 1 ]; then
    bad "signing: gpg is required but not available (the checks below ran against a stub)"
  else
    echo "note  gpg not available: signing tests use a stub (set REQUIRE_GPG=1 to make this a failure)"
  fi
  KEYHOME="$S/release-gnupg"
  mkdir -p "$S/bin"
  cat > "$S/bin/gpg" << 'STUB'
#!/usr/bin/env bash
# Stub gpg: one key per GNUPGHOME, "signatures" that name the key and the
# file's digest. Refuses a wrong passphrase, like gpg does.
set -eu
h() { if command -v sha256sum > /dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }
mode=""; pfd=""; user=""; outf=""; files=()
while [ $# -gt 0 ]; do
  case "$1" in
    --batch | --quiet | --no-tty | --yes | --armor | --with-colons) shift ;;
    --pinentry-mode | --status-fd) shift 2 ;;
    --passphrase-fd) pfd="$2"; shift 2 ;;
    --local-user) user="$2"; shift 2 ;;
    --output) outf="$2"; shift 2 ;;
    --import) mode=import; shift ;;
    --quick-generate-key) mode=generate; shift 5 ;;
    --export-secret-keys) mode=export; shift 2 ;;
    --list-secret-keys) mode=list; shift ;;
    --detach-sign) mode=sign; shift ;;
    --verify) mode=verify; shift ;;
    -*) echo "stub gpg: unexpected option $1" >&2; exit 97 ;;
    *) files+=("$1"); shift ;;
  esac
done
pass=""; if [ -n "$pfd" ]; then IFS= read -r pass <&"$pfd" || true; fi
case "$mode" in
  import) # the "key" is "STUBKEY <fingerprint> <passphrase>"
    read -r tag fpr keypass || true
    [ "$tag" = STUBKEY ] || { echo "stub gpg: no valid key data" >&2; exit 2; }
    echo "$fpr" > "$GNUPGHOME/fpr"; echo "$keypass" > "$GNUPGHOME/pass" ;;
  export) echo "STUBKEY $(cat "$GNUPGHOME/fpr") $(cat "$GNUPGHOME/pass")" ;;
  generate) echo "0123456789ABCDEF0123456789ABCDEF01234567" > "$GNUPGHOME/fpr"; echo "$pass" > "$GNUPGHOME/pass" ;;
  list) [ -f "$GNUPGHOME/fpr" ] || exit 0
    printf 'sec:u:255:22:KEYID:1:::u:::scESC:::+::ed25519:::0:\nfpr:::::::::%s:\nuid:u::::1::X::test::::::::::0:\n' "$(cat "$GNUPGHOME/fpr")" ;;
  sign)
    [ "$user" = "$(cat "$GNUPGHOME/fpr")" ] || { echo "stub gpg: no such key $user" >&2; exit 2; }
    [ "$pass" = "$(cat "$GNUPGHOME/pass")" ] || { echo "stub gpg: bad passphrase" >&2; exit 2; }
    printf 'STUBSIG %s %s\n' "$user" "$(h "${files[0]}")" > "$outf" ;;
  verify)
    read -r tag fpr sum < "${files[0]}"
    [ "$tag" = STUBSIG ] && [ "$sum" = "$(h "${files[1]}")" ] || { echo "[GNUPG:] BADSIG"; exit 1; }
    echo "[GNUPG:] VALIDSIG $fpr 2026-01-01 0 4 0 22 10 00 $fpr" ;;
  *) echo "stub gpg: nothing to do" >&2; exit 97 ;;
esac
STUB
  chmod +x "$S/bin/gpg"
  SIGNPATH="$S/bin:$PATH"
fi
# With a real gpg the agent remembers a passphrase it has seen; forget it
# before a check that depends on the passphrase being asked for again.
forget() { if [ "$REAL_GPG" = 1 ]; then GNUPGHOME="$KEYHOME" gpgconf --kill gpg-agent > /dev/null 2>&1 || true; fi; }

L="$S/log"
key() { run "$1" env PATH="$SIGNPATH" GNUPGHOME="$KEYHOME" GPG_PASSPHRASE=rehearsal bash "$here/signing-key.sh" "$2"; }
fresh() { rm -rf "$S/staging" "$S/bundles"; cp -R "$A/staging.good" "$S/staging"; }
sign() { run "$1" env PATH="$SIGNPATH" GNUPGHOME="$KEYHOME" SIGNING_KEY="${2:-$FPR}" GPG_PASSPHRASE="${3-rehearsal}" bash "$here/sign-bundles.sh" "$S/staging" "$S/bundles"; }
chk() { run "$1" env PATH="$SIGNPATH" GNUPGHOME="$KEYHOME" SIGNING_KEY="${2:-$FPR}" BASE_VERSION="${3:-1.2.3}" bash "$here/check-bundles.sh" "$S/bundles" "$S/staging"; }
NOKEY=00000000000000000000000000000000DEADBEEF

check "signing-key: refuses a keyring directory with another name" fails_with "must be a directory named release-gnupg" "$L.0" \
  env PATH="$SIGNPATH" GNUPGHOME="$S/gnupg" bash "$here/signing-key.sh" remove
check "signing-key: generate creates a throwaway key" key "$L.1" generate
FPR="$(out "$L.1" fingerprint)"
check "signing-key: ... and reports its fingerprint" test -n "$FPR"
check "signing-key: ... in a private directory" test "$(ls -ld "$KEYHOME" | cut -c1-10)" = "drwx------"

fresh; forget
check "sign: a wrong passphrase fails" fails_with "could not sign" "$L.2" sign "$L.2" "$FPR" wrong-passphrase
fresh
check "sign: an unknown key fails" fails_with "could not sign" "$L.3" sign "$L.3" "$NOKEY"
fresh
check "sign: signs and bundles the three versions" sign "$L.4"
check "sign: one bundle per version" is "$(ls "$S/bundles" | tr '\n' ' ')" "indextables_spark-1.2.3_spark_3.5.8-bundle.zip indextables_spark-1.2.3_spark_4.0.3-bundle.zip indextables_spark-1.2.3_spark_4.1.2-bundle.zip "
# The entry list of a bundle, written out in full: this is the set of files
# that is on Maven Central for 0.6.0-rc2 (36 per version).
want_entries="$(for s in .pom .jar -sources.jar -javadoc.jar -linux-x86_64-shaded.jar -jar-with-dependencies.jar; do
  for e in "" .asc .md5 .sha1 .sha256 .sha512; do echo "io/indextables/indextables_spark/1.2.3_spark_4.0.3/indextables_spark-1.2.3_spark_4.0.3$s$e"; done; done | LC_ALL=C sort)"
check "sign: a bundle holds exactly the 36 files Maven Central has for a version" is \
  "$(unzip -Z1 "$S/bundles/indextables_spark-1.2.3_spark_4.0.3-bundle.zip" | grep -v '/$' | LC_ALL=C sort)" "$want_entries"
check "sign: checksum files are not signed" test -z "$(unzip -Z1 "$S/bundles/indextables_spark-1.2.3_spark_4.0.3-bundle.zip" | grep -E '\.(md5|sha1|sha256|sha512)\.asc$')"
check "check: accepts the signed bundles" chk "$L.5"
check "check: ... and says who signed" has "$L.5.summary" "$FPR"
check "sign: a second run on the same directory is refused" fails_with "already signed" "$L.6" sign "$L.6"
check "check: signatures from another key are refused" fails_with "expected $NOKEY" "$L.7" chk "$L.7" "$NOKEY"
check "check: versions of another tag are refused" fails_with "is not 1.2.4_spark_3.5.<patch>" "$L.8" chk "$L.8" "$FPR" 1.2.4
z="$S/bundles/indextables_spark-1.2.3_spark_3.5.8-bundle.zip"
zd=io/indextables/indextables_spark/1.2.3_spark_3.5.8
cp "$z" "$z.keep"
mkdir -p "$S/x/io/indextables/tantivy4java/9.9.9" && echo x > "$S/x/io/indextables/tantivy4java/9.9.9/evil.jar"
(cd "$S/x" && zip -q "$z" io/indextables/tantivy4java/9.9.9/evil.jar)
check "check: a bundle with an extra entry is refused" fails_with "does not hold exactly the expected entries" "$L.9" chk "$L.9"
cp "$z.keep" "$z"; zip -q -d "$z" "$zd/indextables_spark-1.2.3_spark_3.5.8-javadoc.jar.asc"
check "check: a bundle with a missing signature is refused" fails_with "does not hold exactly the expected entries" "$L.10" chk "$L.10"
cp "$z.keep" "$z"
mkdir -p "$S/y/$zd" && echo 0000 > "$S/y/$zd/indextables_spark-1.2.3_spark_3.5.8.jar.sha1"
(cd "$S/y" && zip -q "$z" "$zd/indextables_spark-1.2.3_spark_3.5.8.jar.sha1")
check "check: a wrong checksum is refused" fails_with ".jar.sha1 does not match" "$L.11" chk "$L.11"
# A file replaced after signing, with its checksums recomputed to match: only
# the signature (and the comparison with the staged file) can catch it.
cp "$z.keep" "$z"
mkdir -p "$S/w/$zd" && cp "$T/fx/fat.jar" "$S/w/$zd/indextables_spark-1.2.3_spark_3.5.8.jar"
for algo in $CHECKSUM_ALGOS; do printf '%s' "$(hash_of "$algo" "$S/w/$zd/indextables_spark-1.2.3_spark_3.5.8.jar")" > "$S/w/$zd/indextables_spark-1.2.3_spark_3.5.8.jar.$algo"; done
(cd "$S/w" && zip -q -r "$z" io)
check "check: a file replaced after signing is refused, even with matching checksums" fails_with "the signature of indextables_spark-1.2.3_spark_3.5.8.jar does not verify" "$L.12" chk "$L.12"
cp "$z.keep" "$z"; rm "$z.keep"
staged_pom="$S/staging/bundles/1.2.3_spark_3.5.8/$zd/indextables_spark-1.2.3_spark_3.5.8.pom"
cp "$staged_pom" "$S/pom.keep"; echo "<!-- changed -->" >> "$staged_pom"
check "check: a bundle that differs from the staging directory is refused" fails_with "in the bundle differs from the staged file" "$L.12b" chk "$L.12b"
cp "$S/pom.keep" "$staged_pom"
check "check: the restored bundles pass again" chk "$L.13"
# Signing writes nothing but signatures into the staging directory: with
# those removed it verifies against the original manifest again.
find "$S/staging" -name '*.asc' -exec rm {} +
check "sign: nothing but signatures is written into the staging directory" run "$L.14" env MANIFEST_SHA256="$MANIFEST" BASE_VERSION=1.2.3 bash "$here/verify-staging.sh" "$S/staging"

# The publish job's path: import a passphrase-protected private key from the
# environment, then sign with it. The key is the throwaway key, exported.
PATH="$SIGNPATH" GNUPGHOME="$KEYHOME" gpg --batch --no-tty --pinentry-mode loopback --passphrase-fd 3 --armor \
  --export-secret-keys "$FPR" 3<<< rehearsal > "$S/key.asc" 2> /dev/null
check "signing-key: remove deletes the keyring" key "$L.15" remove
check "signing-key: ... it is gone" test ! -e "$KEYHOME"
check "signing-key: import without a key fails" bash -c "! PATH='$SIGNPATH' GNUPGHOME='$KEYHOME' GPG_PRIVATE_KEY= bash '$here/signing-key.sh' import"
check "signing-key: import of something that is not a key fails" fails_with "could not import the signing key" "$L.16" \
  env PATH="$SIGNPATH" GNUPGHOME="$KEYHOME" GPG_PRIVATE_KEY="not a key" bash "$here/signing-key.sh" import
check "signing-key: import reads the private key from the environment" run "$L.17" env PATH="$SIGNPATH" GNUPGHOME="$KEYHOME" GPG_PRIVATE_KEY="$(cat "$S/key.asc")" bash "$here/signing-key.sh" import
check "signing-key: ... and reports the same fingerprint" is "$(out "$L.17" fingerprint)" "$FPR"
check "signing-key: ... the key material is not echoed" test -z "$(grep -E 'BEGIN PGP|STUBKEY' "$L.17" "$L.17.out" "$L.17.summary" 2> /dev/null)"
fresh; forget
check "sign: the imported key does not sign without its passphrase" fails_with "could not sign" "$L.18" sign "$L.18" "$FPR" ""
fresh
check "sign: the imported key signs with its passphrase" sign "$L.19"
check "check: ... and the bundles pass" chk "$L.20"
key "$L.21" remove > /dev/null 2>&1
rm -f "$S/key.asc"

# ---------------------------------------------------------------------------
# central-upload.sh, against a stub curl
# ---------------------------------------------------------------------------
C="$T/central"; mkdir -p "$C/bin"
cat > "$C/bin/curl" << 'STUB'
#!/usr/bin/env bash
# Stub curl for the Portal API (https://portal.test/api) and the public
# repository (https://repo.test/maven2). Records "<method> <url> [auth]".
set -u
method=GET; out=""; fmt=""; hdr=""; form=""; url=""; failflag=0
while [ $# -gt 0 ]; do
  case "$1" in
    -X) method="$2"; shift 2 ;;
    -H) hdr="$2"; shift 2 ;;
    -o) out="$2"; shift 2 ;;
    -w) fmt="$2"; shift 2 ;;
    -F) form="$2"; shift 2 ;;
    --connect-timeout | --max-time | --retry) shift 2 ;;
    -sS) shift ;;
    -fsS) failflag=1; shift ;;
    -*) echo "stub curl: unexpected option $1" >&2; exit 97 ;;
    *) url="$1"; shift ;;
  esac
done
auth=""
case "$hdr" in
  @*) grep -q '^Authorization: Bearer ' "${hdr#@}" && auth=" auth:$(sed 's/^Authorization: Bearer //' "${hdr#@}")" ;;
  "") ;;
  *) echo "stub curl: header passed on the command line: $hdr" >> "$CFAKE/calls.log" ;;
esac
echo "$method $url$auth" >> "$CFAKE/calls.log"
respond() { # respond <code> <body>
  if [ -n "$out" ]; then printf '%s' "$2" > "$out"; else printf '%s' "$2"; fi
  [ -z "$fmt" ] || printf '%s' "$1"
}
case "$url" in
  https://repo.test/maven2/*.pom)
    v="$(basename "$(dirname "$url")")"
    respond "$(cat "$CFAKE/repo.$v.code" 2> /dev/null || echo 404)" "" ;;
  https://repo.test/maven2/*.sha1)
    f="$CFAKE/repo.$(basename "$url")"
    [ -f "$f" ] || exit 22
    cat "$f" ;;
  "https://portal.test/api/upload?name="*"&publishingType=USER_MANAGED")
    [ "$method" = POST ] || exit 97
    n="$(grep -c 'POST https://portal.test/api/upload' "$CFAKE/calls.log")"
    echo "$form" >> "$CFAKE/forms.log"
    if [ -f "$CFAKE/upload.$n.exit" ]; then exit "$(cat "$CFAKE/upload.$n.exit")"; fi
    respond "$(cat "$CFAKE/upload.$n.code" 2> /dev/null || echo 201)" "$(cat "$CFAKE/upload.$n.body" 2> /dev/null || echo "00000000-0000-4000-8000-00000000000$n")" ;;
  "https://portal.test/api/status?id="*)
    [ "$method" = POST ] || exit 97
    id="${url##*id=}"; f="$CFAKE/status.$id"
    [ -f "$f" ] || echo VALIDATED > "$f"
    st="$(head -n 1 "$f")"
    [ "$(grep -c . "$f")" -le 1 ] || { tail -n +2 "$f" > "$f.next" && mv "$f.next" "$f"; }
    case "$st" in
      HTTP*) respond "${st#HTTP}" "upstream error" ;;
      *) respond 200 "{\"deploymentId\":\"$id\",\"deploymentState\":\"$st\",\"errors\":{\"pkg\":[\"reason from the Portal\"]}}" ;;
    esac ;;
  https://portal.test/api/deployment/*)
    id="${url##*/}"
    case "$method" in
      DELETE) respond "$(cat "$CFAKE/drop.$id.code" 2> /dev/null || echo 204)" "" ;;
      *) echo "stub curl: $method on a deployment is the Publish call" >&2; exit 97 ;;
    esac ;;
  *) echo "stub curl: unexpected request: $method $url" >&2; exit 97 ;;
esac
STUB
chmod +x "$C/bin/curl"
TOKEN="$(printf 'user:pass' | base64 | tr -d '\n')"
new_central() { CFAKE="$C/fake.$1"; rm -rf "$CFAKE" "$C/state"; mkdir -p "$CFAKE"; : > "$CFAKE/calls.log"; export CFAKE; }
central() { # central <log> <subcommand args...>
  local log="$1"; shift
  run "$log" env PATH="$C/bin:$PATH" CENTRAL_USERNAME=user CENTRAL_PASSWORD=pass CENTRAL_API_URL=https://portal.test/api \
    CENTRAL_REPO_URL=https://repo.test/maven2 CENTRAL_POLL_SECONDS=0 CENTRAL_WAIT_SECONDS="${WAIT_SECONDS:-5}" bash "$here/central-upload.sh" "$@"
}
up() { central "$1" upload "$S/bundles" "$S/staging" "$C/state"; }
calls() { grep -c -- "$1" "$CFAKE/calls.log" || true; }
ID1=00000000-0000-4000-8000-000000000001; ID2=00000000-0000-4000-8000-000000000002; ID3=00000000-0000-4000-8000-000000000003
L="$C/log"

new_central ok; printf 'PENDING\nVALIDATING\nVALIDATED\n' > "$CFAKE/status.$ID1"
check "central: three uploads, each validated" up "$L.1"
check "central: ... three upload calls" is "$(calls 'POST https://portal.test/api/upload')" 3
check "central: ... as USER_MANAGED, named after the version" has "$CFAKE/calls.log" "POST https://portal.test/api/upload?name=indextables_spark-1.2.3_spark_3.5.8&publishingType=USER_MANAGED"
check "central: ... the bundle is the form field" has "$CFAKE/forms.log" "bundle=@$S/bundles/indextables_spark-1.2.3_spark_4.1.2-bundle.zip;type=application/octet-stream"
check "central: ... one version at a time: validated before the next upload" is \
  "$(grep -n -E 'upload\?name|status\?id' "$CFAKE/calls.log" | sed -e 's/^[0-9]*:POST https:\/\/portal.test\/api\///' -e 's/[?].*//' | tr '\n' ' ')" "upload status status status upload status upload status "
check "central: ... nothing is dropped" is "$(calls '^DELETE')" 0
check "central: ... the Publish call is never made" is "$(calls '^POST https://portal.test/api/deployment/')" 0
check "central: ... the token goes to the Portal as a Bearer header" is "$(grep -c "portal.test.* auth:$TOKEN\$" "$CFAKE/calls.log")" "$(grep -c 'portal.test' "$CFAKE/calls.log")"
check "central: ... and never to the public repository" is "$(grep 'repo.test' "$CFAKE/calls.log" | grep -c 'auth:')" 0
check "central: ... and never on a command line" hasnt "$CFAKE/calls.log" "header passed on the command line"
check "central: ... the encoded token is masked before use" has "$L.1" "::add-mask::$TOKEN"
check "central: ... the summary says it is not public yet" has "$L.1.summary" "Not public yet"
check "central: ... deployments are recorded" is "$(cat "$C/state/deployments.txt" | tr '\n' ' ')" "1.2.3_spark_3.5.8 $ID1 1.2.3_spark_4.0.3 $ID2 1.2.3_spark_4.1.2 $ID3 "
check "central: a second upload from the same job is refused" fails_with "refusing to upload twice" "$L.1b" up "$L.1b"
: > "$CFAKE/calls.log"
check "central: drop removes this run's deployments" central "$L.1c" drop "$C/state"
check "central: ... all three" is "$(calls '^DELETE https://portal.test/api/deployment/')" 3
check "central: ... and a second drop has nothing left to do" central "$L.1d" drop "$C/state"
check "central: ... (no further calls)" is "$(calls '^DELETE')" 3

new_central rejected; echo FAILED > "$CFAKE/status.$ID2"
check "central: a rejected bundle fails the step" fails_with "Maven Central rejected the bundle for 1.2.3_spark_4.0.3" "$L.2" up "$L.2"
check "central: ... the Portal's reasons are shown" has "$L.2" "reason from the Portal"
check "central: ... the third version is not uploaded" is "$(calls 'POST https://portal.test/api/upload')" 2
check "central: ... both deployments of this run are dropped" is "$(grep '^DELETE' "$CFAKE/calls.log" | sed 's/.*deployment\///' | sed 's/ auth.*//' | tr '\n' ' ')" "$ID1 $ID2 "
check "central: ... nothing is left recorded as waiting" test ! -s "$C/state/deployments.txt"

new_central refused; echo 401 > "$CFAKE/upload.1.code"; echo "invalid token" > "$CFAKE/upload.1.body"
check "central: a refused upload fails the step" fails_with "was refused: HTTP 401 invalid token" "$L.3" up "$L.3"
check "central: ... nothing else is attempted" is "$(calls 'POST https://portal.test/api/upload')$(calls 'status')$(calls '^DELETE')" 100

new_central broken; echo 28 > "$CFAKE/upload.2.exit"
check "central: an upload that does not complete fails the step" fails_with "may or may not have reached the Portal" "$L.4" up "$L.4"
check "central: ... the earlier deployment is dropped" is "$(grep -c "^DELETE https://portal.test/api/deployment/$ID1" "$CFAKE/calls.log")" 1

new_central garbled; echo "<html>ok</html>" > "$CFAKE/upload.1.body"
check "central: an upload answer that is not a deployment id fails the step" fails_with "unexpected response" "$L.5" up "$L.5"

new_central slow; echo VALIDATING > "$CFAKE/status.$ID1"; echo 400 > "$CFAKE/drop.$ID1.code"
WAIT_SECONDS=0
check "central: a validation that does not finish in time fails the step" fails_with "was not validated within 0 seconds" "$L.6" up "$L.6"
unset WAIT_SECONDS
check "central: ... a drop that is refused is reported, with what to do" has "$L.6" "Could not drop deployment $ID1"
check "central: ... 'do not publish it'" has "$L.6" "do not publish it"

new_central flaky; printf 'HTTP502\nHTTP502\nVALIDATED\n' > "$CFAKE/status.$ID1"
check "central: a few failed status calls are tolerated" up "$L.7"
new_central down; echo HTTP503 > "$CFAKE/status.$ID1"
check "central: status calls that keep failing fail the step" fails_with "could not be read six times in a row" "$L.8" up "$L.8"

new_central raced; echo PUBLISHED > "$CFAKE/status.$ID1"
check "central: a deployment published by hand during the run is not an error" up "$L.9"
check "central: ... but is called out" has "$L.9" "someone pressed Publish"

# Re-run after a partial publish: 3.5.8 is already public with these files.
vd="$S/staging/bundles/1.2.3_spark_3.5.8/io/indextables/indextables_spark/1.2.3_spark_3.5.8"
same_on_central() { echo 200 > "$CFAKE/repo.1.2.3_spark_3.5.8.code"; for n in $(artifact_names 1.2.3_spark_3.5.8); do hash_of sha1 "$vd/$n" > "$CFAKE/repo.$n.sha1"; done; }
new_central partial; same_on_central
check "central: a version already public with identical files is skipped" up "$L.10"
check "central: ... only the other two are uploaded" is "$(grep 'upload?name' "$CFAKE/calls.log" | sed -e 's/.*name=indextables_spark-//' -e 's/&.*//' | tr '\n' ' ')" "1.2.3_spark_4.0.3 1.2.3_spark_4.1.2 "
check "central: ... and the summary says so" has "$L.10.summary" "ALREADY_PUBLISHED"
new_central differs; same_on_central; echo 0000000000000000000000000000000000000000 > "$CFAKE/repo.indextables_spark-1.2.3_spark_3.5.8-javadoc.jar.sha1"
check "central: a version already public with other files stops the release" fails_with "cannot be replaced" "$L.11" up "$L.11"
check "central: ... before anything is uploaded" is "$(calls 'portal.test')" 0
new_central unknown; echo 503 > "$CFAKE/repo.1.2.3_spark_4.1.2.code"
check "central: an unreadable public repository stops the release" fails_with "could not tell whether 1.2.3_spark_4.1.2 is already on Maven Central" "$L.12" up "$L.12"
check "central: ... before anything is uploaded" is "$(calls 'portal.test')" 0
new_central nocreds
check "central: missing credentials fail before any upload" bash -c "! PATH='$C/bin:$PATH' CENTRAL_USERNAME= CENTRAL_PASSWORD= CENTRAL_API_URL=https://portal.test/api CENTRAL_REPO_URL=https://repo.test/maven2 bash '$here/central-upload.sh' upload '$S/bundles' '$S/staging' '$C/state'"
check "central: ... (no Portal call)" is "$(calls 'portal.test')" 0
check "central: the default endpoints are the Portal API and repo1" test -n "$(grep -F 'CENTRAL_API_URL:-https://central.sonatype.com/api/v1/publisher}' "$here/central-upload.sh")" -a -n "$(grep -F 'CENTRAL_REPO_URL:-https://repo1.maven.org/maven2}' "$here/central-upload.sh")"
check "central: AUTOMATIC publishing appears nowhere" test -z "$(grep -l 'AUTOMATIC' "$here"/*.sh | grep -v test.sh)"

# ---------------------------------------------------------------------------
# github-release.sh, against a stub gh
# ---------------------------------------------------------------------------
G="$T/gh"; mkdir -p "$G/bin"
cat > "$G/bin/gh" << 'STUB'
#!/usr/bin/env bash
# Stub gh: tag lookups and release view/create/upload/edit from $GFAKE.
set -u
echo "gh $*" >> "$GFAKE/calls.log"
case "$1 ${2:-}" in
  "api repos/o/r/git/ref/tags/"*) [ -f "$GFAKE/ref.json" ] || exit 1; cat "$GFAKE/ref.json" ;;
  "api repos/o/r/git/tags/"*) cat "$GFAKE/tagobj.sha" ;;
  "release view")
    case "$*" in
      *"--json body"*) if [ -f "$GFAKE/body.md" ]; then cat "$GFAKE/body.md"; else cat "$GFAKE/view.err" >&2; exit 1; fi ;;
      *"--json url"*) echo "https://example.invalid/releases/$3" ;;
    esac ;;
  "release upload") ;;
  "release edit") shift 3; [ "$1" = --notes-file ] && cp "$2" "$GFAKE/body.md" ;;
  "release create") printf "## What's Changed\n* a change\n" > "$GFAKE/body.md" ;;
  *) echo "stub gh: unexpected call: $*" >&2; exit 97 ;;
esac
STUB
chmod +x "$G/bin/gh"
new_gh() { GFAKE="$G/fake.$1"; rm -rf "$GFAKE"; mkdir -p "$GFAKE"; : > "$GFAKE/calls.log"; export GFAKE
  printf '{"object":{"type":"commit","sha":"%s"}}\n' "$SHA_D" > "$GFAKE/ref.json"; echo "release not found" > "$GFAKE/view.err"; }
release() { # release <log> [VAR=value...]
  local log="$1"; shift
  run "$log" env PATH="$G/bin:$PATH" GH_TOKEN=t GH_REPO=o/r TAG=v1.2.3 COMMIT="$SHA_D" PRERELEASE=false "$@" bash "$here/github-release.sh" "$A/staging.good"
}
L="$G/log"
new_gh create
check "release: creates the release when there is none" release "$L.1"
check "release: ... for an existing tag, with generated notes" has "$GFAKE/calls.log" "gh release create v1.2.3 --verify-tag --generate-notes $A/staging.good/bundles/"
check "release: ... the generated notes are kept" has "$GFAKE/body.md" "What's Changed"
check "release: ... with the three shaded jars" is "$(grep 'release create' "$GFAKE/calls.log" | tr ' ' '\n' | grep -c -- '-linux-x86_64-shaded.jar$')" 3
check "release: ... and no other file" is "$(grep 'release create' "$GFAKE/calls.log" | tr ' ' '\n' | grep -c '/bundles/')" 3
check "release: ... not marked as a pre-release" test -z "$(grep -- '--prerelease' "$GFAKE/calls.log")"
check "release: ... notes carry the build inputs" has "$GFAKE/body.md" "$T4J_COMMIT"
new_gh pre
check "release: a pre-release tag creates a pre-release" release "$L.2" PRERELEASE=true
check "release: ... flag passed" has "$GFAKE/calls.log" "--prerelease"
new_gh existing; printf 'Hand-written notes.\r\n\r\n- item\r\n\r\n' > "$GFAKE/body.md"
check "release: an existing release gets the jars" release "$L.3"
check "release: ... replacing assets of the same name" has "$GFAKE/calls.log" "gh release upload v1.2.3 --clobber"
check "release: ... it is not created again" test -z "$(grep 'release create' "$GFAKE/calls.log")"
check "release: ... its notes are kept" has "$GFAKE/body.md" "Hand-written notes."
check "release: ... and gain the build inputs" has "$GFAKE/body.md" "$T4J_COMMIT"
release "$L.4" > /dev/null 2>&1
check "release: a re-run replaces the build inputs instead of adding a second block" is "$(grep -c 'release-build-inputs:start' "$GFAKE/body.md")" 1
check "release: ... and keeps the notes" is "$(grep -c 'Hand-written notes.' "$GFAKE/body.md")" 1
new_gh moved; printf '{"object":{"type":"commit","sha":"%s"}}\n' "$SHA_A" > "$GFAKE/ref.json"
check "release: a tag that moved since the build is not released" fails_with "now points to $SHA_A" "$L.5" release "$L.5"
check "release: ... no release call is made" test -z "$(grep 'gh release' "$GFAKE/calls.log")"
new_gh annotated; printf '{"object":{"type":"tag","sha":"%s"}}\n' "$SHA_B" > "$GFAKE/ref.json"; echo "$SHA_D" > "$GFAKE/tagobj.sha"
check "release: an annotated tag is followed to its commit" release "$L.6"
new_gh gone; rm "$GFAKE/ref.json"
check "release: a tag that no longer exists is not released" fails_with "does not exist" "$L.7" release "$L.7"
new_gh error; echo "HTTP 502: Bad Gateway" > "$GFAKE/view.err"
check "release: a failed lookup does not fall through to creating a release" fails_with "could not look up release" "$L.8" release "$L.8"
check "release: ... no create call" test -z "$(grep 'release create' "$GFAKE/calls.log")"

# ---------------------------------------------------------------------------
# The whole chain with the relative paths release.yml passes, from one
# working directory, the way the jobs run it.
# ---------------------------------------------------------------------------
E="$T/chain"; mkdir -p "$E/release/.github/scripts" "$E/src"
cp -R "$here" "$E/release/.github/scripts/release"
cp "$N/pom.xml" "$E/src/pom.xml"
cp "$N/pins.txt" "$E/release/.github/scripts/release/native-pins.txt"
ES=release/.github/scripts/release
chain() { # chain <log> <command...>: run in $E with the stubs on PATH
  local log="$1"; shift
  (cd "$E" && GITHUB_OUTPUT="$log.out" GITHUB_STEP_SUMMARY="$log.summary" PATH="$C/bin:$G/bin:$T/bin:${SIGNPATH}" "$@") > "$log" 2>&1
}
L="$E/log"
check "chain: native resolve" chain "$L.1" env T4J_REPO="file://$N/t4j" POM=src/pom.xml PINS="$ES/native-pins.txt" WORK=work/native OUT=out/native M2_REPO="$E/m2-native" bash "$ES/build-native.sh" resolve
mkdir -p "$E/work/native/protoc/bin" && cp "$N/work/protoc/bin/protoc" "$E/work/native/protoc/bin/protoc" 2> /dev/null \
  || { printf '#!/bin/sh\necho "libprotoc 25.5"\n' > "$E/work/native/protoc/bin/protoc"; chmod +x "$E/work/native/protoc/bin/protoc"; }
check "chain: native build" chain "$L.2" env T4J_REPO="file://$N/t4j" POM=src/pom.xml PINS="$ES/native-pins.txt" WORK=work/native OUT=out/native M2_REPO="$E/m2-native" bash "$ES/build-native.sh" build
mkdir -p "$E/in" && cp -R "$E/out/native" "$E/in/native"
for profile in $PROFILES; do
  rm -rf "$E/src/target" "$E/src/.stamped" "$E/src/dependency-reduced-pom.xml"
  check "chain: build $profile" chain "$L.3" env SRC=src PROFILE="$profile" BASE_VERSION=1.2.3 NATIVE_DIR=in/native OUT=out/artifacts M2_REPO="$E/m2-$profile" bash "$ES/build-artifacts.sh"
  cp -R "$E/out/artifacts" "$E/in/artifacts-$profile"
done
check "chain: assemble" chain "$L.4" env IN=in OUT=staging TAG=v1.2.3 COMMIT="$SHA_D" BASE_VERSION=1.2.3 bash "$ES/assemble.sh"
CM="$(out "$L.4" manifest_sha256)"
check "chain: verify" chain "$L.5" env MANIFEST_SHA256="$CM" BASE_VERSION=1.2.3 bash "$ES/verify-staging.sh" staging
check "chain: generate a key" chain "$L.6" env GNUPGHOME="$KEYHOME" GPG_PASSPHRASE=rehearsal bash "$ES/signing-key.sh" generate
CK="$(out "$L.6" fingerprint)"
check "chain: sign" chain "$L.7" env GNUPGHOME="$KEYHOME" SIGNING_KEY="$CK" GPG_PASSPHRASE=rehearsal bash "$ES/sign-bundles.sh" staging bundles
check "chain: check" chain "$L.8" env GNUPGHOME="$KEYHOME" SIGNING_KEY="$CK" BASE_VERSION=1.2.3 bash "$ES/check-bundles.sh" bundles staging
check "chain: remove the key" chain "$L.9" env GNUPGHOME="$KEYHOME" bash "$ES/signing-key.sh" remove
new_central chain; rm -rf "$E/state"
check "chain: upload" chain "$L.10" env CENTRAL_USERNAME=user CENTRAL_PASSWORD=pass CENTRAL_API_URL=https://portal.test/api CENTRAL_REPO_URL=https://repo.test/maven2 \
  CENTRAL_POLL_SECONDS=0 bash "$ES/central-upload.sh" upload bundles staging "$E/state"
check "chain: ... three bundles went up" is "$(calls 'POST https://portal.test/api/upload')" 3
check "chain: ... as form uploads of the relative bundle paths" has "$CFAKE/forms.log" "bundle=@bundles/indextables_spark-1.2.3_spark_4.0.3-bundle.zip;type=application/octet-stream"
new_gh chain
check "chain: release" chain "$L.11" env GH_TOKEN=t GH_REPO=o/r TAG=v1.2.3 COMMIT="$SHA_D" PRERELEASE=false bash "$ES/github-release.sh" staging
check "chain: ... with the staged shaded jars" is "$(grep 'release create' "$GFAKE/calls.log" | tr ' ' '\n' | grep -c '^staging/bundles/.*-linux-x86_64-shaded.jar$')" 3

# ---------------------------------------------------------------------------
# release.yml
# ---------------------------------------------------------------------------
if command -v ruby > /dev/null 2>&1 && ruby -ryaml -rjson -e 'puts JSON.generate(YAML.load_file(ARGV[0]))' "$workflow" > "$T/wf.json" 2> /dev/null; then
  wf() { jq -r "$1" "$T/wf.json"; }
  secret_jobs="$(wf '[.jobs | to_entries[] | select(.value | tostring | test("secrets\\.")) | .key] | join(",")')"
  check "workflow: started by workflow_dispatch only" is "$(wf '(.on // .["true"]) | keys | join(",")')" workflow_dispatch
  check "workflow: dry-run is a boolean that defaults to true" is "$(wf '(.on // .["true"]).workflow_dispatch.inputs["dry-run"] | [.type, (.default | tostring)] | join(",")')" boolean,true
  check "workflow: the tag is a required input" is "$(wf '(.on // .["true"]).workflow_dispatch.inputs.tag.required | tostring')" true
  check "workflow: no permissions by default" is "$(wf '.permissions | length')" 0
  check "workflow: jobs" is "$(wf '.jobs | keys | join(",")')" assemble,build,native,plan,publish,rehearse
  check "workflow: only publish names an environment" is "$(wf '[.jobs | to_entries[] | select(.value | has("environment")) | .key + "=" + (.value.environment | tostring)] | join(",")')" publish=release
  check "workflow: only publish reads secrets" is "$secret_jobs" publish
  check "workflow: publish reads exactly the four publishing secrets" is "$(wf '[.jobs.publish | tostring | scan("secrets\\.([A-Z_]+)") | .[0]] | unique | join(",")')" CENTRAL_PASSWORD,CENTRAL_USERNAME,GPG_PASSPHRASE,GPG_PRIVATE_KEY
  check "workflow: secrets reach single steps only, never a whole job" is "$(wf '[.env // {}, (.jobs[].env // {})] | tostring | test("secrets\\.")')" false
  check "workflow: each secret goes only to the step that needs it" is "$(wf '[.jobs.publish.steps[] | select(.env // {} | tostring | test("secrets\\.")) | .name + ":" + ([.env | to_entries[] | select(.value | tostring | test("secrets\\.")) | .key] | join("+"))] | join(",")')" \
    "Import the signing key:GPG_PRIVATE_KEY,Sign and bundle:GPG_PASSPHRASE,Upload to Maven Central:CENTRAL_USERNAME+CENTRAL_PASSWORD,Drop Maven Central deployments after a failure:CENTRAL_USERNAME+CENTRAL_PASSWORD"
  check "workflow: only publish can write" is "$(wf '[.jobs | to_entries[] | select(.value.permissions != {"contents": "read"}) | .key + "=" + (.value.permissions | tostring)] | join(",")')" 'publish={"contents":"write"}'
  check "workflow: the job token is used by the GitHub Release step only" is "$(wf '[.jobs[].steps[] | select(tostring | test("github\\.token|GITHUB_TOKEN")) | .name] | join(",")')" "GitHub Release"
  cond="$(wf '.jobs.publish.if')"
  check "workflow: publish needs the plan's verdict" grep -qF "needs.plan.outputs.mode == 'publish'" <<< "$cond"
  check "workflow: ... and, independently, the raw input" grep -qF "!(github.event_name == 'workflow_dispatch' && inputs.dry-run)" <<< "$cond"
  check "workflow: ... joined with &&, no ||, no status function that would override a failed need" test -z "$(grep -E '\|\||always\(\)|failure\(\)|cancelled\(\)' <<< "$cond")"
  check "workflow: publish runs after assemble and the rehearsal" is "$(wf '.jobs.publish.needs | sort | join(",")')" assemble,plan,rehearse
  check "workflow: no other job depends on publish" is "$(wf '[.jobs[] | .needs // [] | if type == "array" then .[] else . end | select(. == "publish")] | length')" 0
  check "workflow: one publish at a time, never cancelled by a newer one" is "$(wf '.jobs.publish.concurrency | [.group, (.["cancel-in-progress"] | tostring)] | join(",")')" release-publish,false
  check "workflow: nothing is restored from or saved to a cache" is "$(wf '[.jobs[].steps[] | select(((.uses // "") | test("cache")) or ((.with // {}) | has("cache")))] | length')" 0
  check "workflow: project code is checked out only by native and build" is "$(wf '[.jobs | to_entries[] | select([.value.steps[] | select((.uses // "") | startswith("actions/checkout@")) | .with.ref // ""] | any(. == "${{ needs.plan.outputs.commit }}")) | .key] | sort | join(",")')" build,native
  check "workflow: assemble, rehearse and publish check out the release scripts only" is "$(wf '[.jobs | to_entries[] | select(.key == "assemble" or .key == "rehearse" or .key == "publish") | .value.steps[] | select((.uses // "") | startswith("actions/checkout@")) | [.with.ref, .with["sparse-checkout"]] | join(" ")] | unique | join(",")')" '${{ github.workflow_sha }} .github/scripts/release'
  check "workflow: no JDK and no Maven in assemble, rehearse or publish" is "$(wf '[.jobs | to_entries[] | select(.key == "assemble" or .key == "rehearse" or .key == "publish") | .value.steps[] | select(((.uses // "") | test("setup-java")) or ((.run // "") | test("mvn|java |cargo")))] | length')" 0
  check "workflow: publish uses two actions, checkout and download-artifact" is "$(wf '[.jobs.publish.steps[] | .uses // empty | sub("@.*"; "")] | unique | join(",")')" actions/checkout,actions/download-artifact
  check "workflow: every checkout drops its credentials" is "$(wf '[.jobs[].steps[] | select((.uses // "") | startswith("actions/checkout@")) | .with["persist-credentials"]] | (length == 8 and all(. == false))')" true
  check "workflow: every job has a timeout" is "$(wf '[.jobs[] | has("timeout-minutes")] | all')" true
  check "workflow: hosted runner, pinned image" is "$(wf '[.jobs[]["runs-on"]] | unique | join(",")')" ubuntu-24.04
  check "workflow: no expression is expanded inside a run script" is "$(wf '[.jobs[].steps[] | .run? // empty | select(test("\\$\\{\\{"))] | length')" 0
  check "workflow: build matrix lists the profiles the scripts expect" is "$(wf '.jobs.build.strategy.matrix.profile | join(" ")')" "$PROFILES"
  steps="$(wf '[.jobs.publish.steps[].name] | join("|")')"
  check "workflow: publish verifies before it imports, and removes the key before any upload" is "$steps" \
    "Checkout release scripts (workflow commit)|Keep the keyring and upload state outside the workspace|Download staging directory|Verify staging directory|Import the signing key|Sign and bundle|Check bundles|Remove the signing key|Upload to Maven Central|GitHub Release|Drop Maven Central deployments after a failure"
  check "workflow: the key is removed even when an earlier step failed" is "$(wf '.jobs.publish.steps[] | select(.name == "Remove the signing key") | .if')" "always()"
  check "workflow: rehearse runs the same verify, sign and check commands as publish" is \
    "$(wf '[.jobs.rehearse.steps[] | .run? // empty | select(test("verify-staging|sign-bundles|check-bundles"))] | join("|")')" \
    "$(wf '[.jobs.publish.steps[] | .run? // empty | select(test("verify-staging|sign-bundles|check-bundles"))] | join("|")')"
  check "workflow: both take the manifest digest from the assemble job's output" is "$(wf '[.jobs.rehearse, .jobs.publish | .steps[] | select(.name == "Verify staging directory") | .env.MANIFEST_SHA256] | unique | join(",")')" '${{ needs.assemble.outputs.manifest-sha256 }}'
  check "workflow: plan gets the inputs as strings, through the environment" is "$(wf '.jobs.plan.steps[] | select(.id == "plan") | .env | [.INPUT_TAG, .INPUT_DRY_RUN, .EVENT_NAME, .REF] | join(" ")')" '${{ inputs.tag }} ${{ inputs.dry-run }} ${{ github.event_name }} ${{ github.ref }}'
  check "workflow: no continue-on-error anywhere" is "$(wf '[.. | objects | select(has("continue-on-error"))] | length')" 0
  # Every `run:` step, as the runner would start it (bash -eo pipefail).
  # Steps that call a release script must name one that exists; the other
  # steps are small enough to execute here.
  missing=""
  while IFS= read -r script; do
    [ -f "$here/$script" ] || missing="$missing $script"
  done < <(wf '.jobs[].steps[] | .run? // empty' | grep -o -E '(\$SCRIPTS|\.github/scripts/release)/[a-z-]+\.sh' | sed 's|.*/||' | sort -u)
  check "workflow: every script a step calls exists" test -z "$missing"
  check "workflow: every run step either calls one release script or is one of the three inline steps" is \
    "$(wf '[.jobs[].steps[] | select(has("run")) | select(.run | test("^bash (\"\\$SCRIPTS|\\.github/scripts/release)/[a-z-]+\\.sh\"?( [a-z-]+| \"\\$CENTRAL_STATE\")*\\n?$") | not) | .name] | join("|")')" \
    "Keep the keyring outside the workspace|Summary|Keep the keyring and upload state outside the workspace"
  inline() { # inline <job> <step name> [VAR=value...]: run that step's script
    local job="$1" name="$2"; shift 2
    wf ".jobs.$job.steps[] | select(.name == \"$name\") | .run" > "$T/inline.sh"
    env RUNNER_TEMP="$T/rt" GITHUB_ENV="$T/inline.env" GITHUB_STEP_SUMMARY="$T/inline.summary" "$@" bash --noprofile --norc -eo pipefail "$T/inline.sh"
  }
  : > "$T/inline.env"
  check "workflow: rehearse sets the keyring directory" inline rehearse "Keep the keyring outside the workspace"
  check "workflow: ... to a directory signing-key.sh accepts" is "$(cat "$T/inline.env")" "GNUPGHOME=$T/rt/release-gnupg"
  : > "$T/inline.env"
  check "workflow: publish sets the keyring and state directories" inline publish "Keep the keyring and upload state outside the workspace"
  check "workflow: ... both outside the workspace" is "$(tr '\n' ' ' < "$T/inline.env")" "GNUPGHOME=$T/rt/release-gnupg CENTRAL_STATE=$T/rt/central-state "
  : > "$T/inline.summary"
  check "workflow: rehearse summary step, dry run" inline rehearse Summary MODE=dry-run
  check "workflow: ... says nothing was uploaded" has "$T/inline.summary" "nothing was uploaded and no release was created"
  : > "$T/inline.summary"
  check "workflow: rehearse summary step, publish run" inline rehearse Summary MODE=publish
  check "workflow: ... says approval comes next" has "$T/inline.summary" "waits for approval"
  check "workflow: ... and does not claim a dry run" hasnt "$T/inline.summary" "Dry run complete"
elif [ "${REQUIRE_WORKFLOW_CHECKS:-0}" = "1" ]; then
  bad "workflow: checks could not run (ruby not available, or release.yml does not parse)"
else
  echo "skip  workflow checks (ruby not available)"
fi

# Pinned actions and runner image, in this workflow and the one that runs
# these tests.
unpinned=""
for f in "$workflow" "$root/.github/workflows/release-scripts.yml"; do
  n=$(grep -c -E '^[[:space:]]*(- )?uses:' "$f")
  m=$(grep -c -E '^[[:space:]]*(- )?uses: [A-Za-z0-9._/-]+@[0-9a-f]{40} # v[0-9]+(\.[0-9]+)*$' "$f")
  if [ "$n" -eq 0 ] || [ "$n" -ne "$m" ]; then unpinned="$unpinned $(basename "$f")"; fi
done
check "pins: every action is a full commit id with its version tag in a comment" test -z "$unpinned"
check "pins: no floating runner label" test -z "$(grep -h -E 'runs-on:' "$workflow" "$root/.github/workflows/release-scripts.yml" | grep -v -E 'runs-on: ubuntu-24\.04$')"
check "scripts: none enables command tracing" test -z "$(grep -l -E '^[[:space:]]*set -[a-z]*x|xtrace' "$here"/*.sh | grep -v test.sh)"
check "scripts: no installer is piped into a shell" test -z "$(grep -n -E 'curl[^|]*\|[[:space:]]*(ba)?sh' "$here"/*.sh | grep -v test.sh)"

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
