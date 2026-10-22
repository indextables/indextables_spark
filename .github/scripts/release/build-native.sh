#!/usr/bin/env bash
# Build the tantivy4java linux-x86_64 jar from source, from pinned commits,
# with nothing restored from a cache.
#
#   build-native.sh resolve          read pom.xml and native-pins.txt, check the
#                                    tag, fetch the pinned commit, check its
#                                    Cargo.lock against the pinned fork commits
#   build-native.sh toolchain        install the pinned protoc, check and record
#                                    the rest of the toolchain
#   build-native.sh build            build, collect the jars, record digests
#   build-native.sh print-pin <ver>  print a native-pins.txt line for a version,
#                                    after checking where the fork commits
#                                    it names can be reached from
#
# Environment (resolve, toolchain, build):
#   POM    pom.xml of the commit being released
#   PINS   native-pins.txt (from the workflow's own commit)
#   WORK   scratch directory, kept between the three subcommands
#   OUT    output directory; becomes the job's artifact:
#            native-inputs.txt   every resolved input and tool version
#            repository/         the files Maven installed for tantivy4java
#            SHA256SUMS          digests of everything above
#
# This script holds no credentials and reads none.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
. "$here/lib.sh"

# Same release and digest as scripts/setup.sh (test.sh checks that they agree).
PROTOC_VERSION=25.5
PROTOC_SHA256=e1ed237a17b2e851cf9662cb5ad02b46e70ff8e060e05984725bc4b4228c6b28
PROTOC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip"

# tag_commit <version>: the commit refs/tags/v<version> points to right now.
tag_commit() {
  local refs commit
  refs="$(git ls-remote "$TANTIVY4JAVA_URL" "refs/tags/v$1" "refs/tags/v$1^{}")" \
    || die "could not list tags of $TANTIVY4JAVA_URL"
  # An annotated tag has a second, peeled line; that is the commit.
  commit="$(printf '%s\n' "$refs" | awk '$2 ~ /\^\{\}$/ { print $1 }' | head -n 1)"
  [ -n "$commit" ] || commit="$(printf '%s\n' "$refs" | awk 'NF == 2 { print $1 }' | head -n 1)"
  [[ "$commit" =~ $SHA_RE ]] || die "tantivy4java has no tag v$1"
  printf '%s\n' "$commit"
}

# fetch_commit <commit> <dir>: a checkout of exactly that commit.
fetch_commit() {
  rm -rf "$2"
  git init -q "$2"
  git -C "$2" remote add origin "$TANTIVY4JAVA_URL"
  git -C "$2" fetch -q --depth 1 origin "$1" || die "could not fetch tantivy4java commit $1"
  git -C "$2" -c advice.detachedHead=false checkout -q --detach FETCH_HEAD
  [ "$(git -C "$2" rev-parse HEAD)" = "$1" ] || die "fetched commit is not $1"
}

# lock_git_sources <Cargo.lock>: "<repository url> <commit>" for every git
# dependency the lock file resolves, one line each.
lock_git_sources() {
  sed -n 's/^source = "git+\(.*\)"$/\1/p' "$1" | sort -u | while IFS= read -r line; do
    commit="${line##*#}"
    url="${line%%[?#]*}"
    url="${url%/}"
    url="${url%.git}"
    if ! [[ "$commit" =~ $SHA_RE ]]; then
      echo "::error::Cargo.lock has a git dependency without a resolved commit: $line" >&2
      exit 1
    fi
    printf '%s %s\n' "$url" "$commit"
  done | sort -u
}

# fork_commit <sources> <url>: the one commit a fork is resolved to, or "-".
fork_commit() {
  local commits count
  commits="$(printf '%s\n' "$1" | awk -v u="$2" '$1 == u { print $2 }' | sort -u)"
  count="$(printf '%s' "$commits" | grep -c . || true)"
  case "$count" in
    0) echo "-" ;;
    1) printf '%s\n' "$commits" ;;
    *) die "Cargo.lock resolves $2 to more than one commit: $(echo $commits)" ;;
  esac
}

# check_sources <checkout>: nothing may redirect a dependency to a location
# that the lock file does not describe.
check_sources() {
  local toml="$1/native/Cargo.toml" cfg
  [ -f "$toml" ] && [ -f "$1/native/Cargo.lock" ] || die "tantivy4java checkout has no native/Cargo.toml and native/Cargo.lock"
  if grep -nE '^[^#]*path[[:space:]]*=[[:space:]]*"(/|\.\./\.\./)' "$toml"; then
    die "native/Cargo.toml depends on a path outside the tantivy4java checkout (above); the release build only supports dependencies pinned in Cargo.lock"
  fi
  for cfg in "$1/.cargo/config.toml" "$1/.cargo/config" "$1/native/.cargo/config.toml" "$1/native/.cargo/config"; do
    [ -f "$cfg" ] || continue
    if grep -nE '^[[:space:]]*(paths[[:space:]]*=|\[patch|\[source)' "$cfg"; then
      die "$cfg overrides dependency sources (above); the release build does not support that"
    fi
  done
}

# fork_reachability <name> <url> <commit>: say (on standard error) where a
# fork commit can be reached from. A commit id alone proves little: GitHub
# serves a commit that exists only in someone's fork of a repository through
# the repository's own URL, so a pin must be on a branch or tag of the fork
# itself, and normally on its default branch.
fork_reachability() {
  local name="$1" url="$2" commit="$3" dir default refs
  [ "$commit" != "-" ] || return 0
  dir="$tmp/$name.git"
  git clone -q --bare --filter=tree:0 "$url" "$dir" 2> /dev/null || die "could not clone $url to check commit $commit"
  default="$(git -C "$dir" symbolic-ref --short HEAD)"
  if git -C "$dir" merge-base --is-ancestor "$commit" "refs/heads/$default" 2> /dev/null; then
    echo "$name $commit: reachable from the default branch ($default) of $url" >&2
    return 0
  fi
  refs="$(git -C "$dir" for-each-ref --contains "$commit" --format='%(refname:short)' refs/heads refs/tags 2> /dev/null | tr '\n' ' ' || true)"
  [ -n "$refs" ] \
    || die "$name commit $commit is not reachable from any branch or tag of $url. It may exist only in a fork of that repository. Do not pin it."
  warn "$name $commit is NOT reachable from the default branch ($default) of $url; it is on: $refs. Pin it only if building from that branch is intended."
}

cmd="${1:-}"
case "$cmd" in
  print-pin)
    version="${2:-}"
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "usage: build-native.sh print-pin <tantivy4java version>"
    need git
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    commit="$(tag_commit "$version")"
    fetch_commit "$commit" "$tmp/src"
    check_sources "$tmp/src"
    sources="$(lock_git_sources "$tmp/src/native/Cargo.lock")"
    quickwit="$(fork_commit "$sources" "$QUICKWIT_URL")"
    tantivy="$(fork_commit "$sources" "$TANTIVY_URL")"
    echo "tantivy4java $commit: what tag v$version of $TANTIVY4JAVA_URL points to" >&2
    fork_reachability quickwit "$QUICKWIT_URL" "$quickwit"
    fork_reachability tantivy "$TANTIVY_URL" "$tantivy"
    echo "$version $commit $quickwit $tantivy"
    exit 0
    ;;
  resolve | toolchain | build) ;;
  *) die "usage: build-native.sh resolve|toolchain|build|print-pin <version>" ;;
esac

: "${POM:?}" "${PINS:?}" "${WORK:?}" "${OUT:?}"
inputs="$OUT/native-inputs.txt"
src="$WORK/tantivy4java"
protoc_dir="$WORK/protoc"
record() { printf '%s=%s\n' "$1" "$2" >> "$inputs"; }

case "$cmd" in
  resolve)
    need git
    [ -f "$POM" ] || die "no pom.xml at $POM"
    [ -f "$PINS" ] || die "no pin file at $PINS"
    mkdir -p "$WORK" "$OUT"
    : > "$inputs"

    version="$(pom_tantivy4java_version "$POM")"
    pin="$(awk -v v="$version" '!/^[[:space:]]*#/ && $1 == v' "$PINS")"
    [ "$(printf '%s' "$pin" | grep -c . || true)" = 1 ] \
      || die "native-pins.txt must have exactly one line for tantivy4java $version (see the comment in that file)"
    # shellcheck disable=SC2086
    set -- $pin
    [ $# = 4 ] || die "malformed pin line for tantivy4java $version: $pin"
    want_commit="$2" want_quickwit="$3" want_tantivy="$4"
    [[ "$want_commit" =~ $SHA_RE ]] || die "pinned tantivy4java commit is not a full commit id: $want_commit"
    for c in "$want_quickwit" "$want_tantivy"; do
      [ "$c" = "-" ] || [[ "$c" =~ $SHA_RE ]] || die "pinned fork commit is not a full commit id or '-': $c"
    done

    now="$(tag_commit "$version")"
    [ "$now" = "$want_commit" ] \
      || die "tantivy4java tag v$version points to $now, but native-pins.txt pins $want_commit. The tag has moved or the pin is wrong; do not release until that is explained."

    fetch_commit "$want_commit" "$src"
    check_sources "$src"
    sources="$(lock_git_sources "$src/native/Cargo.lock")"
    quickwit="$(fork_commit "$sources" "$QUICKWIT_URL")"
    tantivy="$(fork_commit "$sources" "$TANTIVY_URL")"
    [ "$quickwit" = "$want_quickwit" ] \
      || die "Cargo.lock resolves the quickwit fork to $quickwit, but native-pins.txt pins $want_quickwit"
    [ "$tantivy" = "$want_tantivy" ] \
      || die "Cargo.lock resolves the tantivy fork to $tantivy, but native-pins.txt pins $want_tantivy"

    record tantivy4java_version "$version"
    record tantivy4java_tag "v$version"
    record tantivy4java_commit "$want_commit"
    record quickwit_commit "$quickwit"
    record tantivy_commit "$tantivy"
    record other_git_dependencies "$(printf '%s\n' "$sources" | awk -v q="$QUICKWIT_URL" -v t="$TANTIVY_URL" \
      '$1 != q && $1 != t && NF == 2 { printf "%s%s@%s", sep, $1, $2; sep = " " }')"
    record cargo_lock_sha256 "$(hash_of sha256 "$src/native/Cargo.lock")"
    record cargo_lock_packages "$(grep -c '^\[\[package\]\]' "$src/native/Cargo.lock" || true)"
    cat "$inputs"
    ;;

  toolchain)
    [ -s "$inputs" ] || die "run 'build-native.sh resolve' first"
    need java mvn cargo rustc curl unzip
    # Rust comes from the runner image; it is recorded, not installed here.
    # (No installer is piped into a shell in the release path.)

    rm -rf "$protoc_dir" "$WORK/protoc.zip"
    curl -fsSL --retry 3 -o "$WORK/protoc.zip" "$PROTOC_URL" || die "could not download $PROTOC_URL"
    got="$(hash_of sha256 "$WORK/protoc.zip")"
    [ "$got" = "$PROTOC_SHA256" ] \
      || die "protoc ${PROTOC_VERSION} download has SHA-256 $got, expected $PROTOC_SHA256"
    mkdir -p "$protoc_dir"
    unzip -q "$WORK/protoc.zip" -d "$protoc_dir"
    chmod +x "$protoc_dir/bin/protoc"
    rm -f "$WORK/protoc.zip"

    # Needed by the openssl-sys crate. Present on the hosted image; installed
    # from the distribution's archive (and recorded) if it ever is not.
    if command -v pkg-config > /dev/null 2>&1 && pkg-config --exists openssl; then
      record system_packages_installed "none"
    else
      warn "OpenSSL development files are not on the runner image; installing libssl-dev and pkg-config"
      sudo apt-get update -qq
      sudo apt-get install -y libssl-dev pkg-config
      record system_packages_installed "libssl-dev pkg-config"
    fi

    record runner_image "${ImageOS:-unknown} ${ImageVersion:-unknown}"
    record rustc "$(rustc --version)"
    record cargo "$(cargo --version)"
    record protoc "$("$protoc_dir/bin/protoc" --version) (sha256 $PROTOC_SHA256)"
    record openssl "$(pkg-config --modversion openssl)"
    record java "$("${JAVA_HOME:+$JAVA_HOME/bin/}java" -version 2>&1 | head -n 1)"
    record maven "$(mvn --version 2> /dev/null | head -n 1)"
    cat "$inputs"
    ;;

  build)
    [ -s "$inputs" ] || die "run 'build-native.sh resolve' first"
    [ -x "$protoc_dir/bin/protoc" ] || die "run 'build-native.sh toolchain' first"
    need git mvn cargo unzip
    version="$(kv "$inputs" tantivy4java_version)"
    dest="$M2_REPO/io/indextables/tantivy4java/$version"
    jar="$dest/tantivy4java-$version-$NATIVE_CLASSIFIER.jar"

    # Nothing built earlier may be reused: the build below is the only source
    # of the jar that is collected.
    if [ -e "$dest" ]; then
      warn "tantivy4java $version was already in the local Maven repository; removing it before the build"
      rm -rf "$dest"
    fi

    # The pinned protoc first on PATH, the way scripts/setup.sh provides it on
    # macOS: protoc finds its bundled include/ directory next to its bin/.
    protoc_bin="$(cd "$protoc_dir/bin" && pwd)"
    export PATH="$protoc_bin:$PATH"
    (cd "$src" && mvn -B -ntp "-Dmaven.repo.local=$M2_REPO" clean install -DskipTests)

    [ -f "$jar" ] || die "the build did not install $jar"
    # cargo rewrites Cargo.lock when it cannot honour it. Unchanged means the
    # library was compiled from exactly the dependency versions recorded there.
    [ -z "$(git -C "$src" status --porcelain -- native/Cargo.lock)" ] \
      || die "the build modified native/Cargo.lock: the library was not built from the locked dependency versions"
    record cargo_lock_unchanged true

    libs="$(unzip -Z1 "$jar" | grep -E '\.so$' || true)"
    [ -n "$libs" ] || die "$jar contains no native library (*.so)"
    printf '%s\n' "$libs" | while IFS= read -r lib; do
      record "native_library" "$lib sha256 $(unzip -p "$jar" "$lib" | hash_stream sha256)"
    done

    rm -rf "$OUT/repository"
    mkdir -p "$OUT/repository"
    for f in "$dest"/*; do
      case "$(basename "$f")" in
        _remote.repositories | *.lastUpdated | maven-metadata*) ;;
        *) [ -f "$f" ] && cp "$f" "$OUT/repository/" ;;
      esac
    done
    record tantivy4java_jar_sha256 "$(hash_of sha256 "$OUT/repository/$(basename "$jar")")"

    write_sums "$OUT"

    {
      echo "### tantivy4java native build (from source, no cache)"
      echo
      echo "| Input | Value |"
      echo "|---|---|"
      while IFS='=' read -r key value; do
        echo "| ${key//_/ } | \`$value\` |"
      done < "$inputs"
    } | summary
    ;;
esac
