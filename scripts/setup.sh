#!/usr/bin/env bash

# setup.sh - Install and verify development dependencies for IndexTables4Spark
#
# Usage:
#   ./scripts/setup.sh          # Install/verify all dependencies
#   ./scripts/setup.sh -h       # Show help
#
# Supports macOS (Homebrew) and Linux (apt-get, yum, dnf).

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REQUIRED_JAVA_MAJOR=11

# tantivy4java build-from-source configuration
TANTIVY4JAVA_REPO="https://github.com/indextables/tantivy4java.git"
QUICKWIT_REPO="https://github.com/indextables/quickwit.git"
TANTIVY_REPO="https://github.com/indextables/tantivy.git"

# ---------------------------------------------------------------------------
# Extract tantivy4java version from pom.xml (single source of truth)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
POM_FILE="$PROJECT_ROOT/pom.xml"

if [[ ! -f "$POM_FILE" ]]; then
    echo "[ERROR] pom.xml not found at $POM_FILE" >&2
    echo "[ERROR] Run this script from the project root or its scripts/ directory." >&2
    exit 1
fi

# Extract the version from the tantivy4java dependency block in pom.xml.
# Looks for: <artifactId>tantivy4java</artifactId> followed by <version>X.Y.Z</version>
TANTIVY4JAVA_VERSION=$(
    grep -A2 '<artifactId>tantivy4java</artifactId>' "$POM_FILE" \
    | grep '<version>' \
    | sed 's/.*<version>\(.*\)<\/version>.*/\1/' \
    | tr -d '[:space:]'
)

if [[ -z "$TANTIVY4JAVA_VERSION" ]]; then
    echo "[ERROR] Could not extract tantivy4java version from $POM_FILE" >&2
    echo "[ERROR] Expected a <dependency> block with <artifactId>tantivy4java</artifactId> and a <version> element." >&2
    exit 1
fi

TANTIVY4JAVA_TAG="v${TANTIVY4JAVA_VERSION}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "[INFO]  $*"; }
warn()  { echo "[WARN]  $*" >&2; }
error() { echo "[ERROR] $*" >&2; }
ok()    { echo "[OK]    $*"; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            echo "Usage: $0"
            echo ""
            echo "Install and verify development dependencies for IndexTables4Spark."
            echo ""
            echo "Dependencies installed:"
            echo "  - Java 11 (OpenJDK 11)"
            echo "  - Maven"
            echo "  - Rust toolchain (rustc, cargo via rustup)"
            echo "  - Protobuf compiler (protoc)"
            echo "  - tantivy4java ${TANTIVY4JAVA_VERSION} (built from source)"
            echo ""
            echo "Supported platforms:"
            echo "  - macOS (via Homebrew)"
            echo "  - Linux (apt-get, yum, or dnf)"
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            echo "Run '$0 --help' for usage." >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# OS detection
# ---------------------------------------------------------------------------
OS="$(uname -s)"
case "$OS" in
    Darwin) PLATFORM="macos" ;;
    Linux)  PLATFORM="linux" ;;
    *)
        error "Unsupported operating system: $OS"
        error "This script supports macOS (Darwin) and Linux."
        exit 1
        ;;
esac

info "Detected platform: $PLATFORM ($OS)"

# ---------------------------------------------------------------------------
# Platform classifier (matches Maven profile logic in pom.xml)
# ---------------------------------------------------------------------------
ARCH="$(uname -m)"
case "${OS}-${ARCH}" in
    Darwin-arm64)   PLATFORM_CLASSIFIER="darwin-aarch64" ;;
    Darwin-x86_64)  PLATFORM_CLASSIFIER="darwin-x86_64" ;;
    Linux-x86_64)   PLATFORM_CLASSIFIER="linux-x86_64" ;;
    Linux-aarch64)  PLATFORM_CLASSIFIER="linux-aarch64" ;;
    *)
        error "Unsupported OS/architecture combination: ${OS}-${ARCH}"
        exit 1
        ;;
esac

info "Platform classifier: $PLATFORM_CLASSIFIER"

# ---------------------------------------------------------------------------
# Java version detection
# ---------------------------------------------------------------------------
# Returns the major Java version (e.g. 11, 17, 21) or empty string if not found.
get_java_major_version() {
    local java_cmd="$1"
    if ! command -v "$java_cmd" &>/dev/null; then
        echo ""
        return
    fi
    # java -version outputs to stderr; parse "11.0.x" or "1.8.0" style
    local version_output
    version_output=$("$java_cmd" -version 2>&1 | head -1)
    # Extract version string between quotes
    local version
    version=$(echo "$version_output" | sed -n 's/.*"\(.*\)".*/\1/p')
    if [[ -z "$version" ]]; then
        echo ""
        return
    fi
    # Handle 1.x.y (Java 8 and earlier) vs x.y.z (Java 9+)
    local major
    major=$(echo "$version" | cut -d. -f1)
    if [[ "$major" == "1" ]]; then
        major=$(echo "$version" | cut -d. -f2)
    fi
    echo "$major"
}

# Check if Java 11 is already available
check_java() {
    # First check JAVA_HOME if set
    if [[ -n "${JAVA_HOME:-}" ]] && [[ -x "${JAVA_HOME}/bin/java" ]]; then
        local major
        major=$(get_java_major_version "${JAVA_HOME}/bin/java")
        if [[ "$major" == "$REQUIRED_JAVA_MAJOR" ]]; then
            return 0
        fi
    fi
    # Then check java on PATH
    if command -v java &>/dev/null; then
        local major
        major=$(get_java_major_version java)
        if [[ "$major" == "$REQUIRED_JAVA_MAJOR" ]]; then
            return 0
        fi
    fi
    return 1
}

# Check if Maven is available
check_maven() {
    command -v mvn &>/dev/null
}

# Check if Rust toolchain is available
check_rust() {
    command -v rustc &>/dev/null && command -v cargo &>/dev/null
}

# Check if protoc is available
check_protoc() {
    command -v protoc &>/dev/null
}

# Check if tantivy4java JAR exists in local Maven cache
check_tantivy4java() {
    local jar_path
    jar_path="$HOME/.m2/repository/io/indextables/tantivy4java/${TANTIVY4JAVA_VERSION}/tantivy4java-${TANTIVY4JAVA_VERSION}-${PLATFORM_CLASSIFIER}.jar"
    [[ -f "$jar_path" ]]
}

# ---------------------------------------------------------------------------
# macOS installation (Homebrew)
# ---------------------------------------------------------------------------
install_macos() {
    info "Checking Homebrew..."
    if ! command -v brew &>/dev/null; then
        error "Homebrew is not installed."
        error "Install it from https://brew.sh and re-run this script."
        error "  /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
        exit 1
    fi
    ok "Homebrew found: $(brew --prefix)"

    # On macOS, openjdk@11 is keg-only and may not be on PATH.
    # Check the Homebrew keg path directly before deciding to install.
    if [[ -z "${JAVA_HOME:-}" ]]; then
        for brew_prefix in /opt/homebrew /usr/local; do
            local keg_java="${brew_prefix}/opt/openjdk@11/bin/java"
            if [[ -x "$keg_java" ]]; then
                local keg_major
                keg_major=$(get_java_major_version "$keg_java")
                if [[ "$keg_major" == "$REQUIRED_JAVA_MAJOR" ]]; then
                    export JAVA_HOME="${brew_prefix}/opt/openjdk@11"
                    break
                fi
            fi
        done
    fi

    # --- Java 11 ---
    if check_java; then
        ok "Java $REQUIRED_JAVA_MAJOR already installed"
    else
        info "Installing OpenJDK $REQUIRED_JAVA_MAJOR via Homebrew..."
        brew install openjdk@11
        ok "OpenJDK $REQUIRED_JAVA_MAJOR installed"
    fi

    # --- Maven ---
    if check_maven; then
        ok "Maven already installed"
    else
        info "Installing Maven via Homebrew..."
        brew install maven
        ok "Maven installed"
    fi

    # --- Protobuf compiler ---
    if check_protoc; then
        ok "Protobuf compiler (protoc) already installed"
    else
        info "Installing protobuf via Homebrew..."
        brew install protobuf
        ok "Protobuf compiler installed"
    fi
}

# ---------------------------------------------------------------------------
# Linux installation (apt-get / yum / dnf)
# ---------------------------------------------------------------------------
install_linux() {
    # Detect package manager
    local pkg_mgr=""
    if command -v apt-get &>/dev/null; then
        pkg_mgr="apt-get"
    elif command -v dnf &>/dev/null; then
        pkg_mgr="dnf"
    elif command -v yum &>/dev/null; then
        pkg_mgr="yum"
    else
        error "No supported package manager found (apt-get, dnf, yum)."
        exit 1
    fi
    info "Package manager: $pkg_mgr"

    # Check sudo availability
    local sudo_cmd=""
    if [[ "$(id -u)" -ne 0 ]]; then
        if command -v sudo &>/dev/null; then
            sudo_cmd="sudo"
        else
            error "Not running as root and 'sudo' is not available."
            error "Please run as root or install sudo."
            exit 1
        fi
    fi

    # --- Java 11 ---
    if check_java; then
        ok "Java $REQUIRED_JAVA_MAJOR already installed"
    else
        info "Installing OpenJDK $REQUIRED_JAVA_MAJOR..."
        case "$pkg_mgr" in
            apt-get)
                $sudo_cmd apt-get update -qq
                $sudo_cmd apt-get install -y openjdk-11-jdk
                ;;
            dnf)
                $sudo_cmd dnf install -y java-11-openjdk-devel
                ;;
            yum)
                $sudo_cmd yum install -y java-11-openjdk-devel
                ;;
        esac
        ok "OpenJDK $REQUIRED_JAVA_MAJOR installed"
    fi

    # --- Maven ---
    if check_maven; then
        ok "Maven already installed"
    else
        info "Installing Maven..."
        case "$pkg_mgr" in
            apt-get)
                $sudo_cmd apt-get install -y maven
                ;;
            dnf)
                $sudo_cmd dnf install -y maven
                ;;
            yum)
                $sudo_cmd yum install -y maven
                ;;
        esac
        ok "Maven installed"
    fi

    # --- Protobuf compiler ---
    if check_protoc; then
        ok "Protobuf compiler (protoc) already installed"
    else
        info "Installing protobuf compiler..."
        case "$pkg_mgr" in
            apt-get)
                $sudo_cmd apt-get install -y protobuf-compiler
                ;;
            dnf)
                $sudo_cmd dnf install -y protobuf-compiler
                ;;
            yum)
                $sudo_cmd yum install -y protobuf-compiler
                ;;
        esac
        ok "Protobuf compiler installed"
    fi
}

# ---------------------------------------------------------------------------
# Rust toolchain installation (cross-platform via rustup)
# ---------------------------------------------------------------------------
install_rust() {
    if check_rust; then
        ok "Rust toolchain already installed: $(rustc --version)"
        return
    fi

    info "Installing Rust toolchain via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # Source cargo environment for the rest of this script
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env" 2>/dev/null || export PATH="$HOME/.cargo/bin:$PATH"
    ok "Rust toolchain installed: $(rustc --version)"
}

# Ensure cargo is discoverable by Maven's exec-maven-plugin.
# Maven spawns subprocesses that may not inherit the shell PATH, so we
# symlink cargo/rustc into a directory that is on the default system PATH.
ensure_cargo_on_system_path() {
    # Check if cargo is already in a system PATH directory that Maven
    # subprocesses would have on their default PATH.  We cannot rely on
    # `command -v cargo` because install_rust() already sourced
    # $HOME/.cargo/env into the current shell, which makes cargo appear
    # reachable even though Maven subprocesses won't inherit that PATH.
    local system_dirs=("/usr/local/bin" "/usr/bin")
    if [[ "$PLATFORM" == "macos" ]] && command -v brew &>/dev/null; then
        system_dirs=("$(brew --prefix)/bin" "${system_dirs[@]}")
    fi

    for dir in "${system_dirs[@]}"; do
        if [[ -x "$dir/cargo" ]]; then
            ok "cargo already on system PATH: $dir/cargo"
            return
        fi
    done

    local cargo_bin="$HOME/.cargo/bin/cargo"
    local rustc_bin="$HOME/.cargo/bin/rustc"
    if [[ ! -x "$cargo_bin" ]]; then
        error "cargo not found at $cargo_bin"
        exit 1
    fi

    # Pick a target directory that is on the default PATH
    local target_dir=""
    if [[ "$PLATFORM" == "macos" ]]; then
        target_dir="$(brew --prefix 2>/dev/null)/bin"
    fi
    if [[ -z "$target_dir" ]] || [[ ! -d "$target_dir" ]]; then
        target_dir="/usr/local/bin"
    fi

    if [[ -w "$target_dir" ]]; then
        ln -sf "$cargo_bin" "$target_dir/cargo"
        ln -sf "$rustc_bin" "$target_dir/rustc"
        ok "Symlinked cargo/rustc into $target_dir"
    else
        warn "Cannot write to $target_dir; trying with sudo..."
        sudo ln -sf "$cargo_bin" "$target_dir/cargo"
        sudo ln -sf "$rustc_bin" "$target_dir/rustc"
        ok "Symlinked cargo/rustc into $target_dir (via sudo)"
    fi
}

# ---------------------------------------------------------------------------
# tantivy4java build-from-source
# ---------------------------------------------------------------------------
install_tantivy4java() {
    if check_tantivy4java; then
        local jar_path="$HOME/.m2/repository/io/indextables/tantivy4java/${TANTIVY4JAVA_VERSION}/tantivy4java-${TANTIVY4JAVA_VERSION}-${PLATFORM_CLASSIFIER}.jar"
        ok "tantivy4java ${TANTIVY4JAVA_VERSION} (${PLATFORM_CLASSIFIER}) already in local Maven cache"
        return
    fi

    info "Building tantivy4java ${TANTIVY4JAVA_VERSION} from source..."
    info "This requires Rust, protoc, Java 11, and Maven (already verified above)."

    # Create temp build directory
    local build_dir
    build_dir=$(mktemp -d "${TMPDIR:-/tmp}/tantivy4java-build.XXXXXX")
    # Ensure cleanup on any exit (build dirs can be multi-GB)
    trap "rm -rf '$build_dir'" EXIT
    info "Build directory: $build_dir"

    # Clone tantivy4java at the specified tag
    info "Cloning tantivy4java (tag: ${TANTIVY4JAVA_TAG})..."
    git clone --depth 1 --branch "$TANTIVY4JAVA_TAG" "$TANTIVY4JAVA_REPO" "$build_dir/tantivy4java"

    # Clone sibling repos required by Cargo path dependencies in tantivy4java.
    # The tantivy4java native/Cargo.toml uses path deps like ../../quickwit/...
    # and may also use ../../tantivy/... depending on the version.
    local cargo_toml="$build_dir/tantivy4java/native/Cargo.toml"

    # --- quickwit (always required) ---
    info "Cloning quickwit (HEAD)..."
    git clone --depth 1 "$QUICKWIT_REPO" "$build_dir/quickwit"

    # --- tantivy (required if Cargo.toml has ../../tantivy path dependencies) ---
    # The [patch] section in Cargo.toml redirects the git dependency to the local
    # path (../../tantivy), so cargo uses whatever is on disk — not the rev in the
    # git dependency. We extract the pinned rev from Cargo.toml and advance one
    # commit to pick up any immediate fixes (e.g., pub mod visibility) without
    # pulling in breaking changes from HEAD.
    if grep -q 'path = "../../tantivy' "$cargo_toml" 2>/dev/null; then
        local tantivy_rev
        tantivy_rev=$(
            grep 'github.com/indextables/tantivy' "$cargo_toml" \
            | grep -o 'rev = "[^"]*"' \
            | head -1 \
            | sed 's/rev = "\(.*\)"/\1/'
        )
        if [[ -n "$tantivy_rev" ]]; then
            info "Cloning tantivy (pinned rev: ${tantivy_rev})..."
            git clone "$TANTIVY_REPO" "$build_dir/tantivy"
            # Advance one commit past the pinned rev if available (picks up
            # immediate fixes like pub module visibility without pulling in
            # breaking API changes from HEAD)
            local next_commit
            next_commit=$(git -C "$build_dir/tantivy" log --oneline --ancestry-path "${tantivy_rev}..origin/main" 2>/dev/null | tail -1 | awk '{print $1}')
            if [[ -n "$next_commit" ]]; then
                info "Checking out pinned rev + 1 (${next_commit})..."
                git -C "$build_dir/tantivy" checkout "$next_commit" --quiet
            else
                git -C "$build_dir/tantivy" checkout "$tantivy_rev" --quiet
            fi
        else
            info "Cloning tantivy (HEAD)..."
            git clone --depth 1 "$TANTIVY_REPO" "$build_dir/tantivy"
        fi
    fi

    # Set JAVA_HOME for the Maven build
    local build_java_home="${JAVA_HOME:-}"
    if [[ "$PLATFORM" == "macos" ]]; then
        for brew_prefix in /opt/homebrew /usr/local; do
            if [[ -d "${brew_prefix}/opt/openjdk@11" ]]; then
                build_java_home="${brew_prefix}/opt/openjdk@11"
                break
            fi
        done
    fi

    # Ensure cargo is on PATH for the Maven subprocess
    local build_path="$PATH"
    if [[ -d "$HOME/.cargo/bin" ]]; then
        build_path="$HOME/.cargo/bin:$build_path"
    fi

    # Build and install to local Maven cache
    info "Running Maven build (this may take several minutes on first build)..."
    (
        cd "$build_dir/tantivy4java"
        export JAVA_HOME="$build_java_home"
        export PATH="$build_path"
        mvn clean install -DskipTests
    )

    # Verify the JAR was installed
    if check_tantivy4java; then
        ok "tantivy4java ${TANTIVY4JAVA_VERSION} (${PLATFORM_CLASSIFIER}) installed to local Maven cache"
    else
        error "tantivy4java build succeeded but JAR not found in Maven cache."
        error "Expected: ~/.m2/repository/io/indextables/tantivy4java/${TANTIVY4JAVA_VERSION}/tantivy4java-${TANTIVY4JAVA_VERSION}-${PLATFORM_CLASSIFIER}.jar"
        rm -rf "$build_dir"
        exit 1
    fi

    # Clean up
    info "Cleaning up build directory..."
    rm -rf "$build_dir"
    trap - EXIT  # Clear the trap after successful cleanup
}

# ---------------------------------------------------------------------------
# Install dependencies
# ---------------------------------------------------------------------------
echo "=================================================================="
info "IndexTables4Spark - Development Environment Setup"
echo "=================================================================="
echo ""

case "$PLATFORM" in
    macos) install_macos ;;
    linux) install_linux ;;
esac

echo ""
echo "=================================================================="
info "Rust toolchain"
echo "=================================================================="
install_rust
ensure_cargo_on_system_path

echo ""
echo "=================================================================="
info "tantivy4java native dependency"
echo "=================================================================="
install_tantivy4java

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
info "Validating installation"
echo "=================================================================="

ERRORS=0

# Validate Java 11
# On macOS with Homebrew, set JAVA_HOME hint if not already pointing to Java 11
if [[ "$PLATFORM" == "macos" ]]; then
    BREW_JAVA_HOME="/opt/homebrew/opt/openjdk@11"
    # Fall back to Intel Homebrew path
    if [[ ! -d "$BREW_JAVA_HOME" ]]; then
        BREW_JAVA_HOME="/usr/local/opt/openjdk@11"
    fi
    if [[ -d "$BREW_JAVA_HOME" ]]; then
        export JAVA_HOME="$BREW_JAVA_HOME"
    fi
fi

if check_java; then
    # Determine which java we're reporting on
    if [[ -n "${JAVA_HOME:-}" ]] && [[ -x "${JAVA_HOME}/bin/java" ]]; then
        JAVA_VERSION_OUTPUT=$("${JAVA_HOME}/bin/java" -version 2>&1 | head -1)
    else
        JAVA_VERSION_OUTPUT=$(java -version 2>&1 | head -1)
    fi
    ok "Java $REQUIRED_JAVA_MAJOR: $JAVA_VERSION_OUTPUT"
else
    error "Java $REQUIRED_JAVA_MAJOR not found or wrong version"
    if command -v java &>/dev/null; then
        error "  Found: $(java -version 2>&1 | head -1)"
    fi
    ERRORS=$((ERRORS + 1))
fi

# Validate Maven
if check_maven; then
    MVN_VERSION=$(mvn --version 2>&1 | head -1)
    ok "Maven: $MVN_VERSION"
else
    error "Maven not found on PATH"
    ERRORS=$((ERRORS + 1))
fi

# Validate Rust
if check_rust; then
    ok "Rust: $(rustc --version)"
else
    error "Rust toolchain not found"
    ERRORS=$((ERRORS + 1))
fi

# Validate protoc
if check_protoc; then
    ok "Protoc: $(protoc --version)"
else
    error "Protobuf compiler (protoc) not found"
    ERRORS=$((ERRORS + 1))
fi

# Validate tantivy4java
if check_tantivy4java; then
    ok "tantivy4java: ${TANTIVY4JAVA_VERSION} (${PLATFORM_CLASSIFIER})"
else
    error "tantivy4java ${TANTIVY4JAVA_VERSION} (${PLATFORM_CLASSIFIER}) not found in Maven cache"
    ERRORS=$((ERRORS + 1))
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
if [[ "$ERRORS" -gt 0 ]]; then
    error "Setup completed with $ERRORS error(s). See above for details."
    exit 1
else
    info "Setup complete. All dependencies verified."
fi
echo "=================================================================="

# JAVA_HOME hint for macOS
if [[ "$PLATFORM" == "macos" ]] && [[ -n "${JAVA_HOME:-}" ]]; then
    echo ""
    info "Tip: Set JAVA_HOME in your shell profile for builds:"
    info "  export JAVA_HOME=\"$JAVA_HOME\""
fi
