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
