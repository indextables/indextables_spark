#!/usr/bin/env bash

# run-tests.sh - Run Scala test classes individually to avoid OOM
#
# Usage:
#   ./scripts/run-tests.sh                       # Run all tests with auto-detected parallelism
#   ./scripts/run-tests.sh -j 8                  # Run with 8 parallel jobs
#   ./scripts/run-tests.sh -j 1                  # Run sequentially
#   ./scripts/run-tests.sh --dry-run             # Show test classes without running
#   ./scripts/run-tests.sh --exclude 'Cloud*'     # Exclude cloud tests
#   ./scripts/run-tests.sh --only 'Cloud*'        # Run only cloud tests
#   ./scripts/run-tests.sh -j 2 --dry-run

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@11}"

# Auto-detect CPU cores for default parallelism
if command -v nproc &>/dev/null; then
    PARALLEL_JOBS=$(nproc)
elif command -v sysctl &>/dev/null; then
    PARALLEL_JOBS=$(sysctl -n hw.ncpu)
else
    PARALLEL_JOBS=4
fi

DRY_RUN=false
EXCLUDE_PATTERN=""
ONLY_PATTERN=""

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -j)
            if [[ -z "${2:-}" ]] || ! [[ "$2" =~ ^[0-9]+$ ]] || [[ "$2" -lt 1 ]]; then
                echo "[ERROR] -j requires a positive integer argument" >&2
                exit 1
            fi
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --exclude)
            if [[ -z "${2:-}" ]]; then
                echo "[ERROR] --exclude requires a pattern argument" >&2
                exit 1
            fi
            EXCLUDE_PATTERN="$2"
            shift 2
            ;;
        --only)
            if [[ -z "${2:-}" ]]; then
                echo "[ERROR] --only requires a pattern argument" >&2
                exit 1
            fi
            ONLY_PATTERN="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [-j N] [--exclude PATTERN] [--only PATTERN] [--dry-run]"
            echo ""
            echo "Options:"
            echo "  -j N              Number of parallel test jobs (default: auto-detect CPU cores)"
            echo "  --exclude PATTERN Exclude test classes whose simple name matches the glob pattern"
            echo "  --only PATTERN    Include ONLY test classes whose simple name matches the glob pattern"
            echo "  --dry-run         List test classes without running them"
            echo "  -h                Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --exclude 'Cloud*'     # Skip cloud tests (CloudS3*, CloudAzure*)"
            echo "  $0 --only 'Cloud*'        # Run only cloud tests"
            echo "  $0 -j 4 --exclude 'Cloud*'"
            exit 0
            ;;
        *)
            echo "[ERROR] Unknown option: $1" >&2
            echo "Run '$0 --help' for usage." >&2
            exit 1
            ;;
    esac
done

# Validate mutually exclusive options
if [[ -n "$EXCLUDE_PATTERN" && -n "$ONLY_PATTERN" ]]; then
    echo "[ERROR] --exclude and --only are mutually exclusive" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Discover test classes
# ---------------------------------------------------------------------------
echo "[INFO] Finding all Scala test files..."

all_class_names=()
while IFS= read -r test_file; do
    # src/test/scala/io/indextables/spark/core/FooTest.scala
    #   -> io.indextables.spark.core.FooTest
    name="${test_file#src/test/scala/}"
    name="${name%.scala}"
    name="${name//\//.}"
    all_class_names+=("$name")
done < <(find src/test/scala -name "*Test.scala" -type f | sort)

if [[ ${#all_class_names[@]} -eq 0 ]]; then
    echo "[ERROR] No test files found in src/test/scala"
    exit 1
fi

# ---------------------------------------------------------------------------
# Apply include/exclude filters
# ---------------------------------------------------------------------------
class_names=()
skipped=0

for fqcn in "${all_class_names[@]}"; do
    # Extract simple class name (last component after the final dot)
    simple_name="${fqcn##*.}"

    if [[ -n "$ONLY_PATTERN" ]]; then
        # shellcheck disable=SC2053
        if [[ "$simple_name" == $ONLY_PATTERN ]]; then
            class_names+=("$fqcn")
        else
            ((skipped++))
        fi
    elif [[ -n "$EXCLUDE_PATTERN" ]]; then
        # shellcheck disable=SC2053
        if [[ "$simple_name" == $EXCLUDE_PATTERN ]]; then
            ((skipped++))
        else
            class_names+=("$fqcn")
        fi
    else
        class_names+=("$fqcn")
    fi
done

total_discovered=${#all_class_names[@]}
total_tests=${#class_names[@]}

if [[ $skipped -gt 0 ]]; then
    if [[ -n "$ONLY_PATTERN" ]]; then
        echo "[INFO] Found $total_discovered test classes, running $total_tests matching '$ONLY_PATTERN' (skipped $skipped)"
    else
        echo "[INFO] Found $total_discovered test classes, running $total_tests after excluding '$EXCLUDE_PATTERN' (skipped $skipped)"
    fi
else
    echo "[INFO] Found $total_tests test classes"
fi

if [[ $total_tests -eq 0 ]]; then
    echo "[ERROR] No test classes match the specified filter"
    exit 1
fi

# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------
if [[ "$DRY_RUN" == true ]]; then
    echo "[INFO] DRY RUN MODE - listing test classes that would be executed"
    echo ""
    for i in "${!class_names[@]}"; do
        printf "  [%3d/%d] %s\n" "$((i + 1))" "$total_tests" "${class_names[$i]}"
    done
    echo ""
    echo "[INFO] Total: $total_tests test classes (parallelism: $PARALLEL_JOBS)"
    exit 0
fi

# ---------------------------------------------------------------------------
# Compile once up front
# ---------------------------------------------------------------------------
echo "[INFO] Compiling test sources..."
if ! mvn test-compile -q; then
    echo "[ERROR] mvn test-compile failed"
    exit 1
fi
echo "[INFO] Compilation complete"
echo ""

# ---------------------------------------------------------------------------
# Prepare temp directory for per-test logs
# ---------------------------------------------------------------------------
LOG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/indextables-tests.XXXXXX")
echo "[INFO] Per-test log files: $LOG_DIR"
echo "[INFO] Parallelism: $PARALLEL_JOBS"
echo "=================================================================="
echo ""

# ---------------------------------------------------------------------------
# Run tests
# ---------------------------------------------------------------------------
start_epoch=$(date +%s)

# Shared counters via temp files (portable across subshells)
PASS_FILE="$LOG_DIR/.pass_count"
FAIL_FILE="$LOG_DIR/.fail_count"
FAIL_LIST="$LOG_DIR/.fail_list"
echo 0 > "$PASS_FILE"
echo 0 > "$FAIL_FILE"
: > "$FAIL_LIST"

# Lock file for atomic counter updates
LOCK_FILE="$LOG_DIR/.lock"

# Atomically increment a counter file
increment_counter() {
    local file="$1"
    while true; do
        if (set -o noclobber; echo $$ > "$LOCK_FILE") 2>/dev/null; then
            local val
            val=$(<"$file")
            echo $((val + 1)) > "$file"
            rm -f "$LOCK_FILE"
            return
        fi
        # Tiny sleep to avoid busy-wait
        sleep 0.01
    done
}

# Append to fail list atomically
append_fail() {
    local class_name="$1"
    local log_path="$2"
    while true; do
        if (set -o noclobber; echo $$ > "$LOCK_FILE") 2>/dev/null; then
            echo "$class_name|$log_path" >> "$FAIL_LIST"
            rm -f "$LOCK_FILE"
            return
        fi
        sleep 0.01
    done
}

run_single_test() {
    local index="$1"
    local class_name="$2"
    local log_file="$LOG_DIR/${class_name}.log"
    local test_start
    test_start=$(date +%s)

    if mvn scalatest:test -DwildcardSuites="$class_name" > "$log_file" 2>&1; then
        local test_end
        test_end=$(date +%s)
        local duration=$((test_end - test_start))
        printf "[PASS] [%3d/%d] %s (%ds)\n" "$index" "$total_tests" "$class_name" "$duration"
        increment_counter "$PASS_FILE"
    else
        local test_end
        test_end=$(date +%s)
        local duration=$((test_end - test_start))
        printf "[FAIL] [%3d/%d] %s (%ds) -> %s\n" "$index" "$total_tests" "$class_name" "$duration" "$log_file"
        increment_counter "$FAIL_FILE"
        append_fail "$class_name" "$log_file"
    fi
}

export -f run_single_test increment_counter append_fail
export LOG_DIR PASS_FILE FAIL_FILE FAIL_LIST LOCK_FILE total_tests

if [[ "$PARALLEL_JOBS" -eq 1 ]]; then
    # Sequential mode
    for i in "${!class_names[@]}"; do
        run_single_test "$((i + 1))" "${class_names[$i]}"
    done
else
    # Parallel mode using xargs with tab-delimited args
    for i in "${!class_names[@]}"; do
        printf '%d\t%s\n' "$((i + 1))" "${class_names[$i]}"
    done | xargs -P "$PARALLEL_JOBS" -L 1 bash -c 'run_single_test "$1" "$2"' _
fi

end_epoch=$(date +%s)
elapsed=$((end_epoch - start_epoch))
elapsed_min=$((elapsed / 60))
elapsed_sec=$((elapsed % 60))

passed=$(<"$PASS_FILE")
failed=$(<"$FAIL_FILE")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
echo "[INFO] Test Summary"
echo "=================================================================="
echo "[INFO] Total:   $total_tests"
echo "[INFO] Passed:  $passed"
echo "[INFO] Failed:  $failed"
echo "[INFO] Elapsed: ${elapsed_min}m ${elapsed_sec}s"
echo "[INFO] Logs:    $LOG_DIR"

if [[ "$failed" -gt 0 ]]; then
    echo ""
    echo "[FAIL] Failed tests:"
    while IFS='|' read -r name path; do
        echo "  - $name"
        echo "    Log: $path"
    done < "$FAIL_LIST"
    echo ""
    echo "[INFO] To re-run a failed test:"
    echo "  mvn scalatest:test -DwildcardSuites='<class_name>'"
    exit 1
else
    echo ""
    echo "[PASS] All tests passed."
    exit 0
fi
