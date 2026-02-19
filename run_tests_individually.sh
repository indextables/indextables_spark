#!/usr/bin/env bash

# run_tests_individually.sh - Run Scala test classes individually to avoid OOM
#
# Usage:
#   ./run_tests_individually.sh              # Run all tests with 4 parallel jobs
#   ./run_tests_individually.sh -j 8         # Run with 8 parallel jobs
#   ./run_tests_individually.sh -j 1         # Run sequentially
#   ./run_tests_individually.sh --dry-run    # Show test classes without running
#   ./run_tests_individually.sh -j 2 --dry-run

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@11}"

PARALLEL_JOBS=4
DRY_RUN=false

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
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [-j N] [--dry-run]"
            echo ""
            echo "Options:"
            echo "  -j N       Number of parallel test jobs (default: 4, use 1 for sequential)"
            echo "  --dry-run  List test classes without running them"
            echo "  -h         Show this help message"
            exit 0
            ;;
        *)
            echo "[ERROR] Unknown option: $1" >&2
            echo "Run '$0 --help' for usage." >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Discover test classes
# ---------------------------------------------------------------------------
echo "[INFO] Finding all Scala test files..."

class_names=()
while IFS= read -r test_file; do
    # src/test/scala/io/indextables/spark/core/FooTest.scala
    #   -> io.indextables.spark.core.FooTest
    name="${test_file#src/test/scala/}"
    name="${name%.scala}"
    name="${name//\//.}"
    class_names+=("$name")
done < <(find src/test/scala -name "*Test.scala" -type f | sort)

if [[ ${#class_names[@]} -eq 0 ]]; then
    echo "[ERROR] No test files found in src/test/scala"
    exit 1
fi

total_tests=${#class_names[@]}
echo "[INFO] Found $total_tests test classes"

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
