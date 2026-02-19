#!/bin/bash

export SPARK_LOCAL_IP="127.0.0.1"


# Script to run all Scala tests in parallel using Maven scalatest:test
# Usage: ./run_tests_parallel.sh [-j JOBS] [--dry-run]
#   -j, --jobs JOBS: Number of parallel test executions (default: 4)
#   --dry-run: Show what tests would be run without actually running them

set -e

# Set JAVA_HOME for Spark to work properly
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

# Default values
PARALLEL_JOBS=4
DRY_RUN=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -j|--jobs)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [-j JOBS] [--dry-run]"
            echo "  -j, --jobs JOBS  Number of parallel test executions (default: 4)"
            echo "  --dry-run        Show what tests would be run without actually running them"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Validate PARALLEL_JOBS is a number
if ! [[ "$PARALLEL_JOBS" =~ ^[0-9]+$ ]] || [ "$PARALLEL_JOBS" -lt 1 ]; then
    echo "Error: JOBS must be a positive integer"
    exit 1
fi

echo "🔍 Finding all Scala test files..."

# Find all Scala test files and extract class names
test_files=$(find src/test/scala -name "*Test.scala" -type f)

if [ -z "$test_files" ]; then
    echo "❌ No test files found in src/test/scala"
    exit 1
fi

total_tests=$(echo "$test_files" | wc -l | tr -d ' ')
echo "📋 Found $total_tests test files"
echo "🔄 Running with $PARALLEL_JOBS parallel jobs"
echo ""

if [ "$DRY_RUN" = true ]; then
    echo "🔍 DRY RUN MODE - will not actually run tests"
    echo ""
    for test_file in $test_files; do
        class_name=$(echo "$test_file" | sed 's|src/test/scala/||' | sed 's|\.scala$||' | tr '/' '.')
        echo "Would run: $class_name"
    done
    echo ""
    echo "Total: $total_tests tests would be run with $PARALLEL_JOBS parallel jobs"
    exit 0
fi

# Create temp directory for results
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "🔨 Compiling tests first..."
mvn test-compile -q

echo ""
echo "🚀 Running tests in parallel ($PARALLEL_JOBS jobs)..."
echo "=================================================="

# Function to run a single test
run_test() {
    local test_file="$1"
    local index="$2"
    local result_file="$TEMP_DIR/result_$index"
    local output_file="$TEMP_DIR/output_$index"

    # Extract the fully qualified class name
    local class_name=$(echo "$test_file" | sed 's|src/test/scala/||' | sed 's|\.scala$||' | tr '/' '.')

    local start_time=$(date +%s)

    if mvn scalatest:test -DwildcardSuites="$class_name" -q > "$output_file" 2>&1; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo "PASSED|$class_name|$duration" > "$result_file"
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo "FAILED|$class_name|$duration" > "$result_file"
        # Append output for failed tests
        echo "---OUTPUT---" >> "$result_file"
        cat "$output_file" >> "$result_file"

        # Immediately echo failure to console with output (last 30 lines)
        # Use a marker and newlines to separate from other parallel output
        {
            echo ""
            echo "=================================================="
            echo "❌ FAILED: $class_name (${duration}s)"
            echo "=================================================="
            echo "Last 30 lines of output:"
            tail -30 "$output_file"
            echo "=================================================="
            echo ""
        } >&2
    fi
}

export -f run_test
export TEMP_DIR

# Run tests in parallel using xargs
# Each test gets its own background process, limited by PARALLEL_JOBS
start_time=$(date +%s)

# Create a file with indexed test entries (use nl to number lines)
echo "$test_files" | nl -ba -w1 -s'|' > "$TEMP_DIR/test_list"

# Progress tracking in background
(
    while true; do
        sleep 5
        completed=$(ls "$TEMP_DIR"/result_* 2>/dev/null | wc -l | tr -d ' ')
        if [ "$completed" -gt 0 ]; then
            passed=$(grep -l "^PASSED" "$TEMP_DIR"/result_* 2>/dev/null | wc -l | tr -d ' ')
            failed=$(grep -l "^FAILED" "$TEMP_DIR"/result_* 2>/dev/null | wc -l | tr -d ' ')
            remaining=$((total_tests - completed))
            echo "  📊 Progress: $passed passed, $failed failed, $remaining remaining..."
        fi
        if [ "$completed" -ge "$total_tests" ]; then
            break
        fi
    done
) &
PROGRESS_PID=$!

# Run tests in parallel
cat "$TEMP_DIR/test_list" | while IFS='|' read -r index test_file; do
    # Wait if we have too many jobs running
    while [ $(jobs -r | wc -l) -ge "$PARALLEL_JOBS" ]; do
        sleep 0.5
    done
    run_test "$test_file" "$index" &
done

# Wait for all background jobs to complete
wait

# Kill progress tracker
kill $PROGRESS_PID 2>/dev/null || true

end_time=$(date +%s)
total_duration=$((end_time - start_time))

echo ""
echo "=================================================="
echo "🏁 Test Summary"
echo "=================================================="

# Collect results
passed_tests=0
failed_tests=0

# Create files to store failed test info and timing data
> "$TEMP_DIR/failed_names"
> "$TEMP_DIR/passed_names"
> "$TEMP_DIR/all_timings"

for result_file in "$TEMP_DIR"/result_*; do
    if [ -f "$result_file" ]; then
        first_line=$(head -1 "$result_file")
        status=$(echo "$first_line" | cut -d'|' -f1)
        class_name=$(echo "$first_line" | cut -d'|' -f2)
        duration=$(echo "$first_line" | cut -d'|' -f3)

        # Store timing data for all tests (duration|class_name|status)
        echo "$duration|$class_name|$status" >> "$TEMP_DIR/all_timings"

        if [ "$status" = "PASSED" ]; then
            passed_tests=$((passed_tests + 1))
            echo "  ✅ $class_name (${duration}s)"
            echo "$class_name" >> "$TEMP_DIR/passed_names"
        else
            failed_tests=$((failed_tests + 1))
            echo "  ❌ $class_name (${duration}s)"
            echo "$class_name" >> "$TEMP_DIR/failed_names"
            # Copy output to a named file for later display
            sed -n '/^---OUTPUT---$/,$ p' "$result_file" | tail -n +2 > "$TEMP_DIR/failed_output_$failed_tests"
        fi
    fi
done

echo ""
echo "=================================================="
echo "📊 Total tests: $total_tests"
echo "✅ Passed: $passed_tests"
echo "❌ Failed: $failed_tests"
echo "⏱️  Total time: ${total_duration}s (with $PARALLEL_JOBS parallel jobs)"

# Display top 5 longest running tests
echo ""
echo "=================================================="
echo "🐢 Top 5 Longest Running Tests:"
echo "=================================================="
sort -t'|' -k1 -nr "$TEMP_DIR/all_timings" | head -5 | while IFS='|' read -r duration class_name status; do
    # Extract just the class name (last part after the last dot)
    short_name=$(echo "$class_name" | rev | cut -d'.' -f1 | rev)
    if [ "$status" = "PASSED" ]; then
        status_icon="✅"
    else
        status_icon="❌"
    fi
    # Format duration as minutes:seconds if >= 60s
    if [ "$duration" -ge 60 ]; then
        mins=$((duration / 60))
        secs=$((duration % 60))
        printf "  %s %3dm %02ds  %s\n" "$status_icon" "$mins" "$secs" "$short_name"
    else
        printf "  %s %6ds  %s\n" "$status_icon" "$duration" "$short_name"
    fi
done

if [ $failed_tests -gt 0 ]; then
    echo ""
    echo "❌ Failed tests:"
    while read -r failed_test; do
        echo "   - $failed_test"
    done < "$TEMP_DIR/failed_names"

    echo ""
    echo "📋 Failed test output (last 50 lines each):"
    echo "=================================================="
    idx=1
    while read -r failed_test; do
        echo ""
        echo "--- $failed_test ---"
        if [ -f "$TEMP_DIR/failed_output_$idx" ]; then
            tail -50 "$TEMP_DIR/failed_output_$idx"
        fi
        idx=$((idx + 1))
    done < "$TEMP_DIR/failed_names"

    echo ""
    echo "💡 To re-run a specific failed test:"
    echo "   mvn scalatest:test -DwildcardSuites='<test_class_name>'"
    exit 1
else
    echo ""
    echo "🎉 All tests passed!"
    exit 0
fi
