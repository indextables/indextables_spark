#!/bin/bash

# Script to run all Scala tests individually using Maven scalatest:test
# Usage: ./run_tests_individually.sh [--dry-run]
#   --dry-run: Show what tests would be run without actually running them

set -e

# Set JAVA_HOME for Spark to work properly
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

# Check for dry-run flag
DRY_RUN=false
if [ "$1" = "--dry-run" ]; then
    DRY_RUN=true
    echo "🔍 DRY RUN MODE - will not actually run tests"
    echo ""
fi

echo "🔍 Finding all Scala test files..."

# Find all Scala test files and extract class names
test_files=$(find src/test/scala -name "*Test.scala" -type f)

if [ -z "$test_files" ]; then
    echo "❌ No test files found in src/test/scala"
    exit 1
fi

echo "📋 Found $(echo "$test_files" | wc -l) test files"
echo ""

# Counter for tracking progress
total_tests=$(echo "$test_files" | wc -l)
current_test=0
passed_tests=0
failed_tests=0

# Store failed test names
failed_test_names=()

echo "🚀 Running tests individually..."
echo "=================================================="

mvn test-compile
for test_file in $test_files; do
    current_test=$((current_test + 1))

    # Extract the fully qualified class name from the file path
    # Convert src/test/scala/com/tantivy4spark/sql/PartitionedDatasetTest.scala
    # to com.tantivy4spark.sql.PartitionedDatasetTest
    class_name=$(echo "$test_file" | sed 's|src/test/scala/||' | sed 's|\.scala$||' | tr '/' '.')

    echo ""
    echo "[$current_test/$total_tests] Running: $class_name"
    echo "  📁 File: $test_file"

    if [ "$DRY_RUN" = true ]; then
        echo "  🔍 Would run: mvn test-compile scalatest:test -DwildcardSuites='$class_name'"
        passed_tests=$((passed_tests + 1))
    else
        echo "  ⏱️  Starting at $(date '+%H:%M:%S')"

        # Run the specific test
        start_time=$(date +%s)

        if mvn scalatest:test -DwildcardSuites="$class_name" -q; then
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "  ✅ PASSED in ${duration}s"
            passed_tests=$((passed_tests + 1))
        else
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "  ❌ FAILED in ${duration}s"
            failed_tests=$((failed_tests + 1))
            failed_test_names+=("$class_name")
        fi

        echo "  📊 Progress: $passed_tests passed, $failed_tests failed, $((total_tests - current_test)) remaining"
    fi
done

echo ""
echo "=================================================="
echo "🏁 Test Summary"
echo "=================================================="
echo "📊 Total tests: $total_tests"
echo "✅ Passed: $passed_tests"
echo "❌ Failed: $failed_tests"

if [ $failed_tests -gt 0 ]; then
    echo ""
    echo "❌ Failed tests:"
    for failed_test in "${failed_test_names[@]}"; do
        echo "   - $failed_test"
    done
    echo ""
    echo "💡 To re-run a specific failed test:"
    echo "   mvn scalatest:test -DwildcardSuites='<test_class_name>'"
    exit 1
else
    echo ""
    echo "🎉 All tests passed!"
    exit 0
fi
