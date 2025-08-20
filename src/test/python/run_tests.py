#!/usr/bin/env python3
"""
Test runner for Tantivy4Spark PySpark integration tests.

This script sets up the proper environment and runs the complete test suite
with appropriate configuration for different scenarios.
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path


def find_project_root():
    """Find the project root directory."""
    current = Path(__file__).parent
    while current.parent != current:
        if (current / "pom.xml").exists():
            return current
        current = current.parent
    raise FileNotFoundError("Could not find project root (no pom.xml found)")


def check_java_installation():
    """Check if Java is properly installed."""
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✓ Java is installed: {result.stderr.split()[2]}")
            return True
    except FileNotFoundError:
        pass
    
    print("✗ Java is not installed or not in PATH")
    print("  Please install Java 11+ and ensure it's in your PATH")
    return False


def check_maven_build():
    """Check if the project has been built with Maven."""
    project_root = find_project_root()
    target_dir = project_root / "target"
    
    if not target_dir.exists():
        print("✗ Project not built. Please run: mvn clean package")
        return False
    
    # Look for JAR files
    jar_files = list(target_dir.glob("*.jar"))
    if not jar_files:
        print("✗ No JAR files found. Please run: mvn clean package")
        return False
    
    # Check for shaded JAR
    shaded_jars = [jar for jar in jar_files if "shaded" in jar.name]
    if shaded_jars:
        print(f"✓ Shaded JAR found: {shaded_jars[0].name}")
    else:
        regular_jars = [jar for jar in jar_files if "shaded" not in jar.name and "sources" not in jar.name]
        if regular_jars:
            print(f"⚠ Regular JAR found: {regular_jars[0].name} (shaded JAR recommended)")
        else:
            print("✗ No usable JAR files found")
            return False
    
    return True


def install_python_dependencies():
    """Install required Python dependencies."""
    requirements_file = Path(__file__).parent / "requirements.txt"
    
    if not requirements_file.exists():
        print("✗ requirements.txt not found")
        return False
    
    print("Installing Python dependencies...")
    try:
        subprocess.run([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
        ], check=True)
        print("✓ Python dependencies installed")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install dependencies: {e}")
        return False


def run_tests(test_args=None):
    """Run the test suite."""
    test_dir = Path(__file__).parent
    
    # Base pytest command
    cmd = [sys.executable, "-m", "pytest"]
    
    # Add test directory
    cmd.append(str(test_dir / "test_tantivy4spark_integration.py"))
    
    # Add configuration
    cmd.extend([
        "-c", str(test_dir / "pytest.ini"),
        "--tb=short",
        "-v"
    ])
    
    # Add any additional arguments
    if test_args:
        cmd.extend(test_args)
    
    print(f"Running command: {' '.join(cmd)}")
    print("-" * 80)
    
    try:
        subprocess.run(cmd, check=True)
        print("-" * 80)
        print("✓ All tests completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print("-" * 80)
        print(f"✗ Tests failed with exit code: {e.returncode}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run Tantivy4Spark PySpark integration tests")
    parser.add_argument("--skip-deps", action="store_true", 
                      help="Skip Python dependency installation")
    parser.add_argument("--skip-checks", action="store_true",
                      help="Skip environment checks")
    parser.add_argument("--test-filter", type=str,
                      help="Run only tests matching the given pattern")
    parser.add_argument("--parallel", action="store_true",
                      help="Run tests in parallel")
    parser.add_argument("--coverage", action="store_true",
                      help="Generate coverage report")
    parser.add_argument("--html-report", action="store_true",
                      help="Generate HTML test report")
    
    args = parser.parse_args()
    
    print("Tantivy4Spark PySpark Integration Test Runner")
    print("=" * 50)
    
    # Environment checks
    if not args.skip_checks:
        print("\n1. Checking environment...")
        
        if not check_java_installation():
            return 1
        
        if not check_maven_build():
            return 1
    
    # Install dependencies
    if not args.skip_deps:
        print("\n2. Installing Python dependencies...")
        if not install_python_dependencies():
            return 1
    
    # Prepare test arguments
    test_args = []
    
    if args.test_filter:
        test_args.extend(["-k", args.test_filter])
    
    if args.parallel:
        test_args.extend(["-n", "auto"])
    
    if args.coverage:
        test_args.extend(["--cov=.", "--cov-report=html", "--cov-report=term"])
    
    if args.html_report:
        test_args.extend(["--html=test_report.html", "--self-contained-html"])
    
    # Run tests
    print("\n3. Running tests...")
    success = run_tests(test_args)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())