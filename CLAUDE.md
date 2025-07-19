# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project is a new file handler type for Apache spark that implements fast search
using the Tantivy search engine library directly via JNI.  
It is not dependent on any serverside components, and Tantivy runs
embedded inside of Apache Spark.

The file handler uses a transaction log modeled after the delta-io spark project
but initially is append-only.

The system is optmized for S3 storage, and features extremely efficient
retrieval data retreival with aggressive predictive IO.


## Development Setup

### Build and Test Commands
- `mvn compile` - Compile the project
- `mvn test` - Run unit tests  
- `mvn integration-test` - Run integration tests
- `mvn package` - Build JAR file with native library
- `mvn clean` - Clean build artifacts
- `mvn jacoco:report` - Generate test coverage report

### Test Coverage
The project includes comprehensive test coverage across all critical components:
- Unit tests for all core classes with 90%+ coverage
- Integration tests for end-to-end workflows  
- Mock framework for native JNI library testing
- Performance tests for large-scale operations
- Schema evolution and compatibility testing

### Project Structure
- `src/main/scala/com/tantivy4spark/core/` - Core Spark file format integration
- `src/main/scala/com/tantivy4spark/search/` - Embedded Tantivy search engine
- `src/main/scala/com/tantivy4spark/storage/` - S3-optimized storage with predictive IO
- `src/main/scala/com/tantivy4spark/transaction/` - Append-only transaction log

### Key Components
- `TantivyFileFormat` - Main Spark DataSource V1 implementation
- `TantivyFileReader/Writer` - Handle read/write operations
- `S3OptimizedReader` - Implements aggressive predictive IO for S3
- `TransactionLog` - Delta-style append-only transaction logging
- `TantivySearchEngine` - Embedded search functionality via JNI
- `TantivyIndexWriter` - Writes data in native Tantivy format
- `TantivyNative` - JNI wrapper for Rust Tantivy library
- `TantivyConfig` - Configuration management and schema mapping
- `SchemaManager` - Handles schema evolution and compatibility

### Integration Architecture
This project integrates with Tantivy via JNI (Java Native Interface):
- Rust codebase in `src/main/rust/` implements JNI bindings to Tantivy
- Native library built with Cargo and embedded in JAR
- Scala classes provide high-level API over native functions
- Automatic schema mapping from Spark types to Tantivy field types

### Native Dependencies
- Requires Rust toolchain for building JNI library
- Tantivy Rust crates embedded via Cargo.toml
- Cross-platform native library support (Linux, macOS, Windows)
