# Section 12: Error Handling & Resilience (Outline)

## 12.1 Overview

IndexTables4Spark implements comprehensive error handling and resilience mechanisms to ensure production reliability, graceful degradation, and clear error reporting across all system components.

## 12.2 Transaction Log Resilience

### 12.2.1 Atomic Operations
- **ACID guarantees**: All transaction log commits are atomic
- **Isolation**: Concurrent reads/writes see consistent state
- **Durability**: Commits persisted to storage before acknowledgment
- **Consistency**: Schema validation prevents invalid states

### 12.2.2 Optimistic Concurrency Control
- **Version-based conflict detection**: Detects concurrent writes
- **Automatic retry**: Failed commits retry with exponential backoff
- **Maximum retry attempts**: Default 3 retries
- **Conflict resolution**: Last-write-wins with explicit version checking

### 12.2.3 Checkpoint Recovery
- **Automatic checkpoint creation**: Every N transactions
- **Checkpoint validation**: CRC checksums verify integrity
- **Fallback to full read**: If checkpoint corrupted, read all transactions
- **Recovery guarantees**: Always able to reconstruct current state

### 12.2.4 Transaction Log Cleanup Safety
- **Ultra-conservative deletion**: Multiple safety gates
- **Criteria**: age > retention AND version < checkpoint AND version < current
- **Graceful failure handling**: Cleanup failures don't break operations
- **Data consistency**: All data remains accessible regardless of cleanup timing

## 12.3 Storage Layer Resilience

### 12.3.1 S3 Retry Logic
- **Exponential backoff**: 1s, 2s, 4s delays
- **Maximum retries**: 3 attempts by default
- **Transient error detection**: Retry only recoverable errors (network, 503, etc.)
- **Permanent error fast-fail**: Immediate failure for 404, 403, etc.

### 12.3.2 Split Download Error Handling
- **Partial download cleanup**: Delete incomplete files
- **Cache eviction on failure**: Remove invalid cache entries
- **Retry with fresh download**: Avoid corrupted cached files
- **User notification**: Clear error messages with file paths

### 12.3.3 Upload Failure Recovery
- **Multipart abort**: Clean up failed multipart uploads
- **Orphan prevention**: Explicit abort calls in exception handlers
- **Resource cleanup**: Close streams and delete temp files
- **Transaction rollback**: Don't commit if upload fails

## 12.4 Merge Operations Resilience

### 12.4.1 Skipped Files Tracking
- **Cooldown mechanism**: Prevent repeated failures on same files (default: 24 hours)
- **Metadata tracking**: Record skip reason, timestamp, retry timestamp
- **Original files preserved**: Never mark skipped files as "removed"
- **Automatic retry**: Files become eligible after cooldown expires

### 12.4.2 Null/Empty IndexUID Handling
- **Detection**: Handles null, empty string, and whitespace-only values
- **Graceful skip**: Treat as "no merge performed" scenario
- **File preservation**: Original files remain in transaction log
- **Warning logs**: Clear notification of skipped merges

### 12.4.3 Partial Merge Completion
- **Transactional**: Each merge group commits separately
- **Progress preservation**: Successful merges committed even if later groups fail
- **Error reporting**: Detailed metrics on successful vs failed merge groups
- **Retry capability**: Failed groups can be retried independently

## 12.5 Query Execution Resilience

### 12.5.1 Split Access Errors
- **Missing file handling**: Clear error messages with file path
- **Cache invalidation**: Remove stale cache entries
- **Retry with download**: Attempt fresh download on cache miss
- **Task failure**: Fail task but allow Spark retry mechanism

### 12.5.2 Tantivy Query Errors
- **Syntax validation**: Early detection of invalid query syntax
- **Fallback to Spark**: Fall back to Spark execution for unsupported queries
- **Clear error messages**: User-friendly Tantivy error translation
- **Partial results**: Return available results even if some splits fail

### 12.5.3 Schema Mismatch Handling
- **Field validation**: Ensure query fields exist in schema
- **Type compatibility**: Validate filter types match field types
- **Graceful degradation**: Skip unsupported operations with warnings
- **Error reporting**: Clear messages about schema incompatibilities

## 12.6 Write Operations Resilience

### 12.6.1 Index Creation Failures
- **Temporary file cleanup**: Delete partial indexes on failure
- **Resource release**: Close index writers and free native memory
- **Task retry**: Spark retries failed tasks automatically
- **Error propagation**: Clear error messages to user

### 12.6.2 Upload Failures
- **Partial upload cleanup**: Abort multipart uploads
- **Local file cleanup**: Delete temporary split files
- **Transaction prevention**: Don't commit if upload fails
- **Retry capability**: Task retry creates fresh index and uploads

### 12.6.3 Commit Failures
- **Orphaned file detection**: Track files not in transaction log
- **Manual cleanup**: VACUUM-style command (future)
- **Error recovery**: Retry commit with exponential backoff
- **User notification**: Clear error about commit failure

## 12.7 Cache Resilience

### 12.7.1 Cache Corruption Detection
- **File existence check**: Validate cached files still exist
- **Size validation**: Verify file size matches expected
- **Automatic eviction**: Remove corrupted entries
- **Redownload**: Fetch fresh copy from storage

### 12.7.2 Cache Eviction Under Memory Pressure
- **LRU eviction**: Remove least recently used entries
- **Graceful degradation**: Query performance degrades but doesn't fail
- **Automatic recovery**: Cache repopulates on subsequent queries
- **Memory bounds**: Never exceed configured maximum

### 12.7.3 Locality Tracking Failures
- **Degraded scheduling**: Fall back to any executor if locality unknown
- **Background refresh**: Periodic locality updates
- **Broadcast failures**: Gracefully handle failed broadcasts
- **Recovery**: Rebuild locality information incrementally

## 12.8 Configuration Error Handling

### 12.8.1 Validation Errors
- **Type checking**: Immediate failure on type mismatches
- **Range validation**: Enforce minimum/maximum constraints
- **Format validation**: Validate size formats, paths, etc.
- **Clear error messages**: User-friendly validation errors

### 12.8.2 Invalid Paths
- **Directory validation**: Check existence and writability
- **Automatic fallback**: Use system temp if custom path invalid
- **Warning logs**: Notify user of fallback behavior
- **Continued execution**: Don't fail entire job for invalid temp path

### 12.8.3 AWS Credential Errors
- **Early detection**: Validate credentials before starting work
- **Clear messages**: Specific error for missing/invalid credentials
- **Provider fallback**: Try default provider chain if custom provider fails
- **Security**: Never log actual credential values

## 12.9 Partitioning Errors

### 12.9.1 Partition Value Validation
- **Type checking**: Validate partition values match column types
- **Invalid value handling**: Clear error messages for invalid values
- **Null partition values**: Explicit handling with `__HIVE_DEFAULT_PARTITION__`
- **Special character escaping**: Proper URL encoding/decoding

### 12.9.2 Partition Evolution Limitations
- **Schema compatibility**: Prevent incompatible partition column changes
- **Clear error messages**: Explain why partition evolution not supported
- **Workaround guidance**: Suggest recreating table with new partition scheme
- **Data preservation**: Existing data remains readable

### 12.9.3 Partition Pruning Failures
- **Fallback to full scan**: If partition pruning fails, scan all files
- **Warning logs**: Notify about partition pruning failure
- **Performance degradation**: Slower but correct results
- **Root cause logging**: Detailed error information for debugging

## 12.10 Catalyst Integration Errors

### 12.10.1 Expression Conversion Failures
- **Unsupported expression types**: Clear messages about what's not supported
- **Fallback to Spark**: Evaluate in Spark if pushdown not possible
- **Warning logs**: Notify about fallback behavior
- **Performance impact**: Warn if performance will degrade

### 12.10.2 Rule Application Failures
- **Exception isolation**: Catalyst rule failures don't break entire query
- **Logging**: Detailed error logging for debugging
- **Graceful degradation**: Query continues without optimization
- **User notification**: Warning about missing optimization

### 12.10.3 Relation-Scoped Storage Failures
- **WeakHashMap cleanup**: Automatic cleanup prevents memory leaks
- **Missing entries**: Gracefully handle missing IndexQuery entries
- **Concurrent access**: Thread-safe access to relation map
- **Recovery**: Query continues with default behavior

## 12.11 Native Library Errors

### 12.11.1 JNI Error Handling
- **Exception translation**: Convert native exceptions to Java exceptions
- **Resource cleanup**: Ensure native resources released on error
- **Clear error messages**: Translate Rust/Tantivy errors to user-friendly messages
- **Crash prevention**: Validate parameters before native calls

### 12.11.2 tantivy4java Integration
- **Version compatibility**: Check tantivy4java version at startup
- **Feature availability**: Gracefully handle missing features in older versions
- **Native library loading**: Clear error if native library not found
- **Platform compatibility**: Detect unsupported platforms early

### 12.11.3 Memory Management Errors
- **Native memory limits**: Respect configured heap sizes
- **OOM prevention**: Bounded buffers and batch sizes
- **Cleanup on error**: Release native memory even on exceptions
- **GC cooperation**: Explicit cleanup reduces GC pressure

## 12.12 Monitoring and Observability

### 12.12.1 Error Logging
- **Structured logging**: Consistent log format across components
- **Error categorization**: WARN vs ERROR vs INFO
- **Context inclusion**: File paths, sizes, configurations in error messages
- **Stack traces**: Full stack traces for unexpected errors

### 12.12.2 Metrics and Counters
- **Error counters**: Track error rates by category
- **Retry counters**: Monitor retry frequency
- **Cache hit/miss**: Track cache effectiveness
- **Performance metrics**: Query latency, throughput, etc.

### 12.12.3 Health Checks
- **Transaction log health**: Verify log is readable and current
- **Cache health**: Monitor eviction rates and hit rates
- **Storage health**: S3 connectivity and latency
- **Native library health**: Verify tantivy4java functioning

## 12.13 Failure Modes and Recovery

### 12.13.1 Executor Failures
- **Task retry**: Spark automatically retries failed tasks
- **Cache rebuild**: Lost cache entries rebuilt on retry
- **Transaction re-read**: Transaction log re-read from storage
- **Idempotent operations**: All operations safe to retry

### 12.13.2 Driver Failures
- **Broadcast re-creation**: Locality information rebuilt
- **Transaction log re-read**: Full transaction log re-read
- **Query re-planning**: Physical plan recreated
- **No data loss**: All state reconstructable from transaction log

### 12.13.3 Storage Failures
- **S3 unavailability**: Retry with exponential backoff
- **Network partitions**: Timeout and retry mechanisms
- **Permanent failures**: Clear error messages to user
- **Data durability**: Transaction log ensures no data loss

## 12.14 Testing and Validation

### 12.14.1 Error Injection Testing
- **Simulated S3 failures**: Test retry and recovery logic
- **Corrupted file simulation**: Test cache invalidation
- **Concurrent write conflicts**: Test optimistic concurrency
- **Resource exhaustion**: Test bounded cache behavior

### 12.14.2 Chaos Engineering
- **Random task failures**: Verify recovery mechanisms
- **Network failures**: Test retry and backoff logic
- **Storage delays**: Test timeout handling
- **Memory pressure**: Test eviction and OOM prevention

### 12.14.3 Integration Testing
- **Real S3 failures**: Test with actual S3 service issues
- **Large-scale tests**: Verify behavior at production scale
- **Multi-tenant scenarios**: Test isolation and resource limits
- **Long-running tests**: Verify stability over time

## 12.15 Best Practices

### 12.15.1 Production Recommendations
- **Enable transaction log caching**: Improves reliability and performance
- **Configure appropriate retries**: Balance latency vs reliability
- **Monitor error rates**: Alert on elevated error rates
- **Regular checkpoints**: Ensure fast recovery
- **Conservative retention**: Keep longer history for audit and recovery

### 12.15.2 Error Handling Patterns
- **Fail fast for user errors**: Invalid configuration, missing credentials
- **Retry for transient errors**: Network issues, S3 throttling
- **Graceful degradation**: Fall back to slower but correct behavior
- **Clear error messages**: Help users diagnose and fix issues
- **Preserve work**: Commit successful partial results when possible

### 12.15.3 Monitoring and Alerting
- **Error rate thresholds**: Alert on sustained high error rates
- **Retry rate monitoring**: High retry rates indicate problems
- **Cache effectiveness**: Low hit rates impact performance
- **Storage latency**: S3 performance degradation
- **Transaction log health**: Detect checkpoint or cleanup issues

## 12.16 Summary

IndexTables4Spark ensures production reliability through:

✅ **Transaction log resilience**: ACID guarantees, checkpoint recovery, safe cleanup
✅ **Storage layer resilience**: S3 retry logic, partial download cleanup, upload failure recovery
✅ **Merge resilience**: Skipped files tracking, null indexUID handling, partial completion
✅ **Query resilience**: Missing file handling, schema mismatch detection, fallback to Spark
✅ **Cache resilience**: Corruption detection, LRU eviction, locality recovery
✅ **Configuration validation**: Type checking, range validation, clear error messages
✅ **Native library safety**: Exception translation, resource cleanup, crash prevention
✅ **Comprehensive monitoring**: Error logging, metrics, health checks
✅ **Graceful degradation**: Performance degrades but correctness preserved
✅ **Clear error messages**: User-friendly diagnostics for all failure modes

**Key Resilience Features**:
- **Automatic retry mechanisms** with exponential backoff
- **Graceful degradation** when optimization not possible
- **Data consistency guarantees** through transaction log
- **Resource cleanup** even on exceptions
- **Clear error messages** for user action
- **Production-tested** error handling across all components
