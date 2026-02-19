# Protocol Versioning Design: minReader/minWriter Support for IndexTables4Spark

## Executive Summary

This design document proposes adding Delta Lake-style protocol versioning to IndexTables4Spark through `minReaderVersion` and `minWriterVersion` properties. This will enable backwards compatibility management and allow the system to block incompatible readers/writers when breaking changes are introduced.

## Background

### Current State
- IndexTables4Spark has an evolving transaction log format with various actions (AddAction, RemoveAction, SkipAction, MetadataAction)
- No formal protocol versioning exists
- All clients can read/write any transaction log regardless of features used
- Risk of data corruption or crashes when old clients access new features

### Delta Lake's Approach
Delta Lake uses a `Protocol` action in the transaction log with:
- `minReaderVersion`: Minimum version required to read the table
- `minWriterVersion`: Minimum version required to write to the table
- `readerFeatures`: Optional set of reader feature names (for version 3+)
- `writerFeatures`: Optional set of writer feature names (for version 7+)

**Key Principles from Delta:**
1. **Readers and writers MUST check protocol version before any operations**
2. **Clients MUST silently ignore unknown fields and actions**
3. **Protocol changes block incompatible clients explicitly**
4. **Protocol action is written once and updated when features require it**

## Design Proposal

### 1. Protocol Action Structure

Add a new `ProtocolAction` case class to `Actions.scala`:

```scala
case class ProtocolAction(
  minReaderVersion: Int,
  minWriterVersion: Int,
  readerFeatures: Option[Set[String]] = None,  // For future extensibility
  writerFeatures: Option[Set[String]] = None   // For future extensibility
) extends Action
```

#### Version Numbering Scheme

**Version 1 (Current/Legacy)**
- **Reader capabilities**: Read AddAction, RemoveAction, MetadataAction
- **Writer capabilities**: Write AddAction, RemoveAction, MetadataAction
- **Features**: Basic transaction log with checkpoints

**Version 2 (Next Release)**
- **Reader capabilities**: Version 1 + read SkipAction, extended AddAction metadata
- **Writer capabilities**: Version 1 + write SkipAction, extended AddAction metadata
- **Features**: Skipped files tracking, footer offsets, tantivy4java SplitMetadata fields

**Version 3+ (Future)**
- **Reader capabilities**: Reserved for future reader-only features
- **Writer capabilities**: Reserved for future breaking changes
- **Features**: Table features support (similar to Delta's approach)

### 2. Protocol Action Lifecycle

#### 2.1 Table Initialization

When creating a new table, write both MetadataAction and ProtocolAction in version 0:

```scala
def initialize(schema: StructType, partitionColumns: Seq[String]): Unit = {
  if (!cloudProvider.exists(transactionLogPathStr)) {
    cloudProvider.createDirectory(transactionLogPathStr)

    val metadataAction = MetadataAction(...)
    val protocolAction = ProtocolAction(
      minReaderVersion = CURRENT_READER_VERSION,
      minWriterVersion = CURRENT_WRITER_VERSION
    )

    // Write both actions in version 0
    writeActions(0, Seq(metadataAction, protocolAction))
  }
}
```

#### 2.2 Protocol Upgrades

When a write operation requires a higher protocol version:

```scala
def upgradeProtocol(newMinReaderVersion: Int, newMinWriterVersion: Int): Unit = {
  val currentProtocol = getProtocol()

  // Only upgrade if necessary
  if (newMinReaderVersion > currentProtocol.minReaderVersion ||
      newMinWriterVersion > currentProtocol.minWriterVersion) {

    val updatedProtocol = ProtocolAction(
      minReaderVersion = math.max(newMinReaderVersion, currentProtocol.minReaderVersion),
      minWriterVersion = math.max(newMinWriterVersion, currentProtocol.minWriterVersion)
    )

    val version = getNextVersion()
    writeAction(version, updatedProtocol)

    logger.info(s"Upgraded protocol to reader=$newMinReaderVersion, writer=$newMinWriterVersion")
  }
}
```

### 3. Version Checking

#### 3.1 Reader Version Check

Add version checking in `TransactionLog.listFiles()` and any read path:

```scala
private def assertTableReadable(): Unit = {
  val protocol = getProtocol()

  if (protocol.minReaderVersion > CURRENT_READER_VERSION) {
    throw new ProtocolVersionException(
      s"Table requires minReaderVersion=${protocol.minReaderVersion}, " +
      s"but current reader version is $CURRENT_READER_VERSION. " +
      s"Please upgrade IndexTables4Spark to read this table."
    )
  }

  // Check for unknown reader features (for version 3+)
  protocol.readerFeatures.foreach { features =>
    val unsupportedFeatures = features -- SUPPORTED_READER_FEATURES
    if (unsupportedFeatures.nonEmpty) {
      throw new ProtocolVersionException(
        s"Table requires unsupported reader features: ${unsupportedFeatures.mkString(", ")}"
      )
    }
  }
}
```

#### 3.2 Writer Version Check

Add version checking in write operations:

```scala
private def assertTableWritable(): Unit = {
  val protocol = getProtocol()

  if (protocol.minWriterVersion > CURRENT_WRITER_VERSION) {
    throw new ProtocolVersionException(
      s"Table requires minWriterVersion=${protocol.minWriterVersion}, " +
      s"but current writer version is $CURRENT_WRITER_VERSION. " +
      s"Please upgrade IndexTables4Spark to write to this table."
    )
  }

  // Check for unknown writer features (for version 3+)
  protocol.writerFeatures.foreach { features =>
    val unsupportedFeatures = features -- SUPPORTED_WRITER_FEATURES
    if (unsupportedFeatures.nonEmpty) {
      throw new ProtocolVersionException(
        s"Table requires unsupported writer features: ${unsupportedFeatures.mkString(", ")}"
      )
    }
  }
}
```

### 4. Protocol Reading and Caching

#### 4.1 Protocol Snapshot

Add protocol to the transaction log snapshot:

```scala
case class TransactionSnapshot(
  metadata: MetadataAction,
  protocol: ProtocolAction,
  files: Seq[AddAction],
  version: Long
)

def getSnapshot(): TransactionSnapshot = {
  cache.flatMap(_.getCachedSnapshot()) match {
    case Some(snapshot) => snapshot
    case None =>
      val actions = readAllActions()
      val metadata = actions.collectFirst { case m: MetadataAction => m }
        .getOrElse(throw new IllegalStateException("No metadata found"))
      val protocol = actions.collectFirst { case p: ProtocolAction => p }
        .getOrElse(ProtocolAction(1, 1)) // Default to version 1 for legacy tables

      val snapshot = TransactionSnapshot(metadata, protocol, files, getLatestVersion())
      cache.foreach(_.cacheSnapshot(snapshot))
      snapshot
  }
}

def getProtocol(): ProtocolAction = getSnapshot().protocol
```

#### 4.2 Checkpoint Integration

Update checkpoint format to include protocol:

```scala
case class CheckpointData(
  protocol: ProtocolAction,
  metadata: MetadataAction,
  files: Seq[AddAction],
  version: Long,
  timestamp: Long,
  checksum: String
)
```

### 5. Migration Strategy

#### 5.1 Backwards Compatibility

**Existing tables without Protocol action:**
- When reading old tables, default to `ProtocolAction(1, 1)`
- Cache this default to avoid repeated checks
- Log a warning recommending protocol initialization

```scala
def getProtocol(): ProtocolAction = {
  val actions = readAllActions()
  actions.collectFirst { case p: ProtocolAction => p } match {
    case Some(protocol) => protocol
    case None =>
      logger.warn(s"Table at $tablePath has no protocol action, defaulting to version 1")
      ProtocolAction(1, 1)
  }
}
```

**Automatic upgrade on first write:**
```scala
def addFiles(addActions: Seq[AddAction]): Long = {
  // Check if protocol exists
  val currentProtocol = getProtocol()
  val hasProtocolAction = readAllActions().exists(_.isInstanceOf[ProtocolAction])

  if (!hasProtocolAction) {
    logger.info(s"Initializing protocol for legacy table")
    val version = getNextVersion()
    writeAction(version, ProtocolAction(CURRENT_READER_VERSION, CURRENT_WRITER_VERSION))
  }

  // Continue with regular write...
  assertTableWritable()
  // ... rest of implementation
}
```

#### 5.2 Version Migration Path

**Phase 1: Current Release (v1.13)**
- Add ProtocolAction support
- Default all tables to version 1
- No breaking changes
- Writers check protocol but never fail (log warnings only)

**Phase 2: Next Release (v1.14)**
- Enable protocol enforcement
- New tables use version 2 by default
- Legacy tables auto-upgrade to version 2 on first write
- Full reader/writer version checking active

**Phase 3: Future Releases**
- Introduce table features (version 3+)
- Feature-based protocol management
- Fine-grained capability negotiation

### 6. Constants and Configuration

Add protocol version constants:

```scala
object ProtocolVersion {
  // Current versions supported by this release
  val CURRENT_READER_VERSION = 2
  val CURRENT_WRITER_VERSION = 2

  // Minimum versions we can read/write
  val MIN_READER_VERSION = 1
  val MIN_WRITER_VERSION = 1

  // Feature support (for version 3+)
  val SUPPORTED_READER_FEATURES: Set[String] = Set(
    "skippedFiles",
    "extendedMetadata",
    "footerOffsets"
  )

  val SUPPORTED_WRITER_FEATURES: Set[String] = Set(
    "skippedFiles",
    "extendedMetadata",
    "footerOffsets",
    "checkpoint",
    "optimizeWrite"
  )

  // Configuration
  val PROTOCOL_CHECK_ENABLED = "spark.indextables.protocol.checkEnabled"
  val PROTOCOL_AUTO_UPGRADE = "spark.indextables.protocol.autoUpgrade"
}
```

### 7. Error Handling

#### 7.1 Custom Exception

```scala
class ProtocolVersionException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)
```

#### 7.2 Error Messages

Follow Delta's approach with helpful error messages:

```scala
throw new ProtocolVersionException(
  s"""Table at $tablePath requires a newer version of IndexTables4Spark.
     |
     |This table requires:
     |  minReaderVersion = ${protocol.minReaderVersion}
     |  minWriterVersion = ${protocol.minWriterVersion}
     |
     |Your current version supports:
     |  readerVersion = $CURRENT_READER_VERSION
     |  writerVersion = $CURRENT_WRITER_VERSION
     |
     |Please upgrade IndexTables4Spark to version 1.14 or later.
     |See: https://github.com/indextables4spark/docs/protocol-versions
     """.stripMargin
)
```

### 8. Testing Strategy

#### 8.1 Unit Tests

```scala
class ProtocolVersionTest extends AnyFunSuite {
  test("should reject reader when minReaderVersion too high") {
    // Create table with version 999
    // Attempt to read
    // Verify ProtocolVersionException thrown
  }

  test("should reject writer when minWriterVersion too high") {
    // Create table with version 999
    // Attempt to write
    // Verify ProtocolVersionException thrown
  }

  test("should support backwards compatibility for tables without protocol") {
    // Read legacy table
    // Verify defaults to version 1
    // Verify can read successfully
  }

  test("should auto-upgrade protocol on first write to legacy table") {
    // Create legacy table (no protocol action)
    // Perform write operation
    // Verify protocol action added
  }

  test("should upgrade protocol when using new features") {
    // Create version 1 table
    // Write with SkipAction (requires version 2)
    // Verify protocol upgraded to version 2
  }
}
```

#### 8.2 Integration Tests

```scala
class ProtocolIntegrationTest extends AnyFunSuite {
  test("end-to-end: version 1 reader cannot read version 2 table") {
    // Simulate old client reading new table
  }

  test("end-to-end: version 2 reader can read version 1 table") {
    // Forward compatibility test
  }

  test("checkpoint includes protocol action") {
    // Create table, write files, create checkpoint
    // Verify checkpoint contains protocol
  }
}
```

### 9. Documentation Updates

#### 9.1 CLAUDE.md Updates

Add new section:

```markdown
## Protocol Versioning

**New in v1.13**: Protocol versioning support for backwards compatibility management.

### Protocol Versions

IndexTables4Spark uses protocol versioning to ensure compatibility:

- **minReaderVersion**: Minimum version required to read the table
- **minWriterVersion**: Minimum version required to write to the table

### Version History

**Version 1 (Legacy)**
- Basic transaction log (AddAction, RemoveAction, MetadataAction)
- Checkpoint support
- Partitioned datasets

**Version 2 (Current)**
- Version 1 features
- SkipAction support for merge resilience
- Extended AddAction metadata (footer offsets, tantivy4java SplitMetadata)
- Optimized transaction log with parallel I/O

### Checking Table Protocol

```scala
// Check current protocol version
val snapshot = transactionLog.getSnapshot()
println(s"Protocol: reader=${snapshot.protocol.minReaderVersion}, writer=${snapshot.protocol.minWriterVersion}")
```

### Migration Guide

**Existing tables** are automatically compatible:
- Tables without protocol default to version 1
- First write operation adds protocol action
- No data migration required

**Version upgrades** happen automatically:
- Using new features triggers protocol upgrade
- Upgrade is one-way (cannot downgrade)
- Old clients are blocked from accessing upgraded tables
```

### 10. Implementation Phases

#### Phase 1: Foundation (Week 1)
- [ ] Add ProtocolAction case class
- [ ] Add protocol constants and version numbers
- [ ] Add ProtocolVersionException
- [ ] Update JSON serialization/deserialization

#### Phase 2: Core Logic (Week 1-2)
- [ ] Implement getProtocol() with caching
- [ ] Add assertTableReadable() checks
- [ ] Add assertTableWritable() checks
- [ ] Update initialize() to write protocol
- [ ] Add upgradeProtocol() method

#### Phase 3: Integration (Week 2)
- [ ] Integrate checks into all read paths
- [ ] Integrate checks into all write paths
- [ ] Update checkpoint format to include protocol
- [ ] Add backwards compatibility for legacy tables

#### Phase 4: Testing (Week 2-3)
- [ ] Write unit tests for protocol actions
- [ ] Write unit tests for version checking
- [ ] Write integration tests for compatibility
- [ ] Test migration scenarios
- [ ] Performance testing

#### Phase 5: Documentation (Week 3)
- [ ] Update CLAUDE.md with protocol documentation
- [ ] Create migration guide
- [ ] Update API documentation
- [ ] Add inline code comments

## Open Questions

1. **Should we support protocol downgrade?**
   - Delta Lake: No (one-way upgrade only)
   - Recommendation: Follow Delta, no downgrade support

2. **When to introduce table features (version 3+)?**
   - Recommendation: Wait until we have 5+ features that need fine-grained control

3. **Should protocol checks be configurable?**
   - Recommendation: Yes, add `spark.indextables.protocol.checkEnabled` for testing/debugging
   - Default: `true` for production safety

4. **How to handle protocol in optimized transaction log?**
   - Recommendation: Cache protocol separately from files
   - Protocol changes are rare, optimize for the common case

## Success Criteria

1. ✅ Old readers blocked from reading new tables (when minReaderVersion too high)
2. ✅ Old writers blocked from writing to new tables (when minWriterVersion too high)
3. ✅ New readers can read old tables (backwards compatibility)
4. ✅ Legacy tables without protocol work seamlessly
5. ✅ Protocol persists in checkpoints
6. ✅ Zero performance impact when protocol doesn't change
7. ✅ Clear error messages guide users to upgrade
8. ✅ All existing tests pass without modification

## References

- [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Delta Lake Actions Implementation](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala)
- [Delta Lake Protocol Versioning Suite](https://github.com/delta-io/delta/blob/master/spark/src/test/scala/org/apache/spark/sql/delta/DeltaProtocolVersionSuite.scala)

## Appendix A: Example Transaction Log with Protocol

```json
// 00000000000000000000.json (initial version)
{"protocol":{"minReaderVersion":2,"minWriterVersion":2}}
{"metaData":{"id":"uuid-here","name":null,"format":{"provider":"tantivy4spark","options":{}},"schemaString":"{...}","partitionColumns":[],"configuration":{},"createdTime":1234567890}}

// 00000000000000000001.json
{"add":{"path":"s3://bucket/split1.split","partitionValues":{},"size":1048576,"modificationTime":1234567890,"dataChange":true,"numRecords":10000}}

// 00000000000000000002.json
{"add":{"path":"s3://bucket/split2.split","partitionValues":{},"size":2097152,"modificationTime":1234567890,"dataChange":true,"numRecords":20000}}
{"skip":{"path":"s3://bucket/corrupt.split","skipTimestamp":1234567890,"reason":"Corrupted footer","operation":"merge"}}
```

## Appendix B: Version Compatibility Matrix

| Writer Version | Can Write To | Notes |
|----------------|--------------|-------|
| 1 | v1 tables | Legacy features only |
| 2 | v1, v2 tables | Auto-upgrades v1 to v2 when using new features |

| Reader Version | Can Read From | Notes |
|----------------|---------------|-------|
| 1 | v1 tables | Cannot read SkipAction, extended metadata |
| 2 | v1, v2 tables | Full compatibility |

## Conclusion

This design provides a robust protocol versioning system modeled after Delta Lake's proven approach. It enables:

1. **Safe evolution** of the transaction log format
2. **Clear compatibility** boundaries between versions
3. **Graceful migration** from legacy tables
4. **Future-proof** architecture for table features

The implementation is phased to minimize risk and can be deployed incrementally across releases.
