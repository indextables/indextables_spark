# Transaction Log Compression Design

## Overview

High-performance compression for transaction logs and checkpoints to reduce download times and storage costs while maintaining backward compatibility with existing uncompressed files.

## Goals

1. **Reduce download times**: Compress transaction log files and checkpoints for faster S3 retrieval
2. **Reduce storage costs**: Minimize S3 storage requirements for transaction metadata
3. **Backward compatibility**: Support reading legacy uncompressed files without breaking existing tables
4. **Forward compatibility**: Allow gradual migration from uncompressed to compressed format
5. **Performance**: GZIP compression provides excellent speed/ratio tradeoff (3-5x compression, minimal CPU overhead)
6. **Simplicity**: No changes to file naming schemes, compression detected via magic bytes
7. **Zero dependencies**: Uses Java standard library GZIP (no external dependencies required)

## File Format Specification

### Compressed File Format

```
+-------------------+-------------------+-------------------+
| Magic Byte (0x01) | Compression Type  | Compressed Data   |
+-------------------+-------------------+-------------------+
| 1 byte            | 1 byte            | N bytes           |
+-------------------+-------------------+-------------------+
```

### Compression Type Byte Values

- **0x01**: GZIP compression (only supported type initially, uses Java standard library)
- **0x02-0xFF**: Reserved for future compression algorithms (ZSTD, LZ4, Snappy, etc.)

### Uncompressed File Format (Legacy)

```
+-------------------+
| Raw JSON Data     |
+-------------------+
| N bytes           |
+-------------------+
```

**Detection**: If first byte is NOT `0x01`, file is treated as uncompressed legacy format

## Configuration

### New Configuration Options

```scala
// Compression mode for new transaction log files and checkpoints
// Values: "gzip", "none"
// Default: "gzip"
spark.indextables.transaction.compression.enabled = true
spark.indextables.transaction.compression.codec = "gzip"

// GZIP compression level (0-9, higher = better compression but slower)
// Default: 6 (balanced speed/compression, GZIP default)
spark.indextables.transaction.compression.gzip.level = 6

// Whether to compress checkpoints (usually larger, benefit more from compression)
// Default: true
spark.indextables.checkpoint.compression.enabled = true
```

### Configuration Hierarchy

1. **Write Options**: `.option("spark.indextables.transaction.compression.codec", "gzip")`
2. **Spark Session**: `spark.conf.set("spark.indextables.transaction.compression.codec", "gzip")`
3. **System Default**: `"gzip"`

## Implementation Components

### 1. CompressionCodec Interface

```scala
package io.indextables.spark.transaction.compression

import java.io.{InputStream, OutputStream}

/**
 * Abstraction for compression codecs used in transaction logs.
 */
trait CompressionCodec {
  /** Codec identifier byte (0x01 for ZSTD, etc.) */
  def codecByte: Byte

  /** Codec name for configuration */
  def name: String

  /** Compress data from input stream to output stream */
  def compress(input: InputStream, output: OutputStream): Unit

  /** Decompress data from input stream to output stream */
  def decompress(input: InputStream, output: OutputStream): Unit

  /** Compress byte array */
  def compress(data: Array[Byte]): Array[Byte]

  /** Decompress byte array */
  def decompress(data: Array[Byte]): Array[Byte]
}
```

### 2. GzipCompressionCodec Implementation

```scala
package io.indextables.spark.transaction.compression

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream, Deflater}

/**
 * GZIP compression codec for transaction logs.
 *
 * GZIP provides excellent compression ratios (3-5x) with minimal CPU overhead,
 * making it ideal for transaction log compression where download time is critical.
 * Uses Java standard library - no external dependencies required.
 */
class GzipCompressionCodec(level: Int = Deflater.DEFAULT_COMPRESSION) extends CompressionCodec {
  override def codecByte: Byte = 0x01
  override def name: String = "gzip"

  override def compress(input: InputStream, output: OutputStream): Unit = {
    val gzipOut = new GZIPOutputStream(output) {
      // Set compression level via reflection or custom constructor
      this.`def`.setLevel(level)
    }
    try {
      val buffer = new Array[Byte](8192)
      var bytesRead = 0
      while ({bytesRead = input.read(buffer); bytesRead != -1}) {
        gzipOut.write(buffer, 0, bytesRead)
      }
      gzipOut.flush()
    } finally {
      gzipOut.close()
    }
  }

  override def decompress(input: InputStream, output: OutputStream): Unit = {
    val gzipIn = new GZIPInputStream(input)
    try {
      val buffer = new Array[Byte](8192)
      var bytesRead = 0
      while ({bytesRead = gzipIn.read(buffer); bytesRead != -1}) {
        output.write(buffer, 0, bytesRead)
      }
      output.flush()
    } finally {
      gzipIn.close()
    }
  }

  override def compress(data: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val gzipOut = new GZIPOutputStream(baos) {
      this.`def`.setLevel(level)
    }
    try {
      gzipOut.write(data)
      gzipOut.flush()
    } finally {
      gzipOut.close()
    }
    baos.toByteArray
  }

  override def decompress(data: Array[Byte]): Array[Byte] = {
    val bais = new ByteArrayInputStream(data)
    val gzipIn = new GZIPInputStream(bais)
    val baos = new ByteArrayOutputStream()
    try {
      val buffer = new Array[Byte](8192)
      var bytesRead = 0
      while ({bytesRead = gzipIn.read(buffer); bytesRead != -1}) {
        baos.write(buffer, 0, bytesRead)
      }
      baos.toByteArray
    } finally {
      gzipIn.close()
    }
  }
}
```

### 3. CompressionUtils Helper

```scala
package io.indextables.spark.transaction.compression

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

/**
 * Utilities for reading/writing compressed transaction log files.
 */
object CompressionUtils {
  val MAGIC_BYTE: Byte = 0x01

  // Codec registry
  private val codecs: Map[Byte, CompressionCodec] = Map(
    0x01.toByte -> new ZstdCompressionCodec()
  )

  /**
   * Read transaction log file with automatic compression detection.
   *
   * @param data Raw file bytes
   * @return Decompressed JSON bytes
   */
  def readTransactionFile(data: Array[Byte]): Array[Byte] = {
    if (data.isEmpty) {
      throw new IllegalArgumentException("Transaction file is empty")
    }

    // Check for magic byte
    if (data(0) == MAGIC_BYTE) {
      // Compressed format
      if (data.length < 2) {
        throw new IllegalArgumentException("Compressed file too short (missing codec byte)")
      }

      val codecByte = data(1)
      val codec = codecs.getOrElse(codecByte, {
        throw new UnsupportedOperationException(
          s"Unknown compression codec: 0x${Integer.toHexString(codecByte & 0xFF)}"
        )
      })

      // Decompress data (skip magic byte + codec byte)
      val compressedData = data.slice(2, data.length)
      codec.decompress(compressedData)

    } else {
      // Legacy uncompressed format
      data
    }
  }

  /**
   * Write transaction log file with optional compression.
   *
   * @param jsonData JSON bytes to write
   * @param codec Optional compression codec (None = uncompressed)
   * @return File bytes with compression headers if applicable
   */
  def writeTransactionFile(jsonData: Array[Byte], codec: Option[CompressionCodec]): Array[Byte] = {
    codec match {
      case Some(c) =>
        // Compress data
        val compressed = c.compress(jsonData)

        // Build file: magic byte + codec byte + compressed data
        val output = new Array[Byte](2 + compressed.length)
        output(0) = MAGIC_BYTE
        output(1) = c.codecByte
        System.arraycopy(compressed, 0, output, 2, compressed.length)
        output

      case None =>
        // Uncompressed legacy format
        jsonData
    }
  }

  /**
   * Get codec from configuration.
   *
   * @param compressionEnabled Whether compression is enabled
   * @param codecName Codec name ("gzip", "none")
   * @param gzipLevel GZIP compression level (0-9)
   * @return Optional compression codec
   */
  def getCodec(
    compressionEnabled: Boolean,
    codecName: String,
    gzipLevel: Int = 6
  ): Option[CompressionCodec] = {
    if (!compressionEnabled || codecName.equalsIgnoreCase("none")) {
      None
    } else if (codecName.equalsIgnoreCase("gzip")) {
      Some(new GzipCompressionCodec(gzipLevel))
    } else {
      throw new IllegalArgumentException(
        s"Unknown compression codec: $codecName (supported: gzip, none)"
      )
    }
  }

  /**
   * Check if file data is compressed.
   */
  def isCompressed(data: Array[Byte]): Boolean = {
    data.nonEmpty && data(0) == MAGIC_BYTE
  }

  /**
   * Get compression codec from file data.
   */
  def detectCodec(data: Array[Byte]): Option[CompressionCodec] = {
    if (isCompressed(data) && data.length >= 2) {
      codecs.get(data(1))
    } else {
      None
    }
  }
}
```

## Integration Points

### 1. Transaction Log Reading

**Files to Update:**
- `TransactionLog.scala`: Update `readTransactionFile()` method
- `OptimizedTransactionLog.scala`: Update parallel reading logic
- `StandardTransactionLog.scala`: Update sequential reading logic

**Changes:**
```scala
// Before
val jsonData = readBytes(path)
val action = parseJson(jsonData)

// After
val rawData = readBytes(path)
val jsonData = CompressionUtils.readTransactionFile(rawData)
val action = parseJson(jsonData)
```

### 2. Transaction Log Writing

**Files to Update:**
- `TransactionLog.scala`: Update `writeTransactionFile()` method
- `OptimizedTransactionLog.scala`: Update async writing logic

**Changes:**
```scala
// Before
val jsonData = serializeToJson(action)
writeBytes(path, jsonData)

// After
val jsonData = serializeToJson(action)
val codec = CompressionUtils.getCodec(
  compressionEnabled = config.transactionCompressionEnabled,
  codecName = config.transactionCompressionCodec,
  gzipLevel = config.transactionCompressionGzipLevel
)
val fileData = CompressionUtils.writeTransactionFile(jsonData, codec)
writeBytes(path, fileData)
```

### 3. Checkpoint Reading

**Files to Update:**
- `TransactionLog.scala`: Update `readCheckpoint()` method
- `CheckpointManager.scala`: Update checkpoint loading logic

**Changes:**
```scala
// Before
val jsonData = readBytes(checkpointPath)
val checkpoint = parseCheckpoint(jsonData)

// After
val rawData = readBytes(checkpointPath)
val jsonData = CompressionUtils.readTransactionFile(rawData)
val checkpoint = parseCheckpoint(jsonData)
```

### 4. Checkpoint Writing

**Files to Update:**
- `CheckpointManager.scala`: Update `createCheckpoint()` method

**Changes:**
```scala
// Before
val jsonData = serializeCheckpoint(checkpoint)
writeBytes(checkpointPath, jsonData)

// After
val jsonData = serializeCheckpoint(checkpoint)
val codec = CompressionUtils.getCodec(
  compressionEnabled = config.checkpointCompressionEnabled,
  codecName = config.transactionCompressionCodec,
  gzipLevel = config.transactionCompressionGzipLevel
)
val fileData = CompressionUtils.writeTransactionFile(jsonData, codec)
writeBytes(checkpointPath, fileData)
```

### 5. Log Repair Command

**Files to Update:**
- `RepairTransactionLogCommand.scala`: Update to compress repaired logs
- `RepairTransactionLogParser.scala`: No changes needed (just parsing)

**Changes:**
```scala
// In RepairTransactionLogCommand.run()
val repairedActions = analyzeAndRepair(tablePath)

// Write repaired transaction log with compression
val codec = CompressionUtils.getCodec(
  compressionEnabled = true,  // Always compress repaired logs
  codecName = "gzip",
  gzipLevel = 6
)

repairedActions.foreach { case (version, action) =>
  val jsonData = serializeAction(action)
  val fileData = CompressionUtils.writeTransactionFile(jsonData, codec)
  val path = s"$tablePath/_transaction_log/${version.toString.padLeft(20, '0')}.json"
  writeBytes(path, fileData)
}
```

## Configuration Implementation

### Configuration Case Class Updates

```scala
// In IndexTablesConfig.scala or similar

case class TransactionLogConfig(
  // Existing fields...

  // Compression configuration
  compressionEnabled: Boolean = true,
  compressionCodec: String = "gzip",
  compressionGzipLevel: Int = 6,
  checkpointCompressionEnabled: Boolean = true
)

object TransactionLogConfig {
  def fromSparkConf(conf: Map[String, String]): TransactionLogConfig = {
    TransactionLogConfig(
      // Existing fields...

      compressionEnabled = conf.getOrElse(
        "spark.indextables.transaction.compression.enabled", "true"
      ).toBoolean,

      compressionCodec = conf.getOrElse(
        "spark.indextables.transaction.compression.codec", "gzip"
      ),

      compressionGzipLevel = conf.getOrElse(
        "spark.indextables.transaction.compression.gzip.level", "6"
      ).toInt,

      checkpointCompressionEnabled = conf.getOrElse(
        "spark.indextables.checkpoint.compression.enabled", "true"
      ).toBoolean
    )
  }
}
```

## Migration Strategy

### Phase 1: Read Support (Week 1)
1. Implement `CompressionCodec` interface and `GzipCompressionCodec`
2. Implement `CompressionUtils` with read/write helpers
3. Update all transaction log reading code to support compressed files
4. Update all checkpoint reading code to support compressed files
5. **No breaking changes**: All writes remain uncompressed
6. **No new dependencies**: Uses Java standard library only

### Phase 2: Write Support (Week 2)
1. Add configuration options for compression
2. Update transaction log writing to use compression (default: enabled)
3. Update checkpoint writing to use compression (default: enabled)
4. Update log repair command to compress repaired logs
5. **Gradual migration**: New files compressed, old files remain readable

### Phase 3: Testing & Validation (Week 3)
1. Unit tests for compression/decompression
2. Integration tests for mixed compressed/uncompressed files
3. Performance benchmarks (compression ratio, CPU overhead, download time)
4. Compatibility tests with existing tables
5. Log repair command tests with compression

### Phase 4: Documentation & Rollout (Week 4)
1. Update CLAUDE.md with compression documentation
2. Add performance tuning guidelines
3. Create migration guide for existing deployments
4. Monitor production deployments for issues

## Performance Expectations

### Compression Ratios
Based on typical JSON transaction log data:

- **Transaction files**: 4-6x compression (JSON is highly compressible)
- **Checkpoint files**: 5-8x compression (larger files, better ratios)
- **Overall storage reduction**: 75-85% for transaction metadata

### Performance Impact

- **Compression overhead**: 10-20ms per file (GZIP level 6)
- **Decompression overhead**: 5-10ms per file (GZIP)
- **Network time savings**: 70-80% reduction in download time from S3
- **Net performance**: 3-10x faster transaction log loading for remote storage

### Example Performance

**Before (Uncompressed):**
- Transaction file: 50KB
- Download time (S3): 100ms
- Total time: 100ms

**After (GZIP Compressed):**
- Compressed file: 10KB (5x compression)
- Download time (S3): 20ms
- Decompression time: 5ms
- Total time: 25ms
- **Speedup: 4.0x**

## Testing Strategy

### Unit Tests

```scala
class CompressionUtilsTest extends AnyFunSuite {
  test("compress and decompress with GZIP") {
    val original = """{"add":{"path":"file.split","size":1000}}""".getBytes("UTF-8")
    val codec = new GzipCompressionCodec(6)

    val compressed = CompressionUtils.writeTransactionFile(original, Some(codec))
    assert(compressed(0) == CompressionUtils.MAGIC_BYTE)
    assert(compressed(1) == 0x01) // GZIP codec
    assert(compressed.length < original.length) // Should be smaller

    val decompressed = CompressionUtils.readTransactionFile(compressed)
    assert(decompressed.sameElements(original))
  }

  test("read legacy uncompressed files") {
    val original = """{"add":{"path":"file.split","size":1000}}""".getBytes("UTF-8")

    val decompressed = CompressionUtils.readTransactionFile(original)
    assert(decompressed.sameElements(original))
  }

  test("detect compression correctly") {
    val uncompressed = """{"add":{}}""".getBytes("UTF-8")
    assert(!CompressionUtils.isCompressed(uncompressed))

    val codec = new GzipCompressionCodec(6)
    val compressed = CompressionUtils.writeTransactionFile(uncompressed, Some(codec))
    assert(CompressionUtils.isCompressed(compressed))
  }
}
```

### Integration Tests

```scala
class CompressedTransactionLogTest extends AnyFunSuite {
  test("write and read transaction log with compression") {
    val tablePath = "s3://test-bucket/compressed-table"

    // Write with compression enabled
    val df = spark.range(100).toDF()
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .option("spark.indextables.transaction.compression.codec", "gzip")
      .save(tablePath)

    // Read back - should work transparently
    val loaded = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(loaded.count() == 100)
  }

  test("read mixed compressed and uncompressed transaction logs") {
    // Simulate table with legacy uncompressed logs + new compressed logs
    val tablePath = "s3://test-bucket/mixed-table"

    // Write v1 uncompressed
    df1.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "false")
      .save(tablePath)

    // Append v2 compressed
    df2.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.transaction.compression.enabled", "true")
      .mode("append")
      .save(tablePath)

    // Read all data
    val loaded = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)
    assert(loaded.count() == df1.count() + df2.count())
  }
}
```

## Dependencies

### No External Dependencies Required

**GZIP compression uses Java standard library:**
- `java.util.zip.GZIPInputStream`
- `java.util.zip.GZIPOutputStream`
- `java.util.zip.Deflater`

**Benefits:**
- Zero additional dependencies
- Available in all Java environments
- Well-tested and production-proven
- No compatibility concerns
- Smaller deployment footprint

## Error Handling

### Graceful Degradation

```scala
try {
  val decompressed = CompressionUtils.readTransactionFile(rawData)
  parseJson(decompressed)
} catch {
  case e: UnsupportedOperationException =>
    // Unknown compression codec - log error and fail
    logError(s"Unsupported compression codec in file: ${e.getMessage}")
    throw e

  case e: Exception =>
    // Decompression failure - possibly corrupted file
    logWarning(s"Failed to decompress transaction file, trying as uncompressed: ${e.getMessage}")
    parseJson(rawData) // Fallback to uncompressed
}
```

### Validation

```scala
// Validate compression configuration
if (config.compressionGzipLevel < 0 || config.compressionGzipLevel > 9) {
  throw new IllegalArgumentException(
    s"Invalid GZIP compression level: ${config.compressionGzipLevel} (must be 0-9)"
  )
}

if (!Seq("gzip", "none").contains(config.compressionCodec.toLowerCase)) {
  throw new IllegalArgumentException(
    s"Unknown compression codec: ${config.compressionCodec} (supported: gzip, none)"
  )
}
```

## Monitoring & Observability

### Metrics to Track

```scala
// Log compression statistics
logInfo(s"Transaction file compression: " +
  s"original=${originalSize}B, " +
  s"compressed=${compressedSize}B, " +
  s"ratio=${compressionRatio}x, " +
  s"time=${compressionTimeMs}ms")

// Track compression in checkpoint creation
logInfo(s"Checkpoint compression: " +
  s"actions=${actionCount}, " +
  s"uncompressed=${uncompressedSize}B, " +
  s"compressed=${compressedSize}B, " +
  s"ratio=${compressionRatio}x")
```

### Debug Logging

```scala
if (config.debugEnabled) {
  val codec = CompressionUtils.detectCodec(rawData)
  codec match {
    case Some(c) =>
      logDebug(s"Reading compressed file (codec=${c.name}): $path")
    case None =>
      logDebug(s"Reading uncompressed file: $path")
  }
}
```

## Future Enhancements

### Additional Compression Codecs
- **ZSTD**: Better compression ratio, requires dependency (codec byte: 0x02)
- **LZ4**: Faster compression/decompression, requires dependency (codec byte: 0x03)
- **Snappy**: Google's compression, requires dependency (codec byte: 0x04)

### Adaptive Compression
- Automatically choose codec based on file size
- Small files (< 1KB): No compression (overhead not worth it)
- Medium files (1KB-1MB): GZIP level 6 (current default)
- Large files (> 1MB): GZIP level 9 or ZSTD (future)

### Parallel Compression
- Compress checkpoint parts in parallel
- Use streaming compression for large files

## Conclusion

This design provides:

✅ **Backward compatibility**: Legacy uncompressed files work unchanged
✅ **Forward compatibility**: Gradual migration from uncompressed to compressed
✅ **Performance**: 3-10x faster transaction log loading from S3
✅ **Storage savings**: 75-85% reduction in transaction metadata storage
✅ **Simplicity**: No file naming changes, automatic detection
✅ **Extensibility**: Easy to add new compression codecs (ZSTD, LZ4, etc.)
✅ **Safety**: Robust error handling and validation
✅ **Log repair support**: Repair command automatically compresses repaired logs
✅ **Zero dependencies**: Uses Java standard library GZIP (no external dependencies)

Expected implementation time: **2-3 weeks** with comprehensive testing and validation.
