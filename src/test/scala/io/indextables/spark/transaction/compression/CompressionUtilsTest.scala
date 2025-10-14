package io.indextables.spark.transaction.compression

import org.scalatest.funsuite.AnyFunSuite

class CompressionUtilsTest extends AnyFunSuite {

  test("compress and decompress with GZIP") {
    val original = """{"add":{"path":"file.split","size":1000}}""".getBytes("UTF-8")
    val codec    = new GzipCompressionCodec(6)

    val compressed = CompressionUtils.writeTransactionFile(original, Some(codec))
    assert(compressed(0) == CompressionUtils.MAGIC_BYTE)
    assert(compressed(1) == 0x01) // GZIP codec
    // Note: Small JSON may not compress well due to GZIP headers overhead

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

    val codec      = new GzipCompressionCodec(6)
    val compressed = CompressionUtils.writeTransactionFile(uncompressed, Some(codec))
    assert(CompressionUtils.isCompressed(compressed))
  }

  test("GZIP compression with different levels") {
    // Use larger JSON to ensure compression effectiveness
    val original = ("""{"add":{"path":"file.split","size":1000}}""" * 10).getBytes("UTF-8")

    // Test multiple compression levels
    for (level <- Seq(1, 6, 9)) {
      val codec      = new GzipCompressionCodec(level)
      val compressed = CompressionUtils.writeTransactionFile(original, Some(codec))

      // Verify compression
      assert(compressed(0) == CompressionUtils.MAGIC_BYTE)
      assert(compressed(1) == 0x01)
      assert(compressed.length < original.length, s"Level $level: ${compressed.length} should be < ${original.length}")

      // Verify decompression
      val decompressed = CompressionUtils.readTransactionFile(compressed)
      assert(decompressed.sameElements(original))
    }
  }

  test("handle large JSON documents") {
    // Create a large JSON document (1MB)
    val largeJson = """{"actions":[""" +
      (1 to 10000).map(i => s"""{"id":$i,"value":"value_$i"}""").mkString(",") +
      """]}}"""
    val original = largeJson.getBytes("UTF-8")

    val codec      = new GzipCompressionCodec(6)
    val compressed = CompressionUtils.writeTransactionFile(original, Some(codec))

    // Verify compression ratio
    val ratio = original.length.toDouble / compressed.length
    assert(ratio > 2.0, s"Expected compression ratio > 2x, got ${ratio}x")

    // Verify decompression
    val decompressed = CompressionUtils.readTransactionFile(compressed)
    assert(decompressed.sameElements(original))
  }

  test("getCodec returns correct codec") {
    // GZIP enabled
    val gzipCodec = CompressionUtils.getCodec(true, "gzip", 6)
    assert(gzipCodec.isDefined)
    assert(gzipCodec.get.name == "gzip")
    assert(gzipCodec.get.codecByte == 0x01)

    // Compression disabled
    val noCodec = CompressionUtils.getCodec(false, "gzip", 6)
    assert(noCodec.isEmpty)

    // None codec
    val noneCodec = CompressionUtils.getCodec(true, "none", 6)
    assert(noneCodec.isEmpty)
  }

  test("getCodec rejects unknown codec") {
    assertThrows[IllegalArgumentException] {
      CompressionUtils.getCodec(true, "unknown", 6)
    }
  }

  test("detectCodec correctly identifies compression") {
    val original = """{"add":{}}""".getBytes("UTF-8")

    // Uncompressed
    assert(CompressionUtils.detectCodec(original).isEmpty)

    // GZIP compressed
    val codec      = new GzipCompressionCodec(6)
    val compressed = CompressionUtils.writeTransactionFile(original, Some(codec))
    val detected   = CompressionUtils.detectCodec(compressed)
    assert(detected.isDefined)
    assert(detected.get.name == "gzip")
    assert(detected.get.codecByte == 0x01)
  }

  test("handle empty file error") {
    assertThrows[IllegalArgumentException] {
      CompressionUtils.readTransactionFile(Array.empty[Byte])
    }
  }

  test("handle truncated compressed file") {
    assertThrows[IllegalArgumentException] {
      // File with magic byte but missing codec byte
      CompressionUtils.readTransactionFile(Array(CompressionUtils.MAGIC_BYTE))
    }
  }

  test("handle unsupported compression codec") {
    // Create file with unsupported codec byte
    val fakeData = Array[Byte](CompressionUtils.MAGIC_BYTE, 0xFF.toByte) ++ Array.fill(100)(0.toByte)

    val exception = intercept[UnsupportedOperationException] {
      CompressionUtils.readTransactionFile(fakeData)
    }
    assert(exception.getMessage.contains("Unknown compression codec"))
  }

  test("GZIP compression level validation") {
    // Valid levels (0-9)
    for (level <- 0 to 9) {
      val codec = new GzipCompressionCodec(level)
      assert(codec.name == "gzip")
    }

    // Invalid level
    assertThrows[IllegalArgumentException] {
      new GzipCompressionCodec(10)
    }

    assertThrows[IllegalArgumentException] {
      new GzipCompressionCodec(-1)
    }
  }

  test("write transaction file without compression") {
    val original = """{"add":{"path":"file.split","size":1000}}""".getBytes("UTF-8")

    val uncompressed = CompressionUtils.writeTransactionFile(original, None)
    assert(uncompressed.sameElements(original))
    assert(!CompressionUtils.isCompressed(uncompressed))
  }

  test("compression provides space savings") {
    // Typical transaction log JSON repeated to ensure compression effectiveness
    val transaction = """{"add":{"path":"s3://bucket/table/part-00001.split","partitionValues":{"date":"2024-01-01","hour":"10"},"size":104857600,"modificationTime":1704067200000,"dataChange":true}}
"""
    val original    = (transaction * 20).getBytes("UTF-8") // Repeat to get better compression

    val codec      = new GzipCompressionCodec(6)
    val compressed = CompressionUtils.writeTransactionFile(original, Some(codec))

    // GZIP should provide at least 2x compression for repeated JSON
    val ratio = original.length.toDouble / compressed.length
    assert(ratio > 2.0, s"Expected at least 2x compression, got ${ratio}x")
  }

  test("round-trip compression preserves data integrity") {
    val testCases = Seq(
      """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""",
      """{"metaData":{"id":"uuid","name":"table","schemaString":"{}"}}""",
      """{"add":{"path":"file.split","size":1000}}""",
      """{"remove":{"path":"old.split","deletionTimestamp":1704067200000}}""",
      """{"mergeskip":{"path":"skip.split","reason":"corrupted"}}"""
    )

    val codec = new GzipCompressionCodec(6)
    testCases.foreach { json =>
      val original     = json.getBytes("UTF-8")
      val compressed   = CompressionUtils.writeTransactionFile(original, Some(codec))
      val decompressed = CompressionUtils.readTransactionFile(compressed)
      assert(decompressed.sameElements(original), s"Round-trip failed for: $json")
    }
  }
}
