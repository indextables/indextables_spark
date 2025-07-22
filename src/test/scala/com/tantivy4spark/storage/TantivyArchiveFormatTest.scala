package com.tantivy4spark.storage

import com.tantivy4spark.TestBase
import org.apache.hadoop.fs.Path
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.file.{Files, Paths}
import scala.util.Random

class TantivyArchiveFormatTest extends TestBase {

  test("should create and read archive with single component") {
    val testData = "Hello, Tantivy!".getBytes("UTF-8")
    val components = Map("test.txt" -> testData)
    
    // Create archive
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    val archiveData = outputStream.toByteArray
    
    // Verify archive is not empty
    archiveData.length should be > 0
    
    // Create mock storage strategy
    val mockStorage = new MockStorageStrategy(archiveData)
    
    // Read back components
    val readComponents = TantivyArchiveFormat.readAllComponents(mockStorage)
    
    readComponents should have size 1
    readComponents should contain key "test.txt"
    new String(readComponents("test.txt"), "UTF-8") should be ("Hello, Tantivy!")
  }

  test("should create and read archive with multiple components") {
    val components = Map(
      "schema.json" -> """{"fields": ["title", "content"]}""".getBytes("UTF-8"),
      "segment_1.idx" -> generateRandomBytes(1024),
      "segment_1.pos" -> generateRandomBytes(512),
      "segment_1.doc" -> generateRandomBytes(2048),
      "meta.json" -> """{"version": "1.0"}""".getBytes("UTF-8")
    )
    
    // Create archive
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    val archiveData = outputStream.toByteArray
    
    // Create mock storage strategy
    val mockStorage = new MockStorageStrategy(archiveData)
    
    // Read back components
    val readComponents = TantivyArchiveFormat.readAllComponents(mockStorage)
    
    readComponents should have size 5
    components.keys.foreach { name =>
      readComponents should contain key name
      readComponents(name) should equal (components(name))
    }
  }

  test("should handle empty archive") {
    val components = Map.empty[String, Array[Byte]]
    
    // Create archive
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    val archiveData = outputStream.toByteArray
    
    // Create mock storage strategy
    val mockStorage = new MockStorageStrategy(archiveData)
    
    // Read back components
    val readComponents = TantivyArchiveFormat.readAllComponents(mockStorage)
    
    readComponents should be (empty)
  }

  test("should handle large components efficiently") {
    val largeData = generateRandomBytes(10 * 1024 * 1024) // 10MB
    val components = Map(
      "large_segment.idx" -> largeData,
      "small_meta.json" -> """{"size": "large"}""".getBytes("UTF-8")
    )
    
    // Create archive
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    val archiveData = outputStream.toByteArray
    
    // Create mock storage strategy
    val mockStorage = new MockStorageStrategy(archiveData)
    
    // Read back components
    val readComponents = TantivyArchiveFormat.readAllComponents(mockStorage)
    
    readComponents should have size 2
    readComponents("large_segment.idx") should equal (largeData)
    new String(readComponents("small_meta.json"), "UTF-8") should be ("""{"size": "large"}""")
  }

  test("should support efficient random access to components") {
    val components = Map(
      "component_a.dat" -> generateRandomBytes(1000),
      "component_b.dat" -> generateRandomBytes(2000),
      "component_c.dat" -> generateRandomBytes(3000)
    )
    
    // Create archive
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    val archiveData = outputStream.toByteArray
    
    // Create mock storage strategy with range tracking
    val mockStorage = new MockStorageStrategy(archiveData)
    val archiveReader = new TantivyArchiveReader(mockStorage)
    
    // Test random access
    archiveReader.hasComponent("component_b.dat") should be (true)
    archiveReader.hasComponent("nonexistent.dat") should be (false)
    
    val componentNames = archiveReader.getComponentNames()
    componentNames should contain allElementsOf components.keys
    
    // Read individual components
    val componentB = archiveReader.readComponent("component_b.dat")
    componentB should equal (components("component_b.dat"))
    
    val componentA = archiveReader.readComponent("component_a.dat")
    componentA should equal (components("component_a.dat"))
  }

  test("should validate archive format correctly") {
    // Valid archive
    val components = Map("test.dat" -> "test data".getBytes("UTF-8"))
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    val validArchive = outputStream.toByteArray
    
    val validStorage = new MockStorageStrategy(validArchive)
    TantivyArchiveFormat.validateArchive(validStorage) should be (true)
    
    // Invalid archive (random bytes)
    val invalidArchive = generateRandomBytes(100)
    val invalidStorage = new MockStorageStrategy(invalidArchive)
    TantivyArchiveFormat.validateArchive(invalidStorage) should be (false)
    
    // Empty archive
    val emptyStorage = new MockStorageStrategy(Array.empty[Byte])
    TantivyArchiveFormat.validateArchive(emptyStorage) should be (false)
  }

  test("should handle Unicode component names correctly") {
    val components = Map(
      "文档索引.idx" -> "Chinese index".getBytes("UTF-8"),
      "индекс.dat" -> "Russian index".getBytes("UTF-8"),
      "🔍search.json" -> """{"emoji": "supported"}""".getBytes("UTF-8")
    )
    
    // Create archive
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    val archiveData = outputStream.toByteArray
    
    // Create mock storage strategy
    val mockStorage = new MockStorageStrategy(archiveData)
    
    // Read back components
    val readComponents = TantivyArchiveFormat.readAllComponents(mockStorage)
    
    readComponents should have size 3
    components.keys.foreach { name =>
      readComponents should contain key name
      readComponents(name) should equal (components(name))
    }
  }

  test("should throw exception for nonexistent component") {
    val components = Map("existing.dat" -> "data".getBytes("UTF-8"))
    
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    val archiveData = outputStream.toByteArray
    
    val mockStorage = new MockStorageStrategy(archiveData)
    val archiveReader = new TantivyArchiveReader(mockStorage)
    
    intercept[IllegalArgumentException] {
      archiveReader.readComponent("nonexistent.dat")
    }
    
    intercept[IllegalArgumentException] {
      archiveReader.getComponentSize("nonexistent.dat")
    }
  }

  test("should handle archive with very long component names") {
    val longName = "a" * 1000 // 1000 character name
    val components = Map(longName -> "long name data".getBytes("UTF-8"))
    
    val outputStream = new ByteArrayOutputStream()
    TantivyArchiveFormat.createArchive(components, outputStream)
    val archiveData = outputStream.toByteArray
    
    val mockStorage = new MockStorageStrategy(archiveData)
    val readComponents = TantivyArchiveFormat.readAllComponents(mockStorage)
    
    readComponents should contain key longName
    new String(readComponents(longName), "UTF-8") should be ("long name data")
  }

  private def generateRandomBytes(size: Int): Array[Byte] = {
    val random = new Random(42) // Fixed seed for reproducibility
    val bytes = new Array[Byte](size)
    random.nextBytes(bytes)
    bytes
  }
}

/**
 * Mock storage strategy for testing that tracks range reads.
 */
class MockStorageStrategy(data: Array[Byte]) extends StorageStrategy {
  private var closed = false
  
  override def readFile(): Array[Byte] = {
    checkClosed()
    data
  }
  
  override def readRange(offset: Long, length: Long): Array[Byte] = {
    checkClosed()
    if (offset < 0 || length < 0 || offset + length > data.length) {
      throw new IllegalArgumentException(s"Invalid range: offset=$offset, length=$length, fileSize=${data.length}")
    }
    
    val result = new Array[Byte](length.toInt)
    System.arraycopy(data, offset.toInt, result, 0, length.toInt)
    result
  }
  
  override def getFileSize(): Long = {
    checkClosed()
    data.length.toLong
  }
  
  override def close(): Unit = {
    closed = true
  }
  
  private def checkClosed(): Unit = {
    if (closed) {
      throw new IllegalStateException("Storage strategy is closed")
    }
  }
}