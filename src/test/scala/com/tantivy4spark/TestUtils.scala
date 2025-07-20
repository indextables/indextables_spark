/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tantivy4spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.mockito.MockitoSugar
import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.util.Random

object SharedSparkSession {
  @volatile private var _spark: SparkSession = _
  
  def getOrCreate(): SparkSession = {
    if (_spark == null || _spark.sparkContext.isStopped) {
      synchronized {
        if (_spark == null || _spark.sparkContext.isStopped) {
          _spark = SparkSession.builder()
            .appName("TantivyHandlerTest")
            .master("local[2]")
            .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrationRequired", "false")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .getOrCreate()
          
          _spark.sparkContext.setLogLevel("WARN")
        }
      }
    }
    _spark
  }
  
  def stop(): Unit = {
    if (_spark != null && !_spark.sparkContext.isStopped) {
      _spark.stop()
      _spark = null
    }
  }
}

trait TestUtils {
  
  def spark: SparkSession = SharedSparkSession.getOrCreate()
  
  def initializeSpark(): Unit = {
    SharedSparkSession.getOrCreate()
  }
  
  def stopSpark(): Unit = {
    // Only stop if this is the last test or explicit cleanup
    // SharedSparkSession.stop()
  }
  
  def createTempDir(prefix: String = "tantivy-test"): Path = {
    Files.createTempDirectory(prefix)
  }
  
  def deleteTempDir(path: Path): Unit = {
    if (Files.exists(path)) {
      import scala.collection.JavaConverters._
      import scala.util.{Try, Success, Failure}
      
      // Use try-with-resources pattern to ensure stream is closed
      Try {
        val stream = Files.walk(path)
        try {
          // Collect all paths first, then delete in reverse order
          val pathsToDelete = stream.iterator().asScala.toList.reverse
          pathsToDelete.foreach { p =>
            Try(Files.deleteIfExists(p)) match {
              case Success(_) => // File deleted successfully
              case Failure(_) => // Ignore failures - file might already be deleted
            }
          }
        } finally {
          stream.close()
        }
      } match {
        case Success(_) => // Directory deletion completed
        case Failure(_) => // Ignore failures - directory might already be cleaned up
      }
    }
  }
}

object TestSchemas {
  
  val basicSchema = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("title", StringType, nullable = false),
    StructField("content", StringType, nullable = true),
    StructField("timestamp", TimestampType, nullable = false),
    StructField("score", DoubleType, nullable = true),
    StructField("active", BooleanType, nullable = false)
  ))
  
  val logSchema = StructType(Seq(
    StructField("timestamp", TimestampType, nullable = false),
    StructField("level", StringType, nullable = false),
    StructField("message", StringType, nullable = false),
    StructField("service", StringType, nullable = false),
    StructField("host", StringType, nullable = true),
    StructField("duration_ms", LongType, nullable = true),
    StructField("status_code", IntegerType, nullable = true)
  ))
  
  val complexSchema = StructType(Seq(
    StructField("id", StringType, nullable = false),
    StructField("nested", StructType(Seq(
      StructField("field1", StringType, nullable = true),
      StructField("field2", IntegerType, nullable = true)
    )), nullable = true),
    StructField("array_field", ArrayType(StringType), nullable = true),
    StructField("map_field", MapType(StringType, StringType), nullable = true)
  ))
}

object TestDataGenerator {
  
  private val random = new Random(42) // Fixed seed for reproducible tests
  
  def generateBasicRows(count: Int): Seq[InternalRow] = {
    (1 to count).map { i =>
      InternalRow(
        i.toLong,
        UTF8String.fromString(s"Title $i"),
        UTF8String.fromString(s"Content for document $i with some searchable text"),
        System.currentTimeMillis() + i * 1000,
        random.nextDouble() * 100,
        i % 2 == 0
      )
    }
  }
  
  def generateLogRows(count: Int): Seq[InternalRow] = {
    val levels = Array("ERROR", "WARN", "INFO", "DEBUG")
    val services = Array("api", "auth", "db", "cache")
    val hosts = Array("host1", "host2", "host3")
    
    (1 to count).map { i =>
      InternalRow(
        System.currentTimeMillis() + i * 1000,
        UTF8String.fromString(levels(random.nextInt(levels.length))),
        UTF8String.fromString(s"Log message $i with details"),
        UTF8String.fromString(services(random.nextInt(services.length))),
        UTF8String.fromString(hosts(random.nextInt(hosts.length))),
        random.nextLong() % 1000 + 1,
        if (random.nextBoolean()) 200 + random.nextInt(400) else null
      )
    }
  }
  
  def generateSearchableContent(count: Int): Seq[Map[String, Any]] = {
    val words = Array("error", "warning", "success", "failure", "timeout", "connection", "database", "api", "user", "request")
    
    (1 to count).map { i =>
      Map(
        "id" -> s"doc_$i",
        "title" -> s"Document $i",
        "content" -> words.take(Math.max(1, random.nextInt(5) + 1)).mkString(" "),
        "category" -> Seq("tech", "business", "support")(random.nextInt(3)),
        "timestamp" -> (System.currentTimeMillis() + i * 1000L),
        "score" -> random.nextDouble() * 100
      )
    }
  }
}

object MockTantivyNative {
  
  var searchResults: Map[String, String] = Map.empty
  var indexedDocuments: List[String] = List.empty
  var configs: Map[Long, String] = Map.empty
  var engines: Map[Long, String] = Map.empty
  var writers: Map[Long, String] = Map.empty
  var nextId: Long = 1
  
  def reset(): Unit = {
    searchResults = Map.empty
    indexedDocuments = List.empty
    configs = Map.empty
    engines = Map.empty
    writers = Map.empty
    nextId = 1
  }
  
  def addSearchResult(query: String, result: String): Unit = {
    searchResults = searchResults + (query -> result)
  }
  
  def createMockConfig(): Long = {
    val id = nextId
    nextId += 1
    configs = configs + (id -> "mock_config")
    id
  }
  
  def createMockEngine(configId: Long): Long = {
    val id = nextId
    nextId += 1
    engines = engines + (id -> s"mock_engine_$configId")
    id
  }
  
  def createMockWriter(configId: Long): Long = {
    val id = nextId
    nextId += 1
    writers = writers + (id -> s"mock_writer_$configId")
    id
  }
}

case class TestOptions(
    indexId: String = "test_index",
    basePath: String = "./test-data",
    maxResults: Int = 100,
    batchSize: Int = 10,
    segmentSize: Long = 1024 * 1024, // 1MB for tests
    additional: Map[String, String] = Map.empty
) {
  def toMap: Map[String, String] = {
    Map(
      "index.id" -> indexId,
      "tantivy.base.path" -> basePath,
      "max.results" -> maxResults.toString,
      "batch.size" -> batchSize.toString,
      "segment.size" -> segmentSize.toString
    ) ++ additional
  }
}

object FileTestUtils {
  
  def withTempFile[T](suffix: String = ".tmp")(block: File => T): T = {
    val file = File.createTempFile("tantivy-test", suffix)
    try {
      block(file)
    } finally {
      file.delete()
    }
  }
  
  def withTempDir[T](prefix: String = "tantivy-test")(block: Path => T): T = {
    val dir = Files.createTempDirectory(prefix)
    try {
      block(dir)
    } finally {
      deleteRecursively(dir)
    }
  }
  
  private def deleteRecursively(path: Path): Unit = {
    if (Files.exists(path)) {
      import scala.collection.JavaConverters._
      import scala.util.{Try, Success, Failure}
      
      // Use try-with-resources pattern to ensure stream is closed
      Try {
        val stream = Files.walk(path)
        try {
          // Collect all paths first, then delete in reverse order
          val pathsToDelete = stream.iterator().asScala.toList.reverse
          pathsToDelete.foreach { p =>
            Try(Files.deleteIfExists(p)) match {
              case Success(_) => // File deleted successfully
              case Failure(_) => // Ignore failures - file might already be deleted
            }
          }
        } finally {
          stream.close()
        }
      } match {
        case Success(_) => // Directory deletion completed
        case Failure(_) => // Ignore failures - directory might already be cleaned up
      }
    }
  }
  
  def createTestFile(dir: Path, filename: String, content: String): Path = {
    val file = dir.resolve(filename)
    Files.write(file, content.getBytes("UTF-8"))
    file
  }
}

trait TantivyTestBase extends TestUtils with Suite with BeforeAndAfterAll with BeforeAndAfterEach with MockitoSugar {
  
  protected var testDir: Path = _
  protected val testOptions = TestOptions()
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeSpark()
  }

  override def afterAll(): Unit = {
    // Don't stop Spark here - let it be shared across test classes
    // Only clean up test-specific resources
    super.afterAll()  
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    testDir = createTempDir("tantivy-test")
    MockTantivyNative.reset()
  }
  
  override def afterEach(): Unit = {
    if (testDir != null) {
      deleteTempDir(testDir)
    }
    super.afterEach()
  }
  
  protected def createTestConfiguration(): Configuration = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "file:///")
    conf
  }
}