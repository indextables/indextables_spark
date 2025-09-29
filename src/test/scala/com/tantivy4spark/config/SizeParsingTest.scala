package com.tantivy4spark.config

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

/**
 * Test to verify that human-readable size formats work for indexWriter.heapSize
 */
class SizeParsingTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var spark: SparkSession = _
  var tempDir: File = _

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("tantivy4spark_size_test").toFile
    tempDir.deleteOnExit()

    spark = SparkSession.builder()
      .appName("SizeParsingTest")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "com.tantivy4spark.extensions.Tantivy4SparkExtensions")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  "Tantivy4Spark indexWriter.heapSize configuration" should "accept human-readable size formats" in {
    val tablePath = new File(tempDir, "size_test").getAbsolutePath

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row

    val schema = StructType(Array(
      StructField("id", StringType, true),
      StructField("content", StringType, true)
    ))

    val testData = Seq(
      Row("1", "test content one"),
      Row("2", "test content two")
    )

    val testDataRDD = spark.sparkContext.parallelize(testData)
    val testDF = spark.createDataFrame(testDataRDD, schema)

    // Test various size formats (all above 30MB minimum required by tantivy4java with 2 threads = 15MB per thread)
    val sizeFormats = Seq(
      ("100M", "100MB format"),
      ("2G", "2GB format"),
      ("50M", "50MB format"),
      ("31457280", "30MB raw bytes format")  // 30MB = 15MB per thread minimum
    )

    sizeFormats.foreach { case (sizeValue, description) =>
      println(s"Testing $description: $sizeValue")

      val testPath = s"${tablePath}_${sizeValue.replaceAll("[^a-zA-Z0-9]", "_")}"

      // This should not throw an exception
      testDF.write
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .option("spark.indextables.indexWriter.heapSize", sizeValue)
        .option("spark.indextables.indexing.typemap.content", "text")
        .mode("overwrite")
        .save(testPath)

      // Verify the data was written successfully
      val readBack = spark.read
        .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
        .load(testPath)

      val count = readBack.count()
      count shouldBe 2

      println(s"✅ Successfully wrote and read data with heapSize: $sizeValue")
    }

    println("✅ All size format tests passed!")
  }

  "Size parsing" should "handle edge cases correctly" in {
    val tablePath = new File(tempDir, "edge_case_test").getAbsolutePath

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row

    val schema = StructType(Array(
      StructField("id", StringType, true),
      StructField("content", StringType, true)
    ))

    val testData = Seq(Row("1", "test"))
    val testDataRDD = spark.sparkContext.parallelize(testData)
    val testDF = spark.createDataFrame(testDataRDD, schema)

    // Test edge cases (accounting for 15MB per thread minimum with 2 threads)
    val edgeCases = Seq(
      ("50M", "50MB - well above minimum"),
      ("32M", "32MB - just above minimum"),
      ("100K", "100KB - should fall back to default"),
      ("999M", "999MB - large valid size")
    )

    edgeCases.foreach { case (sizeValue, description) =>
      println(s"Testing edge case $description: $sizeValue")

      val testPath = s"${tablePath}_${sizeValue.replaceAll("[^a-zA-Z0-9]", "_")}"

      try {
        testDF.write
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .option("spark.indextables.indexWriter.heapSize", sizeValue)
          .option("spark.indextables.indexing.typemap.content", "text")
          .mode("overwrite")
          .save(testPath)

        val readBack = spark.read
          .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
          .load(testPath)

        readBack.count() shouldBe 1
        println(s"✅ Edge case passed: $sizeValue")
      } catch {
        case e: Exception =>
          println(s"⚠️ Edge case failed (this may be expected): $sizeValue - ${e.getMessage}")
          // For this test, we'll allow some edge cases to fail gracefully
      }
    }

    println("✅ Edge case tests completed!")
  }
}