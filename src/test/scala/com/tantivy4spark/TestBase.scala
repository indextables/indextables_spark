package com.tantivy4spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.io.File
import java.nio.file.{Files, Paths}
import scala.util.Random

trait TestBase extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  protected var spark: SparkSession = _
  protected var tempDir: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Tantivy4Spark Tests")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("tantivy4spark-test").toString
  }

  override def afterEach(): Unit = {
    if (tempDir != null) {
      deleteRecursively(new File(tempDir))
    }
  }

  protected def createTestDataFrame(): DataFrame = {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val data = Seq(
      (1, "John Doe", 30, "Engineer", 75000.0, true),
      (2, "Jane Smith", 25, "Data Scientist", 85000.0, false),
      (3, "Bob Johnson", 35, "Manager", 95000.0, true),
      (4, "Alice Brown", 28, "Designer", 70000.0, false),
      (5, "Charlie Wilson", 32, "Developer", 80000.0, true)
    )

    spark.createDataFrame(data).toDF("id", "name", "age", "role", "salary", "active")
  }

  protected def createLargeTestDataFrame(numRows: Int = 10000): DataFrame = {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val random = new Random(42) // Deterministic seed for tests
    val roles = Array("Engineer", "Data Scientist", "Manager", "Designer", "Developer")
    val names = Array("John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank")
    val lastNames = Array("Doe", "Smith", "Johnson", "Brown", "Wilson", "Davis", "Miller", "Moore")

    val data = (1 until numRows + 1).map { i =>
      val name = s"${names(random.nextInt(names.length))} ${lastNames(random.nextInt(lastNames.length))}"
      val age = 20 + random.nextInt(40)
      val role = roles(random.nextInt(roles.length))
      val salary = 50000 + random.nextInt(100000)
      val active = random.nextBoolean()
      
      (i, name, age, role, salary.toDouble, active)
    }

    spark.createDataFrame(data).toDF("id", "name", "age", "role", "salary", "active")
  }

  protected def getTestSchema(): StructType = {
    StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("role", StringType, nullable = true),
      StructField("salary", DoubleType, nullable = true),
      StructField("active", BooleanType, nullable = true)
    ))
  }

  protected def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  protected def withTempPath(f: String => Unit): Unit = {
    val path = Files.createTempDirectory("tantivy4spark").toString
    try {
      f(path)
    } finally {
      deleteRecursively(new File(path))
    }
  }
}