package io.indextables.spark

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.nio.file.Files
import java.io.File
import java.io.FileInputStream
import java.util.{Properties, UUID}
import scala.util.Using

/**
 * EXACT REPRODUCTION of failing test 647 from RealS3IntegrationTest. This isolates the MERGE SPLITS test to debug the
 * fast field bug. Includes both local file and real S3 tests.
 */
class FastFieldLocalReproTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  var spark: SparkSession                      = _
  var tempDir: File                            = _
  var awsCredentials: Option[(String, String)] = None

  private val S3_BUCKET  = "test-tantivy4sparkbucket"
  private val S3_REGION  = "us-east-2"
  private val testRunId  = UUID.randomUUID().toString.substring(0, 8)
  private val s3TestPath = s"s3a://$S3_BUCKET/fast-field-repro-$testRunId"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession
      .builder()
      .appName("FastFieldLocalReproTest")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    tempDir = Files.createTempDirectory("fast-field-repro-test").toFile
    println(s"✅ Created temp directory: ${tempDir.getAbsolutePath}")

    // Load AWS credentials
    awsCredentials = loadAwsCredentials()
    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get
      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)
      println(s"✅ AWS credentials loaded for S3 bucket: $S3_BUCKET")
      println(s"📍 S3 test path: $s3TestPath")
    } else {
      println(s"⚠️  No AWS credentials - S3 test will be skipped")
    }
  }

  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (credFile.exists()) {
        val props = new Properties()
        Using(new FileInputStream(credFile))(fis => props.load(fis))

        val accessKey = props.getProperty("aws_access_key_id")
        val secretKey = props.getProperty("aws_secret_access_key")

        if (accessKey != null && secretKey != null) {
          Some((accessKey, secretKey))
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }

    if (tempDir != null && tempDir.exists()) {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) file.listFiles.foreach(deleteRecursively)
        file.delete()
      }
      deleteRecursively(tempDir)
      println(s"✅ Cleaned up temp directory")
    }

    super.afterAll()
  }

  test("EXACT REPRO: MERGE SPLITS basic functionality validation (test 647)") {
    val tablePath = s"file://${tempDir.getAbsolutePath}/merge-validation-test"

    println(s"")
    println(s"🔍 ========================================")
    println(s"🔍 EXACT REPRODUCTION OF FAILING TEST 647")
    println(s"🔍 Real S3: MERGE SPLITS basic functionality validation")
    println(s"🔍 ========================================")
    println(s"📁 Table path: $tablePath")

    // EXACT SAME DATA AS TEST 647 (lines 653-662)
    val data = spark
      .range(2000)
      .select(
        col("id"),
        concat(
          lit("This is a comprehensive content string for document "),
          col("id"),
          lit(". It contains substantial text to ensure splits are large enough. "),
          lit("Additional content to reach meaningful split sizes for merge operations. "),
          lit("More text content to create realistic split file sizes.")
        ).as("content"),
        (col("id") % 50).cast("string").as("category")
      )

    println(s"✍️  Writing substantial test data for MERGE SPLITS validation...")

    // EXACT SAME OPTIONS AS TEST 647 (lines 666-668)
    val writeOptions = Map(
      "spark.indextables.indexwriter.batchSize" -> "100" // Force multiple splits
    )

    println(s"")
    println(s"🔍 FIRST WRITE: mode=overwrite, filter: id < 1000")
    // EXACT SAME WRITES AS TEST 647 (lines 671-675)
    data
      .filter(col("id") < 1000)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ First write (overwrite) completed")
    println(s"")

    println(s"🔍 SECOND WRITE: mode=append, filter: id >= 1000")
    // EXACT SAME SECOND WRITE AS TEST 647 (lines 677-681)
    data
      .filter(col("id") >= 1000)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Second write (append) completed")
    println(s"✅ Successfully wrote multi-phase data to create multiple splits")
    println(s"")

    // EXACT SAME PRE-MERGE VERIFICATION AS TEST 647 (lines 685-695)
    println(s"🔍 Pre-merge data verification...")
    val preMergeData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val preMergeCount = preMergeData.count()
    preMergeCount shouldBe 2000

    println(s"✅ Pre-merge verification: $preMergeCount records")
    println(s"")

    // EXACT SAME MERGE OPERATION AS TEST 647 (lines 697-703)
    println(s"🔧 Executing MERGE SPLITS operation...")
    import _root_.io.indextables.spark.sql.IndexTables4SparkSqlParser
    val sqlParser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeCommand = sqlParser
      .parsePlan(s"MERGE SPLITS '$tablePath' TARGET SIZE 2097152")
      .asInstanceOf[_root_.io.indextables.spark.sql.MergeSplitsCommand]
    mergeCommand.run(spark)

    println(s"✅ MERGE SPLITS operation completed successfully")
    println(s"")

    // EXACT SAME POST-MERGE VERIFICATION AS TEST 647 (lines 707-723)
    println(s"🔍 Post-merge data verification...")
    val postMergeData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)

    val postMergeCount = postMergeData.count()
    postMergeCount shouldBe 2000

    // Verify data content integrity
    val categoryCheck = postMergeData.filter(col("category") === "25").count()
    categoryCheck should be > 0L

    println(s"✅ Post-merge verification: $postMergeCount records")
    println(s"✅ Data integrity preserved (category 25 records: $categoryCheck)")
    println(s"")
    println(s"🎉 MERGE SPLITS basic functionality validation successful")
    println(s"")
  }

  test("EXACT REPRO WITH S3: MERGE SPLITS basic functionality validation (test 647)") {
    assume(awsCredentials.isDefined, "AWS credentials required for S3 test")

    val (accessKey, secretKey) = awsCredentials.get
    val tablePath              = s"$s3TestPath/merge-validation-test"

    println(s"")
    println(s"🔍 ========================================")
    println(s"🔍 EXACT REPRODUCTION WITH REAL S3")
    println(s"🔍 Real S3: MERGE SPLITS basic functionality validation")
    println(s"🔍 ========================================")
    println(s"📁 S3 table path: $tablePath")

    // EXACT SAME DATA AS TEST 647
    val data = spark
      .range(2000)
      .select(
        col("id"),
        concat(
          lit("This is a comprehensive content string for document "),
          col("id"),
          lit(". It contains substantial text to ensure splits are large enough. "),
          lit("Additional content to reach meaningful split sizes for merge operations. "),
          lit("More text content to create realistic split file sizes.")
        ).as("content"),
        (col("id") % 50).cast("string").as("category")
      )

    println(s"✍️  Writing substantial test data for MERGE SPLITS validation...")

    // EXACT SAME OPTIONS AS TEST 647 + AWS credentials
    val writeOptions = Map(
      "spark.indextables.indexwriter.batchSize" -> "100", // Force multiple splits
      "spark.indextables.aws.accessKey"         -> accessKey,
      "spark.indextables.aws.secretKey"         -> secretKey,
      "spark.indextables.aws.region"            -> S3_REGION
    )

    println(s"")
    println(s"🔍 FIRST WRITE (S3): mode=overwrite, filter: id < 1000")
    data
      .filter(col("id") < 1000)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("overwrite")
      .save(tablePath)

    println(s"✅ First write (overwrite) completed")
    println(s"")

    println(s"🔍 SECOND WRITE (S3): mode=append, filter: id >= 1000")
    data
      .filter(col("id") >= 1000)
      .write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(writeOptions)
      .mode("append")
      .save(tablePath)

    println(s"✅ Second write (append) completed")
    println(s"✅ Successfully wrote multi-phase data to S3")
    println(s"")

    // Verify data exists before merge
    val readOptions = Map(
      "spark.indextables.aws.accessKey" -> accessKey,
      "spark.indextables.aws.secretKey" -> secretKey,
      "spark.indextables.aws.region"    -> S3_REGION
    )

    println(s"🔍 Pre-merge data verification...")
    val preMergeData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val preMergeCount = preMergeData.count()
    preMergeCount shouldBe 2000

    println(s"✅ Pre-merge verification: $preMergeCount records")
    println(s"")

    // Execute MERGE SPLITS command
    println(s"🔧 Executing MERGE SPLITS operation on S3...")
    import _root_.io.indextables.spark.sql.IndexTables4SparkSqlParser
    val sqlParser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)
    val mergeCommand = sqlParser
      .parsePlan(s"MERGE SPLITS '$tablePath' TARGET SIZE 2097152")
      .asInstanceOf[_root_.io.indextables.spark.sql.MergeSplitsCommand]
    mergeCommand.run(spark)

    println(s"✅ MERGE SPLITS operation completed successfully")
    println(s"")

    // Verify data integrity after merge
    println(s"🔍 Post-merge data verification...")
    val postMergeData = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .options(readOptions)
      .load(tablePath)

    val postMergeCount = postMergeData.count()
    postMergeCount shouldBe 2000

    // Verify data content integrity
    val categoryCheck = postMergeData.filter(col("category") === "25").count()
    categoryCheck should be > 0L

    println(s"✅ Post-merge verification: $postMergeCount records")
    println(s"✅ Data integrity preserved (category 25 records: $categoryCheck)")
    println(s"")
    println(s"🎉 S3 MERGE SPLITS basic functionality validation successful")
    println(s"")
  }
}
