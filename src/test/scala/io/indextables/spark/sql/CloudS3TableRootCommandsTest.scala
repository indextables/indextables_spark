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

package io.indextables.spark.sql

import java.io.File
import java.util.{Properties, UUID}

import scala.util.Using

import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.Row

import org.apache.hadoop.fs.{FileSystem, Path}

import io.indextables.spark.transaction.TransactionLogFactory
import io.indextables.spark.CloudS3TestBase

/**
 * Real AWS S3 integration tests for multi-region table root commands.
 *
 * Tests SET/UNSET/DESCRIBE TABLE ROOTS and read-time designator resolution against real S3 storage.
 *
 * Credentials are loaded from ~/.aws/credentials file.
 */
class CloudS3TableRootCommandsTest extends CloudS3TestBase {

  private val S3_BUCKET    = "test-tantivy4sparkbucket"
  private val S3_REGION    = "us-east-2"
  private val S3_BASE_PATH = s"s3a://$S3_BUCKET"

  // Generate unique test run ID to avoid conflicts
  private val testRunId    = UUID.randomUUID().toString.substring(0, 8)
  private val testBasePath = s"$S3_BASE_PATH/table-root-test-$testRunId"

  // AWS credentials loaded from ~/.aws/credentials
  private var awsCredentials: Option[(String, String)] = None
  private var fs: FileSystem                           = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load AWS credentials from ~/.aws/credentials
    awsCredentials = loadAwsCredentials()

    if (awsCredentials.isDefined) {
      val (accessKey, secretKey) = awsCredentials.get

      // Configure Hadoop config
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("fs.s3a.access.key", accessKey)
      hadoopConf.set("fs.s3a.secret.key", secretKey)
      hadoopConf.set("fs.s3a.endpoint.region", S3_REGION)
      hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
      hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
      hadoopConf.set("spark.indextables.aws.region", S3_REGION)

      // Configure Spark for real S3 access
      spark.conf.set("spark.indextables.aws.accessKey", accessKey)
      spark.conf.set("spark.indextables.aws.secretKey", secretKey)
      spark.conf.set("spark.indextables.aws.region", S3_REGION)

      // Initialize FileSystem
      val testPath = new Path(testBasePath)
      fs = testPath.getFileSystem(hadoopConf)

      // Initialize SplitConversionThrottle for tests
      _root_.io.indextables.spark.storage.SplitConversionThrottle.initialize(
        maxParallelism = Runtime.getRuntime.availableProcessors() max 1
      )

      println(s"Test base path: $testBasePath")
    } else {
      println(s"No AWS credentials found in ~/.aws/credentials - tests will be skipped")
    }
  }

  override def afterAll(): Unit = {
    if (awsCredentials.isDefined && fs != null) {
      try {
        val basePath = new Path(testBasePath)
        if (fs.exists(basePath)) {
          fs.delete(basePath, true)
          println(s"Cleaned up test data at $testBasePath")
        }
      } catch {
        case ex: Exception =>
          println(s"Failed to clean up test data: ${ex.getMessage}")
      }
    }
    super.afterAll()
  }

  /** Load AWS credentials from ~/.aws/credentials file. */
  private def loadAwsCredentials(): Option[(String, String)] =
    try {
      val home     = System.getProperty("user.home")
      val credFile = new File(s"$home/.aws/credentials")

      if (credFile.exists()) {
        val props = new Properties()
        Using(new java.io.FileInputStream(credFile))(fis => props.load(fis))

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

  /** Create a test IndexTables table and write sample data to S3. */
  private def createTestTable(tablePath: String): Unit = {
    val (accessKey, secretKey) = awsCredentials.get
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("value", StringType, nullable = false)
      )
    )
    val data = Seq(Row(1, "one"), Row(2, "two"), Row(3, "three"))
    val df   = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.write
      .format(io.indextables.spark.TestBase.INDEXTABLES_FORMAT)
      .option("spark.indextables.aws.accessKey", accessKey)
      .option("spark.indextables.aws.secretKey", secretKey)
      .option("spark.indextables.aws.region", S3_REGION)
      .mode("overwrite")
      .save(tablePath)
  }

  /** Enable companion mode on an existing IndexTables table. */
  private def enableCompanionMode(tablePath: String): Unit = {
    val optionsMap = new java.util.HashMap[String, String]()
    spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach { case (k, v) => optionsMap.put(k, v) }
    optionsMap.put("spark.indextables.databricks.credential.operation", "PATH_READ_WRITE")
    val options = new CaseInsensitiveStringMap(optionsMap)

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    try {
      val metadata = transactionLog.getMetadata()
      val updatedConfig = metadata.configuration +
        ("indextables.companion.enabled"         -> "true") +
        ("indextables.companion.sourceTablePath" -> s"$testBasePath/source") +
        ("indextables.companion.sourceFormat"    -> "delta")
      val updatedMetadata = metadata.copy(configuration = updatedConfig)
      transactionLog.commitSyncActions(Seq.empty, Seq.empty, Some(updatedMetadata))
      transactionLog.invalidateCache()
    } finally
      transactionLog.close()
  }

  /** Read metadata configuration from a table. */
  private def readMetadataConfig(tablePath: String): Map[String, String] = {
    val optionsMap = new java.util.HashMap[String, String]()
    spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach { case (k, v) => optionsMap.put(k, v) }
    val options = new CaseInsensitiveStringMap(optionsMap)

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    try {
      transactionLog.invalidateCache()
      transactionLog.getMetadata().configuration
    } finally
      transactionLog.close()
  }

  test("Real S3: SET TABLE ROOT should register a root in metadata") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/set-root-test"
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    val result = spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-west/data' AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 1
    rows.head.getString(0) should include("us-west-2")

    val config = readMetadataConfig(tablePath)
    config("indextables.companion.tableRoots.us-west-2") shouldBe "s3://bucket-west/data"
    config should contain key "indextables.companion.tableRoots.us-west-2.timestamp"
  }

  test("Real S3: UNSET TABLE ROOT should remove a root from metadata") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/unset-root-test"
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Set a root first
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-east-1' = 's3://bucket-east/data' AT '$tablePath'"
    )

    // Verify it exists
    val configBefore = readMetadataConfig(tablePath)
    configBefore should contain key "indextables.companion.tableRoots.us-east-1"

    // Unset it
    val result = spark.sql(
      s"UNSET INDEXTABLES TABLE ROOT 'us-east-1' AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 1
    rows.head.getString(0) should include("removed")

    // Verify removal
    val configAfter = readMetadataConfig(tablePath)
    configAfter should not contain key("indextables.companion.tableRoots.us-east-1")
    configAfter should not contain key("indextables.companion.tableRoots.us-east-1.timestamp")
  }

  test("Real S3: DESCRIBE TABLE ROOTS should list registered roots") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/describe-roots-test"
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Register two roots
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-west/data' AT '$tablePath'"
    )
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'eu-central-1' = 's3://bucket-eu/data' AT '$tablePath'"
    )

    val result = spark.sql(
      s"DESCRIBE INDEXTABLES TABLE ROOTS AT '$tablePath'"
    )

    val rows = result.collect()
    rows should have length 2

    result.schema.fieldNames should contain theSameElementsAs Seq("root_name", "root_path", "set_timestamp")

    // Rows are sorted by name
    rows(0).getString(0) shouldBe "eu-central-1"
    rows(0).getString(1) shouldBe "s3://bucket-eu/data"
    rows(1).getString(0) shouldBe "us-west-2"
    rows(1).getString(1) shouldBe "s3://bucket-west/data"
  }

  test("Real S3: designator should resolve registered root") {
    assume(awsCredentials.isDefined, "AWS credentials required for real S3 test")

    val tablePath = s"$testBasePath/designator-test"
    createTestTable(tablePath)
    enableCompanionMode(tablePath)

    // Register a root
    spark.sql(
      s"SET INDEXTABLES TABLE ROOT 'us-west-2' = 's3://bucket-west/data' AT '$tablePath'"
    )

    // Verify the root is stored in metadata
    val config = readMetadataConfig(tablePath)
    config("indextables.companion.tableRoots.us-west-2") shouldBe "s3://bucket-west/data"

    // Test that a missing designator produces an actionable error listing available roots
    import scala.jdk.CollectionConverters._
    val optionsMap = new java.util.HashMap[String, String]()
    spark.conf.getAll.filter(_._1.startsWith("spark.indextables.")).foreach { case (k, v) => optionsMap.put(k, v) }
    optionsMap.put("spark.indextables.companion.tableRootDesignator", "ap-southeast-1")
    val options = new CaseInsensitiveStringMap(optionsMap)

    val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark, options)
    try {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("value", StringType)
        )
      )

      val configMap = optionsMap.asScala.toMap ++ Map(
        "spark.indextables.companion.tableRootDesignator" -> "ap-southeast-1"
      )

      val scanBuilder = new io.indextables.spark.core.IndexTables4SparkScanBuilder(
        spark,
        transactionLog,
        schema,
        options,
        configMap
      )

      val exception = intercept[IllegalArgumentException] {
        scanBuilder.build()
      }
      exception.getMessage should include("ap-southeast-1")
      exception.getMessage should include("not found")
      exception.getMessage should include("us-west-2")
    } finally
      transactionLog.close()
  }
}
