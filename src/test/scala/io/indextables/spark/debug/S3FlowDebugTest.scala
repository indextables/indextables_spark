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

package io.indextables.spark.debug

import java.net.{ServerSocket, URI}

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.findify.s3mock.S3Mock
import io.indextables.spark.io.{CloudStorageProviderFactory, S3CloudStorageProvider}
import io.indextables.spark.TestBase
import org.scalatest.BeforeAndAfterAll
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.S3Client

/**
 * Isolated tests to debug S3 flow issues. These tests isolate each component to identify the root cause of S3 operation
 * failures.
 */
class S3FlowDebugTest extends TestBase with BeforeAndAfterAll {

  private val TEST_BUCKET     = "test-debug-bucket"
  private val ACCESS_KEY      = "debug-access-key"
  private val SECRET_KEY      = "debug-secret-key"
  private val SESSION_TOKEN   = "debug-session-token"
  private var s3MockPort: Int = _
  private var s3Mock: S3Mock  = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Find available port
    s3MockPort = findAvailablePort()

    // Start S3Mock server
    s3Mock = S3Mock(port = s3MockPort, dir = "/tmp/s3debug")
    s3Mock.start

    println(s"🔧 S3Mock server started on port $s3MockPort")
  }

  override def afterAll(): Unit = {
    if (s3Mock != null) {
      s3Mock.stop
    }
    super.afterAll()
  }

  private def findAvailablePort(): Int =
    Using.resource(new ServerSocket(0))(socket => socket.getLocalPort)

  test("S3Mock server basic connectivity") {
    // Test 1: Can we connect to S3Mock at all?
    val s3Client = S3Client
      .builder()
      .endpointOverride(URI.create(s"http://localhost:$s3MockPort"))
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
        )
      )
      .region(Region.US_EAST_1)
      .forcePathStyle(true)
      .build()

    try {
      // Try to create a bucket
      s3Client.createBucket(
        CreateBucketRequest
          .builder()
          .bucket(TEST_BUCKET)
          .build()
      )
      println(s"✅ Successfully created bucket: $TEST_BUCKET")

      // Try to list buckets
      val buckets = s3Client.listBuckets().buckets()
      println(s"✅ Found ${buckets.size()} buckets")
      buckets.asScala.foreach(bucket => println(s"  - ${bucket.name()}"))

    } catch {
      case ex: Exception =>
        println(s"❌ S3Mock connectivity test failed: ${ex.getMessage}")
        fail(s"S3Mock server not accessible: ${ex.getMessage}")
    } finally
      s3Client.close()
  }

  test("CloudStorageProviderFactory with S3Mock") {
    // Test 2: Can CloudStorageProviderFactory create an S3 provider correctly?
    val options = Map(
      "spark.indextables.aws.accessKey"      -> ACCESS_KEY,
      "spark.indextables.aws.secretKey"      -> SECRET_KEY,
      "spark.indextables.aws.sessionToken"   -> SESSION_TOKEN,
      "spark.indextables.aws.region"         -> "us-east-1",
      "spark.indextables.s3.endpoint"        -> s"http://localhost:$s3MockPort",
      "spark.indextables.s3.pathStyleAccess" -> "true"
    ).asJava

    val caseInsensitiveOptions = new CaseInsensitiveStringMap(options)
    val hadoopConf             = spark.sparkContext.hadoopConfiguration

    try {
      val provider = CloudStorageProviderFactory.createProvider(
        s"s3://$TEST_BUCKET/test-path",
        caseInsensitiveOptions,
        hadoopConf
      )

      provider shouldBe a[S3CloudStorageProvider]
      println(s"✅ Successfully created S3CloudStorageProvider")

      provider.close()

    } catch {
      case ex: Exception =>
        println(s"❌ CloudStorageProviderFactory test failed: ${ex.getMessage}")
        ex.printStackTrace()
        fail(s"Failed to create S3CloudStorageProvider: ${ex.getMessage}")
    }
  }

  ignore("S3CloudStorageProvider basic operations") {
    // Test 3: Can S3CloudStorageProvider perform basic file operations?

    // First create the bucket using AWS SDK
    val s3Client = S3Client
      .builder()
      .endpointOverride(URI.create(s"http://localhost:$s3MockPort"))
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
        )
      )
      .region(Region.US_EAST_1)
      .forcePathStyle(true)
      .build()

    try {
      s3Client.createBucket(
        CreateBucketRequest
          .builder()
          .bucket(TEST_BUCKET)
          .build()
      )
      println(s"✅ Created bucket for provider test: $TEST_BUCKET")
    } finally
      s3Client.close()

    val options = Map(
      "spark.indextables.aws.accessKey"      -> ACCESS_KEY,
      "spark.indextables.aws.secretKey"      -> SECRET_KEY,
      "spark.indextables.aws.sessionToken"   -> SESSION_TOKEN,
      "spark.indextables.aws.region"         -> "us-east-1",
      "spark.indextables.s3.endpoint"        -> s"http://localhost:$s3MockPort",
      "spark.indextables.s3.pathStyleAccess" -> "true"
    ).asJava

    val caseInsensitiveOptions = new CaseInsensitiveStringMap(options)
    val hadoopConf             = spark.sparkContext.hadoopConfiguration

    val provider = CloudStorageProviderFactory.createProvider(
      s"s3://$TEST_BUCKET/test-path",
      caseInsensitiveOptions,
      hadoopConf
    )

    try {
      // Test basic file write
      val testPath    = s"s3://$TEST_BUCKET/test-file.txt"
      val testContent = "Hello, S3Mock!".getBytes("UTF-8")

      println(s"🔧 Attempting to write to: $testPath")
      provider.writeFile(testPath, testContent)
      println(s"✅ Successfully wrote file: $testPath")

      // Test file exists
      val exists = provider.exists(testPath)
      exists shouldBe true
      println(s"✅ File exists check passed: $testPath")

      // Test file read
      val readContent = provider.readFile(testPath)
      readContent should equal(testContent)
      println(s"✅ Successfully read file: $testPath")

    } catch {
      case ex: Exception =>
        println(s"❌ S3CloudStorageProvider operations failed: ${ex.getMessage}")
        ex.printStackTrace()
        fail(s"S3CloudStorageProvider operations failed: ${ex.getMessage}")
    } finally
      provider.close()
  }

  ignore("S3CloudStorageProvider directory operations") {
    // Test 4: Can S3CloudStorageProvider handle directory operations?

    // Create bucket first
    val s3Client = S3Client
      .builder()
      .endpointOverride(URI.create(s"http://localhost:$s3MockPort"))
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
        )
      )
      .region(Region.US_EAST_1)
      .forcePathStyle(true)
      .build()

    try {
      s3Client.createBucket(
        CreateBucketRequest
          .builder()
          .bucket(TEST_BUCKET)
          .build()
      )
      println(s"✅ Created bucket for directory test: $TEST_BUCKET")
    } finally
      s3Client.close()

    val options = Map(
      "spark.indextables.aws.accessKey"      -> ACCESS_KEY,
      "spark.indextables.aws.secretKey"      -> SECRET_KEY,
      "spark.indextables.aws.sessionToken"   -> SESSION_TOKEN,
      "spark.indextables.aws.region"         -> "us-east-1",
      "spark.indextables.s3.endpoint"        -> s"http://localhost:$s3MockPort",
      "spark.indextables.s3.pathStyleAccess" -> "true"
    ).asJava

    val caseInsensitiveOptions = new CaseInsensitiveStringMap(options)
    val hadoopConf             = spark.sparkContext.hadoopConfiguration

    val provider = CloudStorageProviderFactory.createProvider(
      s"s3://$TEST_BUCKET/test-dir",
      caseInsensitiveOptions,
      hadoopConf
    )

    try {
      // Test directory creation (this is what TransactionLog does)
      val dirPath = s"s3://$TEST_BUCKET/test-table/_transaction_log"

      println(s"🔧 Attempting to create directory: $dirPath")
      val dirCreated = provider.createDirectory(dirPath)
      println(s"✅ Directory creation result: $dirCreated")

      // Test directory exists
      val dirExists = provider.exists(dirPath)
      println(s"✅ Directory exists check: $dirExists")

      // Test writing a file in the directory (this is what TransactionLog.writeActions does)
      val filePath    = s"$dirPath/00000000000000000000.json"
      val fileContent = """{"metaData": {"id": "test-id", "format": {"provider": "tantivy4spark"}}}""".getBytes("UTF-8")

      println(s"🔧 Attempting to write transaction log file: $filePath")
      try {
        provider.writeFile(filePath, fileContent)
        println(s"✅ Successfully wrote transaction log file: $filePath")
      } catch {
        case ex: Exception =>
          println(s"❌ Failed to write nested file, trying shallow path...")
          // Try a shallower path to see if S3Mock has issues with deep nesting
          val shallowPath = s"s3://$TEST_BUCKET/shallow.json"
          provider.writeFile(shallowPath, fileContent)
          println(s"✅ Shallow file write succeeded: $shallowPath")

          // Now try a single-level nested path
          val singleNestedPath = s"s3://$TEST_BUCKET/single/nested.json"
          provider.writeFile(singleNestedPath, fileContent)
          println(s"✅ Single nested file write succeeded: $singleNestedPath")

          throw new Exception(
            s"S3Mock appears to have issues with deep nested paths like: $filePath. Original error: ${ex.getMessage}"
          )
      }

      // Test reading the file back
      val readContent = provider.readFile(filePath)
      readContent should equal(fileContent)
      println(s"✅ Successfully read transaction log file: $filePath")

    } catch {
      case ex: Exception =>
        println(s"❌ S3CloudStorageProvider directory operations failed: ${ex.getMessage}")
        ex.printStackTrace()
        fail(s"Directory operations failed: ${ex.getMessage}")
    } finally
      provider.close()
  }

  ignore("TransactionLog with S3Mock integration") {
    // Test 5: Can TransactionLog work with S3Mock?
    import io.indextables.spark.transaction.TransactionLogFactory
    import org.apache.hadoop.fs.Path
    import org.apache.spark.sql.types._

    // Create bucket first
    val s3Client = S3Client
      .builder()
      .endpointOverride(URI.create(s"http://localhost:$s3MockPort"))
      .credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
        )
      )
      .region(Region.US_EAST_1)
      .forcePathStyle(true)
      .build()

    try {
      s3Client.createBucket(
        CreateBucketRequest
          .builder()
          .bucket(TEST_BUCKET)
          .build()
      )
      println(s"✅ Created bucket for TransactionLog test: $TEST_BUCKET")
    } finally
      s3Client.close()

    // Configure Spark to use our S3Mock
    spark.conf.set("spark.indextables.aws.accessKey", ACCESS_KEY)
    spark.conf.set("spark.indextables.aws.secretKey", SECRET_KEY)
    spark.conf.set("spark.indextables.aws.sessionToken", SESSION_TOKEN)
    spark.conf.set("spark.indextables.aws.region", "us-east-1")
    spark.conf.set("spark.indextables.s3.endpoint", s"http://localhost:$s3MockPort")
    spark.conf.set("spark.indextables.s3.pathStyleAccess", "true")

    // Also set in Hadoop config for consistency
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("spark.indextables.aws.accessKey", ACCESS_KEY)
    hadoopConf.set("spark.indextables.aws.secretKey", SECRET_KEY)
    hadoopConf.set("spark.indextables.aws.sessionToken", SESSION_TOKEN)
    hadoopConf.set("spark.indextables.aws.region", "us-east-1")
    hadoopConf.set("spark.indextables.s3.endpoint", s"http://localhost:$s3MockPort")
    hadoopConf.set("spark.indextables.s3.pathStyleAccess", "true")

    val options = Map(
      "spark.indextables.aws.accessKey"      -> ACCESS_KEY,
      "spark.indextables.aws.secretKey"      -> SECRET_KEY,
      "spark.indextables.aws.sessionToken"   -> SESSION_TOKEN,
      "spark.indextables.aws.region"         -> "us-east-1",
      "spark.indextables.s3.endpoint"        -> s"http://localhost:$s3MockPort",
      "spark.indextables.s3.pathStyleAccess" -> "true"
    ).asJava

    val tablePath              = new Path(s"s3://$TEST_BUCKET/test-table")
    val caseInsensitiveOptions = new CaseInsensitiveStringMap(options)
    val transactionLog         = TransactionLogFactory.create(tablePath, spark, caseInsensitiveOptions)

    try {
      // Test schema initialization
      val testSchema = StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("name", StringType, nullable = false),
          StructField("description", StringType, nullable = true)
        )
      )

      println(s"🔧 Attempting to initialize TransactionLog with schema")
      transactionLog.initialize(testSchema)
      println(s"✅ Successfully initialized TransactionLog")

      // Test schema retrieval
      val retrievedSchema = transactionLog.getSchema()
      retrievedSchema shouldBe defined
      retrievedSchema.get shouldBe testSchema
      println(s"✅ Successfully retrieved schema from TransactionLog")

    } catch {
      case ex: Exception =>
        println(s"❌ TransactionLog integration failed: ${ex.getMessage}")
        ex.printStackTrace()
        fail(s"TransactionLog with S3Mock failed: ${ex.getMessage}")
    } finally
      transactionLog.close()
  }
}
