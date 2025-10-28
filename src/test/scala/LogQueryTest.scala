import java.io.{File, FileInputStream}
import java.util.Properties

import scala.util.Using

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object LogQueryTest {

  def main(args: Array[String]): Unit = {
    // Load AWS credentials from ~/.aws/credentials
    val awsCredentials = loadAwsCredentials()

    if (awsCredentials.isEmpty) {
      println("❌ No AWS credentials found in ~/.aws/credentials - cannot proceed")
      System.exit(1)
    }

    val (accessKey, secretKey) = awsCredentials.get
    val S3_REGION              = "us-east-2"

    // Create Spark session with IndexTables4Spark extensions
    val spark = SparkSession
      .builder()
      .appName("LogQueryTest")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")
      .config("spark.sql.adaptive.enabled", "false") // Disable AQE for consistent partitioning
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Configure AWS credentials using the real S3 test pattern
    spark.conf.set("spark.indextables.aws.accessKey", accessKey)
    spark.conf.set("spark.indextables.aws.secretKey", secretKey)
    spark.conf.set("spark.indextables.aws.region", S3_REGION)

    // Also configure Hadoop config so CloudStorageProvider can find the region
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("spark.indextables.aws.accessKey", accessKey)
    hadoopConf.set("spark.indextables.aws.secretKey", secretKey)
    hadoopConf.set("spark.indextables.aws.region", S3_REGION)

    println(s"🔐 AWS credentials configured successfully")
    println(s"🌊 Configured Spark for S3 access in region: $S3_REGION")

    try {
      val S3_BUCKET  = "test-tantivy4sparkbucket"
      val outputPath = s"s3a://$S3_BUCKET/biglog_test/table"

      println(s"🔍 Loading IndexTables4Spark table from: $outputPath")

      // Load the dataset
      val logData = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(outputPath)

      // Cache the dataset to memory for consistent query performance
      // logData.cache()

      // Show basic info about the dataset
      println(s"📊 Dataset loaded successfully!")
      // println(s"📈 Total record count: ${logData.count()}")

      // Show schema
      println("\n📋 Schema:")
      logData.printSchema()

      // Show sample data
      println("\n📋 Sample data:")
      logData.show(5, truncate = false)

      // Run the same query 3 times to test performance consistency
      val testQuery = "ERROR OR Exception OR Failed"

      println(s"\n🔍 Running IndexQuery test: '$testQuery' (3 iterations)")

      for (i <- 1 to 3) {
        println(s"\n🚀 Query iteration $i:")
        val startTime = System.currentTimeMillis()

        val results = logData.filter(col("message").contains("FAKEERROR"))
        val count   = results.count()

        val endTime  = System.currentTimeMillis()
        val duration = endTime - startTime

        println(s"   📊 Found $count records matching 'ERROR'")
        println(s"   ⏱️  Query completed in ${duration}ms")

        // Show a few sample results
        if (count > 0) {
          println(s"   📋 Sample results:")
          results
            .select("date", "time", "level", "logger", "message")
            .show(3, truncate = false)
        }
      }

      // Test a more complex query
      println(s"\n🔍 Running noresult query test:")
      val startTime = System.currentTimeMillis()

      val emptyResults = logData
        .filter(col("level") === "FAKEERROR")

      emptyResults.show()

      val endTime  = System.currentTimeMillis()
      val duration = endTime - startTime
      println(s"   ⏱️  Complex query completed in ${duration}ms")

      // Test log level distribution
      println(s"\n📈 Log level distribution:")
      // logData.groupBy("level").count().orderBy(desc("count")).show()

      // Test logger distribution (top 10)
      println(s"\n📈 Top 10 loggers:")
      // logData.groupBy("logger").count().orderBy(desc("count")).limit(10).show(truncate = false)

      // Test date distribution
      println(s"\n📈 Date distribution:")
      // logData.groupBy("date").count().orderBy("date").show()

      println("\n✅ All query tests completed successfully!")

    } catch {
      case e: Exception =>
        println(s"❌ Error during query testing: ${e.getMessage}")
        e.printStackTrace()
    } finally
      spark.stop()
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
          println(s"⚠️  AWS credentials not found in ~/.aws/credentials")
          None
        }
      } else {
        println(s"⚠️  ~/.aws/credentials file not found")
        None
      }
    } catch {
      case e: Exception =>
        println(s"⚠️  Error loading AWS credentials: ${e.getMessage}")
        None
    }
}
