import java.io.{File, FileInputStream}
import java.util.Properties

import scala.util.Using

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LogProcessorTest {

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
      .appName("SparkLogProcessor")
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
      val dataDir    = "data"
      val S3_BUCKET  = "test-tantivy4sparkbucket"
      val outputPath = s"s3a://$S3_BUCKET/biglog_test/table"

      println(s"🚀 Starting log processing...")
      println(s"📁 Reading log files from: ${new File(dataDir).getAbsolutePath}")
      println(s"💾 Output table path: $outputPath")
      println(s"🌐 Using S3 bucket: $S3_BUCKET in region $S3_REGION")

      // Read all log files but limit to ~2GB of data
      val logFiles = findLogFiles(dataDir)
      println(s"📋 Found ${logFiles.length} log files")

      // Estimate file sizes and limit to ~2GB
      val targetBytes  = 2L * 1024 * 1024 * 1024 // 2GB
      val limitedFiles = selectFilesUpToSize(logFiles, targetBytes)

      println(s"📊 Limited to ${limitedFiles.length} files (targeting ~2GB of data)")

      // Process logs in batches to avoid memory issues
      val batchSize    = 100 // Smaller batch size for better memory management
      val totalBatches = (limitedFiles.length + batchSize - 1) / batchSize

      println(s"⚙️  Processing logs in $totalBatches batches of $batchSize files each")

      var allLogsDf: DataFrame = null

      for (batchIndex <- limitedFiles.indices.grouped(batchSize).zipWithIndex) {
        val (batch, batchNum) = batchIndex
        val currentBatch      = limitedFiles.slice(batch.head, batch.last + 1)

        println(s"🔄 Processing batch ${batchNum + 1}/$totalBatches (${currentBatch.length} files)")

        val batchDf    = processLogBatch(spark, currentBatch)
        val batchCount = batchDf.count()
        println(s"   📊 Batch ${batchNum + 1} contains $batchCount log entries")

        if (allLogsDf == null) {
          allLogsDf = batchDf
        } else {
          allLogsDf = allLogsDf.union(batchDf)
        }
      }

      // Show sample data
      println("\n📋 Sample log entries:")
      allLogsDf.show(10, truncate = false)

      // Get total count
      val totalRecords = allLogsDf.count()
      println(s"\n📈 Total log entries to write: $totalRecords")

      // Write to IndexTables4Spark using V2 DataSource with single partition
      println(s"💿 Writing to IndexTables4Spark table at $outputPath...")

      allLogsDf
        .repartition(1) // Single partition as requested
        .write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider") // V2 DataSource
        .option("spark.indextables.indexing.typemap.message", "text")       // Make message field searchable
        .option("spark.indextables.indexing.typemap.logger", "string")      // Exact matching for logger
        .option("spark.indextables.indexing.typemap.level", "string")       // Exact matching for level
        .option("spark.indextables.indexWriter.heapSize", "8G")             // 8GB heap size using human-readable format
        .mode("overwrite")
        .save(outputPath)

      println("✅ Successfully written log data to IndexTables4Spark table!")

      // Verify the write
      println("\n🔍 Verifying written data...")
      val readBack = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(outputPath)

      val verifyCount = readBack.count()
      println(s"✅ Verification: Read back $verifyCount records")

      println("\n📊 Sample of written data:")
      readBack.show(5, truncate = false)

      // Show distribution by log level
      println("\n📈 Log level distribution:")
      readBack.groupBy("level").count().orderBy(desc("count")).show()

      // Test some basic searches
      println("\n🔍 Testing basic searches:")

      println("🔎 Searching for 'ERROR' level logs:")
      readBack.filter(col("level") === "ERROR").show(3, false)

      println("🔎 Searching for logs containing 'executor':")
      readBack.filter(col("message").contains("executor")).show(3, false)

      println("\n🎉 Log processing completed successfully!")

    } catch {
      case e: Exception =>
        println(s"❌ Error during processing: ${e.getMessage}")
        e.printStackTrace()
    } finally
      spark.stop()
  }

  def findLogFiles(dataDir: String): Array[String] = {
    import scala.collection.mutable.ArrayBuffer
    val logFiles = ArrayBuffer[String]()

    def scanDirectory(dir: File): Unit =
      if (dir.exists() && dir.isDirectory) {
        dir.listFiles().foreach { file =>
          if (file.isDirectory) {
            scanDirectory(file)
          } else if (file.getName.endsWith(".log")) {
            logFiles += file.getAbsolutePath
          }
        }
      }

    scanDirectory(new File(dataDir))
    logFiles.toArray
  }

  def selectFilesUpToSize(files: Array[String], maxBytes: Long): Array[String] = {
    import scala.collection.mutable.ArrayBuffer
    val selectedFiles = ArrayBuffer[String]()
    var totalSize     = 0L

    println(s"🎯 Selecting files up to ${maxBytes / (1024 * 1024)} MB...")

    for (filePath <- files) {
      val file = new File(filePath)
      if (file.exists()) {
        val fileSize = file.length()
        if (totalSize + fileSize <= maxBytes) {
          selectedFiles += filePath
          totalSize += fileSize
          if (selectedFiles.length % 500 == 0) {
            println(s"   📄 Selected ${selectedFiles.length} files, total size: ${totalSize / (1024 * 1024)} MB")
          }
        } else {
          println(s"✅ Reached target size: ${totalSize / (1024 * 1024)} MB with ${selectedFiles.length} files")
          return selectedFiles.toArray
        }
      }
    }

    println(s"✅ Selected all ${selectedFiles.length} files, total size: ${totalSize / (1024 * 1024)} MB")
    selectedFiles.toArray
  }

  def processLogBatch(spark: SparkSession, logFiles: Array[String]): DataFrame = {
    import spark.implicits._

    // Read all files as text
    val rawLines = spark.read.textFile(logFiles: _*)

    // Filter out SLF4J warnings and empty lines, then parse log entries
    val parsedLogs = rawLines
      .filter(line => !line.startsWith("SLF4J:") && line.trim.nonEmpty)
      .map(parseLogLine)
      .filter(_ != null) // Filter out unparseable lines

    // Convert to DataFrame with proper schema
    parsedLogs.toDF()
  }

  case class LogEntry(
    date: String,
    time: String,
    level: String,
    logger: String,
    message: String)

  def parseLogLine(line: String): LogEntry =
    try {
      // Example: "15/09/01 18:14:50 INFO executor.CoarseGrainedExecutorBackend: Registered signal handlers for [TERM, HUP, INT]"
      val logPattern = """(\d{2}/\d{2}/\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(\w+)\s+([^:]+):\s*(.*)""".r

      line match {
        case logPattern(date, time, level, logger, message) =>
          LogEntry(date, time, level, logger.trim, message.trim)
        case _ =>
          // For lines that don't match the pattern, treat as a continuation of previous message
          LogEntry("unknown", "unknown", "INFO", "unknown", line.trim)
      }
    } catch {
      case _: Exception =>
        // If parsing fails, create a generic log entry
        LogEntry("unknown", "unknown", "INFO", "unknown", line.trim)
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
