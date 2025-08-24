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


package com.tantivy4spark.core

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, TableScan}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.TransactionLog
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
import org.apache.spark.broadcast.Broadcast

object Tantivy4SparkRelation {
  @transient private lazy val logger = LoggerFactory.getLogger(Tantivy4SparkRelation.getClass)
  def extractZipToDirectory(zipData: Array[Byte], targetDir: java.nio.file.Path): Unit = {
    val bais = new java.io.ByteArrayInputStream(zipData)
    val zis = new java.util.zip.ZipInputStream(bais)
    
    try {
      var entry: java.util.zip.ZipEntry = zis.getNextEntry
      while (entry != null) {
        if (!entry.isDirectory) {
          val filePath = targetDir.resolve(entry.getName)
          val fos = new java.io.FileOutputStream(filePath.toFile)
          
          try {
            val buffer = new Array[Byte](1024)
            var len = zis.read(buffer)
            while (len != -1) {
              fos.write(buffer, 0, len)
              len = zis.read(buffer)
            }
          } finally {
            fos.close()
          }
        }
        
        zis.closeEntry()
        entry = zis.getNextEntry
      }
    } finally {
      zis.close()
      bais.close()
    }
  }
  
  // Standalone function for Spark serialization - no class dependencies
  def processFile(
      filePath: String, 
      serializableSchema: StructType, 
      hadoopConfProps: Map[String, String]
  ): Iterator[org.apache.spark.sql.Row] = {
    // Create local logger for executor to avoid serialization issues
    val executorLogger = LoggerFactory.getLogger(Tantivy4SparkRelation.getClass)
    
    // Recreate Hadoop configuration in executor context
    val localHadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConfProps.foreach { case (key, value) =>
      localHadoopConf.set(key, value)
    }
    
    // DEBUG: Print all tantivy4spark configurations received in executor
    val tantivyConfigs = hadoopConfProps.filter(_._1.startsWith("spark.tantivy4spark."))
    if (executorLogger.isDebugEnabled) {
      executorLogger.debug(s"processFile received ${hadoopConfProps.size} total config properties")
      executorLogger.debug(s"processFile found ${tantivyConfigs.size} tantivy4spark configs:")
      tantivyConfigs.foreach { case (key, value) =>
        val displayValue = if (key.contains("secret") || key.contains("session")) "***" else value
        executorLogger.debug(s"   $key = $displayValue")
      }
    }
    
    // Extract cache configuration with session token support from Hadoop props
    val cacheConfig = com.tantivy4spark.storage.SplitCacheConfig(
      cacheName = hadoopConfProps.getOrElse("spark.tantivy4spark.cache.name", "tantivy4spark-cache"),
      maxCacheSize = hadoopConfProps.getOrElse("spark.tantivy4spark.cache.maxSize", "200000000").toLong,
      maxConcurrentLoads = hadoopConfProps.getOrElse("spark.tantivy4spark.cache.maxConcurrentLoads", "8").toInt,
      enableQueryCache = hadoopConfProps.getOrElse("spark.tantivy4spark.cache.queryCache", "true").toBoolean,
      // AWS configuration with session token support (handle both camelCase and lowercase keys)
      awsAccessKey = hadoopConfProps.get("spark.tantivy4spark.aws.accessKey").orElse(hadoopConfProps.get("spark.tantivy4spark.aws.accesskey")),
      awsSecretKey = hadoopConfProps.get("spark.tantivy4spark.aws.secretKey").orElse(hadoopConfProps.get("spark.tantivy4spark.aws.secretkey")),
      awsSessionToken = hadoopConfProps.get("spark.tantivy4spark.aws.sessionToken").orElse(hadoopConfProps.get("spark.tantivy4spark.aws.sessiontoken")),
      awsRegion = hadoopConfProps.get("spark.tantivy4spark.aws.region"),
      awsEndpoint = hadoopConfProps.get("spark.tantivy4spark.s3.endpoint"),
      // Azure configuration
      azureAccountName = hadoopConfProps.get("spark.tantivy4spark.azure.accountName"),
      azureAccountKey = hadoopConfProps.get("spark.tantivy4spark.azure.accountKey"),
      azureConnectionString = hadoopConfProps.get("spark.tantivy4spark.azure.connectionString"),
      azureEndpoint = hadoopConfProps.get("spark.tantivy4spark.azure.endpoint"),
      // GCP configuration
      gcpProjectId = hadoopConfProps.get("spark.tantivy4spark.gcp.projectId"),
      gcpServiceAccountKey = hadoopConfProps.get("spark.tantivy4spark.gcp.serviceAccountKey"),
      gcpCredentialsFile = hadoopConfProps.get("spark.tantivy4spark.gcp.credentialsFile"),
      gcpEndpoint = hadoopConfProps.get("spark.tantivy4spark.gcp.endpoint")
    )
    
    // Use SplitSearchEngine to read split files directly
    val rows = scala.collection.mutable.ListBuffer[org.apache.spark.sql.Row]()
    
    try {
      executorLogger.info(s"Reading Tantivy split file: $filePath")
      
      // Normalize path for tantivy4java compatibility (s3a:// -> s3://)
      val normalizedPath = if (filePath.startsWith("s3a://") || filePath.startsWith("s3n://")) {
        filePath.replaceFirst("^s3[an]://", "s3://")
      } else {
        filePath
      }
      
      // Use SplitSearchEngine to read from split with proper cache configuration
      val splitSearchEngine = com.tantivy4spark.search.SplitSearchEngine.fromSplitFile(
        serializableSchema, 
        normalizedPath,
        cacheConfig
      )
      executorLogger.debug("Split search engine created successfully")
      
      // Use searchAll to get all documents from the split
      executorLogger.debug("Starting searchAll with limit Int.MaxValue...")
      val results = splitSearchEngine.searchAll(limit = Int.MaxValue)
      executorLogger.debug(s"Search returned ${results.length} results")
      
      if (results.length == 0) {
        executorLogger.warn(s"Search returned 0 results from split file")
        if (executorLogger.isDebugEnabled) {
          executorLogger.debug(s"Schema fields: ${serializableSchema.fieldNames.mkString(", ")}")
        }
      }
      
      // Convert search results to Spark Rows with proper type conversion
      results.foreach { internalRow =>
        try {
          val row = org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala(internalRow, serializableSchema).asInstanceOf[org.apache.spark.sql.Row]
          rows += row
        } catch {
          case ex: Exception =>
            // If catalyst conversion fails, manually convert the row
            executorLogger.debug(s"Catalyst conversion failed, using manual conversion: ${ex.getMessage}")
            val values = serializableSchema.fields.zipWithIndex.map { case (field, idx) =>
              val rawValue = if (idx < internalRow.numFields && !internalRow.isNullAt(idx)) {
                field.dataType match {
                  case org.apache.spark.sql.types.LongType => internalRow.getLong(idx)
                  case org.apache.spark.sql.types.IntegerType => internalRow.getInt(idx)
                  case org.apache.spark.sql.types.DoubleType => internalRow.getDouble(idx)
                  case org.apache.spark.sql.types.BooleanType => internalRow.getBoolean(idx)
                  case org.apache.spark.sql.types.StringType => internalRow.getUTF8String(idx).toString
                  case _: org.apache.spark.sql.types.TimestampType => 
                    val longVal = try {
                      internalRow.getLong(idx)
                    } catch {
                      case _: ClassCastException => internalRow.getInt(idx).toLong
                    }
                    new java.sql.Timestamp(longVal)
                  case _: org.apache.spark.sql.types.DateType => 
                    val longVal = try {
                      internalRow.getLong(idx)
                    } catch {
                      case _: ClassCastException => internalRow.getInt(idx).toLong
                    }
                    new java.sql.Date(longVal)
                  case _ => internalRow.get(idx, field.dataType)
                }
              } else {
                null
              }
              rawValue
            }
            rows += org.apache.spark.sql.Row(values: _*)
        }
      }
      
      splitSearchEngine.close()
      executorLogger.debug(s"Converted ${rows.length} rows from search")
    } catch {
      case ex: Exception =>
        // If we can't read the Tantivy split file, log and return empty
        executorLogger.error(s"Failed to read Tantivy split file $filePath: ${ex.getMessage}")
        ex.printStackTrace()
        // Return empty iterator on error
    }
    
    rows.toIterator
  }
}

class Tantivy4SparkDataSource extends DataSourceRegister with RelationProvider with CreatableRelationProvider {
  @transient private lazy val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkDataSource])
  
  override def shortName(): String = "tantivy4spark"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
  ): BaseRelation = {
    // For reads, create a relation that can handle queries
    val path = parameters.getOrElse("path", throw new IllegalArgumentException("Path is required"))
    new Tantivy4SparkRelation(path, sqlContext, parameters)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    // For writes, delegate to the V2 TableProvider approach
    val path = parameters.getOrElse("path", throw new IllegalArgumentException("Path is required"))
    
    // For V1 DataSource API, the DataFrame write options (.option() calls) are not directly accessible
    // Instead, they should have been set in the Spark configuration or passed via parameters
    // Let's check both Spark configuration and parameters for the options
    val spark = sqlContext.sparkSession
    val sparkConf = spark.conf
    
    // Extract all tantivy4spark options from Spark configuration
    val sparkConfigOptions = try {
      sparkConf.getAll.filter(_._1.startsWith("spark.tantivy4spark.")).toMap
    } catch {
      case _: Exception => Map.empty[String, String]
    }
    
    // Combine all available options with proper precedence: write options > Spark config > defaults
    // DataFrame write options (parameters) take highest precedence over Spark session config
    val allOptions = sparkConfigOptions ++ parameters
    
    // Copy all tantivy4spark options into Hadoop configuration so they're available in executors
    val currentHadoopConf = spark.sparkContext.hadoopConfiguration
    allOptions.foreach { case (key, value) =>
      if (key.startsWith("spark.tantivy4spark.")) {
        currentHadoopConf.set(key, value)
        if (logger.isDebugEnabled) {
          logger.debug(s"Setting Hadoop config: $key = ${if (key.contains("secret") || key.contains("Secret")) "***" else value}")
        }
      }
    }
    val writeOptions = new CaseInsensitiveStringMap(allOptions.asJava)
    val tableProvider = new Tantivy4SparkTableProvider()
    
    // Get or create the table
    val table = tableProvider.getTable(data.schema, Array.empty, writeOptions)
    
    // Create write info
    val writeInfo = new LogicalWriteInfo {
      override def queryId(): String = java.util.UUID.randomUUID().toString
      override def schema(): StructType = data.schema
      override def options(): CaseInsensitiveStringMap = writeOptions
    }
    
    // Get the write builder and execute the write
    val writeBuilder = table.asInstanceOf[Tantivy4SparkTable].newWriteBuilder(writeInfo)
    val write = writeBuilder.build()
    val batchWrite = write.toBatch
    
    // Extract serializable parameters before the closure
    val serializableOptions = parameters
    val serializablePath = path
    val serializableSchema = data.schema
    
    // Pass all merged options (write options + Spark config) to executors
    // Use allOptions which already has the proper precedence: sparkConfigOptions ++ parameters
    val enrichedOptions = allOptions
    
    // Extract essential Hadoop configuration properties as a Map
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val essentialConfProps = Map(
      "fs.defaultFS" -> hadoopConf.get("fs.defaultFS", ""),
      "fs.s3a.access.key" -> hadoopConf.get("fs.s3a.access.key", ""),
      "fs.s3a.secret.key" -> hadoopConf.get("fs.s3a.secret.key", ""),
      "fs.s3a.endpoint" -> hadoopConf.get("fs.s3a.endpoint", ""),
      "fs.s3a.path.style.access" -> hadoopConf.get("fs.s3a.path.style.access", ""),
      "fs.s3a.impl" -> hadoopConf.get("fs.s3a.impl", ""),
      "fs.hdfs.impl" -> hadoopConf.get("fs.hdfs.impl", ""),
      "fs.file.impl" -> hadoopConf.get("fs.file.impl", ""),
      // Add Tantivy4Spark-specific configurations for executor distribution
      "spark.tantivy4spark.aws.accessKey" -> hadoopConf.get("spark.tantivy4spark.aws.accessKey", ""),
      "spark.tantivy4spark.aws.secretKey" -> hadoopConf.get("spark.tantivy4spark.aws.secretKey", ""),
      "spark.tantivy4spark.aws.sessionToken" -> hadoopConf.get("spark.tantivy4spark.aws.sessionToken", ""),
      "spark.tantivy4spark.aws.region" -> hadoopConf.get("spark.tantivy4spark.aws.region", ""),
      "spark.tantivy4spark.s3.endpoint" -> hadoopConf.get("spark.tantivy4spark.s3.endpoint", ""),
      "spark.tantivy4spark.s3.pathStyleAccess" -> hadoopConf.get("spark.tantivy4spark.s3.pathStyleAccess", "")
    ).filter(_._2.nonEmpty)
    
    // Debug: Log what configurations are being distributed to executors
    logger.info(s"Distributing ${essentialConfProps.size} configuration properties to executors:")
    essentialConfProps.foreach { case (key, value) =>
      val maskedValue = if (key.contains("secretKey") || key.contains("secret.key")) "***" else value
      logger.info(s"  $key = $maskedValue")
    }
    
    // Check if optimized write is enabled and apply repartitioning if needed
    val finalData = if (enrichedOptions.getOrElse("optimizeWrite", "true").toBoolean) {
      // Use Tantivy4SparkOptions for proper validation
      import scala.jdk.CollectionConverters._
      val optionsMap = new org.apache.spark.sql.util.CaseInsensitiveStringMap(enrichedOptions.asJava)
      val tantivyOptions = Tantivy4SparkOptions(optionsMap)
      val targetRecords = tantivyOptions.targetRecordsPerSplit.getOrElse(1000000L)
      
      // Estimate total records to determine optimal partitions
      val totalRecords = try {
        data.count()
      } catch {
        case _: Exception => 
          // Fallback if count fails: use current partitions * estimate
          data.rdd.getNumPartitions * 50000L // Assume 50k records per partition
      }
      
      val optimalPartitions = Math.max(1, Math.ceil(totalRecords.toDouble / targetRecords.toDouble).toInt)
      val currentPartitions = data.rdd.getNumPartitions
      
      logger.info(s"OptimizedWrite: total records ~$totalRecords, target $targetRecords per split")
      logger.info(s"OptimizedWrite: current partitions $currentPartitions, optimal partitions $optimalPartitions")
      
      // Only repartition if the optimal partitions are different from current
      // Allow any difference for proper split file count control
      if (optimalPartitions != currentPartitions) {
        logger.info(s"OptimizedWrite: repartitioning from $currentPartitions to $optimalPartitions partitions")
        if (optimalPartitions < currentPartitions) {
          // Use coalesce to reduce partitions (no shuffle)
          logger.info("OptimizedWrite: using coalesce (no shuffle) to reduce partitions")
          data.coalesce(optimalPartitions)
        } else {
          // Use repartition to increase partitions (requires shuffle) - but limit to avoid memory issues
          val safeOptimalPartitions = Math.min(optimalPartitions, 20) // Limit to 20 partitions max for safety
          logger.info(s"OptimizedWrite: using repartition (with shuffle) to increase partitions to $safeOptimalPartitions (limited from $optimalPartitions)")
          data.repartition(safeOptimalPartitions)
        }
      } else {
        logger.info("OptimizedWrite: current partitioning already optimal")
        data
      }
    } else {
      logger.info("OptimizedWrite: disabled, using original DataFrame")
      data
    }

    // Execute write using Spark's mapPartitionsWithIndex
    val commitMessages = finalData.queryExecution.toRdd.mapPartitionsWithIndex { (partitionId, iterator) =>
      // Create a local logger inside the closure to avoid serialization issues
      val executorLogger = LoggerFactory.getLogger(classOf[Tantivy4SparkDataSource])
      
      // Recreate Hadoop configuration with essential properties in the executor
      val localHadoopConf = new org.apache.hadoop.conf.Configuration()
      essentialConfProps.foreach { case (key, value) =>
        localHadoopConf.set(key, value)
      }
      
      // Also add write options to Hadoop config to ensure they override any existing values
      if (executorLogger.isDebugEnabled) {
        executorLogger.debug("Executor: Adding write options to Hadoop config")
        enrichedOptions.foreach { case (key, value) =>
          if (key.startsWith("spark.tantivy4spark.")) {
            localHadoopConf.set(key, value)
            val displayValue = if (key.contains("secret") || key.contains("Secret") || key.contains("session")) "***" else value
            executorLogger.debug(s"  Setting in executor: $key = $displayValue")
          }
        }
      } else {
        // Still need to set the config even when debug is disabled
        enrichedOptions.foreach { case (key, value) =>
          if (key.startsWith("spark.tantivy4spark.")) {
            localHadoopConf.set(key, value)
          }
        }
      }
      
      val localWriterFactory = new com.tantivy4spark.core.Tantivy4SparkWriterFactory(
        new Path(serializablePath),
        serializableSchema,
        new CaseInsensitiveStringMap(enrichedOptions.asJava),
        localHadoopConf
      )
      val writer = localWriterFactory.createWriter(partitionId, 0L)
      try {
        iterator.foreach(row => writer.write(row))
        Seq(writer.commit()).iterator
      } catch {
        case e: Exception =>
          writer.abort()
          throw e
      } finally {
        writer.close()
      }
    }.collect()
    
    // Commit all the writes
    batchWrite.commit(commitMessages)
    
    // Return a relation for reading the written data
    createRelation(sqlContext, parameters)
  }
}

class Tantivy4SparkRelation(
    path: String,
    val sqlContext: SQLContext,
    readOptions: Map[String, String] = Map.empty
) extends BaseRelation with TableScan {
  
  @transient private lazy val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkRelation])
  
  override def schema: StructType = {
    // Get schema from transaction log
    val spark = sqlContext.sparkSession
    val options = new CaseInsensitiveStringMap(java.util.Collections.emptyMap())
    val transactionLog = new TransactionLog(new Path(path), spark, options)
    transactionLog.getSchema().getOrElse {
      throw new IllegalArgumentException(s"Unable to infer schema from path: $path")
    }
  }
  
  override def buildScan(): RDD[org.apache.spark.sql.Row] = {
    val spark = sqlContext.sparkSession
    val transactionLog = new TransactionLog(new Path(path), spark)
    
    // Get list of files from transaction log
    val files = transactionLog.listFiles()
    
    if (files.isEmpty) {
      // Return empty RDD if no files
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row]
    } else {
      // Extract serializable data
      val serializableFiles = files.map(_.path)
      val serializableSchema = schema
      
      // Get Hadoop configuration from driver context - include both traditional Hadoop configs and Tantivy4Spark configs
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val baseHadoopProps = Map(
        "fs.defaultFS" -> hadoopConf.get("fs.defaultFS", ""),
        "fs.s3a.access.key" -> hadoopConf.get("fs.s3a.access.key", ""),
        "fs.s3a.secret.key" -> hadoopConf.get("fs.s3a.secret.key", ""),
        "fs.s3a.endpoint" -> hadoopConf.get("fs.s3a.endpoint", ""),
        "fs.s3a.path.style.access" -> hadoopConf.get("fs.s3a.path.style.access", ""),
        "fs.s3a.impl" -> hadoopConf.get("fs.s3a.impl", ""),
        "fs.hdfs.impl" -> hadoopConf.get("fs.hdfs.impl", ""),
        "fs.file.impl" -> hadoopConf.get("fs.file.impl", "")
      ).filter(_._2.nonEmpty)
      
      // Extract tantivy4spark configurations with proper precedence
      // Precedence: read options > Spark config > Hadoop config
      
      // Extract from Hadoop config (lowest precedence)
      val hadoopTantivyProps = {
        import scala.jdk.CollectionConverters._
        hadoopConf.iterator().asScala
          .filter(_.getKey.startsWith("spark.tantivy4spark."))
          .map(entry => entry.getKey -> entry.getValue)
          .toMap
      }
      
      // Extract from Spark session config (middle precedence)
      val sparkTantivyProps = try {
        spark.conf.getAll.filter(_._1.startsWith("spark.tantivy4spark.")).toMap
      } catch {
        case _: Exception => Map.empty[String, String]
      }
      
      // Extract from read options (highest precedence)
      val readTantivyProps = readOptions.filter(_._1.startsWith("spark.tantivy4spark."))
      
      // Merge with proper precedence: Hadoop < Spark config < read options
      val tantivyProps = hadoopTantivyProps ++ sparkTantivyProps ++ readTantivyProps
      
      val hadoopConfProps = baseHadoopProps ++ tantivyProps
      if (logger.isDebugEnabled) {
        logger.debug(s"V1 buildScan passing ${hadoopConfProps.size} config properties to executors")
        logger.debug(s"Sources: Hadoop(${hadoopTantivyProps.size}), Spark(${sparkTantivyProps.size}), Options(${readTantivyProps.size})")
      }
      
      // Create RDD from file paths using standalone object method for proper serialization
      spark.sparkContext.parallelize(serializableFiles).flatMap { filePath =>
        Tantivy4SparkRelation.processFile(filePath, serializableSchema, hadoopConfProps)
      }
    }
  }
}

class Tantivy4SparkTable(
    path: String,
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends SupportsRead with SupportsWrite {

  private val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkTable])
  private val spark = SparkSession.active
  private val tablePath = new Path(path)
  private val transactionLog = new TransactionLog(tablePath, spark, options)

  override def name(): String = s"tantivy4spark.`$path`"

  override def schema(): StructType = {
    transactionLog.getSchema().getOrElse(schema)
  }

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.TRUNCATE
    ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // Create broadcast variable with proper precedence: read options > Spark config > Hadoop config
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    
    // Extract configurations from Hadoop config (lowest precedence)
    val hadoopTantivyConfigs = hadoopConf.iterator().asScala
      .filter(_.getKey.startsWith("spark.tantivy4spark."))
      .map(entry => entry.getKey -> entry.getValue)
      .toMap
    
    // Extract configurations from Spark session config (middle precedence)
    val sparkTantivyConfigs = try {
      spark.conf.getAll.filter(_._1.startsWith("spark.tantivy4spark.")).toMap
    } catch {
      case _: Exception => Map.empty[String, String]
    }
    
    // Extract configurations from read options (highest precedence)
    val readTantivyConfigs = options.asScala
      .filter(_._1.startsWith("spark.tantivy4spark."))
      .toMap
    
    // Merge with proper precedence: Hadoop < Spark config < read options
    val tantivyConfigs = hadoopTantivyConfigs ++ sparkTantivyConfigs ++ readTantivyConfigs
    
    logger.info(s"🔧 Broadcasting ${tantivyConfigs.size} Tantivy4Spark configurations to executors")
    logger.info(s"🔧 Sources: Hadoop(${hadoopTantivyConfigs.size}), Spark(${sparkTantivyConfigs.size}), Options(${readTantivyConfigs.size})")
    val broadcastConfig = spark.sparkContext.broadcast(tantivyConfigs)
    
    new Tantivy4SparkScanBuilder(transactionLog, schema(), options, broadcastConfig)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    
    // Copy write options to Hadoop configuration so they're available in executors
    // Write options from info.options() should override any existing configuration
    import scala.jdk.CollectionConverters._
    val writeOptions = info.options()
    writeOptions.entrySet().asScala.foreach { entry =>
      val key = entry.getKey
      val value = entry.getValue
      if (key.startsWith("spark.tantivy4spark.")) {
        hadoopConf.set(key, value)
        logger.debug(s"🔧 V2 Write: Setting Hadoop config from write options: $key = ${if (key.contains("secret") || key.contains("Secret")) "***" else value}")
      }
    }
    
    new Tantivy4SparkWriteBuilder(transactionLog, tablePath, info, options, hadoopConf)
  }
}

class Tantivy4SparkTableProvider extends org.apache.spark.sql.connector.catalog.TableProvider {

  /**
   * Extracts paths from options, supporting both direct path parameters and multiple paths.
   * Handles paths from load("path") calls as well as .option("path", "value") and multiple paths.
   */
  protected def getPaths(options: CaseInsensitiveStringMap): Seq[String] = {
    val paths = Option(options.get("paths")).map { pathStr =>
      parsePathsFromJson(pathStr)
    }.getOrElse(Seq.empty)
    
    paths ++ Option(options.get("path")).toSeq
  }
  
  private def parsePathsFromJson(pathStr: String): Seq[String] = {
    try {
      import com.fasterxml.jackson.databind.ObjectMapper
      import com.fasterxml.jackson.module.scala.DefaultScalaModule
      
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
    } catch {
      case _: Exception => 
        throw new IllegalArgumentException(s"Invalid paths format: $pathStr. Expected JSON array of strings.")
    }
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val paths = getPaths(options)
    if (paths.isEmpty) {
      throw new IllegalArgumentException(
        "Path is required. Use load(\"path\") or .option(\"path\", \"value\").load()"
      )
    }

    val spark = SparkSession.active
    val transactionLog = new TransactionLog(new Path(paths.head), spark)
    
    transactionLog.getSchema().getOrElse {
      throw new IllegalArgumentException(s"Unable to infer schema from path: ${paths.head}")
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): org.apache.spark.sql.connector.catalog.Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val paths = getPaths(options)
    
    if (paths.isEmpty) {
      throw new IllegalArgumentException(
        "Path is required. Use load(\"path\") or .option(\"path\", \"value\").load()"
      )
    }

    // Use the first path as the primary table path (support for multiple paths can be added later)
    new Tantivy4SparkTable(paths.head, schema, options)
  }

  override def supportsExternalMetadata(): Boolean = true
}