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
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, TableScan, PrunedFilteredScan, Filter}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, TimestampType, DateType, LongType, StringType, DoubleType, FloatType, IntegerType, BooleanType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.TransactionLog
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
import com.tantivy4spark.io.CloudStorageProviderFactory

object Tantivy4SparkRelation {
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
      hadoopConfProps: Map[String, String],
      filters: Array[Filter] = Array.empty,
      limit: Option[Int] = None
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
      
      // Path should already be normalized by buildScan method
      val normalizedPath = filePath
      
      // Use SplitSearchEngine to read from split with proper cache configuration
      val splitSearchEngine = com.tantivy4spark.search.SplitSearchEngine.fromSplitFile(
        serializableSchema, 
        normalizedPath,
        cacheConfig
      )
      executorLogger.debug("Split search engine created successfully")
      
      // Get field names for schema validation
      val splitFieldNames = try {
        import scala.jdk.CollectionConverters._
        splitSearchEngine.getSchema().getFieldNames().asScala.toSet
      } catch {
        case e: Exception =>
          executorLogger.warn(s"Could not retrieve field names: ${e.getMessage}")
          Set.empty[String]
      }
      
      // Convert filters to Tantivy Query object
      val query = if (filters.nonEmpty) {
        val tantivySchema = splitSearchEngine.getSchema()
        val queryObj = if (splitFieldNames.nonEmpty) {
          val validatedQuery = FiltersToQueryConverter.convertToQuery(filters, tantivySchema, Some(splitFieldNames))
          executorLogger.info(s"V1 API: Created query with schema validation: ${validatedQuery.getClass.getSimpleName}")
          validatedQuery
        } else {
          val fallbackQuery = FiltersToQueryConverter.convertToQuery(filters, tantivySchema)
          executorLogger.info(s"V1 API: Created query without validation: ${fallbackQuery.getClass.getSimpleName}")
          fallbackQuery
        }
        queryObj
      } else {
        null // Use null to indicate no filters
      }
      
      // Calculate effective limit
      val effectiveLimit = limit.getOrElse(5000)
      executorLogger.info(s"V1 API: Pushing down limit: $effectiveLimit")
      
      // Execute search with pushed down query and limit
      val results = if (query != null) {
        executorLogger.info(s"Executing search with Query object and limit: $effectiveLimit")
        splitSearchEngine.search(query, limit = effectiveLimit)
      } else {
        executorLogger.info(s"No filters, executing searchAll with limit: $effectiveLimit")
        splitSearchEngine.searchAll(limit = effectiveLimit)
      }
      executorLogger.debug(s"Search returned ${results.length} results")
      
      if (results.length == 0) {
        executorLogger.warn(s"Search returned 0 results from split file")
        if (executorLogger.isDebugEnabled) {
          executorLogger.debug(s"Schema fields: ${serializableSchema.fieldNames.mkString(", ")}")
        }
      }
      
      // Convert search results to Spark Rows with enhanced error handling
      results.foreach { internalRow =>
        try {
          // Always use manual conversion to avoid Catalyst type conversion issues
          val values = serializableSchema.fields.map { field =>
            executorLogger.debug(s"Processing field ${field.name} with expected type ${field.dataType}")
            
            try {
              // Access field by name, not by index position
              val fieldIndex = try {
                // Find the field index in the InternalRow by field name
                // This is a simple approach - could be optimized with a field map
                val sparkSchema = org.apache.spark.sql.types.StructType(serializableSchema.fields)
                
                
                sparkSchema.fieldIndex(field.name)
              } catch {
                case _: Exception =>
                  executorLogger.warn(s"Could not find field ${field.name} in InternalRow schema")
                  -1
              }
              
              if (fieldIndex == -1 || fieldIndex >= internalRow.numFields || internalRow.isNullAt(fieldIndex)) {
                executorLogger.debug(s"Field ${field.name} is null or not found (row has ${internalRow.numFields} fields)")
                null
              } else {
                try {
                  // Handle temporal types specially since they're stored as i64 in Tantivy
                  val rawValue = field.dataType match {
                    case TimestampType =>
                      // Timestamp is stored as epoch millis, but can be Integer or Long
                      val value = internalRow.get(fieldIndex, field.dataType)
                      val longValue = if (value != null) value.asInstanceOf[Number].longValue() else 0L
                      new java.sql.Timestamp(longValue)
                    case DateType =>
                      // Date is stored as days since epoch, but can be Integer or Long
                      val value = internalRow.get(fieldIndex, field.dataType)
                      val longValue = if (value != null) value.asInstanceOf[Number].longValue() else 0L
                      new java.sql.Date(longValue * 24 * 60 * 60 * 1000L) // Convert days to millis
                    case _ =>
                      // For non-temporal types, convert to proper external Row types
                      val value = internalRow.get(fieldIndex, field.dataType)
                      field.dataType match {
                        case StringType =>
                          value match {
                            case utf8: org.apache.spark.unsafe.types.UTF8String => utf8.toString
                            case s: String => s
                            case other => if (other != null) other.toString else null
                          }
                        case DoubleType =>
                          executorLogger.debug(s"SALARY DEBUG: Processing DoubleType field ${field.name}, raw value: $value (type: ${if (value == null) "null" else value.getClass.getSimpleName})")
                          val result = value match {
                            case d: java.lang.Double => 
                              executorLogger.debug(s"SALARY DEBUG: Found java.lang.Double: $d")
                              d
                            case f: java.lang.Float => 
                              executorLogger.debug(s"SALARY DEBUG: Converting Float $f to Double")
                              f.doubleValue()
                            case s: String => 
                              executorLogger.debug(s"SALARY DEBUG: Converting String '$s' to Double")
                              try { s.toDouble } catch { case _: Exception => 0.0 }
                            case other => 
                              executorLogger.debug(s"SALARY DEBUG: Converting other type ${if (other == null) "null" else other.getClass.getSimpleName} $other to Double")
                              if (other != null) other.asInstanceOf[Number].doubleValue() else null
                          }
                          executorLogger.debug(s"SALARY DEBUG: Final result for ${field.name}: $result (type: ${if (result == null) "null" else result.getClass.getSimpleName})")
                          result
                        case FloatType =>
                          value match {
                            case f: java.lang.Float => f
                            case d: java.lang.Double => d.floatValue()
                            case s: String => try { s.toFloat } catch { case _: Exception => 0.0f }
                            case other => if (other != null) other.asInstanceOf[Number].floatValue() else null
                          }
                        case IntegerType =>
                          value match {
                            case i: java.lang.Integer => i
                            case l: java.lang.Long => l.intValue()
                            case s: String => try { s.toInt } catch { case _: Exception => 0 }
                            case other => if (other != null) other.asInstanceOf[Number].intValue() else null
                          }
                        case LongType =>
                          value match {
                            case l: java.lang.Long => l
                            case i: java.lang.Integer => i.longValue()
                            case s: String => try { s.toLong } catch { case _: Exception => 0L }
                            case other => if (other != null) other.asInstanceOf[Number].longValue() else null
                          }
                        case BooleanType =>
                          value match {
                            case b: java.lang.Boolean => b
                            case i: java.lang.Integer => i != 0
                            case l: java.lang.Long => l != 0
                            case s: String => s.toLowerCase == "true" || s == "1"
                            case other => if (other != null) other.toString.toLowerCase == "true" else false
                          }
                        case _ =>
                          value
                      }
                  }
                  rawValue
                } catch {
                  case e: Exception =>
                    executorLogger.warn(s"Failed to get field ${field.name} at index $fieldIndex: ${e.getMessage}")
                    null
                }
              }
            } catch {
              case e: Exception =>
                executorLogger.warn(s"Could not process field ${field.name}: ${e.getMessage}")
                null
            }
          }
          rows += org.apache.spark.sql.Row(values: _*)
        } catch {
          case ex: Exception =>
            executorLogger.error(s"Failed to convert search result to Row: ${ex.getMessage}")
            // Continue with next row instead of failing completely
        }
      }
      
      splitSearchEngine.close()
      executorLogger.debug(s"Converted ${rows.length} rows from search")
    } catch {
      case ex: Exception =>
        // Re-throw exceptions instead of silently returning empty results
        // This ensures that missing files and other errors are properly surfaced
        executorLogger.error(s"Failed to read Tantivy split file $filePath: ${ex.getMessage}")
        throw new RuntimeException(s"Failed to read Tantivy split file $filePath", ex)
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
    val allOptions = Map.newBuilder[String, String]
    allOptions ++= sparkConfigOptions
    allOptions ++= parameters
    val finalOptions = allOptions.result()
    
    // Copy all tantivy4spark options into Hadoop configuration so they're available in executors
    val currentHadoopConf = spark.sparkContext.hadoopConfiguration
    finalOptions.foreach { case (key, value) =>
      if (key.startsWith("spark.tantivy4spark.")) {
        currentHadoopConf.set(key, value)
        if (logger.isDebugEnabled) {
          logger.debug(s"Setting Hadoop config: $key = ${if (key.contains("secret") || key.contains("Secret")) "***" else value}")
        }
      }
    }
    val writeOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
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
    
    // Handle SaveMode for V1 DataSource API
    val finalWriteBuilder = mode match {
      case SaveMode.Overwrite =>
        logger.info("V1 API: SaveMode.Overwrite detected, enabling overwrite mode")
        // For V1 API, SaveMode.Overwrite should call truncate() to enable overwrite behavior
        writeBuilder.asInstanceOf[Tantivy4SparkWriteBuilder].truncate()
      case _ => 
        logger.info(s"V1 API: SaveMode detected: $mode")
        writeBuilder
    }
    
    val write = finalWriteBuilder.build()
    val batchWrite = write.toBatch
    
    // Extract serializable parameters before the closure
    val serializablePath = path
    val serializableSchema = data.schema
    
    // Pass all merged options (write options + Spark config) to executors
    // Use finalOptions which already has the proper precedence: sparkConfigOptions ++ parameters
    val enrichedOptions = finalOptions
    
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
) extends BaseRelation with TableScan with PrunedFilteredScan {
  
  @transient private lazy val logger = LoggerFactory.getLogger(classOf[Tantivy4SparkRelation])
  
  override def schema: StructType = {
    import scala.jdk.CollectionConverters._
    
    // Get schema from transaction log
    val spark = sqlContext.sparkSession
    
    // Extract tantivy4spark configurations from Spark session for credential propagation
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val tantivyConfigs = hadoopConf.iterator().asScala
      .filter(_.getKey.startsWith("spark.tantivy4spark."))
      .map(entry => entry.getKey -> entry.getValue)
      .toMap
    
    // Also get configs from Spark session (in case they weren't propagated to Hadoop config)
    val sparkConfigs = spark.conf.getAll.filter(_._1.startsWith("spark.tantivy4spark.")).toMap
    
    // Include read options (from DataFrame read API)
    val readTantivyOptions = readOptions.filter(_._1.startsWith("spark.tantivy4spark."))
    
    // Combine all sources with proper precedence to avoid duplicate key warnings
    // readOptions take highest precedence, then sparkConfigs, then hadoopConfigs
    val allConfigs = Map.newBuilder[String, String]
    allConfigs ++= tantivyConfigs
    allConfigs ++= sparkConfigs  
    allConfigs ++= readTantivyOptions
    val options = new CaseInsensitiveStringMap(allConfigs.result().asJava)
    
    val transactionLog = new TransactionLog(new Path(path), spark, options)
    transactionLog.getSchema().getOrElse {
      throw new RuntimeException(s"Table does not exist at path: $path. No transaction log found. Use spark.write to create the table first.")
    }
  }
  
  override def buildScan(): RDD[org.apache.spark.sql.Row] = {
    // Default buildScan without filters/column pruning
    buildScan(schema.fieldNames, Array.empty)
  }
  
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[org.apache.spark.sql.Row] = {
    import scala.jdk.CollectionConverters._
    
    val spark = sqlContext.sparkSession
    
    // Extract tantivy4spark configurations for credential propagation (same as schema method)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val tantivyConfigs = hadoopConf.iterator().asScala
      .filter(_.getKey.startsWith("spark.tantivy4spark."))
      .map(entry => entry.getKey -> entry.getValue)
      .toMap
    
    val sparkConfigs = spark.conf.getAll.filter(_._1.startsWith("spark.tantivy4spark.")).toMap
    
    // Include read options (from DataFrame read API)  
    val readTantivyOptions = readOptions.filter(_._1.startsWith("spark.tantivy4spark."))
    
    // Combine all sources with proper precedence to avoid duplicate key warnings
    // readOptions take highest precedence, then sparkConfigs, then hadoopConfigs
    val allConfigs = Map.newBuilder[String, String]
    allConfigs ++= tantivyConfigs
    allConfigs ++= sparkConfigs
    allConfigs ++= readTantivyOptions
    val options = new CaseInsensitiveStringMap(allConfigs.result().asJava)
    
    val transactionLog = new TransactionLog(new Path(path), spark, options)
    
    // Check if table exists by trying to get schema first
    val tableSchema = transactionLog.getSchema()
    if (tableSchema.isEmpty) {
      // Table doesn't exist - throw exception instead of returning empty results
      throw new RuntimeException(s"Table does not exist at path: $path. No transaction log found. Use spark.write to create the table first.")
    }
    
    // Get list of files from transaction log
    val files = transactionLog.listFiles()
    
    if (files.isEmpty) {
      // Table exists but has no data files (legitimate empty table)
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row]
    } else {
      // Hadoop configuration already available above
      
      // Extract serializable data - resolve relative paths to full paths
      // Normalize table path for tantivy4java compatibility (s3a:// -> s3://)
      val normalizedTablePath = if (path.startsWith("s3a://") || path.startsWith("s3n://")) {
        path.replaceFirst("^s3[an]://", "s3://")
      } else {
        path
      }
      val tablePath = new Path(normalizedTablePath)
      
      val serializableFiles = files.map { addAction =>
        if (addAction.path.startsWith("/") || addAction.path.contains("://")) {
          // Already absolute path - normalize protocol if needed
          val result = if (addAction.path.startsWith("s3a://") || addAction.path.startsWith("s3n://")) {
            addAction.path.replaceFirst("^s3[an]://", "s3://")
          } else {
            addAction.path
          }
          result
        } else {
          // Relative path, resolve against normalized table path
          // Check if this is a flattened S3Mock path (contains ___) 
          if (addAction.path.contains("___")) {
            // This is a flattened key - reconstruct the S3 path directly
            val tableUri = java.net.URI.create(tablePath.toString)
            val reconstructedPath = s"${tableUri.getScheme}://${tableUri.getHost}/${addAction.path}"
            reconstructedPath
          } else {
            // Standard relative path resolution
            val fullPath = new Path(tablePath, addAction.path)
            if (fullPath.toString.startsWith("file:")) {
              // Extract local filesystem path for tantivy4java compatibility
              new java.io.File(fullPath.toUri).getAbsolutePath
            } else {
              fullPath.toString
            }
          }
        }
      }
      val fullSchema = schema
     
      // Apply column pruning if required columns are specified
      // IMPORTANT: Preserve the order specified by requiredColumns for proper type alignment
      val serializableSchema = if (requiredColumns.nonEmpty && !requiredColumns.sameElements(fullSchema.fieldNames)) {
        val fieldMap = fullSchema.fields.map(field => field.name -> field).toMap
        val orderedFields = requiredColumns.flatMap(fieldName => fieldMap.get(fieldName))
        StructType(orderedFields)
      } else {
        fullSchema
      }
      
      if (requiredColumns.nonEmpty) {
        logger.info(s"V1 API: Column pruning - using ${serializableSchema.fields.length}/${fullSchema.fields.length} columns")
        logger.info(s"V1 API: Required columns: ${requiredColumns.mkString(", ")}")
      }
      
      // Use Hadoop configuration from driver context - include both traditional Hadoop configs and Tantivy4Spark configs
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
      
      // Log filter pushdown for V1 API
      if (filters.nonEmpty) {
        logger.info(s"V1 API: Pushing down ${filters.length} filters: ${filters.mkString(", ")}")
      }
      
      // Create RDD from file paths using standalone object method for proper serialization
      // Now with proper filter pushdown support via PrunedFilteredScan
      spark.sparkContext.parallelize(serializableFiles).flatMap { filePath =>
        Tantivy4SparkRelation.processFile(filePath, serializableSchema, hadoopConfProps, filters, None)
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
      throw new RuntimeException(s"Table does not exist at path: ${paths.head}. No transaction log found. Use spark.write to create the table first.")
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
