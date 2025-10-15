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

package io.indextables.spark.core

import java.util

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.{
  BaseRelation,
  CatalystScan,
  CreatableRelationProvider,
  DataSourceRegister,
  Filter,
  PrunedFilteredScan,
  RelationProvider,
  TableScan
}
import org.apache.spark.sql.types.{
  BooleanType,
  DateType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructType,
  TimestampType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.Path

import io.indextables.spark.io.CloudStorageProviderFactory
import io.indextables.spark.transaction.{TransactionLog, TransactionLogFactory}
import org.slf4j.LoggerFactory

object IndexTables4SparkRelation {
  def extractZipToDirectory(zipData: Array[Byte], targetDir: java.nio.file.Path): Unit = {
    val bais = new java.io.ByteArrayInputStream(zipData)
    val zis  = new java.util.zip.ZipInputStream(bais)

    try {
      var entry: java.util.zip.ZipEntry = zis.getNextEntry
      while (entry != null) {
        if (!entry.isDirectory) {
          val filePath = targetDir.resolve(entry.getName)
          val fos      = new java.io.FileOutputStream(filePath.toFile)

          try {
            val buffer = new Array[Byte](1024)
            var len    = zis.read(buffer)
            while (len != -1) {
              fos.write(buffer, 0, len)
              len = zis.read(buffer)
            }
          } finally
            fos.close()
        }

        zis.closeEntry()
        entry = zis.getNextEntry
      }
    } finally {
      zis.close()
      bais.close()
    }
  }

  // REMOVED: processFile method was broken due to tantivy4java metadata reading bug

  // Serializable wrapper for split metadata with footer offsets
  case class SerializableSplitMetadata(
    numDocs: Long,
    uncompressedSize: Long,
    footerStartOffset: Long,
    footerEndOffset: Long,
    hotcacheStartOffset: Long,
    hotcacheLength: Long,
    // Complete tantivy4java SplitMetadata fields
    timeRangeStart: Option[String] = None,
    timeRangeEnd: Option[String] = None,
    splitTags: Option[Set[String]] = None,
    deleteOpstamp: Option[Long] = None,
    numMergeOps: Option[Int] = None,
    docMappingJson: Option[String] = None)
      extends Serializable {

    // Convert to tantivy4java SplitMetadata for use with split readers
    def toTantivySplitMetadata(splitPath: String): io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata = {
      import scala.jdk.CollectionConverters._
      new io.indextables.tantivy4java.split.merge.QuickwitSplit.SplitMetadata(
        splitPath.split("/").last.replace(".split", ""),    // splitId from filename
        "tantivy4spark-index",                              // indexUid (NEW - required)
        0L,                                                 // partitionId (NEW - required)
        "tantivy4spark-source",                             // sourceId (NEW - required)
        "tantivy4spark-node",                               // nodeId (NEW - required)
        numDocs,                                            // numDocs
        uncompressedSize,                                   // uncompressedSizeBytes
        timeRangeStart.map(java.time.Instant.parse).orNull, // timeRangeStart
        timeRangeEnd.map(java.time.Instant.parse).orNull,   // timeRangeEnd
        System.currentTimeMillis() / 1000,                  // createTimestamp (NEW - required)
        "Mature",                                           // maturity (NEW - required)
        splitTags.getOrElse(Set.empty[String]).asJava,      // tags
        footerStartOffset,                                  // footerStartOffset
        footerEndOffset,                                    // footerEndOffset
        deleteOpstamp.getOrElse(0L),                        // deleteOpstamp
        numMergeOps.getOrElse(0),                           // numMergeOps
        "doc-mapping-uid",                                  // docMappingUid (NEW - required)
        docMappingJson.orNull,                              // docMappingJson (MOVED - for performance)
        java.util.Collections.emptyList[String]()           // skippedSplits
      )
    }
  }

  // Helper function to create SerializableSplitMetadata from AddAction footer offset information
  def createSerializableMetadataFromAddAction(addAction: io.indextables.spark.transaction.AddAction)
    : SerializableSplitMetadata = {
    if (!addAction.hasFooterOffsets || addAction.footerStartOffset.isEmpty || addAction.footerEndOffset.isEmpty) {
      throw new RuntimeException(
        s"AddAction for ${addAction.path} does not contain required footer offsets. All 'add' entries in the transaction log must contain footer offset metadata."
      )
    }

    // Safe conversion functions for Option[Any] to Long to handle JSON deserialization type variations
    def toLongSafeOption(opt: Option[Any]): Long = opt match {
      case Some(value) =>
        value match {
          case l: Long              => l
          case i: Int               => i.toLong
          case i: java.lang.Integer => i.toLong
          case l: java.lang.Long    => l
          case _                    => value.toString.toLong
        }
      case None => 0L
    }

    SerializableSplitMetadata(
      numDocs = toLongSafeOption(addAction.numRecords),                      // Safe conversion
      uncompressedSize = toLongSafeOption(addAction.uncompressedSizeBytes),  // Safe conversion
      footerStartOffset = toLongSafeOption(addAction.footerStartOffset),     // Safe conversion
      footerEndOffset = toLongSafeOption(addAction.footerEndOffset),         // Safe conversion
      hotcacheStartOffset = toLongSafeOption(addAction.hotcacheStartOffset), // Safe conversion
      hotcacheLength = toLongSafeOption(addAction.hotcacheLength),           // Safe conversion
      // Complete tantivy4java SplitMetadata fields
      timeRangeStart = addAction.timeRangeStart,
      timeRangeEnd = addAction.timeRangeEnd,
      splitTags = addAction.splitTags,
      deleteOpstamp = addAction.deleteOpstamp.map { value: Any =>
        value match {
          case l: Long              => l
          case i: Int               => i.toLong
          case i: java.lang.Integer => i.toLong
          case l: java.lang.Long    => l
          case _                    => value.toString.toLong
        }
      }, // Safe conversion for deleteOpstamp
      numMergeOps = addAction.numMergeOps,
      docMappingJson = addAction.docMappingJson
    )
  }

  // Enhanced processFile that can handle both Spark filters and custom IndexQuery filters
  def processFileWithCustomFilters(
    filePath: String,
    serializableSchema: StructType,
    hadoopConfProps: Map[String, String],
    sparkFilters: Array[Filter] = Array.empty,
    customFilters: Array[Any] = Array.empty,
    limit: Option[Int] = None,
    splitMetadata: Option[SerializableSplitMetadata] = None,
    readOptions: Map[String, String] = Map.empty
  ): Iterator[org.apache.spark.sql.Row] = {
    import io.indextables.spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

    // Create local logger for executor to avoid serialization issues
    val executorLogger = LoggerFactory.getLogger(IndexTables4SparkRelation.getClass)

    // Recreate Hadoop configuration in executor context
    val localHadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConfProps.foreach {
      case (key, value) =>
        localHadoopConf.set(key, value)
    }

    // DEBUG: Print all tantivy4spark configurations received in executor (including normalized indextables configs)
    import io.indextables.spark.util.ConfigNormalization
    val tantivyConfigs = ConfigNormalization.extractTantivyConfigsFromMap(hadoopConfProps)
    if (executorLogger.isDebugEnabled) {
      executorLogger.debug(s"processFileWithCustomFilters received ${hadoopConfProps.size} total config properties")
      executorLogger.debug(s"processFileWithCustomFilters found ${tantivyConfigs.size} tantivy4spark configs:")
      tantivyConfigs.foreach {
        case (key, value) =>
          val displayValue = if (key.contains("secret") || key.contains("session")) "***" else value
          executorLogger.debug(s"   $key = $displayValue")
      }
    }

    // Extract cache configuration with session token support from Hadoop props
    val cacheConfig = io.indextables.spark.storage.SplitCacheConfig(
      cacheName = {
        val configName = hadoopConfProps.getOrElse("spark.indextables.cache.name", "")
        if (configName.trim.nonEmpty) {
          configName.trim
        } else {
          // Use file path as cache name for table-specific caching
          val tablePath = new org.apache.hadoop.fs.Path(filePath).getParent.toString
          s"tantivy4spark-${tablePath.replaceAll("[^a-zA-Z0-9]", "_")}"
        }
      },
      maxCacheSize = hadoopConfProps.getOrElse("spark.indextables.cache.maxSize", "200000000").toLong,
      maxConcurrentLoads = hadoopConfProps.getOrElse("spark.indextables.cache.maxConcurrentLoads", "8").toInt,
      enableQueryCache = hadoopConfProps.getOrElse("spark.indextables.cache.queryCache", "true").toBoolean,
      enableDocBatch = hadoopConfProps.getOrElse("spark.indextables.docBatch.enabled", "true").toBoolean,
      docBatchMaxSize = hadoopConfProps.getOrElse("spark.indextables.docBatch.maxSize", "1000").toInt,
      // AWS configuration with session token support (handle both camelCase and lowercase keys)
      awsAccessKey = hadoopConfProps
        .get("spark.indextables.aws.accessKey")
        .orElse(hadoopConfProps.get("spark.indextables.aws.accesskey")),
      awsSecretKey = hadoopConfProps
        .get("spark.indextables.aws.secretKey")
        .orElse(hadoopConfProps.get("spark.indextables.aws.secretkey")),
      awsSessionToken = hadoopConfProps
        .get("spark.indextables.aws.sessionToken")
        .orElse(hadoopConfProps.get("spark.indextables.aws.sessiontoken")),
      awsRegion = hadoopConfProps.get("spark.indextables.aws.region"),
      awsEndpoint = hadoopConfProps.get("spark.indextables.s3.endpoint"),
      awsPathStyleAccess = hadoopConfProps.get("spark.indextables.s3.pathStyleAccess").map(_.toLowerCase == "true"),
      // Azure configuration
      azureAccountName = hadoopConfProps.get("spark.indextables.azure.accountName"),
      azureAccountKey = hadoopConfProps.get("spark.indextables.azure.accountKey"),
      azureConnectionString = hadoopConfProps.get("spark.indextables.azure.connectionString"),
      azureEndpoint = hadoopConfProps.get("spark.indextables.azure.endpoint"),
      // GCP configuration
      gcpProjectId = hadoopConfProps.get("spark.indextables.gcp.projectId"),
      gcpServiceAccountKey = hadoopConfProps.get("spark.indextables.gcp.serviceAccountKey"),
      gcpCredentialsFile = hadoopConfProps.get("spark.indextables.gcp.credentialsFile"),
      gcpEndpoint = hadoopConfProps.get("spark.indextables.gcp.endpoint")
    )

    // Use SplitSearchEngine to read split files directly
    val rows = scala.collection.mutable.ListBuffer[org.apache.spark.sql.Row]()

    try {
      executorLogger.info(s"Reading Tantivy split file: $filePath with ${sparkFilters.length} Spark + ${customFilters.length} custom filters")

      // Path should already be normalized by buildScan method
      val normalizedPath = filePath

      // Split metadata is required for all operations
      if (splitMetadata.isEmpty) {
        throw new RuntimeException(
          s"Split metadata is required for $normalizedPath. All split processing operations must have footer offset metadata."
        )
      }

      // Use SplitSearchEngine to read from split with proper cache configuration and footer offset optimization
      executorLogger.info(s"🚀 Using footer offset optimization for $normalizedPath")
      val tantivyMetadata = splitMetadata.get.toTantivySplitMetadata(normalizedPath)
      val splitSearchEngine = io.indextables.spark.search.SplitSearchEngine.fromSplitFileWithMetadata(
        serializableSchema,
        normalizedPath,
        tantivyMetadata,
        cacheConfig
      )
      executorLogger.debug("Split search engine created successfully")

      // Get field names for schema validation
      val splitFieldNames =
        try {
          import scala.jdk.CollectionConverters._
          splitSearchEngine.getSchema().getFieldNames().asScala.toSet
        } catch {
          case e: Exception =>
            executorLogger.warn(s"Could not retrieve field names: ${e.getMessage}")
            Set.empty[String]
        }

      // Convert all filters to SplitQuery object - combine Spark filters and custom filters
      val allFilters = sparkFilters.toSeq ++ customFilters.toSeq
      val splitQuery = if (allFilters.nonEmpty) {
        val queryObj = if (splitFieldNames.nonEmpty) {
          // Convert readOptions to CaseInsensitiveStringMap for passing to FiltersToQueryConverter
          import scala.jdk.CollectionConverters._
          val optionsMap = new org.apache.spark.sql.util.CaseInsensitiveStringMap(readOptions.asJava)

          // Combine all filters for processing by existing FiltersToQueryConverter
          val combinedFilters: Array[Any] = (sparkFilters ++ customFilters.collect {
            case f: IndexQueryFilter    => f
            case f: IndexQueryAllFilter => f
          }).toArray

          val validatedQuery = FiltersToQueryConverter.convertToSplitQuery(
            combinedFilters,
            splitSearchEngine,
            Some(splitFieldNames),
            Some(optionsMap)
          )
          executorLogger.info(
            s"CatalystScan: Created SplitQuery with schema validation: ${validatedQuery.getClass.getSimpleName}"
          )
          validatedQuery
        } else {
          // Convert readOptions to CaseInsensitiveStringMap for passing to FiltersToQueryConverter
          import scala.jdk.CollectionConverters._
          val optionsMap = new org.apache.spark.sql.util.CaseInsensitiveStringMap(readOptions.asJava)

          val combinedFilters: Array[Any] = (sparkFilters ++ customFilters.collect {
            case f: IndexQueryFilter    => f
            case f: IndexQueryAllFilter => f
          }).toArray

          val fallbackQuery =
            FiltersToQueryConverter.convertToSplitQuery(combinedFilters, splitSearchEngine, None, Some(optionsMap))
          executorLogger.info(
            s"CatalystScan: Created SplitQuery without validation: ${fallbackQuery.getClass.getSimpleName}"
          )
          fallbackQuery
        }
        queryObj
      } else {
        import io.indextables.tantivy4java.split.SplitMatchAllQuery
        new SplitMatchAllQuery() // Use match-all query for no filters
      }

      // Calculate effective limit - use MaxInt for unlimited behavior in V1
      val effectiveLimit = limit.getOrElse(Int.MaxValue)
      executorLogger.info(s"CatalystScan: Pushing down limit: $effectiveLimit")

      // Execute search with pushed down SplitQuery and limit
      executorLogger.info(s"Executing search with SplitQuery object [$splitQuery] and limit: $effectiveLimit")
      val results = splitSearchEngine.search(splitQuery, limit = effectiveLimit)
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
              val fieldIndex =
                try {
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
                executorLogger.debug(
                  s"Field ${field.name} is null or not found (row has ${internalRow.numFields} fields)"
                )
                null
              } else {
                try {
                  // Handle temporal types specially since they're stored as i64 in Tantivy
                  val rawValue = field.dataType match {
                    case TimestampType =>
                      // Timestamp is stored as epoch millis in Tantivy, convert back to microseconds for Spark
                      val value     = internalRow.get(fieldIndex, field.dataType)
                      val longValue = if (value != null) value.asInstanceOf[Number].longValue() else 0L
                      // Convert from stored milliseconds back to microseconds, then create Timestamp from millis
                      new java.sql.Timestamp(longValue)
                    case DateType =>
                      // Date is stored as days since epoch, but can be Integer or Long
                      val value     = internalRow.get(fieldIndex, field.dataType)
                      val longValue = if (value != null) value.asInstanceOf[Number].longValue() else 0L
                      new java.sql.Date(longValue * 24 * 60 * 60 * 1000L) // Convert days to millis
                    case _ =>
                      // For non-temporal types, convert to proper external Row types
                      val value = internalRow.get(fieldIndex, field.dataType)
                      field.dataType match {
                        case StringType =>
                          value match {
                            case utf8: org.apache.spark.unsafe.types.UTF8String => utf8.toString
                            case s: String                                      => s
                            case other => if (other != null) other.toString else null
                          }
                        case DoubleType =>
                          executorLogger.debug(
                            s"SALARY DEBUG: Processing DoubleType field ${field.name}, raw value: $value (type: ${if (value == null) "null"
                              else value.getClass.getSimpleName})"
                          )
                          val result = value match {
                            case d: java.lang.Double =>
                              executorLogger.debug(s"SALARY DEBUG: Found java.lang.Double: $d")
                              d
                            case f: java.lang.Float =>
                              executorLogger.debug(s"SALARY DEBUG: Converting Float $f to Double")
                              f.doubleValue()
                            case s: String =>
                              executorLogger.debug(s"SALARY DEBUG: Converting String '$s' to Double")
                              try s.toDouble
                              catch { case _: Exception => 0.0 }
                            case other =>
                              executorLogger.debug(s"SALARY DEBUG: Converting other type ${if (other == null) "null"
                                else other.getClass.getSimpleName} $other to Double")
                              if (other != null) other.asInstanceOf[Number].doubleValue() else null
                          }
                          executorLogger.debug(
                            s"SALARY DEBUG: Final result for ${field.name}: $result (type: ${if (result == null) "null"
                              else result.getClass.getSimpleName})"
                          )
                          result
                        case FloatType =>
                          value match {
                            case f: java.lang.Float  => f
                            case d: java.lang.Double => d.floatValue()
                            case s: String =>
                              try s.toFloat
                              catch { case _: Exception => 0.0f }
                            case other => if (other != null) other.asInstanceOf[Number].floatValue() else null
                          }
                        case IntegerType =>
                          value match {
                            case i: java.lang.Integer => i
                            case l: java.lang.Long    => l.intValue()
                            case s: String =>
                              try s.toInt
                              catch { case _: Exception => 0 }
                            case other => if (other != null) other.asInstanceOf[Number].intValue() else null
                          }
                        case LongType =>
                          value match {
                            case l: java.lang.Long    => l
                            case i: java.lang.Integer => i.longValue()
                            case s: String =>
                              try s.toLong
                              catch { case _: Exception => 0L }
                            case other => if (other != null) other.asInstanceOf[Number].longValue() else null
                          }
                        case BooleanType =>
                          value match {
                            case b: java.lang.Boolean => b
                            case i: java.lang.Integer => i != 0
                            case l: java.lang.Long    => l != 0
                            case s: String            => s.toLowerCase == "true" || s == "1"
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

class IndexTables4SparkDataSource extends DataSourceRegister with RelationProvider with CreatableRelationProvider {
  @transient private lazy val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkDataSource])

  override def shortName(): String = "tantivy4spark"

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]
  ): BaseRelation = {
    // For reads, create a relation that can handle queries
    val path = parameters.getOrElse("path", throw new IllegalArgumentException("Path is required"))
    new IndexTables4SparkRelation(path, sqlContext, parameters)
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
    val spark     = sqlContext.sparkSession
    val sparkConf = spark.conf

    // Extract all tantivy4spark options from Spark configuration
    val sparkConfigOptions =
      try {
        import io.indextables.spark.util.ConfigNormalization
        ConfigNormalization.extractTantivyConfigsFromSpark(spark)
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
    // Apply normalization to ensure both spark.indextables.* and spark.indextables.* are handled
    import io.indextables.spark.util.ConfigNormalization
    val normalizedConfigs = ConfigNormalization.extractTantivyConfigsFromMap(finalOptions)
    normalizedConfigs.foreach {
      case (key, value) =>
        currentHadoopConf.set(key, value)
        if (logger.isDebugEnabled) {
          logger.debug(
            s"Setting Hadoop config: $key = ${if (key.contains("secret") || key.contains("Secret")) "***" else value}"
          )
        }
    }
    val writeOptions  = new CaseInsensitiveStringMap(finalOptions.asJava)
    val tableProvider = new IndexTables4SparkTableProvider()

    // Get or create the table
    val table = tableProvider.getTable(data.schema, Array.empty, writeOptions)

    // Create write info
    val writeInfo = new LogicalWriteInfo {
      override def queryId(): String                   = java.util.UUID.randomUUID().toString
      override def schema(): StructType                = data.schema
      override def options(): CaseInsensitiveStringMap = writeOptions
    }

    // Get the write builder and execute the write
    val writeBuilder = table.asInstanceOf[IndexTables4SparkTable].newWriteBuilder(writeInfo)

    // Handle SaveMode for V1 DataSource API
    val finalWriteBuilder = mode match {
      case SaveMode.Overwrite =>
        logger.info("V1 API: SaveMode.Overwrite detected, enabling overwrite mode")
        // For V1 API, SaveMode.Overwrite should call truncate() to enable overwrite behavior
        writeBuilder.asInstanceOf[IndexTables4SparkWriteBuilder].truncate()
      case _ =>
        logger.info(s"V1 API: SaveMode detected: $mode")
        writeBuilder
    }

    val write      = finalWriteBuilder.build()
    val batchWrite = write.toBatch

    // Extract serializable parameters before the closure
    val serializablePath   = path
    val serializableSchema = data.schema

    // Pass all merged options (write options + Spark config) to executors
    // Use finalOptions which already has the proper precedence: sparkConfigOptions ++ parameters
    val enrichedOptions = finalOptions

    // Extract essential Hadoop configuration properties as a Map
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val essentialConfProps = Map(
      "fs.defaultFS"             -> hadoopConf.get("fs.defaultFS", ""),
      "fs.s3a.access.key"        -> hadoopConf.get("fs.s3a.access.key", ""),
      "fs.s3a.secret.key"        -> hadoopConf.get("fs.s3a.secret.key", ""),
      "fs.s3a.endpoint"          -> hadoopConf.get("fs.s3a.endpoint", ""),
      "fs.s3a.path.style.access" -> hadoopConf.get("fs.s3a.path.style.access", ""),
      "fs.s3a.impl"              -> hadoopConf.get("fs.s3a.impl", ""),
      "fs.hdfs.impl"             -> hadoopConf.get("fs.hdfs.impl", ""),
      "fs.file.impl"             -> hadoopConf.get("fs.file.impl", ""),
      // Add IndexTables4Spark-specific configurations for executor distribution
      "spark.indextables.aws.accessKey"      -> hadoopConf.get("spark.indextables.aws.accessKey", ""),
      "spark.indextables.aws.secretKey"      -> hadoopConf.get("spark.indextables.aws.secretKey", ""),
      "spark.indextables.aws.sessionToken"   -> hadoopConf.get("spark.indextables.aws.sessionToken", ""),
      "spark.indextables.aws.region"         -> hadoopConf.get("spark.indextables.aws.region", ""),
      "spark.indextables.s3.endpoint"        -> hadoopConf.get("spark.indextables.s3.endpoint", ""),
      "spark.indextables.s3.pathStyleAccess" -> hadoopConf.get("spark.indextables.s3.pathStyleAccess", "")
    ).filter(_._2.nonEmpty)

    // Debug: Log what configurations are being distributed to executors
    logger.info(s"Distributing ${essentialConfProps.size} configuration properties to executors:")
    essentialConfProps.foreach {
      case (key, value) =>
        val maskedValue = if (key.contains("secretKey") || key.contains("secret.key")) "***" else value
        logger.info(s"  $key = $maskedValue")
    }

    // Check if optimized write is enabled and apply repartitioning if needed
    val finalData = if (enrichedOptions.getOrElse("optimizeWrite", "true").toBoolean) {
      // Use IndexTables4SparkOptions for proper validation
      import scala.jdk.CollectionConverters._
      val optionsMap     = new org.apache.spark.sql.util.CaseInsensitiveStringMap(enrichedOptions.asJava)
      val tantivyOptions = IndexTables4SparkOptions(optionsMap)

      // Determine target records per split with auto-sizing support
      val (targetRecords, totalRecords) = if (tantivyOptions.autoSizeEnabled.getOrElse(false)) {
        // Auto-sizing is enabled - count DataFrame for accurate partitioning and calculate optimal target
        val actualRecordCount =
          try
            data.count()
          catch {
            case _: Exception =>
              // Fallback if count fails: use current partitions * estimate
              data.rdd.getNumPartitions * 50000L // Assume 50k records per partition
          }

        // Validate auto-sizing configuration early - these errors should not be caught
        val targetSizeBytes = tantivyOptions.autoSizeTargetSplitSize match {
          case Some(sizeStr) =>
            import io.indextables.spark.util.SizeParser
            val bytes = SizeParser.parseSize(sizeStr)
            if (bytes <= 0) {
              throw new IllegalArgumentException(s"Auto-sizing target split size must be positive: $sizeStr")
            }
            bytes
          case None =>
            throw new IllegalArgumentException("Auto-sizing enabled but no target split size specified")
        }

        val calculatedTarget =
          try {
            import io.indextables.spark.util.{SizeParser, SplitSizeAnalyzer}
            logger.info(s"V1 Auto-sizing: target split size = ${SizeParser.formatBytes(targetSizeBytes)}")

            // Analyze historical data to calculate target rows
            val analyzer = SplitSizeAnalyzer(new Path(path), spark, optionsMap)
            analyzer.calculateTargetRows(targetSizeBytes) match {
              case Some(calculatedRows) =>
                logger.info(
                  s"V1 Auto-sizing: calculated target rows per split = $calculatedRows based on historical data"
                )
                calculatedRows
              case None =>
                logger.warn(
                  "V1 Auto-sizing: failed to calculate target rows from historical data, using manual configuration"
                )
                tantivyOptions.targetRecordsPerSplit.getOrElse(1000000L)
            }
          } catch {
            case ex: Exception =>
              logger.error(s"V1 Auto-sizing historical analysis failed: ${ex.getMessage}", ex)
              tantivyOptions.targetRecordsPerSplit.getOrElse(1000000L)
          }

        (calculatedTarget, actualRecordCount)
      } else {
        // Standard optimization - use manual target and estimate records to avoid expensive count()
        val manualTarget = tantivyOptions.targetRecordsPerSplit.getOrElse(1000000L)

        // For small datasets (few partitions), count actual rows for accuracy
        val estimatedRecords = if (data.rdd.getNumPartitions <= 20) {
          try {
            val actualCount = data.count()
            logger.info(s"V1 OptimizedWrite: Actual row count for small dataset: $actualCount")
            actualCount
          } catch {
            case e: Exception =>
              logger.warn(s"V1 OptimizedWrite: Failed to count rows, using estimate: ${e.getMessage}")
              data.rdd.getNumPartitions * 50000L // Fallback estimate
          }
        } else {
          // For larger datasets, use heuristic to avoid expensive count()
          val estimate = data.rdd.getNumPartitions * 50000L
          logger.info(s"V1 OptimizedWrite: Using estimate for large dataset: $estimate")
          estimate
        }

        (manualTarget, estimatedRecords)
      }

      val optimalPartitions = Math.max(1, Math.ceil(totalRecords.toDouble / targetRecords.toDouble).toInt)
      val currentPartitions = data.rdd.getNumPartitions

      logger.debug(s"🔍 V1 OPTIMIZED WRITE: totalRecords=$totalRecords, targetRecords=$targetRecords, optimalPartitions=$optimalPartitions, currentPartitions=$currentPartitions")

      if (tantivyOptions.autoSizeEnabled.getOrElse(false)) {
        logger.info(
          s"V1 Auto-sizing: actual records = $totalRecords (counted), calculated target = $targetRecords per split"
        )
        logger.info(s"V1 Auto-sizing: current partitions = $currentPartitions, optimal partitions = $optimalPartitions")
      } else {
        logger.info(
          s"V1 OptimizedWrite: estimated records = $totalRecords (no count), target = $targetRecords per split"
        )
        logger.info(
          s"V1 OptimizedWrite: current partitions = $currentPartitions, optimal partitions = $optimalPartitions"
        )
      }

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
    val commitMessages = finalData.queryExecution.toRdd
      .mapPartitionsWithIndex { (partitionId, iterator) =>
        // Create a local logger inside the closure to avoid serialization issues
        val executorLogger = LoggerFactory.getLogger(classOf[IndexTables4SparkDataSource])

        // Recreate Hadoop configuration with essential properties in the executor
        val localHadoopConf = new org.apache.hadoop.conf.Configuration()
        essentialConfProps.foreach {
          case (key, value) =>
            localHadoopConf.set(key, value)
        }

        // Also add write options to Hadoop config to ensure they override any existing values
        if (executorLogger.isDebugEnabled) {
          executorLogger.debug("Executor: Adding write options to Hadoop config")
          enrichedOptions.foreach {
            case (key, value) =>
              if (key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")) {
                val normalizedKey = if (key.startsWith("spark.indextables.")) {
                  key.replace("spark.indextables.", "spark.indextables.")
                } else key
                localHadoopConf.set(normalizedKey, value)
                val displayValue =
                  if (
                    normalizedKey.contains("secret") || normalizedKey
                      .contains("Secret") || normalizedKey.contains("session")
                  ) "***"
                  else value
                executorLogger.debug(s"  Setting in executor: $normalizedKey = $displayValue")
              }
          }
        } else {
          // Still need to set the config even when debug is disabled
          enrichedOptions.foreach {
            case (key, value) =>
              if (key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")) {
                val normalizedKey = if (key.startsWith("spark.indextables.")) {
                  key.replace("spark.indextables.", "spark.indextables.")
                } else key
                localHadoopConf.set(normalizedKey, value)
              }
          }
        }

        // Serialize hadoop config to avoid Configuration serialization issues
        val serializedHadoopConfig = {
          val props = scala.collection.mutable.Map[String, String]()
          val iter  = localHadoopConf.iterator()
          while (iter.hasNext) {
            val entry = iter.next()
            if (entry.getKey.startsWith("spark.indextables.")) {
              props.put(entry.getKey, entry.getValue)
            }
          }
          props.toMap
        }

        val localWriterFactory = new io.indextables.spark.core.IndexTables4SparkWriterFactory(
          new Path(serializablePath),
          serializableSchema,
          // Filter options and normalize keys
          enrichedOptions
            .filter {
              case (key, _) =>
                key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
            }
            .map {
              case (key, value) =>
                val normalizedKey = if (key.startsWith("spark.indextables.")) {
                  key.replace("spark.indextables.", "spark.indextables.")
                } else key
                normalizedKey -> value
            },
          serializedHadoopConfig
        )
        val writer = localWriterFactory.createWriter(partitionId, 0L)
        try {
          iterator.foreach(row => writer.write(row))
          Seq(writer.commit()).iterator
        } catch {
          case e: Exception =>
            writer.abort()
            throw e
        } finally
          writer.close()
      }
      .collect()

    // Commit all the writes
    batchWrite.commit(commitMessages)

    // Return a relation for reading the written data
    createRelation(sqlContext, parameters)
  }
}

class IndexTables4SparkRelation(
  path: String,
  val sqlContext: SQLContext,
  readOptions: Map[String, String] = Map.empty)
    extends BaseRelation
    with TableScan
    with PrunedFilteredScan
    with CatalystScan {

  @transient private lazy val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkRelation])

  override def schema: StructType = {
    import scala.jdk.CollectionConverters._

    // Get schema from transaction log
    val spark = sqlContext.sparkSession

    // Extract tantivy4spark/indextables configurations from Spark session for credential propagation
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val tantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter { entry =>
        entry.getKey.startsWith("spark.indextables.") || entry.getKey.startsWith("spark.indextables.")
      }
      .map { entry =>
        val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
          entry.getKey.replace("spark.indextables.", "spark.indextables.")
        } else entry.getKey
        normalizedKey -> entry.getValue
      }
      .toMap

    // Also get configs from Spark session (in case they weren't propagated to Hadoop config)
    val sparkConfigs = spark.conf.getAll
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
      }
      .map {
        case (key, value) =>
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          normalizedKey -> value
      }
      .toMap

    // Include read options (from DataFrame read API)
    val readTantivyOptions = readOptions
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
      }
      .map {
        case (key, value) =>
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          normalizedKey -> value
      }

    // Combine all sources with proper precedence to avoid duplicate key warnings
    // readOptions take highest precedence, then sparkConfigs, then hadoopConfigs
    val allConfigs = Map.newBuilder[String, String]
    allConfigs ++= tantivyConfigs
    allConfigs ++= sparkConfigs
    allConfigs ++= readTantivyOptions
    val options = new CaseInsensitiveStringMap(allConfigs.result().asJava)

    val transactionLog = TransactionLogFactory.create(new Path(path), spark, options)
    transactionLog.getSchema().getOrElse {
      throw new RuntimeException(
        s"Path does not exist: $path. No transaction log found. Use spark.write to create the table first."
      )
    }
  }

  override def buildScan(): RDD[org.apache.spark.sql.Row] =
    // Default buildScan without filters/column pruning
    buildScan(schema.fieldNames, Array.empty[org.apache.spark.sql.sources.Filter])

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[org.apache.spark.sql.Row] = {
    import scala.jdk.CollectionConverters._

    val spark = sqlContext.sparkSession

    // Extract tantivy4spark/indextables configurations for credential propagation (same as schema method)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val tantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter { entry =>
        entry.getKey.startsWith("spark.indextables.") || entry.getKey.startsWith("spark.indextables.")
      }
      .map { entry =>
        val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
          entry.getKey.replace("spark.indextables.", "spark.indextables.")
        } else entry.getKey
        normalizedKey -> entry.getValue
      }
      .toMap

    val sparkConfigs = spark.conf.getAll
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
      }
      .map {
        case (key, value) =>
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          normalizedKey -> value
      }
      .toMap

    // Include read options (from DataFrame read API)
    val readTantivyOptions = readOptions
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
      }
      .map {
        case (key, value) =>
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          normalizedKey -> value
      }

    // Combine all sources with proper precedence to avoid duplicate key warnings
    // readOptions take highest precedence, then sparkConfigs, then hadoopConfigs
    val allConfigs = Map.newBuilder[String, String]
    allConfigs ++= tantivyConfigs
    allConfigs ++= sparkConfigs
    allConfigs ++= readTantivyOptions
    val options = new CaseInsensitiveStringMap(allConfigs.result().asJava)

    val transactionLog = TransactionLogFactory.create(new Path(path), spark, options)

    // Check if table exists by trying to get schema first
    val tableSchema = transactionLog.getSchema()
    if (tableSchema.isEmpty) {
      // Table doesn't exist - throw exception instead of returning empty results
      throw new RuntimeException(
        s"Path does not exist: $path. No transaction log found. Use spark.write to create the table first."
      )
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

      // Create serializable (path, metadata) pairs from AddAction entries
      val serializableFilesWithMetadata = files.map { addAction =>
        // Use centralized path resolution utility to avoid file:/ double-prefixing
        val resolvedPath = if (addAction.path.contains("___")) {
          // This is a flattened S3Mock path - reconstruct the S3 path directly
          val tableUri = java.net.URI.create(tablePath.toString)
          s"${tableUri.getScheme}://${tableUri.getHost}/${addAction.path}"
        } else {
          // Use PathResolutionUtils for consistent path resolution
          PathResolutionUtils.resolveSplitPathAsString(addAction.path, tablePath.toString)
        }

        // Extract serializable metadata from AddAction - required for tantivy4java operations
        val serializableMetadata = if (addAction.hasFooterOffsets && addAction.footerStartOffset.isDefined) {
          Some(IndexTables4SparkRelation.createSerializableMetadataFromAddAction(addAction))
        } else {
          throw new RuntimeException(
            s"AddAction for ${addAction.path} does not contain required footer offsets. All 'add' entries in the transaction log must contain footer offset metadata."
          )
        }

        (resolvedPath, serializableMetadata)
      }
      val fullSchema = schema

      // Apply column pruning if required columns are specified
      // IMPORTANT: Preserve the order specified by requiredColumns for proper type alignment
      val serializableSchema = if (requiredColumns.nonEmpty && !requiredColumns.sameElements(fullSchema.fieldNames)) {
        val fieldMap      = fullSchema.fields.map(field => field.name -> field).toMap
        val orderedFields = requiredColumns.flatMap(fieldName => fieldMap.get(fieldName))
        StructType(orderedFields)
      } else {
        fullSchema
      }

      if (requiredColumns.nonEmpty) {
        logger.info(
          s"V1 API: Column pruning - using ${serializableSchema.fields.length}/${fullSchema.fields.length} columns"
        )
        logger.info(s"V1 API: Required columns: ${requiredColumns.mkString(", ")}")
      }

      // Use Hadoop configuration from driver context - include both traditional Hadoop configs and IndexTables4Spark configs
      val baseHadoopProps = Map(
        "fs.defaultFS"             -> hadoopConf.get("fs.defaultFS", ""),
        "fs.s3a.access.key"        -> hadoopConf.get("fs.s3a.access.key", ""),
        "fs.s3a.secret.key"        -> hadoopConf.get("fs.s3a.secret.key", ""),
        "fs.s3a.endpoint"          -> hadoopConf.get("fs.s3a.endpoint", ""),
        "fs.s3a.path.style.access" -> hadoopConf.get("fs.s3a.path.style.access", ""),
        "fs.s3a.impl"              -> hadoopConf.get("fs.s3a.impl", ""),
        "fs.hdfs.impl"             -> hadoopConf.get("fs.hdfs.impl", ""),
        "fs.file.impl"             -> hadoopConf.get("fs.file.impl", "")
      ).filter(_._2.nonEmpty)

      // Extract tantivy4spark configurations with proper precedence
      // Precedence: read options > Spark config > Hadoop config

      // Extract from Hadoop config (lowest precedence)
      val hadoopTantivyProps = {
        import scala.jdk.CollectionConverters._
        hadoopConf
          .iterator()
          .asScala
          .filter { entry =>
            entry.getKey.startsWith("spark.indextables.") || entry.getKey.startsWith("spark.indextables.")
          }
          .map { entry =>
            val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
              entry.getKey.replace("spark.indextables.", "spark.indextables.")
            } else entry.getKey
            normalizedKey -> entry.getValue
          }
          .toMap
      }

      // Extract from Spark session config (middle precedence)
      val sparkTantivyProps =
        try
          spark.conf.getAll
            .filter {
              case (key, _) =>
                key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
            }
            .map {
              case (key, value) =>
                val normalizedKey = if (key.startsWith("spark.indextables.")) {
                  key.replace("spark.indextables.", "spark.indextables.")
                } else key
                normalizedKey -> value
            }
            .toMap
        catch {
          case _: Exception => Map.empty[String, String]
        }

      // Extract from read options (highest precedence)
      val readTantivyProps = readOptions
        .filter {
          case (key, _) =>
            key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
        }
        .map {
          case (key, value) =>
            val normalizedKey = if (key.startsWith("spark.indextables.")) {
              key.replace("spark.indextables.", "spark.indextables.")
            } else key
            normalizedKey -> value
        }

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

      // Extract readOptions to avoid serialization issues with 'this' reference
      val serializableReadOptions = readOptions

      // Create RDD from file paths using standalone object method for proper serialization
      // Now with proper filter pushdown support via PrunedFilteredScan
      spark.sparkContext.parallelize(serializableFilesWithMetadata).flatMap {
        case (filePath: String, splitMetadata: Option[IndexTables4SparkRelation.SerializableSplitMetadata]) =>
          IndexTables4SparkRelation.processFileWithCustomFilters(
            filePath,
            serializableSchema,
            hadoopConfProps,
            filters,
            Array.empty,
            None,
            splitMetadata,
            serializableReadOptions
          )
      }
    }
  }

  // Helper method to handle both Spark filters and custom IndexQuery filters
  private def buildScanWithCustomFilters(
    requiredColumns: Array[String],
    sparkFilters: Array[Filter],
    customFilters: Array[Any]
  ): RDD[org.apache.spark.sql.Row] = {
    import scala.jdk.CollectionConverters._
    import io.indextables.spark.filters.{IndexQueryFilter, IndexQueryAllFilter}

    val spark = sqlContext.sparkSession

    // Extract tantivy4spark/indextables configurations for credential propagation (same as schema method)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val tantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter { entry =>
        entry.getKey.startsWith("spark.indextables.") || entry.getKey.startsWith("spark.indextables.")
      }
      .map { entry =>
        val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
          entry.getKey.replace("spark.indextables.", "spark.indextables.")
        } else entry.getKey
        normalizedKey -> entry.getValue
      }
      .toMap

    val sparkConfigs = spark.conf.getAll
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
      }
      .map {
        case (key, value) =>
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          normalizedKey -> value
      }
      .toMap

    // Include read options (from DataFrame read API)
    val readTantivyOptions = readOptions
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
      }
      .map {
        case (key, value) =>
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          normalizedKey -> value
      }

    // Combine all sources with proper precedence to avoid duplicate key warnings
    // readOptions take highest precedence, then sparkConfigs, then hadoopConfigs
    val allConfigs = Map.newBuilder[String, String]
    allConfigs ++= tantivyConfigs
    allConfigs ++= sparkConfigs
    allConfigs ++= readTantivyOptions
    val options = new CaseInsensitiveStringMap(allConfigs.result().asJava)

    val transactionLog = TransactionLogFactory.create(new Path(path), spark, options)

    // Check if table exists by trying to get schema first
    val tableSchema = transactionLog.getSchema()
    if (tableSchema.isEmpty) {
      // Table doesn't exist - throw exception instead of returning empty results
      throw new RuntimeException(
        s"Path does not exist: $path. No transaction log found. Use spark.write to create the table first."
      )
    }

    // Get list of files from transaction log
    val files = transactionLog.listFiles()

    if (files.isEmpty) {
      // Table exists but has no data files (legitimate empty table)
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row]
    } else {
      // Extract serializable data - resolve relative paths to full paths
      // Normalize table path for tantivy4java compatibility (s3a:// -> s3://)
      val normalizedTablePath = if (path.startsWith("s3a://") || path.startsWith("s3n://")) {
        path.replaceFirst("^s3[an]://", "s3://")
      } else {
        path
      }
      val tablePath = new Path(normalizedTablePath)

      // Create serializable (path, metadata) pairs from AddAction entries
      val serializableFilesWithMetadata = files.map { addAction =>
        // Use centralized path resolution utility to avoid file:/ double-prefixing
        val resolvedPath = if (addAction.path.contains("___")) {
          // This is a flattened S3Mock path - reconstruct the S3 path directly
          val tableUri = java.net.URI.create(tablePath.toString)
          s"${tableUri.getScheme}://${tableUri.getHost}/${addAction.path}"
        } else {
          // Use PathResolutionUtils for consistent path resolution
          PathResolutionUtils.resolveSplitPathAsString(addAction.path, tablePath.toString)
        }

        // Extract serializable metadata from AddAction - required for tantivy4java operations
        val serializableMetadata = if (addAction.hasFooterOffsets && addAction.footerStartOffset.isDefined) {
          Some(IndexTables4SparkRelation.createSerializableMetadataFromAddAction(addAction))
        } else {
          throw new RuntimeException(
            s"AddAction for ${addAction.path} does not contain required footer offsets. All 'add' entries in the transaction log must contain footer offset metadata."
          )
        }

        (resolvedPath, serializableMetadata)
      }
      val fullSchema = schema

      // Apply column pruning if required columns are specified
      // IMPORTANT: Preserve the order specified by requiredColumns for proper type alignment
      val serializableSchema = if (requiredColumns.nonEmpty && !requiredColumns.sameElements(fullSchema.fieldNames)) {
        val fieldMap      = fullSchema.fields.map(field => field.name -> field).toMap
        val orderedFields = requiredColumns.flatMap(fieldName => fieldMap.get(fieldName))
        StructType(orderedFields)
      } else {
        fullSchema
      }

      if (requiredColumns.nonEmpty) {
        logger.info(
          s"V1 API: Column pruning - using ${serializableSchema.fields.length}/${fullSchema.fields.length} columns"
        )
        logger.info(s"V1 API: Required columns: ${requiredColumns.mkString(", ")}")
      }

      // Use Hadoop configuration from driver context - include both traditional Hadoop configs and IndexTables4Spark configs
      val baseHadoopProps = Map(
        "fs.defaultFS"             -> hadoopConf.get("fs.defaultFS", ""),
        "fs.s3a.access.key"        -> hadoopConf.get("fs.s3a.access.key", ""),
        "fs.s3a.secret.key"        -> hadoopConf.get("fs.s3a.secret.key", ""),
        "fs.s3a.endpoint"          -> hadoopConf.get("fs.s3a.endpoint", ""),
        "fs.s3a.path.style.access" -> hadoopConf.get("fs.s3a.path.style.access", ""),
        "fs.s3a.impl"              -> hadoopConf.get("fs.s3a.impl", ""),
        "fs.hdfs.impl"             -> hadoopConf.get("fs.hdfs.impl", ""),
        "fs.file.impl"             -> hadoopConf.get("fs.file.impl", "")
      ).filter(_._2.nonEmpty)

      // Extract tantivy4spark configurations with proper precedence
      // Precedence: read options > Spark config > Hadoop config

      // Extract from Hadoop config (lowest precedence)
      val hadoopTantivyProps = {
        import scala.jdk.CollectionConverters._
        hadoopConf
          .iterator()
          .asScala
          .filter { entry =>
            entry.getKey.startsWith("spark.indextables.") || entry.getKey.startsWith("spark.indextables.")
          }
          .map { entry =>
            val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
              entry.getKey.replace("spark.indextables.", "spark.indextables.")
            } else entry.getKey
            normalizedKey -> entry.getValue
          }
          .toMap
      }

      // Extract from Spark session config (middle precedence)
      val sparkTantivyProps =
        try
          spark.conf.getAll
            .filter {
              case (key, _) =>
                key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
            }
            .map {
              case (key, value) =>
                val normalizedKey = if (key.startsWith("spark.indextables.")) {
                  key.replace("spark.indextables.", "spark.indextables.")
                } else key
                normalizedKey -> value
            }
            .toMap
        catch {
          case _: Exception => Map.empty[String, String]
        }

      // Extract from read options (highest precedence)
      val readTantivyProps = readOptions
        .filter {
          case (key, _) =>
            key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
        }
        .map {
          case (key, value) =>
            val normalizedKey = if (key.startsWith("spark.indextables.")) {
              key.replace("spark.indextables.", "spark.indextables.")
            } else key
            normalizedKey -> value
        }

      // Merge with proper precedence: Hadoop < Spark config < read options
      val tantivyProps = hadoopTantivyProps ++ sparkTantivyProps ++ readTantivyProps

      val hadoopConfProps = baseHadoopProps ++ tantivyProps
      if (logger.isDebugEnabled) {
        logger.debug(s"V1 buildScan passing ${hadoopConfProps.size} config properties to executors")
        logger.debug(s"Sources: Hadoop(${hadoopTantivyProps.size}), Spark(${sparkTantivyProps.size}), Options(${readTantivyProps.size})")
      }

      // Combine regular Spark filters and custom filters
      val allFilters = sparkFilters.toSeq ++ customFilters.toSeq

      // Log filter pushdown for V1 API
      if (allFilters.nonEmpty) {
        logger.info(s"CatalystScan: Pushing down ${allFilters.length} total filters (${sparkFilters.length} Spark + ${customFilters.length} custom)")
        allFilters.zipWithIndex.foreach {
          case (filter, idx) =>
            logger.info(s"  Filter[$idx]: $filter (${filter.getClass.getSimpleName})")
        }
      }

      // Extract readOptions to avoid serialization issues with 'this' reference
      val serializableReadOptions = readOptions

      // Create RDD from file paths using standalone object method for proper serialization
      // Pass all filters to processFile method
      spark.sparkContext.parallelize(serializableFilesWithMetadata).flatMap {
        case (filePath: String, splitMetadata: Option[IndexTables4SparkRelation.SerializableSplitMetadata]) =>
          IndexTables4SparkRelation.processFileWithCustomFilters(
            filePath,
            serializableSchema,
            hadoopConfProps,
            sparkFilters,
            customFilters,
            None,
            splitMetadata,
            serializableReadOptions
          )
      }
    }
  }

  // CatalystScan interface - handle custom expressions like IndexQueryExpression
  override def buildScan(
    requiredColumns: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
    filters: Seq[Expression]
  ): RDD[org.apache.spark.sql.Row] = {
    import io.indextables.spark.expressions.{IndexQueryExpression, IndexQueryAllExpression}
    import io.indextables.spark.filters.{IndexQueryFilter, IndexQueryAllFilter}
    import io.indextables.spark.conversion.CatalystToSparkFilterConverter

    logger.info(s"CatalystScan.buildScan called with ${filters.length} predicates")
    filters.foreach(predicate => logger.info(s"  Predicate: $predicate (${predicate.getClass.getSimpleName})"))

    // Extract column names from Attribute objects
    val columnNames = requiredColumns.map(_.name).toArray

    // Separate IndexQuery expressions from regular expressions
    val (indexQueryExprs, regularExprs) = filters.partition {
      case _: IndexQueryExpression | _: IndexQueryAllExpression => true
      case _                                                    => false
    }

    // Convert IndexQuery expressions to custom filters
    val indexQueryFilters: Array[Any] = indexQueryExprs.flatMap {
      case indexQuery: IndexQueryExpression =>
        val columnName  = indexQuery.getColumnName.getOrElse("unknown")
        val queryString = indexQuery.getQueryString.getOrElse("")
        logger.info(s"Converting IndexQueryExpression to IndexQueryFilter: column=$columnName, query='$queryString'")
        Some(IndexQueryFilter(columnName, queryString))

      case indexQueryAll: IndexQueryAllExpression =>
        val queryString = indexQueryAll.getQueryString.getOrElse("")
        logger.info(s"Converting IndexQueryAllExpression to IndexQueryAllFilter: query='$queryString'")
        Some(IndexQueryAllFilter(queryString))

      case _ => None
    }.toArray

    // Convert regular expressions to Spark Filter objects
    val sparkFilters: Array[org.apache.spark.sql.sources.Filter] = regularExprs.flatMap { expr =>
      try
        CatalystToSparkFilterConverter.convertExpression(expr)
      catch {
        case ex: Exception =>
          logger.warn(s"Failed to convert expression to Spark filter: $expr (${expr.getClass.getSimpleName}), error: ${ex.getMessage}")
          None
      }
    }.toArray

    logger.info(
      s"Converted ${indexQueryFilters.length} IndexQuery expressions and ${sparkFilters.length} regular expressions"
    )

    // Use a specialized method that can handle both types of filters
    buildScanWithCustomFilters(columnNames, sparkFilters, indexQueryFilters)
  }
}

class IndexTables4SparkTable(
  path: String,
  schema: StructType,
  options: CaseInsensitiveStringMap,
  tablePartitioning: Array[Transform] = Array.empty)
    extends SupportsRead
    with SupportsWrite
    with org.apache.spark.sql.connector.catalog.SupportsMetadataColumns {

  private val logger         = LoggerFactory.getLogger(classOf[IndexTables4SparkTable])
  private val spark          = SparkSession.active
  private val tablePath      = new Path(path)
  private val transactionLog = TransactionLogFactory.create(tablePath, spark, options)

  // Aggressively populate cache on table initialization for faster first reads
  transactionLog.prewarmCache()

  override def name(): String = s"tantivy4spark.`$path`"

  override def schema(): StructType =
    transactionLog.getSchema().getOrElse(schema)

  override def capabilities(): util.Set[TableCapability] =
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.TRUNCATE
    ).asJava

  override def metadataColumns(): Array[org.apache.spark.sql.connector.catalog.MetadataColumn] =
    Array(new org.apache.spark.sql.connector.catalog.MetadataColumn {
      override def name(): String                                  = "_indexall"
      override def dataType(): org.apache.spark.sql.types.DataType = org.apache.spark.sql.types.StringType
      override def isNullable(): Boolean                           = true
      override def comment(): String = "Virtual column for full-text search across all fields"
    })

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    println(s"🚀 newScanBuilder called with options: ${options.asScala.keys.mkString(", ")}")
    // Create broadcast variable with proper precedence: read options > Spark config > Hadoop config
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Extract configurations from Hadoop config (lowest precedence)
    val hadoopTantivyConfigs = hadoopConf
      .iterator()
      .asScala
      .filter { entry =>
        entry.getKey.startsWith("spark.indextables.") || entry.getKey.startsWith("spark.indextables.")
      }
      .map { entry =>
        val normalizedKey = if (entry.getKey.startsWith("spark.indextables.")) {
          entry.getKey.replace("spark.indextables.", "spark.indextables.")
        } else entry.getKey
        normalizedKey -> entry.getValue
      }
      .filter(_._2 != null) // Filter out null values
      .toMap

    // Extract configurations from Spark session config (middle precedence)
    val sparkTantivyConfigs =
      try
        spark.conf.getAll
          .filter {
            case (key, _) =>
              key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
          }
          .map {
            case (key, value) =>
              val normalizedKey = if (key.startsWith("spark.indextables.")) {
                key.replace("spark.indextables.", "spark.indextables.")
              } else key
              normalizedKey -> value
          }
          .filter(_._2 != null)
          .toMap
      catch {
        case _: Exception => Map.empty[String, String]
      }

    // Extract configurations from read options (highest precedence)
    println(s"🔧 V2 Read: ALL options received: ${options.asScala.keys.mkString(", ")}")
    val readTantivyConfigs = options.asScala
      .filter {
        case (key, _) =>
          key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
      }
      .map {
        case (key, value) =>
          val normalizedKey = if (key.startsWith("spark.indextables.")) {
            key.replace("spark.indextables.", "spark.indextables.")
          } else key
          normalizedKey -> value
      }
      .filter(_._2 != null) // Filter out null values
      .toMap

    // Debug: Log all configurations being merged with values
    println(s"🔧 V2 Read: Hadoop configs: ${hadoopTantivyConfigs.keys.mkString(", ")}")
    println(s"🔧 V2 Read: Spark session configs: ${sparkTantivyConfigs.keys.mkString(", ")}")
    println(s"🔧 V2 Read: Read option configs: ${readTantivyConfigs.keys.mkString(", ")}")

    // Debug: Show specific credential keys in each source (check both cases)
    println(s"🔧 HADOOP accessKey: ${if (hadoopTantivyConfigs.contains("spark.indextables.aws.accessKey")) "SET"
      else "MISSING"}")
    println(
      s"🔧 SPARK accessKey: ${if (sparkTantivyConfigs.contains("spark.indextables.aws.accessKey")) "SET" else "MISSING"}"
    )
    println(
      s"🔧 OPTIONS accessKey: ${if (readTantivyConfigs.contains("spark.indextables.aws.accesskey")) "SET(lowercase)"
        else if (readTantivyConfigs.contains("spark.indextables.aws.accessKey")) "SET(camelCase)"
        else "MISSING"}"
    )

    // Merge with proper precedence: Hadoop < Spark config < read options
    val tantivyConfigs = hadoopTantivyConfigs ++ sparkTantivyConfigs ++ readTantivyConfigs

    // DEBUG: Print the final merged config
    println(s"🔍 FINAL MERGED CONFIG: ${tantivyConfigs.size} total configs")
    tantivyConfigs.foreach {
      case (key, value) =>
        println(s"   $key = $value")
    }

    // Validate numeric configuration values early to provide better error messages
    def validateNumericConfig(
      key: String,
      value: String,
      expectedType: String
    ): Unit = {
      println(s"🔧 Validating config: $key = '$value' (expected type: $expectedType)")
      try
        expectedType match {
          case "Long" =>
            val parsed = value.toLong
            println(s"✅ Successfully parsed $key as Long: $parsed")
          case "Int" =>
            val parsed = value.toInt
            println(s"✅ Successfully parsed $key as Int: $parsed")
          case _ => // No validation needed
        }
      catch {
        case e: NumberFormatException =>
          println(s"❌ NumberFormatException for $key: '$value' - ${e.getMessage}")
          throw e
      }
    }

    // Validate ALL configurations that might be numeric
    println(s"🔍 newScanBuilder called - validating ${tantivyConfigs.size} tantivy configs")
    tantivyConfigs.foreach {
      case (key, value) =>
        println(s"📋 Found config: $key = $value")
        key.toLowerCase match {
          case k if k.contains("maxsize") =>
            println(s"🎯 Matching maxSize config: $k")
            validateNumericConfig(key, value, "Long")
          case k if k.contains("maxconcurrentloads") =>
            println(s"🎯 Matching maxConcurrentLoads config: $k")
            validateNumericConfig(key, value, "Int")
          case k if k.contains("targetrecordspersplit") =>
            println(s"🎯 Matching targetRecordsPerSplit config: $k")
            validateNumericConfig(key, value, "Long")
          case _ =>
            println(s"⏭️ Skipping validation for: $key")
        }
    }

    // Debug: Log final configuration passed to scan builder
    logger.info(s"🔧 Passing ${tantivyConfigs.size} IndexTables4Spark configurations to scan builder")
    logger.info(s"🔧 Sources: Hadoop(${hadoopTantivyConfigs.size}), Spark(${sparkTantivyConfigs.size}), Options(${readTantivyConfigs.size})")

    new IndexTables4SparkScanBuilder(spark, transactionLog, schema(), options, tantivyConfigs)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Debug: Log existing hadoop config before modification (including normalized indextables configs)
    logger.info(s"🔧 V2 Write: Existing Hadoop config tantivy4spark properties:")
    import io.indextables.spark.util.ConfigNormalization
    val existingTantivyConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    existingTantivyConfigs.foreach {
      case (key, value) =>
        val displayValue =
          if (key.contains("secret") || key.contains("Secret") || key.contains("session")) "***" else value
        logger.info(s"  $key = $displayValue")
    }

    // Copy write options to Hadoop configuration so they're available in executors
    // Write options from info.options() should override any existing configuration
    // IMPORTANT: Create a COPY of hadoopConf to avoid polluting the shared Spark session configuration
    import scala.jdk.CollectionConverters._
    val enrichedHadoopConf = new org.apache.hadoop.conf.Configuration(hadoopConf)

    val writeOptions = info.options()
    logger.info(s"🔧 V2 Write: DataFrame options to copy:")
    val writeOptionTantivyConfigs = ConfigNormalization.extractTantivyConfigsFromOptions(writeOptions)
    writeOptionTantivyConfigs.foreach {
      case (key, value) =>
        val displayValue =
          if (key.contains("secret") || key.contains("Secret") || key.contains("session")) "***" else value
        logger.info(s"  $key = $displayValue")
    }

    // Apply normalization to ensure both spark.indextables.* and spark.indextables.* are handled
    val normalizedWriteConfigs = ConfigNormalization.extractTantivyConfigsFromOptions(writeOptions)
    normalizedWriteConfigs.foreach {
      case (key, value) =>
        enrichedHadoopConf.set(key, value)
        logger.info(
          s"🔧 V2 Write: Set Hadoop config from write options: $key = ${if (key.contains("secret") || key.contains("Secret") || key.contains("session")) "***"
            else value}"
        )
    }

    // Debug: Log final hadoop config after modification (including normalized indextables configs)
    logger.info(s"🔧 V2 Write: Final Hadoop config tantivy4spark properties:")
    val finalTantivyConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(enrichedHadoopConf)
    finalTantivyConfigs.foreach {
      case (key, value) =>
        val displayValue =
          if (key.contains("secret") || key.contains("Secret") || key.contains("session")) "***" else value
        logger.info(s"  $key = $displayValue")
    }

    // Inject partition information into write options for V2 compatibility
    val enhancedOptions = if (tablePartitioning.nonEmpty) {
      // Extract partition column names from Transform objects
      val partitionColumnNames = tablePartitioning.map { transform =>
        // For identity transforms, extract the column name
        transform match {
          case identityTransform =>
            // Access the field reference and get the name
            try {
              // Use reflection to get the field reference from the identity transform
              val field = identityTransform.getClass.getDeclaredField("ref")
              field.setAccessible(true)
              val fieldRef = field.get(identityTransform)
              // Get the field names from the reference
              val fieldNames = fieldRef.getClass.getDeclaredField("fieldNames")
              fieldNames.setAccessible(true)
              val names = fieldNames.get(fieldRef).asInstanceOf[Array[String]]
              names.head // Take the first (and likely only) field name
            } catch {
              case _: Exception =>
                // Fallback: try to extract from toString
                val transformStr = transform.toString
                if (transformStr.contains("identity(") && transformStr.contains(")")) {
                  transformStr.substring(transformStr.indexOf("identity(") + 9, transformStr.lastIndexOf(")"))
                } else {
                  "unknown_column"
                }
            }
        }
      }

      logger.info(s"V2 Write: Injecting partition columns into options: ${partitionColumnNames.mkString(", ")}")

      // Create enhanced options with partition information
      import scala.jdk.CollectionConverters._
      val optionsMap = options.asScala.toMap

      // Serialize partition columns as JSON array (same format as V1)
      import com.fasterxml.jackson.databind.ObjectMapper
      import com.fasterxml.jackson.module.scala.DefaultScalaModule
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val partitionColumnsJson = mapper.writeValueAsString(partitionColumnNames.toArray)

      val enhancedOptionsMap = optionsMap + ("__partition_columns" -> partitionColumnsJson)
      new org.apache.spark.sql.util.CaseInsensitiveStringMap(enhancedOptionsMap.asJava)
    } else {
      options
    }

    new IndexTables4SparkWriteBuilder(transactionLog, tablePath, info, enhancedOptions, enrichedHadoopConf)
  }

  override def partitioning(): Array[org.apache.spark.sql.connector.expressions.Transform] =
    if (tablePartitioning.nonEmpty) {
      logger.info(s"Table partitioned by: ${tablePartitioning.mkString(", ")}")
      tablePartitioning
    } else {
      // Fallback: try to read from transaction log if it exists
      try {
        val partitionColumns = transactionLog.getPartitionColumns()
        if (partitionColumns.nonEmpty) {
          logger.info(s"Table partitioned by (from transaction log): ${partitionColumns.mkString(", ")}")
          // Convert partition column names to identity transforms
          partitionColumns.map(col => org.apache.spark.sql.connector.expressions.Expressions.identity(col)).toArray
        } else {
          logger.info("Table is not partitioned")
          Array.empty
        }
      } catch {
        case e: Exception =>
          logger.warn(s"Could not read partition columns: ${e.getMessage}")
          Array.empty
      }
    }

}

class IndexTables4SparkTableProvider extends org.apache.spark.sql.connector.catalog.TableProvider {

  /**
   * Extracts paths from options, supporting both direct path parameters and multiple paths. Handles paths from
   * load("path") calls as well as .option("path", "value") and multiple paths.
   */
  protected def getPaths(options: CaseInsensitiveStringMap): Seq[String] = {
    val paths = Option(options.get("paths")).map(pathStr => parsePathsFromJson(pathStr)).getOrElse(Seq.empty)

    paths ++ Option(options.get("path")).toSeq
  }

  private def parsePathsFromJson(pathStr: String): Seq[String] =
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

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val paths = getPaths(options)
    if (paths.isEmpty) {
      throw new IllegalArgumentException(
        "Path is required. Use load(\"path\") or .option(\"path\", \"value\").load()"
      )
    }

    val spark      = SparkSession.active
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Extract configurations with proper precedence hierarchy: Hadoop < Spark config < read options
    val hadoopTantivyConfigs = {
      import io.indextables.spark.util.ConfigNormalization
      ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    }

    val sparkTantivyConfigs =
      try {
        import io.indextables.spark.util.ConfigNormalization
        ConfigNormalization.extractTantivyConfigsFromSpark(spark)
      } catch {
        case _: Exception => Map.empty[String, String]
      }

    val readTantivyConfigs = {
      import io.indextables.spark.util.ConfigNormalization
      ConfigNormalization.extractTantivyConfigsFromOptions(options)
    }

    // Merge with proper precedence: Hadoop < Spark config < read options
    val mergedTantivyConfigs = hadoopTantivyConfigs ++ sparkTantivyConfigs ++ readTantivyConfigs

    // Create a serializable map first, then convert to CaseInsensitiveStringMap
    // This avoids serialization issues when the TransactionLog is used in distributed contexts
    import scala.jdk.CollectionConverters._
    val mergedConfigMap = options.asScala.toMap ++ mergedTantivyConfigs
    val mergedOptions   = new CaseInsensitiveStringMap(mergedConfigMap.asJava)

    val transactionLog = TransactionLogFactory.create(new Path(paths.head), spark, mergedOptions)

    transactionLog.getSchema().getOrElse {
      throw new RuntimeException(
        s"Path does not exist: ${paths.head}. No transaction log found. Use spark.write to create the table first."
      )
    }
  }

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): org.apache.spark.sql.connector.catalog.Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val paths   = getPaths(options)

    if (paths.isEmpty) {
      throw new IllegalArgumentException(
        "Path is required. Use load(\"path\") or .option(\"path\", \"value\").load()"
      )
    }

    val spark      = SparkSession.active
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Apply the same configuration hierarchy as inferSchema: Hadoop < Spark config < table properties
    val hadoopTantivyConfigs = {
      import io.indextables.spark.util.ConfigNormalization
      ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
    }

    val sparkTantivyConfigs =
      try {
        import io.indextables.spark.util.ConfigNormalization
        ConfigNormalization.extractTantivyConfigsFromSpark(spark)
      } catch {
        case _: Exception => Map.empty[String, String]
      }

    val tableTantivyConfigs = {
      import io.indextables.spark.util.ConfigNormalization
      ConfigNormalization.extractTantivyConfigsFromOptions(options)
    }

    // Merge with proper precedence: Hadoop < Spark config < table properties
    val mergedTantivyConfigs = hadoopTantivyConfigs ++ sparkTantivyConfigs ++ tableTantivyConfigs

    // Create a serializable map first, then convert to CaseInsensitiveStringMap
    import scala.jdk.CollectionConverters._
    val mergedConfigMap = options.asScala.toMap ++ mergedTantivyConfigs
    val mergedOptions   = new CaseInsensitiveStringMap(mergedConfigMap.asJava)

    // Use the first path as the primary table path (support for multiple paths can be added later)
    new IndexTables4SparkTable(paths.head, schema, mergedOptions, partitioning)
  }

  override def supportsExternalMetadata(): Boolean = true
}
