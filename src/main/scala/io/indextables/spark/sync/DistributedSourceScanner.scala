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

package io.indextables.spark.sync

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.SparkSession

import io.indextables.spark.arrow.ArrowFfiBridge
import io.indextables.spark.util.CloudPathUtils
import io.indextables.tantivy4java.delta.{DeltaFileEntry, DeltaSnapshotInfo, DeltaTableReader}
import io.indextables.tantivy4java.filter.PartitionFilter
import io.indextables.tantivy4java.iceberg.{IcebergFileEntry, IcebergTableReader}
import io.indextables.tantivy4java.parquet.{ParquetFileEntry, ParquetTableReader}
import org.slf4j.LoggerFactory

/**
 * Result of a distributed source table scan. Contains file listing as an RDD (distributed across executors) plus
 * lightweight metadata collected on the driver.
 */
case class DistributedScanResult(
  filesRDD: RDD[CompanionSourceFile],
  version: Option[Long],
  partitionColumns: Seq[String],
  storageRoot: Option[String],
  sampleFilePath: Option[String],
  numDistributedParts: Int,
  schema: Option[StructType] = None,
  /**
   * True when filesRDD contains ONLY newly added files (from an incremental changeset). When true, the anti-join should
   * be skipped — all files in the RDD are known to be new.
   */
  isIncremental: Boolean = false,
  /**
   * Source file paths removed from the source table since the last sync. Companion splits that indexed these files
   * must be invalidated. Populated for Delta (via getChangesBetween) and Iceberg (via manifest set-difference).
   * Empty for full-scan results and Parquet.
   */
  removedSourcePaths: Seq[String] = Seq.empty,
  /**
   * Files already available on the driver (incremental paths only). When set, the consumer in SyncToExternalCommand
   * skips the RDD collect() call entirely. Avoids a parallelize → collect roundtrip for small incremental changesets
   * that fit comfortably in driver memory.
   */
  driverFiles: Option[Seq[CompanionSourceFile]] = None)

/**
 * Static conversion functions used in RDD closures. These MUST be in the companion object (not instance methods) to
 * avoid capturing the non-serializable DistributedSourceScanner instance in Spark closures.
 */
object DistributedSourceScanner {

  private[sync] def deltaEntryToCompanionFile(entry: DeltaFileEntry): CompanionSourceFile =
    CompanionSourceFile(
      path = entry.getPath,
      partitionValues = entry.getPartitionValues.asScala.toMap,
      size = entry.getSize
    )

  private[sync] def icebergEntryToCompanionFile(
    entry: IcebergFileEntry,
    storageRoot: Option[String]
  ): CompanionSourceFile = {
    val absolutePath    = entry.getPath
    val partitionValues = entry.getPartitionValues.asScala.toMap

    // Convert absolute paths to relative (bucket-independent for cross-region failover)
    val relativePath = storageRoot match {
      case Some(basePath) =>
        val normalizedBase = basePath.stripSuffix("/")
        val normalizedPath = absolutePath.stripSuffix("/")
        if (normalizedPath.startsWith(normalizedBase)) {
          normalizedPath.substring(normalizedBase.length).stripPrefix("/")
        } else {
          absolutePath
        }
      case None => absolutePath
    }

    // If partition values from catalog are empty, extract from Hive-style path
    val effectivePartitionValues = if (partitionValues.isEmpty && storageRoot.isDefined) {
      extractPartitionValuesFromPath(absolutePath, storageRoot.get)
    } else {
      partitionValues
    }

    CompanionSourceFile(
      path = relativePath,
      partitionValues = effectivePartitionValues,
      size = entry.getFileSizeBytes
    )
  }

  private[sync] def parquetEntryToCompanionFile(entry: ParquetFileEntry, basePath: String): CompanionSourceFile = {
    val rawPath = entry.getPath
    // Normalize: strip file: URI scheme, ensure consistent leading-slash handling.
    // The native ParquetTableReader may return local paths without leading slash (e.g. "var/folders/...")
    // while the basePath has one ("/var/folders/...").
    val absolutePath   = normalizeLocalPath(CloudPathUtils.stripFileScheme(rawPath))
    val normalizedBase = normalizeLocalPath(CloudPathUtils.stripFileScheme(basePath)).stripSuffix("/")
    val normalizedAbs  = absolutePath.stripSuffix("/")
    val relativePath = if (normalizedAbs.startsWith(normalizedBase)) {
      normalizedAbs.substring(normalizedBase.length).stripPrefix("/")
    } else {
      // The native ParquetTableReader may return S3 object keys without the scheme+bucket prefix
      // (e.g. "path/to/data/part.parquet" instead of "s3a://bucket/path/to/data/part.parquet").
      // Extract the key portion from basePath and try matching against the raw path.
      val baseKey       = ParquetDirectoryReader.extractObjectKey(basePath)
      val rawNormalized = rawPath.stripSuffix("/")
      if (baseKey.nonEmpty && rawNormalized.startsWith(baseKey)) {
        rawNormalized.substring(baseKey.length).stripPrefix("/")
      } else {
        // Last resort: use filename only
        rawPath.substring(rawPath.lastIndexOf('/') + 1)
      }
    }
    CompanionSourceFile(
      path = relativePath,
      partitionValues = entry.getPartitionValues.asScala.toMap,
      size = entry.getSize
    )
  }

  /** Ensure local filesystem paths have a leading slash for consistent comparison. */
  private[sync] def normalizeLocalPath(path: String): String =
    if (
      !path.startsWith("/") && !path.startsWith("s3://") && !path.startsWith("s3a://") &&
      !path.startsWith("abfss://") && !path.startsWith("wasbs://") && !path.startsWith("az://")
    ) {
      "/" + path
    } else {
      path
    }

  private[sync] def extractPartitionValuesFromPath(filePath: String, basePath: String): Map[String, String] = {
    val normalizedFile = filePath.stripSuffix("/")
    val normalizedBase = basePath.stripSuffix("/")
    val relative = if (normalizedFile.startsWith(normalizedBase)) {
      normalizedFile.substring(normalizedBase.length).stripPrefix("/")
    } else {
      return Map.empty
    }
    val components = relative.split("/").dropRight(1)
    components.flatMap { component =>
      val eqIdx = component.indexOf('=')
      if (eqIdx > 0) Some(component.substring(0, eqIdx) -> component.substring(eqIdx + 1))
      else None
    }.toMap
  }

  /** Parse partition values from a JSON string like {"year":"2024","month":"01"}. */
  private[sync] def parsePartitionValuesJson(
    jsonStr: String,
    mapper: com.fasterxml.jackson.databind.ObjectMapper
  ): Map[String, String] = {
    if (jsonStr == null) return Map.empty
    try {
      val node = mapper.readTree(jsonStr)
      val iter = node.fields()
      val map  = scala.collection.mutable.Map[String, String]()
      while (iter.hasNext) {
        val entry = iter.next()
        map += (entry.getKey -> entry.getValue.asText())
      }
      map.toMap
    } catch {
      case _: Exception => Map.empty
    }
  }

  /**
   * Read a Delta checkpoint part via Arrow FFI (zero-copy columnar export). Columns: 0=path (Utf8), 1=size (Int64),
   * 2=modification_time (Int64), 3=data_change (Boolean), 4=partition_values (Utf8 JSON), 5=stats (Utf8 JSON).
   */
  private val ffiLogger = LoggerFactory.getLogger("DistributedSourceScanner.ArrowFFI")

  private[sync] def readDeltaCheckpointPartArrowFfi(
    kernelPath: String,
    config: java.util.Map[String, String],
    partPath: String,
    filter: PartitionFilter,
    snapshotInfo: DeltaSnapshotInfo = null
  ): Iterator[CompanionSourceFile] = {
    val numCols = 6
    val bridge  = new ArrowFfiBridge()
    try {
      val (arrays, schemas, arrayAddrs, schemaAddrs) = bridge.allocateStructs(numCols)
      val numRows =
        try
          DeltaTableReader.readCheckpointPartArrowFfi(
            kernelPath,
            config,
            partPath,
            filter,
            snapshotInfo,
            arrayAddrs,
            schemaAddrs
          )
        catch {
          case ex: Exception =>
            arrays.foreach(a =>
              try a.close()
              catch { case _: Exception => }
            )
            schemas.foreach(s =>
              try s.close()
              catch { case _: Exception => }
            )
            throw ex
        }
      if (numRows == 0) {
        arrays.foreach(_.close())
        schemas.foreach(_.close())
        ffiLogger.debug(s"Arrow FFI: 0 rows from checkpoint part ${partPath.substring(partPath.lastIndexOf('/') + 1)}")
        return Iterator.empty
      }
      val batch = bridge.importAsColumnarBatch(arrays, schemas, numRows)
      try {
        val mapper       = io.indextables.spark.util.JsonUtil.mapper
        val results      = new scala.collection.mutable.ArrayBuffer[CompanionSourceFile](numRows)
        var i            = 0
        var loggedSample = false
        while (i < numRows) {
          val pathStr = batch.column(0).getUTF8String(i)
          if (pathStr != null) {
            val path        = pathStr.toString
            val size        = batch.column(1).getLong(i)
            val partJsonStr = batch.column(4).getUTF8String(i)
            val partitionValues =
              parsePartitionValuesJson(if (partJsonStr != null) partJsonStr.toString else null, mapper)
            // Log first file's partition values for diagnostic purposes
            if (!loggedSample) {
              ffiLogger.info(
                s"Arrow FFI sample from ${partPath.substring(partPath.lastIndexOf('/') + 1)}: " +
                  s"numRows=$numRows, path=$path, partitionValuesJson=${if (partJsonStr != null) partJsonStr.toString
                    else "null"}, " +
                  s"parsedPartitionValues=$partitionValues"
              )
              loggedSample = true
            }
            results += CompanionSourceFile(path = path, partitionValues = partitionValues, size = size)
          }
          i += 1
        }
        ffiLogger.info(
          s"Arrow FFI: ${results.size} files from checkpoint part ${partPath.substring(partPath.lastIndexOf('/') + 1)}"
        )
        results.iterator
      } finally
        batch.close()
    } finally
      bridge.close()
  }

  /**
   * Read an Iceberg manifest file via Arrow FFI (zero-copy columnar export). Columns: 0=path (Utf8), 1=file_format
   * (Utf8), 2=record_count (Int64), 3=file_size_bytes (Int64), 4=partition_values (Utf8 JSON), 5=content (Int32),
   * 6=snapshot_id (Int64).
   */
  private[sync] def readIcebergManifestArrowFfi(
    catalogName: String,
    namespace: String,
    tableName: String,
    config: java.util.Map[String, String],
    manifestPath: String,
    filter: PartitionFilter,
    storageRoot: Option[String]
  ): Iterator[CompanionSourceFile] = {
    val numCols = 7
    val bridge  = new ArrowFfiBridge()
    try {
      val (arrays, schemas, arrayAddrs, schemaAddrs) = bridge.allocateStructs(numCols)
      val numRows =
        try
          IcebergTableReader.readManifestFileArrowFfi(
            catalogName,
            namespace,
            tableName,
            config,
            manifestPath,
            filter,
            arrayAddrs,
            schemaAddrs
          )
        catch {
          case ex: Exception =>
            arrays.foreach(a =>
              try a.close()
              catch { case _: Exception => }
            )
            schemas.foreach(s =>
              try s.close()
              catch { case _: Exception => }
            )
            throw ex
        }
      if (numRows == 0) {
        arrays.foreach(_.close())
        schemas.foreach(_.close())
        return Iterator.empty
      }
      val batch = bridge.importAsColumnarBatch(arrays, schemas, numRows)
      try {
        val mapper  = io.indextables.spark.util.JsonUtil.mapper
        val results = new scala.collection.mutable.ArrayBuffer[CompanionSourceFile](numRows)
        var i       = 0
        while (i < numRows) {
          // Filter parquet-only files
          val fileFormat = batch.column(1).getUTF8String(i)
          if (fileFormat == null || fileFormat.toString.equalsIgnoreCase("parquet")) {
            val pathStr = batch.column(0).getUTF8String(i)
            if (pathStr != null) {
              val absolutePath = pathStr.toString
              val fileSize     = batch.column(3).getLong(i)
              val partJsonStr  = batch.column(4).getUTF8String(i)
              val partitionValues =
                parsePartitionValuesJson(if (partJsonStr != null) partJsonStr.toString else null, mapper)

              // Convert absolute path to relative
              val relativePath = storageRoot match {
                case Some(basePath) =>
                  val normalizedBase = basePath.stripSuffix("/")
                  val normalizedPath = absolutePath.stripSuffix("/")
                  if (normalizedPath.startsWith(normalizedBase))
                    normalizedPath.substring(normalizedBase.length).stripPrefix("/")
                  else absolutePath
                case None => absolutePath
              }

              // If partition values from catalog are empty, extract from Hive-style path
              val effectivePartitionValues = if (partitionValues.isEmpty && storageRoot.isDefined) {
                extractPartitionValuesFromPath(absolutePath, storageRoot.get)
              } else partitionValues

              results += CompanionSourceFile(
                path = relativePath,
                partitionValues = effectivePartitionValues,
                size = fileSize
              )
            }
          }
          i += 1
        }
        results.iterator
      } finally
        batch.close()
    } finally
      bridge.close()
  }
}

/**
 * Distributed file listing for companion source tables. Splits the work between a lightweight driver call (metadata,
 * checkpoint/manifest discovery) and parallelizable executor calls (reading checkpoint parts, manifest files, or
 * partition directories).
 *
 * Uses tantivy4java 0.31.0 distributed table scanner primitives:
 *   - Delta: getSnapshotInfo() + readCheckpointPart() + readPostCheckpointChanges()
 *   - Iceberg: getSnapshotInfo() + readManifestFile()
 *   - Parquet: getTableInfo() + listPartitionFiles()
 */
class DistributedSourceScanner(spark: SparkSession) {
  import DistributedSourceScanner._

  private val logger = LoggerFactory.getLogger(classOf[DistributedSourceScanner])

  private val arrowFfiEnabled: Boolean =
    spark.sparkContext.getConf
      .getOption("spark.indextables.companion.sync.arrowFfi.enabled")
      .forall(_.equalsIgnoreCase("true"))

  /**
   * Scan a Delta table using distributed checkpoint reading.
   *
   * Flow:
   *   1. getSnapshotInfo() on driver → checkpoint part paths + commit file paths + schema + partition columns 2. Build
   *      PartitionFilter from WHERE predicates using snapshot metadata (no listFiles!) 3. readPostCheckpointChanges()
   *      on driver → adds/removes after checkpoint 4. sc.parallelize(checkpointPartPaths) → flatMap(readCheckpointPart)
   *      on executors 5. Filter out removed paths, union with post-checkpoint adds 6. Map DeltaFileEntry →
   *      CompanionSourceFile
   */
  def scanDeltaTable(
    path: String,
    credentials: Map[String, String],
    partitionFilter: Option[PartitionFilter] = None,
    wherePredicates: Seq[String] = Seq.empty,
    fromVersion: Option[Long] = None
  ): DistributedScanResult = {
    val deltaKernelPath = DeltaLogReader.normalizeForDeltaKernel(path)
    val deltaConfig     = DeltaLogReader.translateCredentials(credentials)

    logger.info(
      s"Distributed Delta scan: getting snapshot info for $path" +
        partitionFilter.map(f => s" (native filter: ${f.toJson})").getOrElse("")
    )
    val snapshotInfo = DeltaTableReader.getSnapshotInfo(deltaKernelPath, deltaConfig)

    // getSnapshotInfo() returns logical partition column names (translated from physical IDs
    // for column mapping tables by tantivy4java 0.31.0).
    val snapshotPartCols = snapshotInfo.getPartitionColumns.asScala.toSeq
    val schemaJson       = snapshotInfo.getSchemaJson
    if (snapshotInfo.getColumnNameMapping != null && !snapshotInfo.getColumnNameMapping.isEmpty) {
      logger.info(s"Column mapping active: ${snapshotInfo.getColumnNameMapping.size} mapped columns")
    }
    logger.info(s"Snapshot partition columns: ${snapshotPartCols.mkString(", ")}")

    // Parse schema (using logical names from Delta schema JSON)
    val schemaOpt =
      try
        Some(DataType.fromJson(schemaJson).asInstanceOf[StructType])
      catch { case _: Exception => None }

    // True current version = checkpoint version + number of post-checkpoint commits.
    // snapshotInfo.getVersion is ONLY the checkpoint version; post-checkpoint commits
    // are tracked separately in getCommitFilePaths. Using snapshotInfo.getVersion alone
    // would cause the streaming incremental path to miss all post-checkpoint commits.
    val trueCurrentVersion: Long =
      snapshotInfo.getVersion + snapshotInfo.getCommitFilePaths.size.toLong
    assert(
      trueCurrentVersion >= snapshotInfo.getVersion,
      s"trueCurrentVersion ($trueCurrentVersion) must be >= checkpoint version (${snapshotInfo.getVersion})"
    )

    // Build PartitionFilter from WHERE predicates using snapshot metadata.
    // This avoids calling reader.partitionColumns() which triggers the blocking listFiles() call.
    val effectiveFilter = partitionFilter.orElse {
      if (wherePredicates.nonEmpty) {
        try
          if (snapshotPartCols.nonEmpty) {
            logger.info(
              s"Building PartitionFilter: wherePredicates=${wherePredicates.mkString("; ")}, " +
                s"partCols=${snapshotPartCols.mkString(",")}, schemaFields=${schemaOpt.map(_.fieldNames.mkString(",")).getOrElse("none")}"
            )
            val filter = SparkPredicateToPartitionFilter.convert(wherePredicates, snapshotPartCols, spark, schemaOpt)
            filter match {
              case Some(f) => logger.info(s"Built native PartitionFilter from snapshot metadata: ${f.toJson}")
              case None =>
                logger.warn(
                  s"PartitionFilter build returned None — WHERE predicates may reference columns " +
                    s"not in partition columns [${snapshotPartCols.mkString(",")}], or expressions are unsupported"
                )
            }
            filter
          } else None
        catch {
          case e: Exception =>
            logger.warn(s"Cannot build PartitionFilter from snapshot metadata: ${e.getMessage}")
            None
        }
      } else None
    }
    logger.info(
      s"Delta snapshot: version=${snapshotInfo.getVersion}, " +
        s"checkpointParts=${snapshotInfo.getCheckpointPartPaths.size}, " +
        s"commitFiles=${snapshotInfo.getCommitFilePaths.size}, " +
        s"effectiveFilter=${effectiveFilter.map(_.toJson).getOrElse("none")}, " +
        s"arrowFfiEnabled=$arrowFfiEnabled"
    )

    // ── Incremental fast-path ──────────────────────────────────────────────
    // When fromVersion is provided (streaming incremental cycle), use getChangesBetween()
    // to read only the commit JSON files in the version range. This skips the expensive
    // distributed checkpoint parquet reading (which reads all checkpoint parts across executors).
    //
    // Uses trueCurrentVersion (checkpoint + post-checkpoint count), NOT snapshotInfo.getVersion
    // (checkpoint only). If we used the checkpoint version here and a table had post-checkpoint
    // commits, the incremental comparison would always see "no change" and miss those commits.
    fromVersion match {
      case Some(fv) =>
        if (trueCurrentVersion == fv) {
          // No new commits since last sync — return empty incremental result.
          logger.info(s"Delta incremental: no new commits since version $fv (checkpoint=${snapshotInfo.getVersion}, postCheckpointCommits=${snapshotInfo.getCommitFilePaths.size})")
          val sc = spark.sparkContext
          return DistributedScanResult(
            filesRDD = sc.emptyRDD[CompanionSourceFile],
            version = Some(trueCurrentVersion),
            partitionColumns = snapshotPartCols,
            storageRoot = None,
            sampleFilePath = None,
            numDistributedParts = 0,
            schema = schemaOpt,
            isIncremental = true
          )
        } else {
          // Check whether the version gap is small enough for incremental commit-log reads.
          // get_changes_between reads commit JSON files serially (1 GET per file). For large
          // catch-up scenarios this is slower than a full distributed checkpoint scan.
          val maxIncrementalCommits = scala.util
            .Try(
              spark.conf.get("spark.indextables.companion.sync.maxIncrementalCommits", "100").toLong
            )
            .getOrElse(100L)
          val versionGap = trueCurrentVersion - fv
          if (versionGap > maxIncrementalCommits) {
            logger.warn(
              s"Delta incremental: version gap $versionGap exceeds maxIncrementalCommits=$maxIncrementalCommits — " +
                s"falling back to full scan (from=$fv, current=$trueCurrentVersion)"
            )
            // fall through to full scan below
          } else {
            // New commits available — read only the delta (commit JSON files from fv+1 to trueCurrentVersion).
            // This is O(delta_commits) instead of O(checkpoint_parts + delta_commits).
            logger.info(s"Delta incremental: reading changes from version $fv to $trueCurrentVersion (checkpoint=${snapshotInfo.getVersion}, postCheckpointCommits=${snapshotInfo.getCommitFilePaths.size})")
            val changes =
              DeltaTableReader.getChangesBetween(deltaKernelPath, deltaConfig, fv, trueCurrentVersion, snapshotInfo)
            val addedFiles   = changes.getAddedFiles.asScala.map(deltaEntryToCompanionFile).toSeq
            val removedPaths = changes.getRemovedPaths.asScala.toSeq
            logger.info(s"Delta incremental: ${addedFiles.size} added files, ${removedPaths.size} removed paths")
            val sc = spark.sparkContext
            return DistributedScanResult(
              filesRDD = sc.parallelize(addedFiles),
              version = Some(trueCurrentVersion),
              partitionColumns = snapshotPartCols,
              storageRoot = None,
              sampleFilePath = addedFiles.headOption.map(_.path),
              numDistributedParts = 0,
              schema = schemaOpt,
              isIncremental = true,
              removedSourcePaths = removedPaths,
              driverFiles = Some(addedFiles)
            )
          }
        }
      case None => // fall through to full scan below
    }

    // Read post-checkpoint changes on driver (small: just JSON commit files after last checkpoint).
    // Pass snapshotInfo for column mapping translation of partition values.
    val postCheckpointChanges = DeltaTableReader.readPostCheckpointChanges(
      deltaKernelPath,
      deltaConfig,
      snapshotInfo.getCommitFilePaths,
      effectiveFilter.orNull,
      snapshotInfo
    )
    val addedAfterCheckpoint = postCheckpointChanges.getAddedFiles.asScala.toSeq
    val removedPaths         = postCheckpointChanges.getRemovedPaths.asScala.toSet

    logger.info(
      s"Post-checkpoint changes: ${addedAfterCheckpoint.size} added, ${removedPaths.size} removed"
    )

    // Distributed: read checkpoint parts on executors
    val checkpointPartPaths = snapshotInfo.getCheckpointPartPaths.asScala.toSeq
    val sc                  = spark.sparkContext

    // Broadcast config, removed paths, filter, Arrow FFI flag, and snapshotInfo (Serializable) for executor use.
    // snapshotInfo carries column mapping so Rust translates physical→logical partition names.
    val broadcastConfig       = sc.broadcast((deltaKernelPath, deltaConfig))
    val broadcastRemoved      = sc.broadcast(removedPaths)
    val broadcastFilter       = sc.broadcast(effectiveFilter.orNull)
    val broadcastArrowFfi     = sc.broadcast(arrowFfiEnabled)
    val broadcastSnapshotInfo = sc.broadcast(snapshotInfo)

    val checkpointFilesRDD: RDD[CompanionSourceFile] = if (checkpointPartPaths.nonEmpty) {
      val numPartitions = math.min(checkpointPartPaths.size, sc.defaultParallelism)
      sc.parallelize(checkpointPartPaths, numPartitions)
        .flatMap { partPath =>
          val (kernelPath, config) = broadcastConfig.value
          val filter               = broadcastFilter.value
          val snapInfo             = broadcastSnapshotInfo.value
          if (broadcastArrowFfi.value) {
            readDeltaCheckpointPartArrowFfi(kernelPath, config, partPath, filter, snapInfo)
          } else {
            DeltaTableReader
              .readCheckpointPart(kernelPath, config, partPath, filter, snapInfo)
              .asScala
              .map(deltaEntryToCompanionFile)
          }
        }
        .filter(file => !broadcastRemoved.value.contains(file.path))
    } else {
      sc.emptyRDD[CompanionSourceFile]
    }

    // Convert post-checkpoint added files (driver-side collection .map, safe) and union.
    // Column mapping translation is handled by tantivy4java (readPostCheckpointChanges with snapshotInfo).
    val addedFilesRDD = if (addedAfterCheckpoint.nonEmpty) {
      sc.parallelize(addedAfterCheckpoint.map(deltaEntryToCompanionFile))
    } else {
      sc.emptyRDD[CompanionSourceFile]
    }

    val allFilesRDD = checkpointFilesRDD.union(addedFilesRDD)

    // Sample file path for schema extraction (from post-checkpoint adds or first checkpoint entry)
    val sampleFile = addedAfterCheckpoint.headOption.map(_.getPath)

    DistributedScanResult(
      filesRDD = allFilesRDD,
      version = Some(trueCurrentVersion),
      partitionColumns = snapshotPartCols,
      storageRoot = None, // Delta uses relative paths from table root
      sampleFilePath = sampleFile,
      numDistributedParts = checkpointPartPaths.size,
      schema = schemaOpt
    )
  }

  /**
   * Scan an Iceberg table using distributed manifest reading.
   *
   * Flow:
   *   1. getSnapshotInfo() on driver → manifest file paths + snapshot metadata 2. Read first manifest on driver for
   *      sampleFilePath, storageRoot, and partitionColumns 3. sc.parallelize(manifestPaths) → flatMap(readManifestFile)
   *      on executors 4. Filter parquet-only, map to CompanionSourceFile with relative paths
   */
  def scanIcebergTable(
    catalogName: String,
    namespace: String,
    tableName: String,
    icebergConfig: java.util.Map[String, String],
    snapshotId: Option[Long],
    partitionFilter: Option[PartitionFilter] = None,
    wherePredicates: Seq[String] = Seq.empty,
    fromSnapshotId: Option[Long] = None
  ): DistributedScanResult = {
    logger.info(
      s"Distributed Iceberg scan: getting snapshot info for $catalogName.$namespace.$tableName" +
        partitionFilter.map(f => s" (native filter: ${f.toJson})").getOrElse("")
    )
    // For incremental scans, always get the CURRENT snapshot (not the previously-synced snapshot).
    // fromSnapshotId is the last synced snapshot ID; snapshotId is used only for time-travel queries.
    val snapshotInfo = if (fromSnapshotId.isDefined) {
      IcebergTableReader.getSnapshotInfo(catalogName, namespace, tableName, icebergConfig)
    } else {
      snapshotId match {
        case Some(id) => IcebergTableReader.getSnapshotInfo(catalogName, namespace, tableName, icebergConfig, id)
        case None     => IcebergTableReader.getSnapshotInfo(catalogName, namespace, tableName, icebergConfig)
      }
    }

    val manifestPaths = snapshotInfo.getManifestFilePaths.asScala.toSeq
    logger.info(
      s"Iceberg snapshot: id=${snapshotInfo.getSnapshotId}, manifests=${manifestPaths.size}"
    )

    // Parse schema from snapshotInfo for propagation to incremental results.
    // Avoids a separate IcebergTableReader.readSchema() JNI call on incremental cycles.
    val icebergSchemaOpt: Option[StructType] =
      try {
        val schemaJson = snapshotInfo.getSchemaJson
        if (schemaJson != null && schemaJson.nonEmpty)
          Some(DataType.fromJson(schemaJson).asInstanceOf[StructType])
        else None
      } catch { case _: Exception => None }

    // ── Incremental fast-path ──────────────────────────────────────────────
    // When fromSnapshotId is provided (streaming incremental cycle), compute changes via
    // manifest-path set-difference. Reads entries from changed manifests only (old-only and
    // new-only), then computes file-level adds and deletes. Shared manifests are skipped
    // entirely since their contents are unchanged.
    fromSnapshotId match {
      case Some(fsnap) =>
        val currentSnapId = snapshotInfo.getSnapshotId
        if (currentSnapId == fsnap) {
          logger.info(s"Iceberg incremental: no new snapshots since $fsnap")
          val sc = spark.sparkContext
          return DistributedScanResult(
            filesRDD = sc.emptyRDD[CompanionSourceFile],
            version = Some(currentSnapId),
            partitionColumns = Seq.empty,
            storageRoot = None,
            sampleFilePath = None,
            numDistributedParts = 0,
            schema = icebergSchemaOpt,
            isIncremental = true
          )
        } else {
          logger.info(s"Iceberg incremental: reading changes since snapshot $fsnap (current=$currentSnapId)")
          val oldManifestPaths: Set[String] = {
            val oldInfo = IcebergTableReader.getSnapshotInfo(catalogName, namespace, tableName, icebergConfig, fsnap)
            oldInfo.getManifestFilePaths.asScala.toSet
          }
          val currentManifestPaths: Set[String] = snapshotInfo.getManifestFilePaths.asScala.toSet

          // Partition manifests into old-only (removed/replaced), new-only (added/replacement),
          // and shared (unchanged — skip entirely).
          val oldOnlyManifests = (oldManifestPaths -- currentManifestPaths).toSeq
          val newOnlyManifests = (currentManifestPaths -- oldManifestPaths).toSeq
          val totalChangedManifests = oldOnlyManifests.size + newOnlyManifests.size

          // Check whether each side of the manifest delta is small enough for driver-side reads.
          // Old-only and new-only are capped independently so that pure appends (0 old, N new)
          // retain the same 50-manifest threshold as before, while compaction-heavy workloads
          // (N old, 1 new) are also capped to avoid driver OOM from reading many old manifests.
          val maxIncrementalManifests = scala.util
            .Try(
              spark.conf.get("spark.indextables.companion.sync.iceberg.maxIncrementalManifests", "50").toInt
            )
            .getOrElse(50)
          if (oldOnlyManifests.size > maxIncrementalManifests || newOnlyManifests.size > maxIncrementalManifests) {
            logger.warn(
              s"Iceberg incremental: manifest count exceeds per-side limit " +
                s"(${oldOnlyManifests.size} old, ${newOnlyManifests.size} new, limit=$maxIncrementalManifests each) — " +
                s"falling back to full scan (from=$fsnap, current=$currentSnapId)"
            )
            // fall through to full scan below
          } else {
            // Filter: only Parquet data files (same as the full-scan path)
            def isParquetEntry(e: io.indextables.tantivy4java.iceberg.IcebergFileEntry): Boolean = {
              val fmt = e.getFileFormat; fmt == null || fmt.equalsIgnoreCase("parquet")
            }

            // Read entries from old-only manifests (files that WERE in the old snapshot's changed manifests).
            // If an old manifest has been garbage-collected (expire_snapshots), fall back to full scan.
            val oldOnlyEntriesOpt: Option[Seq[io.indextables.tantivy4java.iceberg.IcebergFileEntry]] = try {
              Some(oldOnlyManifests.flatMap { manifestPath =>
                IcebergTableReader
                  .readManifestFile(catalogName, namespace, tableName, icebergConfig, manifestPath)
                  .asScala
                  .filter(isParquetEntry)
              }.toSeq)
            } catch {
              case NonFatal(e) =>
                logger.warn(
                  s"Iceberg incremental: cannot read old manifest (expired/GC'd?): ${e.getMessage} — falling back to full scan"
                )
                None
            }
            oldOnlyEntriesOpt match {
              case Some(oldOnlyEntries) =>
                val oldOnlyFilePaths: Set[String] = oldOnlyEntries.map(_.getPath).toSet

                // Read entries from new-only manifests (files that ARE in the current snapshot's changed manifests).
                // Same NonFatal guard as old-only reads — a corrupt or unreadable manifest should
                // trigger a full scan fallback, not propagate to the single-call reader path.
                val newOnlyEntriesOpt: Option[Seq[io.indextables.tantivy4java.iceberg.IcebergFileEntry]] = try {
                  Some(newOnlyManifests.flatMap { manifestPath =>
                    IcebergTableReader
                      .readManifestFile(catalogName, namespace, tableName, icebergConfig, manifestPath)
                      .asScala
                      .filter(isParquetEntry)
                  }.toSeq)
                } catch {
                  case NonFatal(e) =>
                    logger.warn(
                      s"Iceberg incremental: cannot read new manifest: ${e.getMessage} — falling back to full scan"
                    )
                    None
                }
                newOnlyEntriesOpt match {
                  case Some(newOnlyEntries) =>
                    val newOnlyFilePaths: Set[String] = newOnlyEntries.map(_.getPath).toSet

                    // Set difference: added = in new but not old, deleted = in old but not new
                    val addedEntries = newOnlyEntries.filter(e => !oldOnlyFilePaths.contains(e.getPath))
                    val removedPaths = (oldOnlyFilePaths -- newOnlyFilePaths).toSeq

                    // Derive storageRoot and partCols from any available entry (added, new-only, or
                    // old-only). Old-only entries are needed for the pure-deletion case where no new
                    // entries exist, so that removed paths can be correctly relativized.
                    val anyEntry = addedEntries.headOption
                      .orElse(newOnlyEntries.headOption)
                      .orElse(oldOnlyEntries.headOption)
                    val storageRoot = anyEntry.map(e => SyncTaskExecutor.extractTableBasePath(e.getPath))
                    val partCols = anyEntry
                      .map(_.getPartitionValues.keySet.asScala.toSeq.sorted)
                      .getOrElse(Seq.empty)
                    val files = addedEntries.map(e => icebergEntryToCompanionFile(e, storageRoot)).toSeq

                    // Normalize removed paths relative to storage root (same relative-path output
                    // as added files). Apply stripFileScheme + normalizeLocalPath to handle file:
                    // scheme variations between entries from different manifests.
                    val normalizedRemovedPaths = storageRoot match {
                      case Some(basePath) =>
                        val normalizedBase = normalizeLocalPath(CloudPathUtils.stripFileScheme(basePath)).stripSuffix("/")
                        removedPaths.map { p =>
                          val normalized = normalizeLocalPath(CloudPathUtils.stripFileScheme(p)).stripSuffix("/")
                          if (normalized.startsWith(normalizedBase))
                            normalized.substring(normalizedBase.length).stripPrefix("/")
                          else p
                        }
                      case None => removedPaths
                    }

                    logger.info(
                      s"Iceberg incremental: ${files.size} added, ${normalizedRemovedPaths.size} removed " +
                        s"from ${totalChangedManifests} changed manifests (${oldOnlyManifests.size} old, ${newOnlyManifests.size} new)"
                    )
                    val sc = spark.sparkContext
                    return DistributedScanResult(
                      filesRDD = sc.parallelize(files),
                      version = Some(currentSnapId),
                      partitionColumns = partCols,
                      storageRoot = storageRoot,
                      sampleFilePath = addedEntries.headOption.orElse(newOnlyEntries.headOption).map(_.getPath),
                      numDistributedParts = 0,
                      schema = icebergSchemaOpt,
                      isIncremental = true,
                      removedSourcePaths = normalizedRemovedPaths,
                      driverFiles = Some(files)
                    )
                  case None =>
                    logger.warn("Iceberg incremental: new manifest entries unavailable — falling back to full scan")
                } // end newOnlyEntriesOpt match
              case None =>
                logger.warn("Iceberg incremental: old manifest entries unavailable — falling back to full scan")
            } // end oldOnlyEntriesOpt match
          } // end else (incremental path)
        }   // end else (currentSnapId != fsnap)
      case None => // fall through to full scan below
    }

    // Read first manifest on driver (non-FFI) to discover storageRoot, sample file path, and partition columns.
    // Cache entries to avoid redundant JNI call.
    val firstManifestEntries = if (manifestPaths.nonEmpty) {
      IcebergTableReader
        .readManifestFile(catalogName, namespace, tableName, icebergConfig, manifestPaths.head)
        .asScala
        .toSeq
    } else {
      Seq.empty
    }

    val firstEntry          = firstManifestEntries.headOption
    val computedStorageRoot = firstEntry.map(e => SyncTaskExecutor.extractTableBasePath(e.getPath))
    val sampleFilePath      = firstEntry.map(_.getPath)
    val partitionColumns    = firstEntry.map(_.getPartitionValues.keySet.asScala.toSeq.sorted).getOrElse(Seq.empty)

    // Distributed: read all manifests on executors
    val sc                = spark.sparkContext
    val broadcastConfig   = sc.broadcast((catalogName, namespace, tableName, icebergConfig))
    val broadcastRoot     = sc.broadcast(computedStorageRoot)
    val broadcastFilter   = sc.broadcast(partitionFilter.orNull)
    val broadcastArrowFfi = sc.broadcast(arrowFfiEnabled)

    val numPartitions = math.min(math.max(manifestPaths.size, 1), sc.defaultParallelism)
    val allFilesRDD: RDD[CompanionSourceFile] = sc
      .parallelize(manifestPaths, numPartitions)
      .flatMap { manifestPath =>
        val (cat, ns, tbl, config) = broadcastConfig.value
        val filter                 = broadcastFilter.value
        val root                   = broadcastRoot.value
        if (broadcastArrowFfi.value) {
          readIcebergManifestArrowFfi(cat, ns, tbl, config, manifestPath, filter, root)
        } else if (filter != null) {
          val entries = IcebergTableReader.readManifestFile(cat, ns, tbl, config, manifestPath, false, filter)
          entries.asScala
            .filter { entry =>
              val fmt = entry.getFileFormat
              fmt == null || fmt.equalsIgnoreCase("parquet")
            }
            .map(entry => icebergEntryToCompanionFile(entry, root))
            .iterator
        } else {
          val entries = IcebergTableReader.readManifestFile(cat, ns, tbl, config, manifestPath)
          entries.asScala
            .filter { entry =>
              val fmt = entry.getFileFormat
              fmt == null || fmt.equalsIgnoreCase("parquet")
            }
            .map(entry => icebergEntryToCompanionFile(entry, root))
            .iterator
        }
      }

    DistributedScanResult(
      filesRDD = allFilesRDD,
      version = Some(snapshotInfo.getSnapshotId),
      partitionColumns = partitionColumns,
      storageRoot = computedStorageRoot,
      sampleFilePath = sampleFilePath,
      numDistributedParts = manifestPaths.size
    )
  }

  /**
   * Scan a bare Parquet directory using distributed partition directory listing.
   *
   * Flow:
   *   1. getTableInfo() on driver → partition directories + root files 2. If partitioned: sc.parallelize(partitionDirs)
   *      → flatMap(listPartitionFiles) 3. If unpartitioned: sc.parallelize(rootFiles) 4. Map ParquetFileEntry →
   *      CompanionSourceFile
   */
  def scanParquetDirectory(
    path: String,
    credentials: Map[String, String],
    partitionFilter: Option[PartitionFilter] = None,
    wherePredicates: Seq[String] = Seq.empty
  ): DistributedScanResult = {
    val normalizedPath = ParquetDirectoryReader.normalizeForObjectStore(path)
    val nativeConfig   = ParquetDirectoryReader.translateCredentials(credentials)

    logger.info(
      s"Distributed Parquet scan: getting table info for $path" +
        partitionFilter.map(f => s" (native filter: ${f.toJson})").getOrElse("")
    )
    val tableInfo = partitionFilter match {
      case Some(filter) => ParquetTableReader.getTableInfo(normalizedPath, nativeConfig, filter)
      case None         => ParquetTableReader.getTableInfo(normalizedPath, nativeConfig)
    }
    logger.info(
      s"Parquet table: partitioned=${tableInfo.isPartitioned}, " +
        s"partitionDirs=${tableInfo.getPartitionDirectories.size}, " +
        s"rootFiles=${tableInfo.getRootFiles.size}, " +
        s"partitionColumns=${tableInfo.getPartitionColumns.asScala.mkString(",")}"
    )

    val sc              = spark.sparkContext
    val broadcastConfig = sc.broadcast((normalizedPath, nativeConfig))
    val broadcastFilter = sc.broadcast(partitionFilter.orNull)

    val (allFilesRDD, numParts, sampleFile) = if (tableInfo.isPartitioned) {
      val partitionDirs = tableInfo.getPartitionDirectories.asScala.toSeq
      val numPartitions = math.min(math.max(partitionDirs.size, 1), sc.defaultParallelism)

      val filesRDD: RDD[CompanionSourceFile] = sc
        .parallelize(partitionDirs, numPartitions)
        .flatMap { partDir =>
          val (normPath, config) = broadcastConfig.value
          val filter             = broadcastFilter.value
          val entries = if (filter != null) {
            ParquetTableReader.listPartitionFiles(normPath, config, partDir, filter)
          } else {
            ParquetTableReader.listPartitionFiles(normPath, config, partDir)
          }
          entries.asScala.map(entry => parquetEntryToCompanionFile(entry, normPath))
        }

      val sample = if (partitionDirs.nonEmpty) {
        val firstDirEntries = ParquetTableReader.listPartitionFiles(normalizedPath, nativeConfig, partitionDirs.head)
        if (!firstDirEntries.isEmpty) Some(firstDirEntries.get(0).getPath) else None
      } else None

      (filesRDD, partitionDirs.size, sample)
    } else {
      val rootFiles = tableInfo.getRootFiles.asScala.toSeq
      // Driver-side collection .map — safe, no serialization concern
      val filesRDD = sc.parallelize(rootFiles.map(e => parquetEntryToCompanionFile(e, normalizedPath)))
      val sample   = rootFiles.headOption.map(_.getPath)
      (filesRDD, rootFiles.size, sample)
    }

    DistributedScanResult(
      filesRDD = allFilesRDD,
      version = None,
      partitionColumns = tableInfo.getPartitionColumns.asScala.toSeq,
      storageRoot = None,
      sampleFilePath = sampleFile,
      numDistributedParts = numParts
    )
  }
}
