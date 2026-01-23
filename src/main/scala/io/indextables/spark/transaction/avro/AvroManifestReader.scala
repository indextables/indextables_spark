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

package io.indextables.spark.transaction.avro

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.AddAction

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, InputStream}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * Reader for Avro manifest files containing FileEntry records.
 *
 * This reader supports:
 *   - Reading single manifest files
 *   - Reading multiple manifests in parallel
 *   - Filtering by addedAtVersion for streaming reads
 *   - Converting FileEntry records to AddAction for compatibility
 */
class AvroManifestReader(cloudProvider: CloudStorageProvider) {

  private val log = LoggerFactory.getLogger(getClass)

  // Use shared thread pool from companion object to avoid creating threads on every parallel read
  private implicit val sharedExecutionContext: ExecutionContext = AvroManifestReader.sharedExecutionContext

  /**
   * Read all file entries from a single Avro manifest file.
   *
   * @param manifestPath
   *   Full path to the manifest file (e.g., "s3://bucket/table/_transaction_log/state-v.../manifest-xxx.avro")
   * @return
   *   Sequence of FileEntry records
   */
  def readManifest(manifestPath: String): Seq[FileEntry] = {
    log.debug(s"Reading Avro manifest: $manifestPath")
    val startTime = System.currentTimeMillis()

    val entries = Try {
      val inputStream = cloudProvider.openInputStream(manifestPath)
      try {
        readFromStream(inputStream)
      } finally {
        inputStream.close()
      }
    } match {
      case Success(result) => result
      case Failure(e) =>
        log.error(s"Failed to read Avro manifest: $manifestPath", e)
        throw new RuntimeException(s"Failed to read Avro manifest: $manifestPath", e)
    }

    val duration = System.currentTimeMillis() - startTime
    log.debug(s"Read ${entries.size} entries from $manifestPath in ${duration}ms")
    entries
  }

  /**
   * Read file entries from an input stream.
   *
   * @param inputStream
   *   Input stream containing Avro data
   * @return
   *   Sequence of FileEntry records
   */
  def readFromStream(inputStream: InputStream): Seq[FileEntry] = {
    val reader    = new GenericDatumReader[GenericRecord](AvroSchemas.FILE_ENTRY_SCHEMA)
    val entries   = ArrayBuffer[FileEntry]()
    var dataStream: DataFileStream[GenericRecord] = null

    try {
      dataStream = new DataFileStream[GenericRecord](inputStream, reader)
      while (dataStream.hasNext) {
        val record = dataStream.next()
        entries += AvroSchemas.toFileEntry(record)
      }
      entries.toSeq
    } finally {
      if (dataStream != null) {
        dataStream.close()
      }
    }
  }

  /**
   * Read file entries from a byte array.
   *
   * @param data
   *   Byte array containing Avro data
   * @return
   *   Sequence of FileEntry records
   */
  def readFromBytes(data: Array[Byte]): Seq[FileEntry] = {
    readFromStream(new ByteArrayInputStream(data))
  }

  /**
   * Read file entries from multiple manifest files in parallel.
   *
   * @param manifestPaths
   *   Paths to manifest files
   * @param parallelism
   *   Maximum number of parallel reads (default: 8, ignored - uses shared thread pool)
   * @return
   *   Combined sequence of all file entries
   */
  def readManifestsParallel(
      manifestPaths: Seq[String],
      parallelism: Int = StateConfig.READ_PARALLELISM_DEFAULT): Seq[FileEntry] = {
    if (manifestPaths.isEmpty) {
      return Seq.empty
    }

    if (manifestPaths.size == 1) {
      return readManifest(manifestPaths.head)
    }

    log.debug(s"Reading ${manifestPaths.size} manifests in parallel (using shared thread pool)")
    val startTime = System.currentTimeMillis()

    // Use shared thread pool from companion object - avoids thread creation overhead on every call
    val futures = manifestPaths.map { path =>
      Future {
        readManifest(path)
      }(sharedExecutionContext)
    }

    val results = Await.result(Future.sequence(futures)(implicitly, sharedExecutionContext), 5.minutes)
    val allEntries = results.flatten

    val duration = System.currentTimeMillis() - startTime
    log.info(
      s"Read ${allEntries.size} entries from ${manifestPaths.size} manifests in ${duration}ms " +
        s"(avg ${duration / manifestPaths.size}ms per manifest)")

    allEntries
  }

  /**
   * Read file entries filtered by addedAtVersion (for streaming reads).
   *
   * @param manifestPath
   *   Path to manifest file
   * @param sinceVersion
   *   Only return entries with addedAtVersion > sinceVersion
   * @return
   *   Filtered sequence of FileEntry records
   */
  def readManifestSinceVersion(manifestPath: String, sinceVersion: Long): Seq[FileEntry] = {
    readManifest(manifestPath).filter(_.addedAtVersion > sinceVersion)
  }

  /**
   * Read multiple manifests and filter by version range.
   *
   * For efficiency, only reads manifests whose version bounds overlap with the requested range.
   *
   * @param manifests
   *   ManifestInfo records to read
   * @param stateDir
   *   Base directory containing manifests
   * @param sinceVersion
   *   Only return entries with addedAtVersion > sinceVersion
   * @param parallelism
   *   Maximum number of parallel reads
   * @return
   *   Filtered sequence of FileEntry records
   */
  def readManifestsSinceVersion(
      manifests: Seq[ManifestInfo],
      stateDir: String,
      sinceVersion: Long,
      parallelism: Int = StateConfig.READ_PARALLELISM_DEFAULT): Seq[FileEntry] = {

    // Filter manifests by version bounds - skip manifests that can't contain new entries
    val relevantManifests = manifests.filter(_.maxAddedAtVersion > sinceVersion)

    if (relevantManifests.isEmpty) {
      log.debug(s"No manifests contain entries with version > $sinceVersion")
      return Seq.empty
    }

    log.debug(
      s"Version filtering: ${manifests.size} manifests -> ${relevantManifests.size} " +
        s"(filtered ${manifests.size - relevantManifests.size} by version bounds)")

    val paths = relevantManifests.map(m => s"$stateDir/${m.path}")
    readManifestsParallel(paths, parallelism).filter(_.addedAtVersion > sinceVersion)
  }

  /**
   * Convert a FileEntry to an AddAction for compatibility with existing code.
   *
   * @param entry
   *   FileEntry to convert
   * @param schemaRegistry
   *   Optional schema registry for restoring docMappingJson from docMappingRef
   * @return
   *   AddAction representation
   */
  def toAddAction(entry: FileEntry, schemaRegistry: Map[String, String] = Map.empty): AddAction = {
    FileEntry.toAddAction(entry, schemaRegistry)
  }

  /**
   * Convert multiple FileEntries to AddActions.
   *
   * @param entries
   *   FileEntries to convert
   * @param schemaRegistry
   *   Optional schema registry for restoring docMappingJson from docMappingRef
   * @return
   *   AddAction representations
   */
  def toAddActions(entries: Seq[FileEntry], schemaRegistry: Map[String, String] = Map.empty): Seq[AddAction] = {
    entries.map(e => FileEntry.toAddAction(e, schemaRegistry))
  }
}

object AvroManifestReader {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Shared thread pool for parallel manifest reads.
   * Using a cached thread pool allows threads to be reused across reads while
   * still scaling up for parallel workloads. Threads are terminated after 60s of idle time.
   */
  private lazy val sharedThreadPool: java.util.concurrent.ExecutorService = {
    log.info("Initializing shared thread pool for Avro manifest parallel reads")
    java.util.concurrent.Executors.newCachedThreadPool(new java.util.concurrent.ThreadFactory {
      private val counter = new java.util.concurrent.atomic.AtomicInteger(0)
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"avro-manifest-reader-${counter.incrementAndGet()}")
        t.setDaemon(true) // Don't prevent JVM shutdown
        t
      }
    })
  }

  /**
   * Shared execution context backed by the shared thread pool.
   * This avoids creating new thread pools on every parallel read operation.
   */
  private[avro] lazy val sharedExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(sharedThreadPool)

  /**
   * Create a reader for the given cloud provider.
   *
   * @param cloudProvider
   *   Cloud storage provider for file access
   * @return
   *   AvroManifestReader instance
   */
  def apply(cloudProvider: CloudStorageProvider): AvroManifestReader = {
    new AvroManifestReader(cloudProvider)
  }
}
