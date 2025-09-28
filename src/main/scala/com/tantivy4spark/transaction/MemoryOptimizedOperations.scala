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

package com.tantivy4spark.transaction

import com.tantivy4spark.io.CloudStorageProvider
import com.tantivy4spark.util.JsonUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.collection.mutable
import scala.util.{Failure, Success, Try, Using}
import java.util.concurrent.atomic.AtomicLong

/**
 * Memory-optimized operations for transaction log processing.
 * Implements streaming, columnar processing, and external sorting for large datasets.
 */
class MemoryOptimizedOperations(
    transactionLogPath: Path,
    cloudProvider: CloudStorageProvider,
    spark: SparkSession
) {

  private val logger = LoggerFactory.getLogger(classOf[MemoryOptimizedOperations])

  import spark.implicits._

  // Configuration
  private val memoryThreshold = 512 * 1024 * 1024L // 512MB threshold for external sorting
  private val bufferSize = 64 * 1024 // 64KB buffer for streaming operations
  private val batchSize = 10000 // Records per batch for streaming
  private val tempDirectory = System.getProperty("spark.tantivy4spark.indexWriter.tempDirectoryPath",
                                                  System.getProperty("java.io.tmpdir"))

  // Note: Dataset-based reconstruction is not currently used.
  // The ParallelTransactionLogOperations.reconstructStateParallel is used instead.
  // Keeping this commented for potential future use with Spark SQL optimization.
  /*
  def reconstructState(actions: Dataset[Action]): Dataset[FileState] = {
    // Would require conversion between Row and Action types
    // Implementation deferred until needed
    ???
  }
  */

  /**
   * Create checkpoint with streaming to avoid memory issues for large tables
   */
  def createStreamingCheckpoint(
      version: Long,
      actionsIterator: Iterator[Action]
  ): CheckpointInfo = {

    val checkpointPath = new Path(transactionLogPath, f"$version%020d.checkpoint.json")
    val tempFile = Files.createTempFile(Paths.get(tempDirectory), "checkpoint_", ".tmp")
    val totalActions = new AtomicLong(0)
    val numFiles = new AtomicLong(0)

    try {
      // Write to temporary file first
      Using.resource(new BufferedWriter(new FileWriter(tempFile.toFile), bufferSize)) { writer =>
        actionsIterator.grouped(batchSize).foreach { batch =>
          batch.foreach { action =>
            val wrappedAction = wrapAction(action)
            writer.write(JsonUtil.mapper.writeValueAsString(wrappedAction))
            writer.newLine()

            totalActions.incrementAndGet()
            if (action.isInstanceOf[AddAction]) {
              numFiles.incrementAndGet()
            }
          }
          writer.flush() // Flush after each batch
        }
      }

      // Upload the temporary file to cloud storage
      val fileSize = Files.size(tempFile)
      Using.resource(Files.newInputStream(tempFile)) { inputStream =>
        cloudProvider.writeFileFromStream(checkpointPath.toString, inputStream, Some(fileSize))
      }

      CheckpointInfo(
        version = version,
        size = totalActions.get(),
        sizeInBytes = fileSize,
        numFiles = numFiles.get(),
        createdTime = System.currentTimeMillis()
      )

    } finally {
      // Clean up temporary file
      Files.deleteIfExists(tempFile)
    }
  }

  /**
   * Memory-efficient merge operation using external sorting for large datasets
   */
  def mergeSplitsOptimized(splits: Seq[SplitInfo]): MergeResult = {
    val totalSize = splits.map(_.sizeBytes).sum

    if (totalSize > memoryThreshold) {
      logger.info(s"Using external merge sort for ${splits.size} splits (${totalSize / 1024 / 1024}MB)")
      externalMergeSort(splits)
    } else {
      logger.info(s"Using in-memory merge for ${splits.size} splits (${totalSize / 1024 / 1024}MB)")
      inMemoryMerge(splits)
    }
  }

  /**
   * External merge sort for very large datasets
   */
  private def externalMergeSort(splits: Seq[SplitInfo]): MergeResult = {
    val tempFiles = mutable.ListBuffer[File]()
    val recordsProcessed = new AtomicLong(0)

    try {
      // Phase 1: Sort each split and write to temp files
      val sortedTempFiles = splits.map { split =>
        val tempFile = Files.createTempFile(Paths.get(tempDirectory), s"merge_${split.id}_", ".tmp").toFile
        tempFiles += tempFile

        Using.resource(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile), bufferSize))) { out =>
          val records = readSplitRecords(split).sortBy(_.sortKey)
          records.foreach { record =>
            writeRecord(out, record)
            recordsProcessed.incrementAndGet()
          }
        }

        SortedTempFile(tempFile, getFileRecordCount(tempFile))
      }

      // Phase 2: K-way merge of sorted files
      val mergedFile = Files.createTempFile(Paths.get(tempDirectory), "merged_", ".tmp").toFile
      tempFiles += mergedFile

      kWayMerge(sortedTempFiles, mergedFile)

      // Phase 3: Upload merged result
      val resultPath = new Path(transactionLogPath.getParent, s"merged_${System.currentTimeMillis()}.split")
      uploadFile(mergedFile, resultPath)

      MergeResult(
        outputPath = resultPath.toString,
        recordCount = recordsProcessed.get(),
        sizeBytes = mergedFile.length(),
        success = true
      )

    } finally {
      // Clean up all temp files
      tempFiles.foreach(_.delete())
    }
  }

  /**
   * In-memory merge for smaller datasets
   */
  private def inMemoryMerge(splits: Seq[SplitInfo]): MergeResult = {
    // Collect all records in memory
    val allRecords = splits.flatMap(readSplitRecords)

    // Sort by key
    val sortedRecords = allRecords.sortBy(_.sortKey)

    // Write to output
    val outputPath = new Path(transactionLogPath.getParent, s"merged_${System.currentTimeMillis()}.split")
    val tempFile = Files.createTempFile(Paths.get(tempDirectory), "inmem_merge_", ".tmp").toFile

    try {
      Using.resource(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile), bufferSize))) { out =>
        sortedRecords.foreach(record => writeRecord(out, record))
      }

      uploadFile(tempFile, outputPath)

      MergeResult(
        outputPath = outputPath.toString,
        recordCount = sortedRecords.size,
        sizeBytes = tempFile.length(),
        success = true
      )

    } finally {
      tempFile.delete()
    }
  }

  /**
   * K-way merge of sorted files
   */
  private def kWayMerge(sortedFiles: Seq[SortedTempFile], outputFile: File): Unit = {
    // Create readers for each file
    val readers = sortedFiles.map { file =>
      new BufferedRecordReader(file.file, bufferSize)
    }

    Using.resource(new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile), bufferSize))) { out =>
      // Priority queue for k-way merge
      implicit val ordering: Ordering[RecordWithReader] = Ordering.by[RecordWithReader, String](_.record.sortKey).reverse
      val pq = mutable.PriorityQueue[RecordWithReader]()

      // Initialize with first record from each reader
      readers.zipWithIndex.foreach { case (reader, idx) =>
        reader.nextRecord().foreach { record =>
          pq.enqueue(RecordWithReader(record, idx))
        }
      }

      // Merge process
      while (pq.nonEmpty) {
        val RecordWithReader(record, readerIdx) = pq.dequeue()
        writeRecord(out, record)

        // Get next record from the same reader
        readers(readerIdx).nextRecord().foreach { nextRecord =>
          pq.enqueue(RecordWithReader(nextRecord, readerIdx))
        }
      }
    }

    // Close all readers
    readers.foreach(_.close())
  }

  /**
   * Streaming action processor for memory-efficient processing
   */
  def processActionsStreaming[T](
      versions: Seq[Long],
      processor: Action => Option[T]
  ): Iterator[T] = {

    new Iterator[T] {
      private var currentVersionIdx = 0
      private var currentActions: Iterator[Action] = Iterator.empty
      private var nextValue: Option[T] = None

      override def hasNext: Boolean = {
        if (nextValue.isDefined) return true

        while (currentVersionIdx < versions.length || currentActions.hasNext) {
          if (currentActions.hasNext) {
            processor(currentActions.next()) match {
              case Some(value) =>
                nextValue = Some(value)
                return true
              case None => // Continue to next action
            }
          } else if (currentVersionIdx < versions.length) {
            // Load next version
            currentActions = readVersionStreaming(versions(currentVersionIdx))
            currentVersionIdx += 1
          }
        }

        false
      }

      override def next(): T = {
        if (!hasNext) throw new NoSuchElementException
        val value = nextValue.get
        nextValue = None
        value
      }
    }
  }

  /**
   * Read version actions in streaming fashion
   */
  private def readVersionStreaming(version: Long): Iterator[Action] = {
    val versionFile = new Path(transactionLogPath, f"$version%020d.json")

    if (!cloudProvider.exists(versionFile.toString)) {
      return Iterator.empty
    }

    val inputStream = cloudProvider.openInputStream(versionFile.toString)
    val reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"), bufferSize)

    new Iterator[Action] {
      private var nextLine: Option[String] = Option(reader.readLine())

      override def hasNext: Boolean = nextLine.isDefined

      override def next(): Action = {
        if (!hasNext) throw new NoSuchElementException

        val line = nextLine.get
        nextLine = Option(reader.readLine())

        if (!hasNext) {
          reader.close()
          inputStream.close()
        }

        parseAction(line)
      }
    }
  }

  // Helper methods

  private def getOptimalPartitions(actions: Dataset[_]): Int = {
    val defaultParallelism = spark.sparkContext.defaultParallelism
    val dataSize = actions.rdd.partitions.length
    Math.min(Math.max(dataSize, defaultParallelism), 200)
  }

  private def wrapAction(action: Action): Map[String, Any] = {
    action match {
      case metadata: MetadataAction => Map("metaData" -> metadata)
      case add: AddAction => Map("add" -> add)
      case remove: RemoveAction => Map("remove" -> remove)
      case skip: SkipAction => Map("mergeskip" -> skip)
    }
  }

  private def parseAction(line: String): Action = {
    val jsonNode = JsonUtil.mapper.readTree(line)

    if (jsonNode.has("metaData")) {
      JsonUtil.mapper.readValue(jsonNode.get("metaData").toString, classOf[MetadataAction])
    } else if (jsonNode.has("add")) {
      JsonUtil.mapper.readValue(jsonNode.get("add").toString, classOf[AddAction])
    } else if (jsonNode.has("remove")) {
      JsonUtil.mapper.readValue(jsonNode.get("remove").toString, classOf[RemoveAction])
    } else if (jsonNode.has("mergeskip")) {
      JsonUtil.mapper.readValue(jsonNode.get("mergeskip").toString, classOf[SkipAction])
    } else {
      throw new IllegalArgumentException(s"Unknown action type: $line")
    }
  }

  private def readSplitRecords(split: SplitInfo): Seq[MergeRecord] = {
    // Simplified implementation - would read actual split data
    Seq.empty
  }

  private def writeRecord(out: DataOutputStream, record: MergeRecord): Unit = {
    out.writeUTF(record.key)
    out.writeLong(record.timestamp)
    out.writeInt(record.data.length)
    out.write(record.data)
  }

  private def getFileRecordCount(file: File): Long = {
    // Count records in file
    0L
  }

  private def uploadFile(localFile: File, remotePath: Path): Unit = {
    Using.resource(Files.newInputStream(localFile.toPath)) { inputStream =>
      cloudProvider.writeFileFromStream(remotePath.toString, inputStream, Some(localFile.length()))
    }
  }
}

/**
 * In-memory log replay for state reconstruction
 */
class InMemoryLogReplay {
  private val activeFiles = mutable.HashMap[String, AddAction]()
  private var latestMetadata: Option[MetadataAction] = None

  def append(actions: Iterator[Action]): Unit = {
    actions.foreach {
      case add: AddAction =>
        activeFiles(add.path) = add
      case remove: RemoveAction =>
        activeFiles.remove(remove.path)
      case metadata: MetadataAction =>
        latestMetadata = Some(metadata)
      case _ => // Ignore other actions
    }
  }

  def getActiveFiles: Seq[FileState] = {
    activeFiles.values.map { add =>
      FileState(
        path = add.path,
        size = add.size,
        modificationTime = add.modificationTime,
        dataChange = add.dataChange,
        partitionValues = add.partitionValues
      )
    }.toSeq
  }

  def getMetadata: Option[MetadataAction] = latestMetadata
}

/**
 * Buffered record reader for efficient file reading
 */
class BufferedRecordReader(file: File, bufferSize: Int) extends Closeable {
  private val input = new DataInputStream(new BufferedInputStream(new FileInputStream(file), bufferSize))
  private var nextRec: Option[MergeRecord] = readNextRecord()

  def nextRecord(): Option[MergeRecord] = {
    val current = nextRec
    nextRec = readNextRecord()
    current
  }

  private def readNextRecord(): Option[MergeRecord] = {
    Try {
      val key = input.readUTF()
      val timestamp = input.readLong()
      val dataLen = input.readInt()
      val data = new Array[Byte](dataLen)
      input.readFully(data)
      MergeRecord(key, timestamp, data)
    }.toOption
  }

  override def close(): Unit = input.close()
}

// Data structures

case class FileState(
  path: String,
  size: Long,
  modificationTime: Long,
  dataChange: Boolean,
  partitionValues: Map[String, String]
)

case class SplitInfo(
  id: String,
  path: String,
  sizeBytes: Long
)

case class MergeResult(
  outputPath: String,
  recordCount: Long,
  sizeBytes: Long,
  success: Boolean
)

case class MergeRecord(
  key: String,
  timestamp: Long,
  data: Array[Byte]
) {
  def sortKey: String = s"$key-$timestamp"
}

case class SortedTempFile(
  file: File,
  recordCount: Long
)

case class RecordWithReader(
  record: MergeRecord,
  readerIndex: Int
)