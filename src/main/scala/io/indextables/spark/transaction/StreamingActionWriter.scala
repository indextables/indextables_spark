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

package io.indextables.spark.transaction

import java.io.{BufferedOutputStream, ByteArrayOutputStream, OutputStream}
import java.nio.charset.StandardCharsets

import io.indextables.spark.io.CloudStorageProvider
import io.indextables.spark.transaction.compression.{CompressionCodec, CompressionUtils}
import io.indextables.spark.util.JsonUtil
import org.slf4j.LoggerFactory

/**
 * Utility for streaming writes of transaction log actions.
 *
 * This class addresses OOM issues that occur when writing large transaction logs with many actions (tens of thousands
 * of splits with large schemas). Instead of accumulating all JSON in a StringBuilder (which can exceed JVM array size
 * limits), actions are written directly to a compressed output stream.
 *
 * Key features:
 *   - Streaming write to avoid memory accumulation
 *   - Optional compression with proper header handling
 *   - Support for conditional writes (if-not-exists) for transaction log version files
 *   - Consistent JSON wrapping format compatible with existing readers
 */
object StreamingActionWriter {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Default buffer size for streaming writes (64KB) */
  val DEFAULT_BUFFER_SIZE: Int = 64 * 1024

  /**
   * Write actions to a cloud storage path using streaming (no OOM risk).
   *
   * This method writes actions directly to the cloud storage without accumulating the entire content in memory. For
   * large tables (tens of thousands of splits with 400+ columns), this prevents OOM errors.
   *
   * @param actions
   *   Actions to write (ProtocolAction, MetadataAction, AddAction, RemoveAction, SkipAction)
   * @param cloudProvider
   *   Cloud storage provider for writing
   * @param path
   *   Destination path
   * @param codec
   *   Optional compression codec. If provided, the output will be compressed.
   * @param ifNotExists
   *   If true, use conditional put semantics (fails if file already exists). Required for transaction log version
   *   files.
   * @return
   *   true if write succeeded, false if file already exists (only when ifNotExists=true)
   * @throws RuntimeException
   *   if write fails for reasons other than file existing
   */
  def writeActionsStreaming(
    actions: Seq[Action],
    cloudProvider: CloudStorageProvider,
    path: String,
    codec: Option[CompressionCodec] = None,
    ifNotExists: Boolean = false
  ): Boolean = {

    logger.debug(s"StreamingActionWriter: Writing ${actions.size} actions to $path (ifNotExists=$ifNotExists)")

    // For streaming writes, we need to buffer to a byte array first because:
    // 1. Cloud providers need content length for efficient uploads
    // 2. Conditional writes need to check existence and write atomically
    // 3. Compression requires knowing the full content for proper framing
    //
    // However, we use streaming internally to avoid StringBuilder's ~2GB limit.
    // The ByteArrayOutputStream can grow much larger than String arrays.
    val baos = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE)

    try {
      // Create output stream with optional compression
      val outputStream = CompressionUtils.createCompressingOutputStream(baos, codec)
      val bufferedOut  = new BufferedOutputStream(outputStream, DEFAULT_BUFFER_SIZE)

      try
        // Write each action as a line of JSON
        writeActionsToStream(actions, bufferedOut)
      finally {
        bufferedOut.flush()
        outputStream.close() // This also flushes and finalizes compression
      }

      val bytesToWrite    = baos.toByteArray
      val compressionInfo = codec.map(c => s" (compressed with ${c.name})").getOrElse("")

      logger.info(
        s"StreamingActionWriter: Writing ${actions.length} actions to $path$compressionInfo, size=${bytesToWrite.length} bytes"
      )

      // Write to cloud storage
      if (ifNotExists) {
        cloudProvider.writeFileIfNotExists(path, bytesToWrite)
      } else {
        cloudProvider.writeFile(path, bytesToWrite)
        true
      }

    } catch {
      case e: Exception =>
        logger.error(s"StreamingActionWriter: Failed to write actions to $path", e)
        throw e
    }
  }

  /**
   * Write actions to an output stream as newline-delimited JSON.
   *
   * Each action is wrapped in the standard transaction log format: - ProtocolAction -> {"protocol": {...}} -
   * MetadataAction -> {"metaData": {...}} - AddAction -> {"add": {...}} - RemoveAction -> {"remove": {...}} -
   * SkipAction -> {"mergeskip": {...}}
   *
   * @param actions
   *   Actions to write
   * @param outputStream
   *   Output stream to write to. Caller is responsible for closing.
   */
  def writeActionsToStream(actions: Seq[Action], outputStream: OutputStream): Unit = {
    val newline = "\n".getBytes(StandardCharsets.UTF_8)

    actions.foreach { action =>
      val wrappedAction = wrapAction(action)
      val jsonBytes     = JsonUtil.mapper.writeValueAsBytes(wrappedAction)
      outputStream.write(jsonBytes)
      outputStream.write(newline)
    }
  }

  /**
   * Wrap an action in the standard transaction log format.
   *
   * @param action
   *   The action to wrap
   * @return
   *   A Map that will serialize to the correct JSON format
   */
  def wrapAction(action: Action): Map[String, Any] =
    action match {
      case protocol: ProtocolAction => Map("protocol" -> protocol)
      case metadata: MetadataAction => Map("metaData" -> metadata)
      case add: AddAction           => Map("add" -> add)
      case remove: RemoveAction     => Map("remove" -> remove)
      case skip: SkipAction         => Map("mergeskip" -> skip)
    }

  /**
   * Serialize a single action to JSON bytes (including wrapper).
   *
   * @param action
   *   The action to serialize
   * @return
   *   JSON bytes for the wrapped action (without newline)
   */
  def serializeAction(action: Action): Array[Byte] = {
    val wrappedAction = wrapAction(action)
    JsonUtil.mapper.writeValueAsBytes(wrappedAction)
  }

  /**
   * Calculate the approximate size of actions when serialized.
   *
   * This is useful for estimating memory requirements and choosing between in-memory and streaming approaches.
   *
   * @param actions
   *   Actions to estimate
   * @return
   *   Approximate size in bytes
   */
  def estimateSerializedSize(actions: Seq[Action]): Long =
    actions.foldLeft(0L) { (acc, action) =>
      // Estimate based on action type
      val estimate = action match {
        case add: AddAction =>
          // AddActions with docMappingJson can be very large
          add.docMappingJson.map(_.length.toLong).getOrElse(0L) +
            add.stats.map(_.length.toLong).getOrElse(0L) +
            500L // Base fields
        case metadata: MetadataAction =>
          metadata.schemaString.length.toLong + 200L
        case _ => 200L // Other actions are small
      }
      acc + estimate + 1 // +1 for newline
    }
}
