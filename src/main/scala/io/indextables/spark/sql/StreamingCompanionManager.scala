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

package io.indextables.spark.sql

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Manages the streaming companion sync loop for BUILD INDEXTABLES COMPANION ... WITH STREAMING.
 *
 * Runs an initial full sync followed by repeated incremental cycles at the configured poll
 * interval. Each cycle calls executeSyncInternal on the underlying command. On incremental cycles,
 * the last seen source version is passed as fromVersion (Delta) or fromSnapshot (Iceberg) so that
 * when tantivy4java adds efficient range reads, the hint is already wired up. Until then, the
 * anti-join in executeSyncInternal guarantees correctness — files already indexed are never
 * re-processed.
 *
 * Stops when:
 *   - The calling thread is interrupted (Ctrl+C, notebook cancel, sparkContext.cancelAllJobs())
 *   - sparkContext.isStopped returns true
 *   - Consecutive error limit is exceeded (MaxConsecutiveErrors)
 */
private[sql] class StreamingCompanionManager(
  command: SyncToExternalCommand,
  pollIntervalMs: Long
) {

  private val logger              = LoggerFactory.getLogger(classOf[StreamingCompanionManager])
  private val MaxConsecutiveErrors = 10

  def runStreaming(sparkSession: SparkSession): Unit = {
    logger.info(
      s"Starting streaming companion sync: source=${command.sourcePath}, " +
        s"dest=${command.destPath}, pollIntervalMs=$pollIntervalMs"
    )

    val metrics = new StreamingCompanionMetrics(sparkSession.sparkContext)

    var lastSourceVersion: Option[Long] = None
    var consecutiveErrors               = 0
    var cycle                           = 0

    while (!Thread.currentThread().isInterrupted && !sparkSession.sparkContext.isStopped) {
      cycle += 1
      val cycleStart = System.currentTimeMillis()
      logger.info(s"Streaming sync cycle $cycle starting (lastSourceVersion=$lastSourceVersion)")

      try {
        val cycleCommand = buildCycleCommand(lastSourceVersion)
        val rows         = cycleCommand.executeSyncInternal(sparkSession, cycleStart)

        consecutiveErrors = 0

        // Output schema: table_path(0), source_path(1), status(2), source_version(3),
        //   splits_created(4), splits_invalidated(5), parquet_files_indexed(6),
        //   parquet_bytes_downloaded(7), split_bytes_uploaded(8), duration_ms(9), message(10)
        val (filesIndexed, durationMs) = rows.headOption.map { row =>
          if (!row.isNullAt(3)) lastSourceVersion = Some(row.getLong(3))
          val files = if (!row.isNullAt(6)) row.getInt(6).toLong else 0L
          val dur   = if (!row.isNullAt(9)) row.getLong(9) else System.currentTimeMillis() - cycleStart
          (files, dur)
        }.getOrElse((0L, System.currentTimeMillis() - cycleStart))

        metrics.recordCycleSuccess(filesIndexed, durationMs)

        logger.info(
          s"Streaming sync cycle $cycle complete: durationMs=$durationMs, " +
            s"filesIndexed=$filesIndexed, sourceVersion=$lastSourceVersion"
        )
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          logger.info(s"Streaming sync interrupted during cycle $cycle")
          return
        case e: Exception =>
          consecutiveErrors += 1
          metrics.recordCycleError()
          logger.error(
            s"Streaming sync cycle $cycle failed " +
              s"($consecutiveErrors/$MaxConsecutiveErrors consecutive errors): ${e.getMessage}",
            e
          )
          if (consecutiveErrors >= MaxConsecutiveErrors) {
            throw new RuntimeException(
              s"Streaming companion sync aborted after $MaxConsecutiveErrors consecutive errors",
              e
            )
          }
      }

      waitForNextCycle()
    }

    logger.info("Streaming companion sync stopped")
  }

  /**
   * Build the command for one sync cycle. On incremental cycles, injects the last source version
   * as fromVersion (Delta) or fromSnapshot (Iceberg). streamingPollIntervalMs is cleared so that
   * executeSyncInternal is called directly and does not recurse into streaming dispatch.
   */
  private def buildCycleCommand(lastSourceVersion: Option[Long]): SyncToExternalCommand =
    lastSourceVersion match {
      case Some(v) if command.sourceFormat == "delta" =>
        command.copy(fromVersion = Some(v), streamingPollIntervalMs = None)
      case Some(v) if command.sourceFormat == "iceberg" =>
        command.copy(fromSnapshot = Some(v), streamingPollIntervalMs = None)
      case _ =>
        command.copy(streamingPollIntervalMs = None)
    }

  private def waitForNextCycle(): Unit =
    try {
      Thread.sleep(pollIntervalMs)
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    }
}
