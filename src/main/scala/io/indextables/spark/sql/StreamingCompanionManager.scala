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

import org.apache.spark.sql.{Row, SparkSession}

import io.indextables.spark.util.ConfigNormalization
import org.slf4j.LoggerFactory

/**
 * Manages the streaming companion sync loop for BUILD INDEXTABLES COMPANION ... WITH STREAMING.
 *
 * Runs an initial full sync followed by repeated incremental cycles at the configured poll interval. On each
 * incremental cycle:
 *   1. A cheap version probe is attempted: Delta: 1 GET + O(k) HEAD probes, no parquet read. Iceberg: 1
 *      catalog.load_table() call, no manifest list read. Parquet: no probe available, always falls through to step 2.
 *      If the source version is unchanged since the last sync, the full sync is skipped entirely. 2. If changed (or no
 *      cheap probe available), syncFn is called with the last known source version so the underlying scanner uses an
 *      incremental changeset instead of a full anti-join.
 *
 * Stops when:
 *   - The calling thread is interrupted (Ctrl+C, notebook cancel, sparkContext.cancelAllJobs())
 *   - sparkContext.isStopped returns true
 *   - Consecutive error limit is exceeded
 *
 * Configurable via SparkSession: spark.indextables.companion.stream.maxConsecutiveErrors (default: 10)
 * spark.indextables.companion.stream.errorBackoffMultiplier (default: 2)
 * spark.indextables.companion.stream.quietPollLogInterval (default: 10)
 */
private[sql] class StreamingCompanionManager(
  command: SyncToExternalCommand,
  pollIntervalMs: Long,
  syncFn: (SparkSession, Long, Option[Long]) => Seq[Row]) {

  private val logger = LoggerFactory.getLogger(classOf[StreamingCompanionManager])

  def runStreaming(sparkSession: SparkSession): Unit = {
    val maxConsecutiveErrors = sparkSession.conf
      .getOption("spark.indextables.companion.stream.maxConsecutiveErrors")
      .map(_.toInt)
      .getOrElse(10)
    val errorBackoffMultiplier = sparkSession.conf
      .getOption("spark.indextables.companion.stream.errorBackoffMultiplier")
      .map(_.toLong)
      .getOrElse(2L)
    val quietPollLogInterval = sparkSession.conf
      .getOption("spark.indextables.companion.stream.quietPollLogInterval")
      .map(_.toInt)
      .getOrElse(10)

    val streamStart = System.currentTimeMillis()
    logger.info(
      s"[IndextablesCompanionStream] Starting streaming companion sync: " +
        s"source=${command.sourcePath} (${command.sourceFormat.toUpperCase}), " +
        s"dest=${command.destPath}, pollIntervalMs=$pollIntervalMs, " +
        s"maxConsecutiveErrors=$maxConsecutiveErrors, errorBackoffMultiplier=$errorBackoffMultiplier"
    )

    val metrics = new StreamingCompanionMetrics(sparkSession.sparkContext)

    // Pre-compute merged configs once — reused every poll cycle by cheapSourceVersion.
    // Avoids re-reading SparkConf/HadoopConf on every cycle (O(1) map lookup instead of reflection).
    val cachedBaseConfigs: Option[Map[String, String]] =
      try {
        val hadoopConf    = sparkSession.sparkContext.hadoopConfiguration
        val sparkConfigs  = ConfigNormalization.extractTantivyConfigsFromSpark(sparkSession)
        val hadoopConfigs = ConfigNormalization.extractTantivyConfigsFromHadoop(hadoopConf)
        Some(
          ConfigNormalization.mergeWithPrecedence(hadoopConfigs, sparkConfigs) +
            ("spark.indextables.databricks.credential.operation" -> "PATH_READ_WRITE")
        )
      } catch { case _: Exception => None }

    // Attempt to resume from the last committed source version in the companion transaction log.
    // On a clean first run this returns None and a full sync is performed.
    // On restart after an outage this skips the full anti-join and resumes incrementally.
    val resumedVersion = command.readLastSyncedVersionFromLog(sparkSession)
    if (resumedVersion.isDefined) {
      logger.info(
        s"[IndextablesCompanionStream] Resuming from lastSyncedVersion=${resumedVersion.get} " +
          s"(read from companion transaction log)"
      )
    }

    var lastSyncedVersion: Option[Long]  = resumedVersion
    var lastCurrentVersion: Option[Long] = None
    var consecutiveErrors                = 0
    var cycle                            = 0
    var pollsWithNoChanges               = 0
    var lastSyncCompleteMs               = 0L
    var lastSyncDurationMs               = 0L
    var lastFilesIndexed                 = 0L
    var lastSplitsCreated                = 0L

    while (!Thread.currentThread().isInterrupted && !sparkSession.sparkContext.isStopped) {
      cycle += 1
      val cycleStart = System.currentTimeMillis()

      // ── Cheap version pre-check ──────────────────────────────────────────────────────────────
      // Delta:   1 GET (_last_checkpoint) + O(k) HEAD probes, no parquet read.
      // Iceberg: 1 catalog.load_table() call, no manifest list read.
      // Parquet / unknown: returns None — fall through to executeSyncInternal.
      // If the probe throws for any reason, returns None and falls through to full sync.
      val cheapVersion     = command.cheapSourceVersion(sparkSession, cachedBaseConfigs)
      val versionUnchanged = cheapVersion.exists(cv => lastSyncedVersion.contains(cv))
      var hadError         = false
      var nextSleepMs      = pollIntervalMs

      if (versionUnchanged) {
        // ── No-change path ─────────────────────────────────────────────────────────────────────
        pollsWithNoChanges += 1
        metrics.recordPollWithNoChanges()
        lastCurrentVersion = cheapVersion

        // Log on first no-change cycle and every quietPollLogInterval thereafter to reduce noise.
        val shouldLog = pollsWithNoChanges == 1 || (pollsWithNoChanges % quietPollLogInterval == 0)
        if (shouldLog) {
          val uptimeSec      = (cycleStart - streamStart) / 1000
          val lastSyncAgoSec = if (lastSyncCompleteMs > 0) (cycleStart - lastSyncCompleteMs) / 1000 else -1L
          val lastSyncLine =
            if (lastSyncAgoSec >= 0)
              s"\n  Last sync: ${lastSyncAgoSec}s ago | Duration: ${lastSyncDurationMs / 1000.0}s | " +
                s"Files: $lastFilesIndexed | Splits: $lastSplitsCreated"
            else ""
          logger.info(
            s"[IndextablesCompanionStream] POLLING | Source: ${command.sourcePath} (${command.sourceFormat.toUpperCase})" +
              s"\n  Current version: ${cheapVersion.get} | Last synced: ${lastSyncedVersion.getOrElse("none")} | Lag: 0 versions (caught up)" +
              lastSyncLine +
              s"\n  Total: ${metrics.syncCycles.value} cycles | ${metrics.totalFilesIndexed.value} files indexed | " +
              s"${metrics.errorCount.value} errors | Uptime: ${uptimeSec}s"
          )
        }

      } else {
        // ── Sync path ───────────────────────────────────────────────────────────────────────────
        pollsWithNoChanges = 0
        cheapVersion.foreach(cv => lastCurrentVersion = Some(cv))
        val lagVersions = for {
          cur    <- lastCurrentVersion
          synced <- lastSyncedVersion
        } yield cur - synced
        logger.info(
          s"[IndextablesCompanionStream] SYNCING cycle $cycle | " +
            s"source=${command.sourcePath} (${command.sourceFormat.toUpperCase})" +
            lagVersions.map(l => s" | Lag: $l versions").getOrElse("") +
            s" | lastSyncedVersion=$lastSyncedVersion"
        )

        try {
          val rows = syncFn(sparkSession, cycleStart, lastSyncedVersion)

          consecutiveErrors = 0

          // Row schema: table_path(0), source_path(1), status(2), source_version(3),
          //   splits_created(4), splits_invalidated(5), parquet_files_indexed(6),
          //   parquet_bytes_downloaded(7), split_bytes_uploaded(8), duration_ms(9), message(10)
          val (filesIndexed, splitsCreated, durationMs) = rows.headOption
            .map { row =>
              if (!row.isNullAt(3)) {
                lastSyncedVersion = Some(row.getLong(3))
                lastCurrentVersion = lastSyncedVersion
              }
              val files  = if (!row.isNullAt(6)) row.getInt(6).toLong else 0L
              val splits = if (!row.isNullAt(4)) row.getInt(4).toLong else 0L
              val dur    = if (!row.isNullAt(9)) row.getLong(9) else System.currentTimeMillis() - cycleStart
              (files, splits, dur)
            }
            .getOrElse((0L, 0L, System.currentTimeMillis() - cycleStart))

          lastSyncCompleteMs = System.currentTimeMillis()
          lastSyncDurationMs = durationMs
          lastFilesIndexed = filesIndexed
          lastSplitsCreated = splitsCreated

          metrics.recordCycleSuccess(filesIndexed, durationMs, splitsCreated)
          logger.info(
            s"[IndextablesCompanionStream] Sync cycle #$cycle complete" +
              s"\n  Duration: ${durationMs / 1000.0}s | Files indexed: $filesIndexed | Splits created: $splitsCreated" +
              s"\n  Synced version: $lastSyncedVersion" +
              s"\n  Total: ${metrics.syncCycles.value} cycles | ${metrics.totalFilesIndexed.value} files indexed | " +
              s"${metrics.errorCount.value} errors"
          )

        } catch {
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            logger.info(s"[IndextablesCompanionStream] Streaming sync interrupted during cycle $cycle")
            return
          case e: Exception =>
            hadError = true
            consecutiveErrors += 1
            metrics.recordCycleError()
            val rawBackoff = math.min(
              math.pow(errorBackoffMultiplier.toDouble, consecutiveErrors),
              10.0
            ) * pollIntervalMs
            nextSleepMs = math.min(rawBackoff.toLong, pollIntervalMs * 10L)
            logger.error(
              s"[IndextablesCompanionStream] ERROR in sync cycle #$cycle " +
                s"($consecutiveErrors/$maxConsecutiveErrors consecutive errors, " +
                s"will retry in ${nextSleepMs / 1000}s): ${e.getMessage}",
              e
            )
            if (consecutiveErrors >= maxConsecutiveErrors) {
              throw new RuntimeException(
                s"Streaming companion sync aborted after $maxConsecutiveErrors consecutive errors",
                e
              )
            }
        }
      }

      waitForNextCycle(nextSleepMs)
    }

    val uptimeSec = (System.currentTimeMillis() - streamStart) / 1000
    logger.info(
      s"[IndextablesCompanionStream] Streaming companion sync stopped. " +
        s"Total: ${metrics.syncCycles.value} cycles | ${metrics.totalFilesIndexed.value} files indexed | " +
        s"${metrics.errorCount.value} errors | Uptime: ${uptimeSec}s"
    )
  }

  private def waitForNextCycle(sleepMs: Long): Unit =
    try
      Thread.sleep(sleepMs)
    catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    }
}
