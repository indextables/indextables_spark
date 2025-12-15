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

package io.indextables.spark.xref

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession

import io.indextables.spark.transaction.AddXRefAction
import org.slf4j.LoggerFactory

/**
 * Wrapper for tantivy4java XRef search functionality.
 *
 * This object provides a bridge between the Spark-level XRef query routing and the low-level tantivy4java XRef search
 * API. It handles:
 *   - Checking XRef API availability
 *   - Opening XRef splits for search
 *   - Executing term-based queries to identify matching source splits
 *   - Managing resources and timeouts
 *
 * The XRef search returns split filenames that contain documents matching the query, enabling efficient query routing
 * that skips splits that cannot contain matching results.
 */
object XRefSearcher {

  private val logger = LoggerFactory.getLogger(getClass)

  // Flag to track if we've already checked for API availability
  @volatile private var availabilityChecked: Boolean = false
  @volatile private var xrefApiAvailable: Boolean = false

  /**
   * Test hook: Optional search override function for testing.
   *
   * When set, this function is called instead of the normal search path, allowing tests to simulate XRef search
   * behavior without requiring the actual tantivy4java XRef API.
   *
   * The function receives (xref, query, actualSourceSplits) and returns the matching split filenames.
   *
   * @note
   *   This is intended for testing only. Clear with clearTestSearchOverride() after each test.
   */
  @volatile private var testSearchOverride: Option[(AddXRefAction, String, Seq[String]) => Seq[String]] = None

  /**
   * Set a test search override function.
   *
   * When set, searchSplits will use this function instead of the normal search path.
   *
   * @param searchFn
   *   Function that takes (xref, query, actualSourceSplitFileNames) and returns matching split filenames
   */
  def setTestSearchOverride(searchFn: (AddXRefAction, String, Seq[String]) => Seq[String]): Unit =
    synchronized {
      testSearchOverride = Some(searchFn)
      logger.info("Test search override set")
    }

  /**
   * Clear the test search override.
   */
  def clearTestSearchOverride(): Unit =
    synchronized {
      testSearchOverride = None
      logger.info("Test search override cleared")
    }

  /**
   * Check if a test search override is set.
   */
  def hasTestSearchOverride: Boolean = testSearchOverride.isDefined

  /**
   * Check if the XRef search API is available.
   *
   * This checks whether the tantivy4java XRef classes are on the classpath and functional. The check is performed once
   * and cached for subsequent calls.
   *
   * Note: When a test override is set, this always returns true to allow testing of the XRef routing code path.
   *
   * @return
   *   true if XRef search is available (or test override is set), false otherwise
   */
  def isAvailable(): Boolean = {
    // If test override is set, always return true to allow testing
    if (testSearchOverride.isDefined) {
      return true
    }

    if (!availabilityChecked) {
      synchronized {
        if (!availabilityChecked) {
          xrefApiAvailable = checkXRefApiAvailability()
          availabilityChecked = true
          if (xrefApiAvailable) {
            logger.info("XRef search API is available")
          } else {
            logger.info("XRef search API is not available - XRef queries will return all source splits")
          }
        }
      }
    }
    xrefApiAvailable
  }

  /**
   * Reset availability check (for testing).
   */
  def resetAvailabilityCheck(): Unit =
    synchronized {
      availabilityChecked = false
      xrefApiAvailable = false
    }

  /**
   * Search an XRef for splits containing documents matching the query.
   *
   * @param xrefPath
   *   Full path to the XRef split file
   * @param xref
   *   XRef action containing metadata (footer offsets, source splits)
   * @param query
   *   Search query string (extracted from filters)
   * @param timeoutMs
   *   Query timeout in milliseconds
   * @param tablePath
   *   Path to the table (for resolving relative paths)
   * @param sparkSession
   *   The Spark session
   * @return
   *   Sequence of split filenames that may contain matching documents
   */
  def searchSplits(
    xrefPath: String,
    xref: AddXRefAction,
    query: String,
    timeoutMs: Int,
    tablePath: String,
    sparkSession: SparkSession
  ): Seq[String] = {

    logger.debug(s"Searching XRef ${xref.xrefId} for query: $query")

    // Check for test override first
    testSearchOverride.foreach { searchFn =>
      val actualSourceSplitFileNames = xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
      logger.debug(s"Using test search override for XRef ${xref.xrefId}")
      val result = searchFn(xref, query, actualSourceSplitFileNames)
      logger.debug(s"Test search override returned ${result.size} matching splits")
      return result
    }

    if (!isAvailable()) {
      logger.debug("XRef API not available, returning all source splits")
      return xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
    }

    Try {
      executeXRefSearch(xrefPath, xref, query, timeoutMs, tablePath, sparkSession)
    } match {
      case Success(matchingSplits) =>
        logger.debug(s"XRef search returned ${matchingSplits.size} matching splits")
        matchingSplits

      case Failure(e) =>
        logger.warn(s"XRef search failed for ${xref.xrefId}: ${e.getMessage}")
        // On failure, conservatively return all source splits
        xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
    }
  }

  /**
   * Check if the tantivy4java XRef API classes are available.
   */
  private def checkXRefApiAvailability(): Boolean =
    Try {
      // Check if the XRef-specific classes exist
      // The actual class names will depend on the tantivy4java API
      // For now, check for the base tantivy4java classes
      Class.forName("com.tantivy4java.Index")
      Class.forName("com.tantivy4java.Searcher")

      // TODO: Check for XRef-specific classes when available
      // Class.forName("com.tantivy4java.xref.XRefIndex")
      // Class.forName("com.tantivy4java.xref.XRefSearcher")

      // For now, return false until XRef API is implemented in tantivy4java
      // This will cause XRefSearcher to conservatively return all source splits
      false
    } match {
      case Success(available) => available
      case Failure(_)         => false
    }

  /**
   * Execute the actual XRef search using tantivy4java.
   *
   * This is the integration point with the tantivy4java XRef API. When the API becomes available, this method will:
   *   1. Open the XRef split using footer offset information
   *   2. Parse the query and search the XRef term dictionary
   *   3. Return split filenames that contain matching terms
   *
   * @param xrefPath
   *   Full path to the XRef split
   * @param xref
   *   XRef metadata
   * @param query
   *   Search query
   * @param timeoutMs
   *   Timeout
   * @param tablePath
   *   Table path
   * @param sparkSession
   *   Spark session for file system access
   * @return
   *   Matching split filenames
   */
  private def executeXRefSearch(
    xrefPath: String,
    xref: AddXRefAction,
    query: String,
    timeoutMs: Int,
    tablePath: String,
    sparkSession: SparkSession
  ): Seq[String] = {

    // TODO: Implement actual tantivy4java XRef search when API is available
    //
    // Expected implementation pattern:
    //
    // val fs = FileSystem.get(new Path(xrefPath).toUri, sparkSession.sparkContext.hadoopConfiguration)
    // val xrefReader = XRefReader.open(fs, new Path(xrefPath), xref.footerStartOffset, xref.footerEndOffset)
    // try {
    //   // Parse query into terms
    //   val terms = parseQueryTerms(query)
    //
    //   // Search XRef for each term
    //   val matchingSplits = terms.flatMap { term =>
    //     xrefReader.searchTerm(term, timeoutMs)
    //   }.distinct
    //
    //   matchingSplits.map(XRefStorageUtils.extractFileName)
    // } finally {
    //   xrefReader.close()
    // }

    // Placeholder: Return all source splits until API is implemented
    logger.debug(s"XRef search not yet implemented, returning all ${xref.sourceSplitPaths.size} source splits")
    xref.sourceSplitPaths.map(XRefStorageUtils.extractFileName)
  }

  /**
   * Parse a query string into individual search terms.
   *
   * This handles basic query syntax:
   *   - Simple terms: "word" -> Seq("word")
   *   - Multiple terms: "word1 word2" -> Seq("word1", "word2")
   *   - Phrases: "\"exact phrase\"" -> Seq("exact phrase")
   *
   * @param query
   *   Query string
   * @return
   *   Sequence of terms to search
   */
  def parseQueryTerms(query: String): Seq[String] = {
    if (query == null || query.trim.isEmpty) {
      return Seq.empty
    }

    val terms = scala.collection.mutable.ArrayBuffer[String]()
    var inQuotes = false
    var currentTerm = new StringBuilder()
    var i = 0

    while (i < query.length) {
      val c = query.charAt(i)
      c match {
        case '"' =>
          if (inQuotes) {
            // End of quoted phrase
            if (currentTerm.nonEmpty) {
              terms += currentTerm.toString()
              currentTerm = new StringBuilder()
            }
            inQuotes = false
          } else {
            // Start of quoted phrase
            if (currentTerm.nonEmpty) {
              terms += currentTerm.toString()
              currentTerm = new StringBuilder()
            }
            inQuotes = true
          }

        case ' ' | '\t' | '\n' | '\r' =>
          if (inQuotes) {
            currentTerm.append(c)
          } else if (currentTerm.nonEmpty) {
            terms += currentTerm.toString()
            currentTerm = new StringBuilder()
          }

        case _ =>
          currentTerm.append(c)
      }
      i += 1
    }

    // Add final term
    if (currentTerm.nonEmpty) {
      terms += currentTerm.toString()
    }

    terms.toSeq.filter(_.nonEmpty)
  }
}
