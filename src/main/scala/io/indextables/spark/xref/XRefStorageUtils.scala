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

import java.util.UUID

import org.apache.hadoop.fs.Path

/**
 * Utilities for XRef split storage management.
 *
 * XRef splits are stored in a hash-based directory structure to avoid directory fragmentation when tables have many
 * partitions. The directory name is derived from the XRef filename's hash code.
 *
 * Directory structure:
 * {{{
 *   {table_root}/_xrefsplits/
 *     ├── aaaa/
 *     │   └── xref-{uuid}.split
 *     ├── abcd/
 *     │   └── xref-{uuid}.split
 *     └── zzzz/
 *         └── xref-{uuid}.split
 * }}}
 */
object XRefStorageUtils {

  /** Default XRef storage directory name. */
  val DEFAULT_XREF_DIRECTORY = "_xrefsplits"

  /** XRef file prefix. */
  val XREF_FILE_PREFIX = "xref-"

  /** XRef file extension. */
  val XREF_FILE_EXTENSION = ".split"

  /**
   * Derive hash directory from XRef filename.
   *
   * Maps filename hash to a 4-character string in range [aaaa, zzzz]. Total buckets: 26^4 = 456,976
   *
   * @param xrefFileName
   *   The XRef filename (e.g., "xref-abc123.split")
   * @return
   *   4-character directory name in range [aaaa, zzzz]
   */
  def getHashDirectory(xrefFileName: String): String = {
    val hash = Math.abs(xrefFileName.hashCode)
    // Map to 4 characters, each in range [a-z] (26 options)
    val chars     = Array.fill(4)('a')
    var remaining = hash
    for (i <- 3 to 0 by -1) {
      chars(i) = ('a' + (remaining % 26)).toChar
      remaining = remaining / 26
    }
    new String(chars)
  }

  /**
   * Generate a unique XRef ID.
   *
   * @return
   *   UUID-based XRef identifier
   */
  def generateXRefId(): String =
    UUID.randomUUID().toString.replace("-", "").take(16)

  /**
   * Build XRef filename from ID.
   *
   * @param xrefId
   *   The XRef identifier
   * @return
   *   XRef filename (e.g., "xref-abc123def456.split")
   */
  def getXRefFileName(xrefId: String): String =
    s"$XREF_FILE_PREFIX$xrefId$XREF_FILE_EXTENSION"

  /**
   * Build relative path for XRef file within table.
   *
   * @param xrefId
   *   The XRef identifier
   * @param storageDirectory
   *   The XRef storage directory (default: "_xrefsplits")
   * @return
   *   Relative path (e.g., "_xrefsplits/kmpq/xref-abc123.split")
   */
  def getXRefRelativePath(xrefId: String, storageDirectory: String = DEFAULT_XREF_DIRECTORY): String = {
    val fileName = getXRefFileName(xrefId)
    val hashDir  = getHashDirectory(fileName)
    s"$storageDirectory/$hashDir/$fileName"
  }

  /**
   * Build full path for XRef file.
   *
   * @param tablePath
   *   The table root path
   * @param xrefId
   *   The XRef identifier
   * @param storageDirectory
   *   The XRef storage directory (default: "_xrefsplits")
   * @return
   *   Full Hadoop Path to XRef file
   */
  def getXRefFullPath(
    tablePath: Path,
    xrefId: String,
    storageDirectory: String = DEFAULT_XREF_DIRECTORY
  ): Path = {
    val relativePath = getXRefRelativePath(xrefId, storageDirectory)
    new Path(tablePath, relativePath)
  }

  /**
   * Build full path for XRef file.
   *
   * @param tablePath
   *   The table root path as string
   * @param xrefId
   *   The XRef identifier
   * @param storageDirectory
   *   The XRef storage directory (default: "_xrefsplits")
   * @return
   *   Full path as string
   */
  def getXRefFullPathString(
    tablePath: String,
    xrefId: String,
    storageDirectory: String = DEFAULT_XREF_DIRECTORY
  ): String =
    getXRefFullPath(new Path(tablePath), xrefId, storageDirectory).toString

  /**
   * Extract XRef ID from path.
   *
   * @param xrefPath
   *   Full or relative XRef path
   * @return
   *   XRef identifier or None if not a valid XRef path
   */
  def extractXRefId(xrefPath: String): Option[String] = {
    val fileName = new Path(xrefPath).getName
    if (fileName.startsWith(XREF_FILE_PREFIX) && fileName.endsWith(XREF_FILE_EXTENSION)) {
      Some(fileName.stripPrefix(XREF_FILE_PREFIX).stripSuffix(XREF_FILE_EXTENSION))
    } else {
      None
    }
  }

  /**
   * Check if a path is an XRef file path.
   *
   * @param path
   *   Path to check
   * @return
   *   true if this is an XRef file path
   */
  def isXRefPath(path: String): Boolean = {
    val fileName = new Path(path).getName
    fileName.startsWith(XREF_FILE_PREFIX) && fileName.endsWith(XREF_FILE_EXTENSION)
  }

  /**
   * Get the XRef storage directory path for a table.
   *
   * @param tablePath
   *   The table root path
   * @param storageDirectory
   *   The XRef storage directory name (default: "_xrefsplits")
   * @return
   *   Path to the XRef storage directory
   */
  def getXRefStorageDirectoryPath(
    tablePath: Path,
    storageDirectory: String = DEFAULT_XREF_DIRECTORY
  ): Path =
    new Path(tablePath, storageDirectory)

  /**
   * Extract filename from a split path for matching purposes.
   *
   * XRef results are matched by filename since XRefs may reference splits from multiple partitions and the full paths
   * may differ between XRef metadata and transaction log entries.
   *
   * @param splitPath
   *   Full or relative split path
   * @return
   *   Just the filename (e.g., "part-00000-abc.split")
   */
  def extractFileName(splitPath: String): String =
    splitPath.split("/").last

  /**
   * Build a path-to-filename mapping for efficient lookups.
   *
   * @param paths
   *   Collection of split paths
   * @return
   *   Map from filename to full path
   */
  def buildFileNameIndex(paths: Seq[String]): Map[String, String] =
    paths.map(p => extractFileName(p) -> p).toMap

  /**
   * Build a filename-to-paths mapping (for cases where multiple paths have same filename).
   *
   * @param paths
   *   Collection of split paths
   * @return
   *   Map from filename to all matching paths
   */
  def buildFileNameToPathsIndex(paths: Seq[String]): Map[String, Seq[String]] =
    paths.groupBy(extractFileName)
}
