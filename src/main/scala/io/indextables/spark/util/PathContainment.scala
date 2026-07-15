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

package io.indextables.spark.util

import java.io.File
import java.net.URI

/**
 * Path-traversal containment guards (CWE-22).
 *
 * Companion sync and split resolution both take path components that originate from lower-trust
 * sources (an external table's file listing, a transaction-log AddAction). Left unchecked, a `..`
 * segment or an absolute/scheme-qualified redirect lets the resolved path escape the directory it
 * is supposed to stay within — enabling arbitrary file writes on executors (VULN-001), confused
 * deputy reads of arbitrary local/remote files (VULN-002), and split-read redirection (VULN-003).
 *
 * The helpers here anchor a resolved path to a permitted root and fail closed with a
 * [[SecurityException]] when it would escape.
 */
object PathContainment {

  /**
   * Resolve `relativePath` against a local `root` directory and guarantee the result stays inside
   * `root`. Rejects `..` traversal that would escape the root (VULN-001).
   *
   * @return
   *   the resolved [[File]], guaranteed to canonicalize to `root` itself or a descendant of it.
   * @throws SecurityException
   *   if the resolved path escapes `root`.
   */
  def resolveContained(root: File, relativePath: String): File = {
    val candidate          = new File(root, relativePath)
    val rootCanonical      = root.getCanonicalPath
    val candidateCanonical = candidate.getCanonicalPath
    if (candidateCanonical != rootCanonical && !candidateCanonical.startsWith(rootCanonical + File.separator))
      throw new SecurityException(
        s"Path traversal blocked: '$relativePath' resolves to '$candidateCanonical', " +
          s"which is outside the permitted directory '$rootCanonical'"
      )
    candidate
  }

  /**
   * Confine a companion-sync SOURCE path to the declared table root (VULN-002).
   *
   * When `tableRoot` is a concrete location (has a URI scheme, or is an absolute local path), the
   * resolved source must be the root itself or a descendant of it; any `file://`, cross-bucket, or
   * `..`-escaping source is rejected. When `tableRoot` is not a location (e.g. an Iceberg
   * `db.table` identifier) it cannot anchor containment, so the source is required to be free of
   * `..` traversal and must not be a local/`file:` path (which would allow local-file disclosure).
   *
   * @throws SecurityException
   *   if the source path is not confined to the table root.
   */
  def assertSourceUnderRoot(sourcePath: String, tableRoot: String): Unit =
    if (isLocation(tableRoot)) {
      assertUnderRoot(sourcePath, tableRoot, "companion-sync source")
    } else {
      // Non-location root (e.g. Iceberg identifier): cannot anchor, so block the concrete
      // disclosure/traversal primitives.
      if (containsTraversal(sourcePath) || isLocalPath(sourcePath))
        throw new SecurityException(
          s"companion-sync source path '$sourcePath' is not permitted for table '$tableRoot': " +
            "absolute/local/traversal source paths are rejected when the table root is not a location"
        )
    }

  /**
   * Assert that a resolved split `path` stays under `tableRoot` (VULN-003). Applied to both absolute
   * and relative split-path resolutions so an AddAction cannot redirect a read outside the table.
   *
   * @throws SecurityException
   *   if the resolved split path escapes the table root.
   */
  def assertSplitUnderTable(path: String, tableRoot: String): Unit =
    assertUnderRoot(path, tableRoot, "split")

  /** True if `p` looks like a resolvable location: has a URI scheme or is an absolute local path. */
  private def isLocation(p: String): Boolean =
    p.startsWith("/") || p.startsWith("file:") || schemeOf(p).isDefined

  /** True if `p` is a bare local filesystem path or a `file:` URI. */
  private def isLocalPath(p: String): Boolean =
    p.startsWith("/") || p.startsWith("file:")

  /** True if any path segment is a `..` traversal token. */
  private def containsTraversal(p: String): Boolean =
    p.split("[/\\\\]").exists(_ == "..")

  private def assertUnderRoot(resolved: String, tableRoot: String, kind: String): Unit = {
    val normalizedRoot     = normalizeLocation(tableRoot)
    val normalizedResolved = normalizeLocation(resolved)
    if (
      normalizedResolved != normalizedRoot &&
      !normalizedResolved.startsWith(normalizedRoot + "/")
    )
      throw new SecurityException(
        s"Path traversal blocked: $kind path '$resolved' resolves to '$normalizedResolved', " +
          s"which is outside the table root '$tableRoot' ('$normalizedRoot')"
      )
  }

  /**
   * Normalize a location to a canonical, comparable form:
   *   - `file:` URIs and bare local paths collapse to a canonical filesystem path (resolves `..`).
   *   - scheme URIs (s3/s3a/abfss/…) keep a lowercased scheme + authority with the path component
   *     normalized so `..` segments are collapsed (`s3a` is folded to `s3`). No filesystem access.
   *
   * Trailing slashes are stripped so prefix comparisons are exact.
   */
  private def normalizeLocation(p: String): String = {
    val trimmed = p.stripSuffix("/")
    schemeOf(trimmed) match {
      case Some(scheme) if scheme != "file" =>
        try {
          val uri        = new URI(trimmed).normalize()
          val schemeNorm = if (scheme == "s3a") "s3" else scheme
          val authority  = Option(uri.getAuthority).getOrElse("")
          val path       = Option(uri.getPath).getOrElse("").stripSuffix("/")
          s"$schemeNorm://$authority$path"
        } catch {
          case _: Exception => trimmed
        }
      case _ =>
        // file: URI or bare local path — canonicalize on the local filesystem to collapse '..'.
        new File(CloudPathUtils.stripFileScheme(trimmed)).getCanonicalPath
    }
  }

  /** Extract a lowercased URI scheme (e.g. "s3", "abfss", "file") if `p` is scheme-qualified. */
  private def schemeOf(p: String): Option[String] = {
    val idx = p.indexOf("://")
    if (idx > 0) Some(p.substring(0, idx).toLowerCase)
    else if (p.startsWith("file:")) Some("file")
    else None
  }
}
