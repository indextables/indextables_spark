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
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

/**
 * Security test for VULN-001 (CWE-22): companion-sync path traversal → arbitrary file WRITE.
 *
 * `SyncTaskExecutor` resolves each source file to a local destination via
 * `PathContainment.resolveContained(tempDir, relativePath)` (previously `new File(tempDir, rel)`).
 * A `relativePath` carrying `..` segments — injected through the external table's file listing —
 * must NOT be allowed to escape `tempDir`, else an attacker writes arbitrary bytes to any path on
 * the executor (authorized_keys, cron, a classpath jar) → RCE.
 */
class CompanionSyncArbitraryWriteSecurityTest extends AnyFunSuite {

  test("resolveContained rejects a '..' relative path that escapes the temp dir (VULN-001)") {
    val tempDir = Files.createTempDirectory("sync-secure").toFile
    try {
      val malicious = "../../../../../../tmp/PWNED_authorized_keys"
      val ex = intercept[SecurityException] {
        PathContainment.resolveContained(tempDir, malicious)
      }
      assert(ex.getMessage.contains("traversal"), s"unexpected message: ${ex.getMessage}")
    } finally deleteRecursively(tempDir)
  }

  test("resolveContained rejects an embedded '..' escape (VULN-001)") {
    val tempDir = Files.createTempDirectory("sync-secure").toFile
    try
      intercept[SecurityException] {
        PathContainment.resolveContained(tempDir, "sub/dir/../../../../escape.bin")
      }
    finally deleteRecursively(tempDir)
  }

  test("resolveContained allows a legitimate nested relative path (regression guard)") {
    val tempDir = Files.createTempDirectory("sync-secure").toFile
    try {
      val resolved = PathContainment.resolveContained(tempDir, "part=1/file-00000.parquet")
      assert(
        resolved.getCanonicalPath.startsWith(tempDir.getCanonicalPath + File.separator),
        s"legitimate path was resolved outside tempDir: ${resolved.getCanonicalPath}"
      )
    } finally deleteRecursively(tempDir)
  }

  test("resolveContained allows a bare filename (regression guard)") {
    val tempDir = Files.createTempDirectory("sync-secure").toFile
    try {
      val resolved = PathContainment.resolveContained(tempDir, "file-00000.parquet")
      assert(resolved.getCanonicalPath == new File(tempDir, "file-00000.parquet").getCanonicalPath)
    } finally deleteRecursively(tempDir)
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles).foreach(_.foreach(deleteRecursively))
    f.delete()
    ()
  }
}
