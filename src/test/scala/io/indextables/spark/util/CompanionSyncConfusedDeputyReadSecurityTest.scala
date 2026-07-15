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

import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

/**
 * Security test for VULN-002 (CWE-22): companion-sync confused-deputy arbitrary file READ.
 *
 * `SyncTaskExecutor.downloadFile` calls `PathContainment.assertSourceUnderRoot(sourcePath,
 * tableRoot)` before dereferencing a source path with the operator's credentials. A source path
 * pointing outside the declared table root — `file:///etc/passwd` (local disclosure) or
 * `s3://victim-bucket/...` (cross-bucket confused deputy) — must be rejected.
 */
class CompanionSyncConfusedDeputyReadSecurityTest extends AnyFunSuite {

  private val s3Root = "s3://ext-table/root"

  test("assertSourceUnderRoot rejects a file:// local-disclosure source (VULN-002)") {
    val ex = intercept[SecurityException] {
      PathContainment.assertSourceUnderRoot("file:///etc/passwd", s3Root)
    }
    assert(ex.getMessage.contains("traversal") || ex.getMessage.contains("outside"), ex.getMessage)
  }

  test("assertSourceUnderRoot rejects a cross-bucket confused-deputy source (VULN-002)") {
    intercept[SecurityException] {
      PathContainment.assertSourceUnderRoot("s3://victim-bucket/private/secret.parquet", s3Root)
    }
  }

  test("assertSourceUnderRoot rejects a '..' escape that textually starts with the root (VULN-002)") {
    intercept[SecurityException] {
      PathContainment.assertSourceUnderRoot("s3://ext-table/root/../../../../tmp/evil.parquet", s3Root)
    }
  }

  test("assertSourceUnderRoot rejects a local /etc/passwd source under a local table root (VULN-002)") {
    val secret = Files.createTempFile("victim-secret", ".txt")
    Files.write(secret, "TOP-SECRET".getBytes)
    val localRoot = Files.createTempDirectory("ext-table-root").toString
    intercept[SecurityException] {
      PathContainment.assertSourceUnderRoot(secret.toAbsolutePath.toString, localRoot)
    }
    intercept[SecurityException] {
      PathContainment.assertSourceUnderRoot("file://" + secret.toAbsolutePath.toString, localRoot)
    }
  }

  test("assertSourceUnderRoot allows a legitimate in-root source (regression guard)") {
    // Must not throw for a source that genuinely lives under the declared table root.
    PathContainment.assertSourceUnderRoot("s3://ext-table/root/part-00000.parquet", s3Root)
    PathContainment.assertSourceUnderRoot("s3a://ext-table/root/part-00001.parquet", s3Root)
  }

  // Non-location root (e.g. an Iceberg 'db.table' identifier) cannot anchor containment, so the
  // guard still blocks the concrete disclosure/traversal primitives.
  private val identifierRoot = "default.my_table"

  test("assertSourceUnderRoot rejects a local/file: source under a non-location (Iceberg) root (VULN-002)") {
    intercept[SecurityException] {
      PathContainment.assertSourceUnderRoot("file:///etc/passwd", identifierRoot)
    }
    intercept[SecurityException] {
      PathContainment.assertSourceUnderRoot("/etc/passwd", identifierRoot)
    }
  }

  test("assertSourceUnderRoot rejects a '..' traversal under a non-location (Iceberg) root (VULN-002)") {
    intercept[SecurityException] {
      PathContainment.assertSourceUnderRoot("../../secret/data.parquet", identifierRoot)
    }
  }

  test("assertSourceUnderRoot allows a traversal-free relative source under a non-location root (regression guard)") {
    // A catalog-issued relative file name with no traversal is permitted under an identifier root.
    PathContainment.assertSourceUnderRoot("data/part-00000.parquet", identifierRoot)
  }
}
