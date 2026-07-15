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

package io.indextables.spark.core

import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

/**
 * Security test for VULN-003 (CWE-22): transaction-log split path traversal / confused-deputy read.
 *
 * `PathResolutionUtils.resolveSplitPath[AsString]` is the chokepoint every reachable split reader
 * uses to turn an AddAction `path` into a location. A tampered AddAction whose path is an absolute
 * cross-bucket redirect or a `..` traversal must be rejected so the reader cannot open a split
 * outside the table root with the querying user's credentials.
 */
class SplitPathTraversalSecurityTest extends AnyFunSuite {

  private val tablePath = "s3://my-table/data"

  test("resolveSplitPathAsString rejects an absolute cross-bucket redirect (VULN-003)") {
    val ex = intercept[SecurityException] {
      PathResolutionUtils.resolveSplitPathAsString("s3://victim-bucket/private/data.split", tablePath)
    }
    assert(ex.getMessage.contains("outside") || ex.getMessage.contains("traversal"), ex.getMessage)
  }

  test("resolveSplitPathAsString rejects a '..' relative traversal (VULN-003)") {
    intercept[SecurityException] {
      PathResolutionUtils.resolveSplitPathAsString("../../../../other-table/secret.split", tablePath)
    }
  }

  test("resolveSplitPath (Path overload) rejects an absolute cross-bucket redirect (VULN-003)") {
    intercept[SecurityException] {
      PathResolutionUtils.resolveSplitPath("s3://victim-bucket/private/data.split", tablePath)
    }
  }

  test("resolveSplitPathAsString allows a legitimate relative split under the table (regression guard)") {
    val resolved = PathResolutionUtils.resolveSplitPathAsString("part-00000-abc.split", tablePath)
    assert(resolved.startsWith(tablePath + "/"), s"legitimate relative split was altered: $resolved")
  }

  test("resolveSplitPathAsString allows an absolute split that is under the table root (regression guard)") {
    val underTable = tablePath + "/part-00001-def.split"
    val resolved   = PathResolutionUtils.resolveSplitPathAsString(underTable, tablePath)
    assert(resolved == underTable)
  }

  test("resolveSplitPathAsString allows a relative split under a file: table root (regression guard)") {
    val dir       = Files.createTempDirectory("split-table").toString
    val fileRoot  = "file://" + dir
    // Must not throw — a relative split under a local file: table root is legitimate.
    val resolved  = PathResolutionUtils.resolveSplitPathAsString("part-00000-ghi.split", fileRoot)
    assert(resolved.contains("part-00000-ghi.split"), s"unexpected resolution: $resolved")
  }
}
