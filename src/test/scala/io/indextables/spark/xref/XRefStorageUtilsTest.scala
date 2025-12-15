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

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class XRefStorageUtilsTest extends AnyFunSuite with Matchers {

  test("getHashDirectory should return 4-character string in range [aaaa-zzzz]") {
    val testNames = Seq(
      "xref-abc123.split",
      "xref-def456.split",
      "xref-xyz789.split",
      "xref-000000.split",
      "xref-ffffff.split"
    )

    testNames.foreach { name =>
      val hashDir = XRefStorageUtils.getHashDirectory(name)
      hashDir.length shouldBe 4
      hashDir.forall(c => c >= 'a' && c <= 'z') shouldBe true
    }
  }

  test("getHashDirectory should be deterministic") {
    val name = "xref-test123.split"
    val hash1 = XRefStorageUtils.getHashDirectory(name)
    val hash2 = XRefStorageUtils.getHashDirectory(name)
    hash1 shouldBe hash2
  }

  test("getHashDirectory should produce different results for different inputs") {
    val hash1 = XRefStorageUtils.getHashDirectory("xref-aaa.split")
    val hash2 = XRefStorageUtils.getHashDirectory("xref-bbb.split")
    val hash3 = XRefStorageUtils.getHashDirectory("xref-ccc.split")

    // Different inputs should generally produce different hashes
    // (though collisions are possible, they should be rare)
    Set(hash1, hash2, hash3).size should be >= 2
  }

  test("generateXRefId should produce unique IDs") {
    val ids = (1 to 100).map(_ => XRefStorageUtils.generateXRefId())
    ids.toSet.size shouldBe 100
  }

  test("generateXRefId should produce 16-character IDs") {
    val id = XRefStorageUtils.generateXRefId()
    id.length shouldBe 16
  }

  test("getXRefFileName should format correctly") {
    val fileName = XRefStorageUtils.getXRefFileName("abc123")
    fileName shouldBe "xref-abc123.split"
  }

  test("getXRefRelativePath should include hash directory") {
    val xrefId = "testxref123"
    val relativePath = XRefStorageUtils.getXRefRelativePath(xrefId)

    relativePath should startWith("_xrefsplits/")
    relativePath should endWith(s"/xref-$xrefId.split")

    // Should have hash directory in the middle
    val parts = relativePath.split("/")
    parts.length shouldBe 3
    parts(0) shouldBe "_xrefsplits"
    parts(1).length shouldBe 4
    parts(2) shouldBe s"xref-$xrefId.split"
  }

  test("getXRefRelativePath should use custom storage directory") {
    val xrefId = "test123"
    val relativePath = XRefStorageUtils.getXRefRelativePath(xrefId, "custom_xrefs")

    relativePath should startWith("custom_xrefs/")
    relativePath should endWith(s"/xref-$xrefId.split")
  }

  test("getXRefFullPath should combine table path correctly") {
    val tablePath = new Path("s3://bucket/table")
    val xrefId = "abc123"
    val fullPath = XRefStorageUtils.getXRefFullPath(tablePath, xrefId)

    fullPath.toString should startWith("s3://bucket/table/_xrefsplits/")
    fullPath.toString should endWith("/xref-abc123.split")
  }

  test("getXRefFullPathString should work with string table path") {
    val tablePath = "/local/path/table"
    val xrefId = "def456"
    val fullPath = XRefStorageUtils.getXRefFullPathString(tablePath, xrefId)

    fullPath should startWith("/local/path/table/_xrefsplits/")
    fullPath should endWith("/xref-def456.split")
  }

  test("extractXRefId should extract ID from valid XRef path") {
    val path1 = "_xrefsplits/abcd/xref-abc123def456.split"
    val path2 = "s3://bucket/table/_xrefsplits/wxyz/xref-xyz789.split"

    XRefStorageUtils.extractXRefId(path1) shouldBe Some("abc123def456")
    XRefStorageUtils.extractXRefId(path2) shouldBe Some("xyz789")
  }

  test("extractXRefId should return None for invalid paths") {
    val invalidPaths = Seq(
      "part-00000-abc.split",
      "xref-abc.json",
      "not-an-xref-file.split",
      "_xrefsplits/directory/"
    )

    invalidPaths.foreach { path =>
      XRefStorageUtils.extractXRefId(path) shouldBe None
    }
  }

  test("isXRefPath should identify XRef paths correctly") {
    XRefStorageUtils.isXRefPath("xref-abc123.split") shouldBe true
    XRefStorageUtils.isXRefPath("_xrefsplits/abcd/xref-test.split") shouldBe true
    XRefStorageUtils.isXRefPath("s3://bucket/table/_xrefsplits/wxyz/xref-xyz.split") shouldBe true

    XRefStorageUtils.isXRefPath("part-00000-abc.split") shouldBe false
    XRefStorageUtils.isXRefPath("xref-abc.json") shouldBe false
    XRefStorageUtils.isXRefPath("random-file.txt") shouldBe false
  }

  test("getXRefStorageDirectoryPath should return correct path") {
    val tablePath = new Path("s3://bucket/table")
    val dirPath = XRefStorageUtils.getXRefStorageDirectoryPath(tablePath)

    dirPath.toString shouldBe "s3://bucket/table/_xrefsplits"
  }

  test("getXRefStorageDirectoryPath should use custom directory name") {
    val tablePath = new Path("/local/table")
    val dirPath = XRefStorageUtils.getXRefStorageDirectoryPath(tablePath, "my_xrefs")

    dirPath.toString shouldBe "/local/table/my_xrefs"
  }

  test("extractFileName should extract just the filename") {
    XRefStorageUtils.extractFileName("s3://bucket/path/to/file.split") shouldBe "file.split"
    XRefStorageUtils.extractFileName("/local/path/to/file.split") shouldBe "file.split"
    XRefStorageUtils.extractFileName("file.split") shouldBe "file.split"
  }

  test("buildFileNameIndex should create filename-to-path mapping") {
    val paths = Seq(
      "s3://bucket/table/part-001.split",
      "s3://bucket/table/date=2024/part-002.split",
      "s3://bucket/table/date=2024/hour=10/part-003.split"
    )

    val index = XRefStorageUtils.buildFileNameIndex(paths)

    index("part-001.split") shouldBe "s3://bucket/table/part-001.split"
    index("part-002.split") shouldBe "s3://bucket/table/date=2024/part-002.split"
    index("part-003.split") shouldBe "s3://bucket/table/date=2024/hour=10/part-003.split"
  }

  test("buildFileNameToPathsIndex should handle duplicate filenames") {
    val paths = Seq(
      "s3://bucket/table/date=2024-01/part-001.split",
      "s3://bucket/table/date=2024-02/part-001.split", // Same filename, different partition
      "s3://bucket/table/date=2024-01/part-002.split"
    )

    val index = XRefStorageUtils.buildFileNameToPathsIndex(paths)

    index("part-001.split") should contain allOf (
      "s3://bucket/table/date=2024-01/part-001.split",
      "s3://bucket/table/date=2024-02/part-001.split"
    )
    index("part-002.split") should contain("s3://bucket/table/date=2024-01/part-002.split")
  }

  test("hash distribution should be reasonably uniform") {
    // Generate many XRef filenames and check hash distribution
    val numSamples = 10000
    val hashes = (1 to numSamples).map { i =>
      XRefStorageUtils.getHashDirectory(s"xref-${i.toString.reverse.padTo(16, '0')}.split")
    }

    // Check that we get reasonable diversity (at least 1000 unique hashes out of 10000)
    val uniqueHashes = hashes.toSet.size
    uniqueHashes should be >= 1000
  }
}
