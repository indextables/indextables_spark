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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source

/**
 * Regression tests to ensure the raw JSON hashing bug stays fixed.
 *
 * BACKGROUND:
 * We discovered a bug where schema JSON was being hashed directly without normalization.
 * This caused semantically identical schemas (with different field orderings from tantivy4java)
 * to produce different hashes, leading to:
 *   - N unique schema registry entries for N splits (instead of 1)
 *   - O(n) JSON parsing on every table read
 *   - 20+ second read times for tables with 10k+ splits
 *
 * THE FIX:
 * All schema hashing MUST use SchemaDeduplication.computeSchemaHash() which normalizes
 * JSON (sorts object keys and named arrays) before computing the hash.
 *
 * THIS TEST:
 * Scans main source files to detect any raw `hashSchema` functions or patterns that
 * suggest schema hashing without normalization. Fails if any violations are found.
 */
class SchemaHashingRegressionTest extends AnyFunSuite with Matchers {

  // Path to main source directory
  private val mainSourceDir = new File("src/main/scala")

  /**
   * REGRESSION TEST: No local hashSchema functions should exist in main source.
   *
   * Any `def hashSchema` that computes SHA-256 directly on raw JSON is a bug.
   * All schema hashing must go through SchemaDeduplication.computeSchemaHash().
   */
  test("REGRESSION: no local hashSchema functions should exist in main source") {
    val violations = findFilesWithPattern(
      mainSourceDir,
      """def\s+hashSchema\s*\(""".r,
      Set("SchemaDeduplication.scala") // The canonical implementation is allowed
    )

    if (violations.nonEmpty) {
      val details = violations.map { case (file, lines) =>
        s"\n  ${file.getPath}:\n${lines.map(l => s"    $l").mkString("\n")}"
      }.mkString

      fail(
        s"""REGRESSION DETECTED: Found local hashSchema function(s) in main source!
           |
           |All schema hashing MUST use SchemaDeduplication.computeSchemaHash() to ensure
           |proper JSON normalization. Direct SHA-256 hashing on raw JSON causes the schema
           |deduplication bug where semantically identical schemas get different hashes.
           |
           |Violations found:$details
           |
           |FIX: Replace hashSchema(json) calls with SchemaDeduplication.computeSchemaHash(json)
           |""".stripMargin
      )
    }
  }

  /**
   * REGRESSION TEST: No raw MessageDigest hashing on docMappingJson.
   *
   * Patterns like `digest.digest(json.getBytes` near `docMapping` are suspicious
   * and likely indicate raw hashing without normalization.
   */
  test("REGRESSION: no raw MessageDigest hashing near docMapping references") {
    val violations = scala.collection.mutable.ListBuffer[(File, Seq[String])]()

    findScalaFiles(mainSourceDir)
      .filterNot(_.getName == "SchemaDeduplication.scala")
      .foreach { file =>
        val content = Source.fromFile(file).getLines().toSeq

        // Check if file has both MessageDigest and docMapping (potential issue)
        val hasMessageDigest = content.exists(_.contains("MessageDigest"))
        val hasDocMapping = content.exists(_.contains("docMapping"))
        val usesSchemaDeduplication = content.exists(_.contains("SchemaDeduplication.computeSchemaHash"))

        // If file has MessageDigest + docMapping but doesn't use SchemaDeduplication.computeSchemaHash,
        // it's likely a violation (manual review needed)
        if (hasMessageDigest && hasDocMapping && !usesSchemaDeduplication) {
          val relevantLines = content.zipWithIndex
            .filter { case (line, _) =>
              line.contains("MessageDigest") ||
              (line.contains("digest") && line.contains("docMapping"))
            }
            .map { case (line, idx) => s"Line ${idx + 1}: ${line.trim}" }

          if (relevantLines.nonEmpty) {
            violations += ((file, relevantLines))
          }
        }
      }

    if (violations.nonEmpty) {
      val details = violations.map { case (file, lines) =>
        s"\n  ${file.getPath}:\n${lines.map(l => s"    $l").mkString("\n")}"
      }.mkString

      fail(
        s"""REGRESSION DETECTED: Found potential raw schema hashing in main source!
           |
           |Files with MessageDigest + docMapping but NOT using SchemaDeduplication.computeSchemaHash:$details
           |
           |If these are legitimate uses (e.g., hashing table paths), add the file to the exclusion list.
           |Otherwise, fix by using SchemaDeduplication.computeSchemaHash(json) for all schema hashing.
           |""".stripMargin
      )
    }
  }

  /**
   * REGRESSION TEST: SchemaDeduplication.computeSchemaHash produces consistent hashes.
   *
   * Verifies that the normalization logic correctly produces identical hashes
   * for semantically equivalent schemas with different JSON orderings.
   */
  test("REGRESSION: computeSchemaHash produces identical hashes for reordered schemas") {
    // Different JSON orderings of the same schema
    val schemaVariants = Seq(
      // Variant 1: field_a, field_b, field_c ordering
      """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_a","type":"text"},{"name":"field_b","type":"u64"},{"name":"field_c","type":"bool"}]}]""",
      // Variant 2: field_c, field_a, field_b ordering
      """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_c","type":"bool"},{"name":"field_a","type":"text"},{"name":"field_b","type":"u64"}]}]""",
      // Variant 3: field_b, field_c, field_a ordering
      """[{"name":"struct_0","type":"object","field_mappings":[{"name":"field_b","type":"u64"},{"name":"field_c","type":"bool"},{"name":"field_a","type":"text"}]}]""",
      // Variant 4: Different top-level key ordering (name before type vs type before name)
      """[{"type":"object","name":"struct_0","field_mappings":[{"type":"text","name":"field_a"},{"type":"u64","name":"field_b"},{"type":"bool","name":"field_c"}]}]"""
    )

    val hashes = schemaVariants.map(SchemaDeduplication.computeSchemaHash)
    val uniqueHashes = hashes.toSet

    withClue(s"All ${schemaVariants.size} schema variants should produce the same hash.\n" +
      s"Hashes: ${hashes.mkString(", ")}\n") {
      uniqueHashes.size shouldBe 1
    }
  }

  /**
   * REGRESSION TEST: deduplicateSchemas consolidates semantically identical schemas.
   *
   * Simulates the production scenario where different executors produce the same
   * schema with different JSON field orderings.
   */
  test("REGRESSION: deduplicateSchemas consolidates schemas with different JSON orderings") {
    // Simulate AddActions from different executors with different JSON orderings
    val schema1 = """[{"name":"field","type":"text","field_mappings":[{"name":"a","type":"u64"},{"name":"b","type":"bool"}]}]"""
    val schema2 = """[{"name":"field","type":"text","field_mappings":[{"name":"b","type":"bool"},{"name":"a","type":"u64"}]}]"""
    val schema3 = """[{"type":"text","name":"field","field_mappings":[{"type":"u64","name":"a"},{"type":"bool","name":"b"}]}]"""

    val addActions = Seq(
      AddAction("split1.split", Map.empty, 1000L, System.currentTimeMillis(), dataChange = true,
        docMappingJson = Some(schema1)),
      AddAction("split2.split", Map.empty, 1000L, System.currentTimeMillis(), dataChange = true,
        docMappingJson = Some(schema2)),
      AddAction("split3.split", Map.empty, 1000L, System.currentTimeMillis(), dataChange = true,
        docMappingJson = Some(schema3))
    )

    val (deduplicatedActions, schemaRegistry) = SchemaDeduplication.deduplicateSchemas(addActions, Map.empty)

    // Should have exactly 1 schema entry (all 3 are semantically identical)
    val schemaEntries = schemaRegistry.filterKeys(_.startsWith(SchemaDeduplication.SCHEMA_KEY_PREFIX))
    withClue(s"Expected 1 unique schema, got ${schemaEntries.size}. Registry keys: ${schemaEntries.keys.mkString(", ")}\n") {
      schemaEntries.size shouldBe 1
    }

    // All actions should reference the same hash
    val refs = deduplicatedActions.collect { case a: AddAction => a }.flatMap(_.docMappingRef).toSet
    withClue(s"All actions should have the same docMappingRef. Got: ${refs.mkString(", ")}\n") {
      refs.size shouldBe 1
    }
  }

  // Helper methods

  private def findScalaFiles(dir: File): Seq[File] = {
    if (!dir.exists() || !dir.isDirectory) {
      Seq.empty
    } else {
      val (files, dirs) = dir.listFiles().partition(_.isFile)
      files.filter(_.getName.endsWith(".scala")) ++ dirs.flatMap(findScalaFiles)
    }
  }

  private def findFilesWithPattern(
      dir: File,
      pattern: scala.util.matching.Regex,
      excludeFiles: Set[String]
  ): Seq[(File, Seq[String])] = {
    findScalaFiles(dir)
      .filterNot(f => excludeFiles.contains(f.getName))
      .flatMap { file =>
        val source = Source.fromFile(file)
        try {
          val matchingLines = source.getLines().zipWithIndex
            .filter { case (line, _) => pattern.findFirstIn(line).isDefined }
            .map { case (line, idx) => s"Line ${idx + 1}: ${line.trim}" }
            .toSeq

          if (matchingLines.nonEmpty) Some((file, matchingLines)) else None
        } finally {
          source.close()
        }
      }
  }
}
