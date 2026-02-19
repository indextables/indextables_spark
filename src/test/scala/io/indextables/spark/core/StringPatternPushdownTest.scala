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

import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase

/**
 * Test suite for string pattern filter pushdown configuration.
 *
 * Tests verify that:
 *   - StringStartsWith, StringEndsWith, StringContains filters work correctly
 *   - Pushdown can be enabled via configuration
 *   - COUNT works via Spark post-filtering when pattern pushdown is disabled
 *   - Aggregate pushdown works when pattern filters are enabled
 */
class StringPatternPushdownTest extends TestBase {

  private val format = "io.indextables.spark.core.IndexTables4SparkTableProvider"

  // Helper to create test data with string fields for pattern matching
  private def createPatternTestData() = {
    val sparkImplicits = spark.implicits
    import sparkImplicits._
    Seq(
      (1, "log_2024_01_01.json", "category_A", 10),
      (2, "log_2024_01_02.json", "category_B", 20),
      (3, "data_2024_01_01.csv", "category_A", 30),
      (4, "report_2024_01.pdf", "category_C", 40),
      (5, "log_2024_02_01.json", "category_A", 50)
    ).toDF("id", "filename", "category", "score")
  }

  // ============ StringStartsWith Tests ============

  test("StringStartsWith works with pushdown disabled (default)") {
    withTempPath { path =>
      // Write test data with string field configuration
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // Read WITHOUT enabling startsWith pushdown (default)
      val result = spark.read
        .format(format)
        .load(path)
        .filter(col("filename").startsWith("log_"))
        .collect()

      // Filter should work via Spark post-filtering
      result.length shouldBe 3
      result.map(_.getString(1)).foreach(_ should startWith("log_"))
    }
  }

  test("StringStartsWith works with pushdown enabled") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // Read WITH startsWith pushdown enabled
      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringStartsWith.pushdown", "true")
        .load(path)
        .filter(col("filename").startsWith("log_"))
        .collect()

      result.length shouldBe 3
      result.map(_.getString(1)).foreach(_ should startWith("log_"))
    }
  }

  test("COUNT with StringStartsWith works without pushdown via Spark post-filtering") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // Without pushdown enabled, Spark handles filtering post-retrieval
      val count = spark.read
        .format(format)
        .load(path)
        .filter(col("filename").startsWith("log_"))
        .count()

      count shouldBe 3
    }
  }

  test("COUNT with StringStartsWith succeeds when pushdown enabled") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // WITH pushdown enabled, aggregate should work
      val count = spark.read
        .format(format)
        .option("spark.indextables.filter.stringStartsWith.pushdown", "true")
        .load(path)
        .filter(col("filename").startsWith("log_"))
        .count()

      count shouldBe 3
    }
  }

  test("StringStartsWith with string field for prefix matching") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringStartsWith.pushdown", "true")
        .load(path)
        .filter(col("filename").startsWith("data_"))
        .collect()

      result.length shouldBe 1
      result.head.getString(1) shouldBe "data_2024_01_01.csv"
    }
  }

  test("StringStartsWith in compound filters with AND") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringStartsWith.pushdown", "true")
        .load(path)
        .filter(col("filename").startsWith("log_") && col("category") === "category_A")
        .collect()

      result.length shouldBe 2
    }
  }

  // ============ StringEndsWith Tests ============

  test("StringEndsWith works with pushdown disabled (default)") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .load(path)
        .filter(col("filename").endsWith(".json"))
        .collect()

      result.length shouldBe 3
      result.map(_.getString(1)).foreach(_ should endWith(".json"))
    }
  }

  test("StringEndsWith works with pushdown enabled") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringEndsWith.pushdown", "true")
        .load(path)
        .filter(col("filename").endsWith(".json"))
        .collect()

      result.length shouldBe 3
      result.map(_.getString(1)).foreach(_ should endWith(".json"))
    }
  }

  test("COUNT with StringEndsWith works without pushdown via Spark post-filtering") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // Without pushdown enabled, Spark handles filtering post-retrieval
      val count = spark.read
        .format(format)
        .load(path)
        .filter(col("filename").endsWith(".json"))
        .count()

      count shouldBe 3
    }
  }

  test("COUNT with StringEndsWith succeeds when pushdown enabled") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val count = spark.read
        .format(format)
        .option("spark.indextables.filter.stringEndsWith.pushdown", "true")
        .load(path)
        .filter(col("filename").endsWith(".json"))
        .count()

      count shouldBe 3
    }
  }

  test("StringEndsWith with string field for suffix matching") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringEndsWith.pushdown", "true")
        .load(path)
        .filter(col("filename").endsWith(".pdf"))
        .collect()

      result.length shouldBe 1
      result.head.getString(1) shouldBe "report_2024_01.pdf"
    }
  }

  test("StringEndsWith in compound filters with OR") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringEndsWith.pushdown", "true")
        .load(path)
        .filter(col("filename").endsWith(".json") || col("filename").endsWith(".csv"))
        .collect()

      result.length shouldBe 4
    }
  }

  // ============ StringContains Tests ============

  test("StringContains works with pushdown disabled (default)") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .load(path)
        .filter(col("filename").contains("2024_01"))
        .collect()

      result.length shouldBe 4
      result.map(_.getString(1)).foreach(_ should include("2024_01"))
    }
  }

  test("StringContains works with pushdown enabled") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(path)
        .filter(col("filename").contains("2024_01"))
        .collect()

      result.length shouldBe 4
      result.map(_.getString(1)).foreach(_ should include("2024_01"))
    }
  }

  test("COUNT with StringContains works without pushdown via Spark post-filtering") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // Without pushdown enabled, Spark handles filtering post-retrieval
      val count = spark.read
        .format(format)
        .load(path)
        .filter(col("filename").contains("2024"))
        .count()

      count shouldBe 5
    }
  }

  test("COUNT with StringContains succeeds when pushdown enabled") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val count = spark.read
        .format(format)
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(path)
        .filter(col("filename").contains("2024"))
        .count()

      count shouldBe 5
    }
  }

  test("StringContains with string field for substring matching") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(path)
        .filter(col("filename").contains("_02_"))
        .collect()

      result.length shouldBe 1
      result.head.getString(1) shouldBe "log_2024_02_01.json"
    }
  }

  test("StringContains in compound filters with AND") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(path)
        .filter(col("filename").contains("2024") && col("score") > 20)
        .collect()

      result.length shouldBe 3
    }
  }

  // ============ Combined/Edge Cases ============

  test("Multiple pattern filters with mixed pushdown settings") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // Enable only startsWith, not endsWith
      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringStartsWith.pushdown", "true")
        .option("spark.indextables.filter.stringEndsWith.pushdown", "false")
        .load(path)
        .filter(col("filename").startsWith("log_"))
        .collect()

      result.length shouldBe 3
    }
  }

  test("All three pattern types in single query with all enabled individually") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringStartsWith.pushdown", "true")
        .option("spark.indextables.filter.stringEndsWith.pushdown", "true")
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(path)
        .filter(
          col("filename").startsWith("log_") &&
            col("filename").endsWith(".json") &&
            col("filename").contains("2024_01")
        )
        .collect()

      result.length shouldBe 2
    }
  }

  test("Master switch enables all three pattern types") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // Use master switch to enable all pattern pushdowns
      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringPattern.pushdown", "true")
        .load(path)
        .filter(
          col("filename").startsWith("log_") &&
            col("filename").endsWith(".json") &&
            col("filename").contains("2024_01")
        )
        .collect()

      result.length shouldBe 2
    }
  }

  test("COUNT with master switch enabled") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // Use master switch and verify aggregates work
      val count = spark.read
        .format(format)
        .option("spark.indextables.filter.stringPattern.pushdown", "true")
        .load(path)
        .filter(col("filename").startsWith("log_"))
        .count()

      count shouldBe 3
    }
  }

  test("GROUP BY with pattern filter when all enabled") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.typemap.category", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringStartsWith.pushdown", "true")
        .load(path)
        .filter(col("filename").startsWith("log_"))
        .groupBy("category")
        .agg(count("*").as("cnt"))
        .collect()

      result.length should be > 0
      val totalCount = result.map(_.getLong(1)).sum
      totalCount shouldBe 3
    }
  }

  test("Pattern filters with special characters") {
    withTempPath { path =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._
      Seq(
        (1, "file_with_underscore.txt", 10),
        (2, "file-with-dash.txt", 20),
        (3, "file.with.dots.txt", 30)
      ).toDF("id", "filename", "score")
        .write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename")
        .mode("overwrite")
        .save(path)

      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(path)
        .filter(col("filename").contains("_with_"))
        .collect()

      result.length shouldBe 1
    }
  }

  test("Pattern filter with empty string matches all for contains") {
    withTempPath { path =>
      createPatternTestData().write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .option("spark.indextables.indexing.fastfields", "score,filename,category")
        .mode("overwrite")
        .save(path)

      // Empty string contains should match all
      val result = spark.read
        .format(format)
        .option("spark.indextables.filter.stringContains.pushdown", "true")
        .load(path)
        .filter(col("filename").contains(""))
        .collect()

      // All records contain empty string
      result.length shouldBe 5
    }
  }
}
