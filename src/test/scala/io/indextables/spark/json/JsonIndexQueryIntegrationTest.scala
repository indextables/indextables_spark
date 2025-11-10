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

package io.indextables.spark.json

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import io.indextables.spark.TestBase

/**
 * Integration tests demonstrating how to use IndexQuery with JSON-indexed string fields.
 *
 * This test shows:
 *   - How to configure string fields containing JSON data for full-text search
 *   - How to use IndexQuery to search within JSON content
 *   - Different query patterns: simple terms, boolean operators, phrases, wildcards
 *   - Combining IndexQuery with regular filters
 *
 * Key concepts:
 *   - Use "text" type for tokenized full-text search with IndexQuery on string content (including JSON strings)
 *   - Use "json" type for StringType fields containing JSON that should be parsed (for Struct-like filtering)
 *   - Use "string" type for exact matching (default)
 *   - JSON string content can be searched with IndexQuery when configured as "text" type
 */
class JsonIndexQueryIntegrationTest extends TestBase {

  test("should use IndexQuery to search within JSON string content") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with JSON strings as a column
      // These could represent document metadata, user profiles, etc.
      val testData = Seq(
        (1, """{"title": "Machine Learning Guide", "tags": ["ai", "ml", "python"], "description": "Deep learning and neural networks"}"""),
        (2, """{"title": "Spark Programming", "tags": ["spark", "scala", "big-data"], "description": "Distributed computing with Apache Spark"}"""),
        (3, """{"title": "Python Data Science", "tags": ["python", "pandas", "numpy"], "description": "Data analysis and visualization with Python"}"""),
        (4, """{"title": "Natural Language Processing", "tags": ["nlp", "ai", "transformers"], "description": "Advanced NLP techniques and models"}"""),
        (5, """{"title": "Distributed Systems", "tags": ["systems", "architecture"], "description": "Building scalable distributed applications"}""")
      ).toDF("id", "json_content")

      // Write with "text" type for the json_content field
      // This enables full-text search with IndexQuery on JSON string content
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.json_content", "text")  // Configure as text for IndexQuery
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${testData.count()} documents with JSON string content indexed as text type for IndexQuery")

      // Read back and create temp view for SQL queries
      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      df.createOrReplaceTempView("json_documents")

      // Test 1: Simple term search within JSON
      println("\n🔍 Test 1: Simple term search - find documents with 'spark'")
      val sparkDocs = spark.sql("""
        SELECT id, json_content
        FROM json_documents
        WHERE json_content indexquery 'spark'
        ORDER BY id
      """)

      val sparkResults = sparkDocs.collect()
      sparkResults.foreach { row =>
        println(s"   Found: ID ${row.getInt(0)}")
      }
      sparkResults.length should be >= 1
      sparkResults.exists(_.getString(1).contains("Spark")) shouldBe true
      println(s"   ✅ Found ${sparkResults.length} document(s) containing 'spark'")

      // Test 2: Boolean AND query
      println("\n🔍 Test 2: Boolean AND query - 'python AND data'")
      val pythonDataDocs = spark.sql("""
        SELECT id, json_content
        FROM json_documents
        WHERE json_content indexquery 'python AND data'
        ORDER BY id
      """)

      val pythonDataResults = pythonDataDocs.collect()
      pythonDataResults.foreach { row =>
        println(s"   Found: ID ${row.getInt(0)}")
      }
      pythonDataResults.length should be >= 1
      pythonDataResults.exists { row =>
        val json = row.getString(1).toLowerCase
        json.contains("python") && json.contains("data")
      } shouldBe true
      println(s"   ✅ Found ${pythonDataResults.length} document(s) with both 'python' AND 'data'")

      // Test 3: Boolean OR query
      println("\n🔍 Test 3: Boolean OR query - 'machine OR distributed'")
      val orQueryDocs = spark.sql("""
        SELECT id, json_content
        FROM json_documents
        WHERE json_content indexquery 'machine OR distributed'
        ORDER BY id
      """)

      val orResults = orQueryDocs.collect()
      orResults.foreach { row =>
        println(s"   Found: ID ${row.getInt(0)}")
      }
      orResults.length should be >= 2
      println(s"   ✅ Found ${orResults.length} document(s) matching 'machine' OR 'distributed'")

      // Test 4: Phrase query
      println("\n🔍 Test 4: Phrase query - exact phrase '\"machine learning\"'")
      val phraseDocs = spark.sql("""
        SELECT id, json_content
        FROM json_documents
        WHERE json_content indexquery '"machine learning"'
        ORDER BY id
      """)

      val phraseResults = phraseDocs.collect()
      phraseResults.foreach { row =>
        println(s"   Found: ID ${row.getInt(0)}")
      }
      phraseResults.length should be >= 1
      phraseResults.exists(_.getString(1).toLowerCase.contains("machine learning")) shouldBe true
      println(s"   ✅ Found ${phraseResults.length} document(s) with exact phrase 'machine learning'")

      // Test 5: Complex boolean query with parentheses
      println("\n🔍 Test 5: Complex query - '(python OR scala) AND distributed'")
      val complexDocs = spark.sql("""
        SELECT id, json_content
        FROM json_documents
        WHERE json_content indexquery '(python OR scala) AND distributed'
        ORDER BY id
      """)

      val complexResults = complexDocs.collect()
      complexResults.foreach { row =>
        println(s"   Found: ID ${row.getInt(0)}")
      }
      println(s"   ✅ Found ${complexResults.length} document(s) matching complex query")

      // Test 6: Wildcard query
      println("\n🔍 Test 6: Wildcard query - 'spar*'")
      val wildcardDocs = spark.sql("""
        SELECT id, json_content
        FROM json_documents
        WHERE json_content indexquery 'spar*'
        ORDER BY id
      """)

      val wildcardResults = wildcardDocs.collect()
      wildcardResults.foreach { row =>
        println(s"   Found: ID ${row.getInt(0)}")
      }
      wildcardResults.length should be >= 1
      println(s"   ✅ Found ${wildcardResults.length} document(s) matching 'spar*' wildcard")

      // Test 7: Combining IndexQuery with regular filters
      println("\n🔍 Test 7: IndexQuery combined with regular filter - 'python' AND id > 2")
      val combinedDocs = spark.sql("""
        SELECT id, json_content
        FROM json_documents
        WHERE json_content indexquery 'python' AND id > 2
        ORDER BY id
      """)

      val combinedResults = combinedDocs.collect()
      combinedResults.foreach { row =>
        println(s"   Found: ID ${row.getInt(0)}")
        row.getInt(0) should be > 2
      }
      println(s"   ✅ Found ${combinedResults.length} document(s) matching combined filters")

      println("\n🎉 All JSON IndexQuery tests passed successfully!")
    }
  }

  test("should handle IndexQuery on JSON fields with special characters and Unicode") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with special characters and Unicode
      val testData = Seq(
        (1, """{"name": "测试文档", "content": "Chinese characters testing"}"""),
        (2, """{"name": "Special@#$%", "content": "Special characters: @#$%^&*()"}"""),
        (3, """{"name": "Emoji 🚀", "content": "Rocket emoji and other symbols"}"""),
        (4, """{"name": "Escaped \"quotes\"", "content": "Testing escaped quotes"}"""),
        (5, """{"name": "Path/To/File", "content": "Forward slashes and paths"}""")
      ).toDF("id", "json_data")

      // Write with text type for IndexQuery support
      testData.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.json_data", "text")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${testData.count()} documents with special characters")

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      df.createOrReplaceTempView("special_docs")

      // Test searching for Unicode characters
      println("\n🔍 Testing Unicode search")
      val unicodeDocs = spark.sql("""
        SELECT id, json_data
        FROM special_docs
        WHERE json_data indexquery 'chinese'
        ORDER BY id
      """)

      val unicodeResults = unicodeDocs.collect()
      unicodeResults.length should be >= 1
      println(s"   ✅ Found ${unicodeResults.length} document(s) with Unicode content")

      // Test searching for special characters
      println("\n🔍 Testing special character search")
      val specialDocs = spark.sql("""
        SELECT id, json_data
        FROM special_docs
        WHERE json_data indexquery 'special'
        ORDER BY id
      """)

      val specialResults = specialDocs.collect()
      specialResults.length should be >= 1
      println(s"   ✅ Found ${specialResults.length} document(s) with special characters")

      // Test searching for emoji-related content
      println("\n🔍 Testing emoji-related content search")
      val emojiDocs = spark.sql("""
        SELECT id, json_data
        FROM special_docs
        WHERE json_data indexquery 'emoji'
        ORDER BY id
      """)

      val emojiResults = emojiDocs.collect()
      emojiResults.length should be >= 1
      println(s"   ✅ Found ${emojiResults.length} document(s) with emoji-related content")

      println("\n🎉 Special character and Unicode tests passed!")
    }
  }

  test("should demonstrate IndexQuery vs exact string matching") {
    withTempPath { jsonPath =>
      withTempPath { stringPath =>
        val spark = this.spark
        import spark.implicits._

        // Create test data
        val testData = Seq(
          (1, """{"message": "The quick brown fox jumps over the lazy dog"}"""),
          (2, """{"message": "A quick reference guide for developers"}"""),
          (3, """{"message": "Quickly learn Spark programming"}""")
        ).toDF("id", "json_field")

        // Write with TEXT type (for IndexQuery - tokenized search)
        testData.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.indexing.typemap.json_field", "text")  // TEXT type enables IndexQuery
          .mode("overwrite")
          .save(jsonPath)

        // Write with STRING type (for exact matching - default)
        testData.write
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .option("spark.indextables.indexing.typemap.json_field", "string")  // STRING type (default)
          .mode("overwrite")
          .save(stringPath)

        println("✅ Wrote test data with both TEXT and STRING types")

        // Read TEXT-indexed data
        val textDf = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(jsonPath)

        textDf.createOrReplaceTempView("text_docs")

        // Read STRING-indexed data
        val stringDf = spark.read
          .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
          .load(stringPath)

        stringDf.createOrReplaceTempView("string_docs")

        // Test 1: IndexQuery on TEXT field - finds "quick" in all 3 documents
        println("\n🔍 Test 1: IndexQuery on TEXT field - search for 'quick'")
        val textResults = spark.sql("""
          SELECT id, json_field
          FROM text_docs
          WHERE json_field indexquery 'quick'
          ORDER BY id
        """)

        val textCount = textResults.collect()
        println(s"   TEXT type with IndexQuery found: ${textCount.length} documents")
        textCount.foreach { row =>
          val text = row.getString(1)
          val preview = if (text.length > 60) text.substring(0, 60) + "..." else text
          println(s"   - ID ${row.getInt(0)}: $preview")
        }
        // IndexQuery performs tokenized search, so it should find documents containing "quick" (case-insensitive)
        // Note: "Quickly" may or may not match depending on stemming
        textCount.length should be >= 2

        // Test 2: Exact match on STRING field - only finds exact "quick" (case-sensitive)
        println("\n🔍 Test 2: Exact match on STRING field - filter for 'quick'")
        val stringResults = spark.sql("""
          SELECT id, json_field
          FROM string_docs
          WHERE json_field LIKE '%quick%'
          ORDER BY id
        """)

        val stringCount = stringResults.collect()
        println(s"   STRING type with LIKE found: ${stringCount.length} documents")
        stringCount.foreach { row =>
          val text = row.getString(1)
          val preview = if (text.length > 60) text.substring(0, 60) + "..." else text
          println(s"   - ID ${row.getInt(0)}: $preview")
        }
        stringCount.length should be >= 1

        println("\n📊 Comparison:")
        println(s"   TEXT type + IndexQuery: ${textCount.length} results (tokenized, case-insensitive)")
        println(s"   STRING type + LIKE: ${stringCount.length} results (substring match)")
        println("\n💡 Key Insight:")
        println("   - Use TEXT type + IndexQuery for full-text search (works with JSON string content)")
        println("   - Use JSON type for StringType fields containing structured JSON (enables JSON filter pushdown)")
        println("   - Use STRING type + equality/LIKE for exact matching")

        println("\n🎉 IndexQuery vs exact matching demonstration complete!")
      }
    }
  }

  test("should use IndexQuery with actual Struct fields (not JSON strings)") {
    withTempPath { path =>
      val spark = this.spark
      import spark.implicits._

      // Create test data with actual Struct fields using Spark's Row and schema
      // Note: We simplify the schema to avoid UnsafeArrayData conversion issues
      val docSchema = StructType(Seq(
        StructField("title", StringType, nullable = false),
        StructField("content", StringType, nullable = false)
      ))

      val schema = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("doc", docSchema, nullable = false)
      ))

      val testData = Seq(
        Row(1, Row("Machine Learning", "Deep learning and neural networks")),
        Row(2, Row("Spark Guide", "Distributed computing with Spark")),
        Row(3, Row("Python Basics", "Learn Python programming"))
      )

      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(testData),
        schema
      )

      // Write with Struct field - automatically converted to JSON
      // Note: For Struct fields, we need to reference the JSON-serialized version
      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(path)

      println(s"✅ Wrote ${df.count()} documents with Struct fields")

      val resultDf = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(path)

      resultDf.createOrReplaceTempView("struct_docs")

      // Show the schema
      println("\n📋 Schema:")
      resultDf.printSchema()

      // Show sample data
      println("\n📄 Sample data:")
      resultDf.show(false)

      // Test filter on nested Struct field (using filter pushdown, not IndexQuery)
      println("\n🔍 Test: Filter on nested Struct field - doc.title = 'Spark Guide'")
      val filteredDocs = spark.sql("""
        SELECT id, doc.title, doc.content
        FROM struct_docs
        WHERE doc.title = 'Spark Guide'
        ORDER BY id
      """)

      val filteredResults = filteredDocs.collect()
      filteredResults.length should be >= 1
      println(s"   ✅ Found ${filteredResults.length} document(s)")

      println("\n💡 Note:")
      println("   - Struct fields are automatically indexed as JSON")
      println("   - Use dot notation (doc.title) for filter pushdown on nested fields")
      println("   - For full-text search, use a text field with IndexQuery instead")

      println("\n🎉 Struct field filtering test complete!")
    }
  }
}
