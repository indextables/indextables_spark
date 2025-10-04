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

import io.indextables.spark.filters.{IndexQueryAllFilter, IndexQueryFilter}
import io.indextables.spark.TestBase

class IndexQueryBehaviorTest extends TestBase {

  test("should document IndexQuery vs IndexQueryAll implementation differences") {

    println("=== IndexQuery vs IndexQueryAll Implementation ===")
    println()

    println("1. IndexQuery (single field):")
    println("   - Creates: IndexQueryFilter(columnName, queryString)")
    println("   - parseQuery call: index.parseQuery(queryString, List(columnName).asJava)")
    println("   - Behavior: Searches only in the specified field")
    println("   - Example: title indexquery 'engine' -> finds documents where 'engine' appears in title field")
    println()

    println("2. IndexQueryAll (all fields):")
    println("   - Creates: IndexQueryAllFilter(queryString)")
    println("   - parseQuery call: index.parseQuery(queryString)  // single-argument version")
    println("   - Behavior: Searches across all indexed fields")
    println("   - Example: _indexall indexquery 'engine' -> finds documents where 'engine' appears in ANY field")
    println()

    println("3. Virtual Column Implementation:")
    println("   - _indexall exposed via SupportsMetadataColumns interface")
    println("   - V2IndexQueryExpressionRule detects _indexall column name")
    println("   - Automatically converts IndexQueryExpression(_indexall, query) -> IndexQueryAllFilter(query)")
    println("   - User syntax: _indexall indexquery 'VERIZON OR T-MOBILE'")
    println()

    println("4. Key parseQuery Difference:")
    println("   - With field list: parseQuery('engine', ['title']) -> searches only title field")
    println("   - Without field list: parseQuery('engine') -> searches ALL fields by default")
    println()

    // Test the filter objects directly
    val singleFieldFilter = IndexQueryFilter("title", "engine")
    val allFieldsFilter   = IndexQueryAllFilter("engine")

    println("5. Filter Object Verification:")
    println(s"   - IndexQueryFilter: columnName='${singleFieldFilter.columnName}', queryString='${singleFieldFilter.queryString}'")
    println(
      s"   - IndexQueryAllFilter: queryString='${allFieldsFilter.queryString}', references=${allFieldsFilter.references
          .mkString("[", ",", "]")}"
    )

    assert(singleFieldFilter.columnName == "title")
    assert(singleFieldFilter.queryString == "engine")
    assert(allFieldsFilter.queryString == "engine")
    assert(allFieldsFilter.references.isEmpty, "IndexQueryAllFilter should have empty references (searches all fields)")

    println()
    println("✅ Implementation differences documented and verified")
  }

  test("should verify metadata column implementation") {
    withTempPath { tempPath =>
      val spark = this.spark
      import spark.implicits._

      // Create test data
      val testData = Seq(
        (1, "test document", "category")
      ).toDF("id", "title", "category")

      testData.write.format("tantivy4spark").mode("overwrite").save(tempPath)

      // Verify metadata column implementation
      val tableProvider = new IndexTables4SparkTableProvider()
      import scala.jdk.CollectionConverters._
      val options = new org.apache.spark.sql.util.CaseInsensitiveStringMap(Map("path" -> tempPath).asJava)

      val table = tableProvider.getTable(null, Array.empty, options)

      assert(
        table.isInstanceOf[org.apache.spark.sql.connector.catalog.SupportsMetadataColumns],
        "Table should implement SupportsMetadataColumns"
      )

      val metadataSupport = table.asInstanceOf[org.apache.spark.sql.connector.catalog.SupportsMetadataColumns]
      val metadataColumns = metadataSupport.metadataColumns()

      assert(metadataColumns.length == 1, s"Expected 1 metadata column, got ${metadataColumns.length}")
      assert(
        metadataColumns(0).name() == "_indexall",
        s"Expected _indexall metadata column, got ${metadataColumns(0).name()}"
      )

      println("✅ _indexall metadata column properly implemented")
    }
  }
}
