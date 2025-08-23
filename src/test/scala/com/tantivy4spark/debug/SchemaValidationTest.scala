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

package com.tantivy4spark.debug

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.sources._
import com.tantivy4spark.core.FiltersToQueryConverter

class SchemaValidationTest extends AnyFunSuite {

  test("schema validation should filter out non-existent fields") {
    val filters = Array[Filter](
      EqualTo("title", "test"),      // exists
      EqualTo("missing", "value"),   // doesn't exist
      GreaterThan("score", 10)       // exists
    )
    
    val schemaFields = Set("title", "content", "score") // "missing" not in schema
    
    // Test with schema validation
    val validatedQuery = FiltersToQueryConverter.convert(filters, Some(schemaFields))
    println(s"Query with schema validation: '$validatedQuery'")
    
    // Should only contain filters for "title" and "score", not "missing"
    assert(!validatedQuery.contains("missing"), "Query should not contain 'missing' field")
    assert(validatedQuery.contains("title"), "Query should contain 'title' field")
    assert(validatedQuery.contains("score"), "Query should contain 'score' field")
    
    // Test without schema validation (should contain all filters)
    val unvalidatedQuery = FiltersToQueryConverter.convert(filters)
    println(s"Query without schema validation: '$unvalidatedQuery'")
    
    // Should contain all fields including "missing"
    assert(unvalidatedQuery.contains("missing"), "Query should contain 'missing' field without validation")
    assert(unvalidatedQuery.contains("title"), "Query should contain 'title' field")
    assert(unvalidatedQuery.contains("score"), "Query should contain 'score' field")
  }

  test("complex filters should be validated recursively") {
    val filters = Array[Filter](
      And(
        EqualTo("title", "test"),      // exists
        EqualTo("missing", "value")    // doesn't exist
      ),
      Or(
        GreaterThan("score", 10),      // exists  
        LessThan("nonexistent", 5)     // doesn't exist
      )
    )
    
    val schemaFields = Set("title", "content", "score")
    
    val validatedQuery = FiltersToQueryConverter.convert(filters, Some(schemaFields))
    println(s"Complex query with schema validation: '$validatedQuery'")
    
    // The And filter should be completely filtered out because it contains "missing"
    // The Or filter should be completely filtered out because it contains "nonexistent"
    // So the result should be empty
    assert(validatedQuery.isEmpty, s"Query should be empty but was: '$validatedQuery'")
  }
}