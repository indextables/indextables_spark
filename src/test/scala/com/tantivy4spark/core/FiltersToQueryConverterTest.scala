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


package com.tantivy4spark.core

import com.tantivy4spark.TestBase
import org.apache.spark.sql.sources._

class FiltersToQueryConverterTest extends TestBase {

  test("should convert EqualTo filter") {
    val filter = EqualTo("name", "John Doe")
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe """name:"John Doe""""
  }

  test("should convert GreaterThan filter") {
    val filter = GreaterThan("age", 30)
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe "age:{30 TO *}"
  }

  test("should convert LessThan filter") {
    val filter = LessThan("salary", 50000)
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe "salary:{* TO 50000}"
  }

  test("should convert GreaterThanOrEqual filter") {
    val filter = GreaterThanOrEqual("age", 25)
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe "age:[25 TO *]"
  }

  test("should convert LessThanOrEqual filter") {
    val filter = LessThanOrEqual("salary", 100000)
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe "salary:[* TO 100000]"
  }

  test("should convert In filter") {
    val filter = In("role", Array("Engineer", "Manager", "Designer"))
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe """role:("Engineer" OR "Manager" OR "Designer")"""
  }

  test("should convert IsNull filter") {
    val filter = IsNull("optional_field")
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe "NOT _exists_:optional_field"
  }

  test("should convert IsNotNull filter") {
    val filter = IsNotNull("required_field")
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe "_exists_:required_field"
  }

  test("should convert And filter") {
    val filter = And(
      EqualTo("role", "Engineer"),
      GreaterThan("salary", 70000)
    )
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe """(role:"Engineer") AND (salary:{70000 TO *})"""
  }

  test("should convert Or filter") {
    val filter = Or(
      EqualTo("role", "Engineer"),
      EqualTo("role", "Manager")
    )
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe """(role:"Engineer") OR (role:"Manager")"""
  }

  test("should convert Not filter") {
    val filter = Not(EqualTo("active", true))
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe """NOT (active:"true")"""
  }

  test("should convert StringStartsWith filter") {
    val filter = StringStartsWith("name", "John")
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe "name:John*"
  }

  test("should convert StringEndsWith filter") {
    val filter = StringEndsWith("name", "Doe")
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe "name:*Doe"
  }

  test("should convert StringContains filter") {
    val filter = StringContains("description", "spark")
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe "description:*spark*"
  }

  test("should combine multiple filters with AND") {
    val filters = Array(
      EqualTo("role", "Engineer"),
      GreaterThan("age", 25),
      LessThan("salary", 100000)
    )
    val query = FiltersToQueryConverter.convert(filters.asInstanceOf[Array[Filter]])
    
    query shouldBe """(role:"Engineer") AND (age:{25 TO *}) AND (salary:{* TO 100000})"""
  }

  test("should handle empty filter array") {
    val query = FiltersToQueryConverter.convert(Array.empty)
    query shouldBe ""
  }

  test("should handle single filter") {
    val filter = EqualTo("id", 123)
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query shouldBe """id:"123""""
  }

  test("should escape special characters in values") {
    val filter = EqualTo("description", "test+query-with:special*chars")
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query should include("test\\+query\\-with\\:special\\*chars")
  }

  test("should handle complex nested filters") {
    val filter = And(
      Or(
        EqualTo("role", "Engineer"),
        EqualTo("role", "Developer")
      ),
      And(
        GreaterThan("age", 25),
        LessThan("age", 40)
      )
    )
    val query = FiltersToQueryConverter.convert(Array(filter))
    
    query should include("role:\"Engineer\"")
    query should include("role:\"Developer\"")
    query should include("age:{25 TO *}")
    query should include("age:{* TO 40}")
    query should include(" OR ")
    query should include(" AND ")
  }
}