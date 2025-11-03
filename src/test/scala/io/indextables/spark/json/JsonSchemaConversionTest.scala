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

import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.indextables.spark.core.IndexTables4SparkOptions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class JsonSchemaConversionTest extends AnyFunSuite with Matchers {

  private def createOptions(configMap: Map[String, String] = Map.empty): IndexTables4SparkOptions = {
    new IndexTables4SparkOptions(new CaseInsensitiveStringMap(configMap.asJava))
  }

  test("shouldUseJsonField detects StructType") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    val structField = StructField("user", StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    )))

    mapper.shouldUseJsonField(structField) shouldBe true
  }

  test("shouldUseJsonField detects ArrayType") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    val arrayField = StructField("tags", ArrayType(StringType))

    mapper.shouldUseJsonField(arrayField) shouldBe true
  }

  test("shouldUseJsonField detects StringType with json configuration") {
    val options = createOptions(Map(
      "spark.indextables.indexing.typemap.payload" -> "json"
    ))
    val mapper = new SparkSchemaToTantivyMapper(options)

    val stringField = StructField("payload", StringType)

    mapper.shouldUseJsonField(stringField) shouldBe true
  }

  test("shouldUseJsonField returns false for regular StringType") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    val stringField = StructField("name", StringType)

    mapper.shouldUseJsonField(stringField) shouldBe false
  }

  test("shouldUseJsonField returns false for primitive types") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    mapper.shouldUseJsonField(StructField("age", IntegerType)) shouldBe false
    mapper.shouldUseJsonField(StructField("score", DoubleType)) shouldBe false
    mapper.shouldUseJsonField(StructField("active", BooleanType)) shouldBe false
  }

  test("getFieldType returns configured type") {
    val options = createOptions(Map(
      "spark.indextables.indexing.typemap.title" -> "string",
      "spark.indextables.indexing.typemap.content" -> "text",
      "spark.indextables.indexing.typemap.payload" -> "json"
    ))
    val mapper = new SparkSchemaToTantivyMapper(options)

    mapper.getFieldType("title") shouldBe "string"
    mapper.getFieldType("content") shouldBe "text"
    mapper.getFieldType("payload") shouldBe "json"
  }

  test("getFieldType returns default 'string' for unconfigured fields") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    mapper.getFieldType("unknown_field") shouldBe "string"
  }

  test("requiresRangeQueries detects fast field configuration") {
    val options = createOptions(Map(
      "spark.indextables.indexing.fastfields" -> "score,timestamp,value"
    ))
    val mapper = new SparkSchemaToTantivyMapper(options)

    mapper.requiresRangeQueries("score") shouldBe true
    mapper.requiresRangeQueries("timestamp") shouldBe true
    mapper.requiresRangeQueries("value") shouldBe true
    mapper.requiresRangeQueries("other") shouldBe false
  }

  test("validateJsonFieldConfiguration accepts valid Struct configuration") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    val schema = StructType(Seq(
      StructField("id", StringType),
      StructField("user", StructType(Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )))
    ))

    noException should be thrownBy mapper.validateJsonFieldConfiguration(schema)
  }

  test("validateJsonFieldConfiguration accepts valid Array configuration") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    val schema = StructType(Seq(
      StructField("id", StringType),
      StructField("tags", ArrayType(StringType))
    ))

    noException should be thrownBy mapper.validateJsonFieldConfiguration(schema)
  }

  test("validateJsonFieldConfiguration rejects conflicting Struct type mapping") {
    val options = createOptions(Map(
      "spark.indextables.indexing.typemap.user" -> "string"
    ))
    val mapper = new SparkSchemaToTantivyMapper(options)

    val schema = StructType(Seq(
      StructField("user", StructType(Seq(
        StructField("name", StringType)
      )))
    ))

    an[IllegalArgumentException] should be thrownBy {
      mapper.validateJsonFieldConfiguration(schema)
    }
  }

  test("validateJsonFieldConfiguration accepts valid JSON string configuration") {
    val options = createOptions(Map(
      "spark.indextables.indexing.typemap.payload" -> "json"
    ))
    val mapper = new SparkSchemaToTantivyMapper(options)

    val schema = StructType(Seq(
      StructField("id", StringType),
      StructField("payload", StringType)
    ))

    noException should be thrownBy mapper.validateJsonFieldConfiguration(schema)
  }

  test("validateJsonFieldConfiguration rejects invalid field type") {
    val options = createOptions(Map(
      "spark.indextables.indexing.typemap.name" -> "invalid_type"
    ))
    val mapper = new SparkSchemaToTantivyMapper(options)

    val schema = StructType(Seq(
      StructField("name", StringType)
    ))

    an[IllegalArgumentException] should be thrownBy {
      mapper.validateJsonFieldConfiguration(schema)
    }
  }

  test("validateJsonFieldConfiguration rejects json type for non-JSON fields") {
    val options = createOptions(Map(
      "spark.indextables.indexing.typemap.age" -> "json"
    ))
    val mapper = new SparkSchemaToTantivyMapper(options)

    val schema = StructType(Seq(
      StructField("age", IntegerType)
    ))

    an[IllegalArgumentException] should be thrownBy {
      mapper.validateJsonFieldConfiguration(schema)
    }
  }

  test("getJsonFieldConfig returns correct configuration") {
    val options = createOptions(Map(
      "spark.indextables.indexing.fastfields" -> "payload"
    ))
    val mapper = new SparkSchemaToTantivyMapper(options)

    val config = mapper.getJsonFieldConfig("payload")
    config.parseOnWrite shouldBe true
    config.failOnInvalidJson shouldBe false
    config.enableRangeQueries shouldBe true
  }

  test("getJsonFieldConfig returns default configuration for unconfigured field") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    val config = mapper.getJsonFieldConfig("other")
    config.parseOnWrite shouldBe true
    config.failOnInvalidJson shouldBe false
    config.enableRangeQueries shouldBe false
  }

  test("validateJsonFieldConfiguration handles nested structs") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    val schema = StructType(Seq(
      StructField("user", StructType(Seq(
        StructField("address", StructType(Seq(
          StructField("city", StringType),
          StructField("coordinates", StructType(Seq(
            StructField("lat", DoubleType),
            StructField("lon", DoubleType)
          )))
        )))
      )))
    ))

    noException should be thrownBy mapper.validateJsonFieldConfiguration(schema)
  }

  test("validateJsonFieldConfiguration handles array of structs") {
    val options = createOptions()
    val mapper = new SparkSchemaToTantivyMapper(options)

    val schema = StructType(Seq(
      StructField("reviews", ArrayType(StructType(Seq(
        StructField("rating", IntegerType),
        StructField("comment", StringType)
      ))))
    ))

    noException should be thrownBy mapper.validateJsonFieldConfiguration(schema)
  }
}
