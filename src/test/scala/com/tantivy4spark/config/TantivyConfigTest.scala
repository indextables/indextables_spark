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

package com.tantivy4spark.config

import com.tantivy4spark.{TantivyTestBase, TestSchemas, TestOptions}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Success, Failure}

class TantivyConfigTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  "TantivyConfig" should "create configuration from Spark schema with default options" in {
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, testOptions.toMap)
    
    config.basePath shouldBe testOptions.basePath
    config.indexes should have length 1
    
    val index = config.indexes.head
    index.indexId shouldBe testOptions.indexId
    index.docMapping.fieldMappings should not be empty
    index.docMapping.mode shouldBe "strict"
  }
  
  it should "map Spark data types to Tantivy types correctly" in {
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, testOptions.toMap)
    val fieldMappings = config.indexes.head.docMapping.fieldMappings
    
    fieldMappings("id").fieldType shouldBe "i64"
    fieldMappings("title").fieldType shouldBe "text"
    fieldMappings("content").fieldType shouldBe "text"
    fieldMappings("timestamp").fieldType shouldBe "datetime"
    fieldMappings("score").fieldType shouldBe "f64"
    fieldMappings("active").fieldType shouldBe "bool"
  }
  
  it should "handle all supported Spark data types" in {
    val allTypesSchema = StructType(Seq(
      StructField("string_field", StringType),
      StructField("byte_field", ByteType),
      StructField("short_field", ShortType),
      StructField("int_field", IntegerType),
      StructField("long_field", LongType),
      StructField("float_field", FloatType),
      StructField("double_field", DoubleType),
      StructField("boolean_field", BooleanType),
      StructField("timestamp_field", TimestampType),
      StructField("date_field", DateType),
      StructField("decimal_field", DecimalType(10, 2)),
      StructField("array_field", ArrayType(StringType)),
      StructField("map_field", MapType(StringType, IntegerType)),
      StructField("struct_field", StructType(Seq(StructField("nested", StringType))))
    ))
    
    val config = TantivyConfig.fromSpark(allTypesSchema, testOptions.toMap)
    val fieldMappings = config.indexes.head.docMapping.fieldMappings
    
    fieldMappings("string_field").fieldType shouldBe "text"
    fieldMappings("byte_field").fieldType shouldBe "i32"
    fieldMappings("short_field").fieldType shouldBe "i32"
    fieldMappings("int_field").fieldType shouldBe "i32"
    fieldMappings("long_field").fieldType shouldBe "i64"
    fieldMappings("float_field").fieldType shouldBe "f32"
    fieldMappings("double_field").fieldType shouldBe "f64"
    fieldMappings("boolean_field").fieldType shouldBe "bool"
    fieldMappings("timestamp_field").fieldType shouldBe "datetime"
    fieldMappings("date_field").fieldType shouldBe "date"
    fieldMappings("decimal_field").fieldType shouldBe "text"
    fieldMappings("array_field").fieldType shouldBe "json"
    fieldMappings("map_field").fieldType shouldBe "json"
    fieldMappings("struct_field").fieldType shouldBe "json"
  }
  
  it should "respect custom field options" in {
    val customOptions = testOptions.copy(additional = Map(
      "field.title.indexed" -> "false",
      "field.content.stored" -> "false",
      "field.score.fast" -> "true",
      "field.active.field_norms" -> "false"
    )).toMap
    
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, customOptions)
    val fieldMappings = config.indexes.head.docMapping.fieldMappings
    
    fieldMappings("title").indexed shouldBe false
    fieldMappings("content").stored shouldBe false
    fieldMappings("score").fast shouldBe true
    fieldMappings("active").fieldNorms shouldBe false
  }
  
  it should "handle custom mapping mode" in {
    val customOptions = testOptions.copy(additional = Map(
      "mapping.mode" -> "lenient"
    )).toMap
    
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, customOptions)
    config.indexes.head.docMapping.mode shouldBe "lenient"
  }
  
  it should "handle custom timestamp field" in {
    val customOptions = testOptions.copy(additional = Map(
      "timestamp.field" -> "created_at"
    )).toMap
    
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, customOptions)
    config.indexes.head.docMapping.timestampField shouldBe Some("created_at")
  }
  
  it should "auto-detect default search fields" in {
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, testOptions.toMap)
    val defaultSearchFields = config.indexes.head.docMapping.defaultSearchFields
    
    defaultSearchFields should contain("title")
    defaultSearchFields should contain("content")
    defaultSearchFields should not contain("id")
    defaultSearchFields should not contain("score")
  }
  
  it should "respect custom default search fields" in {
    val customOptions = testOptions.copy(additional = Map(
      "default.search.fields" -> "title,content,description"
    )).toMap
    
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, customOptions)
    val defaultSearchFields = config.indexes.head.docMapping.defaultSearchFields
    
    defaultSearchFields shouldBe List("title", "content", "description")
  }
  
  it should "serialize to JSON correctly" in {
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, testOptions.toMap)
    val json = TantivyConfig.toJson(config)
    
    json should include("base_path")
    json should include("indexes")
    json should include("metastore")
    json should include("field_mappings")
  }
  
  it should "deserialize from JSON correctly" in {
    val originalConfig = TantivyConfig.fromSpark(TestSchemas.basicSchema, testOptions.toMap)
    val json = TantivyConfig.toJson(originalConfig)
    
    TantivyConfig.fromJson(json) match {
      case Success(deserializedConfig) =>
        deserializedConfig.basePath shouldBe originalConfig.basePath
        deserializedConfig.indexes should have length originalConfig.indexes.length
        deserializedConfig.indexes.head.indexId shouldBe originalConfig.indexes.head.indexId
      case Failure(exception) =>
        fail(s"Failed to deserialize config: ${exception.getMessage}")
    }
  }
  
  it should "handle malformed JSON gracefully" in {
    val malformedJson = """{"invalid": "json structure"""
    
    TantivyConfig.fromJson(malformedJson) match {
      case Success(_) => fail("Should have failed with malformed JSON")
      case Failure(_) => // Expected
    }
  }
  
  it should "validate configuration successfully" in {
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, testOptions.toMap)
    val errors = TantivyConfig.validateConfig(config)
    
    errors shouldBe empty
  }
  
  it should "detect validation errors" in {
    val invalidConfig = TantivyGlobalConfig(
      basePath = "", // Empty base path
      metastore = TantivyMetastoreConfig("", "file"),
      storageUri = "file://invalid",
      indexes = List(
        TantivyIndexConfig(
          indexId = "", // Empty index ID
          indexUri = "file://test",
          docMapping = TantivyDocMapping(
            fieldMappings = Map.empty, // No field mappings
            timestampField = Some("non_existent_field") // Invalid timestamp field
          )
        )
      )
    )
    
    val errors = TantivyConfig.validateConfig(invalidConfig)
    
    errors should not be empty
    errors should contain("Base path cannot be empty")
    errors should contain("Index ID cannot be empty")
    errors.exists(_.contains("must have at least one field mapping")) shouldBe true
    errors.exists(_.contains("Timestamp field 'non_existent_field' not found")) shouldBe true
  }
  
  it should "handle custom indexing settings" in {
    val customOptions = testOptions.copy(additional = Map(
      "commit.timeout.secs" -> "120",
      "split.num.docs" -> "5000000",
      "split.num.bytes" -> "1000000000"
    )).toMap
    
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, customOptions)
    val indexingSettings = config.indexes.head.indexingSettings
    
    indexingSettings.commitTimeoutSecs shouldBe 120
    indexingSettings.splitNumDocs shouldBe 5000000L
    indexingSettings.splitNumBytes shouldBe 1000000000L
  }
  
  it should "handle custom search settings" in {
    val customOptions = testOptions.copy(additional = Map(
      "max.hits" -> "5000"
    )).toMap
    
    val config = TantivyConfig.fromSpark(TestSchemas.basicSchema, customOptions)
    val searchSettings = config.indexes.head.searchSettings
    
    searchSettings.maxHits shouldBe 5000
  }
}

class TantivyConfigDataClassTest extends AnyFlatSpec with Matchers {
  
  "TantivyFieldMapping" should "be created with default values" in {
    val mapping = TantivyFieldMapping("text")
    
    mapping.fieldType shouldBe "text"
    mapping.indexed shouldBe true
    mapping.stored shouldBe true
    mapping.fast shouldBe false
    mapping.fieldNorms shouldBe true
  }
  
  it should "support custom values" in {
    val mapping = TantivyFieldMapping(
      fieldType = "i64",
      indexed = false,
      stored = false,
      fast = true,
      fieldNorms = false
    )
    
    mapping.fieldType shouldBe "i64"
    mapping.indexed shouldBe false
    mapping.stored shouldBe false
    mapping.fast shouldBe true
    mapping.fieldNorms shouldBe false
  }
  
  "TantivyDocMapping" should "be created with default values" in {
    val mapping = TantivyDocMapping(fieldMappings = Map("field1" -> TantivyFieldMapping("text")))
    
    mapping.mode shouldBe "strict"
    mapping.timestampField shouldBe Some("_timestamp")
    mapping.defaultSearchFields shouldBe List.empty
  }
  
  "TantivyIndexConfig" should "be created with all required fields" in {
    val docMapping = TantivyDocMapping(fieldMappings = Map("test" -> TantivyFieldMapping("text")))
    val config = TantivyIndexConfig(
      indexId = "test_index",
      indexUri = "file://test",
      docMapping = docMapping
    )
    
    config.indexId shouldBe "test_index"
    config.indexUri shouldBe "file://test"
    config.docMapping shouldBe docMapping
    config.searchSettings shouldBe TantivySearchSettings()
    config.indexingSettings shouldBe TantivyIndexingSettings()
  }
  
  "TantivyGlobalConfig" should "be created with all required fields" in {
    val metastore = TantivyMetastoreConfig("file://metastore", "file")
    val config = TantivyGlobalConfig(
      basePath = "/test/path",
      metastore = metastore,
      storageUri = "file://storage"
    )
    
    config.basePath shouldBe "/test/path"
    config.metastore shouldBe metastore
    config.storageUri shouldBe "file://storage"
    config.indexes shouldBe List.empty
  }
}