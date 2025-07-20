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

import com.tantivy4spark.{TantivyTestBase, TestSchemas, TestOptions, FileTestUtils}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Success, Failure}
import java.nio.file.{Files, Paths}

class SchemaManagerTest extends AnyFlatSpec with Matchers with TantivyTestBase {
  
  private def createSchemaManager(): SchemaManager = {
    val options = testOptions.copy(additional = Map(
      "schema.path" -> testDir.resolve("schemas").toString
    )).toMap
    new SchemaManager(options)
  }
  
  "SchemaManager" should "initialize with custom schema path" in {
    val manager = createSchemaManager()
    manager shouldNot be(null)
  }
  
  it should "save schema successfully" in {
    val manager = createSchemaManager()
    val indexId = "test_save_schema"
    
    val result = manager.saveSchema(indexId, TestSchemas.basicSchema)
    result shouldBe a[Success[_]]
    
    // Verify file was created
    val schemaFile = testDir.resolve("schemas").resolve(s"$indexId.json")
    Files.exists(schemaFile) shouldBe true
  }
  
  it should "load saved schema correctly" in {
    val manager = createSchemaManager()
    val indexId = "test_load_schema"
    
    // Save schema first
    manager.saveSchema(indexId, TestSchemas.basicSchema) shouldBe a[Success[_]]
    
    // Load schema
    val loadResult = manager.loadSchema(indexId)
    loadResult shouldBe a[Success[_]]
    
    loadResult match {
      case Success(config) =>
        config.indexes should have length 1
        config.indexes.head.indexId shouldBe indexId
      case Failure(exception) =>
        fail(s"Failed to load schema: ${exception.getMessage}")
    }
  }
  
  it should "fail to load non-existent schema" in {
    val manager = createSchemaManager()
    val loadResult = manager.loadSchema("non_existent_index")
    
    loadResult shouldBe a[Failure[_]]
  }
  
  it should "check schema existence correctly" in {
    val manager = createSchemaManager()
    val indexId = "test_exists_schema"
    
    // Should not exist initially
    manager.schemaExists(indexId) shouldBe false
    
    // Save schema
    manager.saveSchema(indexId, TestSchemas.basicSchema)
    
    // Should exist now
    manager.schemaExists(indexId) shouldBe true
  }
  
  it should "list all schemas" in {
    val manager = createSchemaManager()
    val indexIds = List("schema1", "schema2", "schema3")
    
    // Save multiple schemas
    indexIds.foreach { indexId =>
      manager.saveSchema(indexId, TestSchemas.basicSchema)
    }
    
    val listedSchemas = manager.listSchemas()
    listedSchemas should contain theSameElementsAs indexIds
  }
  
  it should "return empty list when no schemas exist" in {
    val manager = createSchemaManager()
    val schemas = manager.listSchemas()
    
    schemas shouldBe empty
  }
  
  it should "delete schema successfully" in {
    val manager = createSchemaManager()
    val indexId = "test_delete_schema"
    
    // Save schema first
    manager.saveSchema(indexId, TestSchemas.basicSchema)
    manager.schemaExists(indexId) shouldBe true
    
    // Delete schema
    val deleteResult = manager.deleteSchema(indexId)
    deleteResult shouldBe a[Success[_]]
    
    // Should not exist anymore
    manager.schemaExists(indexId) shouldBe false
  }
  
  it should "handle deleting non-existent schema gracefully" in {
    val manager = createSchemaManager()
    val deleteResult = manager.deleteSchema("non_existent_schema")
    
    deleteResult shouldBe a[Success[_]]
  }
  
  it should "validate schema compatibility with no existing schema" in {
    val manager = createSchemaManager()
    val indexId = "new_schema_compatibility"
    
    val compatibilityResult = manager.validateSchemaCompatibility(indexId, TestSchemas.basicSchema)
    
    compatibilityResult shouldBe a[Success[_]]
    compatibilityResult.get shouldBe empty // No warnings for new schema
  }
  
  it should "validate schema compatibility with compatible changes" in {
    val manager = createSchemaManager()
    val indexId = "compatible_schema"
    
    // Save original schema
    manager.saveSchema(indexId, TestSchemas.basicSchema)
    
    // Create extended schema (adding fields)
    val extendedSchema = StructType(TestSchemas.basicSchema.fields ++
      Seq(StructField("new_field", StringType, nullable = true)))
    
    val compatibilityResult = manager.validateSchemaCompatibility(indexId, extendedSchema)
    
    compatibilityResult shouldBe a[Success[_]]
    compatibilityResult.get shouldBe empty // Adding fields should be compatible
  }
  
  it should "detect incompatible schema changes" in {
    val manager = createSchemaManager()
    val indexId = "incompatible_schema"
    
    // Save original schema
    manager.saveSchema(indexId, TestSchemas.basicSchema)
    
    // Create schema with removed field
    val reducedSchema = StructType(TestSchemas.basicSchema.fields.take(3))
    
    val compatibilityResult = manager.validateSchemaCompatibility(indexId, reducedSchema)
    
    compatibilityResult shouldBe a[Success[_]]
    val warnings = compatibilityResult.get
    warnings should not be empty
    warnings.exists(_.contains("Fields removed")) shouldBe true
  }
  
  it should "detect field type changes" in {
    val manager = createSchemaManager()
    val indexId = "type_change_schema"
    
    // Save original schema
    manager.saveSchema(indexId, TestSchemas.basicSchema)
    
    // Create schema with changed field type
    val changedSchema = StructType(Seq(
      StructField("id", StringType, nullable = false), // Changed from LongType
      StructField("title", StringType, nullable = false),
      StructField("content", StringType, nullable = true),
      StructField("timestamp", TimestampType, nullable = false),
      StructField("score", DoubleType, nullable = true),
      StructField("active", BooleanType, nullable = false)
    ))
    
    val compatibilityResult = manager.validateSchemaCompatibility(indexId, changedSchema)
    
    compatibilityResult shouldBe a[Success[_]]
    val warnings = compatibilityResult.get
    warnings should not be empty
    warnings.exists(_.contains("type changed")) shouldBe true
  }
  
  it should "generate sample configuration" in {
    val manager = createSchemaManager()
    val sampleConfig = manager.generateSampleConfig(TestSchemas.basicSchema)
    
    sampleConfig should not be empty
    sampleConfig should include("field_mappings")
    sampleConfig should include("index_id")
  }
  
  it should "get schema statistics" in {
    val manager = createSchemaManager()
    val indexId = "stats_schema"
    
    // Save schema first
    manager.saveSchema(indexId, TestSchemas.basicSchema)
    
    val statsResult = manager.getSchemaStatistics(indexId)
    
    statsResult shouldBe a[Success[_]]
    val stats = statsResult.get
    
    stats("index_id") shouldBe indexId
    stats("field_count") shouldBe TestSchemas.basicSchema.fields.length
    stats("text_fields").asInstanceOf[Int] should be > 0
    stats("numeric_fields").asInstanceOf[Int] should be > 0
  }
  
  it should "fail to get statistics for non-existent schema" in {
    val manager = createSchemaManager()
    val statsResult = manager.getSchemaStatistics("non_existent_stats")
    
    statsResult shouldBe a[Failure[_]]
  }
  
  it should "handle complex schema with all field types" in {
    val manager = createSchemaManager()
    val indexId = "complex_schema"
    
    // Save complex schema
    manager.saveSchema(indexId, TestSchemas.complexSchema)
    
    // Load and verify
    val loadResult = manager.loadSchema(indexId)
    loadResult shouldBe a[Success[_]]
    
    // Get statistics
    val statsResult = manager.getSchemaStatistics(indexId)
    statsResult shouldBe a[Success[_]]
    
    val stats = statsResult.get
    stats("field_count") shouldBe TestSchemas.complexSchema.fields.length
  }
  
  it should "handle log schema configuration" in {
    val manager = createSchemaManager()
    val indexId = "log_schema"
    
    manager.saveSchema(indexId, TestSchemas.logSchema) shouldBe a[Success[_]]
    
    val loadResult = manager.loadSchema(indexId)
    loadResult shouldBe a[Success[_]]
    
    val config = loadResult.get
    val fieldMappings = config.indexes.head.docMapping.fieldMappings
    
    fieldMappings("timestamp").fieldType shouldBe "datetime"
    fieldMappings("level").fieldType shouldBe "text"
    fieldMappings("message").fieldType shouldBe "text"
    fieldMappings("duration_ms").fieldType shouldBe "i64"
    fieldMappings("status_code").fieldType shouldBe "i32"
  }
  
  it should "preserve custom field options in saved schema" in {
    val customOptions = testOptions.copy(additional = Map(
      "schema.path" -> testDir.resolve("schemas").toString,
      "field.title.indexed" -> "false",
      "field.content.fast" -> "true"
    )).toMap
    
    val manager = new SchemaManager(customOptions)
    val indexId = "custom_options_schema"
    
    manager.saveSchema(indexId, TestSchemas.basicSchema) shouldBe a[Success[_]]
    
    val loadResult = manager.loadSchema(indexId)
    loadResult shouldBe a[Success[_]]
    
    val config = loadResult.get
    val fieldMappings = config.indexes.head.docMapping.fieldMappings
    
    fieldMappings("title").indexed shouldBe false
    fieldMappings("content").fast shouldBe true
  }
}