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


package com.tantivy4spark.search

import com.tantivy4spark.TestBase
import org.apache.spark.sql.types._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class SchemaConverterTest extends TestBase {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  test("should convert basic Spark schema to Tantivy schema") {
    val sparkSchema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("salary", DoubleType, nullable = true),
      StructField("active", BooleanType, nullable = true)
    ))

    val tantivySchemaJson = SchemaConverter.sparkToTantivySchema(sparkSchema)
    
    tantivySchemaJson should not be empty
    
    val schemaMap = mapper.readValue(tantivySchemaJson, classOf[Map[String, Any]])
    schemaMap should contain key "fields"
    
    val fields = schemaMap("fields").asInstanceOf[Seq[Map[String, Any]]]
    fields should have length 4
    
    val idField = fields.find(_("name") == "id").get
    idField("type") shouldBe "i64"
    idField("indexed") shouldBe true
    idField("stored") shouldBe true
    
    val nameField = fields.find(_("name") == "name").get
    nameField("type") shouldBe "text"
    
    val salaryField = fields.find(_("name") == "salary").get
    salaryField("type") shouldBe "f64"
    
    val activeField = fields.find(_("name") == "active").get
    activeField("type") shouldBe "i64" // Boolean stored as i64
  }

  test("should handle complex data types") {
    val sparkSchema = StructType(Array(
      StructField("timestamp", TimestampType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("binary_data", BinaryType, nullable = true),
      StructField("complex_field", ArrayType(StringType), nullable = true)
    ))

    val tantivySchemaJson = SchemaConverter.sparkToTantivySchema(sparkSchema)
    
    val schemaMap = mapper.readValue(tantivySchemaJson, classOf[Map[String, Any]])
    val fields = schemaMap("fields").asInstanceOf[Seq[Map[String, Any]]]
    
    val timestampField = fields.find(_("name") == "timestamp").get
    timestampField("type") shouldBe "i64"
    
    val dateField = fields.find(_("name") == "date").get
    dateField("type") shouldBe "i64"
    
    val binaryField = fields.find(_("name") == "binary_data").get
    binaryField("type") shouldBe "bytes"
    
    val complexField = fields.find(_("name") == "complex_field").get
    complexField("type") shouldBe "text" // Fallback to text for complex types
  }

  test("should handle empty schema") {
    val sparkSchema = StructType(Array.empty[StructField])
    
    val tantivySchemaJson = SchemaConverter.sparkToTantivySchema(sparkSchema)
    
    val schemaMap = mapper.readValue(tantivySchemaJson, classOf[Map[String, Any]])
    val fields = schemaMap("fields").asInstanceOf[Seq[Map[String, Any]]]
    
    fields should be (empty)
  }
}