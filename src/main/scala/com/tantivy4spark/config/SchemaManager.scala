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

import org.apache.spark.sql.types.StructType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.util.{Try, Success, Failure}
import java.nio.file.{Files, Paths}

class SchemaManager(options: Map[String, String]) {
  
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  private val schemaPath = options.getOrElse("schema.path", "./tantivy-schemas")
  
  def saveSchema(indexId: String, sparkSchema: StructType): Try[Unit] = {
    Try {
      val optionsWithIndexId = options + ("index.id" -> indexId)
      val quickwitConfig = TantivyConfig.fromSpark(sparkSchema, optionsWithIndexId)
      val schemaJson = TantivyConfig.toJson(quickwitConfig)
      
      // Ensure schema directory exists
      val schemaDir = Paths.get(schemaPath)
      if (!Files.exists(schemaDir)) {
        Files.createDirectories(schemaDir)
      }
      
      // Write schema file
      val schemaFile = schemaDir.resolve(s"$indexId.json")
      Files.write(schemaFile, schemaJson.getBytes("UTF-8"))
    }
  }
  
  def loadSchema(indexId: String): Try[TantivyGlobalConfig] = {
    Try {
      val schemaFile = Paths.get(schemaPath, s"$indexId.json")
      if (!Files.exists(schemaFile)) {
        throw new IllegalArgumentException(s"Schema file not found for index: $indexId")
      }
      
      val schemaJson = new String(Files.readAllBytes(schemaFile), "UTF-8")
      TantivyConfig.fromJson(schemaJson) match {
        case Success(config) => config
        case Failure(exception) => throw exception
      }
    }
  }
  
  def schemaExists(indexId: String): Boolean = {
    val schemaFile = Paths.get(schemaPath, s"$indexId.json")
    Files.exists(schemaFile)
  }
  
  def listSchemas(): List[String] = {
    Try {
      val schemaDir = Paths.get(schemaPath)
      if (!Files.exists(schemaDir)) {
        return List.empty
      }
      
      import scala.collection.JavaConverters._
      import java.util.stream.Collectors
      Files.list(schemaDir)
        .filter(p => p.toString.endsWith(".json"))
        .map[String](p => p.getFileName.toString.replace(".json", ""))
        .collect(Collectors.toList())
        .asScala
        .toList
    }.getOrElse(List.empty)
  }
  
  def deleteSchema(indexId: String): Try[Unit] = {
    Try {
      val schemaFile = Paths.get(schemaPath, s"$indexId.json")
      if (Files.exists(schemaFile)) {
        Files.delete(schemaFile)
      }
    }
  }
  
  def validateSchemaCompatibility(indexId: String, newSchema: StructType): Try[List[String]] = {
    loadSchema(indexId) match {
      case Success(existingConfig) =>
        Try {
          val optionsWithIndexId = options + ("index.id" -> indexId)
          val newConfig = TantivyConfig.fromSpark(newSchema, optionsWithIndexId)
          compareSchemas(existingConfig, newConfig)
        }
      case Failure(_) =>
        // No existing schema, so any schema is compatible
        Success(List.empty)
    }
  }
  
  private def compareSchemas(existing: TantivyGlobalConfig, updated: TantivyGlobalConfig): List[String] = {
    val warnings = scala.collection.mutable.ListBuffer[String]()
    
    // Compare indexes (assuming single index for now)
    if (existing.indexes.nonEmpty && updated.indexes.nonEmpty) {
      val existingIndex = existing.indexes.head
      val updatedIndex = updated.indexes.head
      
      // Check for removed fields
      val existingFields = existingIndex.docMapping.fieldMappings.keySet
      val updatedFields = updatedIndex.docMapping.fieldMappings.keySet
      
      val removedFields = existingFields -- updatedFields
      if (removedFields.nonEmpty) {
        warnings += s"Warning: Fields removed from schema: ${removedFields.mkString(", ")}"
      }
      
      // Check for type changes
      val commonFields = existingFields.intersect(updatedFields)
      commonFields.foreach { fieldName =>
        val existingType = existingIndex.docMapping.fieldMappings(fieldName).fieldType
        val updatedType = updatedIndex.docMapping.fieldMappings(fieldName).fieldType
        
        if (existingType != updatedType) {
          warnings += s"Warning: Field '$fieldName' type changed from $existingType to $updatedType"
        }
      }
      
      // Check for timestamp field changes (only warn if we're not just adding fields)
      if (existingIndex.docMapping.timestampField != updatedIndex.docMapping.timestampField) {
        // Only warn if fields were removed or types changed, not if we just added fields
        if (removedFields.nonEmpty || commonFields.exists { fieldName =>
          val existingType = existingIndex.docMapping.fieldMappings(fieldName).fieldType
          val updatedType = updatedIndex.docMapping.fieldMappings(fieldName).fieldType
          existingType != updatedType
        }) {
          warnings += "Warning: Timestamp field configuration changed"
        }
      }
    }
    
    warnings.toList
  }
  
  def generateSampleConfig(sparkSchema: StructType): String = {
    val config = TantivyConfig.fromSpark(sparkSchema, options)
    TantivyConfig.toJson(config)
  }
  
  def getSchemaStatistics(indexId: String): Try[Map[String, Any]] = {
    loadSchema(indexId).map { config =>
      val index = config.indexes.head
      Map(
        "index_id" -> index.indexId,
        "field_count" -> index.docMapping.fieldMappings.size,
        "indexed_fields" -> index.docMapping.fieldMappings.count(_._2.indexed),
        "stored_fields" -> index.docMapping.fieldMappings.count(_._2.stored),
        "text_fields" -> index.docMapping.fieldMappings.count(_._2.fieldType == "text"),
        "numeric_fields" -> index.docMapping.fieldMappings.count(f => 
          List("i32", "i64", "f32", "f64").contains(f._2.fieldType)
        ),
        "timestamp_field" -> index.docMapping.timestampField.getOrElse("none"),
        "default_search_fields" -> index.docMapping.defaultSearchFields
      )
    }
  }
}
