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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.types.StructType
import scala.util.{Try, Success, Failure}

case class TantivyFieldMapping(
    @JsonProperty("field_type") fieldType: String,
    indexed: Boolean = true,
    stored: Boolean = true,
    fast: Boolean = false,
    @JsonProperty("field_norms") fieldNorms: Boolean = true
)

case class TantivyDocMapping(
    mode: String = "strict", // strict, lenient, or dynamic
    @JsonProperty("field_mappings") fieldMappings: Map[String, TantivyFieldMapping],
    @JsonProperty("timestamp_field") timestampField: Option[String] = Some("_timestamp"),
    @JsonProperty("default_search_fields") defaultSearchFields: List[String] = List.empty
)

case class TantivyIndexConfig(
    @JsonProperty("index_id") indexId: String,
    @JsonProperty("index_uri") indexUri: String,
    @JsonProperty("doc_mapping") docMapping: TantivyDocMapping,
    @JsonProperty("search_settings") searchSettings: TantivySearchSettings = TantivySearchSettings(),
    @JsonProperty("indexing_settings") indexingSettings: TantivyIndexingSettings = TantivyIndexingSettings()
)

case class TantivySearchSettings(
    @JsonProperty("default_search_fields") defaultSearchFields: List[String] = List.empty,
    @JsonProperty("max_hits") maxHits: Int = 10000,
    @JsonProperty("enable_aggregations") enableAggregations: Boolean = true
)

case class TantivyIndexingSettings(
    @JsonProperty("commit_timeout_secs") commitTimeoutSecs: Int = 60,
    @JsonProperty("split_num_docs") splitNumDocs: Long = 10000000L,
    @JsonProperty("split_num_bytes") splitNumBytes: Long = 2000000000L, // 2GB
    @JsonProperty("merge_policy") mergePolicy: String = "log_merge",
    resources: TantivyResources = TantivyResources()
)

case class TantivyResources(
    @JsonProperty("max_merge_write_throughput") maxMergeWriteThroughput: String = "100MB",
    @JsonProperty("heap_size") heapSize: String = "2GB"
)

case class TantivySourceConfig(
    sourceId: String,
    sourceType: String = "file", // file, kafka, kinesis, etc.
    params: Map[String, String] = Map.empty
)

case class TantivyMetastoreConfig(
    @JsonProperty("metastore_uri") metastoreUri: String,
    @JsonProperty("metastore_type") metastoreType: String = "file" // file, postgresql, etc.
)

case class TantivyGlobalConfig(
    @JsonProperty("base_path") basePath: String,
    metastore: TantivyMetastoreConfig,
    @JsonProperty("storage_uri") storageUri: String,
    indexes: List[TantivyIndexConfig] = List.empty
)

object TantivyConfig {
  
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  def fromSpark(schema: StructType, options: Map[String, String]): TantivyGlobalConfig = {
    val indexId = options.getOrElse("index.id", "spark_index")
    val basePath = options.getOrElse("tantivy.base.path", "./tantivy-data")
    val storageUri = options.getOrElse("storage.uri", s"file://$basePath")
    
    // Generate field mappings from Spark schema
    val fieldMappings = generateFieldMappings(schema, options)
    
    // Create document mapping
    val docMapping = TantivyDocMapping(
      mode = options.getOrElse("mapping.mode", "strict"),
      fieldMappings = fieldMappings,
      timestampField = options.get("timestamp.field"),
      defaultSearchFields = extractDefaultSearchFields(schema, options)
    )
    
    // Create index configuration
    val indexConfig = TantivyIndexConfig(
      indexId = indexId,
      indexUri = s"$storageUri/$indexId",
      docMapping = docMapping,
      searchSettings = TantivySearchSettings(
        defaultSearchFields = docMapping.defaultSearchFields,
        maxHits = options.getOrElse("max.hits", "10000").toInt
      ),
      indexingSettings = TantivyIndexingSettings(
        commitTimeoutSecs = options.getOrElse("commit.timeout.secs", "60").toInt,
        splitNumDocs = options.getOrElse("split.num.docs", "10000000").toLong,
        splitNumBytes = options.getOrElse("split.num.bytes", "2000000000").toLong
      )
    )
    
    // Create metastore configuration
    val metastore = TantivyMetastoreConfig(
      metastoreUri = s"$storageUri/metastore",
      metastoreType = options.getOrElse("metastore.type", "file")
    )
    
    TantivyGlobalConfig(
      basePath = basePath,
      metastore = metastore,
      storageUri = storageUri,
      indexes = List(indexConfig)
    )
  }
  
  private def generateFieldMappings(schema: StructType, options: Map[String, String]): Map[String, TantivyFieldMapping] = {
    schema.fields.map { field =>
      val tantivyType = mapSparkTypeToTantivy(field.dataType)
      val fieldName = field.name
      
      // Check for field-specific options
      val indexed = options.getOrElse(s"field.$fieldName.indexed", "true").toBoolean
      val stored = options.getOrElse(s"field.$fieldName.stored", "true").toBoolean
      val fast = options.getOrElse(s"field.$fieldName.fast", "false").toBoolean
      val fieldNorms = options.getOrElse(s"field.$fieldName.field_norms", "true").toBoolean
      
      fieldName -> TantivyFieldMapping(
        fieldType = tantivyType,
        indexed = indexed,
        stored = stored,
        fast = fast,
        fieldNorms = fieldNorms
      )
    }.toMap
  }
  
  private def mapSparkTypeToTantivy(dataType: org.apache.spark.sql.types.DataType): String = {
    import org.apache.spark.sql.types._
    
    dataType match {
      case StringType => "text"
      case ByteType | ShortType | IntegerType => "i32"
      case LongType => "i64"
      case FloatType => "f32"
      case DoubleType => "f64"
      case BooleanType => "bool"
      case TimestampType => "datetime"
      case DateType => "date"
      case _: DecimalType => "text" // Tantivy doesn't have native decimal support
      case _: ArrayType => "json" // Arrays stored as JSON
      case _: MapType => "json" // Maps stored as JSON
      case _: StructType => "json" // Nested structures as JSON
      case _ => "text" // Default fallback
    }
  }
  
  private def extractDefaultSearchFields(schema: StructType, options: Map[String, String]): List[String] = {
    options.get("default.search.fields") match {
      case Some(fields) => fields.split(",").map(_.trim).toList
      case None =>
        // Auto-detect text fields as default search fields
        schema.fields
          .filter(_.dataType == org.apache.spark.sql.types.StringType)
          .map(_.name)
          .toList
    }
  }
  
  def toJson(config: TantivyGlobalConfig): String = {
    objectMapper.writeValueAsString(config)
  }
  
  def fromJson(json: String): Try[TantivyGlobalConfig] = {
    Try(objectMapper.readValue(json, classOf[TantivyGlobalConfig]))
  }
  
  def validateConfig(config: TantivyGlobalConfig): List[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()
    
    // Validate base path
    if (config.basePath.isEmpty) {
      errors += "Base path cannot be empty"
    }
    
    // Validate indexes
    config.indexes.foreach { index =>
      if (index.indexId.isEmpty) {
        errors += "Index ID cannot be empty"
      }
      
      if (index.docMapping.fieldMappings.isEmpty) {
        errors += s"Index ${index.indexId} must have at least one field mapping"
      }
      
      // Validate timestamp field exists if specified
      index.docMapping.timestampField.foreach { timestampField =>
        if (!index.docMapping.fieldMappings.contains(timestampField)) {
          errors += s"Timestamp field '$timestampField' not found in field mappings for index ${index.indexId}"
        }
      }
    }
    
    errors.toList
  }
}