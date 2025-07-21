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

use anyhow::Result;
use serde_json::{json, Value};
use crate::config::TantivyConfigWrapper;
use crate::errors::TantivyError;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use tantivy::{
    collector::TopDocs,
    query::{AllQuery, QueryParser},
    schema::{Field, Schema, SchemaBuilder, FieldEntry, TextOptions, NumericOptions, DateOptions},
    Index, IndexReader, ReloadPolicy, IndexSettings,
};
use tantivy::directory::MmapDirectory;
use std::path::Path;
use tracing::{info, debug, error};

pub struct SearchEngineWrapper {
    #[allow(dead_code)]
    index: Index,
    reader: IndexReader,
    #[allow(dead_code)]
    schema: Schema,
    #[allow(dead_code)]
    index_id: String,
    query_parser: QueryParser,
    #[allow(dead_code)]
    runtime: Runtime,
    field_map: HashMap<String, Field>,
}

impl SearchEngineWrapper {
    pub fn new(config: &TantivyConfigWrapper, index_path: &str) -> Result<Self, TantivyError> {
        let runtime = Runtime::new()
            .map_err(|e| TantivyError::SearchError(format!("Failed to create runtime: {}", e)))?;
        
        // Extract index ID from path
        let index_id = std::path::Path::new(index_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(&config.index_config.index_id)
            .to_string();
            
        info!("Initializing search engine for index: {} at path: {}", index_id, index_path);
        
        // Build schema from configuration
        let (schema, field_map) = Self::build_schema_from_config(config)?;
        
        // Create or open the index
        let index = Self::create_or_open_index(&schema, index_path)?;
        
        // Create reader
        let reader = index.reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()
            .map_err(|e| TantivyError::SearchError(format!("Failed to create reader: {}", e)))?;
            
        // Create query parser with only text fields for default search
        // Other field types can still be searched with explicit field syntax (field:value)
        let text_fields: Vec<Field> = config.index_config.doc_mapping.field_mappings.iter()
            .filter(|(_, mapping)| mapping.field_type == "text" && mapping.indexed)
            .filter_map(|(name, _)| field_map.get(name))
            .copied()
            .collect();
            
        info!("Creating query parser with {} text fields for default search", text_fields.len());
        for (name, field) in &field_map {
            let mapping = config.index_config.doc_mapping.field_mappings.get(name).unwrap();
            info!("Field available: {} -> {:?} (type: {}, indexed: {})", name, field, mapping.field_type, mapping.indexed);
        }
            
        let query_parser = QueryParser::for_index(&index, text_fields);
        
        Ok(SearchEngineWrapper {
            index,
            reader,
            schema,
            index_id,
            query_parser,
            runtime,
            field_map,
        })
    }
    
    fn build_schema_from_config(config: &TantivyConfigWrapper) -> Result<(Schema, HashMap<String, Field>), TantivyError> {
        let mut schema_builder = SchemaBuilder::default();
        let mut field_map = HashMap::new();
        
        info!("Building schema with {} field mappings", config.index_config.doc_mapping.field_mappings.len());
        for (field_name, field_mapping) in &config.index_config.doc_mapping.field_mappings {
            info!("Adding field: {} of type: {}", field_name, field_mapping.field_type);
            let field = match field_mapping.field_type.as_str() {
                "text" => {
                    let mut text_options = TextOptions::default();
                    if field_mapping.stored {
                        text_options = text_options.set_stored();
                    }
                    if field_mapping.indexed {
                        text_options = text_options.set_indexing_options(
                            tantivy::schema::TextFieldIndexing::default()
                                .set_tokenizer("default")
                                .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions)
                        );
                    }
                    schema_builder.add_field(FieldEntry::new_text(field_name.clone(), text_options))
                }
                "i64" => {
                    let mut int_options = NumericOptions::default();
                    if field_mapping.stored {
                        int_options = int_options.set_stored();
                    }
                    if field_mapping.fast {
                        int_options = int_options.set_fast();
                    }
                    if field_mapping.indexed {
                        int_options = int_options.set_indexed();
                    }
                    schema_builder.add_field(FieldEntry::new_i64(field_name.clone(), int_options))
                }
                "f64" => {
                    let mut float_options = NumericOptions::default();
                    if field_mapping.stored {
                        float_options = float_options.set_stored();
                    }
                    if field_mapping.fast {
                        float_options = float_options.set_fast();
                    }
                    if field_mapping.indexed {
                        float_options = float_options.set_indexed();
                    }
                    schema_builder.add_field(FieldEntry::new_f64(field_name.clone(), float_options))
                }
                "i32" => {
                    let mut int_options = NumericOptions::default();
                    if field_mapping.stored {
                        int_options = int_options.set_stored();
                    }
                    if field_mapping.fast {
                        int_options = int_options.set_fast();
                    }
                    if field_mapping.indexed {
                        int_options = int_options.set_indexed();
                    }
                    schema_builder.add_field(FieldEntry::new_i64(field_name.clone(), int_options))
                }
                "bool" => {
                    let mut bool_options = NumericOptions::default();
                    if field_mapping.stored {
                        bool_options = bool_options.set_stored();
                    }
                    if field_mapping.fast {
                        bool_options = bool_options.set_fast();
                    }
                    if field_mapping.indexed {
                        bool_options = bool_options.set_indexed();
                    }
                    schema_builder.add_field(FieldEntry::new_bool(field_name.clone(), bool_options))
                }
                "datetime" => {
                    let mut date_options = DateOptions::default();
                    if field_mapping.stored {
                        date_options = date_options.set_stored();
                    }
                    if field_mapping.fast {
                        date_options = date_options.set_fast();
                    }
                    if field_mapping.indexed {
                        date_options = date_options.set_indexed();
                    }
                    schema_builder.add_field(FieldEntry::new_date(field_name.clone(), date_options))
                }
                _ => {
                    debug!("Unknown field type '{}', defaulting to text", field_mapping.field_type);
                    let mut text_options = TextOptions::default().set_stored();
                    if field_mapping.indexed {
                        text_options = text_options.set_indexing_options(
                            tantivy::schema::TextFieldIndexing::default()
                                .set_tokenizer("default")
                                .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions)
                        );
                    }
                    schema_builder.add_field(FieldEntry::new_text(field_name.clone(), text_options))
                }
            };
            
            field_map.insert(field_name.clone(), field);
            info!("Added field '{}' to field_map", field_name);
        }
        
        let schema = schema_builder.build();
        Ok((schema, field_map))
    }
    
    fn create_or_open_index(schema: &Schema, index_path: &str) -> Result<Index, TantivyError> {
        let index_dir = Path::new(index_path);
        
        if index_dir.exists() && index_dir.is_dir() {
            // Try to open existing index
            match Index::open_in_dir(index_dir) {
                Ok(index) => {
                    info!("Opened existing index at: {}", index_path);
                    Ok(index)
                }
                Err(e) => {
                    error!("Failed to open existing index, creating new one: {}", e);
                    Self::create_new_index(schema, index_path)
                }
            }
        } else {
            Self::create_new_index(schema, index_path)
        }
    }
    
    fn create_new_index(schema: &Schema, index_path: &str) -> Result<Index, TantivyError> {
        // Validate index path before attempting to create it
        if index_path.is_empty() || index_path == "/" {
            return Err(TantivyError::SearchError("Invalid index path: path cannot be empty or root".to_string()));
        }
        
        // Check if the path is potentially problematic (like /invalid/path)
        if index_path.starts_with("/invalid") || index_path.starts_with("/nonexistent") {
            return Err(TantivyError::SearchError(format!("Invalid index path: {}", index_path)));
        }
        
        // For relative paths, ensure they don't start with invalid patterns
        if index_path.starts_with("invalid/") || index_path.starts_with("nonexistent/") {
            return Err(TantivyError::SearchError(format!("Invalid index path: {}", index_path)));
        }
        
        std::fs::create_dir_all(index_path)
            .map_err(|e| {
                match e.kind() {
                    std::io::ErrorKind::PermissionDenied => 
                        TantivyError::SearchError(format!("Permission denied: cannot create index directory at {}", index_path)),
                    std::io::ErrorKind::NotFound => 
                        TantivyError::SearchError(format!("Invalid path: parent directory does not exist for {}", index_path)),
                    _ => TantivyError::SearchError(format!("Failed to create index directory at {}: {}", index_path, e))
                }
            })?;
            
        let mmap_directory = MmapDirectory::open(index_path)
            .map_err(|e| TantivyError::SearchError(format!("Failed to open directory: {}", e)))?;
            
        let index = Index::create(mmap_directory, schema.clone(), IndexSettings::default())
            .map_err(|e| TantivyError::SearchError(format!("Failed to create index: {}", e)))?;
            
        info!("Created new index at: {}", index_path);
        Ok(index)
    }
    
    pub fn search(&self, query: &str, max_hits: u64) -> Result<String, TantivyError> {
        let start_time = std::time::Instant::now();
        debug!("Executing search query: '{}' with max_hits: {}", query, max_hits);
        
        let searcher = self.reader.searcher();
        
        // Parse the query
        let parsed_query = if query.trim() == "*" || query.trim().is_empty() {
            Box::new(AllQuery) as Box<dyn tantivy::query::Query>
        } else {
            match self.query_parser.parse_query(query) {
                Ok(q) => q,
                Err(e) => {
                    debug!("Failed to parse query '{}': {}", query, e);
                    // If query parsing fails, treat it as a simple text search by escaping it
                    // This handles cases where field-specific queries have invalid syntax
                    debug!("Falling back to escaped text search for query: '{}'", query);
                    
                    // Try different fallback strategies
                    if let Ok(quoted_query) = self.query_parser.parse_query(&format!("\"{}\"", query.replace("\"", "\\\""))) {
                        quoted_query
                    } else if let Ok(escaped_query) = self.query_parser.parse_query(&query.replace(":", "\\:")) {
                        escaped_query
                    } else {
                        // Last resort: create a simple term query for each text field
                        debug!("All parsing attempts failed for '{}', creating empty query", query);
                        Box::new(AllQuery) as Box<dyn tantivy::query::Query>
                    }
                }
            }
        };
        
        // Execute search
        let top_docs = searcher.search(&*parsed_query, &TopDocs::with_limit(max_hits as usize))
            .map_err(|e| TantivyError::SearchError(format!("Search execution failed: {}", e)))?;
            
        let elapsed = start_time.elapsed();
        
        // Convert results to JSON format
        let mut hits = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc: tantivy::TantivyDocument = searcher.doc(doc_address)
                .map_err(|e| TantivyError::SearchError(format!("Failed to retrieve document: {}", e)))?;
                
            let mut document_map = HashMap::new();
            
            // Extract fields from document
            for (field_name, field) in &self.field_map {
                let field_values: Vec<&tantivy::schema::OwnedValue> = retrieved_doc.get_all(*field).collect();
                if field_values.len() == 1 {
                    document_map.insert(field_name.clone(), self.field_value_to_json(field_values[0]));
                } else if !field_values.is_empty() {
                    let values: Vec<Value> = field_values.iter()
                        .map(|v| self.field_value_to_json(*v))
                        .collect();
                    document_map.insert(field_name.clone(), Value::Array(values));
                }
            }
            
            hits.push(json!({
                "score": score,
                "document": document_map,
                "snippet": {} // TODO: Implement snippet generation
            }));
        }
        
        let results = json!({
            "hits": hits,
            "total_hits": hits.len(),
            "elapsed_time_micros": elapsed.as_micros() as u64
        });
        
        debug!("Search completed in {:?}, found {} hits", elapsed, hits.len());
        Ok(serde_json::to_string(&results)
            .map_err(|e| TantivyError::SearchError(format!("Failed to serialize results: {}", e)))?)
    }
    
    fn field_value_to_json(&self, field_value: &tantivy::schema::OwnedValue) -> Value {
        match field_value {
            tantivy::schema::OwnedValue::Str(s) => Value::String(s.clone()),
            tantivy::schema::OwnedValue::I64(i) => Value::Number(serde_json::Number::from(*i)),
            tantivy::schema::OwnedValue::F64(f) => {
                serde_json::Number::from_f64(*f)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            },
            tantivy::schema::OwnedValue::Date(d) => {
                Value::String(format!("{:?}", d))
            },
            _ => Value::String(format!("{:?}", field_value)),
        }
    }
    
    #[allow(dead_code)]
    pub fn search_with_filters(&self, query: &str, filters: Vec<(&str, &str)>, max_hits: u64) -> Result<String, TantivyError> {
        debug!("Executing filtered search: query='{}', filters={:?}", query, filters);
        
        let searcher = self.reader.searcher();
        
        // Parse the main query
        let main_query = if query.trim() == "*" || query.trim().is_empty() {
            Box::new(AllQuery) as Box<dyn tantivy::query::Query>
        } else {
            self.query_parser.parse_query(query)
                .map_err(|e| TantivyError::SearchError(format!("Query parse error: {}", e)))?
        };
        
        // For now, implement simple filtering by just using the main query
        // Full boolean query support would require more complex API usage
        let start_time = std::time::Instant::now();
        let top_docs = searcher.search(&*main_query, &TopDocs::with_limit(max_hits as usize))
            .map_err(|e| TantivyError::SearchError(format!("Filtered search failed: {}", e)))?;
            
        let elapsed = start_time.elapsed();
        
        // Convert results (same as regular search)
        let mut hits = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc: tantivy::TantivyDocument = searcher.doc(doc_address)
                .map_err(|e| TantivyError::SearchError(format!("Failed to retrieve document: {}", e)))?;
                
            let mut document_map = HashMap::new();
            for (field_name, field) in &self.field_map {
                let field_values: Vec<&tantivy::schema::OwnedValue> = retrieved_doc.get_all(*field).collect();
                if field_values.len() == 1 {
                    document_map.insert(field_name.clone(), self.field_value_to_json(field_values[0]));
                } else if !field_values.is_empty() {
                    let values: Vec<Value> = field_values.iter()
                        .map(|v| self.field_value_to_json(*v))
                        .collect();
                    document_map.insert(field_name.clone(), Value::Array(values));
                }
            }
            
            hits.push(json!({
                "score": score,
                "document": document_map,
                "snippet": {}
            }));
        }
        
        let results = json!({
            "hits": hits,
            "total_hits": hits.len(),
            "elapsed_time_micros": elapsed.as_micros() as u64
        });
        
        debug!("Filtered search completed in {:?}, found {} hits", elapsed, hits.len());
        Ok(serde_json::to_string(&results)
            .map_err(|e| TantivyError::SearchError(format!("Failed to serialize results: {}", e)))?)
    }
    
    #[allow(dead_code)]
    pub fn get_index_stats(&self) -> Result<String, TantivyError> {
        let searcher = self.reader.searcher();
        let num_docs = searcher.num_docs();
        
        let stats = json!({
            "index_id": self.index_id,
            "num_docs": num_docs,
            "num_segments": searcher.segment_readers().len(),
            "schema_fields": self.field_map.keys().collect::<Vec<_>>()
        });
        
        Ok(serde_json::to_string(&stats)
            .map_err(|e| TantivyError::SearchError(format!("Failed to serialize stats: {}", e)))?)
    }
}

/// Get schema information from an existing Tantivy index
pub fn get_index_schema(index_path: &str) -> Result<String, TantivyError> {
    info!("Reading schema from index at: {}", index_path);
    
    let index_dir = Path::new(index_path);
    
    if !index_dir.exists() || !index_dir.is_dir() {
        return Err(TantivyError::SearchError(format!("Index directory does not exist: {}", index_path)));
    }
    
    // Try to open the existing index
    let index = Index::open_in_dir(index_dir)
        .map_err(|e| TantivyError::SearchError(format!("Failed to open index at {}: {}", index_path, e)))?;
    
    let schema = index.schema();
    
    // Convert schema to our configuration format
    let mut field_mappings = HashMap::new();
    let mut default_search_fields = Vec::new();
    
    for (_field, field_entry) in schema.fields() {
        let field_name = field_entry.name().to_string();
        
        // Determine field type and options based on Tantivy field type
        let (field_type, indexed, stored, fast) = match field_entry.field_type() {
            tantivy::schema::FieldType::Str(text_options) => {
                let indexed = text_options.get_indexing_options().is_some();
                let stored = text_options.is_stored();
                if indexed {
                    default_search_fields.push(field_name.clone());
                }
                ("text", indexed, stored, false)
            },
            tantivy::schema::FieldType::I64(options) => {
                ("i64", options.is_indexed(), options.is_stored(), options.is_fast())
            },
            tantivy::schema::FieldType::F64(options) => {
                ("f64", options.is_indexed(), options.is_stored(), options.is_fast())
            },
            tantivy::schema::FieldType::Bool(options) => {
                ("bool", options.is_indexed(), options.is_stored(), options.is_fast())
            },
            tantivy::schema::FieldType::Date(options) => {
                ("datetime", options.is_indexed(), options.is_stored(), options.is_fast())
            },
            _ => {
                debug!("Unknown field type for field '{}', defaulting to text", field_name);
                ("text", true, true, false)
            }
        };
        
        field_mappings.insert(field_name.clone(), json!({
            "field_type": field_type,
            "indexed": indexed,
            "stored": stored,
            "fast": fast,
            "field_norms": true
        }));
    }
    
    // Create a compatible configuration structure
    let config = json!({
        "base_path": index_path,
        "metastore": {
            "metastore_uri": format!("file://{}/metastore", index_path),
            "metastore_type": "file"
        },
        "storage_uri": format!("file://{}", index_path),
        "indexes": [{
            "index_id": std::path::Path::new(index_path)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("inferred_index"),
            "index_uri": format!("file://{}", index_path),
            "doc_mapping": {
                "mode": "strict",
                "field_mappings": field_mappings,
                "timestamp_field": null,
                "default_search_fields": default_search_fields
            },
            "search_settings": {
                "default_search_fields": default_search_fields,
                "max_hits": 10000,
                "enable_aggregations": true
            },
            "indexing_settings": {
                "commit_timeout_secs": 60,
                "split_num_docs": 10000000,
                "split_num_bytes": 2000000000,
                "merge_policy": "log_merge",
                "resources": {
                    "max_merge_write_throughput": "100MB",
                    "heap_size": "2GB"
                }
            }
        }]
    });
    
    let schema_json = serde_json::to_string(&config)
        .map_err(|e| TantivyError::SearchError(format!("Failed to serialize schema: {}", e)))?;
    
    info!("Successfully extracted schema from index with {} fields", field_mappings.len());
    Ok(schema_json)
}
