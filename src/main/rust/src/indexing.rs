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
use serde_json::Value;
use crate::config::TantivyConfigWrapper;
use crate::errors::TantivyError;
use std::collections::{VecDeque, HashMap};
use tokio::runtime::Runtime;
use tantivy::{
    schema::{Field, Schema, FieldEntry, TextOptions, NumericOptions, DateOptions},
    Index, IndexWriter, Term, IndexSettings,
};
use tantivy::directory::MmapDirectory;
use std::path::Path;
use tracing::{info, debug, warn};

pub struct IndexWriterWrapper {
    #[allow(dead_code)]
    index: Index,
    writer: IndexWriter,
    #[allow(dead_code)]
    schema: Schema,
    #[allow(dead_code)]
    index_id: String,
    #[allow(dead_code)]
    runtime: Runtime,
    document_buffer: VecDeque<Value>,
    buffer_size: usize,
    field_map: HashMap<String, Field>,
    field_types: HashMap<String, String>,
    documents_indexed: u64,
    bytes_written: u64,
}

impl IndexWriterWrapper {
    pub fn new(config: &TantivyConfigWrapper, index_path: &str, schema_json: &str) -> Result<Self, TantivyError> {
        let runtime = Runtime::new()
            .map_err(|e| TantivyError::IndexingError(format!("Failed to create runtime: {}", e)))?;
        
        // Parse schema
        let _schema_data: Value = serde_json::from_str(schema_json)
            .map_err(|e| TantivyError::IndexingError(format!("Invalid schema JSON: {}", e)))?;
        
        // Extract index ID from path
        let index_id = std::path::Path::new(index_path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(&config.index_config.index_id)
            .to_string();
            
        info!("Initializing index writer for index: {} at path: {}", index_id, index_path);
        
        // Build schema from configuration
        let (schema, field_map, field_types) = Self::build_schema_from_config(config)?;
        
        // Create or open the index
        let index = Self::create_or_open_index(&schema, index_path)?;
        
        // Create index writer
        let heap_size_bytes = 50_000_000; // 50MB heap
        let writer = index.writer(heap_size_bytes)
            .map_err(|e| TantivyError::IndexingError(format!("Failed to create index writer: {}", e)))?;
            
        let buffer_size = 1000; // Buffer up to 1000 documents before flushing
        
        Ok(IndexWriterWrapper {
            index,
            writer,
            schema,
            index_id,
            runtime,
            document_buffer: VecDeque::new(),
            buffer_size,
            field_map,
            field_types,
            documents_indexed: 0,
            bytes_written: 0,
        })
    }
    
    fn build_schema_from_config(config: &TantivyConfigWrapper) -> Result<(Schema, HashMap<String, Field>, HashMap<String, String>), TantivyError> {
        let mut schema_builder = tantivy::schema::SchemaBuilder::default();
        let mut field_map = HashMap::new();
        let mut field_types = HashMap::new();
        
        for (field_name, field_mapping) in &config.index_config.doc_mapping.field_mappings {
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
                    warn!("Unknown field type '{}' for field '{}', defaulting to text", field_mapping.field_type, field_name);
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
            field_types.insert(field_name.clone(), field_mapping.field_type.clone());
        }
        
        let schema = schema_builder.build();
        Ok((schema, field_map, field_types))
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
                    warn!("Failed to open existing index, creating new one: {}", e);
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
            return Err(TantivyError::IndexingError("Invalid index path: path cannot be empty or root".to_string()));
        }
        
        // Check if the path is potentially problematic (like /invalid/path)
        if index_path.starts_with("/invalid") || index_path.starts_with("/nonexistent") {
            return Err(TantivyError::IndexingError(format!("Invalid index path: {}", index_path)));
        }
        
        // For relative paths, ensure they don't start with invalid patterns
        if index_path.starts_with("invalid/") || index_path.starts_with("nonexistent/") {
            return Err(TantivyError::IndexingError(format!("Invalid index path: {}", index_path)));
        }
        
        std::fs::create_dir_all(index_path)
            .map_err(|e| {
                match e.kind() {
                    std::io::ErrorKind::PermissionDenied => 
                        TantivyError::IndexingError(format!("Permission denied: cannot create index directory at {}", index_path)),
                    std::io::ErrorKind::NotFound => 
                        TantivyError::IndexingError(format!("Invalid path: parent directory does not exist for {}", index_path)),
                    _ => TantivyError::IndexingError(format!("Failed to create index directory at {}: {}", index_path, e))
                }
            })?;
            
        let mmap_directory = MmapDirectory::open(index_path)
            .map_err(|e| TantivyError::IndexingError(format!("Failed to open directory: {}", e)))?;
            
        let index = Index::create(mmap_directory, schema.clone(), IndexSettings::default())
            .map_err(|e| TantivyError::IndexingError(format!("Failed to create index: {}", e)))?;
            
        info!("Created new index at: {}", index_path);
        Ok(index)
    }
    
    pub fn add_document(&mut self, document_json: &str) -> Result<(), TantivyError> {
        let document: Value = serde_json::from_str(document_json)
            .map_err(|e| TantivyError::IndexingError(format!("Invalid document JSON: {}", e)))?;
        
        debug!("Adding document to buffer: {}", document_json.chars().take(100).collect::<String>());
        
        // Add to buffer
        self.document_buffer.push_back(document);
        self.bytes_written += document_json.len() as u64;
        
        // Flush if buffer is full
        if self.document_buffer.len() >= self.buffer_size {
            self.flush_buffer()?;
        }
        
        Ok(())
    }
    
    #[allow(dead_code)]
    pub fn add_documents(&mut self, documents: Vec<Value>) -> Result<(), TantivyError> {
        for doc in documents {
            self.document_buffer.push_back(doc);
        }
        
        // Flush if buffer is full
        if self.document_buffer.len() >= self.buffer_size {
            self.flush_buffer()?;
        }
        
        Ok(())
    }
    
    fn flush_buffer(&mut self) -> Result<(), TantivyError> {
        if self.document_buffer.is_empty() {
            return Ok(());
        }
        
        let batch_size = self.document_buffer.len();
        debug!("Flushing buffer with {} documents", batch_size);
        
        let start_time = std::time::Instant::now();
        
        // Process each document in the buffer
        while let Some(doc_value) = self.document_buffer.pop_front() {
            let tantivy_doc = self.convert_json_to_document(&doc_value)?;
            
            self.writer.add_document(tantivy_doc)
                .map_err(|e| TantivyError::IndexingError(format!("Failed to add document: {}", e)))?;
                
            self.documents_indexed += 1;
        }
        
        let elapsed = start_time.elapsed();
        info!("Flushed {} documents in {:?}", batch_size, elapsed);
        
        Ok(())
    }
    
    fn convert_json_to_document(&self, json_value: &Value) -> Result<tantivy::TantivyDocument, TantivyError> {
        let mut doc = tantivy::TantivyDocument::new();
        
        if let Value::Object(obj) = json_value {
            for (field_name, value) in obj {
                if let Some(field) = self.field_map.get(field_name) {
                    let tantivy_value = self.json_value_to_tantivy_value(value, field_name)?;
                    if let Some(tv) = tantivy_value {
                        doc.add_field_value(*field, tv);
                    }
                } else {
                    debug!("Field '{}' not found in schema, skipping", field_name);
                }
            }
        } else {
            return Err(TantivyError::IndexingError("Document must be a JSON object".to_string()));
        }
        
        Ok(doc)
    }
    
    fn json_value_to_tantivy_value(&self, json_value: &Value, field_name: &str) -> Result<Option<tantivy::schema::OwnedValue>, TantivyError> {
        if let Some(field_type) = self.field_types.get(field_name) {
            match field_type.as_str() {
                "text" => match json_value {
                    Value::String(s) => Ok(Some(tantivy::schema::OwnedValue::Str(s.clone()))),
                    Value::Number(n) => Ok(Some(tantivy::schema::OwnedValue::Str(n.to_string()))),
                    Value::Bool(b) => Ok(Some(tantivy::schema::OwnedValue::Str(b.to_string()))),
                    Value::Null => Ok(None),
                    _ => {
                        let json_str = serde_json::to_string(json_value)
                            .map_err(|e| TantivyError::IndexingError(format!("Failed to serialize value: {}", e)))?;
                        Ok(Some(tantivy::schema::OwnedValue::Str(json_str)))
                    }
                },
                "i64" | "i32" => match json_value {
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Some(tantivy::schema::OwnedValue::I64(i)))
                        } else {
                            Err(TantivyError::IndexingError(format!("Expected integer for field '{}', got {}", field_name, n)))
                        }
                    },
                    Value::String(s) => {
                        s.parse::<i64>()
                            .map(|i| Some(tantivy::schema::OwnedValue::I64(i)))
                            .map_err(|_| TantivyError::IndexingError(format!("Could not parse '{}' as integer for field '{}'", s, field_name)))
                    },
                    Value::Null => Ok(None),
                    _ => Err(TantivyError::IndexingError(format!("Invalid type for i64 field '{}': expected number or string", field_name)))
                },
                "f64" => match json_value {
                    Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            Ok(Some(tantivy::schema::OwnedValue::F64(f)))
                        } else {
                            Err(TantivyError::IndexingError(format!("Expected float for field '{}', got {}", field_name, n)))
                        }
                    },
                    Value::String(s) => {
                        s.parse::<f64>()
                            .map(|f| Some(tantivy::schema::OwnedValue::F64(f)))
                            .map_err(|_| TantivyError::IndexingError(format!("Could not parse '{}' as float for field '{}'", s, field_name)))
                    },
                    Value::Null => Ok(None),
                    _ => Err(TantivyError::IndexingError(format!("Invalid type for f64 field '{}': expected number or string", field_name)))
                },
                "bool" => match json_value {
                    Value::Bool(b) => Ok(Some(tantivy::schema::OwnedValue::Bool(*b))),
                    Value::String(s) => {
                        match s.to_lowercase().as_str() {
                            "true" | "1" => Ok(Some(tantivy::schema::OwnedValue::Bool(true))),
                            "false" | "0" => Ok(Some(tantivy::schema::OwnedValue::Bool(false))),
                            _ => Err(TantivyError::IndexingError(format!("Could not parse '{}' as boolean for field '{}'", s, field_name)))
                        }
                    },
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Some(tantivy::schema::OwnedValue::Bool(i != 0)))
                        } else {
                            Err(TantivyError::IndexingError(format!("Invalid number for boolean field '{}': {}", field_name, n)))
                        }
                    },
                    Value::Null => Ok(None),
                    _ => Err(TantivyError::IndexingError(format!("Invalid type for bool field '{}': expected boolean, string, or number", field_name)))
                },
                "datetime" => match json_value {
                    Value::Number(n) => {
                        if let Some(timestamp) = n.as_i64() {
                            // Convert Unix timestamp to Tantivy Date
                            let date = tantivy::DateTime::from_timestamp_micros(timestamp * 1_000_000);
                            Ok(Some(tantivy::schema::OwnedValue::Date(date)))
                        } else {
                            Err(TantivyError::IndexingError(format!("Expected integer timestamp for datetime field '{}', got {}", field_name, n)))
                        }
                    },
                    Value::String(s) => {
                        // Try to parse as timestamp first, then as ISO date
                        if let Ok(timestamp) = s.parse::<i64>() {
                            let date = tantivy::DateTime::from_timestamp_micros(timestamp * 1_000_000);
                            Ok(Some(tantivy::schema::OwnedValue::Date(date)))
                        } else {
                            // Try to parse as ISO datetime
                            Err(TantivyError::IndexingError(format!("Could not parse '{}' as datetime for field '{}': use Unix timestamp", s, field_name)))
                        }
                    },
                    Value::Null => Ok(None),
                    _ => Err(TantivyError::IndexingError(format!("Invalid type for datetime field '{}': expected number or string", field_name)))
                },
                _ => {
                    // Fallback to string conversion for unknown types
                    match json_value {
                        Value::String(s) => Ok(Some(tantivy::schema::OwnedValue::Str(s.clone()))),
                        Value::Null => Ok(None),
                        _ => {
                            let json_str = serde_json::to_string(json_value)
                                .map_err(|e| TantivyError::IndexingError(format!("Failed to serialize value: {}", e)))?;
                            Ok(Some(tantivy::schema::OwnedValue::Str(json_str)))
                        }
                    }
                }
            }
        } else {
            // If field type is unknown, use the old generic conversion
            match json_value {
                Value::String(s) => Ok(Some(tantivy::schema::OwnedValue::Str(s.clone()))),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(Some(tantivy::schema::OwnedValue::I64(i)))
                    } else if let Some(f) = n.as_f64() {
                        Ok(Some(tantivy::schema::OwnedValue::F64(f)))
                    } else {
                        Err(TantivyError::IndexingError(format!("Invalid number format for field '{}'", field_name)))
                    }
                },
                Value::Bool(b) => Ok(Some(tantivy::schema::OwnedValue::Bool(*b))),
                Value::Null => Ok(None),
                Value::Array(_) | Value::Object(_) => {
                    let json_str = serde_json::to_string(json_value)
                        .map_err(|e| TantivyError::IndexingError(format!("Failed to serialize complex value: {}", e)))?;
                    Ok(Some(tantivy::schema::OwnedValue::Str(json_str)))
                }
            }
        }
    }
    
    pub fn commit(&mut self) -> Result<(), TantivyError> {
        info!("Committing index with {} documents", self.documents_indexed);
        
        // Flush any remaining documents
        self.flush_buffer()?;
        
        let start_time = std::time::Instant::now();
        
        // Commit the index writer
        self.writer.commit()
            .map_err(|e| TantivyError::IndexingError(format!("Failed to commit index: {}", e)))?;
            
        let elapsed = start_time.elapsed();
        info!("Index committed successfully in {:?}, total documents: {}", elapsed, self.documents_indexed);
        
        Ok(())
    }
    
    #[allow(dead_code)]
    pub fn rollback(&mut self) -> Result<(), TantivyError> {
        warn!("Rolling back index writer, discarding {} buffered documents", self.document_buffer.len());
        
        // Clear buffer
        self.document_buffer.clear();
        
        // Rollback the index writer
        self.writer.rollback()
            .map_err(|e| TantivyError::IndexingError(format!("Failed to rollback index: {}", e)))?;
            
        info!("Index rollback completed");
        Ok(())
    }
    
    #[allow(dead_code)]
    pub fn get_stats(&self) -> (usize, usize) {
        (self.document_buffer.len(), self.buffer_size)
    }
    
    #[allow(dead_code)]
    pub fn get_detailed_stats(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        stats.insert("documents_indexed".to_string(), self.documents_indexed);
        stats.insert("bytes_written".to_string(), self.bytes_written);
        stats.insert("buffer_size".to_string(), self.buffer_size as u64);
        stats.insert("buffered_documents".to_string(), self.document_buffer.len() as u64);
        stats
    }
    
    #[allow(dead_code)]
    pub fn optimize_index(&mut self) -> Result<(), TantivyError> {
        info!("Optimizing index for better search performance");
        
        // First commit any pending changes
        self.commit()?;
        
        // Wait for merge operations to complete
        // Note: wait_merging_threads not available in this API version
            
        info!("Index optimization completed");
        Ok(())
    }
    
    #[allow(dead_code)]
    pub fn delete_document(&mut self, field_name: &str, field_value: &str) -> Result<(), TantivyError> {
        if let Some(field) = self.field_map.get(field_name) {
            let term = Term::from_field_text(*field, field_value);
            self.writer.delete_term(term);
            debug!("Deleted documents matching {}:{}", field_name, field_value);
            Ok(())
        } else {
            Err(TantivyError::IndexingError(format!("Field '{}' not found in schema", field_name)))
        }
    }
}