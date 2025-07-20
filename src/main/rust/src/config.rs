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
use std::path::PathBuf;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::errors::TantivyError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfigData {
    pub index_id: String,
    pub index_uri: String,
    pub doc_mapping: DocMappingConfig,
    pub indexing_settings: IndexingSettings,
    pub search_settings: SearchSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocMappingConfig {
    pub mode: String,
    pub field_mappings: HashMap<String, FieldMapping>,
    pub timestamp_field: Option<String>,
    pub default_search_fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    pub field_type: String,
    pub indexed: bool,
    pub stored: bool,
    pub fast: bool,
    pub field_norms: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingSettings {
    pub commit_timeout_secs: u64,
    pub split_num_docs: u64,
    pub split_num_bytes: u64,
    pub merge_policy: String,
    pub resources: ResourceSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSettings {
    pub max_merge_write_throughput: String,
    pub heap_size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchSettings {
    pub default_search_fields: Vec<String>,
    pub max_hits: u64,
    pub enable_aggregations: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetastoreConfig {
    pub metastore_uri: String,
    pub metastore_type: String,
}

#[derive(Debug, Clone)]
pub struct TantivyConfigWrapper {
    pub index_config: IndexConfigData,
    #[allow(dead_code)]
    pub base_path: PathBuf,
    #[allow(dead_code)]
    pub storage_uri: String,
    #[allow(dead_code)]
    pub metastore: MetastoreConfig,
}

impl TantivyConfigWrapper {
    pub fn new(config_json: &str) -> Result<Self, TantivyError> {
        let config_value: Value = serde_json::from_str(config_json)
            .map_err(|e| TantivyError::ConfigError(format!("Invalid JSON: {}", e)))?;
        
        // Extract base configuration
        let base_path = config_value
            .get("base_path")
            .and_then(|v| v.as_str())
            .unwrap_or("./tantivy-data")
            .into();
            
        let storage_uri = config_value
            .get("storage_uri")
            .and_then(|v| v.as_str())
            .unwrap_or("file://./tantivy-data")
            .to_string();
            
        // Parse metastore configuration
        let metastore = if let Some(metastore_value) = config_value.get("metastore") {
            serde_json::from_value(metastore_value.clone())
                .map_err(|e| TantivyError::ConfigError(format!("Invalid metastore config: {}", e)))?
        } else {
            MetastoreConfig {
                metastore_uri: format!("{}/metastore", storage_uri),
                metastore_type: "file".to_string(),
            }
        };
            
        // Parse index configuration
        let index_config = if let Some(indexes_value) = config_value.get("indexes") {
            if let Some(indexes_array) = indexes_value.as_array() {
                if let Some(first_index) = indexes_array.first() {
                    serde_json::from_value(first_index.clone())
                        .map_err(|e| TantivyError::ConfigError(format!("Invalid index config: {}", e)))?
                } else {
                    return Err(TantivyError::ConfigError("Empty indexes array".to_string()));
                }
            } else {
                return Err(TantivyError::ConfigError("indexes field is not an array".to_string()));
            }
        } else if let Some(index_config_value) = config_value.get("index_config") {
            // Fallback to old format for backward compatibility
            serde_json::from_value(index_config_value.clone())
                .map_err(|e| TantivyError::ConfigError(format!("Invalid index config: {}", e)))?
        } else {
            // Create a default index configuration
            Self::create_default_index_config()
        };
        
        Ok(TantivyConfigWrapper {
            index_config,
            base_path,
            storage_uri,
            metastore,
        })
    }
    
    fn create_default_index_config() -> IndexConfigData {
        let mut field_mappings = HashMap::new();
        field_mappings.insert("_id".to_string(), FieldMapping {
            field_type: "text".to_string(),
            indexed: true,
            stored: true,
            fast: false,
            field_norms: false,
        });
        field_mappings.insert("_timestamp".to_string(), FieldMapping {
            field_type: "datetime".to_string(),
            indexed: true,
            stored: true,
            fast: true,
            field_norms: false,
        });
        
        IndexConfigData {
            index_id: "default_index".to_string(),
            index_uri: "file://./tantivy-data/default_index".to_string(),
            doc_mapping: DocMappingConfig {
                mode: "strict".to_string(),
                field_mappings,
                timestamp_field: Some("_timestamp".to_string()),
                default_search_fields: vec![],
            },
            indexing_settings: IndexingSettings {
                commit_timeout_secs: 60,
                split_num_docs: 10_000_000,
                split_num_bytes: 2_000_000_000,
                merge_policy: "log_merge".to_string(),
                resources: ResourceSettings {
                    max_merge_write_throughput: "100MB".to_string(),
                    heap_size: "2GB".to_string(),
                },
            },
            search_settings: SearchSettings {
                default_search_fields: vec![],
                max_hits: 10000,
                enable_aggregations: true,
            },
        }
    }
    
    #[allow(dead_code)]
    pub fn get_index_uri(&self, index_id: &str) -> String {
        format!("{}/{}", self.storage_uri, index_id)
    }
    
    #[allow(dead_code)]
    pub fn get_metastore_uri(&self) -> String {
        self.metastore.metastore_uri.clone()
    }
    
    #[allow(dead_code)]
    pub fn get_index_path(&self, index_id: &str) -> PathBuf {
        self.base_path.join(index_id)
    }
    
    #[allow(dead_code)]
    pub fn validate(&self) -> Result<(), TantivyError> {
        if self.index_config.index_id.is_empty() {
            return Err(TantivyError::ConfigError("Index ID cannot be empty".to_string()));
        }
        
        if self.index_config.doc_mapping.field_mappings.is_empty() {
            return Err(TantivyError::ConfigError("Field mappings cannot be empty".to_string()));
        }
        
        // Validate timestamp field exists if specified
        if let Some(timestamp_field) = &self.index_config.doc_mapping.timestamp_field {
            if !self.index_config.doc_mapping.field_mappings.contains_key(timestamp_field) {
                return Err(TantivyError::ConfigError(
                    format!("Timestamp field '{}' not found in field mappings", timestamp_field)
                ));
            }
        }
        
        Ok(())
    }
    
    #[allow(dead_code)]
    pub fn to_json(&self) -> Result<String, TantivyError> {
        let config_map = serde_json::json!({
            "base_path": self.base_path,
            "storage_uri": self.storage_uri,
            "metastore": self.metastore,
            "index_config": self.index_config
        });
        
        serde_json::to_string_pretty(&config_map)
            .map_err(|e| TantivyError::ConfigError(format!("Failed to serialize config: {}", e)))
    }
}