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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TantivyError {
    #[error("JNI error: {0}")]
    JniError(String),
    
    #[error("Invalid ID: {0}")]
    InvalidId(String),
    
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Search error: {0}")]
    SearchError(String),
    
    #[error("Indexing error: {0}")]
    IndexingError(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
    
    #[error("Tantivy error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),
    
    #[error("Anyhow error: {0}")]
    AnyhowError(#[from] anyhow::Error),
}

impl Default for TantivyError {
    fn default() -> Self {
        TantivyError::ConfigError("Unknown error".to_string())
    }
}

impl From<TantivyError> for i64 {
    fn from(_: TantivyError) -> Self {
        -1
    }
}

// Helper trait for converting errors to JNI-safe values
#[allow(dead_code)]
pub trait JniSafe<T> {
    fn jni_safe(self) -> T;
}

impl JniSafe<i64> for Result<i64, TantivyError> {
    fn jni_safe(self) -> i64 {
        match self {
            Ok(val) => val,
            Err(_) => -1,
        }
    }
}

impl JniSafe<u8> for Result<u8, TantivyError> {
    fn jni_safe(self) -> u8 {
        match self {
            Ok(val) => val,
            Err(_) => 0,
        }
    }
}