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

use jni::objects::{JClass, JString};
use jni::sys::{jlong, jstring, jboolean};
use jni::JNIEnv;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use anyhow::Result;
use once_cell::sync::Lazy;
use tracing::{info, error, debug};

mod search;
mod indexing;
mod config;
mod errors;

use search::SearchEngineWrapper;
use indexing::IndexWriterWrapper;
use config::TantivyConfigWrapper;
use errors::TantivyError;

// Initialize tracing
static TRACING_INIT: Lazy<()> = Lazy::new(|| {
    tracing_subscriber::fmt::init();
});

// Global state management for Tantivy instances
static SEARCH_ENGINES: Lazy<Arc<Mutex<HashMap<i64, SearchEngineWrapper>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));
static INDEX_WRITERS: Lazy<Arc<Mutex<HashMap<i64, IndexWriterWrapper>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));
static CONFIGS: Lazy<Arc<Mutex<HashMap<i64, TantivyConfigWrapper>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));
static NEXT_ID: Lazy<Arc<Mutex<i64>>> = Lazy::new(|| Arc::new(Mutex::new(1)));

fn get_next_id() -> i64 {
    Lazy::force(&TRACING_INIT);
    let mut id = NEXT_ID.lock().unwrap();
    let current = *id;
    *id += 1;
    debug!("Generated new ID: {}", current);
    current
}

// Helper function to convert Java string to Rust string
fn jstring_to_string(env: &mut JNIEnv, jstr: JString) -> Result<String, TantivyError> {
    env.get_string(&jstr)
        .map(|s| s.into())
        .map_err(|e| TantivyError::JniError(format!("Failed to convert JString: {}", e)))
}

// Helper function to create Java string from Rust string
fn string_to_jstring(env: &mut JNIEnv, s: String) -> Result<jstring, TantivyError> {
    env.new_string(s)
        .map(|jstr| jstr.into_raw())
        .map_err(|e| TantivyError::JniError(format!("Failed to create JString: {}", e)))
}

// Helper function to handle errors and return -1 for invalid operations
fn handle_error<T>(result: Result<T, TantivyError>) -> T 
where 
    T: Default + From<i64>
{
    match result {
        Ok(value) => value,
        Err(e) => {
            error!("Error in JNI operation: {}", e);
            T::from(-1)
        }
    }
}

// Configuration Management
#[no_mangle]
pub extern "system" fn Java_com_tantivy4spark_native_TantivyNative_createConfig(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
) -> jlong {
    let result = (|| -> Result<jlong, TantivyError> {
        let config_str = jstring_to_string(&mut env, config_json)?;
        info!("Creating config with JSON: {}", config_str);
        
        let config = TantivyConfigWrapper::new(&config_str)?;
        let id = get_next_id();
        
        CONFIGS.lock().unwrap().insert(id, config);
        info!("Created config with ID: {}", id);
        Ok(id)
    })();
    
    handle_error(result)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4spark_native_TantivyNative_destroyConfig(
    _env: JNIEnv,
    _class: JClass,
    config_id: jlong,
) {
    info!("Destroying config with ID: {}", config_id);
    if let Some(_) = CONFIGS.lock().unwrap().remove(&config_id) {
        debug!("Successfully destroyed config: {}", config_id);
    } else {
        error!("Config not found for ID: {}", config_id);
    }
}

// Search Engine Management
#[no_mangle]
pub extern "system" fn Java_com_tantivy4spark_native_TantivyNative_createSearchEngine(
    mut env: JNIEnv,
    _class: JClass,
    config_id: jlong,
    index_path: JString,
) -> jlong {
    let result = (|| -> Result<jlong, TantivyError> {
        let path_str = jstring_to_string(&mut env, index_path)?;
        info!("Creating search engine for config: {}, path: {}", config_id, path_str);
        
        let configs = CONFIGS.lock().unwrap();
        let config = configs.get(&config_id)
            .ok_or_else(|| TantivyError::InvalidId(format!("Config not found: {}", config_id)))?;
        
        let engine = SearchEngineWrapper::new(config, &path_str)?;
        let id = get_next_id();
        drop(configs); // Release the lock
        
        SEARCH_ENGINES.lock().unwrap().insert(id, engine);
        info!("Created search engine with ID: {}", id);
        Ok(id)
    })();
    
    handle_error(result)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4spark_native_TantivyNative_search(
    mut env: JNIEnv,
    _class: JClass,
    engine_id: jlong,
    query: JString,
    max_hits: jlong,
) -> jstring {
    let result = (|| -> Result<jstring, TantivyError> {
        let query_str = jstring_to_string(&mut env, query)?;
        debug!("Executing search: engine={}, query={}, max_hits={}", engine_id, query_str, max_hits);
        
        let engines = SEARCH_ENGINES.lock().unwrap();
        let engine = engines.get(&engine_id)
            .ok_or_else(|| TantivyError::InvalidId(format!("Search engine not found: {}", engine_id)))?;
        
        let results_json = engine.search(&query_str, max_hits as u64)?;
        drop(engines); // Release the lock
        
        string_to_jstring(&mut env, results_json)
    })();
    
    match result {
        Ok(jstr) => jstr,
        Err(e) => {
            error!("Search failed: {}", e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4spark_native_TantivyNative_destroySearchEngine(
    _env: JNIEnv,
    _class: JClass,
    engine_id: jlong,
) {
    info!("Destroying search engine with ID: {}", engine_id);
    if let Some(_) = SEARCH_ENGINES.lock().unwrap().remove(&engine_id) {
        debug!("Successfully destroyed search engine: {}", engine_id);
    } else {
        error!("Search engine not found for ID: {}", engine_id);
    }
}

// Index Writer Management
#[no_mangle]
pub extern "system" fn Java_com_tantivy4spark_native_TantivyNative_createIndexWriter(
    mut env: JNIEnv,
    _class: JClass,
    config_id: jlong,
    index_path: JString,
    schema_json: JString,
) -> jlong {
    let result = (|| -> Result<jlong, TantivyError> {
        let path_str = jstring_to_string(&mut env, index_path)?;
        let schema_str = jstring_to_string(&mut env, schema_json)?;
        info!("Creating index writer for config: {}, path: {}", config_id, path_str);
        
        let configs = CONFIGS.lock().unwrap();
        let config = configs.get(&config_id)
            .ok_or_else(|| TantivyError::InvalidId(format!("Config not found: {}", config_id)))?;
        
        let writer = IndexWriterWrapper::new(config, &path_str, &schema_str)?;
        let id = get_next_id();
        drop(configs); // Release the lock
        
        INDEX_WRITERS.lock().unwrap().insert(id, writer);
        info!("Created index writer with ID: {}", id);
        Ok(id)
    })();
    
    handle_error(result)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4spark_native_TantivyNative_indexDocument(
    mut env: JNIEnv,
    _class: JClass,
    writer_id: jlong,
    document_json: JString,
) -> jboolean {
    let result = (|| -> Result<jboolean, TantivyError> {
        let doc_str = jstring_to_string(&mut env, document_json)?;
        debug!("Indexing document: writer={}, doc_len={}", writer_id, doc_str.len());
        
        let mut writers = INDEX_WRITERS.lock().unwrap();
        let writer = writers.get_mut(&writer_id)
            .ok_or_else(|| TantivyError::InvalidId(format!("Index writer not found: {}", writer_id)))?;
        
        writer.add_document(&doc_str)?;
        Ok(1)
    })();
    
    match result {
        Ok(success) => success,
        Err(e) => {
            error!("Failed to index document: {}", e);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4spark_native_TantivyNative_commitIndex(
    _env: JNIEnv,
    _class: JClass,
    writer_id: jlong,
) -> jboolean {
    let result = (|| -> Result<jboolean, TantivyError> {
        info!("Committing index: writer={}", writer_id);
        
        let mut writers = INDEX_WRITERS.lock().unwrap();
        let writer = writers.get_mut(&writer_id)
            .ok_or_else(|| TantivyError::InvalidId(format!("Index writer not found: {}", writer_id)))?;
        
        writer.commit()?;
        info!("Successfully committed index: {}", writer_id);
        Ok(1)
    })();
    
    match result {
        Ok(success) => success,
        Err(e) => {
            error!("Failed to commit index: {}", e);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4spark_native_TantivyNative_destroyIndexWriter(
    _env: JNIEnv,
    _class: JClass,
    writer_id: jlong,
) {
    info!("Destroying index writer with ID: {}", writer_id);
    if let Some(_) = INDEX_WRITERS.lock().unwrap().remove(&writer_id) {
        debug!("Successfully destroyed index writer: {}", writer_id);
    } else {
        error!("Index writer not found for ID: {}", writer_id);
    }
}