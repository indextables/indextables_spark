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


use jni::JNIEnv;
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jstring, jlong, jboolean, jint};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

mod index_manager;
mod schema_mapper;
mod error;

pub use index_manager::IndexManager;
pub use schema_mapper::SchemaMapper;
pub use error::TantivyError;

type IndexRegistry = Arc<Mutex<HashMap<i64, Arc<IndexManager>>>>;

lazy_static::lazy_static! {
    static ref INDEXES: IndexRegistry = Arc::new(Mutex::new(HashMap::new()));
}

#[no_mangle]
pub extern "C" fn Java_com_tantivy4spark_search_TantivyNative_00024_createIndex(
    mut env: JNIEnv,
    _class: JClass,
    schema_json: JString,
) -> jlong {
    let schema_str: String = match env.get_string(&schema_json) {
        Ok(s) => s.into(),
        Err(_) => return -1,
    };

    match IndexManager::create_from_schema(&schema_str) {
        Ok(manager) => {
            let handle = rand::random::<i64>();
            let mut registry = INDEXES.lock().unwrap();
            registry.insert(handle, Arc::new(manager));
            handle
        },
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn Java_com_tantivy4spark_search_TantivyNative_00024_addDocument(
    mut env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
    document_json: JString,
) -> jboolean {
    let doc_str: String = match env.get_string(&document_json) {
        Ok(s) => s.into(),
        Err(_) => return false as jboolean,
    };

    let registry = INDEXES.lock().unwrap();
    if let Some(manager) = registry.get(&index_handle) {
        manager.add_document(&doc_str).is_ok() as jboolean
    } else {
        false as jboolean
    }
}

#[no_mangle]
pub extern "C" fn Java_com_tantivy4spark_search_TantivyNative_00024_commit(
    _env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
) -> jboolean {
    let registry = INDEXES.lock().unwrap();
    if let Some(manager) = registry.get(&index_handle) {
        manager.commit().is_ok() as jboolean
    } else {
        false as jboolean
    }
}

#[no_mangle]
pub extern "C" fn Java_com_tantivy4spark_search_TantivyNative_00024_search(
    mut env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
    query_str: JString,
    limit: jint,
) -> jstring {
    let query: String = match env.get_string(&query_str) {
        Ok(s) => s.into(),
        Err(_) => return JObject::null().as_raw(),
    };

    let registry = INDEXES.lock().unwrap();
    if let Some(manager) = registry.get(&index_handle) {
        match manager.search(&query, limit as usize) {
            Ok(results) => {
                match serde_json::to_string(&results) {
                    Ok(json) => env.new_string(json).unwrap().as_raw(),
                    Err(_) => JObject::null().as_raw(),
                }
            },
            Err(_) => JObject::null().as_raw(),
        }
    } else {
        JObject::null().as_raw()
    }
}

#[no_mangle]
pub extern "C" fn Java_com_tantivy4spark_search_TantivyNative_00024_searchAll(
    env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
    limit: jint,
) -> jstring {
    let registry = INDEXES.lock().unwrap();
    if let Some(manager) = registry.get(&index_handle) {
        match manager.search_all(limit as usize) {
            Ok(results) => {
                match serde_json::to_string(&results) {
                    Ok(json) => env.new_string(json).unwrap().as_raw(),
                    Err(_) => JObject::null().as_raw(),
                }
            },
            Err(_) => JObject::null().as_raw(),
        }
    } else {
        JObject::null().as_raw()
    }
}

#[no_mangle]
pub extern "C" fn Java_com_tantivy4spark_search_TantivyNative_00024_closeIndex(
    _env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
) -> jboolean {
    let mut registry = INDEXES.lock().unwrap();
    registry.remove(&index_handle).is_some() as jboolean
}

#[no_mangle]
pub extern "C" fn Java_com_tantivy4spark_search_TantivyNative_00024_createIndexFromComponents(
    mut env: JNIEnv,
    _class: JClass,
    schema_json: JString,
    components_json: JString,
) -> jlong {
    let schema_str: String = match env.get_string(&schema_json) {
        Ok(s) => s.into(),
        Err(_) => return -1,
    };

    let components_str: String = match env.get_string(&components_json) {
        Ok(s) => s.into(),
        Err(_) => return -1,
    };

    match IndexManager::create_from_components(&schema_str, &components_str) {
        Ok(manager) => {
            let handle = rand::random::<i64>();
            let mut registry = INDEXES.lock().unwrap();
            registry.insert(handle, Arc::new(manager));
            handle
        },
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn Java_com_tantivy4spark_search_TantivyNative_00024_getIndexComponents(
    env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
) -> jstring {
    let registry = INDEXES.lock().unwrap();
    if let Some(manager) = registry.get(&index_handle) {
        match manager.get_components() {
            Ok(components) => {
                match serde_json::to_string(&components) {
                    Ok(json) => env.new_string(json).unwrap().as_raw(),
                    Err(_) => JObject::null().as_raw(),
                }
            },
            Err(_) => JObject::null().as_raw(),
        }
    } else {
        JObject::null().as_raw()
    }
}