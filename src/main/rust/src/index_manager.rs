use tantivy::{Index, IndexWriter, query::QueryParser, collector::TopDocs};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use crate::error::TantivyError;
use crate::schema_mapper::SchemaMapper;

pub struct IndexManager {
    index: Index,
    writer: Arc<Mutex<IndexWriter>>,
    schema_mapper: SchemaMapper,
    // Keep track of documents added for serialization
    documents: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl IndexManager {
    pub fn create_from_schema(schema_json: &str) -> Result<Self, TantivyError> {
        let schema_mapper = SchemaMapper::from_json(schema_json)?;
        let schema = schema_mapper.to_tantivy_schema()?;
        
        let index = Index::create_in_ram(schema);
        let writer = index.writer(50_000_000)?;
        
        Ok(IndexManager {
            index,
            writer: Arc::new(Mutex::new(writer)),
            schema_mapper,
            documents: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn add_document(&self, document_json: &str) -> Result<(), TantivyError> {
        let doc_value: Value = serde_json::from_str(document_json)?;
        let document = self.schema_mapper.json_to_document(&doc_value)?;
        
        // Store the document for later serialization
        {
            let mut documents = self.documents.lock().unwrap();
            documents.push(doc_value.clone());
        }
        
        let writer = self.writer.lock().unwrap();
        writer.add_document(document)?;
        Ok(())
    }

    pub fn commit(&self) -> Result<(), TantivyError> {
        let mut writer = self.writer.lock().unwrap();
        writer.commit()?;
        Ok(())
    }

    pub fn search_all(&self, limit: usize) -> Result<Value, TantivyError> {
        // Handle edge case: if limit is 0, return empty results
        if limit == 0 {
            let result = serde_json::json!({
                "hits": []
            });
            return Ok(result);
        }
        
        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        
        // Use Tantivy's AllQuery to match all documents
        use tantivy::query::AllQuery;
        let query = AllQuery;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;
        
        let mut hits = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc(doc_address)?;
            let json_doc = self.schema_mapper.document_to_json(&retrieved_doc)?;
            let hit = serde_json::json!({
                "score": score,
                "doc": json_doc
            });
            hits.push(hit);
        }
        
        let result = serde_json::json!({
            "hits": hits
        });
        
        Ok(result)
    }

    pub fn search(&self, query_str: &str, limit: usize) -> Result<Value, TantivyError> {
        // Handle edge case: if limit is 0, return empty results
        if limit == 0 {
            let result = serde_json::json!({
                "hits": []
            });
            return Ok(result);
        }
        
        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        
        let query_parser = QueryParser::for_index(&self.index, self.schema_mapper.get_default_search_fields());
        let query = query_parser.parse_query(query_str).map_err(|e| TantivyError::QueryError(format!("Parse error: {:?}", e)))?;
        
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;
        
        let mut hits = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc(doc_address)?;
            let json_doc = self.schema_mapper.document_to_json(&retrieved_doc)?;
            let hit = serde_json::json!({
                "score": score,
                "doc": json_doc
            });
            hits.push(hit);
        }
        
        let result = serde_json::json!({
            "hits": hits
        });
        
        Ok(result)
    }

    pub fn create_from_components(schema_json: &str, components_json: &str) -> Result<Self, TantivyError> {
        use base64::{Engine as _, engine::general_purpose};
        
        let schema_mapper = SchemaMapper::from_json(schema_json)?;
        let schema = schema_mapper.to_tantivy_schema()?;
        
        // Parse components from JSON (base64 encoded components)
        let components: HashMap<String, String> = serde_json::from_str(components_json)?;
        
        // Create a new index in RAM
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer(50_000_000)?;
        let mut stored_documents = Vec::new();
        
        // If we have a data.json component, restore the documents
        if let Some(data_base64) = components.get("data.json") {
            println!("Found data.json component, restoring documents...");
            let data_bytes = general_purpose::STANDARD.decode(data_base64)
                .map_err(|e| TantivyError::SerializationError(format!("Failed to decode data.json: {}", e)))?;
            let data_str = String::from_utf8(data_bytes)
                .map_err(|e| TantivyError::SerializationError(format!("Failed to parse data.json as UTF-8: {}", e)))?;
            
            let documents: Vec<serde_json::Value> = serde_json::from_str(&data_str)
                .map_err(|e| TantivyError::SerializationError(format!("Failed to parse documents JSON: {}", e)))?;
            
            // Add all documents to the index and store them
            let doc_count = documents.len();
            println!("Restoring {} documents to index", doc_count);
            for doc_value in &documents {
                let document = schema_mapper.json_to_document(doc_value)?;
                writer.add_document(document)?;
            }
            
            // Store the documents for future serialization
            stored_documents = documents;
            
            // Commit the documents
            writer.commit()?;
            println!("Successfully restored {} documents", doc_count);
        }
        
        Ok(IndexManager {
            index,
            writer: Arc::new(Mutex::new(writer)),
            schema_mapper,
            documents: Arc::new(Mutex::new(stored_documents)),
        })
    }

    pub fn get_components(&self) -> Result<HashMap<String, String>, TantivyError> {
        use base64::{Engine as _, engine::general_purpose};
        
        let mut components = HashMap::new();
        
        // Add schema
        components.insert("schema.json".to_string(), general_purpose::STANDARD.encode(self.schema_mapper.to_json()?));
        
        // Force commit to ensure all data is written
        {
            let mut writer = self.writer.lock().unwrap();
            writer.commit()?;
        }
        
        // Use the same pattern as the search method to get a reader
        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        let total_docs: u32 = searcher.segment_readers().iter()
            .map(|sr| sr.max_doc())
            .sum();
        
        // Create a simple representation of segments for meta.json
        let segments: Vec<_> = searcher.segment_readers().iter()
            .map(|sr| (sr.segment_id(), sr.max_doc()))
            .collect();
        
        
        // Create meta.json with actual segment information
        let documents = self.documents.lock().unwrap();
        let meta_json = format!(r#"{{"version": "0.22.1", "segments": [{}], "doc_count": {}}}"#, 
            segments.iter()
                .map(|(segment_id, max_doc)| format!(r#"{{"segment_id": "{}", "max_doc": {}}}"#, segment_id, max_doc))
                .collect::<Vec<_>>()
                .join(","),
            total_docs
        );
        components.insert("meta.json".to_string(), general_purpose::STANDARD.encode(&meta_json));
        
        // Use the stored documents for serialization (reuse the lock from above)
        if !documents.is_empty() {
            let data_json = serde_json::to_string(&*documents)?;
            components.insert("data.json".to_string(), general_purpose::STANDARD.encode(&data_json));
        }
        
        
        Ok(components)
    }
}