use tantivy::schema::{Schema, SchemaBuilder, Field, TextOptions, NumericOptions};
use tantivy::TantivyDocument;
use serde_json::{Value, Map};
use std::collections::HashMap;
use crate::error::TantivyError;
use base64::{Engine as _, engine::general_purpose};

pub struct SchemaMapper {
    schema: Schema,
    field_mapping: HashMap<String, Field>,
    reverse_mapping: HashMap<Field, String>,
}

impl SchemaMapper {
    pub fn from_json(schema_json: &str) -> Result<Self, TantivyError> {
        let schema_value: Value = serde_json::from_str(schema_json)?;
        let fields = schema_value["fields"].as_array()
            .ok_or_else(|| TantivyError::SchemaError("Missing fields array".to_string()))?;

        let mut schema_builder = SchemaBuilder::default();
        let mut field_mapping = HashMap::new();
        let mut reverse_mapping = HashMap::new();

        for field_def in fields {
            let name = field_def["name"].as_str()
                .ok_or_else(|| TantivyError::SchemaError("Missing field name".to_string()))?;
            let field_type = field_def["type"].as_str()
                .ok_or_else(|| TantivyError::SchemaError("Missing field type".to_string()))?;
            let indexed = field_def["indexed"].as_bool().unwrap_or(true);
            let stored = field_def["stored"].as_bool().unwrap_or(true);

            let field = match field_type {
                "text" => {
                    let mut options = TextOptions::default();
                    if stored { options = options.set_stored(); }
                    if indexed { options = options.set_indexing_options(
                        tantivy::schema::TextFieldIndexing::default()
                            .set_tokenizer("default")
                            .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions)
                    ); }
                    schema_builder.add_text_field(name, options)
                },
                "i64" | "long" => {
                    let mut options = NumericOptions::default();
                    if stored { options = options.set_stored(); }
                    options = options.set_indexed().set_fast();
                    schema_builder.add_i64_field(name, options)
                },
                "f64" | "double" => {
                    let mut options = NumericOptions::default();
                    if stored { options = options.set_stored(); }
                    options = options.set_indexed().set_fast();
                    schema_builder.add_f64_field(name, options)
                },
                "bytes" => {
                    let mut options = tantivy::schema::BytesOptions::default();
                    if stored { options = options.set_stored(); }
                    if indexed { options = options.set_indexed(); }
                    schema_builder.add_bytes_field(name, options)
                },
                _ => return Err(TantivyError::SchemaError(format!("Unsupported field type: {}", field_type))),
            };

            field_mapping.insert(name.to_string(), field);
            reverse_mapping.insert(field, name.to_string());
        }

        let schema = schema_builder.build();

        Ok(SchemaMapper {
            schema,
            field_mapping,
            reverse_mapping,
        })
    }

    pub fn to_tantivy_schema(&self) -> Result<Schema, TantivyError> {
        Ok(self.schema.clone())
    }

    pub fn json_to_document(&self, json_value: &Value) -> Result<TantivyDocument, TantivyError> {
        let mut document = TantivyDocument::default();
        
        if let Some(obj) = json_value.as_object() {
            for (key, value) in obj {
                if let Some(field) = self.field_mapping.get(key) {
                    match value {
                        Value::String(s) => document.add_text(*field, s),
                        Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                document.add_i64(*field, i);
                            } else if let Some(f) = n.as_f64() {
                                document.add_f64(*field, f);
                            }
                        },
                        Value::Bool(b) => document.add_i64(*field, if *b { 1 } else { 0 }),
                        _ => {
                            let json_str = serde_json::to_string(value)?;
                            document.add_text(*field, &json_str);
                        }
                    }
                }
            }
        }
        
        Ok(document)
    }

    pub fn document_to_json(&self, document: &TantivyDocument) -> Result<Value, TantivyError> {
        let mut result = Map::new();
        
        for field_value in document.field_values() {
            let field = field_value.field();
            let value = field_value.value();
            if let Some(field_name) = self.reverse_mapping.get(&field) {
                let json_value = match value {
                    tantivy::schema::OwnedValue::Str(s) => Value::String(s.clone()),
                    tantivy::schema::OwnedValue::I64(i) => Value::Number((*i).into()),
                    tantivy::schema::OwnedValue::F64(f) => {
                        Value::Number(serde_json::Number::from_f64(*f).unwrap_or_else(|| 0.into()))
                    },
                    tantivy::schema::OwnedValue::Bytes(b) => Value::String(general_purpose::STANDARD.encode(b)),
                    _ => Value::Null,
                };
                result.insert(field_name.clone(), json_value);
            }
        }
        
        Ok(Value::Object(result))
    }

    pub fn get_default_search_fields(&self) -> Vec<Field> {
        self.field_mapping.values().copied().collect()
    }

    pub fn to_json(&self) -> Result<String, TantivyError> {
        // Convert the internal schema back to JSON representation
        let mut fields = Vec::new();
        
        for (field_name, field) in &self.field_mapping {
            let field_entry = self.schema.get_field_entry(*field);
            let field_type = field_entry.field_type();
            
            let (type_name, indexed, stored) = match field_type {
                tantivy::schema::FieldType::Str(options) => {
                    ("text", options.get_indexing_options().is_some(), options.is_stored())
                },
                tantivy::schema::FieldType::I64(options) => {
                    ("i64", options.is_indexed(), options.is_stored())
                },
                tantivy::schema::FieldType::F64(options) => {
                    ("f64", options.is_indexed(), options.is_stored())
                },
                tantivy::schema::FieldType::Bytes(options) => {
                    ("bytes", options.is_indexed(), options.is_stored())
                },
                _ => ("unknown", false, false),
            };
            
            let field_def = serde_json::json!({
                "name": field_name,
                "type": type_name,
                "indexed": indexed,
                "stored": stored
            });
            
            fields.push(field_def);
        }
        
        let schema_json = serde_json::json!({
            "fields": fields
        });
        
        Ok(serde_json::to_string(&schema_json)?)
    }
}