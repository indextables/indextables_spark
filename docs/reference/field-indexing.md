# Field Indexing Reference

Detailed field type configuration for IndexTables4Spark.

---

## Field Types

```scala
// String fields (default) - exact matching, full filter pushdown
spark.indextables.indexing.typemap.<field>: "string"

// Text fields - full-text search, IndexQuery only
spark.indextables.indexing.typemap.<field>: "text"

// JSON fields - automatic for Struct/Array/Map types
// No configuration needed - auto-detected
// Optional: Control JSON indexing mode
spark.indextables.indexing.json.mode: "full" (default) or "minimal"
```

## List-Based Typemap Syntax (Recommended)

Configure multiple fields with the same type in one line:

```scala
// New syntax: typemap.<type> = "field1,field2,..."
spark.indextables.indexing.typemap.text: "title,content,body,description"
spark.indextables.indexing.typemap.string: "status,category,tags"
spark.indextables.indexing.typemap.json: "metadata,attributes"

// Old per-field syntax still works
spark.indextables.indexing.typemap.title: "text"
```

---

## Index Record Options (for text fields)

Controls what information is stored in the inverted index:
- `basic` - Document IDs only (smallest index)
- `freq` - Document IDs + term frequency (enables TF-IDF scoring)
- `position` - Document IDs + frequency + positions (enables phrase queries, default)

```scala
// Default for all text fields (default: "position")
spark.indextables.indexing.text.indexRecordOption: "position"

// List-based syntax (recommended)
spark.indextables.indexing.indexrecordoption.position: "title,content,body"
spark.indextables.indexing.indexrecordoption.basic: "logs,metrics"

// Old per-field syntax still works
spark.indextables.indexing.indexrecordoption.logs: "basic"
```

---

## Tokenizers (for text fields)

```scala
// List-based syntax: tokenizer.<tokenizer_name> = "field1,field2,..."
spark.indextables.indexing.tokenizer.en_stem: "title,content,body"  // English stemming
spark.indextables.indexing.tokenizer.default: "exact_match_field"   // No stemming

// Old per-field syntax still works
spark.indextables.indexing.tokenizer.content: "en_stem"

// Available tokenizers: default, raw, en_stem, whitespace
```

---

## Token Length Limits (for text fields)

Tokens longer than the limit are **filtered out** (not truncated) during tokenization.

**Token length constants:**

| Constant | Value | Description |
|----------|-------|-------------|
| `tantivy_max` | 65,530 | Maximum supported by Tantivy (u16::MAX - 5) |
| `default` | 255 | Quickwit-compatible default (recommended) |
| `legacy` | 40 | Original tantivy4java default |
| `min` | 1 | Minimum valid limit |

```scala
// Default for all text fields (default: 255, Quickwit-compatible)
spark.indextables.indexing.text.maxTokenLength: 255

// Per-field overrides using list-based syntax (recommended)
spark.indextables.indexing.tokenLength.255: "content,body"
spark.indextables.indexing.tokenLength.legacy: "short_content"
spark.indextables.indexing.tokenLength.tantivy_max: "url,base64"

// Old per-field syntax also works
spark.indextables.indexing.tokenLength.content: "255"
spark.indextables.indexing.tokenLength.url: "tantivy_max"
```

**Breaking change notice:** The default token length changed from 40 bytes to 255 bytes. Tokens between 41-255 bytes that were previously filtered out will now be indexed. To restore the legacy 40-byte behavior:
```scala
spark.indextables.indexing.text.maxTokenLength: "legacy"
```

**Use cases:**
- **URLs:** Use `tantivy_max` for URL fields that may contain very long URLs
- **Base64 data:** Use `tantivy_max` for fields containing encoded data
- **Backward compatibility:** Use `legacy` to maintain compatibility with existing indices

---

## Fast Fields (for aggregations)

```scala
spark.indextables.indexing.fastfields: "score,value,timestamp"
// Auto-fast-field: first numeric field becomes fast if not configured
```
