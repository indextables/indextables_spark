# tantivy4java Feature Requests: Arrow FFI Transaction Log

Four feature requests to enable Arrow FFI as the primary interchange format for transaction log operations and move remaining filtering logic to native.

**Design principle — no JSON round-trips:** JSON is acceptable for one-time, small payloads sent once per query (e.g., a filter predicate). It is NOT acceptable for per-row data in Arrow columns. Every `HashMap` or array that currently crosses JNI as a JSON-encoded Utf8 string must be replaced with a native Arrow type or eliminated from the output entirely.

---

## Feature Request 1: `nativeListFilesArrowFfi` — Read Path with All Filtering

### Summary

New JNI entry point that combines snapshot reading, manifest loading, **all filtering** (partition pruning, min/max data skipping, cooldown exclusion), post-checkpoint replay, and schema deduplication into a single native call, returning results via Arrow FFI instead of TANT binary.

This replaces the current multi-step JVM orchestration:
1. `getSnapshotInfo()` → get manifest paths
2. `readManifest()` × N → get file entries from each manifest
3. `readPostCheckpointChanges()` → get adds/removes since checkpoint
4. JVM-side log replay (merge adds, subtract removes)
5. JVM-side `PartitionPruning.prunePartitions()` → filter by partition values
6. JVM-side `shouldSkipFile()` / `canFileMatchFilters()` → min/max data skipping (~300 lines in IndexTables4SparkScan)
7. JVM-side `filterFilesInCooldown()` → exclude skipped/cooldown files (separate JNI round-trip)
8. JVM-side `restoreSchemas()` → resolve docMappingRef → docMappingJson

### JNI Signature

```java
package io.indextables.jni.txlog;

/**
 * Result of nativeListFilesArrowFfi. Combines the Arrow file listing
 * with table metadata and filtering metrics in a single return value,
 * eliminating separate getSchema/getMetadata/getProtocol JNI round-trips.
 */
public class ListFilesResult {
    public final long numRows;           // Number of file entries in Arrow batch (-1 if not initialized)
    public final String schemaJson;      // Table schema as JSON string (StructType-compatible)
    public final String[] partitionColumns; // Partition column names
    public final String protocolJson;    // ProtocolAction as JSON
    public final String metadataConfigJson; // Metadata configuration map as JSON (for schema dedup registry)

    // Filtering metrics (for DESCRIBE DATA SKIPPING STATS / Spark UI)
    public final long totalFilesBeforeFiltering;
    public final long filesAfterPartitionPruning;
    public final long filesAfterDataSkipping;
    public final long filesAfterCooldownFiltering;
    public final long manifestsTotal;
    public final long manifestsPruned;   // Manifests skipped by partition bounds
}

public class TransactionLogReader {
    /**
     * List files in the transaction log with all filtering applied natively,
     * exported via Arrow C Data Interface.
     *
     * This is a single-call replacement for the entire JVM orchestration:
     * getSnapshotInfo + readManifest × N + readPostCheckpointChanges +
     * log replay + partition pruning + data skipping + cooldown filtering +
     * schema restore + getSchema + getMetadata + getProtocol.
     *
     * @param tablePath            Normalized table path (s3://bucket/path)
     * @param config               Storage credentials + cache TTL + checkpoint interval
     * @param partitionFilterJson  Partition filters as JSON (nullable = no partition filtering).
     *                             Uses existing PartitionFilter serde format.
     * @param dataFilterJson       Non-partition filters for min/max data skipping as JSON
     *                             (nullable = no data skipping). Same PartitionFilter format
     *                             but evaluated against min_values/max_values instead of
     *                             partition_values.
     * @param excludeCooldownFiles If true, exclude files in skip/cooldown state
     * @param arrowArrayAddrs      Pre-allocated Arrow C ArrowArray struct addresses
     * @param arrowSchemaAddrs     Pre-allocated Arrow C ArrowSchema struct addresses
     * @return ListFilesResult with row count, table metadata, and filtering metrics
     */
    public static native ListFilesResult nativeListFilesArrowFfi(
        String tablePath,
        java.util.Map<String, String> config,
        String partitionFilterJson,
        String dataFilterJson,
        boolean excludeCooldownFiles,
        long[] arrowArrayAddrs,
        long[] arrowSchemaAddrs
    );
}
```

**Design notes:**
- `partitionColumns` moved from input parameter to output — native reads it from metadata, so JVM doesn't need to pass it in. This eliminates the chicken-and-egg problem where the JVM needed to call `getPartitionColumns()` before `listFiles()`.
- Table metadata (schema, protocol, partition columns, config) is returned alongside the file listing. This eliminates 3 separate JNI round-trips that previously each triggered their own snapshot read.
- Filtering metrics enable `DataSkippingMetrics.recordScan()` and Spark UI metrics without JVM-side counting.
- `metadataConfigJson` provides the schema dedup registry and other table configuration that the JVM still needs (e.g., for `DocMappingMetadata` lookups on executors).

### Implementation Requirements

#### Filter Handling (Partition + Data Skipping)

Both `partitionFilterJson` and `dataFilterJson` use the same `PartitionFilter` serde format. The difference is what they're evaluated against:

| Parameter | Evaluated against | Evaluation mode |
|-----------|------------------|-----------------|
| `partitionFilterJson` | `AddAction.partition_values` | Exact match (existing `evaluate()`) |
| `dataFilterJson` | `AddAction.min_values` / `max_values` | Range overlap (new `can_skip()`) |

**Partition filter** example:
```json
{"op": "eq", "column": "date", "value": "2024-01-15"}
```

**Data filter** example (same format, different semantics):
```json
{"op": "and", "filters": [
  {"op": "gt", "column": "price", "value": "100"},
  {"op": "eq", "column": "category", "value": "electronics"}
]}
```

When evaluated as a data skipping filter against `min_values`/`max_values`:
- `Eq("price", "100")` → skip if `100 < min_price` OR `100 > max_price`
- `Gt("price", "100")` → skip if `max_price <= 100`
- `Lt("price", "100")` → skip if `min_price >= 100`
- `Gte("price", "100")` → skip if `max_price < 100` (with truncation awareness)
- `Lte("price", "100")` → skip if `min_price > 100` (with truncation awareness)
- `In("category", ["a","b"])` → skip if file range `[min,max]` doesn't overlap IN list range `[min(a,b), max(a,b)]`
- `StringStartsWith("name", "foo")` → skip if `"foo" > max_name`
- `And(f1, f2)` → skip if either f1 or f2 says skip (both must match)
- `Or(f1, f2)` → skip if both f1 and f2 say skip (at least one must match)
- `Not(f)` → never skip (conservative — can't prove all values in range fail NOT)

**Truncation awareness:** When `max_value` could be truncated (e.g., `max="aaa"` but actual max might be `"aaaz"`), `Gte` must not skip if the filter value starts with max. The existing JVM code handles this — port the same logic.

**Numeric-aware comparison:** The existing `PartitionFilter.evaluate()` already tries i64 → f64 → string comparison. Reuse the same logic for data skipping.

**Missing operators to add to `PartitionFilter` enum:**
- `IsNull { column }` — matches when value is null/empty
- `IsNotNull { column }` — matches when value is present
- `Neq { column, value }` — not-equal (Java `PartitionFilter` class already has this)
- `StringStartsWith { column, prefix }` — prefix match (for data skipping)
- `StringEndsWith { column, suffix }` — suffix match (conservative: only skip when min==max)
- `StringContains { column, value }` — substring match (conservative: only skip when min==max)

#### New method on PartitionFilter: `can_skip_by_stats()`

```rust
impl PartitionFilter {
    /// Evaluate whether a file can be skipped based on min/max statistics.
    /// Returns true if the file CANNOT match the filter (safe to skip).
    /// Conservative: returns false (don't skip) when uncertain.
    pub fn can_skip_by_stats(
        &self,
        min_values: &HashMap<String, String>,
        max_values: &HashMap<String, String>,
    ) -> bool {
        match self {
            PartitionFilter::Eq { column, value } => {
                match (min_values.get(column), max_values.get(column)) {
                    (Some(min), Some(max)) if !min.is_empty() && !max.is_empty() => {
                        let (cv, cmin, cmax) = numeric_aware_convert(value, min, max);
                        cv < cmin || cv > cmax
                    }
                    _ => false,
                }
            }
            PartitionFilter::Gt { column, value } => {
                // Skip if max <= value (all values in file are too small)
                match max_values.get(column) {
                    Some(max) if !max.is_empty() => {
                        let (cv, _, cmax) = numeric_aware_convert(value, "", max);
                        cmax <= cv
                    }
                    _ => false,
                }
            }
            PartitionFilter::Lt { column, value } => {
                // Skip if min >= value
                match min_values.get(column) {
                    Some(min) if !min.is_empty() => {
                        let (cv, cmin, _) = numeric_aware_convert(value, min, "");
                        cmin >= cv
                    }
                    _ => false,
                }
            }
            PartitionFilter::Gte { column, value } => {
                // Skip if max < value, BUT not if value starts with max (truncation)
                match max_values.get(column) {
                    Some(max) if !max.is_empty() => {
                        let (cv, _, cmax) = numeric_aware_convert(value, "", max);
                        cmax < cv && !value.starts_with(max)
                    }
                    _ => false,
                }
            }
            PartitionFilter::Lte { column, value } => {
                // Skip if min > value, BUT not if value starts with min (truncation)
                match min_values.get(column) {
                    Some(min) if !min.is_empty() => {
                        let (cv, cmin, _) = numeric_aware_convert(value, min, "");
                        cmin > cv && !value.starts_with(min)
                    }
                    _ => false,
                }
            }
            PartitionFilter::In { column, values } if !values.is_empty() => {
                // Skip if file's [min,max] range doesn't overlap IN list's [min,max]
                match (min_values.get(column), max_values.get(column)) {
                    (Some(file_min), Some(file_max)) if !file_min.is_empty() && !file_max.is_empty() => {
                        let in_min = values.iter().min().unwrap();
                        let in_max = values.iter().max().unwrap();
                        let (_, cf_min, cf_max) = numeric_aware_convert(in_min, file_min, file_max);
                        let (ci_min, _, _) = numeric_aware_convert(in_min, in_min, in_min);
                        let (ci_max, _, _) = numeric_aware_convert(in_max, in_max, in_max);
                        cf_max < ci_min || cf_min > ci_max
                    }
                    _ => false,
                }
            }
            PartitionFilter::StringStartsWith { column, prefix } => {
                match max_values.get(column) {
                    Some(max) if !max.is_empty() => prefix.as_str() > max.as_str(),
                    _ => false,
                }
            }
            PartitionFilter::StringEndsWith { column, suffix } |
            PartitionFilter::StringContains { column, value: suffix } => {
                // Conservative: only skip when min==max and doesn't match
                match (min_values.get(column), max_values.get(column)) {
                    (Some(min), Some(max)) if min == max => {
                        !min.ends_with(suffix) // or !min.contains(suffix)
                    }
                    _ => false,
                }
            }
            PartitionFilter::And { filters } => {
                // Skip if ANY sub-filter says skip (all must match for AND)
                filters.iter().any(|f| f.can_skip_by_stats(min_values, max_values))
            }
            PartitionFilter::Or { filters } => {
                // Skip only if ALL sub-filters say skip (at least one must match for OR)
                filters.iter().all(|f| f.can_skip_by_stats(min_values, max_values))
            }
            PartitionFilter::Not { .. } => {
                // Cannot prove NOT via range stats — always conservative
                false
            }
            _ => false,
        }
    }
}
```

#### Execution Flow

```rust
pub struct ListFilesResult {
    pub num_rows: i64,
    pub schema_json: String,
    pub partition_columns: Vec<String>,
    pub protocol_json: String,
    pub metadata_config_json: Option<String>,
    // Filtering metrics
    pub total_files_before_filtering: i64,
    pub files_after_partition_pruning: i64,
    pub files_after_data_skipping: i64,
    pub files_after_cooldown_filtering: i64,
    pub manifests_total: i64,
    pub manifests_pruned: i64,
}

pub fn list_files_arrow_ffi(
    table_path: &str,
    config: &HashMap<String, String>,
    partition_filter_json: Option<&str>,
    data_filter_json: Option<&str>,
    exclude_cooldown_files: bool,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<ListFilesResult> {
    // 1. Get snapshot (uses existing cache)
    let snapshot = get_snapshot_info(table_path, config)?;

    // 2. Extract table metadata from snapshot (returned to JVM — no separate calls needed)
    let metadata = parse_metadata(&snapshot.metadata_json)?;
    let protocol = parse_protocol(&snapshot.protocol_json)?;
    let partition_columns = metadata.partition_columns.clone();

    // 3. Parse filters (both use same PartitionFilter serde format)
    let partition_filters: Option<PartitionFilter> = partition_filter_json
        .map(|json| serde_json::from_str(json))
        .transpose()?;
    let data_filters: Option<PartitionFilter> = data_filter_json
        .map(|json| serde_json::from_str(json))
        .transpose()?;

    // 4. Read manifests with manifest-level pruning (partition bounds)
    let manifest_infos = &snapshot.manifest_infos;
    let manifests_total = manifest_infos.len() as i64;
    let pruned_manifests = match &partition_filters {
        Some(f) => prune_manifests(manifest_infos, &[f.clone()]),
        None => manifest_infos.to_vec(),
    };
    let manifests_pruned = manifests_total - pruned_manifests.len() as i64;

    // 5. Read matching manifests → Vec<FileEntry>
    let mut entries: Vec<FileEntry> = Vec::new();
    for manifest in &pruned_manifests {
        let manifest_entries = read_manifest(table_path, config, &snapshot.state_dir, &manifest.path)?;
        entries.extend(manifest_entries);
    }

    // 6. Apply post-checkpoint changes (adds/removes)
    let changes = read_post_checkpoint_changes(table_path, config, &snapshot.post_checkpoint_paths)?;
    apply_changes(&mut entries, &changes);

    let total_files = entries.len() as i64;

    // 7. File-level partition pruning
    if let Some(ref f) = partition_filters {
        entries.retain(|entry| f.evaluate(&entry.add_action.partition_values));
    }
    let after_partition = entries.len() as i64;

    // 8. Min/max data skipping
    if let Some(ref f) = data_filters {
        entries.retain(|entry| {
            match (&entry.add_action.min_values, &entry.add_action.max_values) {
                (Some(min_vals), Some(max_vals)) => !f.can_skip_by_stats(min_vals, max_vals),
                _ => true, // No stats → conservative, keep the file
            }
        });
    }
    let after_data_skip = entries.len() as i64;

    // 9. Exclude files in cooldown/skip state
    if exclude_cooldown_files {
        let cooldown_ms = config.get("spark.indextables.skippedFiles.cooldownDuration")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(24) * 3600 * 1000;
        let skipped = list_skip_actions(table_path, config, cooldown_ms).await?;
        let cooldown_paths: HashSet<&str> = skipped.iter()
            .filter(|s| s.retry_after.map_or(false, |ra| ra > now_millis()))
            .map(|s| s.path.as_str())
            .collect();
        if !cooldown_paths.is_empty() {
            entries.retain(|e| !cooldown_paths.contains(e.add_action.path.as_str()));
        }
    }
    let after_cooldown = entries.len() as i64;

    // 10. Restore deduplicated schemas
    restore_schemas(&mut entries, &snapshot);

    // 11. Export via Arrow FFI with dynamic partition columns
    let num_rows = unsafe {
        export_file_entries_ffi_v2(&entries, &partition_columns, array_addrs, schema_addrs)?
    };

    Ok(ListFilesResult {
        num_rows: num_rows as i64,
        schema_json: metadata.schema_string.clone(),
        partition_columns,
        protocol_json: snapshot.protocol_json.clone(),
        metadata_config_json: serialize_config(&metadata.configuration),
        total_files_before_filtering: total_files,
        files_after_partition_pruning: after_partition,
        files_after_data_skipping: after_data_skip,
        files_after_cooldown_filtering: after_cooldown,
        manifests_total,
        manifests_pruned,
    })
}
```

#### Schema Deduplication Restore

The `restoreSchemas` step must resolve `doc_mapping_ref` → `doc_mapping_json` using the schema registry stored in metadata configuration. This is currently done in JVM (`NativeTransactionLog.restoreSchemas()`). The native layer already has access to metadata configuration via `snapshot.metadata_json` — the schema registry entries are stored as `docMappingSchema.<hash>` keys.

Implementation: iterate entries, for any with `doc_mapping_ref.is_some() && doc_mapping_json.is_none()`, look up the ref in the metadata configuration map and populate `doc_mapping_json`.

#### Arrow Schema (v2 — no JSON-in-Arrow)

The existing v1 schema encodes `partition_values`, `min_values`, `max_values`, `split_tags`, and `companion_source_files` as JSON-encoded Utf8 strings. This forces the JVM to JSON-parse every row to access individual values — defeating the purpose of Arrow.

The v2 schema eliminates all JSON-in-Arrow columns:

**Static columns (always present):**

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| path | Utf8 | No | |
| size | Int64 | No | |
| modification_time | Int64 | No | |
| data_change | Boolean | No | |
| num_records | Int64 | Yes | |
| footer_start_offset | Int64 | Yes | |
| footer_end_offset | Int64 | Yes | |
| has_footer_offsets | Boolean | Yes | |
| delete_opstamp | Int64 | Yes | |
| num_merge_ops | Int32 | Yes | |
| doc_mapping_json | Utf8 | Yes | Opaque string, not parsed by JVM — passed through to executor |
| doc_mapping_ref | Utf8 | Yes | |
| uncompressed_size_bytes | Int64 | Yes | |
| time_range_start | Int64 | Yes | |
| time_range_end | Int64 | Yes | |
| companion_delta_version | Int64 | Yes | |
| companion_fast_field_mode | Utf8 | Yes | |
| split_tags | List\<Utf8\> | Yes | **Changed: native Arrow list, not JSON string** |
| companion_source_files | List\<Utf8\> | Yes | **Changed: native Arrow list, not JSON string** |

**Dynamic partition columns (appended based on `partitionColumns` parameter):**

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `partition:{name}` | Utf8 | Yes | One column per partition column. Prefixed to avoid name collisions. |

Example: for `partitionColumns = ["date", "region"]`, the schema includes `partition:date` (Utf8) and `partition:region` (Utf8) as the last two columns.

**Removed columns (no longer needed in Arrow output):**

| Removed | Why |
|---------|-----|
| `partition_values` (JSON map) | Replaced by dynamic `partition:{name}` columns — direct Arrow access, no JSON parsing |
| `min_values` (JSON map) | Data skipping is now done natively before export. JVM doesn't need these. |
| `max_values` (JSON map) | Same as min_values. |
| `stats` (JSON) | Never used by JVM after data skipping moves to native. Statistics are consumed during native filtering. |
| `added_at_version` | Only used by purge cursor. Not needed for read path. |
| `added_at_timestamp` | Same. |

**Why this eliminates all JSON round-trips:**

| Before (v1) | After (v2) |
|-------------|------------|
| `partition_values` → JSON map → JVM parses per row | `partition:date`, `partition:region` → direct Utf8 column read |
| `min_values` → JSON map → JVM parses for data skipping | Data skipping done in native. Column not exported. |
| `max_values` → JSON map → JVM parses for data skipping | Same. |
| `stats` → JSON → JVM parses for statistics | Statistics consumed natively. Column not exported. |
| `split_tags` → JSON array → JVM parses | `List<Utf8>` Arrow type → direct access |
| `companion_source_files` → JSON array → JVM parses | `List<Utf8>` Arrow type → direct access |

**Column count:** 19 static + N dynamic partition columns (typically 0-3).

The JVM side knows the partition column names (from metadata) and can compute the column indices: `19 + partitionColumns.indexOf(name)`.

#### Fallback for min/max on JVM side

With Feature Request 4, `SplitReaderContext`'s redundant filter elimination moves to the native split search engine, removing the last per-split consumer of min/max on the JVM read path.

The remaining JVM consumer is `IndexTables4SparkStatistics` (Spark cost-based optimizer), which aggregates min/max across all files for table-level statistics. For this case, add an optional `includeStats` boolean parameter. When true, append `min_values` and `max_values` as JSON Utf8 columns at the end of the schema. Default false — the hot read path never needs them.

#### Error Handling

- Table not initialized → return -1 (not an error — matches current behavior)
- Invalid partition filter JSON → return error with descriptive message
- Storage errors → propagate as Java RuntimeException via `to_java_exception()`

#### Caching

- Snapshot info: use existing `TxLogCache` (LRU + TTL)
- File list results: optionally cache in the existing file_list cache, keyed by `(version, filter_hash)`
- Cache invalidation: existing write-path invalidation continues to work

### Testing

- Unit tests: verify partition pruning produces same results as JVM `PartitionPruning.scala`
- Arrow FFI tests: extend existing `arrow_ffi_tests.rs` with filtered listing tests
- Integration: existing 362 Spark test classes validate end-to-end correctness

### Notes

- The existing `export_file_entries_ffi()` function in `arrow_ffi.rs` already handles the full 25-column schema. This feature request just adds a new entry point that combines snapshot reading + pruning + export.
- For tables with very large file counts (100K+), consider supporting a cursor/streaming pattern similar to the retained files cursor. For v1, a single batch is acceptable — most tables have <50K active files.

---

## Feature Request 2: `nativeWriteVersionArrowFfi` — Unified Write Path

### Summary

Single JNI entry point that accepts an Arrow RecordBatch containing **all** action types (adds, removes, skips, protocol, metadata) via a unified schema with an `action_type` discriminator column. This eliminates JSON serialization entirely from the write path — no mixed formats, one code path, one schema.

### Design Rationale

A previous iteration proposed Arrow for AddActions but kept JSON for RemoveActions. This was rejected because:
- **Inconsistency**: Two serialization formats means two code paths to maintain on both JVM and Rust sides
- **Can't delete ActionJsonSerializer**: The whole point is to eliminate JSON — keeping it for removes defeats the purpose
- **Unnecessary complexity**: The JNI signature for mixed format (`arrowAddr + jsonString`) is ugly
- **Arrow handles sparse nulls efficiently**: A remove row with 20 null AddAction columns costs ~20 bits (null bitmap), not 20 empty fields

### JNI Signature

```java
package io.indextables.jni.txlog;

public class TransactionLogWriter {
    /**
     * Write a new version to the transaction log using Arrow FFI.
     * The batch contains all action types distinguished by the action_type column.
     * Replaces nativeWriteVersion (JSON-lines), nativeAddFiles (JSON array),
     * and nativeWriteVersionOnce (JSON-lines, no retry).
     *
     * @param tablePath       Normalized table path
     * @param config          Storage credentials + cache config
     * @param arrowArrayAddr  Arrow C ArrowArray struct address
     * @param arrowSchemaAddr Arrow C ArrowSchema struct address
     * @param numRows         Number of rows in the batch
     * @param retry           If true, retry on conflict (like writeVersion). If false, single attempt (like writeVersionOnce).
     * @return WriteResult with version, retries, conflicts. Version = -1 if conflict and retry=false.
     */
    public static native WriteResult nativeWriteVersionArrowFfi(
        String tablePath,
        java.util.Map<String, String> config,
        long arrowArrayAddr,
        long arrowSchemaAddr,
        int numRows,
        boolean retry
    );
}
```

### Unified Action Arrow Schema

A single schema covers all action types. The `action_type` column discriminates which fields are meaningful per row. Non-applicable columns are null.

| Column | Type | Used by | Notes |
|--------|------|---------|-------|
| **action_type** | Utf8 | all | **Required.** One of: `"add"`, `"remove"`, `"skip"`, `"protocol"`, `"metadata"` |
| **path** | Utf8 | add, remove, skip | File path |
| **size** | Int64 | add, remove | File size in bytes |
| **modification_time** | Int64 | add | Epoch millis |
| **data_change** | Boolean | add, remove | Whether this is a data change |
| **partition_values** | Utf8 (JSON) | add, remove, skip | JSON-encoded HashMap |
| **deletion_timestamp** | Int64 | remove | Epoch millis when file was removed |
| **extended_file_metadata** | Boolean | remove | |
| **skip_timestamp** | Int64 | skip | Epoch millis |
| **reason** | Utf8 | skip | Skip reason string |
| **operation** | Utf8 | skip | "merge", "read", etc. |
| **retry_after** | Int64 | skip | Epoch millis when retry is allowed |
| **skip_count** | Int32 | skip | Number of times skipped |
| **min_reader_version** | Int32 | protocol | |
| **min_writer_version** | Int32 | protocol | |
| **reader_features** | Utf8 (JSON array) | protocol | |
| **writer_features** | Utf8 (JSON array) | protocol | |
| **metadata_id** | Utf8 | metadata | Table ID |
| **metadata_name** | Utf8 | metadata | |
| **metadata_description** | Utf8 | metadata | |
| **schema_string** | Utf8 | metadata | JSON schema |
| **partition_columns** | Utf8 (JSON array) | metadata | |
| **configuration** | Utf8 (JSON) | metadata | HashMap as JSON |
| **created_time** | Int64 | metadata | |
| **format_provider** | Utf8 | metadata | e.g. "indextables" |
| **format_options** | Utf8 (JSON) | metadata | |
| **num_records** | Int64 | add | |
| **stats** | Utf8 | add | Statistics JSON |
| **min_values** | Utf8 (JSON) | add | |
| **max_values** | Utf8 (JSON) | add | |
| **footer_start_offset** | Int64 | add | |
| **footer_end_offset** | Int64 | add | |
| **has_footer_offsets** | Boolean | add | |
| **split_tags** | Utf8 (JSON array) | add | |
| **delete_opstamp** | Int64 | add | |
| **num_merge_ops** | Int32 | add | |
| **doc_mapping_json** | Utf8 | add | |
| **doc_mapping_ref** | Utf8 | add | |
| **uncompressed_size_bytes** | Int64 | add | |
| **time_range_start** | Int64 | add | |
| **time_range_end** | Int64 | add | |
| **companion_source_files** | Utf8 (JSON array) | add | |
| **companion_delta_version** | Int64 | add | |
| **companion_fast_field_mode** | Utf8 | add | |

**Null overhead for non-add rows:** A remove row uses ~6 columns (action_type, path, deletion_timestamp, data_change, partition_values, size) and has ~36 null columns. At 1 bit per null, that's 4.5 bytes per remove row — negligible compared to the JSON alternative.

### Implementation Requirements

#### Arrow Batch → Vec<Action> Conversion

```rust
fn arrow_batch_to_actions(batch: &RecordBatch) -> Result<Vec<Action>> {
    let num_rows = batch.num_rows();
    let mut actions = Vec::with_capacity(num_rows);

    let action_type_col = batch.column_by_name("action_type")
        .context("Missing required action_type column")?
        .as_string::<i32>();
    let path_col = batch.column_by_name("path").map(|c| c.as_string::<i32>());
    let size_col = batch.column_by_name("size").map(|c| c.as_primitive::<Int64Type>());
    // ... extract all column references upfront

    for i in 0..num_rows {
        let action_type = action_type_col.value(i);
        let action = match action_type {
            "add" => Action::Add(extract_add_action(i, &batch)?),
            "remove" => Action::Remove(extract_remove_action(i, &batch)?),
            "skip" => Action::Skip(extract_skip_action(i, &batch)?),
            "protocol" => Action::Protocol(extract_protocol_action(i, &batch)?),
            "metadata" => Action::Metadata(extract_metadata_action(i, &batch)?),
            other => bail!("Unknown action_type: {}", other),
        };
        actions.push(action);
    }

    Ok(actions)
}
```

#### Arrow Import

Use the existing pattern from `parquet_companion/arrow_ffi_import.rs`:
```rust
let batch = import_arrow_batch(array_ptr as *mut FFI_ArrowArray, schema_ptr as *mut FFI_ArrowSchema)?;
batch.align_buffers(); // Handle Java memory alignment
let actions = arrow_batch_to_actions(&batch)?;
```

#### Schema Validation

- `action_type` column is **required** — error if missing
- All other columns: if missing, treat as null for every row
- Extra/unknown columns: silently ignored (forward compatibility)
- Type mismatches: error with descriptive message

#### Retry Logic

The `retry` parameter maps directly to existing behavior:
- `retry=true` → calls `distributed::write_version()` with exponential backoff (same as `nativeWriteVersion`)
- `retry=false` → calls `distributed::write_version_once()` (same as `nativeWriteVersionOnce`), returns version=-1 on conflict

#### Auto-Checkpoint

Trigger auto-checkpoint after successful write, same as existing `nativeAddFiles()` and `nativeWriteVersion()`.

### Testing

- **Round-trip test**: build Arrow batch with all 5 action types → import → verify each action parsed correctly
- **Null handling test**: verify sparse null columns don't cause panics
- **Add-only batch test**: batch with only "add" rows (most common case) → verify same result as `nativeAddFiles()`
- **Mixed batch test**: adds + removes in same batch → verify same result as `nativeWriteVersion()` with JSON-lines
- **Concurrency test**: verify retry logic works correctly with Arrow input
- **Schema evolution test**: verify missing/extra columns handled gracefully
- **Write + read round-trip**: write via Arrow → read via Arrow (Feature Request 1) → verify exact match

---

## Feature Request 3: `nativeReadNextRetainedFilesBatchArrowFfi` — Purge Cursor

### Summary

Modify the existing retained files cursor to export via Arrow FFI instead of TANT binary format. This aligns the purge path with the read path (Feature Request 1) and provides access to the full 25-column schema instead of the current 3-column subset.

### JNI Signature

```java
package io.indextables.jni.txlog;

public class TransactionLogReader {
    /**
     * Fetch the next batch of retained files from cursor, exported via Arrow FFI.
     *
     * Replaces nativeReadNextRetainedFilesBatch() which uses TANT binary format
     * and only exports 3 columns (path, size, version).
     *
     * @param cursorHandle     Opaque cursor handle from nativeOpenRetainedFilesCursor()
     * @param batchSize        Maximum rows per batch
     * @param arrowArrayAddrs  Pre-allocated Arrow C ArrowArray struct addresses
     * @param arrowSchemaAddrs Pre-allocated Arrow C ArrowSchema struct addresses
     * @return Number of rows in batch, 0 if cursor exhausted, -1 on error
     */
    public static native int nativeReadNextRetainedFilesBatchArrowFfi(
        long cursorHandle,
        int batchSize,
        long[] arrowArrayAddrs,
        long[] arrowSchemaAddrs
    );
}
```

### Implementation Requirements

#### Reuse Existing Infrastructure

- Cursor open/close: no changes needed — `nativeOpenRetainedFilesCursor()` and `nativeCloseRetainedFilesCursor()` stay as-is
- The cursor already iterates over `FileEntry` objects internally
- Replace the TANT serialization with `export_file_entries_ffi()` (existing function)

#### Changes to `purge.rs`

The `read_next_retained_files_batch()` function currently:
1. Takes N entries from the cursor's internal `VecDeque<FileEntry>`
2. Serializes to TANT binary with 3 columns (path, size, version)

Change to:
1. Take N entries from cursor (same as before)
2. Call `export_file_entries_ffi(&entries, array_addrs, schema_addrs)` (existing function)
3. Return row count

This is a ~20-line change in `purge.rs`.

#### Full Schema Access

The current TANT export only includes `path`, `size`, and `version`. Arrow FFI export provides all 25 columns, which enables the JVM purge executor to:
- Access `partition_values` for partition-aware purge decisions
- Access `num_records` for size-based retention policies
- Access `modification_time` for time-based filtering
- Access `companion_source_files` for companion-aware purge

### Backward Compatibility

Keep the existing `nativeReadNextRetainedFilesBatch()` function for backward compatibility. The new Arrow FFI variant is an addition, not a replacement. Deprecate the old function once the JVM side is migrated.

### Testing

- Extend existing `purge_tests.rs` with Arrow FFI export validation
- Verify cursor exhaustion behavior (returns 0 rows, not error)
- Verify batch boundary handling (partial last batch)

---

## Feature Request 4: Native Redundant Range Filter Elimination in Split Search

### Summary

Move the executor-side redundant filter optimization from `SplitReaderContext` (JVM) into the native split search engine. Currently, the JVM uses each split's `min_values`/`max_values` to detect range filters that are guaranteed to match all documents in that split, and removes them from the Tantivy query before execution. This optimization should happen inside the native search engine.

### Current JVM Implementation

In `SplitReaderContext.scala:192-194`:
```scala
val optimizedFilters = if (addAction.minValues.nonEmpty && addAction.maxValues.nonEmpty) {
  filters.filterNot(f =>
    isRangeFilterRedundantByStats(f, addAction.minValues.get, addAction.maxValues.get, fullTableSchema))
} else filters
```

For example: if a split has `min(price) = 150, max(price) = 300` and the query has `WHERE price > 100`, the `price > 100` filter is always true for every document in this split. Tantivy doesn't need to evaluate it per-document — removing it from the query improves search throughput.

### Why This Must Move to Native

With Feature Request 1, `min_values`/`max_values` are no longer exported in the Arrow batch (they're consumed during native data skipping and not passed to the JVM). Without min/max on the JVM side, `SplitReaderContext` can no longer perform this optimization. The native search engine must do it instead.

### Implementation Approach

The native split search engine (`SplitSearchEngine` / `SplitSearcher` in tantivy4java) already opens the split and has access to its footer metadata. The optimization should happen **after** the query is parsed but **before** execution:

```rust
impl SplitSearcher {
    /// Optimize a query by removing clauses that are guaranteed to match
    /// all documents in this split based on column statistics.
    fn optimize_query_with_stats(&self, query: SplitQuery) -> SplitQuery {
        match query {
            SplitQuery::Boolean(clauses) => {
                let optimized: Vec<_> = clauses.into_iter()
                    .filter(|clause| !self.is_clause_always_true(clause))
                    .collect();
                if optimized.is_empty() {
                    SplitQuery::MatchAll
                } else {
                    SplitQuery::Boolean(optimized)
                }
            }
            other => other,
        }
    }

    fn is_clause_always_true(&self, clause: &BooleanClause) -> bool {
        // Only optimize MUST clauses (AND semantics)
        if clause.occur != Occur::Must { return false; }

        match &clause.query {
            SplitQuery::Range { field, lower, upper, .. } => {
                // Check if split's [min, max] for this field is entirely within query range
                let stats = self.get_field_stats(field);
                match (stats, lower, upper) {
                    (Some(FieldStats { min, max }), Some(lower_bound), None) => {
                        // Query: field > lower_bound. Redundant if split_min > lower_bound
                        min > lower_bound
                    }
                    (Some(FieldStats { min, max }), None, Some(upper_bound)) => {
                        // Query: field < upper_bound. Redundant if split_max < upper_bound
                        max < upper_bound
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }
}
```

### Source of Statistics

Two options for where the native search engine gets min/max:

**Option A: Split footer metadata (preferred)**
If the split file format (QuickwitSplit) stores per-field min/max in its footer, the native search engine already has access. This is the cleanest approach — no external data needed.

**Option B: Pass min/max via JNI search call**
If the split footer doesn't have min/max (they're only in the transaction log), add an optional `stats_json` parameter to the native search JNI function. The JVM would need to carry min/max through to executors in this case, which means re-adding them to the Arrow output. This is less desirable.

Investigate which option is feasible given the current split format. Option A should be preferred if possible.

### JVM Code Removable

| Code | Lines | Location |
|------|-------|----------|
| `isRangeFilterRedundantByStats()` | ~50 | `SplitReaderContext.scala` |
| Related min/max access patterns | ~10 | `SplitReaderContext.scala:192-194` |

### Testing

- Verify query optimization produces same search results (must be semantically equivalent)
- Benchmark: measure query throughput with/without optimization on splits where range filters are redundant
- Edge cases: truncated statistics, null statistics, mixed-type comparisons

---

## Implementation Notes (apply to all four)

### Arrow FFI Memory Protocol

The JVM side pre-allocates Arrow C structs and passes their memory addresses to native. Native fills the structs and the JVM imports them. This is the established pattern used by the split read path (`ArrowFfiBridge.scala`).

**Critical:** Native must call `std::ptr::replace()` (not `std::ptr::write()`) to safely transfer ownership, as shown in the existing `parquet_companion/arrow_ffi_import.rs`.

### Column-Level Null Handling

For nullable columns, use Arrow's null bitmap. The existing `arrow_ffi.rs` already handles this correctly with `builder.append_null()` / `builder.append_value()` patterns.

### HashMap Columns (partition_values, min_values, max_values)

These are serialized as JSON strings in the Arrow Utf8 column. The JVM side must parse them if needed. This matches the current TANT behavior and avoids the complexity of Arrow Map types.

### Thread Safety

All functions must use `block_on_operation()` on the shared async runtime, matching the existing JNI pattern. Cache access is already thread-safe via `parking_lot::RwLock`.

### Version Compatibility

Add a version field to the Arrow schema metadata so the JVM side can detect schema changes across tantivy4java upgrades. Use Arrow schema metadata: `{"version": "1"}`.
