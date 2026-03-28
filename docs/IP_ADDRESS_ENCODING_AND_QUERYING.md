# IP Address Encoding and Querying: A Cross-Layer Technical Reference

This document provides a comprehensive treatment of how IP address values flow through the full IndexTables4Spark stack — from Spark SQL down through tantivy4java's JNI bridge, through Quickwit's split format, and into Tantivy's core index structures. It covers both **standard splits** (written directly by IndexTables4Spark) and **companion splits** (built from external Delta/Parquet/Iceberg tables).

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Write Path: End-to-End Encoding](#2-write-path-end-to-end-encoding)
3. [Read Path: End-to-End Querying](#3-read-path-end-to-end-querying)
4. [Layer 1 — Tantivy Core](#4-layer-1--tantivy-core)
5. [Layer 2 — Quickwit Split Format](#5-layer-2--quickwit-split-format)
6. [Layer 3 — tantivy4java JNI Bridge](#6-layer-3--tantivy4java-jni-bridge)
7. [Layer 4 — IndexTables4Spark](#7-layer-4--indextables4spark)
8. [Standard Splits vs Companion Splits](#8-standard-splits-vs-companion-splits)
9. [IPv4/IPv6 Normalization](#9-ipv4ipv6-normalization)
10. [Query Semantics and Operators](#10-query-semantics-and-operators)
11. [Known Issues and Workarounds](#11-known-issues-and-workarounds)
12. [Appendix: Configuration Reference](#12-appendix-configuration-reference)

---

## 1. Architecture Overview

### System Layer Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                     Spark SQL / DataFrame API                    │
│  df.filter($"ip" === "192.168.1.1")                             │
│  spark.sql("SELECT * FROM t WHERE ip indexquery '[A TO B]'")    │
└──────────────────────────┬───────────────────────────────────────┘
                           │  Spark Filter / IndexQuery expression
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│              IndexTables4Spark  (Scala / JVM)                    │
│                                                                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────┐  │
│  │ FiltersToQuery   │  │ IndexTables      │  │ SchemaMapping  │  │
│  │ Converter        │  │ DirectInterface  │  │ (read-back)    │  │
│  │ (filter→query)   │  │ (write path)     │  │                │  │
│  └────────┬─────────┘  └────────┬─────────┘  └───────┬────────┘  │
│           │ SplitTermQuery       │ addIpAddr()        │ strip     │
│           │ SplitRangeQuery      │                    │ ::ffff:   │
└───────────┼──────────────────────┼────────────────────┼──────────┘
            │                      │                    │
            ▼                      ▼                    ▲
┌──────────────────────────────────────────────────────────────────┐
│              tantivy4java  (Java API + Rust JNI)                 │
│                                                                  │
│  Java side:                                                      │
│    SchemaBuilder.addIpAddrField(name, stored, indexed, fast)     │
│    Document.addIpAddr(name, "192.168.1.1")                       │
│    BatchDocument.addIpAddr(name, "192.168.1.1")                  │
│                                                                  │
│  Rust/JNI side:                                                  │
│    IpAddr::from_str("192.168.1.1")                               │
│    ipv4.to_ipv6_mapped() → ::ffff:192.168.1.1                   │
│    batch_parsing.rs → TANT binary protocol                       │
└──────────────────────────┬───────────────────────────────────────┘
                           │  Ipv6Addr (128-bit)
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│              Quickwit  (Split Format Layer)                       │
│                                                                  │
│  QuickwitIpAddrOptions: stored, indexed, fast                    │
│  Field mapping: "ip" → IpAddr field type                         │
│  Normalization: all IPs → Ipv6Addr via IntoIpv6Addr trait        │
│  Output: IPv4-mapped addrs converted back to canonical IPv4      │
└──────────────────────────┬───────────────────────────────────────┘
                           │  Ipv6Addr → u128 big-endian
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│              Tantivy  (Core Index Engine)                         │
│                                                                  │
│  Schema: FieldType::IpAddr(IpAddrOptions)                        │
│  Type code: b'p'                                                 │
│  Term encoding: [4B field_id][1B type][16B u128 big-endian]      │
│  Columnar/fast: ColumnType::IpAddr                               │
│  Range queries: u128 arithmetic on Ipv6Addr                      │
│  Postings: standard inverted index on 16-byte terms              │
└──────────────────────────────────────────────────────────────────┘
```

### Key Principle: Everything is IPv6 Internally

Across every layer below IndexTables4Spark, **all IP addresses — whether IPv4 or IPv6 — are stored as 128-bit IPv6 addresses**. IPv4 addresses are mapped to their IPv4-mapped IPv6 representation (`::ffff:x.x.x.x` or equivalently the upper 80 bits zero, next 16 bits all ones, final 32 bits the IPv4 address). This unification simplifies comparison, ordering, and range query logic throughout the stack.

---

## 2. Write Path: End-to-End Encoding

This section traces the journey of a single IP address value, say `"192.168.1.1"`, from a Spark DataFrame row all the way into the on-disk index.

### Workflow: Standard Split Write

```
User DataFrame Row
  ┌────────────────────────────────────────┐
  │  Row("192.168.1.1", "web-server", 80)  │
  │         ↑ ip_addr field (StringType)   │
  └────────────────────┬───────────────────┘
                       │
         ① Spark executor calls IndexTablesDirectInterface
                       │
                       ▼
  ┌──────────────────────────────────────────────────────┐
  │  IndexTablesDirectInterface.addFieldGeneric()         │
  │                                                      │
  │  Detects field type "ip" from typemap config          │
  │  Extracts UTF8String → Java String "192.168.1.1"     │
  │  Calls adder.addIpAddr("ip_addr", "192.168.1.1")     │
  └──────────────────────┬───────────────────────────────┘
                         │
         ② JNI boundary: String passed to Rust
                         │
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │  tantivy4java Rust native code                        │
  │                                                      │
  │  Parses "192.168.1.1" → IpAddr::V4(192.168.1.1)     │
  │  Converts → Ipv4Addr.to_ipv6_mapped()                │
  │  Result: Ipv6Addr(::ffff:192.168.1.1)                │
  │  Passes to Quickwit split builder                     │
  └──────────────────────┬───────────────────────────────┘
                         │
         ③ Quickwit split builder indexes the value
                         │
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │  Tantivy segment writer                               │
  │                                                      │
  │  Creates Term:                                        │
  │    [field_id:4B][type:b'p'][ipv6:16B big-endian]     │
  │                                                      │
  │  Posts to inverted index (postings writer)            │
  │  Records in columnar writer (fast field)              │
  │  Stores in document store (if stored=true)            │
  └──────────────────────┬───────────────────────────────┘
                         │
         ④ Segment committed → .split file on disk
                         │
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │  On-disk split file (QuickwitSplit format)             │
  │                                                      │
  │  Inverted index: term → [doc_id, doc_id, ...]        │
  │  Fast field column: doc_id → u128 value              │
  │  Doc store: doc_id → {field: "::ffff:192.168.1.1"}   │
  └──────────────────────────────────────────────────────┘
```

### Workflow: Companion Split Write

```
External table (Delta / Parquet / Iceberg)
  ┌──────────────────────────────────────────┐
  │  Parquet file with ip_addr column        │
  │  (StringType column containing IPs)      │
  └────────────────────┬─────────────────────┘
                       │
         ① BUILD INDEXTABLES COMPANION command
            with INDEXING MODES ('ip_addr':'ipaddress')
                       │
                       ▼
  ┌──────────────────────────────────────────────────────┐
  │  SyncTaskExecutor                                     │
  │                                                      │
  │  Detects mode "ipaddress" or "ip" from INDEXING MODES│
  │  Calls companionConfig.withIpAddressFields("ip_addr")│
  │  Passes to QuickwitSplit.createFromParquet()          │
  └──────────────────────┬───────────────────────────────┘
                         │
         ② Same pipeline as standard: JNI → Tantivy
            (encoding is identical from this point on)
                         │
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │  Companion .split file                                │
  │  (same QuickwitSplit format, same encoding)           │
  │                                                      │
  │  Stored alongside the external table's data files    │
  │  Referenced by companion transaction log             │
  └──────────────────────────────────────────────────────┘
```

The critical difference: companion splits are built from Parquet rows (not Spark InternalRow), but the tantivy4java calls and encoding pipeline are identical. The `withIpAddressFields()` configuration tells the Parquet-to-split converter which string columns should be treated as IP address fields rather than plain text.

---

## 3. Read Path: End-to-End Querying

### Filter Pushdown Flow

```
Spark SQL query:
  df.filter($"ip_addr" >= "10.0.0.0" && $"ip_addr" <= "10.0.0.255")
                       │
         ① Spark pushes filters to DataSource
                       │
                       ▼
  ┌──────────────────────────────────────────────────────┐
  │  FiltersToQueryConverter                              │
  │                                                      │
  │  Recognizes FieldType.IP_ADDR as numeric-like         │
  │  Converts each bound to SplitRangeQuery:              │
  │    field="ip_addr", type="ip"                         │
  │    lower=inclusive("10.0.0.0")                         │
  │    upper=inclusive("10.0.0.255")                       │
  │  Wraps in BooleanQuery (AND)                          │
  └──────────────────────┬───────────────────────────────┘
                         │
         ② Query sent to tantivy4java via JNI
                         │
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │  tantivy4java: parses IP strings, builds Tantivy     │
  │  RangeQuery with Ipv6Addr bounds                     │
  └──────────────────────┬───────────────────────────────┘
                         │
         ③ Tantivy executes range query on fast field
                         │
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │  Tantivy range_query_fastfield.rs                     │
  │                                                      │
  │  Loads Column<Ipv6Addr> from fast field               │
  │  Converts bounds to inclusive u128 range:              │
  │    Included(10.0.0.0) → ::ffff:10.0.0.0 as u128     │
  │    Included(10.0.0.255) → ::ffff:10.0.0.255 as u128 │
  │  Scans column, collects matching doc_ids              │
  │  Returns RangeDocSet                                  │
  └──────────────────────┬───────────────────────────────┘
                         │
         ④ Matching documents retrieved from doc store
                         │
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │  TANT binary protocol (batch_parsing.rs)              │
  │                                                      │
  │  Reads stored Ipv6Addr from doc store                 │
  │  Converts to string: "::ffff:10.0.0.42"              │
  │  Serializes into TANT binary response                 │
  └──────────────────────┬───────────────────────────────┘
                         │
         ⑤ Response crosses JNI boundary to Java/Scala
                         │
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │  SchemaMapping.scala                                  │
  │                                                      │
  │  Receives "::ffff:10.0.0.42"                          │
  │  Detects IPv4-mapped prefix "::ffff:"                 │
  │  Strips prefix → "10.0.0.42"                          │
  │  Returns UTF8String to Spark                          │
  └──────────────────────────────────────────────────────┘
```

### IndexQuery Flow

IndexQuery bypasses Spark's filter pushdown and sends queries in native Tantivy syntax directly:

```
spark.sql("SELECT * FROM t WHERE ip_addr indexquery '[10.0.0.0 TO 10.0.0.255]'")
                       │
         ① IndexQuery parsed by Spark SQL extensions
                       │
                       ▼
  ┌──────────────────────────────────────────────────────┐
  │  QueryParser                                          │
  │                                                      │
  │  Passes raw query string to tantivy4java's           │
  │  parseQuery() method unchanged                        │
  │  Tantivy's native query parser handles IP ranges      │
  └──────────────────────┬───────────────────────────────┘
                         │
         ② Tantivy parses range syntax natively
            [inclusive TO inclusive]
            {exclusive TO exclusive}
                         │
                         ▼
         (same execution pipeline as filter pushdown from ③ onward)
```

---

## 4. Layer 1 — Tantivy Core

### Schema Representation

Tantivy treats IP addresses as a first-class field type with a dedicated type code, `b'p'`. The field type is defined as `FieldType::IpAddr(IpAddrOptions)`, where `IpAddrOptions` controls whether the field is stored, indexed, has fast (columnar) access, and tracks field norms. This mirrors the pattern used for numeric and text types.

### The IPv6 Unification Strategy

At the heart of Tantivy's IP handling is the `IntoIpv6Addr` trait, which maps all IP addresses — whether v4 or v6 — into a single `Ipv6Addr` representation. IPv4 addresses are embedded using the standard IPv4-mapped IPv6 format defined in RFC 4291 Section 2.5.5.2. In this encoding, the first 80 bits are zero, the next 16 bits are all ones (`0xFFFF`), and the final 32 bits contain the IPv4 address.

This means `192.168.1.1` becomes `::ffff:192.168.1.1`, which in raw bytes is:

```
00 00 00 00 00 00 00 00 00 00 FF FF C0 A8 01 01
└──────── 80 zeros ─────────┘└FFFF┘└─192.168.1.1─┘
```

### Term Encoding

Each IP address term in the inverted index occupies exactly 21 bytes:

```
┌──────────────┬──────────┬──────────────────────────────────┐
│ Field ID     │ Type     │ IPv6 Address                     │
│ (4 bytes LE) │ (1 byte) │ (16 bytes, u128 big-endian)      │
│              │ b'p'     │                                  │
└──────────────┴──────────┴──────────────────────────────────┘
```

The big-endian byte ordering of the u128 is crucial: it preserves the natural lexicographic ordering of IP addresses, which means the term dictionary is automatically sorted in IP address order. This enables efficient range scans on the term dictionary without any special comparison logic.

### Fast Field (Columnar) Storage

When `fast=true` (which IndexTables4Spark always enables for IP fields), the IP address is also written to a columnar store as `ColumnType::IpAddr`. This stores one `u128` value per document, enabling:

- **Range queries** that scan the column directly rather than walking the term dictionary
- **Aggregation operations** (COUNT, GROUP BY) via fast field access
- **Min/max metadata** for query planning and short-circuiting

### Range Query Execution

Tantivy's range query implementation for IP addresses operates on u128 arithmetic:

1. Parse the bound IP addresses into `Ipv6Addr` values
2. For **exclusive** bounds, adjust by ±1 on the u128 representation (e.g., excluding `10.0.0.5` means the effective bound becomes `10.0.0.6` for lower bounds or `10.0.0.4` for upper bounds)
3. Load the `Column<Ipv6Addr>` from the fast field store
4. Use the column's min/max metadata to potentially short-circuit (if the entire segment is outside the range, return an empty scorer immediately)
5. Scan the column, producing a `RangeDocSet` of matching document IDs

### Inverted Index for Term Queries

For exact-match queries (`ip_addr:192.168.1.1`), Tantivy constructs a term with the 16-byte IPv6 encoding and looks it up directly in the term dictionary. The postings list (list of document IDs containing that term) is returned via the standard postings reader — no special IP-specific logic is needed here because the term is just a fixed-width byte sequence.

---

## 5. Layer 2 — Quickwit Split Format

### Field Mapping

Quickwit's doc mapper defines IP address fields via `QuickwitIpAddrOptions`, which wraps Tantivy's `IpAddrOptions` with Quickwit-specific defaults:

| Property  | Default | Notes |
|-----------|---------|-------|
| `stored`  | true    | Include raw value in document store |
| `indexed` | true    | Build inverted index terms |
| `fast`    | false   | Quickwit defaults to off; IndexTables4Spark overrides to true |

Quickwit maps the user-facing type name `"ip"` to Tantivy's `Type::IpAddr`. The mapping supports both single-valued fields and multi-valued (array) fields via a `Cardinality` parameter.

### Indexing Pipeline

When a JSON document arrives at Quickwit for indexing, the IP field value goes through:

1. **String parsing**: The string value (e.g., `"192.168.1.1"`) is parsed via `IpAddr::from_str()`
2. **IPv6 normalization**: The parsed address is converted to `Ipv6Addr` using the same `IntoIpv6Addr` trait that Tantivy uses
3. **Tantivy value creation**: A `TantivyValue::IpAddr(ipv6_value)` is produced and handed to Tantivy's segment writer

On the output side (when reading documents for query results), Quickwit's `tantivy_val_to_json` module reverses the normalization: it checks whether an `Ipv6Addr` is actually an IPv4-mapped address, and if so, extracts and formats the underlying IPv4 address. This ensures users see `"192.168.1.1"` rather than `"::ffff:192.168.1.1"`.

### Split File Contents

A `.split` file in QuickwitSplit format contains all the Tantivy segment data for a batch of documents. For IP address fields, this includes:

- **Term dictionary**: sorted 16-byte IPv6 entries with postings lists
- **Fast field column**: array of u128 values (one per document), with min/max metadata
- **Document store** (if stored): serialized field values including IPv6 string representation

The split file is self-contained and portable — it can be uploaded to S3/Azure and downloaded by any executor for querying.

---

## 6. Layer 3 — tantivy4java JNI Bridge

### Java API Surface

tantivy4java exposes IP address support through three API entry points:

**Schema Building:**
`SchemaBuilder.addIpAddrField(fieldName, stored, indexed, fast)` — Registers an IP address field in the schema. Maps directly to Tantivy's `schema_builder.add_ip_addr_field()` on the Rust side. The `fast` parameter must be `true` for range query support.

**Single Document Indexing:**
`Document.addIpAddr(fieldName, ipString)` — Adds an IP address value to a document. Accepts IPv4 (`"192.168.1.1"`) or IPv6 (`"2001:db8::1"`) strings. Parsing and IPv6 normalization happen on the Rust side.

**Batch Document Indexing:**
`BatchDocument.addIpAddr(fieldName, ipString)` — Same as above but for batch mode, which amortizes JNI overhead across many documents.

### JNI Boundary Characteristics

The JNI boundary for IP addresses is **string-based**: the Java side passes IP address values as UTF-8 strings, and the Rust side parses them. This design is simpler and safer than passing raw bytes, at the cost of a parse operation per value. The parsing cost is negligible relative to I/O.

### The FieldType Enum

tantivy4java defines a `FieldType` Java enum that includes `IP_ADDR`. This enum is used throughout IndexTables4Spark to route IP fields through the correct encoding and query construction paths. It was introduced in tantivy4java 0.28.8.

### TANT Binary Protocol (Read Path)

When retrieving documents via the batch API (`docBatchProjected`, `docBatchViaByteBuffer`), tantivy4java's Rust code serializes field values into its TANT binary protocol. For IP address fields, the Rust code in `batch_parsing.rs`:

1. Reads the stored `Ipv6Addr` value from Tantivy's document store
2. Calls `to_ipv6_mapped()` (which is a no-op for already-mapped addresses but normalizes any edge cases)
3. Formats the address as a string and writes it into the TANT binary response

This produces strings like `"::ffff:192.168.1.1"` for IPv4 addresses, which IndexTables4Spark must then clean up (see Section 9).

### SplitRangeQuery and SplitTermQuery

For queries, tantivy4java provides `SplitRangeQuery` and `SplitTermQuery` classes that accept a field type string. IP address range queries use the type string `"ip"`, which the Rust side maps to Tantivy's `Type::IpAddr` when constructing the native range query. Range bounds are specified as strings and parsed on the Rust side.

---

## 7. Layer 4 — IndexTables4Spark

### Field Configuration

IP address fields are declared in the Spark configuration using the typemap system:

**Per-field syntax:** `spark.indextables.indexing.typemap.client_ip: "ip"`

**List-based syntax (recommended):** `spark.indextables.indexing.typemap.ip: "client_ip,server_ip"`

**Companion syntax (SQL):** `BUILD INDEXTABLES COMPANION FOR ... INDEXING MODES ('client_ip':'ipaddress')`

Both `"ip"` and `"ipaddress"` are recognized mode names (case-insensitive). The IndexingModes utility normalizes these and validates that IP fields support exact match pushdown and range queries.

### Spark Schema Representation

IP address fields are represented as `StringType` in the Spark schema. There is no dedicated Spark SQL type for IP addresses; the type information lives in the typemap configuration and is carried through the IndexTables4Spark metadata layer. Users read and write IP addresses as plain strings in their DataFrames.

### Write Path Implementation

During document writing, `IndexTablesDirectInterface.addFieldGeneric()` checks the typemap configuration for each `StringType` field. If the field is configured as `"ip"`, the value is extracted from the Spark `UTF8String` representation, converted to a Java `String`, and passed to `adder.addIpAddr()`. The `FieldAdder` abstraction routes this call to either the single-document or batch-document API depending on the write mode.

Schema creation always sets `fast=true` for IP address fields. This is a hard-coded requirement because range queries (a primary use case for IP filtering) depend on fast field access in Tantivy.

### Filter Pushdown Implementation

`FiltersToQueryConverter` classifies `FieldType.IP_ADDR` as a numeric-like type, which enables the full range of comparison operators. The converter maps each Spark filter to a tantivy4java query object:

| Spark Filter | tantivy4java Query | Notes |
|---|---|---|
| `EqualTo` | `SplitTermQuery` | Exact IP match |
| `In` | Boolean OR of `SplitTermQuery` | Multiple IP match |
| `GreaterThan` | `SplitRangeQuery` (exclusive lower) | IP ordering |
| `GreaterThanOrEqual` | `SplitRangeQuery` (inclusive lower) | IP ordering |
| `LessThan` | `SplitRangeQuery` (exclusive upper) | IP ordering |
| `LessThanOrEqual` | `SplitRangeQuery` (inclusive upper) | IP ordering |
| `Not(EqualTo)` | Boolean NOT of `SplitTermQuery` | Exclusion |
| Combined range | Boolean AND of two `SplitRangeQuery` | Subnet filtering |

A special "date or IP field workaround" flag allows range queries on IP fields even when they are not explicitly listed in the fast fields configuration, because IP fields are always fast by construction.

### SchemaMapping Read Conversion

When reading IP values back from splits, `SchemaMapping` converts `FieldType.IP_ADDR` values to `StringType`. The critical step is stripping the `::ffff:` prefix that the TANT binary protocol adds to IPv4 addresses. This ensures users see canonical IPv4 format (`192.168.1.1`) rather than the internal IPv4-mapped IPv6 format.

---

## 8. Standard Splits vs Companion Splits

### Standard Splits

Standard splits are created by IndexTables4Spark's write path when a user writes a DataFrame directly:

```scala
df.write.format("io.indextables.provider.IndexTablesProvider")
  .option("spark.indextables.indexing.typemap.ip", "client_ip,server_ip")
  .save("s3://bucket/path")
```

The IP field configuration flows through `IndexTables4SparkOptions` → `IndexTablesDirectInterface` → tantivy4java's `SchemaBuilder.addIpAddrField()`. Each executor builds splits independently, and the split files are registered in the transaction log upon commit.

### Companion Splits

Companion splits are built from existing external tables (Delta, Parquet, Iceberg) via the SQL command:

```sql
BUILD INDEXTABLES COMPANION FOR delta.`/path/to/table`
  AT 's3://bucket/companion'
  INDEXING MODES ('client_ip':'ipaddress', 'hostname':'string')
```

The companion build path differs from standard writes in two ways:

1. **Configuration entry point**: The `INDEXING MODES` clause is parsed by `SyncTaskExecutor`, which extracts fields with mode `"ip"` or `"ipaddress"` and registers them via `companionConfig.withIpAddressFields()`

2. **Data source**: Instead of reading from Spark's InternalRow format, companion splits are built from Parquet files directly via `QuickwitSplit.createFromParquet()`. The IP address string values are read from Parquet columns and passed through the same tantivy4java `addIpAddr()` path.

### Encoding Equivalence

Once the data reaches tantivy4java, the encoding pipeline is **identical** for both standard and companion splits. The same Rust code parses the IP string, normalizes to IPv6, and indexes via Tantivy's segment writer. The resulting `.split` files are structurally indistinguishable — a query executor does not know (or care) whether a split was built from a standard write or a companion build.

### Query Path Equivalence

Both standard and companion splits are queried through the same `FiltersToQueryConverter` and IndexQuery pipelines. The Spark DataSource reads metadata from the transaction log (standard or companion), determines which splits to scan, and dispatches queries to executors. Each executor opens the split, runs the query against Tantivy, and returns results through `SchemaMapping` — with the same `::ffff:` stripping logic applied in both cases.

---

## 9. IPv4/IPv6 Normalization

### The Normalization Chain

IP addresses undergo several transformations as they move through the stack:

```
User input:  "192.168.1.1"   (IPv4 string)
                    │
                    ▼
Rust parse:  IpAddr::V4(Ipv4Addr(192,168,1,1))
                    │
                    ▼
IPv6 mapped: Ipv6Addr(::ffff:192.168.1.1)    ← to_ipv6_mapped()
                    │
                    ▼
u128 value:  0x00000000000000000000FFFFC0A80101
                    │
                    ▼
Term bytes:  00 00 00 00 00 00 00 00 00 00 FF FF C0 A8 01 01
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
   Inverted Index          Fast Field Column
   (postings list)         (u128 per doc)
```

### Round-Trip Behavior

On the return path, Tantivy stores the IPv6-mapped form. When the TANT binary protocol serializes this for transport back to Java, it produces the string `"::ffff:192.168.1.1"`. IndexTables4Spark's `SchemaMapping` then detects and strips the prefix to restore `"192.168.1.1"`.

For native IPv6 addresses (e.g., `"2001:db8::1"`), no mapping or stripping occurs — the address passes through unchanged at every layer.

### Ordering Implications

Because IPv4-mapped addresses occupy the `::ffff:0.0.0.0` through `::ffff:255.255.255.255` range in the IPv6 address space, all IPv4 addresses sort together in a contiguous block within the overall IPv6 ordering. This means:

- Range queries across IPv4 addresses work correctly (lexicographic order on the u128 matches numeric IPv4 order)
- IPv6 addresses sort separately from IPv4 addresses
- A range query spanning from IPv4 into IPv6 space would include all IPv4 addresses above the lower bound AND all IPv6 addresses below the upper bound, which may produce unexpected results for mixed-protocol ranges

---

## 10. Query Semantics and Operators

### Spark DataFrame API

```scala
// Exact match
df.filter($"ip" === "192.168.1.1")

// Range (subnet approximation)
df.filter($"ip" >= "10.0.0.0" && $"ip" <= "10.0.0.255")

// IN list
df.filter($"ip".isin("192.168.1.1", "10.0.0.1", "172.16.0.1"))

// Exclusion
df.filter($"ip" =!= "192.168.1.1")

// Between
df.filter($"ip".between("192.168.1.0", "192.168.1.255"))
```

All of these are pushed down to Tantivy — no post-filter evaluation on the Spark side.

### IndexQuery (Native Tantivy Syntax)

```sql
-- Exact match
SELECT * FROM t WHERE ip indexquery '192.168.1.1'

-- Inclusive range (both bounds included)
SELECT * FROM t WHERE ip indexquery '[10.0.0.0 TO 10.0.0.255]'

-- Exclusive range (both bounds excluded)
SELECT * FROM t WHERE ip indexquery '{10.0.0.0 TO 10.0.0.255}'

-- Mixed bounds (lower inclusive, upper exclusive)
SELECT * FROM t WHERE ip indexquery '[10.0.0.0 TO 10.0.1.0}'

-- Open-ended ranges
SELECT * FROM t WHERE ip indexquery '{192.168.1.0 TO *]'
SELECT * FROM t WHERE ip indexquery '[* TO 192.168.1.255}'

-- IPv6 (must be quoted due to colons)
SELECT * FROM t WHERE ip indexquery '"2001:db8::1"'

-- OR of multiple ranges
SELECT * FROM t WHERE ip indexquery '[10.0.0.0 TO 10.0.0.255] OR [172.16.0.0 TO 172.16.0.255]'

-- NOT (exclusion)
SELECT * FROM t WHERE ip indexquery 'NOT 192.168.1.1'
```

### Aggregation Support

Because IP fields always have fast field access enabled, they support aggregation operations:

```scala
// Count with IP filter
df.filter($"ip" === "192.168.1.1").agg(count("*"))

// Group by IP
df.groupBy("ip").agg(count("*"), sum("bytes"))

// Group by with filter
df.filter($"ip" >= "10.0.0.0").groupBy("ip").agg(avg("latency"))
```

### CIDR and Wildcard Patterns

CIDR notation and IPv4 wildcard patterns are transparently expanded by tantivy4java at the native layer. Callers pass these strings as normal term values and receive correct range semantics automatically.

**Supported syntax:**

| Pattern | Expands to |
|---|---|
| `192.168.1.0/24` | range `[192.168.1.0, 192.168.1.255]` |
| `10.0.0.0/8` | range `[10.0.0.0, 10.255.255.255]` |
| `192.168.1.0/32` | exact match on `192.168.1.0` |
| `0.0.0.0/0` | match all IP documents |
| `192.168.1.*` | range `[192.168.1.0, 192.168.1.255]` (same as `/24`) |
| `10.0.*.*` | range `[10.0.0.0, 10.0.255.255]` |
| `*.*.*.*` | match all IP documents |

IPv6 CIDR (`2001:db8::/32`) is also supported. IPv6 wildcards are not standard and are not supported.

> **IPv6 in IndexQuery requires quoting.** Colons in IPv6 addresses confuse Tantivy's query parser (it interprets them as field separators). Wrap any IPv6 address or CIDR in double quotes inside the IndexQuery string:
> ```sql
> -- Correct
> WHERE ip indexquery '"2001:db8::/32"'
> WHERE ip indexquery '"2001:db8::1"'
>
> -- Wrong — colons will be misinterpreted
> WHERE ip indexquery '2001:db8::/32'
> ```
> `EqualTo` and `IN` filter pushdown do **not** require quoting because they bypass the query parser entirely.

**Usage examples — all three query surfaces:**

```scala
// EqualTo (Spark filter pushdown) — no quoting needed
df.filter($"ip" === "192.168.1.0/24")
df.filter($"ip" === "192.168.1.*")

// IN (expanded to a union of ranges) — no quoting needed
df.filter($"ip".isin("10.0.0.0/8", "192.168.1.0/24"))
df.filter($"ip".isin("10.0.0.0/8", "192.168.1.1"))  // CIDR + exact IP

// IndexQuery SQL — IPv4 CIDR/wildcard unquoted, IPv6 must be quoted
spark.sql("SELECT * FROM t WHERE ip indexquery '192.168.1.0/24'")
spark.sql("SELECT * FROM t WHERE ip indexquery '192.168.1.*'")
spark.sql("SELECT * FROM t WHERE ip indexquery '192.168.1.0/24 OR 10.0.0.0/8'")
spark.sql("""SELECT * FROM t WHERE ip indexquery '"2001:db8::/32"'""")
```

**Boundary behaviour:** Both the network address (`192.168.1.0`) and the broadcast address (`192.168.1.255`) are **included** in the range.

**Non-trailing wildcards** (`10.*.1.*`) produce a single contiguous range over the first wildcard position; IPs that match the literal octets but fall inside the expanded range are over-approximated. Use explicit range queries when exact non-contiguous subnet semantics are required.

### What Is NOT Supported

- **Full-text search**: IP fields are not tokenized; they are atomic values.
- **Regular expressions on IPs**: Not supported as a query type.
- **IPv6 wildcards**: Non-standard; use IPv6 CIDR notation instead.

---

## 11. Known Issues and Workarounds

### IPv4-Mapped IPv6 Output Format (Active Workaround)

**Affected versions:** tantivy4java 0.29.3+

**Symptom:** When retrieving IP address values via the TANT binary protocol (used by `docBatchProjected()` and `docBatchViaByteBuffer()`), IPv4 addresses are returned in IPv6-mapped format (`::ffff:192.168.1.1`) instead of canonical IPv4 format (`192.168.1.1`).

**Root cause:** The `batch_parsing.rs` module in tantivy4java calls `to_ipv6_mapped()` on all IP address values during serialization, which is redundant (the values are already in IPv6-mapped form internally) but harmless from a data perspective. The issue is that the string formatting includes the `::ffff:` prefix.

**Workaround:** IndexTables4Spark's `SchemaMapping` strips the `::ffff:` prefix on all retrieved IP values. This produces the expected canonical format for IPv4 addresses while leaving IPv6 addresses unchanged.

**Expected resolution:** The tantivy4java team should either skip the `to_ipv6_mapped()` call in the output path or detect IPv4-mapped addresses and format them as plain IPv4 strings.

---

## 12. Appendix: Configuration Reference

### Writer Configuration

| Setting | Value | Description |
|---|---|---|
| `spark.indextables.indexing.typemap.<field>` | `"ip"` | Per-field IP type declaration |
| `spark.indextables.indexing.typemap.ip` | `"field1,field2"` | List-based IP type declaration |

### Companion Build Configuration

| SQL Clause | Value | Description |
|---|---|---|
| `INDEXING MODES ('field':'ip')` | — | Declare IP field in companion build |
| `INDEXING MODES ('field':'ipaddress')` | — | Alternative mode name |

### Implicit Behaviors

- **Fast field:** Always enabled for IP fields (hard-coded `fast=true`)
- **Stored:** Enabled by default (configurable)
- **Indexed:** Enabled by default (configurable)
- **Spark type:** Always `StringType` in the DataFrame schema
- **IPv4-mapped stripping:** Always active on the read path
