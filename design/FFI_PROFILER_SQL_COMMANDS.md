# Design: FFI Profiler SQL Commands

## Motivation

The tantivy4java FFI profiler provides near-zero-overhead instrumentation of the native Rust read path (54 timed sections + 5 cache counter pairs). Today it's only accessible via Java API (`FfiProfiler.enable()`, `.snapshot()`, etc.). Exposing it through SQL commands makes it usable from notebooks, `spark-sql`, and any JDBC/ODBC client without writing Scala/Java code.

## SQL Syntax

All commands support both `INDEXTABLES` and `TANTIVY4SPARK` keywords per existing convention.

### 1. ENABLE PROFILER

```sql
ENABLE INDEXTABLES PROFILER
```

Calls `FfiProfiler.enable()` on the driver. Since profiling state is a global atomic in the native library, and splits are read on executors, this command must also broadcast the enable signal to all active executors.

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| status | STRING | `"enabled"` |
| message | STRING | Human-readable confirmation |

**Example output:**
```
+--------+---------------------------------------------+
| status | message                                     |
+--------+---------------------------------------------+
| enabled| FFI profiler enabled (counters auto-reset)  |
+--------+---------------------------------------------+
```

### 2. DISABLE PROFILER

```sql
DISABLE INDEXTABLES PROFILER
```

Calls `FfiProfiler.disable()` on the driver and broadcasts to executors. Counters are preserved and remain readable after disable.

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| status | STRING | `"disabled"` |
| message | STRING | Human-readable confirmation |

### 3. DESCRIBE PROFILER

```sql
DESCRIBE INDEXTABLES PROFILER
```

Returns the current section counters as a DataFrame. Calls `FfiProfiler.snapshot()` on the driver. Sections with zero invocations are omitted.

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| section | STRING | Section name (e.g., `"leaf_search"`) |
| category | STRING | Logical group (e.g., `"search"`, `"aggregation"`, `"doc_retrieval"`, `"parquet_companion"`, `"streaming"`, `"arrow_ffi"`, `"prewarm"`, `"cache"`) |
| calls | LONG | Number of invocations |
| total_ms | DOUBLE | Cumulative wall-clock milliseconds |
| avg_us | DOUBLE | Average microseconds per call |
| min_us | DOUBLE | Fastest invocation in microseconds |
| max_us | DOUBLE | Slowest invocation in microseconds |

**Example output:**
```
+-------------------+----------+------+----------+---------+--------+---------+
| section           | category | calls| total_ms | avg_us  | min_us | max_us  |
+-------------------+----------+------+----------+---------+--------+---------+
| query_convert     | search   | 1000 |    21.50 |   21.50 |  18.20 |   45.00 |
| leaf_search       | search   | 1000 |  2841.00 | 2841.00 |2100.00 | 5200.00 |
| doc_batch_prefetch| doc_retr | 1000 |   320.00 |  320.00 | 280.00 |  450.00 |
+-------------------+----------+------+----------+---------+--------+---------+
```

**Cache counters variant:**

```sql
DESCRIBE INDEXTABLES PROFILER CACHE
```

Returns cache hit/miss counters only, via `FfiProfiler.cacheCounters()`.

**Output schema:**

| Column | Type | Description |
|--------|------|-------------|
| cache | STRING | Cache name (e.g., `"byte_range"`, `"l2_disk"`, `"searcher"`) |
| hits | LONG | Hit count |
| misses | LONG | Miss count |
| hit_rate | DOUBLE | `hits / (hits + misses)`, or `null` if both are 0 |

**Example output:**
```
+-------------------+------+--------+----------+
| cache             | hits | misses | hit_rate |
+-------------------+------+--------+----------+
| byte_range        | 4520 |    380 |     0.92 |
| l2_disk           |  200 |    800 |     0.20 |
| searcher          |  990 |     10 |     0.99 |
| parquet_metadata  |  500 |      0 |     1.00 |
| pq_column         |  300 |     50 |     0.86 |
+-------------------+------+--------+----------+
```

### 4. RESET PROFILER

```sql
RESET INDEXTABLES PROFILER
```

Calls `FfiProfiler.reset()` and `FfiProfiler.resetCacheCounters()` on the driver, returning the pre-reset values. Output schema is the same two-part result as DESCRIBE: first the section rows, then the cache rows.

**Output schema** (combined — same as DESCRIBE PROFILER, with cache rows appended):

| Column | Type | Description |
|--------|------|-------------|
| section | STRING | Section name or cache counter name |
| category | STRING | `"search"`, `"cache"`, etc. |
| calls | LONG | Invocation count (for cache rows: hit count) |
| total_ms | DOUBLE | Total ms (for cache rows: miss count cast to double) |
| avg_us | DOUBLE | Average us (for cache rows: hit rate) |
| min_us | DOUBLE | Min us (for cache rows: null) |
| max_us | DOUBLE | Max us (for cache rows: null) |

> **Alternative:** Return the same schema as DESCRIBE PROFILER for sections only, discarding cache counters on reset. The cache counters can be reset separately with `RESET INDEXTABLES PROFILER CACHE`. This avoids overloading columns. **Recommendation: use this alternative** — see Detailed Design below.

### Refined Syntax (Recommended)

```sql
-- Enable/disable
ENABLE  INDEXTABLES PROFILER
DISABLE INDEXTABLES PROFILER

-- Read counters (non-destructive)
DESCRIBE INDEXTABLES PROFILER                -- section timings
DESCRIBE INDEXTABLES PROFILER CACHE          -- cache hit/miss

-- Read-and-reset (atomic snapshot + zero)
RESET INDEXTABLES PROFILER                   -- section timings
RESET INDEXTABLES PROFILER CACHE             -- cache hit/miss
```

This gives 6 commands total but only 4 grammar rules (ENABLE/DISABLE are parameterless; DESCRIBE/RESET each have an optional CACHE modifier).

## ANTLR Grammar Additions

New keywords needed: `ENABLE`, `DISABLE`, `RESET`, `PROFILER`.

```antlr
// In the statement rule, add before the passThrough catch-all:
    | ENABLE indexTablesKeyword PROFILER                          #enableProfiler
    | DISABLE indexTablesKeyword PROFILER                         #disableProfiler
    | DESCRIBE indexTablesKeyword PROFILER CACHE?                 #describeProfiler
    | RESET indexTablesKeyword PROFILER CACHE?                    #resetProfiler
```

New keyword tokens:

```antlr
ENABLE: [Ee][Nn][Aa][Bb][Ll][Ee];
DISABLE: [Dd][Ii][Ss][Aa][Bb][Ll][Ee];
RESET: [Rr][Ee][Ss][Ee][Tt];
PROFILER: [Pp][Rr][Oo][Ff][Ii][Ll][Ee][Rr];
```

Add `ENABLE | DISABLE | RESET | PROFILER` to the `nonReserved` rule so they can still be used as identifiers.

## AST Builder Additions

In `IndexTables4SparkSqlAstBuilder.scala`:

```scala
override def visitEnableProfiler(ctx: EnableProfilerContext): LogicalPlan =
  EnableFfiProfilerCommand()

override def visitDisableProfiler(ctx: DisableProfilerContext): LogicalPlan =
  DisableFfiProfilerCommand()

override def visitDescribeProfiler(ctx: DescribeProfilerContext): LogicalPlan =
  DescribeFfiProfilerCommand(cacheOnly = ctx.CACHE() != null)

override def visitResetProfiler(ctx: ResetProfilerContext): LogicalPlan =
  ResetFfiProfilerCommand(cacheOnly = ctx.CACHE() != null)
```

## Command Implementations

### File: `FfiProfilerCommands.scala`

All four commands, shared schemas/helpers (`FfiProfilerCommands`), and the cluster-wide dispatch helper (`FfiProfilerExecutorHelper`) in a single file.

**Key implementation decisions:**

1. **ProfileEntry is not Serializable.** The executor-side closures extract primitive tuples `(count, totalNanos, minNanos, maxNanos)` before returning results through Spark's RDD collect. A type alias `SectionData = (Long, Long, Long, Long)` is used throughout.

2. **Local mode double-counting prevention.** `FfiProfilerExecutorHelper.runOnEachHost` detects when there are no real executors (`executorCount <= 0`) and returns empty. The caller always collects driver counters separately, so in local mode only the driver collection runs.

3. **Schemas are shared.** `FfiProfilerCommands` object holds `sectionSchema` and `cacheSchema` as vals, referenced by both DESCRIBE and RESET commands.

#### Command classes (all `LeafRunnableCommand`)

- `EnableFfiProfilerCommand` — calls `FfiProfiler.enable()` on driver, broadcasts via `FfiProfilerExecutorHelper.broadcastToHosts`. Output: `(status, host_count, message)`.
- `DisableFfiProfilerCommand` — same pattern with `FfiProfiler.disable()`.
- `DescribeFfiProfilerCommand(cacheOnly: Boolean)` — delegates to `FfiProfilerExecutorHelper.collectSectionCounters` or `collectCacheCounters` with `reset = false`.
- `ResetFfiProfilerCommand(cacheOnly: Boolean)` — same delegation with `reset = true`.

### Section Category Mapping

A private helper maps section name prefixes to human-readable categories:

```scala
private def categoryFor(section: String): String = section match {
  case s if s.startsWith("agg_")            => "aggregation"
  case s if s.startsWith("doc_batch_")      => "doc_retrieval"
  case s if s.startsWith("pq_doc_")         => "pq_companion_doc"
  case s if s.startsWith("single_doc_")     => "single_doc"
  case s if s.startsWith("pc_hash_")        => "pq_companion_hash"
  case s if s.startsWith("pc_")             => "pq_companion_transcode"
  case s if s.startsWith("stream_")         => "streaming"
  case s if s.startsWith("arrow_ffi_")      => "arrow_ffi"
  case "prewarm_components"                 => "prewarm"
  case _                                    => "search"
}
```

### Cache Counter Pairing

The `cacheCounters()` map returns flat keys like `byte_range_hit`, `byte_range_miss`. `buildCacheRows` groups them into paired rows with computed `hit_rate`. Caches with zero hits and misses are omitted. Input is `Map[String, Long]` (already deserialized from executor primitives).

## Driver-Only vs. Cluster-Wide Scope

The FFI profiler state is **global per host** (global atomics in the native library). This means:

- `FfiProfiler.enable()` on the driver enables profiling **only on that host**
- Each host has its own independent profiler state
- `FfiProfiler.snapshot()` on the driver reads **only driver-host counters**

Since all actual search work happens on executor hosts, we need the counters from every host.

### Approach: Preferred-Location RDD (reusing prewarm pattern)

The prewarm command already solves the "run on every host" problem using `DriverSplitLocalityManager.getAvailableHosts(sc)` for host discovery and `sc.makeRDD()` with preferred locations. We reuse this pattern. Since profiler counters are global per host, `getAvailableHosts()` deduplication by hostname is exactly right — we want one partition per unique host, not per executor JVM.

#### Shared Helper: `FfiProfilerExecutorHelper`

A `private[sql]` object in `FfiProfilerCommands.scala` encapsulates the cluster-wide dispatch logic. Key methods:

- `runOnEachHost[T](sc, fn)` — Creates one partition per unique host via `sc.makeRDD()` with preferred locations. Returns `Seq[T]`. In local mode (no real executors), returns empty — callers always collect driver counters separately.
- `collectSectionCounters(sc, reset)` — Collects from all hosts + driver, merges via `mergeProfiles`. Returns `Map[String, SectionData]` where `SectionData = (Long, Long, Long, Long)` (count, totalNanos, minNanos, maxNanos). The tuple type is used instead of `ProfileEntry` because **ProfileEntry is not Serializable** — primitives must be extracted inside the executor closure before Spark serializes task results.
- `collectCacheCounters(sc, reset)` — Same pattern, returns `Map[String, Long]`.
- `broadcastToHosts(sc, enable)` — Broadcasts enable/disable to all executor hosts. Returns count of hosts reached.

#### Why `sc.makeRDD` with Preferred Locations

A naive `sc.parallelize(1 to N, N).foreachPartition(...)` gives Spark no location hints — the scheduler may colocate multiple partitions on the same executor and skip others entirely. The `sc.makeRDD(Seq[(T, Seq[String])])` overload accepts per-partition preferred locations, which the Spark scheduler uses as **soft hints** during task assignment. By creating exactly one partition per known host, each with that host as its preferred location, we maximize the chance of hitting every host exactly once.

This is the same mechanism used by `PrewarmCacheCommand.runSync()` where it creates `tasksWithLocations = tasks.map(t => (t, Seq(t.hostname)))` and passes them to `sc.makeRDD()`.

The host discovery reuses `DriverSplitLocalityManager.getAvailableHosts(sc)`, which:
1. Gets all block managers via `sc.getExecutorMemoryStatus.keys`
2. Filters out the driver's block manager
3. Extracts and **deduplicates** hostnames from `host:port` pairs
4. Falls back to localhost in local mode

The hostname deduplication is critical: since profiler counters are global per host, collecting from multiple executors on the same host would double-count. One partition per unique host is exactly right.

#### Local Mode Double-Counting Prevention

In local mode, `getAvailableHosts()` returns `Set(localhost)` but there are no real executors — the driver IS the only JVM. If we collected via `runOnEachHost` AND separately collected driver counters, we'd double-count. The implementation handles this by checking `sc.getExecutorMemoryStatus.size - 1` (exclude driver) — if zero, `runOnEachHost` returns empty, so only the driver collection runs.

#### Locality Mismatch Handling

Unlike the prewarm command, we do **not** retry on locality mismatch. The prewarm command retries because it needs specific splits on specific executors (cache locality). For the profiler, if a task lands on a host we already visited, the merged result simply double-counts that host's counters — but since preferred locations are soft hints and we create exactly one partition per host, this is unlikely. If a host is missed entirely, its counters are absent from the result.

This is acceptable for a diagnostic tool. The user should run ENABLE before their workload and DESCRIBE/RESET after — the hosts that actually served the workload will have the relevant counters.

## Implementation Status

All items implemented and tested (19 tests passing):

1. **ANTLR grammar** (`IndexTables4SparkSqlBase.g4`) — 4 statement rules, 4 keyword tokens, `nonReserved` updated
2. **AST builder** (`IndexTables4SparkSqlAstBuilder.scala`) — 4 visitor methods, 4 imports
3. **Command file** (`FfiProfilerCommands.scala`) — 4 command case classes, `FfiProfilerCommands` shared object, `FfiProfilerExecutorHelper` dispatch object
4. **Tests** (`FfiProfilerCommandsTest.scala`) — 19 tests covering parsing, schemas, enable/disable, describe/reset, keyword aliases, case insensitivity, queryability, integration with real data write/read
5. **Reference docs** (`docs/reference/sql-commands.md`) — Full syntax, output schemas, example workflow
6. **CLAUDE.md** — Updated Available commands list

## Example User Workflow

```sql
-- 1. Enable profiling
ENABLE INDEXTABLES PROFILER;

-- 2. Run workload
SELECT * FROM my_index WHERE title = 'example' LIMIT 100;
SELECT COUNT(*), AVG(score) FROM my_index WHERE category = 'tech';

-- 3. Check timing breakdown
DESCRIBE INDEXTABLES PROFILER;
-- Shows which sections consumed the most time

-- 4. Check cache effectiveness
DESCRIBE INDEXTABLES PROFILER CACHE;
-- Shows hit rates for each cache layer

-- 5. Reset and run another workload to compare
RESET INDEXTABLES PROFILER;
SELECT * FROM my_index WHERE content indexquery 'machine learning';
DESCRIBE INDEXTABLES PROFILER;

-- 6. Disable when done
DISABLE INDEXTABLES PROFILER;
```
